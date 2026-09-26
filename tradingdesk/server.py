"""The Trading Desk web app.

A FastAPI service that starts TradingAgents runs, streams their progress to the
browser and keeps the finished reports, plus the single-page UI that drives it.
Configuration comes from the same ``.env`` and ``TRADINGAGENTS_*`` variables as
the CLI, and API keys never leave the server. ``tradingdesk`` on the command
line serves it with uvicorn.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import secrets
import uuid
from collections.abc import Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from cli.display import ANALYST_ORDER
from cli.models import AnalystType, AssetType
from cli.prompts import (
    detect_asset_type,
    filter_analysts_for_asset_type,
    is_valid_ticker_input,
    normalize_ticker_symbol,
)
from tradingagents.dataflows.date_window import get_current_date
from tradingagents.dataflows.symbols import safe_ticker_component
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.portfolio import PortfolioContext
from tradingdesk import catalog
from tradingdesk.keys import env_file_path, forget_api_key, save_api_key
from tradingdesk.runs import TERMINAL_STATUSES, Run, RunManager

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
SSE_KEEPALIVE_SECONDS = 15
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")

# Request field -> the provider whose reasoning knob it is.
_THINKING_FIELDS = {
    "openai_reasoning_effort": "openai",
    "google_thinking_level": "google",
    "anthropic_effort": "anthropic",
}


def _version() -> str:
    try:
        return metadata.version("tradingagents")
    except metadata.PackageNotFoundError:
        return "unknown"


@dataclass
class Settings:
    """How the server runs. ``from_env`` reads the ``TRADINGDESK_*`` variables."""

    results_dir: str
    api_token: str | None = None
    max_workers: int = 1
    # Where keys saved from the browser go; None means the .env the package loads.
    env_file: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            results_dir=DEFAULT_CONFIG["results_dir"],
            api_token=os.environ.get("TRADINGDESK_API_TOKEN") or None,
            max_workers=int(os.environ.get("TRADINGDESK_MAX_CONCURRENT_RUNS") or 1),
            env_file=os.environ.get("TRADINGDESK_ENV_FILE") or None,
        )


class RunRequest(BaseModel):
    """A run as the form submits it; unset fields take the server's configured defaults."""

    ticker: str = Field(min_length=1, max_length=32)
    analysis_date: str | None = Field(default=None, validate_default=True)
    analysts: list[str] = Field(default_factory=lambda: list(ANALYST_ORDER))
    research_depth: int = Field(
        default_factory=lambda: DEFAULT_CONFIG["max_debate_rounds"], ge=1, le=5
    )
    llm_provider: str = Field(default_factory=lambda: DEFAULT_CONFIG["llm_provider"])
    backend_url: str | None = None
    quick_think_llm: str = Field(default_factory=lambda: DEFAULT_CONFIG["quick_think_llm"])
    deep_think_llm: str = Field(default_factory=lambda: DEFAULT_CONFIG["deep_think_llm"])
    output_language: str = Field(default_factory=lambda: DEFAULT_CONFIG["output_language"])
    openai_reasoning_effort: str | None = None
    google_thinking_level: str | None = None
    anthropic_effort: str | None = None
    checkpoint: bool | None = None
    portfolio: PortfolioContext | None = None

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        value = value.strip()
        if not value or not is_valid_ticker_input(value):
            raise ValueError("use letters, digits and . _ - ^ = only, e.g. SPY, 0700.HK, BTC-USD")
        canonical = normalize_ticker_symbol(value)
        safe_ticker_component(canonical)  # rejects values like ".." that would escape a directory
        return canonical

    @field_validator("analysis_date")
    @classmethod
    def _analysis_date(cls, value: str | None) -> str:
        today = get_current_date()
        if value is None or not value.strip():
            return today
        value = value.strip()
        try:
            canonical = datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d") == value
        except ValueError:
            canonical = False
        if not canonical:
            raise ValueError("must be a date in YYYY-MM-DD format")
        if value > today:
            raise ValueError("cannot be in the future")
        return value

    @field_validator("analysts")
    @classmethod
    def _analysts(cls, value: list[str]) -> list[str]:
        known = {analyst.value for analyst in AnalystType}
        chosen = {str(item).strip().lower() for item in value}
        unknown = sorted(chosen - known)
        if unknown:
            raise ValueError(
                f"unknown analysts: {', '.join(unknown)}; choose from {', '.join(ANALYST_ORDER)}"
            )
        ordered = [key for key in ANALYST_ORDER if key in chosen]
        if not ordered:
            raise ValueError("select at least one analyst")
        return ordered

    @field_validator("llm_provider")
    @classmethod
    def _provider(cls, value: str) -> str:
        key = value.strip().lower()
        if key not in catalog.known_providers():
            raise ValueError(f"unknown provider {value!r}")
        return key

    @field_validator("quick_think_llm", "deep_think_llm", "output_language")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("backend_url")
    @classmethod
    def _backend_url(cls, value: str | None) -> str | None:
        value = (value or "").strip()
        if not value:
            return None
        if not value.startswith(("http://", "https://")):
            raise ValueError("must start with http:// or https://")
        return value

    @field_validator("openai_reasoning_effort", "google_thinking_level", "anthropic_effort")
    @classmethod
    def _thinking(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is None or not value.strip():
            return None
        provider = _THINKING_FIELDS[info.field_name]
        allowed = [choice["value"] for choice in catalog.THINKING_OPTIONS[provider]["choices"]]
        if value not in allowed:
            raise ValueError(f"must be one of {', '.join(allowed)}")
        return value


class ApiKeyRequest(BaseModel):
    """A provider API key pasted into the form."""

    api_key: str = Field(min_length=1, max_length=512)

    @field_validator("api_key")
    @classmethod
    def _api_key(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("paste the key first")
        if any(ch.isspace() for ch in value) or any(ch in value for ch in "'\""):
            raise ValueError("a key has no spaces or quotes; check what was pasted")
        return value


def resolve_backend_url(req: RunRequest) -> str | None:
    """The form's URL; else the env override, when it was set for this provider; else the menu default."""
    if req.backend_url:
        return req.backend_url
    if req.llm_provider == DEFAULT_CONFIG["llm_provider"] and DEFAULT_CONFIG["backend_url"]:
        return DEFAULT_CONFIG["backend_url"]
    return catalog.default_backend_url(req.llm_provider)


def build_run_config(req: RunRequest, backend_url: str | None, results_dir: str) -> dict[str, Any]:
    """The graph config for a request: the server's defaults with the form's choices on top."""
    config = dict(DEFAULT_CONFIG)
    config["results_dir"] = results_dir
    config["llm_provider"] = req.llm_provider
    config["backend_url"] = backend_url
    config["quick_think_llm"] = req.quick_think_llm
    config["deep_think_llm"] = req.deep_think_llm
    config["max_debate_rounds"] = req.research_depth
    config["max_risk_discuss_rounds"] = req.research_depth
    config["output_language"] = req.output_language
    for key, provider in _THINKING_FIELDS.items():
        if req.llm_provider != provider:
            config[key] = None
            continue
        value = getattr(req, key)
        config[key] = DEFAULT_CONFIG[key] if value is None else value
    if req.checkpoint is not None:
        config["checkpoint_enabled"] = req.checkpoint
    return config


def public_settings(config: dict[str, Any], req: RunRequest) -> dict[str, Any]:
    """The part of a run's config worth showing and keeping: no paths, no secrets."""
    thinking = next(
        (config[key] for key, provider in _THINKING_FIELDS.items() if provider == req.llm_provider),
        None,
    )
    return {
        "llm_provider": config["llm_provider"],
        "backend_url": config["backend_url"],
        "quick_think_llm": config["quick_think_llm"],
        "deep_think_llm": config["deep_think_llm"],
        "research_depth": req.research_depth,
        "output_language": config["output_language"],
        "thinking": thinking,
        "checkpoint_enabled": bool(config["checkpoint_enabled"]),
        "portfolio": req.portfolio is not None,
    }


def _sse(event: dict) -> str:
    payload = json.dumps(event["data"], ensure_ascii=False)
    return f"id: {event['seq']}\nevent: {event['kind']}\ndata: {payload}\n\n"


def _is_terminal(event: dict) -> bool:
    return event["kind"] == "status" and event["data"].get("status") in TERMINAL_STATUSES


def event_stream(run: Run, after_seq: int = 0) -> Iterator[str]:
    """Replay the run's events after ``after_seq``, then relay new ones until it ends."""
    subscriber, backlog = run.subscribe(after_seq)
    try:
        for event in backlog:
            yield _sse(event)
            if _is_terminal(event):
                return
        if run.status in TERMINAL_STATUSES:
            return
        while True:
            try:
                event = subscriber.get(timeout=SSE_KEEPALIVE_SECONDS)
            except queue.Empty:
                yield ": keep-alive\n\n"
                continue
            yield _sse(event)
            if _is_terminal(event):
                return
    finally:
        run.unsubscribe(subscriber)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the application; ``settings`` defaults to the environment's."""
    settings = settings or Settings.from_env()
    # Keys saved from the browser live here; pick them up before the first run.
    key_file = env_file_path(settings.env_file)
    if settings.env_file and key_file.is_file():
        load_dotenv(key_file, override=False)
    manager = RunManager(settings.results_dir, max_workers=settings.max_workers)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        manager.shutdown()

    app = FastAPI(
        title="Trading Desk",
        version=_version(),
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )
    app.state.settings = settings
    app.state.runs = manager

    def require_token(request: Request) -> None:
        if settings.api_token is None:
            return
        scheme, _, credential = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() == "bearer" and secrets.compare_digest(
            credential.strip(), settings.api_token
        ):
            return
        raise HTTPException(
            status_code=401,
            detail="A valid API token is required.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    api = APIRouter(prefix="/api", dependencies=[Depends(require_token)])

    def get_run(run_id: str) -> Run:
        run = manager.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="no such run")
        return run

    @app.get("/api/health")
    def health() -> dict:
        return {
            "status": "ok",
            "version": _version(),
            "auth_required": settings.api_token is not None,
            "max_concurrent_runs": settings.max_workers,
        }

    def key_env_for(provider: str) -> tuple[str, str]:
        key = provider.strip().lower()
        row = catalog.provider_row(key)
        if row is None:
            raise HTTPException(status_code=404, detail="no such provider")
        if row["api_key_env"] is None:
            raise HTTPException(
                status_code=422, detail=f"the {key} provider does not use an API key"
            )
        return key, row["api_key_env"]

    @api.get("/options")
    def options() -> dict:
        data = catalog.options()
        data["env_file"] = str(key_file)
        return data

    @api.put("/keys/{provider}")
    def save_key(provider: str, req: ApiKeyRequest) -> dict:
        key, env_var = key_env_for(provider)
        path = save_api_key(env_var, req.api_key, settings.env_file)
        logger.info("Saved %s to %s", env_var, path)
        return {"provider": catalog.provider_row(key), "env_file": str(path)}

    @api.delete("/keys/{provider}")
    def forget_key(provider: str) -> dict:
        key, env_var = key_env_for(provider)
        path = forget_api_key(env_var, settings.env_file)
        logger.info("Removed %s from %s", env_var, path)
        return {"provider": catalog.provider_row(key), "env_file": str(path)}

    @api.get("/runs")
    def list_runs() -> list[dict]:
        return manager.list()

    @api.post("/runs", status_code=202)
    def start_run(req: RunRequest) -> dict:
        asset_type = detect_asset_type(req.ticker).value
        analysts = [
            analyst.value
            for analyst in filter_analysts_for_asset_type(
                [AnalystType(key) for key in req.analysts], AssetType(asset_type)
            )
        ]
        if not analysts:
            raise HTTPException(
                status_code=422,
                detail="Crypto runs have no fundamentals analyst; pick at least one other analyst.",
            )
        missing = catalog.missing_api_key(req.llm_provider)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{missing} is not set on the server, so the {req.llm_provider} provider "
                    "cannot run. Paste the key in the API key box under the provider dropdown, "
                    "or add it to the server's .env."
                ),
            )
        backend_url = resolve_backend_url(req)
        if req.llm_provider == "openai_compatible" and not backend_url:
            raise HTTPException(
                status_code=422,
                detail="The OpenAI-compatible provider needs a backend URL, e.g. http://localhost:8000/v1.",
            )
        config = build_run_config(req, backend_url, settings.results_dir)
        run = Run(
            id=uuid.uuid4().hex[:12],
            ticker=req.ticker,
            analysis_date=req.analysis_date,
            asset_type=asset_type,
            analysts=analysts,
            settings=public_settings(config, req),
            config=config,
            portfolio=req.portfolio,
        )
        manager.submit(run)
        return run.snapshot()

    @api.get("/runs/{run_id}")
    def get_run_snapshot(run_id: str) -> dict:
        return get_run(run_id).snapshot()

    @api.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str) -> dict:
        run = get_run(run_id)
        if not manager.cancel(run.id):
            raise HTTPException(status_code=409, detail=f"run is already {run.status}")
        return run.snapshot()

    @api.get("/runs/{run_id}/events")
    def run_events(run_id: str, request: Request) -> StreamingResponse:
        run = get_run(run_id)
        try:
            after = int(request.headers.get("last-event-id") or 0)
        except ValueError:
            after = 0
        return StreamingResponse(
            event_stream(run, after),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @api.get("/runs/{run_id}/report")
    def run_report(run_id: str) -> FileResponse:
        run = get_run(run_id)
        path = run.report_file()
        if path is None:
            raise HTTPException(status_code=404, detail="this run has no report")
        return FileResponse(
            path,
            media_type="text/markdown",
            filename=f"{run.ticker}_{run.analysis_date}_report.md",
        )

    app.include_router(api)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


def main(argv: list[str] | None = None) -> None:
    """Serve the app: ``tradingdesk [--host H] [--port P] [--reload]``."""
    parser = argparse.ArgumentParser(prog="tradingdesk", description="Serve the Trading Desk web app.")
    parser.add_argument(
        "--host",
        default=os.environ.get("TRADINGDESK_HOST") or "127.0.0.1",
        help="address to listen on (default: TRADINGDESK_HOST or 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("TRADINGDESK_PORT") or 8000),
        help="port to listen on (default: TRADINGDESK_PORT or 8000)",
    )
    parser.add_argument(
        "--reload", action="store_true", help="restart when the code changes (development)"
    )
    args = parser.parse_args(argv)

    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not os.environ.get("TRADINGDESK_API_TOKEN") and args.host not in LOOPBACK_HOSTS:
        logger.warning(
            "Serving on %s without TRADINGDESK_API_TOKEN: anyone who can reach this port can "
            "start runs with this server's API keys.",
            args.host,
        )
    uvicorn.run(
        "tradingdesk.server:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
