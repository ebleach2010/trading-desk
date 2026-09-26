"""Running analyses for the web app.

A run executes on a worker thread exactly as ``cli.run.run_analysis`` does: the
graph is streamed, each state chunk moves the agent statuses along and fills in
report sections, and a clean finish logs the decision and writes the report
tree. Instead of a terminal, every change is appended to the run's event log
and fanned out to subscribers, which the server relays as server-sent events.
The finished run is written as ``run.json`` next to its reports under the
results directory, so history survives a restart.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cli.display import MessageBuffer, classify_message_type, update_analyst_statuses
from cli.stats_handler import StatsCallbackHandler
from tradingagents.agents.rating import is_review
from tradingagents.dataflows.config import run_config
from tradingagents.dataflows.symbols import safe_ticker_component
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.reporting import write_report_tree

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
RUN_FILE = "run.json"
MESSAGE_LOG = "messages.log"
MESSAGE_PREVIEW_CHARS = 500

RESEARCH_TEAM = ("Bull Researcher", "Bear Researcher", "Research Manager")
RISK_HISTORIES = (
    ("aggressive_history", "Aggressive Analyst"),
    ("conservative_history", "Conservative Analyst"),
    ("neutral_history", "Neutral Analyst"),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RunCancelled(Exception):
    """Raised inside the worker when a stop was requested between graph steps."""


def sections_from_state(state: dict) -> dict[str, str]:
    """The report sections present in a graph state, keyed like ``catalog.SECTIONS``.

    Chunks streamed with ``stream_mode="values"`` are full states, so the same
    mapping serves both the live updates and the final state.
    """
    out: dict[str, str] = {}
    for key in ("market_report", "sentiment_report", "news_report", "fundamentals_report",
                "trader_investment_plan", "final_trade_decision"):
        if state.get(key):
            out[key] = str(state[key]).strip()
    debate = state.get("investment_debate_state") or {}
    for key in ("bull_history", "bear_history"):
        if debate.get(key):
            out[key] = str(debate[key]).strip()
    if debate.get("judge_decision"):
        out["research_manager"] = str(debate["judge_decision"]).strip()
    risk = state.get("risk_debate_state") or {}
    for key, _ in RISK_HISTORIES:
        if risk.get(key):
            out[key] = str(risk[key]).strip()
    if "final_trade_decision" not in out and risk.get("judge_decision"):
        out["final_trade_decision"] = str(risk["judge_decision"]).strip()
    return {key: text for key, text in out.items() if text}


@dataclass(eq=False)
class Run:
    """One analysis: what was asked, where it is, and everything it has produced."""

    id: str
    ticker: str
    analysis_date: str
    asset_type: str
    analysts: list[str]
    settings: dict[str, Any]
    config: dict[str, Any] = field(default_factory=dict, repr=False)
    portfolio: Any = field(default=None, repr=False)
    status: str = "queued"
    created_at: str = field(default_factory=_now)
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    signal: str | None = None
    review: bool = False
    agent_status: dict[str, str] = field(default_factory=dict)
    sections: dict[str, str] = field(default_factory=dict)
    messages: deque = field(default_factory=lambda: deque(maxlen=200))
    stats: dict[str, int] = field(default_factory=dict)
    out_dir: Path | None = None
    report_path: Path | None = None
    cancel_requested: bool = False
    events: list[dict] = field(default_factory=list, repr=False)
    subscribers: list[queue.Queue] = field(default_factory=list, repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # --- live updates -------------------------------------------------------

    def _emit(self, kind: str, data: dict) -> None:
        """Append an event and hand it to every subscriber. Caller holds ``lock``."""
        event = {"seq": len(self.events) + 1, "kind": kind, "at": _now(), "data": data}
        self.events.append(event)
        for subscriber in self.subscribers:
            subscriber.put(event)

    def subscribe(self, after_seq: int = 0) -> tuple[queue.Queue, list[dict]]:
        """A queue that receives every event from here on, plus the ones after ``after_seq``."""
        with self.lock:
            subscriber: queue.Queue = queue.Queue()
            self.subscribers.append(subscriber)
            backlog = [event for event in self.events if event["seq"] > after_seq]
            return subscriber, backlog

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self.lock:
            if subscriber in self.subscribers:
                self.subscribers.remove(subscriber)

    def set_status(self, status: str, **extra: Any) -> None:
        with self.lock:
            self.status = status
            for key, value in extra.items():
                setattr(self, key, value)
            self._emit("status", {"status": status, **extra})

    def set_agents(self, agents: dict[str, str]) -> None:
        with self.lock:
            if agents == self.agent_status:
                return
            self.agent_status = dict(agents)
            self._emit("agents", {"agents": dict(agents)})

    def set_section(self, key: str, content: str) -> None:
        with self.lock:
            if self.sections.get(key) == content:
                return
            self.sections[key] = content
            self._emit("section", {"key": key, "content": content})

    def set_stats(self, stats: dict[str, int]) -> None:
        with self.lock:
            if stats == self.stats:
                return
            self.stats = dict(stats)
            self._emit("stats", dict(stats))

    def add_message(self, kind: str, content: str) -> None:
        """Keep a preview for the live feed; the full text goes to the run's log file."""
        text = " ".join(str(content).split())
        preview = text if len(text) <= MESSAGE_PREVIEW_CHARS else text[:MESSAGE_PREVIEW_CHARS - 1] + "…"
        entry = {"at": _now(), "kind": kind, "content": preview}
        with self.lock:
            self.messages.append(entry)
            self._emit("message", entry)
        if self.out_dir is not None:
            try:
                with open(self.out_dir / MESSAGE_LOG, "a", encoding="utf-8") as handle:
                    handle.write(f"{entry['at']} [{kind}] {text}\n")
            except OSError:
                logger.warning("Could not append to %s", self.out_dir / MESSAGE_LOG, exc_info=True)

    # --- views --------------------------------------------------------------

    def summary(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "ticker": self.ticker,
                "analysis_date": self.analysis_date,
                "asset_type": self.asset_type,
                "status": self.status,
                "signal": self.signal,
                "review": self.review,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "llm_provider": self.settings.get("llm_provider"),
            }

    def record(self) -> dict:
        """The run as written to disk: everything but the live plumbing."""
        with self.lock:
            return {
                "id": self.id,
                "ticker": self.ticker,
                "analysis_date": self.analysis_date,
                "asset_type": self.asset_type,
                "analysts": list(self.analysts),
                "settings": dict(self.settings),
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "error": self.error,
                "signal": self.signal,
                "review": self.review,
                "agent_status": dict(self.agent_status),
                "sections": dict(self.sections),
                "messages": list(self.messages),
                "stats": dict(self.stats),
                "report_path": str(self.report_path) if self.report_path else None,
            }

    def snapshot(self) -> dict:
        """What the browser needs to draw the run and to subscribe from the right point."""
        data = self.record()
        with self.lock:
            data["last_seq"] = len(self.events)
        data["report_available"] = self.report_file() is not None
        return data

    def report_file(self) -> Path | None:
        path = Path(self.report_path) if self.report_path else None
        return path if path is not None and path.is_file() else None

    @classmethod
    def from_record(cls, data: dict) -> Run:
        run = cls(
            id=str(data["id"]),
            ticker=str(data["ticker"]),
            analysis_date=str(data["analysis_date"]),
            asset_type=str(data.get("asset_type") or "stock"),
            analysts=list(data.get("analysts") or []),
            settings=dict(data.get("settings") or {}),
            status=str(data.get("status") or "failed"),
            created_at=str(data.get("created_at") or _now()),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            error=data.get("error"),
            signal=data.get("signal"),
            review=bool(data.get("review")),
            agent_status=dict(data.get("agent_status") or {}),
            sections=dict(data.get("sections") or {}),
            stats=dict(data.get("stats") or {}),
            report_path=Path(data["report_path"]) if data.get("report_path") else None,
        )
        run.messages.extend(data.get("messages") or [])
        return run


class RunManager:
    """Owns every run: queues them onto worker threads and remembers finished ones."""

    def __init__(self, results_dir: str | Path, max_workers: int = 1):
        self.results_dir = Path(results_dir)
        self._runs: dict[str, Run] = {}
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)), thread_name_prefix="tradingdesk-run"
        )
        self._load_history()

    # --- registry -----------------------------------------------------------

    def _load_history(self) -> None:
        """Pick up runs written by earlier server processes."""
        for path in sorted(self.results_dir.glob(f"*/*/runs/*/{RUN_FILE}")):
            try:
                run = Run.from_record(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError, KeyError, TypeError):
                logger.warning("Skipping unreadable run record %s", path, exc_info=True)
                continue
            run.out_dir = path.parent
            if run.status not in TERMINAL_STATUSES:
                # It was in flight when its server stopped; nothing will finish it.
                run.status = "failed"
                run.error = "The server stopped while this run was in progress."
                run.finished_at = run.finished_at or _now()
            self._runs[run.id] = run

    def list(self) -> list[dict]:
        with self._lock:
            runs = list(self._runs.values())
        return [run.summary() for run in sorted(runs, key=lambda r: r.created_at, reverse=True)]

    def get(self, run_id: str) -> Run | None:
        with self._lock:
            return self._runs.get(run_id)

    def submit(self, run: Run) -> Run:
        """Register ``run`` and queue it; it starts as soon as a worker is free."""
        run.out_dir = (
            self.results_dir / safe_ticker_component(run.ticker) / run.analysis_date / "runs" / run.id
        )
        with self._lock:
            self._runs[run.id] = run
        self._pool.submit(self._execute, run)
        return run

    def cancel(self, run_id: str) -> bool:
        """Ask a run to stop after its current agent step; True if it was still going."""
        run = self.get(run_id)
        if run is None or run.status in TERMINAL_STATUSES:
            return False
        run.cancel_requested = True
        return True

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    # --- execution ----------------------------------------------------------

    def _execute(self, run: Run) -> None:
        if run.cancel_requested:
            self._finish(run, "cancelled")
            return
        run.set_status("running", started_at=_now())
        self._persist(run)
        try:
            self._run_graph(run)
        except RunCancelled:
            self._finish(run, "cancelled")
        except Exception as exc:  # noqa: BLE001 - anything the graph raises ends the run
            logger.exception("Run %s for %s failed", run.id, run.ticker)
            self._finish(run, "failed", error=f"{type(exc).__name__}: {exc}")
        else:
            self._finish(run, "completed")

    def _run_graph(self, run: Run) -> None:
        stats = StatsCallbackHandler()
        graph = TradingAgentsGraph(run.analysts, config=run.config, debug=False, callbacks=[stats])

        buffer = MessageBuffer()
        buffer.init_for_analysis(run.analysts)
        run.set_agents(buffer.agent_status)

        run.add_message("System", f"Selected ticker: {run.ticker}")
        if run.asset_type != "stock":
            run.add_message("System", f"Detected asset type: {run.asset_type}")
        run.add_message("System", f"Analysis date: {run.analysis_date}")
        run.add_message("System", f"Selected analysts: {', '.join(run.analysts)}")

        buffer.update_agent_status(MessageBuffer.ANALYST_MAPPING[run.analysts[0]], "in_progress")
        run.set_agents(buffer.agent_status)

        with run_config(run.config):
            init_state = graph.create_run_state(
                run.ticker, run.analysis_date, run.asset_type, run.portfolio
            )
            args = graph.propagator.get_graph_args(callbacks=[stats])
            thread_id = graph.begin_checkpoint(
                run.ticker, run.analysis_date, run.asset_type, run.portfolio
            )
            if thread_id is not None:
                args.setdefault("config", {}).setdefault("configurable", {})["thread_id"] = thread_id
                verb = "Resuming the saved run" if getattr(graph, "_resuming", False) else "Starting fresh"
                run.add_message("System", f"{verb} for {run.ticker} on {run.analysis_date}")

            trace = []
            try:
                for chunk in graph.graph.stream(graph.checkpoint_input(init_state), **args):
                    if run.cancel_requested:
                        raise RunCancelled()
                    self._absorb_chunk(run, buffer, chunk)
                    run.set_stats(stats.get_stats())
                    trace.append(chunk)
                final_state: dict = {}
                for chunk in trace:
                    final_state.update(chunk)
                graph.record_decision(run.ticker, run.analysis_date, final_state)
                graph.clear_checkpoint_on_success(
                    run.ticker, run.analysis_date, run.asset_type, run.portfolio
                )
            finally:
                graph.end_checkpoint()

        for agent in buffer.agent_status:
            buffer.update_agent_status(agent, "completed")
        run.set_agents(buffer.agent_status)
        for key, text in sections_from_state(final_state).items():
            run.set_section(key, text)

        run.signal = graph.process_signal(final_state.get("final_trade_decision", ""))
        run.review = is_review(run.signal)
        run.report_path = write_report_tree(final_state, run.ticker, run.out_dir / "reports")
        run.set_stats(stats.get_stats())
        run.add_message("System", f"Completed analysis for {run.analysis_date}")
        if run.review:
            run.add_message(
                "System",
                "No rating could be read from the final decision, so this run is recorded "
                "for review rather than as a position.",
            )

    def _absorb_chunk(self, run: Run, buffer: MessageBuffer, chunk: dict) -> None:
        """Feed one streamed state into the feed, the sections and the agent statuses."""
        for message in chunk.get("messages", []):
            msg_id = getattr(message, "id", None)
            if msg_id is not None:
                if msg_id in buffer._processed_message_ids:
                    continue
                buffer._processed_message_ids.add(msg_id)
            msg_type, content = classify_message_type(message)
            if content and content.strip():
                run.add_message(msg_type, content)
            for tool_call in getattr(message, "tool_calls", None) or []:
                if isinstance(tool_call, dict):
                    name, arguments = tool_call["name"], tool_call["args"]
                else:
                    name, arguments = tool_call.name, tool_call.args
                run.add_message("Tool", f"{name}: {arguments}")

        for key, text in sections_from_state(chunk).items():
            run.set_section(key, text)

        # The status transitions cli.run.run_analysis applies, in the same order.
        update_analyst_statuses(buffer, chunk)
        debate = chunk.get("investment_debate_state") or {}
        if _has(debate, "bull_history") or _has(debate, "bear_history"):
            for agent in RESEARCH_TEAM:
                buffer.update_agent_status(agent, "in_progress")
        if _has(debate, "judge_decision"):
            for agent in RESEARCH_TEAM:
                buffer.update_agent_status(agent, "completed")
            buffer.update_agent_status("Trader", "in_progress")
        if chunk.get("trader_investment_plan") and buffer.agent_status.get("Trader") != "completed":
            buffer.update_agent_status("Trader", "completed")
            buffer.update_agent_status("Aggressive Analyst", "in_progress")
        risk = chunk.get("risk_debate_state") or {}
        for key, agent in RISK_HISTORIES:
            if _has(risk, key) and buffer.agent_status.get(agent) != "completed":
                buffer.update_agent_status(agent, "in_progress")
        if _has(risk, "judge_decision"):
            for _, agent in RISK_HISTORIES:
                buffer.update_agent_status(agent, "completed")
            buffer.update_agent_status("Portfolio Manager", "completed")
        run.set_agents(buffer.agent_status)

    def _finish(self, run: Run, status: str, error: str | None = None) -> None:
        if status != "completed":
            agents = {
                agent: ("error" if status == "failed" and state == "in_progress" else state)
                for agent, state in run.agent_status.items()
            }
            run.set_agents(agents)
        if error:
            run.add_message("System", f"Run failed: {error}")
        elif status == "cancelled":
            run.add_message("System", "Run stopped at the user's request")
        run.set_status(
            status, finished_at=_now(), error=error, signal=run.signal, review=run.review
        )
        self._persist(run)

    def _persist(self, run: Run) -> None:
        if run.out_dir is None:
            return
        try:
            run.out_dir.mkdir(parents=True, exist_ok=True)
            (run.out_dir / RUN_FILE).write_text(
                json.dumps(run.record(), indent=2, ensure_ascii=False), encoding="utf-8"
            )
        except OSError:
            logger.warning("Could not write %s", run.out_dir / RUN_FILE, exc_info=True)


def _has(state: dict, key: str) -> bool:
    return bool(str(state.get(key) or "").strip())
