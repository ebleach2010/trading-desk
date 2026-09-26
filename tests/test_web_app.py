"""The Trading Desk web app: options, run lifecycle, streaming, auth and history.

The graph is replaced by a scripted stand-in so the suite needs no API key and
no network. What is exercised is everything around it: request validation, the
status and section bookkeeping, event replay, persistence and the token guard.
"""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage

import tradingdesk.runs as runs_module
from tradingagents.agents.rating import parse_rating
from tradingdesk import catalog
from tradingdesk.runs import RunManager, sections_from_state
from tradingdesk.server import Settings, create_app, main

TERMINAL = ("completed", "failed", "cancelled")


def _state(**overrides):
    base = {
        "messages": [],
        "market_report": "",
        "sentiment_report": "",
        "news_report": "",
        "fundamentals_report": "",
        "investment_debate_state": {"bull_history": "", "bear_history": "", "judge_decision": ""},
        "risk_debate_state": {
            "aggressive_history": "", "conservative_history": "", "neutral_history": "",
            "judge_decision": "",
        },
    }
    base.update(overrides)
    return base


def _scripted_chunks() -> list[dict]:
    """Full-state chunks like ``stream_mode="values"`` yields, one agent step at a time."""
    tool_call = AIMessage(
        content="", id="m2",
        tool_calls=[{"name": "get_stock_data", "args": {"symbol": "AAPL"}, "id": "t1"}],
    )
    chunks = []
    state = _state(
        market_report="# Market\nUptrend intact.",
        messages=[AIMessage(content="Market done", id="m1"), tool_call],
    )
    chunks.append(state)
    state = {**state, "news_report": "Nothing alarming in the news.", "messages": []}
    chunks.append(state)
    state = {**state, "investment_debate_state": {
        "bull_history": "Bull: momentum is strong", "bear_history": "Bear: valuation is rich",
        "judge_decision": "",
    }}
    chunks.append(state)
    state = {
        **state,
        "investment_debate_state": {
            **state["investment_debate_state"], "judge_decision": "Rating: Overweight. Lean long.",
        },
        "investment_plan": "Rating: Overweight. Lean long.",
    }
    chunks.append(state)
    state = {**state, "trader_investment_plan": "Buy 10 units at the open."}
    chunks.append(state)
    state = {**state, "risk_debate_state": {
        "aggressive_history": "Go bigger", "conservative_history": "Size down",
        "neutral_history": "Split the difference", "judge_decision": "",
    }}
    chunks.append(state)
    state = {
        **state,
        "risk_debate_state": {**state["risk_debate_state"], "judge_decision": "Final Rating: Buy"},
        "final_trade_decision": "Final Rating: Buy",
    }
    chunks.append(state)
    return chunks


class FakeGraph:
    """Stands in for TradingAgentsGraph: streams scripted chunks, records the bookkeeping calls."""

    chunks: list[dict] = []
    gate: threading.Event | None = None  # when set, the stream waits on it before its second chunk
    fail_with: Exception | None = None  # when set, the stream raises it instead of its second chunk
    instances: list[FakeGraph] = []

    def __init__(self, selected_analysts, config=None, debug=False, callbacks=None):
        self.selected_analysts = tuple(selected_analysts)
        self.config = config
        self.callbacks = callbacks
        self._resuming = False
        self.run_state_args = None
        self.recorded = []
        self.checkpoints_cleared = 0
        self.ended = False
        self.propagator = SimpleNamespace(
            get_graph_args=lambda callbacks=None: {"stream_mode": "values", "config": {}}
        )
        self.graph = SimpleNamespace(stream=self._stream)
        type(self).instances.append(self)

    def create_run_state(self, ticker, date, asset_type="stock", portfolio=None):
        self.run_state_args = (ticker, date, asset_type, portfolio)
        return {"company_of_interest": ticker, "trade_date": date}

    def begin_checkpoint(self, *args):
        return None

    def checkpoint_input(self, init_state):
        return init_state

    def end_checkpoint(self):
        self.ended = True

    def record_decision(self, ticker, date, final_state):
        self.recorded.append((ticker, date, final_state.get("final_trade_decision")))

    def clear_checkpoint_on_success(self, *args):
        self.checkpoints_cleared += 1

    def process_signal(self, text):
        return parse_rating(text)

    def _stream(self, init_state, **args):
        for index, chunk in enumerate(type(self).chunks):
            if index == 1:
                if type(self).fail_with is not None:
                    raise type(self).fail_with
                if type(self).gate is not None:
                    type(self).gate.wait(timeout=10)
            yield chunk


@pytest.fixture
def fake_graph(monkeypatch):
    monkeypatch.setattr(runs_module, "TradingAgentsGraph", FakeGraph)
    FakeGraph.chunks = _scripted_chunks()
    FakeGraph.gate = None
    FakeGraph.fail_with = None
    FakeGraph.instances = []
    return FakeGraph


@pytest.fixture
def client(tmp_path, fake_graph):
    app = create_app(Settings(results_dir=str(tmp_path)))
    with TestClient(app) as test_client:
        yield test_client


def _wait_terminal(client, run_id, timeout=10.0):
    deadline = time.monotonic() + timeout
    while True:
        body = client.get(f"/api/runs/{run_id}").json()
        if body["status"] in TERMINAL:
            return body
        assert time.monotonic() < deadline, f"run {run_id} did not finish: {body['status']}"
        time.sleep(0.02)


def _parse_sse(text: str) -> list[tuple[str, dict]]:
    events = []
    for block in text.split("\n\n"):
        kind, data = None, ""
        for line in block.splitlines():
            if line.startswith("event:"):
                kind = line[6:].strip()
            elif line.startswith("data:"):
                data += line[5:].strip()
        if kind and data:
            events.append((kind, json.loads(data)))
    return events


@pytest.mark.unit
def test_options_come_from_the_cli_tables(client):
    body = client.get("/api/options").json()
    assert [a["key"] for a in body["analysts"]] == ["market", "social", "news", "fundamentals"]
    assert [d["value"] for d in body["research_depths"]] == [1, 3, 5]
    providers = {p["key"]: p for p in body["providers"]}
    assert providers["openai"]["api_key_env"] == "OPENAI_API_KEY"
    assert providers["openai"]["key_required"] is True
    assert providers["openai"]["key_configured"] is True  # conftest's placeholder key
    assert "gpt-6-luna" in [m["value"] for m in providers["openai"]["models"]["quick"]]
    assert "custom" not in [m["value"] for m in providers["openai"]["models"]["deep"]]
    assert providers["ollama"]["key_required"] is False
    assert providers["openai_compatible"]["needs_backend_url"] is True
    assert providers["openrouter"]["models"] is None
    assert providers["qwen-cn"]["region_of"] == "qwen"
    assert providers["anthropic"]["thinking"]["config_key"] == "anthropic_effort"
    assert body["defaults"]["llm_provider"] == "openai"
    assert body["defaults"]["research_depth"] == 1
    assert body["teams"][0]["name"] == "Analyst Team"
    assert [s["key"] for s in body["sections"]] == catalog.SECTION_KEYS


@pytest.mark.unit
def test_run_streams_sections_and_persists(client, tmp_path, fake_graph):
    started = client.post(
        "/api/runs", json={"ticker": "aapl", "analysts": ["news", "market"], "research_depth": 3}
    )
    assert started.status_code == 202, started.text
    run = started.json()
    assert run["ticker"] == "AAPL"
    assert run["analysts"] == ["market", "news"]  # canonical order, whatever the form sent
    assert run["status"] in ("queued", "running")
    assert run["settings"]["llm_provider"] == "openai"
    assert run["settings"]["backend_url"] == "https://api.openai.com/v1"

    done = _wait_terminal(client, run["id"])
    assert done["status"] == "completed", done
    assert done["signal"] == "Buy"
    assert done["review"] is False
    assert set(done["sections"]) == {
        "market_report", "news_report", "bull_history", "bear_history", "research_manager",
        "trader_investment_plan", "aggressive_history", "conservative_history",
        "neutral_history", "final_trade_decision",
    }
    assert done["sections"]["research_manager"] == "Rating: Overweight. Lean long."
    assert set(done["agent_status"].values()) == {"completed"}
    assert "Fundamentals Analyst" not in done["agent_status"]
    messages = [(m["kind"], m["content"]) for m in done["messages"]]
    assert ("Agent", "Market done") in messages
    assert any(kind == "Tool" and text.startswith("get_stock_data:") for kind, text in messages)
    assert done["stats"] == {"llm_calls": 0, "tool_calls": 0, "tokens_in": 0, "tokens_out": 0}
    assert done["report_available"] is True

    graph = fake_graph.instances[0]
    assert graph.selected_analysts == ("market", "news")
    assert graph.config["llm_provider"] == "openai"
    assert graph.config["max_debate_rounds"] == 3
    assert graph.config["max_risk_discuss_rounds"] == 3
    assert graph.config["results_dir"] == str(tmp_path)
    assert graph.run_state_args == ("AAPL", done["analysis_date"], "stock", None)
    assert graph.recorded == [("AAPL", done["analysis_date"], "Final Rating: Buy")]
    assert graph.ended is True
    assert graph.checkpoints_cleared == 1

    # The event log replays in order and ends with the terminal status.
    with client.stream(
        "GET", f"/api/runs/{run['id']}/events", headers={"Last-Event-ID": "0"}
    ) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        events = _parse_sse("".join(response.iter_text()))
    assert events[0][0] == "status" and events[0][1]["status"] == "running"
    assert events[-1][0] == "status" and events[-1][1]["status"] == "completed"
    assert {"agents", "section", "message", "stats"} <= {kind for kind, _ in events}
    section_keys = [data["key"] for kind, data in events if kind == "section"]
    assert section_keys.index("market_report") < section_keys.index("research_manager")
    assert section_keys.index("research_manager") < section_keys.index("final_trade_decision")
    assert len(events) == done["last_seq"]

    # A client that already saw everything but the last event gets only that one.
    with client.stream(
        "GET", f"/api/runs/{run['id']}/events",
        headers={"Last-Event-ID": str(done["last_seq"] - 1)},
    ) as response:
        assert [kind for kind, _ in _parse_sse("".join(response.iter_text()))] == ["status"]

    report = client.get(f"/api/runs/{run['id']}/report")
    assert report.status_code == 200
    assert "Trading Analysis Report: AAPL" in report.text
    assert "Final Rating: Buy" in report.text

    run_dir = tmp_path / "AAPL" / done["analysis_date"] / "runs" / run["id"]
    record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert record["status"] == "completed" and record["signal"] == "Buy"
    assert (run_dir / "reports" / "complete_report.md").is_file()
    assert "Market done" in (run_dir / "messages.log").read_text(encoding="utf-8")

    # A later server picks the finished run up from disk.
    reloaded = RunManager(tmp_path)
    try:
        assert [r["id"] for r in reloaded.list()] == [run["id"]]
        snapshot = reloaded.get(run["id"]).snapshot()
        assert snapshot["sections"]["final_trade_decision"] == "Final Rating: Buy"
        assert snapshot["report_available"] is True
    finally:
        reloaded.shutdown()


@pytest.mark.unit
def test_crypto_ticker_is_normalized_and_drops_fundamentals(client):
    started = client.post("/api/runs", json={"ticker": "btcusd"})
    assert started.status_code == 202, started.text
    run = started.json()
    assert run["ticker"] == "BTC-USD"
    assert run["asset_type"] == "crypto"
    assert run["analysts"] == ["market", "social", "news"]
    _wait_terminal(client, run["id"])

    only_fundamentals = client.post("/api/runs", json={"ticker": "ETH-USD", "analysts": ["fundamentals"]})
    assert only_fundamentals.status_code == 422
    assert "fundamentals" in only_fundamentals.json()["detail"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("payload", "needle"),
    [
        ({"ticker": "AA PL"}, "ticker"),
        ({"ticker": ".."}, "ticker"),
        ({"ticker": "AAPL", "analysis_date": "2999-01-01"}, "future"),
        ({"ticker": "AAPL", "analysis_date": "01/02/2026"}, "YYYY-MM-DD"),
        ({"ticker": "AAPL", "analysts": []}, "at least one"),
        ({"ticker": "AAPL", "analysts": ["quant"]}, "unknown analysts"),
        ({"ticker": "AAPL", "llm_provider": "hal9000"}, "unknown provider"),
        ({"ticker": "AAPL", "research_depth": 9}, "research_depth"),
        ({"ticker": "AAPL", "openai_reasoning_effort": "extreme"}, "one of"),
        ({"ticker": "AAPL", "backend_url": "localhost:8000"}, "http"),
        ({"ticker": "AAPL", "quick_think_llm": "  "}, "empty"),
        ({"ticker": "AAPL", "portfolio": {"positions": [{"ticker": "AAPL"}]}}, "quantity"),
    ],
)
def test_bad_requests_are_rejected(client, fake_graph, payload, needle):
    response = client.post("/api/runs", json=payload)
    assert response.status_code == 422, response.text
    assert needle in json.dumps(response.json()["detail"])
    assert fake_graph.instances == []


@pytest.mark.unit
def test_openai_compatible_needs_a_backend_url(client):
    refused = client.post("/api/runs", json={"ticker": "AAPL", "llm_provider": "openai_compatible"})
    assert refused.status_code == 422
    assert "backend URL" in refused.json()["detail"]

    accepted = client.post("/api/runs", json={
        "ticker": "AAPL", "llm_provider": "openai_compatible",
        "backend_url": "http://localhost:1234/v1",
        "quick_think_llm": "local-small", "deep_think_llm": "local-large",
    })
    assert accepted.status_code == 202, accepted.text
    assert accepted.json()["settings"]["backend_url"] == "http://localhost:1234/v1"
    _wait_terminal(client, accepted.json()["id"])


@pytest.mark.unit
def test_missing_provider_key_is_refused_before_the_run(client, monkeypatch, fake_graph):
    monkeypatch.setenv("XAI_API_KEY", "")
    providers = {p["key"]: p for p in client.get("/api/options").json()["providers"]}
    assert providers["xai"]["key_configured"] is False

    refused = client.post("/api/runs", json={"ticker": "AAPL", "llm_provider": "xai"})
    assert refused.status_code == 400
    assert "XAI_API_KEY" in refused.json()["detail"]
    assert fake_graph.instances == []

    monkeypatch.setenv("XAI_API_KEY", "placeholder")
    accepted = client.post("/api/runs", json={"ticker": "AAPL", "llm_provider": "xai"})
    assert accepted.status_code == 202, accepted.text
    _wait_terminal(client, accepted.json()["id"])


@pytest.mark.unit
def test_api_token_guards_the_api(tmp_path, fake_graph):
    app = create_app(Settings(results_dir=str(tmp_path), api_token="s3cret"))
    with TestClient(app) as client:
        assert client.get("/api/health").json()["auth_required"] is True
        assert client.get("/").status_code == 200
        assert client.get("/api/runs").status_code == 401
        assert client.get("/api/runs", headers={"Authorization": "Bearer nope"}).status_code == 401
        assert client.post("/api/runs", json={"ticker": "AAPL"}).status_code == 401
        assert client.get("/api/runs", headers={"Authorization": "Bearer s3cret"}).json() == []
    assert fake_graph.instances == []


@pytest.mark.unit
def test_stop_ends_the_run_after_the_current_step(client, tmp_path, fake_graph):
    fake_graph.gate = threading.Event()
    run = client.post("/api/runs", json={"ticker": "MSFT"}).json()
    deadline = time.monotonic() + 10
    while "market_report" not in client.get(f"/api/runs/{run['id']}").json()["sections"]:
        assert time.monotonic() < deadline, "the first chunk never arrived"
        time.sleep(0.02)

    assert client.post(f"/api/runs/{run['id']}/cancel").status_code == 200
    fake_graph.gate.set()
    done = _wait_terminal(client, run["id"])
    assert done["status"] == "cancelled"
    assert done["signal"] is None
    assert done["report_available"] is False
    assert done["sections"] == {"market_report": "# Market\nUptrend intact."}
    assert fake_graph.instances[0].ended is True
    assert fake_graph.instances[0].recorded == []
    assert client.post(f"/api/runs/{run['id']}/cancel").status_code == 409
    record_path = tmp_path / "MSFT" / done["analysis_date"] / "runs" / run["id"] / "run.json"
    assert json.loads(record_path.read_text(encoding="utf-8"))["status"] == "cancelled"


@pytest.mark.unit
def test_a_failing_graph_marks_the_run_failed(client, fake_graph):
    fake_graph.fail_with = RuntimeError("provider exploded")
    run = client.post("/api/runs", json={"ticker": "NVDA", "analysts": ["market"]}).json()
    done = _wait_terminal(client, run["id"])
    assert done["status"] == "failed"
    assert "provider exploded" in done["error"]
    assert done["agent_status"]["Market Analyst"] == "completed"  # its report landed first
    assert done["agent_status"]["Bull Researcher"] == "error"  # in progress when it died
    assert done["agent_status"]["Trader"] == "pending"
    assert fake_graph.instances[0].ended is True
    assert any("provider exploded" in m["content"] for m in done["messages"])


@pytest.mark.unit
def test_history_marks_interrupted_runs_failed_and_skips_junk(tmp_path):
    run_dir = tmp_path / "AAPL" / "2026-09-01" / "runs" / "abc123"
    run_dir.mkdir(parents=True)
    (run_dir / "run.json").write_text(json.dumps({
        "id": "abc123", "ticker": "AAPL", "analysis_date": "2026-09-01",
        "status": "running", "created_at": "2026-09-01T10:00:00+00:00",
    }), encoding="utf-8")
    junk_dir = tmp_path / "AAPL" / "2026-09-01" / "runs" / "broken"
    junk_dir.mkdir(parents=True)
    (junk_dir / "run.json").write_text("{not json", encoding="utf-8")

    manager = RunManager(tmp_path)
    try:
        runs = manager.list()
        assert [r["id"] for r in runs] == ["abc123"]
        assert runs[0]["status"] == "failed"
        assert "server stopped" in manager.get("abc123").snapshot()["error"]
        assert manager.cancel("abc123") is False
    finally:
        manager.shutdown()


@pytest.mark.unit
def test_sections_from_state_matches_the_catalog():
    state = _scripted_chunks()[-1]
    sections = sections_from_state(state)
    assert set(sections) <= set(catalog.SECTION_KEYS)
    assert sections["final_trade_decision"] == "Final Rating: Buy"
    assert sections["research_manager"] == "Rating: Overweight. Lean long."
    # Without the graph's own final decision the risk judge's stands in, as in the report tree.
    without = {**state, "final_trade_decision": ""}
    assert sections_from_state(without)["final_trade_decision"] == "Final Rating: Buy"
    assert sections_from_state(_state()) == {}


@pytest.mark.unit
def test_ui_and_docs_are_served(client):
    page = client.get("/")
    assert page.status_code == 200
    assert "Trading Desk" in page.text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/styles.css").status_code == 200
    assert client.get("/api/openapi.json").status_code == 200
    assert client.get("/api/runs/nope").status_code == 404
    assert client.get("/api/health").json()["status"] == "ok"


@pytest.mark.unit
def test_cli_help_exits_cleanly(capsys):
    with pytest.raises(SystemExit) as exited:
        main(["--help"])
    assert exited.value.code == 0
    assert "--port" in capsys.readouterr().out
