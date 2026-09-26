"""What the web app can offer for a run, derived from the CLI's own tables.

The form in the browser and the interactive CLI must agree on the analysts,
research depths, providers, models and languages a run can be configured with,
so everything here is read from ``cli`` and ``tradingagents`` rather than
restated. The few tables the CLI keeps inside its prompt functions (regional
endpoints, reasoning knobs) are repeated here with a pointer to their source.
"""

from __future__ import annotations

import os

from cli.display import ANALYST_ORDER, MessageBuffer
from cli.prompts import TICKER_INPUT_EXAMPLES, _llm_provider_table, provider_default_url
from tradingagents.dataflows.date_window import get_current_date
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.llm_clients.api_key_env import get_api_key_env
from tradingagents.llm_clients.model_catalog import MODEL_OPTIONS, get_model_options
from tradingagents.llm_clients.openai_client import OPENAI_COMPATIBLE_PROVIDERS

ANALYSTS = [{"key": key, "label": MessageBuffer.ANALYST_MAPPING[key]} for key in ANALYST_ORDER]

# cli.prompts.select_research_depth: one number sets both debate and risk rounds.
RESEARCH_DEPTHS = [
    {"value": 1, "label": "Shallow",
     "description": "Quick research, few debate and strategy discussion rounds"},
    {"value": 3, "label": "Medium",
     "description": "Middle ground, moderate debate rounds and strategy discussion"},
    {"value": 5, "label": "Deep",
     "description": "Comprehensive research, in depth debate and strategy discussion"},
]

# cli.prompts.ask_output_language; any other language is accepted as free text.
LANGUAGES = [
    "English", "Chinese", "Japanese", "Korean", "Hindi", "Spanish",
    "Portuguese", "French", "German", "Arabic", "Russian",
]

# Regional endpoints the CLI asks about in a second prompt
# (cli.prompts.ask_qwen_region / ask_glm_region / ask_minimax_region):
# (label, provider key, backend URL, the provider it is a region of).
REGION_PROVIDERS = [
    ("Qwen (China, DashScope)", "qwen-cn",
     "https://dashscope.aliyuncs.com/compatible-mode/v1", "qwen"),
    ("GLM (China, BigModel)", "glm-cn", "https://open.bigmodel.cn/api/paas/v4/", "glm"),
    ("MiniMax (China)", "minimax-cn", "https://api.minimaxi.com/v1", "minimax"),
]

# Provider-specific reasoning knobs: the config key each sets and the choices
# the CLI offers (cli.prompts.ask_openai_reasoning_effort and friends).
THINKING_OPTIONS = {
    "openai": {
        "config_key": "openai_reasoning_effort",
        "label": "Reasoning effort",
        "choices": [
            {"value": "medium", "label": "Medium (default)"},
            {"value": "high", "label": "High (more thorough)"},
            {"value": "low", "label": "Low (faster)"},
        ],
    },
    "google": {
        "config_key": "google_thinking_level",
        "label": "Thinking mode",
        "choices": [
            {"value": "high", "label": "Enable thinking (recommended)"},
            {"value": "minimal", "label": "Minimal / disable thinking"},
        ],
    },
    "anthropic": {
        "config_key": "anthropic_effort",
        "label": "Effort level",
        "choices": [
            {"value": "high", "label": "High (recommended)"},
            {"value": "medium", "label": "Medium (balanced)"},
            {"value": "low", "label": "Low (faster, cheaper)"},
        ],
    },
}

# The progress table, team by team, as the CLI's live view lays it out.
TEAMS = [
    {"name": "Analyst Team", "agents": [MessageBuffer.ANALYST_MAPPING[k] for k in ANALYST_ORDER]},
    *({"name": name, "agents": list(agents)} for name, agents in MessageBuffer.FIXED_AGENTS.items()),
]

# The report sections a run produces, in reading order, matching the report
# tree ``tradingagents.reporting.write_report_tree`` writes.
SECTIONS = [
    {"key": "market_report", "title": "Market Analyst", "group": "I. Analyst Team"},
    {"key": "sentiment_report", "title": "Sentiment Analyst", "group": "I. Analyst Team"},
    {"key": "news_report", "title": "News Analyst", "group": "I. Analyst Team"},
    {"key": "fundamentals_report", "title": "Fundamentals Analyst", "group": "I. Analyst Team"},
    {"key": "bull_history", "title": "Bull Researcher", "group": "II. Research Team"},
    {"key": "bear_history", "title": "Bear Researcher", "group": "II. Research Team"},
    {"key": "research_manager", "title": "Research Manager", "group": "II. Research Team"},
    {"key": "trader_investment_plan", "title": "Trader", "group": "III. Trading Team"},
    {"key": "aggressive_history", "title": "Aggressive Analyst", "group": "IV. Risk Management"},
    {"key": "conservative_history", "title": "Conservative Analyst", "group": "IV. Risk Management"},
    {"key": "neutral_history", "title": "Neutral Analyst", "group": "IV. Risk Management"},
    {"key": "final_trade_decision", "title": "Portfolio Manager", "group": "V. Portfolio Management"},
]
SECTION_KEYS = [section["key"] for section in SECTIONS]


def model_choices(provider: str) -> dict[str, list[dict]] | None:
    """The catalog's models for ``provider`` per mode, or None when the model is free text.

    OpenRouter's list is fetched live by the CLI and Azure names a deployment,
    so neither has a static list to offer.
    """
    if provider not in MODEL_OPTIONS:
        return None
    return {
        mode: [
            {"value": value, "label": label}
            for label, value in get_model_options(provider, mode)
            if value != "custom"
        ]
        for mode in ("quick", "deep")
    }


def missing_api_key(provider: str) -> str | None:
    """The env var a run with ``provider`` needs but the server does not have, else None."""
    env_var = get_api_key_env(provider)
    if env_var is None:
        return None
    spec = OPENAI_COMPATIBLE_PROVIDERS.get(provider)
    if spec is not None and spec.key_optional:
        return None
    return None if os.environ.get(env_var) else env_var


def default_backend_url(provider: str) -> str | None:
    """The endpoint the CLI menu would pick for ``provider``, regional keys included."""
    for _, key, url, _ in REGION_PROVIDERS:
        if key == provider:
            return url
    return provider_default_url(provider)


def _provider_row(label: str, key: str, url: str | None, region_of: str | None = None) -> dict:
    env_var = get_api_key_env(key)
    spec = OPENAI_COMPATIBLE_PROVIDERS.get(key)
    key_optional = spec is not None and spec.key_optional
    return {
        "key": key,
        "label": label,
        "region_of": region_of,
        "default_url": url,
        "needs_backend_url": key == "openai_compatible",
        "api_key_env": env_var,
        "key_required": env_var is not None and not key_optional,
        "key_configured": bool(os.environ.get(env_var)) if env_var else None,
        "models": model_choices(key),
        "thinking": THINKING_OPTIONS.get(key),
    }


def providers() -> list[dict]:
    """Every provider the CLI menu offers, plus the regional variants of three of them."""
    rows = [_provider_row(label, key, url) for label, key, url in _llm_provider_table()]
    rows += [_provider_row(label, key, url, base) for label, key, url, base in REGION_PROVIDERS]
    return rows


def known_providers() -> set[str]:
    return {row["key"] for row in providers()}


def options() -> dict:
    """Everything the run form needs, with the server's own defaults filled in."""
    return {
        "analysts": ANALYSTS,
        "research_depths": RESEARCH_DEPTHS,
        "languages": LANGUAGES,
        "providers": providers(),
        "teams": TEAMS,
        "sections": SECTIONS,
        "ticker_examples": TICKER_INPUT_EXAMPLES,
        "defaults": {
            "analysis_date": get_current_date(),
            "llm_provider": DEFAULT_CONFIG["llm_provider"],
            "backend_url": DEFAULT_CONFIG["backend_url"],
            "quick_think_llm": DEFAULT_CONFIG["quick_think_llm"],
            "deep_think_llm": DEFAULT_CONFIG["deep_think_llm"],
            "research_depth": DEFAULT_CONFIG["max_debate_rounds"],
            "output_language": DEFAULT_CONFIG["output_language"],
            "openai_reasoning_effort": DEFAULT_CONFIG["openai_reasoning_effort"],
            "google_thinking_level": DEFAULT_CONFIG["google_thinking_level"],
            "anthropic_effort": DEFAULT_CONFIG["anthropic_effort"],
            "checkpoint_enabled": DEFAULT_CONFIG["checkpoint_enabled"],
        },
    }
