"""Deterministic contract tests for the maintained live qualification planner."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

from xret.data.models import MarketIdentity

_SCRIPT = Path(__file__).parents[3] / "tools" / "ccxt_qualification_gate.py"
_SPEC = importlib.util.spec_from_file_location("xret_ccxt_qualification_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


@dataclass(frozen=True)
class _Definition:
    identity: MarketIdentity
    active: bool
    timeframes: frozenset[str]


def _definitions() -> dict[str, tuple[_Definition, ...]]:
    return {
        "spot": (
            _Definition(
                MarketIdentity(exchange="bybit", symbol="BTC/USDT", market="spot"),
                True,
                frozenset({"1m", "1h", "1d", "1M"}),
            ),
            _Definition(
                MarketIdentity(exchange="bybit", symbol="ETH/USDT", market="spot"),
                True,
                frozenset({"1m", "1h", "1d", "1M"}),
            ),
        ),
        "perpetual": (
            _Definition(
                MarketIdentity(
                    exchange="bybit",
                    symbol="BTC/USDT",
                    market="perpetual",
                    settle="USDT",
                ),
                True,
                frozenset({"1m", "1h", "1d", "1M"}),
            ),
        ),
    }


def test_999_confidence_requires_135_zero_failure_samples() -> None:
    assert gate.required_zero_failure_samples(0.05, 0.999) == 135


def test_mandatory_risk_cases_do_not_count_toward_statistical_sample() -> None:
    cases = gate.plan_cases(
        _definitions(),
        symbol_limit=2,
        target_samples=135,
        preferred=gate.PREFERRED_SYMBOLS,
        seed=20260823,
        page_bars=300,
    )

    statistical = [case for case in cases if case.kind == "statistical"]
    mandatory = [case for case in cases if case.kind == "mandatory"]

    assert len(statistical) == 135
    assert mandatory
    assert {
        "one-bar",
        "finality-boundary",
        "pagination-n-minus-1",
        "pagination-n",
        "pagination-n-plus-1",
        "pagination-two-n-plus-1",
        "month-partition-boundary",
        "year-boundary",
        "incremental-left-right",
        "concurrent-same-dataset",
        "reject-zero-length",
        "reject-misaligned",
        "long-history-1y",
        "long-history-3y",
        "advertised-timeframe",
    } <= {case.scenario for case in mandatory}


def test_statistical_plan_is_reproducible_and_covers_time_strata() -> None:
    arguments = {
        "symbol_limit": 2,
        "target_samples": 135,
        "preferred": gate.PREFERRED_SYMBOLS,
        "seed": 20260823,
        "page_bars": 300,
    }

    first = gate.plan_cases(_definitions(), **arguments)
    second = gate.plan_cases(_definitions(), **arguments)

    assert first == second
    statistical = [case for case in first if case.kind == "statistical"]
    assert {case.end_offset_days for case in statistical} == set(gate._STATISTICAL_OFFSETS_DAYS)
    assert {case.bars for case in statistical} == set(gate._STATISTICAL_WINDOW_BARS)
