"""Deterministic contract tests for the maintained live qualification planner."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest
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
    derivative: object | None = None


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
            _Definition(
                MarketIdentity(
                    exchange="bybit",
                    symbol="ETH/USDT",
                    market="perpetual",
                    settle="USDT",
                ),
                True,
                frozenset({"1m", "1h", "1d", "1M"}),
            ),
        ),
    }


def _page_bars_by_scope(
    definitions: dict[str, tuple[_Definition, ...]],
    page_bars: int = 300,
) -> dict[gate.PlanScope, int]:
    return {
        gate.PlanScope.from_definition(definition): page_bars
        for available in definitions.values()
        for definition in available
    }


def test_999_confidence_requires_135_zero_failure_samples() -> None:
    assert gate.required_zero_failure_samples(0.05, 0.999) == 135


def test_gate_creates_output_and_state_roots(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "results"
    state = tmp_path / "other" / "state"

    gate._prepare_directories(output, state)

    assert output.is_dir()
    assert state.is_dir()


def test_default_candidate_universe_matches_the_approved_priority_list() -> None:
    assert len(gate.VENUES) == 36
    assert set(gate.VENUES) == {
        "apex",
        "aster",
        "binance",
        "bingx",
        "bitfinex",
        "bitget",
        "bithumb",
        "bitrue",
        "bitso",
        "bitstamp",
        "bitvavo",
        "btcturk",
        "bybit",
        "coinbase",
        "cryptocom",
        "derive",
        "deribit",
        "dydx",
        "extended",
        "gate",
        "hashkey",
        "htx",
        "hyperliquid",
        "kraken",
        "kucoin",
        "lbank",
        "lighter",
        "mexc",
        "okx",
        "pacifica",
        "paradex",
        "phemex",
        "toobit",
        "upbit",
        "woo",
        "xt",
    }


def test_semantic_probe_uses_only_complete_middle_trade_minutes() -> None:
    trades = [
        {"timestamp": 59_999, "price": 9.0, "amount": 1.0},
        {"timestamp": 60_000, "price": 10.0, "amount": 2.0},
        {"timestamp": 90_000, "price": 12.0, "amount": 3.0},
        {"timestamp": 120_000, "price": 11.0, "amount": 4.0},
    ]

    assert gate._complete_trade_buckets(trades, step_ms=60_000) == [(60_000, trades[1:3])]


def test_settlement_filter_applies_only_to_perpetual_definitions() -> None:
    definitions = _definitions()

    assert gate.select_settlements(definitions["spot"], "spot", ["USDC"]) == definitions["spot"]
    assert gate.select_settlements(definitions["perpetual"], "perpetual", ["USDC"]) == ()
    assert (
        gate.select_settlements(definitions["perpetual"], "perpetual", ["USDT"])
        == definitions["perpetual"]
    )


def test_expected_semantic_scopes_keep_derivative_settlements_separate() -> None:
    definitions = _definitions()
    btc_inverse = _Definition(
        MarketIdentity(
            exchange="kraken",
            symbol="BTC/USD",
            market="perpetual",
            settle="BTC",
        ),
        True,
        frozenset({"1m"}),
    )
    definitions["perpetual"] += (btc_inverse,)

    assert gate._expected_scope_keys(definitions) == {
        "spot",
        "perpetual/USDT",
        "perpetual/BTC",
    }


def test_requested_market_discovery_error_prevents_a_full_gate_pass() -> None:
    arguments = {
        "cases": [{"status": "pass"}],
        "expected_scopes": {"perpetual/USDT"},
        "witnessed_scopes": {"perpetual/USDT"},
        "semantic_scopes": {"perpetual/USDT"},
    }

    assert gate._full_gate_status(market_errors={}, **arguments) == "pass"
    assert (
        gate._full_gate_status(
            market_errors={"spot": "ProviderError: discovery failed"},
            **arguments,
        )
        == "candidate"
    )


def test_semantic_trade_aggregate_converts_contracts_to_base_volume() -> None:
    trades = [
        {"timestamp": 1, "id": "a", "price": 100.0, "amount": 2.0},
        {"timestamp": 2, "id": "b", "price": 110.0, "amount": 3.0},
    ]

    linear = gate._aggregate_trades(
        trades,
        {"contract": True, "contractSize": 0.01, "inverse": False},
    )
    inverse = gate._aggregate_trades(
        trades,
        {"contract": True, "contractSize": 10.0, "inverse": True},
    )

    assert linear == [100.0, 110.0, 100.0, 110.0, 0.05]
    assert inverse[:4] == [100.0, 110.0, 100.0, 110.0]
    assert inverse[4] == 2.0 * 10.0 / 100.0 + 3.0 * 10.0 / 110.0

    already_base = gate._aggregate_trades(
        [{"timestamp": 1, "price": 100.0, "amount": 0.2, "cost": 20.0}],
        {"contract": True, "contractSize": 0.001, "inverse": False},
    )
    assert already_base[4] == 0.2

    adapter_converted = gate._aggregate_trades(
        [
            {
                "timestamp": 1,
                "price": 100.0,
                "amount": 0.2,
                "cost": 0.2,
                "info": {"a": "20"},
            }
        ],
        {"contract": True, "contractSize": 0.01, "inverse": False},
    )
    assert adapter_converted[4] == 0.2


def test_semantic_open_and_close_accept_timestamp_ties_without_inventing_order() -> None:
    trades = [
        {"timestamp": 1, "id": "z", "price": 11.0, "amount": 1.0},
        {"timestamp": 1, "id": "a", "price": 10.0, "amount": 1.0},
        {"timestamp": 2, "id": "b", "price": 12.0, "amount": 1.0},
        {"timestamp": 2, "id": "y", "price": 9.0, "amount": 1.0},
    ]

    matches = gate._semantic_field_matches(
        [11.0, 12.0, 9.0, 9.0, 4.0],
        trades,
        {"contract": False},
    )

    assert all(matches.values())


def test_rest_semantic_mismatch_is_adjudicated_by_pro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from xret.data.providers.ccxt import markets

    definition = _Definition(
        MarketIdentity(exchange="bybit", symbol="BTC/USDT", market="spot"),
        True,
        frozenset({"1m"}),
    )
    trades = [
        {"timestamp": 1, "price": 10.0, "amount": 1.0},
        {"timestamp": 60_000, "price": 10.0, "amount": 1.0},
        {"timestamp": 90_000, "price": 11.0, "amount": 1.0},
        {"timestamp": 120_000, "price": 11.0, "amount": 1.0},
    ]

    class Exchange:
        has = {"fetchTrades": True}

        def load_markets(self) -> None:
            return None

        def market(self, _symbol: str) -> dict[str, object]:
            return {"spot": True, "contract": False}

        def fetch_trades(self, _symbol: str, _since: None, _limit: int) -> list[dict[str, object]]:
            return trades

        def fetch_ohlcv(
            self,
            _symbol: str,
            _timeframe: str,
            since: int,
            _limit: int,
            _params: dict[str, int],
        ) -> list[list[float]]:
            return [[since, 10.0, 11.0, 10.0, 11.0, 99.0]]

    monkeypatch.setattr(
        markets,
        "resolve",
        lambda _identity, _exchange: SimpleNamespace(native_symbol="BTC/USDT"),
    )
    pro_calls: list[str] = []

    def pro_probe(*_args: object, **kwargs: object) -> dict[str, object]:
        pro_calls.append(str(kwargs["rest_failure"]))
        return {"status": "pass", "source": "ccxt_pro_watch_trades"}

    monkeypatch.setattr(gate, "_pro_semantic_probe", pro_probe)

    result = gate._semantic_probe(
        "bybit",
        {"spot": (definition,)},
        lambda _client_id: Exchange(),
        symbol_limit=1,
        pro_timeout_seconds=1.0,
    )

    assert result["spot"]["status"] == "pass"
    assert result["spot"]["rest_evidence"]["status"] == "fail"
    assert pro_calls == [
        "REST public-trade buckets disagreed with the candle; the bounded response may be truncated"
    ]


def test_missing_complete_rest_minute_is_adjudicated_by_pro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from xret.data.providers.ccxt import markets

    definition = _Definition(
        MarketIdentity(exchange="kraken", symbol="BTC/USD", market="perpetual", settle="BTC"),
        True,
        frozenset({"1m"}),
    )

    class Exchange:
        has = {"fetchTrades": True}

        def load_markets(self) -> None:
            return None

        def market(self, _symbol: str) -> dict[str, object]:
            return {"contract": True, "inverse": True, "contractSize": 1.0}

        def fetch_trades(self, _symbol: str, _since: None, _limit: int) -> list[dict[str, object]]:
            return [{"timestamp": 60_000, "price": 10.0, "amount": 1.0}]

    monkeypatch.setattr(
        markets,
        "resolve",
        lambda _identity, _exchange: SimpleNamespace(native_symbol="BTC/USD:BTC"),
    )
    monkeypatch.setattr(
        gate,
        "_pro_semantic_probe",
        lambda *_args, **_kwargs: {"status": "pass", "source": "ccxt_pro_watch_trades"},
    )

    result = gate._semantic_probe(
        "kraken",
        {"perpetual": (definition,)},
        lambda _client_id: Exchange(),
        symbol_limit=1,
        pro_timeout_seconds=1.0,
    )

    assert result["perpetual"]["status"] == "pass"
    assert result["perpetual"]["rest_evidence"]["attempted"][0]["complete_trade_minutes"] == 0


def test_semantic_probe_uses_the_qualified_native_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from xret.data.providers.ccxt import markets

    definition = _Definition(
        MarketIdentity(
            exchange="dydx",
            symbol="BTC/USDC",
            market="perpetual",
            settle="USDC",
        ),
        True,
        frozenset({"1m"}),
    )
    trades = [
        {"timestamp": 1, "price": 10.0, "amount": 1.0},
        {"timestamp": 60_000, "price": 10.0, "amount": 1.0},
        {"timestamp": 90_000, "price": 11.0, "amount": 2.0},
        {"timestamp": 120_000, "price": 11.0, "amount": 1.0},
    ]

    class Exchange:
        has = {"fetchTrades": True}

        def load_markets(self) -> None:
            return None

        def market(self, _symbol: str) -> dict[str, object]:
            return {"contract": False}

        def fetch_trades(self, _symbol: str, _since: None, _limit: int) -> list[dict[str, object]]:
            return trades

        def fetch_ohlcv(
            self,
            _symbol: str,
            _timeframe: str,
            since: None,
            _limit: int,
            params: dict[str, str],
        ) -> list[list[float]]:
            assert since is None
            assert params == {
                "fromISO": "1970-01-01T00:01:00.000Z",
                "toISO": "1970-01-01T00:01:59.999Z",
            }
            return [[60_000, 10.0, 11.0, 10.0, 11.0, 3.0]]

    monkeypatch.setattr(
        markets,
        "resolve",
        lambda _identity, _exchange: SimpleNamespace(native_symbol="BTC/USDC:USDC"),
    )

    result = gate._semantic_probe(
        "dydx",
        {"perpetual": (definition,)},
        lambda _client_id: Exchange(),
        symbol_limit=1,
        pro_timeout_seconds=1.0,
    )

    assert result["perpetual"]["status"] == "pass"


def test_mandatory_risk_cases_do_not_count_toward_statistical_sample() -> None:
    definitions = _definitions()
    cases = gate.plan_cases(
        definitions,
        symbol_limit=2,
        target_samples=135,
        preferred=gate.PREFERRED_SYMBOLS,
        seed=20260823,
        page_bars_by_scope=_page_bars_by_scope(definitions),
    )

    statistical = [case for case in cases if case.kind == "statistical"]
    mandatory = [case for case in cases if case.kind == "mandatory"]

    statistical_by_scope = {
        scope: sum(gate.PlanScope(case.market, case.settle) == scope for case in statistical)
        for scope in _page_bars_by_scope(definitions)
    }

    assert statistical_by_scope == {
        gate.PlanScope("spot", None): 135,
        gate.PlanScope("perpetual", "USDT"): 135,
    }
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
    definitions = _definitions()
    arguments = {
        "symbol_limit": 2,
        "target_samples": 135,
        "preferred": gate.PREFERRED_SYMBOLS,
        "seed": 20260823,
        "page_bars_by_scope": _page_bars_by_scope(definitions),
    }

    first = gate.plan_cases(definitions, **arguments)
    second = gate.plan_cases(definitions, **arguments)

    assert first == second
    statistical = [case for case in first if case.kind == "statistical"]
    assert {case.end_offset_days for case in statistical} == set(gate._STATISTICAL_OFFSETS_DAYS)
    assert {case.bars for case in statistical} == set(gate._STATISTICAL_WINDOW_BARS)


def test_each_settlement_receives_its_own_mandatory_edge_suite() -> None:
    definitions = _definitions()
    definitions["perpetual"] += (
        _Definition(
            MarketIdentity(
                exchange="bybit",
                symbol="BTC/USDC",
                market="perpetual",
                settle="USDC",
            ),
            True,
            frozenset({"1m", "1h", "1d", "1M"}),
        ),
        _Definition(
            MarketIdentity(
                exchange="bybit",
                symbol="ETH/USDC",
                market="perpetual",
                settle="USDC",
            ),
            True,
            frozenset({"1m", "1h", "1d", "1M"}),
        ),
    )

    cases = gate.plan_cases(
        definitions,
        symbol_limit=2,
        target_samples=135,
        preferred=gate.PREFERRED_SYMBOLS,
        seed=20260823,
        page_bars_by_scope=_page_bars_by_scope(definitions),
    )

    required = {
        "pagination-n-minus-1",
        "pagination-n",
        "pagination-n-plus-1",
        "pagination-two-n-plus-1",
        "incremental-left-right",
        "concurrent-same-dataset",
        "long-history-1y",
        "long-history-3y",
    }
    for settle in ("USDT", "USDC"):
        scenarios = {
            case.scenario
            for case in cases
            if case.kind == "mandatory" and case.market == "perpetual" and case.settle == settle
        }
        assert required <= scenarios
