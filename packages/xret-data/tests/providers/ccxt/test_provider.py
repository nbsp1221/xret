"""Deterministic, network-free tests for the built-in CCXT provider.

No test here needs `ccxt` installed or the network: every exchange is a
small fake supplied through `CcxtProvider` constructor injection.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from xret.data.errors import InvalidRequestError, ProviderError, UnsupportedMarketError
from xret.data.market_data import MarketData
from xret.data.models import BarRequest, MarketIdentity
from xret.data.providers import DerivativeInterpretation, ResolvedBarMarket
from xret.data.providers import runtime as provider_runtime
from xret.data.providers.ccxt import CcxtProvider, markets
from xret.data.providers.ccxt import provider as ccxt_provider
from xret.data.providers.runtime import ProviderRuntime
from xret.data.schema import OHLCV_COLUMNS
from xret.data.timeframe import TimeBar


@pytest.fixture(autouse=True)
def _reset_provider_seams():
    _exchanges.clear()
    provider_runtime._set_clock_override(None)
    yield
    _exchanges.clear()
    provider_runtime._set_clock_override(None)


def _set_now(value: datetime) -> None:
    provider_runtime._set_clock_override(lambda: value)


class FakeExchange:
    """A minimal fake satisfying `CCXTExchange`'s structural contract."""

    def __init__(
        self,
        *,
        client_id: str = "fakeex",
        markets: dict | None = None,
        has_fetch_ohlcv: bool = True,
        timeframes: dict | None = None,
        candles: list[list[float]] | None = None,
        fetch_override=None,
        page_size: int | None = None,
    ) -> None:
        self.id = client_id
        self.has = {"fetchOHLCV": has_fetch_ohlcv}
        self.markets = markets if markets is not None else _default_markets()
        self.timeframes = timeframes if timeframes is not None else {"1m": "1m", "1h": "1h"}
        self._candles = candles or []
        self._fetch_override = fetch_override
        self._page_size = page_size
        self.fetch_calls: list[tuple] = []
        self.load_markets_calls = 0

    def load_markets(self, reload: bool = False) -> dict:
        self.load_markets_calls += 1
        return self.markets

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None,
    ) -> list[list[float]]:
        self.fetch_calls.append((symbol, timeframe, since, limit))
        if self._fetch_override is not None:
            return self._fetch_override(symbol, timeframe, since, limit, params)
        until = params.get("until") if params is not None else None
        rows = [
            row
            for row in self._candles
            if row[0] >= (since or 0) and (until is None or row[0] < until)
        ]
        page_size = self._page_size if self._page_size is not None else (limit or len(rows))
        return rows[:page_size]


#: All fake candle timestamps are offsets from this anchor so they line up
#: with the 2024-01-01-based `start`/`end` values used throughout this file.
_BASE_MS = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1000)


def _row(offset_ms: int, price: float = 100.0, volume: float = 1.0) -> list[float]:
    ts_ms = _BASE_MS + offset_ms
    return [ts_ms, price, price + 1, price - 1, price, volume]


def _default_markets() -> dict:
    return {
        "BTC/USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT",
            "base": "BTC",
            "quote": "USDT",
            "spot": True,
        },
        "BTC/USDT:USDT": {
            "id": "BTCUSDT",
            "symbol": "BTC/USDT:USDT",
            "base": "BTC",
            "quote": "USDT",
            "swap": True,
            "settle": "USDT",
        },
    }


def _spot_identity() -> MarketIdentity:
    return MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot")


def _perp_identity(settle: str | None = None) -> MarketIdentity:
    return MarketIdentity(exchange="binance", symbol="BTC/USDT", market="perpetual", settle=settle)


def _register_spot(exchange: FakeExchange) -> None:
    _exchanges["binance"] = exchange


def _register_perp(exchange: FakeExchange) -> None:
    _exchanges["binanceusdm"] = exchange


_exchanges: dict[str, FakeExchange] = {}


def _exchange_factory(client_id: str) -> FakeExchange:
    return _exchanges[client_id]


def _ccxt_provider(**options) -> CcxtProvider:
    return CcxtProvider(
        exchange_factory=_exchange_factory,
        version_provider=lambda: "4.5.0",
        **options,
    )


def _market_data() -> MarketData:
    return MarketData(provider=_ccxt_provider())


def _observe(
    identity: MarketIdentity,
    timeframe: str,
    start: datetime,
    end: datetime,
    *,
    page_limit: int = ccxt_provider.DEFAULT_PAGE_LIMIT,
    max_retries: int = ccxt_provider.DEFAULT_MAX_RETRIES,
    retry_backoff_base: float = ccxt_provider.DEFAULT_RETRY_BACKOFF_BASE,
    sleep=None,
):
    provider = _ccxt_provider(
        page_limit=page_limit,
        max_retries=max_retries,
        retry_backoff_base=retry_backoff_base,
        sleep=sleep,
    )
    return ProviderRuntime(provider).observe(
        BarRequest(
            identity=identity,
            timeframe=timeframe,
            start=start,
            end=end,
        )
    )


def _fetch_bars(
    identity: MarketIdentity,
    timeframe: str,
    start: datetime,
    end: datetime,
    **provider_options,
):
    return _observe(
        identity,
        timeframe,
        start,
        end,
        **provider_options,
    ).frame


# --------------------------------------------------------------------------
# CCXT client id resolution (Decisions 5-6)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exchange", "expected"),
    [
        ("binance", "binanceusdm"),
        ("kraken", "krakenfutures"),
        ("kucoin", "kucoinfutures"),
    ],
)
def test_perpetual_client_id_uses_official_mapping_for_known_exchanges(
    exchange: str, expected: str
) -> None:
    identity = MarketIdentity(exchange=exchange, symbol="BTC/USD", market="perpetual")
    assert markets.client_id(identity) == expected


def test_perpetual_client_id_defaults_to_slug_when_unmapped() -> None:
    identity = MarketIdentity(exchange="okx", symbol="BTC/USDT", market="perpetual")
    assert markets.client_id(identity) == "okx"


def test_spot_client_id_is_always_the_slug() -> None:
    assert markets.client_id(_spot_identity()) == "binance"


# --------------------------------------------------------------------------
# `bars()` is no-I/O (Decision 9); `fetch` is the first I/O boundary
# --------------------------------------------------------------------------


def test_bars_performs_no_io() -> None:
    calls: list[str] = []
    provider = CcxtProvider(
        exchange_factory=lambda _client_id: (
            calls.append("built"),
            FakeExchange(client_id="binanceusdm", candles=[_row(0)]),
        )[1],
        version_provider=lambda: "test",
    )
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    market_data = MarketData(provider=provider)
    bars = market_data.bars(
        exchange="binance", symbol="BTC/USDT", market="perpetual", settle="USDT", timeframe="1m"
    )

    assert calls == []
    bars.fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    assert calls == ["built"]


def test_future_market_rejected_before_any_provider_interaction() -> None:
    calls: list[str] = []

    def unexpected_factory(_client_id: str) -> FakeExchange:
        calls.append("built")
        raise AssertionError("invalid market must fail before provider interaction")

    provider = CcxtProvider(
        exchange_factory=unexpected_factory,
        version_provider=lambda: "test",
    )

    market_data = MarketData(provider=provider)
    with pytest.raises(UnsupportedMarketError):
        market_data.bars(exchange="binance", symbol="BTC/USDT", market="future", timeframe="1h")

    assert calls == []


# --------------------------------------------------------------------------
# Settlement inference: 0 / 1 / N candidates (Decision 9, pre-mortem #1)
# --------------------------------------------------------------------------


def test_omitted_settle_infers_the_single_safe_candidate() -> None:
    exchange = FakeExchange(client_id="binanceusdm", candles=[_row(0)])
    _register_perp(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="perpetual", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    )

    assert (frame["settle"] == "USDT").all()
    assert exchange.fetch_calls[0][0] == "BTC/USDT:USDT"


def test_omitted_settle_with_zero_candidates_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={"BTC/USDT": {"base": "BTC", "quote": "USDT", "spot": True}},
    )
    _register_perp(exchange)

    with pytest.raises(UnsupportedMarketError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_omitted_settle_with_ambiguous_candidates_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            "BTC/USDT:USDT": {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT:USDT",
                "base": "BTC",
                "quote": "USDT",
                "swap": True,
                "settle": "USDT",
            },
            "BTC/USDT:USDC": {
                "id": "BTCUSDC",
                "symbol": "BTC/USDT:USDC",
                "base": "BTC",
                "quote": "USDT",
                "swap": True,
                "settle": "USDC",
            },
        },
    )
    _register_perp(exchange)

    with pytest.raises(UnsupportedMarketError, match="ambiguous"):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_duplicate_same_settle_perpetual_candidates_fail_closed() -> None:
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            "BTC/USDT:USDT": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDT"},
            "BTC/USDT:USDT-2": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDT"},
        },
    )
    _register_perp(exchange)

    with pytest.raises(UnsupportedMarketError, match="ambiguous"):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    with pytest.raises(UnsupportedMarketError, match="ambiguous"):
        _market_data().bars(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
            timeframe="1m",
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_explicit_settle_not_listed_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(client_id="binanceusdm")
    _register_perp(exchange)

    with pytest.raises(UnsupportedMarketError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", settle="BUSD", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_explicit_settle_never_reads_other_candidates() -> None:
    # Two settle currencies exist, but an explicit choice must never trigger
    # ambiguity: inference is bypassed entirely once `settle` is given.
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            "BTC/USDT:USDT": {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT:USDT",
                "base": "BTC",
                "quote": "USDT",
                "swap": True,
                "settle": "USDT",
            },
            "BTC/USDT:USDC": {
                "id": "BTCUSDC",
                "symbol": "BTC/USDT:USDC",
                "base": "BTC",
                "quote": "USDT",
                "swap": True,
                "settle": "USDC",
            },
        },
        candles=[_row(0)],
    )
    _register_perp(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", settle="USDC", timeframe="1m"
        )
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    )

    assert (frame["settle"] == "USDC").all()
    assert exchange.fetch_calls[0][0] == "BTC/USDT:USDC"


# --------------------------------------------------------------------------
# Spot resolution and capability checks
# --------------------------------------------------------------------------


def test_spot_fetch_uses_the_public_symbol_as_the_native_symbol() -> None:
    exchange = FakeExchange(candles=[_row(0)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    )

    assert exchange.fetch_calls[0][0] == "BTC/USDT"
    assert frame["settle"].null_count() == frame.height
    assert (frame["market"] == "spot").all()


def test_observation_spot_metadata_uses_the_resolved_ccxt_market() -> None:
    exchange = FakeExchange(
        client_id="binance",
        markets={
            "BTC/USDT": {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "spot": True,
            }
        },
        candles=[_row(0)],
    )
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))
    observation = _observe(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert observation.source.descriptor.name == "ccxt"
    assert observation.source.descriptor.version == "4.5.0"
    assert observation.source.descriptor.api_version == 1
    assert observation.source.native_market_id == "BTCUSDT"
    assert observation.source.native_symbol == "BTC/USDT"
    assert observation.market.derivative is None
    assert exchange.load_markets_calls == 1
    assert exchange.fetch_calls == [("BTC/USDT", "1m", _BASE_MS, 1000)]


def test_observation_perpetual_metadata_includes_native_market_interpretation() -> None:
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            "BTC/USDT:USDT": {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT:USDT",
                "base": "BTC",
                "quote": "USDT",
                "swap": True,
                "settle": "USDT",
                "linear": True,
                "inverse": False,
                "contractSize": 0.001,
            }
        },
        candles=[_row(0)],
    )
    _register_perp(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))
    observation = _observe(
        _perp_identity("USDT"),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert observation.source.descriptor.name == "ccxt"
    assert observation.source.descriptor.version == "4.5.0"
    assert observation.source.native_market_id == "BTCUSDT"
    assert observation.source.native_symbol == "BTC/USDT:USDT"
    assert observation.market.derivative == DerivativeInterpretation(
        linear=True, inverse=False, contract_size="0.001"
    )
    assert observation.frame["settle"].to_list() == ["USDT"]
    assert exchange.load_markets_calls == 1
    assert exchange.fetch_calls == [("BTC/USDT:USDT", "1m", _BASE_MS, 1000)]


def test_unlisted_spot_symbol_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(markets={})
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_resolved_market_without_native_id_fails_closed() -> None:
    exchange = FakeExchange(
        markets={
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "spot": True,
            }
        }
    )
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError, match="native CCXT market id"):
        _market_data().bars(
            exchange="binance",
            symbol="BTC/USDT",
            market="spot",
            timeframe="1m",
        ).fetch(
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        )


def test_missing_fetch_ohlcv_capability_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(has_fetch_ohlcv=False)
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_unsupported_timeframe_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(timeframes={"5m": "5m"})
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_absent_or_non_mapping_timeframes_fail_closed() -> None:
    for remove_metadata in (False, True):
        exchange = FakeExchange(candles=[_row(0)])
        if remove_metadata:
            del exchange.timeframes
        else:
            exchange.timeframes = ["1m"]
        _register_spot(exchange)

        with pytest.raises(UnsupportedMarketError):
            _market_data().bars(
                exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
            ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


# --------------------------------------------------------------------------
# Native bar types outside Xret's vocabulary
#
# A venue legitimately advertises bar types Xret cannot express (OKX `3M`,
# Upbit `1y`, Backpack bare `15`). Those are true facts about the venue, not
# contract violations, so they must not make the venue unresolvable.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "native_only",
    [
        pytest.param("3M", id="calendar-amount-above-one"),
        pytest.param("2w", id="calendar-week-amount-above-one"),
        pytest.param("1y", id="unit-outside-grammar"),
        pytest.param("1Y", id="unit-outside-grammar-uppercase"),
        pytest.param("15", id="bare-number-without-unit"),
        pytest.param("1H", id="non-canonical-casing"),
    ],
)
def test_native_timeframe_outside_vocabulary_is_excluded_not_fatal(native_only: str) -> None:
    exchange = FakeExchange(timeframes={"1h": "1h", native_only: native_only})

    assert markets.supported_timeframes(exchange) == frozenset({"1h"})


def test_canonical_calendar_timeframes_survive_filtering() -> None:
    """The filter removes only what the grammar rejects, not calendar units."""
    exchange = FakeExchange(
        timeframes=dict.fromkeys(["1m", "1h", "1d", "1w", "1M", "3M", "2w", "1y"], "x")
    )

    assert markets.supported_timeframes(exchange) == frozenset({"1m", "1h", "1d", "1w", "1M"})


@pytest.mark.parametrize(
    "native_only",
    [
        pytest.param("3M", id="calendar-amount-above-one"),
        pytest.param("1y", id="unit-outside-grammar"),
        pytest.param("15", id="bare-number-without-unit"),
    ],
)
def test_venue_stays_resolvable_despite_inexpressible_bar_type(native_only: str) -> None:
    exchange = FakeExchange(timeframes={"1h": "1h", native_only: native_only})
    _register_spot(exchange)

    resolved = _ccxt_provider().resolve_market(_spot_identity())

    assert resolved.timeframes == frozenset({"1h"})


def test_canonical_timeframe_is_observable_despite_inexpressible_bar_type() -> None:
    exchange = FakeExchange(
        timeframes={"1h": "1h", "3M": "3M"},
        candles=[_row(0), _row(3_600_000)],
    )
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 3, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1h",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 2, tzinfo=UTC),
    )

    assert frame.height == 2


@pytest.mark.parametrize("excluded", ["3M", "1y", "15", "1H"])
def test_excluded_bar_type_is_rejected_before_any_provider_call(excluded: str) -> None:
    """Exclusion never substitutes another bar type.

    A timeframe the filter drops is by definition non-canonical, so the
    grammar rejects the request itself; `ProviderRuntime`'s
    `UnsupportedMarketError` path only covers canonical timeframes a venue
    omits (see `test_unsupported_timeframe_raises_unsupported_market_error`).
    """
    with pytest.raises(InvalidRequestError):
        BarRequest(
            identity=_spot_identity(),
            timeframe=excluded,
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 4, 1, tzinfo=UTC),
        )


def test_venue_advertising_only_inexpressible_bar_types_supports_nothing() -> None:
    exchange = FakeExchange(timeframes={"3M": "3M", "1y": "1y"})
    _register_spot(exchange)

    resolved = _ccxt_provider().resolve_market(_spot_identity())

    assert resolved.timeframes == frozenset()


# --------------------------------------------------------------------------
# Canonical output schema (IR-2): no `run_id`, provider-independent identity
# --------------------------------------------------------------------------


def test_fetch_returns_canonical_schema_with_no_run_id() -> None:
    exchange = FakeExchange(candles=[_row(0)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    )

    assert tuple(frame.columns) == OHLCV_COLUMNS
    assert "run_id" not in frame.columns
    row = frame.row(0, named=True)
    assert row["exchange"] == "binance"
    assert row["symbol"] == "BTC/USDT"
    assert row["market"] == "spot"


def test_fetch_has_no_local_side_effects_and_is_repeatable() -> None:
    exchange = FakeExchange(candles=[_row(0)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))
    bars = _market_data().bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")

    first = bars.fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    second = bars.fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    assert first.equals(second)
    assert exchange.load_markets_calls == 1
    assert len(exchange.fetch_calls) == 2


# --------------------------------------------------------------------------
# Pagination
# --------------------------------------------------------------------------


def test_monotonic_pagination_collects_all_pages() -> None:
    minute = 60_000
    candles = [_row(i * minute) for i in range(5)]
    exchange = FakeExchange(candles=candles, page_size=2)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        page_limit=2,
    )

    assert frame.height == 5
    timestamps = frame["timestamp"].to_list()
    assert timestamps == sorted(timestamps)


def test_pagination_honors_the_requested_limit_when_it_is_below_the_provider_cap() -> None:
    minute = 60_000
    candles = [_row(i * minute) for i in range(5)]
    exchange = FakeExchange(candles=candles, page_size=2)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        page_limit=2,
    )

    assert frame.height == 5
    assert len(exchange.fetch_calls) == 3


def test_fetch_traverses_an_empty_bounded_page_and_returns_later_data() -> None:
    minute = 60_000

    class SparseWindowExchange(FakeExchange):
        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            self.fetch_calls.append((symbol, timeframe, since, limit, params))
            if since == _BASE_MS:
                return [_row(0)]
            if since == _BASE_MS + minute:
                return []
            if since == _BASE_MS + 2 * minute:
                return [_row(2 * minute)]
            return []

    exchange = SparseWindowExchange(client_id="binance")
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 3, tzinfo=UTC),
        page_limit=1,
    )

    assert frame["timestamp"].to_list() == [
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
    ]


def test_fetch_traverses_an_empty_first_page_before_later_listing_data() -> None:
    minute = 60_000
    exchange = FakeExchange(candles=[_row(minute)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
        page_limit=1,
    )

    assert frame["timestamp"].to_list() == [datetime(2024, 1, 1, 0, 1, tzinfo=UTC)]
    assert len(exchange.fetch_calls) == 2


def test_fetch_traverses_every_page_of_a_fully_empty_range() -> None:
    exchange = FakeExchange(candles=[])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 3, tzinfo=UTC),
        page_limit=1,
    )

    assert frame.is_empty()
    assert len(exchange.fetch_calls) == 3


def test_fetch_rejects_provider_boundary_extras_instead_of_clipping_them() -> None:
    """A venue overshooting the window by one bar is still a contract violation.

    Clipping the extras would leave the same code path silently accepting a
    response drawn from an entirely unrelated range, which is how a false
    `unavailable` gets minted. Lane 3 measurement on 2026-07-31 found no
    qualified venue that overshoots, so rejecting costs nothing real.
    """
    minute = 60_000

    def boundary_extras(
        _symbol: str,
        _timeframe: str,
        since: int,
        _limit: int,
        params: dict,
    ) -> list[list[float]]:
        return [
            _row(since - _BASE_MS - minute),
            _row(since - _BASE_MS),
            _row(params["until"] - _BASE_MS + 1),
        ]

    exchange = FakeExchange(fetch_override=boundary_extras)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    with pytest.raises(ProviderError, match="outside the requested window"):
        _fetch_bars(
            _spot_identity(),
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            page_limit=1,
        )


def test_half_open_pages_do_not_overfill_an_inclusive_endpoint_limit() -> None:
    minute = 60_000
    candles = [_row(index * minute) for index in range(4)]

    def newest_rows_from_inclusive_window(
        _symbol: str,
        _timeframe: str,
        since: int,
        limit: int,
        params: dict,
    ) -> list[list[float]]:
        candidates = [row for row in candles if since <= int(row[0]) <= params["until"]]
        return candidates[-limit:]

    exchange = FakeExchange(client_id="bybit", fetch_override=newest_rows_from_inclusive_window)
    _exchanges["bybit"] = exchange
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = _fetch_bars(
        MarketIdentity(exchange="bybit", symbol="BTC/USDT", market="spot"),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 4, tzinfo=UTC),
        page_limit=2,
    )

    assert frame["timestamp"].to_list() == [
        datetime(2024, 1, 1, 0, minute, tzinfo=UTC) for minute in range(4)
    ]


def test_coinbase_effective_limit_does_not_skip_a_later_window() -> None:
    minute = 60_000
    identity = MarketIdentity(exchange="coinbase", symbol="BTC/USDT", market="spot")
    exchange = FakeExchange(
        client_id="coinbase",
        candles=[_row(0), _row(300 * minute)],
    )
    _exchanges["coinbase"] = exchange
    _set_now(datetime(2024, 1, 2, tzinfo=UTC))

    frame = _fetch_bars(
        identity,
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 5, 1, tzinfo=UTC),
        page_limit=1000,
    )

    assert frame.height == 2
    assert [call[3] for call in exchange.fetch_calls] == [300, 300]


def test_unqualified_exchange_pagination_fails_closed() -> None:
    identity = MarketIdentity(exchange="kraken", symbol="BTC/USDT", market="spot")
    exchange = FakeExchange(client_id="kraken")
    _exchanges["kraken"] = exchange

    with pytest.raises(UnsupportedMarketError, match="no qualified exhaustive"):
        _fetch_bars(
            identity,
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        )


# --------------------------------------------------------------------------
# Bounded-window observation evidence
#
# A page proves its window only when the response honors the request that
# produced it. A venue ignoring `until` or `limit` would otherwise let Xret
# record `unavailable` for a range it never actually observed.
# --------------------------------------------------------------------------


def _window_probe(rows_for: Callable[[int], list[list[float]]]) -> None:
    """Register a venue whose page response ignores the requested window."""

    def fetch(
        _symbol: str,
        _timeframe: str,
        since: int,
        _limit: int,
        _params: dict,
    ) -> list[list[float]]:
        return rows_for(since)

    _register_spot(FakeExchange(client_id="binance", fetch_override=fetch))
    _set_now(datetime(2024, 1, 1, 1, tzinfo=UTC))


def _probe_observe(page_limit: int = 1000):
    return _observe(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 4, tzinfo=UTC),
        page_limit=page_limit,
    )


@pytest.mark.parametrize(
    "offsets",
    [
        pytest.param([-60_000], id="row-before-the-window"),
        pytest.param([240_000], id="row-exactly-at-the-exclusive-end"),
        pytest.param([600_000], id="row-far-after-the-window"),
        pytest.param([0, 240_000], id="only-violation-is-one-bar-past-the-end"),
        pytest.param([0, 600_000], id="one-row-inside-one-outside"),
    ],
)
def test_a_page_row_outside_its_window_fails_closed(offsets: list[int]) -> None:
    _window_probe(lambda _since: [_row(offset) for offset in offsets])

    with pytest.raises(ProviderError, match="outside the requested window"):
        _probe_observe()


def test_a_page_ignoring_until_entirely_fails_closed() -> None:
    """The whole response comes from an unrelated later range."""
    unrelated = 500 * 60_000
    _window_probe(lambda _since: [_row(unrelated + index * 60_000) for index in range(3)])

    with pytest.raises(ProviderError, match="outside the requested window"):
        _probe_observe()


def test_an_out_of_window_page_failure_names_the_venue_and_the_row() -> None:
    _window_probe(lambda _since: [_row(600_000)])

    with pytest.raises(ProviderError) as excinfo:
        _probe_observe()

    message = str(excinfo.value)
    assert "BTC/USDT" in message
    assert "binance" in message
    assert datetime(2024, 1, 1, 0, 10, tzinfo=UTC).isoformat() in message


def test_an_unrepresentable_timestamp_still_names_the_venue() -> None:
    """A venue emitting microsecond epochs must not degrade into a clock error."""
    _window_probe(lambda since: [[since * 1000, 100.0, 101.0, 99.0, 100.0, 1.0]])

    with pytest.raises(ProviderError, match="outside the requested window") as excinfo:
        _probe_observe()

    assert "binance" in str(excinfo.value)


@pytest.mark.parametrize(
    "timestamp",
    [
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="inf"),
        pytest.param(float("-inf"), id="negative-inf"),
        pytest.param(None, id="none"),
        pytest.param("not-a-number", id="non-numeric-string"),
    ],
)
def test_an_uncoercible_timestamp_is_reported_as_a_malformed_candle(timestamp: object) -> None:
    """Conversion failures must not escape as raw ValueError or OverflowError."""
    _window_probe(lambda _since: [[timestamp, 100.0, 101.0, 99.0, 100.0, 1.0]])

    with pytest.raises(ProviderError, match="malformed candle") as excinfo:
        _probe_observe()

    assert "binance" in str(excinfo.value)


def test_a_decimal_string_timestamp_is_coerced_like_a_collected_row() -> None:
    """The validator and the collector must agree on what a timestamp is."""
    _window_probe(lambda since: [[f"{since}.0", 100.0, 101.0, 99.0, 100.0, 1.0]])

    observation = _probe_observe()

    assert observation.frame["timestamp"].to_list() == [datetime(2024, 1, 1, tzinfo=UTC)]


def test_a_saturated_in_window_page_proves_its_window() -> None:
    """A full page is not suspicious: the window is exactly `limit` bars wide.

    That sizing is also why no separate row-count check exists. Aligned,
    unique, in-window rows cannot exceed the limit, so an over-limit response
    must carry either out-of-window rows (rejected above) or duplicate and
    off-boundary rows, which fail fatal quality validation before any coverage
    is recorded.
    """
    minute = 60_000
    exchange = FakeExchange(candles=[_row(index * minute) for index in range(4)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 1, tzinfo=UTC))

    observation = _observe(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 4, tzinfo=UTC),
        page_limit=2,
    )

    assert observation.frame.height == 4
    assert len(exchange.fetch_calls) == 2
    assert [(window.start, window.end) for window in observation.observed] == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 2, tzinfo=UTC)),
        (datetime(2024, 1, 1, 0, 2, tzinfo=UTC), datetime(2024, 1, 1, 0, 4, tzinfo=UTC)),
    ]


def test_a_duplicated_in_window_page_fails_fatal_quality_validation() -> None:
    """One way an over-limit page can stay entirely inside the window.

    The other requires off-timeframe timestamps. Both fail fatal quality
    validation before any coverage is recorded, which is why the page validator
    carries no separate row-count check.
    """

    def duplicated(
        _symbol: str,
        _timeframe: str,
        since: int,
        _limit: int,
        _params: dict,
    ) -> list[list[float]]:
        return [[since, 100.0, 101.0, 99.0, 100.0, 1.0]] * 4

    _register_spot(FakeExchange(fetch_override=duplicated))
    _set_now(datetime(2024, 1, 1, 1, tzinfo=UTC))

    with pytest.raises(ProviderError, match="identity.duplicate"):
        _observe(
            _spot_identity(),
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
            page_limit=2,
        )


@pytest.mark.parametrize(
    "present_offsets",
    [
        pytest.param([], id="empty-page"),
        pytest.param([0], id="one-of-four"),
        pytest.param([0, 180_000], id="two-of-four-with-a-hole"),
    ],
)
def test_an_under_filled_in_window_page_proves_its_window(present_offsets: list[int]) -> None:
    """Sparse markets are the reason evidence is separate from returned rows.

    Rejecting a short response would reintroduce the defect fixed in
    `.internal/bugs/coinbase-empty-page-pagination`.
    """
    _register_spot(FakeExchange(candles=[_row(offset) for offset in present_offsets]))
    _set_now(datetime(2024, 1, 1, 1, tzinfo=UTC))

    observation = _probe_observe()

    assert observation.frame["timestamp"].to_list() == [
        datetime(2024, 1, 1, tzinfo=UTC) + timedelta(milliseconds=offset)
        for offset in present_offsets
    ]
    assert [(window.start, window.end) for window in observation.observed] == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 4, tzinfo=UTC))
    ]


def test_ccxt_observation_does_not_fallback_to_reresolving_an_unowned_market() -> None:
    exchange = FakeExchange()
    _register_spot(exchange)
    identity = _spot_identity()
    market = ResolvedBarMarket(
        identity=identity,
        native_market_id="BTCUSDT",
        native_symbol="BTC/USDT",
        timeframes=frozenset({"1m"}),
    )

    with pytest.raises(ProviderError, match="not resolved by this provider instance"):
        CcxtProvider().observe_bars(
            BarRequest(
                identity=identity,
                timeframe="1m",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            ),
            market,
        )

    assert exchange.load_markets_calls == 0

    assert exchange.fetch_calls == []


def test_concurrent_same_market_fetches_share_one_stable_serialized_resolution() -> None:
    resolution_barrier = threading.Barrier(2)
    active_lock = threading.Lock()
    active_fetches = 0
    max_active_fetches = 0

    def fetch(*_args):
        nonlocal active_fetches, max_active_fetches
        with active_lock:
            active_fetches += 1
            max_active_fetches = max(max_active_fetches, active_fetches)
        time.sleep(0.01)
        with active_lock:
            active_fetches -= 1
        return [_row(0, price=123.0)]

    exchange = FakeExchange(
        client_id="coinbase",
        markets={
            "BTC/USDT": {
                "id": "BTC-USDT",
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "spot": True,
            }
        },
        fetch_override=fetch,
    )
    factory_calls = 0

    def factory(_client_id: str) -> FakeExchange:
        nonlocal factory_calls
        factory_calls += 1
        return exchange

    class SynchronizedProvider(CcxtProvider):
        def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
            market = super().resolve_market(identity)
            resolution_barrier.wait(timeout=1)
            return market

    provider = SynchronizedProvider(
        exchange_factory=factory,
        version_provider=lambda: "test",
    )
    identity = MarketIdentity(
        exchange="coinbase",
        symbol="BTC/USDT",
        market="spot",
    )
    request = BarRequest(
        identity=identity,
        timeframe="1m",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    with ThreadPoolExecutor(max_workers=2) as executor:
        frames = tuple(
            executor.map(
                lambda _: ProviderRuntime(provider).observe(request).frame,
                range(2),
            )
        )

    assert factory_calls == 1
    assert max_active_fetches == 1
    assert [frame["open"][0] for frame in frames] == [123.0, 123.0]
    assert len(exchange.fetch_calls) == 2


def test_calendar_month_pages_advance_without_fixed_duration_approximation() -> None:
    january = datetime(2024, 1, 1, tzinfo=UTC)
    march = datetime(2024, 3, 1, tzinfo=UTC)
    rows = [
        [int(value.timestamp() * 1000), 100.0, 101.0, 99.0, 100.0, 1.0]
        for value in (january, march)
    ]
    exchange = FakeExchange(timeframes={"1M": "1M"}, candles=rows)
    _register_spot(exchange)
    _set_now(datetime(2024, 5, 1, tzinfo=UTC))

    frame = _fetch_bars(
        _spot_identity(),
        "1M",
        january,
        datetime(2024, 4, 1, tzinfo=UTC),
        page_limit=1,
    )

    assert frame["timestamp"].to_list() == [january, march]
    assert [call[2] for call in exchange.fetch_calls] == [
        int(datetime(2024, month, 1, tzinfo=UTC).timestamp() * 1000) for month in (1, 2, 3)
    ]


def test_request_range_filter_is_half_open() -> None:
    minute = 60_000
    candles = [_row(i * minute) for i in range(5)]
    exchange = FakeExchange(candles=candles, page_size=10)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 3, tzinfo=UTC))
    )

    assert frame.height == 3


def test_nonpositive_page_limit_raises_provider_error() -> None:
    exchange = FakeExchange()
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    with pytest.raises(ProviderError, match="page limit must be positive"):
        _fetch_bars(
            _spot_identity(),
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
            page_limit=0,
        )


def test_non_ascending_batch_raises_provider_error() -> None:
    exchange = FakeExchange(fetch_override=lambda *_args: [_row(2_000), _row(1_000)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    with pytest.raises(ProviderError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 5, tzinfo=UTC))


@pytest.mark.parametrize(
    "row",
    [
        [_BASE_MS],
        ["not-a-timestamp", 100.0, 101.0, 99.0, 100.0, 1.0],
    ],
)
def test_malformed_provider_rows_raise_chained_provider_error(row: list[object]) -> None:
    exchange = FakeExchange(fetch_override=lambda *_args: [row])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    with pytest.raises(ProviderError) as excinfo:
        _fetch_bars(
            _spot_identity(),
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        )

    assert excinfo.value.__cause__ is not None


# --------------------------------------------------------------------------
# Completed-bar policy (Decision 13) and half-open bounds
# --------------------------------------------------------------------------


def test_candle_within_grace_window_is_dropped() -> None:
    minute = 60_000
    candles = [_row(0), _row(1 * minute)]
    exchange = FakeExchange(candles=candles, page_size=10)
    _register_spot(exchange)
    # t=1min bar closes at t=2min; "now" is 1:01 after that close, inside grace.
    _set_now(datetime(2024, 1, 1, 0, 2, 1, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 5, tzinfo=UTC))
    )

    assert frame["timestamp"].to_list() == [datetime(2024, 1, 1, tzinfo=UTC)]


def test_candle_past_grace_window_is_included() -> None:
    minute = 60_000
    candles = [_row(0), _row(1 * minute)]
    exchange = FakeExchange(candles=candles, page_size=10)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 2, 10, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 5, tzinfo=UTC))
    )

    assert frame.height == 2


# --------------------------------------------------------------------------
# Omitted `end` default (IR-3: boundary + adapter-owned grace, fetch only)
# --------------------------------------------------------------------------


def test_default_end_is_the_latest_completed_bar_boundary_with_grace() -> None:
    time_bar = TimeBar.parse("1m")
    _set_now(datetime(2024, 1, 1, 0, 0, 5, tzinfo=UTC))

    assert provider_runtime.default_end(time_bar) == datetime(2024, 1, 1, tzinfo=UTC)


def test_fetch_with_omitted_end_uses_provider_grace_boundary() -> None:
    exchange = FakeExchange(candles=[_row(0)], page_size=10)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 0, 5, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2023, 12, 31, 23, 59, tzinfo=UTC))
    )

    assert frame.height == 0  # [23:59, 00:00) excludes the t=0 candle


# --------------------------------------------------------------------------
# Retry behavior
# --------------------------------------------------------------------------


class RateLimitExceeded(Exception):
    pass


class PermanentExchangeError(Exception):
    pass


def test_transient_error_retries_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    attempts = {"count": 0}
    monkeypatch.setattr("time.sleep", sleeps.append)

    def flaky(*_args):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RateLimitExceeded("slow down")
        return [_row(0)]

    exchange = FakeExchange(fetch_override=flaky)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        _market_data()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        )
    )

    assert frame.height == 1
    assert attempts["count"] == 3
    assert len(sleeps) == 2


def test_transient_error_exhausts_retries_and_raises_provider_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    exchange = FakeExchange(
        fetch_override=lambda *_args: (_ for _ in ()).throw(RateLimitExceeded())
    )
    _register_spot(exchange)

    with pytest.raises(ProviderError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_permanent_error_does_not_retry() -> None:
    def boom(*_args):
        raise PermanentExchangeError("nope")

    exchange = FakeExchange(fetch_override=boom)
    _register_spot(exchange)

    with pytest.raises(ProviderError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    assert len(exchange.fetch_calls) == 1


# --------------------------------------------------------------------------
# Errors chain to their cause (P-1)
# --------------------------------------------------------------------------


def test_provider_error_chains_the_underlying_cause() -> None:
    def boom(*_args):
        raise PermanentExchangeError("nope")

    exchange = FakeExchange(fetch_override=boom)
    _register_spot(exchange)

    with pytest.raises(ProviderError) as excinfo:
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    assert isinstance(excinfo.value.__cause__, PermanentExchangeError)


def test_naive_start_is_rejected() -> None:
    exchange = FakeExchange()
    _register_spot(exchange)

    with pytest.raises(InvalidRequestError):
        _market_data().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1))  # noqa: DTZ001 - intentionally naive


def test_observation_uses_pre_call_evidence_time_and_post_call_completion_time() -> None:
    exchange = FakeExchange(
        candles=[_row(0)],
        markets={
            "BTC/USDT": {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "spot": True,
            }
        },
    )
    _register_spot(exchange)
    samples = iter(
        [
            datetime(2024, 1, 1, 0, 1, 4, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 1, 6, tzinfo=UTC),
        ]
    )
    calls = 0

    def clock() -> datetime:
        nonlocal calls
        calls += 1
        return next(samples)

    provider_runtime._set_clock_override(clock)
    observation = _observe(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert observation.frame.is_empty()
    assert observation.evidence_at == datetime(2024, 1, 1, 0, 1, 4, tzinfo=UTC)
    assert observation.completed_at == datetime(2024, 1, 1, 0, 1, 6, tzinfo=UTC)
    assert calls == 2
