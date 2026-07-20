"""Deterministic, network-free tests for `xret.data.provider`, wired through
the public `MarketData` -> `BarDataset.fetch` path (S2 walking skeleton).

No test here needs `ccxt` installed or the network: every exchange is a
small fake injected through the module-private test seam in `provider.py`
(P-3) -- there is no public provider/clock injection parameter anywhere in
this file.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from xret.data import provider
from xret.data.errors import InvalidRequestError, ProviderError, UnsupportedMarketError
from xret.data.market_data import MarketData
from xret.data.models import MarketIdentity
from xret.data.schema import OHLCV_COLUMNS
from xret.data.timeframe import TimeBar


@pytest.fixture(autouse=True)
def _reset_provider_seams():
    provider._reset_test_seams()
    yield
    provider._reset_test_seams()


def _set_now(value: datetime) -> None:
    provider._set_clock_override(lambda: value)


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
        self, symbol: str, timeframe: str, since: int | None = None, limit: int | None = None
    ) -> list[list[float]]:
        self.fetch_calls.append((symbol, timeframe, since, limit))
        if self._fetch_override is not None:
            return self._fetch_override(symbol, timeframe, since, limit)
        rows = [row for row in self._candles if row[0] >= (since or 0)]
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
        "BTC/USDT": {"base": "BTC", "quote": "USDT", "spot": True},
        "BTC/USDT:USDT": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDT"},
    }


def _spot_identity() -> MarketIdentity:
    return MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot")


def _perp_identity(settle: str | None = None) -> MarketIdentity:
    return MarketIdentity(exchange="binance", symbol="BTC/USDT", market="perpetual", settle=settle)


def _register_spot(exchange: FakeExchange) -> None:
    provider._register_exchange_factory("binance", lambda: exchange)


def _register_perp(exchange: FakeExchange) -> None:
    provider._register_exchange_factory("binanceusdm", lambda: exchange)


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
    assert provider._ccxt_client_id(identity) == expected


def test_perpetual_client_id_defaults_to_slug_when_unmapped() -> None:
    identity = MarketIdentity(exchange="okx", symbol="BTC/USDT", market="perpetual")
    assert provider._ccxt_client_id(identity) == "okx"


def test_spot_client_id_is_always_the_slug() -> None:
    assert provider._ccxt_client_id(_spot_identity()) == "binance"


# --------------------------------------------------------------------------
# `bars()` is no-I/O (Decision 9); `fetch` is the first I/O boundary
# --------------------------------------------------------------------------


def test_bars_performs_no_io() -> None:
    calls: list[str] = []
    provider._register_exchange_factory(
        "binanceusdm",
        lambda: (calls.append("built"), FakeExchange(client_id="binanceusdm", candles=[_row(0)]))[
            1
        ],
    )
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    market_data = MarketData()
    bars = market_data.bars(
        exchange="binance", symbol="BTC/USDT", market="perpetual", settle="USDT", timeframe="1m"
    )

    assert calls == []
    bars.fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    assert calls == ["built"]


def test_future_market_rejected_before_any_provider_interaction() -> None:
    calls: list[str] = []
    provider._register_exchange_factory("binance", lambda: calls.append("built"))

    market_data = MarketData()
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
        MarketData()
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
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_omitted_settle_with_ambiguous_candidates_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            "BTC/USDT:USDT": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDT"},
            "BTC/USDT:USDC": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDC"},
        },
    )
    _register_perp(exchange)

    with pytest.raises(UnsupportedMarketError, match="ambiguous"):
        MarketData().bars(
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
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    with pytest.raises(UnsupportedMarketError, match="ambiguous"):
        MarketData().bars(
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
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="perpetual", settle="BUSD", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_explicit_settle_never_reads_other_candidates() -> None:
    # Two settle currencies exist, but an explicit choice must never trigger
    # ambiguity: inference is bypassed entirely once `settle` is given.
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            "BTC/USDT:USDT": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDT"},
            "BTC/USDT:USDC": {"base": "BTC", "quote": "USDT", "swap": True, "settle": "USDC"},
        },
        candles=[_row(0)],
    )
    _register_perp(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        MarketData()
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
        MarketData()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    )

    assert exchange.fetch_calls[0][0] == "BTC/USDT"
    assert frame["settle"].null_count() == frame.height
    assert (frame["market"] == "spot").all()


def test_observation_spot_metadata_uses_the_resolved_ccxt_market(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    monkeypatch.setattr(provider, "_ccxt_version", lambda: "4.5.0")

    observation = provider._observe_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert observation.provider.name == "binance"
    assert observation.provider.ccxt_version == "4.5.0"
    assert observation.provider.market_id == "BTCUSDT"
    assert observation.provider.native_symbol == "BTC/USDT"
    assert observation.derivative is None
    assert exchange.load_markets_calls == 1
    assert exchange.fetch_calls == [("BTC/USDT", "1m", _BASE_MS, 1000)]


def test_observation_perpetual_metadata_includes_native_market_interpretation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    monkeypatch.setattr(provider, "_ccxt_version", lambda: "4.5.0")

    observation = provider._observe_bars(
        _perp_identity("USDT"),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert observation.provider.name == "binanceusdm"
    assert observation.provider.ccxt_version == "4.5.0"
    assert observation.provider.market_id == "BTCUSDT"
    assert observation.provider.native_symbol == "BTC/USDT:USDT"
    assert observation.derivative == provider.DerivativeInterpretation(
        linear=True, inverse=False, contract_size="0.001"
    )
    assert observation.frame["settle"].to_list() == ["USDT"]
    assert exchange.load_markets_calls == 1
    assert exchange.fetch_calls == [("BTC/USDT:USDT", "1m", _BASE_MS, 1000)]


def test_unlisted_spot_symbol_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(markets={})
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError):
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_missing_fetch_ohlcv_capability_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(has_fetch_ohlcv=False)
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError):
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_unsupported_timeframe_raises_unsupported_market_error() -> None:
    exchange = FakeExchange(timeframes={"5m": "5m"})
    _register_spot(exchange)

    with pytest.raises(UnsupportedMarketError):
        MarketData().bars(
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
            MarketData().bars(
                exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
            ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


# --------------------------------------------------------------------------
# Canonical output schema (IR-2): no `run_id`, provider-independent identity
# --------------------------------------------------------------------------


def test_fetch_returns_canonical_schema_with_no_run_id() -> None:
    exchange = FakeExchange(candles=[_row(0)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 5, tzinfo=UTC))

    frame = (
        MarketData()
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
    bars = MarketData().bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")

    first = bars.fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    second = bars.fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    assert first.equals(second)
    assert exchange.load_markets_calls == 2  # each fetch resolves independently; nothing cached


# --------------------------------------------------------------------------
# Pagination
# --------------------------------------------------------------------------


def test_monotonic_pagination_collects_all_pages() -> None:
    minute = 60_000
    candles = [_row(i * minute) for i in range(5)]
    exchange = FakeExchange(candles=candles, page_size=2)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = provider.fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        page_limit=2,
    )

    assert frame.height == 5
    timestamps = frame["timestamp"].to_list()
    assert timestamps == sorted(timestamps)


def test_pagination_continues_when_provider_caps_pages_below_requested_limit() -> None:
    minute = 60_000
    candles = [_row(i * minute) for i in range(5)]
    exchange = FakeExchange(candles=candles, page_size=2)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = provider.fetch_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        page_limit=100,
    )

    assert frame.height == 5
    assert len(exchange.fetch_calls) == 3


def test_request_range_filter_is_half_open() -> None:
    minute = 60_000
    candles = [_row(i * minute) for i in range(5)]
    exchange = FakeExchange(candles=candles, page_size=10)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    frame = (
        MarketData()
        .bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m")
        .fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 3, tzinfo=UTC))
    )

    assert frame.height == 3


def test_non_progress_pagination_raises_provider_error() -> None:
    # A misbehaving exchange that ignores `since` and always returns the
    # same stale candle must not spin forever.
    exchange = FakeExchange(fetch_override=lambda *_args: [_row(1_000)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    with pytest.raises(ProviderError, match="no progress"):
        provider.fetch_bars(
            _spot_identity(),
            "1m",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
            page_limit=1,
        )


def test_non_ascending_batch_raises_provider_error() -> None:
    exchange = FakeExchange(fetch_override=lambda *_args: [_row(2_000), _row(1_000)])
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 10, tzinfo=UTC))

    with pytest.raises(ProviderError):
        MarketData().bars(
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
        provider.fetch_bars(
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
        MarketData()
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
        MarketData()
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

    assert provider.default_end(time_bar) == datetime(2024, 1, 1, tzinfo=UTC)


def test_fetch_with_omitted_end_uses_provider_grace_boundary() -> None:
    exchange = FakeExchange(candles=[_row(0)], page_size=10)
    _register_spot(exchange)
    _set_now(datetime(2024, 1, 1, 0, 0, 5, tzinfo=UTC))

    frame = (
        MarketData()
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
        MarketData()
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
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))


def test_permanent_error_does_not_retry() -> None:
    def boom(*_args):
        raise PermanentExchangeError("nope")

    exchange = FakeExchange(fetch_override=boom)
    _register_spot(exchange)

    with pytest.raises(ProviderError):
        MarketData().bars(
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
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))

    assert isinstance(excinfo.value.__cause__, PermanentExchangeError)


def test_naive_start_is_rejected() -> None:
    exchange = FakeExchange()
    _register_spot(exchange)

    with pytest.raises(InvalidRequestError):
        MarketData().bars(
            exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1m"
        ).fetch(datetime(2024, 1, 1))  # noqa: DTZ001 - intentionally naive


def test_observation_uses_one_clock_sample_for_finality_and_absence_evidence() -> None:
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

    provider._set_clock_override(clock)
    observation = provider._observe_bars(
        _spot_identity(),
        "1m",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert observation.frame.is_empty()
    assert observation.completed_at == datetime(2024, 1, 1, 0, 1, 4, tzinfo=UTC)
    assert calls == 1
