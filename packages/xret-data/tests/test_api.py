"""Deterministic regression tests for the public market-data API.

Each test injects a `CcxtProvider` with a local exchange factory. No test
needs CCXT installed, uses the network, or touches the real Xret data tree.
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from xret.data import dataset
from xret.data.config import MarketDataConfig
from xret.data.dataset import _coalesce_fetch_ranges
from xret.data.errors import (
    CatalogError,
    CoverageError,
    InvalidRequestError,
    ProviderError,
    SyncError,
    UnsupportedMarketError,
)
from xret.data.market_data import MarketData
from xret.data.models import CoverageInterval, CoverageStatus, DatasetKey, MarketIdentity, YearMonth
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    BarObservation,
    ObservedWindow,
    ProviderDescriptor,
    ResolvedBarMarket,
)
from xret.data.providers import runtime as provider_runtime
from xret.data.providers.ccxt import CcxtProvider
from xret.data.storage import catalog as catalog_storage
from xret.data.storage import paths as storage_paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    SCHEMA_VERSION,
    Catalog,
    CoverageSegment,
    IngestionRunMetadata,
    _CommitUncertainCatalogError,
)
from xret.data.storage.parquet import read_month_file
from xret.data.timeframe import TimeBar

CLIENT_ID = "binance"
_exchanges: dict[str, FakeExchange] = {}

_BASE_MS = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1000)


class FakeExchange:
    """A minimal fake satisfying `CCXTExchange`'s structural contract."""

    def __init__(
        self,
        *,
        client_id: str = CLIENT_ID,
        markets: dict | None = None,
        timeframes: dict | None = None,
        candles: list[list[float]] | None = None,
        has_fetch_ohlcv: bool = True,
    ) -> None:
        self.id = client_id
        self.has = {"fetchOHLCV": has_fetch_ohlcv}
        self.markets = markets if markets is not None else _SPOT_MARKETS
        self.timeframes = timeframes if timeframes is not None else {"1h": "1h"}
        self._candles = candles or []
        self.fetch_calls: list[tuple] = []

    def load_markets(self, reload: bool = False) -> dict:
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
        until = params.get("until") if params is not None else None
        rows = [
            row
            for row in self._candles
            if row[0] >= (since or 0) and (until is None or row[0] < until)
        ]
        return rows[: (limit or len(rows))]


def _row(hour: int, price: float = 100.0, volume: float = 10.0) -> list[float]:
    ts_ms = _BASE_MS + hour * 3_600_000
    return [ts_ms, price, price + 1, price - 1, price + 0.5, volume]


def _hourly_candles(hours: range, *, skip: frozenset[int] = frozenset()) -> list[list[float]]:
    return [_row(h) for h in hours if h not in skip]


def _now(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _reset_seams():
    _exchanges.clear()
    provider_runtime._set_clock_override(None)
    dataset._reset_test_seams()
    yield
    _exchanges.clear()
    provider_runtime._set_clock_override(None)
    dataset._reset_test_seams()


def _configure(tmp_path: Path) -> MarketDataConfig:
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    dataset._set_config_override(config)
    return config


def _register(exchange: FakeExchange) -> None:
    _exchanges[CLIENT_ID] = exchange


def _register_perp(exchange: FakeExchange) -> None:
    _exchanges["binanceusdm"] = exchange


def _register_as(client_id: str, exchange: FakeExchange) -> None:
    _exchanges[client_id] = exchange


def _exchange_factory(client_id: str) -> FakeExchange:
    return _exchanges[client_id]


def _ccxt_provider() -> CcxtProvider:
    return CcxtProvider(
        exchange_factory=_exchange_factory,
        version_provider=lambda: "test",
    )


def _market_data(config: MarketDataConfig) -> MarketData:
    return MarketData(config=config, provider=_ccxt_provider())


def _bars(market_data: MarketData) -> dataset.BarDataset:
    return market_data.bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1h")


def _perp_bars(market_data: MarketData, *, settle: str | None = None) -> dataset.BarDataset:
    return market_data.bars(
        exchange="binance", symbol="BTC/USDT", market="perpetual", settle=settle, timeframe="1h"
    )


_SPOT_MARKETS = {
    "BTC/USDT": {
        "id": "BTCUSDT",
        "symbol": "BTC/USDT",
        "base": "BTC",
        "quote": "USDT",
        "spot": True,
    }
}

_PERP_MARKETS = {
    "BTC/USDT:USDT": {
        "id": "BTCUSDT",
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "swap": True,
        "contract": True,
        "linear": True,
        "inverse": False,
    }
}


# --------------------------------------------------------------------------
# fetch: no local storage side effects
# --------------------------------------------------------------------------


def test_fetch_has_no_storage_side_effect(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(5))

    frame = _bars(_market_data(config)).fetch(_now(0), _now(3))

    assert frame.height == 3
    assert len(exchange.fetch_calls) == 1
    assert not config.state_dir.exists()
    assert not config.data_dir.exists()


# --------------------------------------------------------------------------
# sync + scan
# --------------------------------------------------------------------------


def test_sync_is_idempotent_and_scan_is_lazy(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    bars = _bars(_market_data(config))

    first = bars.sync(_now(0), _now(3))
    assert first.is_complete
    assert first.changed
    assert first.fetched_rows == 3
    assert first.written_partitions == 1

    second = bars.sync(_now(0), _now(3))
    assert second.is_complete
    assert not second.changed
    assert second.fetched_rows == 0
    assert len(exchange.fetch_calls) == 1  # fully covered: no re-fetch

    result = bars.scan(_now(0), _now(3))
    assert isinstance(result, pl.LazyFrame)
    collected = result.collect()
    assert collected.height == 3
    assert collected.get_column("timestamp").is_sorted()


def test_successful_empty_sync_records_unavailable_and_terminalizes_run(
    tmp_path: Path,
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=[])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    bars = _bars(_market_data(config))
    result = bars.sync(_now(0), _now(3))

    assert not result.is_complete
    assert result.changed
    assert result.fetched_rows == 0
    assert {gap.status.value for gap in result.gaps} == {"unavailable"}
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        run = catalog.connection.execute(
            "SELECT status, completed_at, row_count FROM ingestion_runs WHERE run_id = ?",
            (result.run_id,),
        ).fetchone()
    assert run is not None
    assert (run["status"], run["row_count"]) == ("completed", 0)
    assert run["completed_at"] is not None
    assert result.written_partitions == 0


def test_sync_traverses_an_empty_bounded_window_before_later_data(tmp_path: Path) -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=201)
    exchange = FakeExchange(
        client_id="okx",
        candles=[_row(0), _row(200)],
    )
    _register_as("okx", exchange)
    provider_runtime._set_clock_override(lambda: end + timedelta(hours=10))
    dataset._set_clock_override(lambda: end + timedelta(hours=10))
    config = _configure(tmp_path)
    bars = _market_data(config).bars(
        exchange="okx", symbol="BTC/USDT", market="spot", timeframe="1h"
    )

    first = bars.sync(start, end)
    partial = bars.scan_partial(start, end)
    calls_after_first = len(exchange.fetch_calls)
    second = bars.sync(start, end)

    assert first.fetched_rows == 2
    assert partial.data.collect()["timestamp"].to_list() == [
        start,
        start + timedelta(hours=200),
    ]
    assert {gap.status for gap in partial.gaps} == {CoverageStatus.UNAVAILABLE}
    assert calls_after_first == 3
    assert not second.changed
    assert second.fetched_rows == 0
    assert len(exchange.fetch_calls) == calls_after_first


def test_sync_rejects_incomplete_provider_observation_before_publication(
    tmp_path: Path,
) -> None:
    config = _configure(tmp_path)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    frame = pl.DataFrame(
        {
            "timestamp": [_now(0)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        },
        schema=PROVIDER_BAR_SCHEMA,
    )

    class IncompleteProvider:
        descriptor = ProviderDescriptor("incomplete", "test", PROVIDER_API_VERSION)

        def resolve_market(self, identity):
            return ResolvedBarMarket(
                identity=identity,
                native_market_id="BTCUSDT",
                native_symbol="BTC/USDT",
                timeframes=frozenset({"1h"}),
            )

        def observe_bars(self, request, market):
            return BarObservation(
                frame=frame,
                observed=(ObservedWindow(_now(0), _now(1)),),
            )

    bars = _bars(MarketData(config=config, provider=IncompleteProvider()))

    with pytest.raises(ProviderError, match="incomplete provider observation"):
        bars.sync(_now(0), _now(3))

    partial = bars.scan_partial(_now(0), _now(3))
    assert partial.data.collect().height == 0
    assert {(gap.start, gap.end, gap.status) for gap in partial.gaps} == {
        (_now(0), _now(3), CoverageStatus.MISSING)
    }
    assert not list(config.data_dir.rglob("*.parquet"))


def test_unqualified_pagination_leaves_sync_coverage_missing(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(client_id="kraken", candles=[_row(0)])
    _register_as("kraken", exchange)
    bars = _market_data(config).bars(
        exchange="kraken", symbol="BTC/USDT", market="spot", timeframe="1h"
    )

    with pytest.raises(UnsupportedMarketError, match="no qualified exhaustive"):
        bars.sync(_now(0), _now(3))

    partial = bars.scan_partial(_now(0), _now(3))
    assert partial.data.collect().height == 0
    assert {(gap.start, gap.end, gap.status) for gap in partial.gaps} == {
        (_now(0), _now(3), CoverageStatus.MISSING)
    }
    assert not list(config.data_dir.rglob("*.parquet"))


def test_middle_page_failure_publishes_no_partial_observation(tmp_path: Path) -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=201)

    class FailingMiddleWindowExchange(FakeExchange):
        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            if since == int((start + timedelta(hours=100)).timestamp() * 1000):
                raise RuntimeError("middle page failed")
            return super().fetch_ohlcv(symbol, timeframe, since, limit, params)

    config = _configure(tmp_path)
    exchange = FailingMiddleWindowExchange(client_id="okx", candles=[_row(0), _row(200)])
    _register_as("okx", exchange)
    bars = _market_data(config).bars(
        exchange="okx", symbol="BTC/USDT", market="spot", timeframe="1h"
    )

    with pytest.raises(ProviderError, match="middle page failed"):
        bars.sync(start, end)

    partial = bars.scan_partial(start, end)
    assert partial.data.collect().height == 0
    assert {(gap.start, gap.end, gap.status) for gap in partial.gaps} == {
        (start, end, CoverageStatus.MISSING)
    }
    assert not list(config.data_dir.rglob("*.parquet"))


def test_nonfatal_quality_warning_is_run_linked_and_rebuild_resets_it(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3), skip=frozenset({1})))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    result = _bars(_market_data(config)).sync(_now(0), _now(3))

    assert [(warning.code, warning.start, warning.end) for warning in result.warnings] == [
        ("coverage.timeframe_gap", _now(0), _now(3))
    ]
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        events = catalog.list_quality_events(result.dataset_key)
    assert [(event.code, event.run_id) for event in events] == [
        ("coverage.timeframe_gap", result.run_id)
    ]

    _market_data(config).maintenance.rebuild_catalog()

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.list_quality_events(result.dataset_key) == ()


def test_sync_aggregates_same_month_missing_gaps_before_publication(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 4)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))
    key = DatasetKey.from_identity(bars.identity, timeframe=bars.timeframe)

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        catalog.apply_coverage(key, CoverageSegment(_now(0), _now(1), CoverageStatus.AVAILABLE))
        catalog.apply_coverage(key, CoverageSegment(_now(2), _now(3), CoverageStatus.AVAILABLE))

    result = bars.sync(_now(0), _now(4))

    assert result.changed
    # Coalesced window [1,4) returns hours 1,2,3; hour 2 is bridging
    assert result.fetched_rows == 3
    assert result.written_partitions == 1
    assert bars.scan_partial(_now(0), _now(4)).data.collect().get_column("timestamp").to_list() == [
        _now(1),
        _now(3),
    ]


def test_sync_leaves_explicit_unfinalized_tail_missing(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=[])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(4))
    dataset._set_clock_override(lambda: _now(4))

    result = _bars(_market_data(config)).sync(_now(0), _now(6))

    assert result.changed
    assert result.written_partitions == 0
    assert {(gap.start, gap.end, gap.status) for gap in result.gaps} == {
        (_now(0), _now(3), CoverageStatus.UNAVAILABLE),
        (_now(3), _now(6), CoverageStatus.MISSING),
    }


def test_distinct_dataset_syncs_overlap_provider_work_and_preserve_catalog_coherence(
    tmp_path: Path,
) -> None:
    class BlockingExchange(FakeExchange):
        def __init__(self) -> None:
            super().__init__(
                markets={
                    **_SPOT_MARKETS,
                    "ETH/USDT": {
                        "id": "ETHUSDT",
                        "symbol": "ETH/USDT",
                        "base": "ETH",
                        "quote": "USDT",
                        "spot": True,
                    },
                },
                candles=_hourly_candles(range(0, 3)),
            )
            self._started_symbols: set[str] = set()
            self._started_lock = threading.Lock()
            self.distinct_fetches_started = threading.Event()
            self.release_fetches = threading.Event()

        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            with self._started_lock:
                self._started_symbols.add(symbol)
                if len(self._started_symbols) == 2:
                    self.distinct_fetches_started.set()
            if not self.release_fetches.wait(timeout=5):
                raise RuntimeError("test did not release blocked provider work")
            return super().fetch_ohlcv(symbol, timeframe, since, limit, params)

    config = _configure(tmp_path)
    exchange = BlockingExchange()
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    market_data = _market_data(config)
    btc_bars = _bars(market_data)
    eth_bars = market_data.bars(
        exchange="binance", symbol="ETH/USDT", market="spot", timeframe="1h"
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        btc_sync = executor.submit(btc_bars.sync, _now(0), _now(3))
        eth_sync = executor.submit(eth_bars.sync, _now(0), _now(3))
        try:
            assert exchange.distinct_fetches_started.wait(timeout=5)
        finally:
            exchange.release_fetches.set()
        btc_result = btc_sync.result(timeout=5)
        eth_result = eth_sync.result(timeout=5)

    assert btc_result.is_complete
    assert eth_result.is_complete
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert {file.dataset_key for file in catalog.list_files()} == {
            btc_result.dataset_key,
            eth_result.dataset_key,
        }
    assert btc_bars.scan(_now(0), _now(3)).collect().height == 3
    assert eth_bars.scan(_now(0), _now(3)).collect().height == 3


def test_same_dataset_syncs_serialize_provider_work_and_preserve_canonical_state(
    tmp_path: Path,
) -> None:
    class BlockingExchange(FakeExchange):
        def __init__(self) -> None:
            super().__init__(candles=_hourly_candles(range(0, 3)))
            self._fetch_count = 0
            self._fetch_lock = threading.Lock()
            self.first_fetch_started = threading.Event()
            self.release_first_fetch = threading.Event()
            self.second_fetch_started_before_release = threading.Event()

        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            with self._fetch_lock:
                self._fetch_count += 1
                fetch_count = self._fetch_count
                if fetch_count == 1:
                    self.first_fetch_started.set()
                elif not self.release_first_fetch.is_set():
                    self.second_fetch_started_before_release.set()
            if fetch_count == 1 and not self.release_first_fetch.wait(timeout=5):
                raise RuntimeError("test did not release the first provider fetch")
            return super().fetch_ohlcv(symbol, timeframe, since, limit, params)

    config = _configure(tmp_path)
    exchange = BlockingExchange()
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))
    second_sync_started = threading.Event()

    def sync_second() -> dataset.SyncResult:
        second_sync_started.set()
        return bars.sync(_now(0), _now(3))

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_sync = executor.submit(bars.sync, _now(0), _now(3))
        assert exchange.first_fetch_started.wait(timeout=5)
        second_sync = executor.submit(sync_second)
        assert second_sync_started.wait(timeout=5)
        try:
            assert not exchange.second_fetch_started_before_release.is_set()
        finally:
            exchange.release_first_fetch.set()
        first_result = first_sync.result(timeout=5)
        second_result = second_sync.result(timeout=5)

    assert first_result.is_complete
    assert first_result.changed
    assert second_result.is_complete
    assert not second_result.changed
    assert second_result.fetched_rows == 0
    assert second_result.written_partitions == 0
    assert len(exchange.fetch_calls) == 1
    assert not exchange.second_fetch_started_before_release.is_set()
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        files = catalog.list_files()
    assert len(files) == 1
    assert files[0].dataset_key == first_result.dataset_key == second_result.dataset_key
    scanned = bars.scan(_now(0), _now(3)).collect()
    assert scanned.height == 3
    assert scanned.get_column("timestamp").is_sorted()


# --------------------------------------------------------------------------
# non-finite volume never becomes canonical
# --------------------------------------------------------------------------


class _NonFiniteVolumeExchange(FakeExchange):
    """A venue whose volume overflows to `inf`, as `float(str(...))` can."""

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None,
    ) -> list[list[float]]:
        self.fetch_calls.append((symbol, timeframe, since, limit))
        return [
            [_BASE_MS, 100.0, 101.0, 99.0, 100.5, float("inf")],
            [_BASE_MS + 3_600_000, 100.0, 101.0, 99.0, 100.5, 10.0],
        ]


def test_non_finite_volume_never_reaches_canonical_storage(tmp_path: Path) -> None:
    """`ProviderRuntime` enforces canonical quality before `dataset.sync` can
    remap the finding, so both verbs raise `ProviderError`. Correcting that
    classification is tracked separately (review M-21)."""
    config = _configure(tmp_path)
    _register(_NonFiniteVolumeExchange())
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    with pytest.raises(ProviderError, match="volume.non_finite"):
        bars.fetch(_now(0), _now(2))
    with pytest.raises(ProviderError, match="volume.non_finite"):
        bars.sync(_now(0), _now(2))

    partial = bars.scan_partial(_now(0), _now(2))
    assert not partial.covered
    assert [gap.status for gap in partial.gaps] == [CoverageStatus.MISSING]
    assert not list(config.data_dir.rglob("*.parquet"))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        runs = catalog.list_ingestion_run_ids(
            DatasetKey.from_identity(bars.identity, timeframe="1h")
        )
    assert len(runs) == 1
    status, completed_at = _run_status(config, runs[0])
    assert status == "failed"
    assert completed_at is not None

    _register(FakeExchange(candles=_hourly_candles(range(0, 2))))
    recovered = _bars(_market_data(config)).sync(_now(0), _now(2)).require_complete()

    assert recovered.fetched_rows == 2
    assert _bars(_market_data(config)).scan(_now(0), _now(2)).collect().height == 2


# --------------------------------------------------------------------------
# unproved observation windows never become negative coverage
# --------------------------------------------------------------------------


class _WindowIgnoringExchange(FakeExchange):
    """A venue that ignores `until` and always answers with recent candles."""

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
        params: dict | None = None,
    ) -> list[list[float]]:
        self.fetch_calls.append((symbol, timeframe, since, limit))
        unrelated = _BASE_MS + 500 * 3_600_000
        return [
            [unrelated + index * 3_600_000, 100.0, 101.0, 99.0, 100.5, 10.0] for index in range(3)
        ]


def test_a_venue_ignoring_the_window_records_no_coverage_and_stays_retryable(
    tmp_path: Path,
) -> None:
    """The defect this replaces minted permanent `unavailable` from a bad page.

    Failing closed keeps the range `missing`, so a later sync against a venue
    that honors the window still recovers it.
    """
    config = _configure(tmp_path)
    _register(_WindowIgnoringExchange())
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    with pytest.raises(ProviderError, match="outside the requested window"):
        bars.sync(_now(0), _now(3))

    partial = bars.scan_partial(_now(0), _now(3))
    assert not partial.covered
    assert [gap.status for gap in partial.gaps] == [CoverageStatus.MISSING]
    assert partial.data.collect().height == 0

    _register(FakeExchange(candles=_hourly_candles(range(0, 3))))
    recovered = _bars(_market_data(config)).sync(_now(0), _now(3)).require_complete()

    assert recovered.fetched_rows == 3
    assert _bars(_market_data(config)).scan(_now(0), _now(3)).collect().height == 3


# --------------------------------------------------------------------------
# month-crossing bars (see local_read._required_months)
# --------------------------------------------------------------------------


def _weekly_candles(weeks: int) -> list[list[float]]:
    return [_row(week * 168) for week in range(weeks)]


def _weekly_bars(market_data: MarketData) -> dataset.BarDataset:
    return market_data.bars(exchange="binance", symbol="BTC/USDT", market="spot", timeframe="1w")


@pytest.mark.parametrize(
    ("end", "expected_rows", "expected_partitions"),
    [
        pytest.param(datetime(2024, 1, 29, tzinfo=UTC), 4, 1, id="ends-inside-january"),
        pytest.param(datetime(2024, 2, 5, tzinfo=UTC), 5, 1, id="last-bar-crosses-into-february"),
        pytest.param(datetime(2024, 2, 12, tzinfo=UTC), 6, 2, id="ends-inside-february"),
        pytest.param(datetime(2024, 3, 4, tzinfo=UTC), 9, 2, id="last-bar-crosses-into-march"),
    ],
)
def test_month_crossing_bars_stay_readable(
    tmp_path: Path, end: datetime, expected_rows: int, expected_partitions: int
) -> None:
    config = _configure(tmp_path)
    _register(FakeExchange(timeframes={"1w": "1w"}, candles=_weekly_candles(12)))
    after_range = datetime(2024, 4, 1, tzinfo=UTC)
    provider_runtime._set_clock_override(lambda: after_range)
    dataset._set_clock_override(lambda: after_range)
    start = datetime(2024, 1, 1, tzinfo=UTC)

    bars = _weekly_bars(_market_data(config))
    result = bars.sync(start, end).require_complete()
    assert result.written_partitions == expected_partitions

    strict = bars.scan(start, end).collect()
    assert strict.height == expected_rows

    partial = bars.scan_partial(start, end)
    assert partial.is_complete
    assert partial.data.collect().equals(strict)


def test_month_crossing_scan_still_detects_a_deleted_partition(tmp_path: Path) -> None:
    """The fix narrows which partitions are required, not whether they exist."""
    config = _configure(tmp_path)
    _register(FakeExchange(timeframes={"1w": "1w"}, candles=_weekly_candles(12)))
    after_range = datetime(2024, 4, 1, tzinfo=UTC)
    provider_runtime._set_clock_override(lambda: after_range)
    dataset._set_clock_override(lambda: after_range)
    start, end = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 3, 4, tzinfo=UTC)

    bars = _weekly_bars(_market_data(config))
    bars.sync(start, end).require_complete()
    storage_paths.month_file_path(
        config.data_dir,
        DatasetKey.from_identity(
            MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot"), timeframe="1w"
        ),
        YearMonth(year=2024, month=1),
    ).unlink()

    with pytest.raises(CatalogError):
        bars.scan(start, end).collect()


# --------------------------------------------------------------------------
# strict and partial scan
# --------------------------------------------------------------------------


def test_strict_and_partial_scan_contracts(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))

    bars = _bars(_market_data(config))
    bars.sync(_now(0), _now(3))

    with pytest.raises(CoverageError):
        bars.scan(_now(0), _now(5))

    partial = bars.scan_partial(_now(0), _now(5))
    assert isinstance(partial.data, pl.LazyFrame)
    assert partial.data.collect().height == 3
    assert partial.gaps
    assert partial.warnings
    assert not partial.is_complete


def test_provider_internal_gap_is_not_marked_available(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3), skip=frozenset({1})))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))

    bars = _bars(_market_data(config))
    bars.sync(_now(0), _now(3))

    with pytest.raises(CoverageError):
        bars.scan(_now(0), _now(3))

    partial = bars.scan_partial(_now(0), _now(3))
    assert partial.data.collect().height == 2
    assert any(gap.start.hour == 1 and gap.end.hour == 2 for gap in partial.gaps)


def test_sync_surfaces_unsupported_capability(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(has_fetch_ohlcv=False)
    _register(exchange)

    bars = _bars(_market_data(config))

    with pytest.raises(UnsupportedMarketError):
        bars.sync(_now(0), _now(3))


def test_malformed_provider_observation_leaves_coverage_missing_and_unpublished(
    tmp_path: Path,
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=[[_BASE_MS]])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))
    key = DatasetKey.from_identity(bars.identity, timeframe=bars.timeframe)

    with pytest.raises(ProviderError, match="malformed candle"):
        bars.sync(_now(0), _now(3))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_coverage(key) == ()
        assert catalog.list_files() == ()
    with pytest.raises(CoverageError):
        bars.scan(_now(0), _now(3))
    partial = bars.scan_partial(_now(0), _now(3))
    assert partial.data.collect().height == 0
    assert {(gap.start, gap.end, gap.status) for gap in partial.gaps} == {
        (_now(0), _now(3), CoverageStatus.MISSING)
    }
    assert not list(config.data_dir.rglob("*.parquet"))


def test_provider_exception_leaves_coverage_missing_and_unpublished(tmp_path: Path) -> None:
    class FailingExchange(FakeExchange):
        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            raise RuntimeError("injected provider failure")

    config = _configure(tmp_path)
    _register(FailingExchange())
    bars = _bars(_market_data(config))
    key = DatasetKey.from_identity(bars.identity, timeframe=bars.timeframe)

    with pytest.raises(ProviderError, match="injected provider failure"):
        bars.sync(_now(0), _now(3))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_coverage(key) == ()
        assert catalog.list_files() == ()
    with pytest.raises(CoverageError):
        bars.scan(_now(0), _now(3))
    partial = bars.scan_partial(_now(0), _now(3))
    assert partial.data.collect().height == 0
    assert {(gap.start, gap.end, gap.status) for gap in partial.gaps} == {
        (_now(0), _now(3), CoverageStatus.MISSING)
    }
    assert not list(config.data_dir.rglob("*.parquet"))


def _run_status(config: MarketDataConfig, run_id: str) -> tuple[str, str | None]:
    """Return (status, completed_at) for a run_id from the catalog."""
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        row = catalog.connection.execute(
            "SELECT status, completed_at FROM ingestion_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    assert row is not None, f"run_id {run_id!r} not found in ingestion_runs"
    return row["status"], row["completed_at"]


def test_sync_records_failed_run_on_provider_error(tmp_path: Path) -> None:
    class FailingExchange(FakeExchange):
        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            raise RuntimeError("provider down")

    config = _configure(tmp_path)
    _register(FailingExchange())
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    with pytest.raises(ProviderError, match="provider down"):
        bars.sync(_now(0), _now(3))

    # Find the run_id: exactly one run should exist
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        runs = catalog.list_ingestion_run_ids(
            DatasetKey.from_identity(bars.identity, timeframe="1h")
        )
    assert len(runs) == 1
    status, completed_at = _run_status(config, runs[0])
    assert status == "failed"
    assert completed_at is not None


def test_sync_records_failed_run_on_quality_error(tmp_path: Path) -> None:
    # Malformed candle (only 1 element) triggers ProviderError during observation
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=[[_BASE_MS]])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    with pytest.raises(ProviderError, match="malformed candle"):
        bars.sync(_now(0), _now(3))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        runs = catalog.list_ingestion_run_ids(
            DatasetKey.from_identity(bars.identity, timeframe="1h")
        )
    assert len(runs) == 1
    status, completed_at = _run_status(config, runs[0])
    assert status == "failed"
    assert completed_at is not None


def test_sync_records_failed_run_on_prepare_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    def failing_prepare(*args, **kwargs):
        raise SyncError("injected prepare failure")

    monkeypatch.setattr(dataset, "prepare_month", failing_prepare)

    with pytest.raises(SyncError, match="injected prepare failure"):
        bars.sync(_now(0), _now(3))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        runs = catalog.list_ingestion_run_ids(
            DatasetKey.from_identity(bars.identity, timeframe="1h")
        )
    assert len(runs) == 1
    status, completed_at = _run_status(config, runs[0])
    assert status == "failed"
    assert completed_at is not None


def test_sync_failed_run_note_when_catalog_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingExchange(FakeExchange):
        def fetch_ohlcv(
            self,
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
            params: dict | None = None,
        ) -> list[list[float]]:
            raise RuntimeError("provider down")

    config = _configure(tmp_path)
    _register(FailingExchange())
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    # Make the failed-run recording itself fail by breaking Catalog.open
    original_open = Catalog.open

    call_count = 0

    @classmethod
    def failing_open(cls, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:  # Let Phase 1 succeed, break the failed-run recording
            raise OSError("disk full")
        return original_open(*args, **kwargs)

    monkeypatch.setattr(Catalog, "open", failing_open)

    with pytest.raises(ProviderError, match="provider down") as exc_info:
        bars.sync(_now(0), _now(3))

    # Original exception preserved, secondary failure noted
    assert exc_info.value.__notes__ is not None
    assert any("failed to record ingestion run" in note for note in exc_info.value.__notes__)


def test_sync_records_failed_run_on_pre_publication_phase3_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Phase 3 failure before any publish still records failed."""
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))

    def failing_publish(*args, **kwargs):
        raise OSError("filesystem error before replace")

    monkeypatch.setattr(dataset, "publish_prepared_file", failing_publish)

    with pytest.raises(OSError, match="filesystem error"):
        bars.sync(_now(0), _now(3))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        runs = catalog.list_ingestion_run_ids(
            DatasetKey.from_identity(bars.identity, timeframe="1h")
        )
    assert len(runs) == 1
    status, completed_at = _run_status(config, runs[0])
    assert status == "failed"
    assert completed_at is not None
    assert not list(config.data_dir.rglob("*.parquet"))


def test_uncertain_terminal_commit_uses_one_read_only_visibility_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    original_transaction = Catalog.transaction
    original_record_ingestion_run = Catalog.record_ingestion_run
    original_visibility_check = catalog_storage.terminal_commit_is_visible
    visibility_checks = 0
    terminal_recorded = False

    def mark_terminal_record(self: Catalog, run: IngestionRunMetadata) -> None:
        nonlocal terminal_recorded
        original_record_ingestion_run(self, run)
        if run.status == "completed":
            terminal_recorded = True

    @contextmanager
    def commit_then_report_uncertain(self: Catalog):
        is_root_transaction = not self.connection.in_transaction
        with original_transaction(self):
            yield
        if is_root_transaction and terminal_recorded:
            raise _CommitUncertainCatalogError("injected post-commit failure")

    def count_visibility_checks(
        db_path: Path, run: IngestionRunMetadata, expected_files: tuple[tuple[str, str], ...]
    ) -> bool:
        nonlocal visibility_checks
        visibility_checks += 1
        return original_visibility_check(db_path, run, expected_files)

    monkeypatch.setattr(Catalog, "record_ingestion_run", mark_terminal_record)
    monkeypatch.setattr(Catalog, "transaction", commit_then_report_uncertain)
    monkeypatch.setattr(catalog_storage, "terminal_commit_is_visible", count_visibility_checks)

    bars = _bars(_market_data(config))
    result = bars.sync(_now(0), _now(3))

    assert visibility_checks == 1
    assert result.is_complete
    assert result.changed
    assert result.written_partitions == 1
    assert len(exchange.fetch_calls) == 1
    assert bars.scan(_now(0), _now(3)).collect().height == 3
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert len(catalog.list_files()) == 1


def test_uncertain_terminal_commit_fails_when_terminal_facts_are_not_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    original_transaction = Catalog.transaction
    original_record_ingestion_run = Catalog.record_ingestion_run
    original_visibility_check = catalog_storage.terminal_commit_is_visible
    terminal_recorded = False
    visibility_checks = 0

    def mark_terminal_record(self: Catalog, run: IngestionRunMetadata) -> None:
        nonlocal terminal_recorded
        original_record_ingestion_run(self, run)
        if run.status == "completed":
            terminal_recorded = True

    @contextmanager
    def rollback_then_report_uncertain(self: Catalog):
        is_root_transaction = not self.connection.in_transaction
        with original_transaction(self):
            yield
            if is_root_transaction and terminal_recorded:
                raise _CommitUncertainCatalogError("injected pre-commit failure")

    def count_visibility_checks(
        db_path: Path, run: IngestionRunMetadata, expected_files: tuple[tuple[str, str], ...]
    ) -> bool:
        nonlocal visibility_checks
        visibility_checks += 1
        return original_visibility_check(db_path, run, expected_files)

    monkeypatch.setattr(Catalog, "record_ingestion_run", mark_terminal_record)
    monkeypatch.setattr(Catalog, "transaction", rollback_then_report_uncertain)
    monkeypatch.setattr(catalog_storage, "terminal_commit_is_visible", count_visibility_checks)

    with pytest.raises(
        SyncError, match=r"catalog update failed after publishing canonical Parquet data"
    ):
        _bars(_market_data(config)).sync(_now(0), _now(3))

    assert visibility_checks == 1


def test_uncertain_terminal_commit_without_publication_uses_one_visibility_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=[])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    original_transaction = Catalog.transaction
    original_record_ingestion_run = Catalog.record_ingestion_run
    original_visibility_check = catalog_storage.terminal_commit_is_visible
    terminal_recorded = False
    visibility_checks = 0

    def mark_terminal_record(self: Catalog, run: IngestionRunMetadata) -> None:
        nonlocal terminal_recorded
        original_record_ingestion_run(self, run)
        if run.status == "completed":
            terminal_recorded = True

    @contextmanager
    def commit_then_report_uncertain(self: Catalog):
        is_root_transaction = not self.connection.in_transaction
        with original_transaction(self):
            yield
        if is_root_transaction and terminal_recorded:
            raise _CommitUncertainCatalogError("injected post-commit failure")

    def count_visibility_checks(
        db_path: Path, run: IngestionRunMetadata, expected_files: tuple[tuple[str, str], ...]
    ) -> bool:
        nonlocal visibility_checks
        visibility_checks += 1
        assert expected_files == ()
        return original_visibility_check(db_path, run, expected_files)

    monkeypatch.setattr(Catalog, "record_ingestion_run", mark_terminal_record)
    monkeypatch.setattr(Catalog, "transaction", commit_then_report_uncertain)
    monkeypatch.setattr(catalog_storage, "terminal_commit_is_visible", count_visibility_checks)

    result = _bars(_market_data(config)).sync(_now(0), _now(3))

    assert visibility_checks == 1
    assert result.changed
    assert result.written_partitions == 0
    assert {gap.status for gap in result.gaps} == {CoverageStatus.UNAVAILABLE}


def test_later_month_publication_failure_leaves_only_prior_month_canonical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    start = datetime(2024, 1, 31, 23, tzinfo=UTC)
    end = datetime(2024, 2, 1, 1, tzinfo=UTC)
    exchange = FakeExchange(candles=[_row(743), _row(744)])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: datetime(2024, 3, 1, tzinfo=UTC))
    original_publish = dataset.publish_prepared_file
    publish_calls = 0

    def fail_second_publication(prepared):
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == 2:
            raise CatalogError("injected later publication failure")
        return original_publish(prepared)

    monkeypatch.setattr(dataset, "publish_prepared_file", fail_second_publication)
    bars = _bars(_market_data(config))
    key = DatasetKey.from_identity(bars.identity, timeframe=bars.timeframe)

    with pytest.raises(
        SyncError, match=r"catalog update failed after publishing canonical Parquet data"
    ):
        bars.sync(start, end)

    assert publish_calls == 2
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_coverage(key) == ()
        assert catalog.list_files() == ()
    assert len(list(config.data_dir.rglob("*.parquet"))) == 1
    with pytest.raises(CoverageError):
        bars.scan(start, end)
    partial = bars.scan_partial(start, end)
    assert partial.data.collect().height == 0
    assert {(gap.start, gap.end, gap.status) for gap in partial.gaps} == {
        (start, end, CoverageStatus.MISSING)
    }


def test_catalog_failure_after_publication_fails_closed_and_requires_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))

    def fail_record_file(self: Catalog, metadata: object, *, run_id: str | None = None) -> None:
        raise CatalogError("injected catalog failure")

    monkeypatch.setattr(Catalog, "record_file", fail_record_file)

    bars = _bars(_market_data(config))
    with pytest.raises(
        SyncError, match=r"publishing canonical Parquet data; run maintenance\.validate\(\)"
    ) as error:
        bars.sync(_now(0), _now(3))

    assert isinstance(error.value.__cause__, CatalogError)
    assert any(config.data_dir.rglob("data.parquet"))

    validation = _market_data(config).maintenance.validate()
    assert not validation.is_valid
    assert any("orphan canonical file not indexed" in issue for issue in validation.issues)

    db_path = config.state_dir / CATALOG_FILE_NAME
    with Catalog.open(db_path) as catalog:
        assert catalog.list_files() == ()


# --------------------------------------------------------------------------
# explicit `MarketData(config=...)` threading, without the module seam
# --------------------------------------------------------------------------


def test_explicit_config_is_honored_without_the_module_override_seam(tmp_path: Path) -> None:
    # Deliberately skip `_configure`/`_set_config_override`: only
    # `MarketData(config=...)` threaded through `BarDataset` may determine
    # where this sync reads/writes.
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    result = _bars(_market_data(config)).sync(_now(0), _now(3))

    assert result.changed
    assert result.fetched_rows == 3
    assert (config.state_dir / CATALOG_FILE_NAME).is_file()
    assert any(config.data_dir.rglob("data.parquet"))


# --------------------------------------------------------------------------
# perpetual sync with omitted settle: safe resolution before `DatasetKey`
# --------------------------------------------------------------------------


def test_perpetual_sync_infers_omitted_settle_before_dataset_key(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(
        client_id="binanceusdm", markets=_PERP_MARKETS, candles=_hourly_candles(range(0, 3))
    )
    _register_perp(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    result = _perp_bars(_market_data(config)).sync(_now(0), _now(3))

    assert result.is_complete
    assert result.changed
    assert result.dataset_key.settle == "USDT"


def test_perpetual_sync_with_ambiguous_settle_raises_unsupported_market_error(
    tmp_path: Path,
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(
        client_id="binanceusdm",
        markets={
            **_PERP_MARKETS,
            "BTC/USDT:USDC": {
                "id": "BTCUSDC",
                "symbol": "BTC/USDT:USDC",
                "base": "BTC",
                "quote": "USDT",
                "settle": "USDC",
                "swap": True,
                "contract": True,
                "linear": True,
                "inverse": False,
            },
        },
    )
    _register_perp(exchange)

    with pytest.raises(UnsupportedMarketError):
        _perp_bars(_market_data(config)).sync(_now(0), _now(3))


# --------------------------------------------------------------------------
# local perpetual read behavior: locally resolvable key, or a deliberate
# `InvalidRequestError` -- never leaked storage-layer sentinel error
# --------------------------------------------------------------------------


def test_local_perpetual_read_without_settle_or_local_coverage_raises_invalid_request(
    tmp_path: Path,
) -> None:
    config = _configure(tmp_path)
    bars = _perp_bars(_market_data(config))

    with pytest.raises(InvalidRequestError):
        bars.scan(_now(0), _now(3))
    with pytest.raises(InvalidRequestError):
        bars.scan_partial(_now(0), _now(3))


def test_local_perpetual_read_resolves_settle_from_local_catalog(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(
        client_id="binanceusdm", markets=_PERP_MARKETS, candles=_hourly_candles(range(0, 3))
    )
    _register_perp(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    _perp_bars(_market_data(config)).sync(_now(0), _now(3))

    read_bars = _perp_bars(_market_data(config))
    collected = read_bars.scan(_now(0), _now(3)).collect()
    assert collected.height == 3
    assert (collected["settle"] == "USDT").all()

    partial = read_bars.scan_partial(_now(0), _now(3))
    assert partial.is_complete
    assert partial.data.collect().height == 3


def test_local_perpetual_scans_infer_settle_without_mutating_catalog(
    tmp_path: Path,
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(
        client_id="binanceusdm", markets=_PERP_MARKETS, candles=_hourly_candles(range(0, 3))
    )
    _register_perp(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    _perp_bars(_market_data(config)).sync(_now(0), _now(3))

    db_path = config.state_dir / CATALOG_FILE_NAME
    before = db_path.read_bytes()

    bars = _perp_bars(_market_data(config))
    assert bars.scan(_now(0), _now(3)).collect().height == 3
    assert bars.scan_partial(_now(0), _now(3)).data.collect().height == 3

    assert db_path.read_bytes() == before


def test_read_only_catalog_snapshot_uses_coherent_active_wal_state(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    _bars(_market_data(config)).sync(_now(0), _now(3))

    db_path = config.state_dir / CATALOG_FILE_NAME
    writer = sqlite3.connect(db_path, isolation_level=None)
    try:
        writer.execute("PRAGMA journal_mode = WAL")
        writer.execute("PRAGMA wal_autocheckpoint = 0")
        settle = writer.execute("SELECT settle FROM datasets LIMIT 1").fetchone()[0]
        baseline_count = writer.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            """
            INSERT INTO datasets (
                exchange, symbol, market, settle, timeframe, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("committed", "BTC/USDT", "spot", settle, "1m", "now", "now"),
        )
        writer.execute("COMMIT")
        wal_path = db_path.with_name(db_path.name + "-wal")
        assert wal_path.stat().st_size > 32

        with Catalog.open_read_only(db_path) as snapshot:
            assert (
                snapshot.connection.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
                == baseline_count + 1
            )

        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            """
            INSERT INTO datasets (
                exchange, symbol, market, settle, timeframe, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("uncommitted", "BTC/USDT", "spot", settle, "1m", "now", "now"),
        )
        with Catalog.open_read_only(db_path) as snapshot:
            assert (
                snapshot.connection.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
                == baseline_count + 1
            )
    finally:
        if writer.in_transaction:
            writer.execute("ROLLBACK")
        writer.close()


# --------------------------------------------------------------------------
# provider domain taxonomy: unsupported capability/listing/timeframe raise
# `UnsupportedMarketError`, never `ProviderError`
# --------------------------------------------------------------------------


def test_unlisted_symbol_raises_unsupported_market_error(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(markets={})
    _register(exchange)

    with pytest.raises(UnsupportedMarketError):
        _bars(_market_data(config)).fetch(_now(0), _now(1))


def test_unsupported_timeframe_raises_unsupported_market_error(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 1)), timeframes={"5m": "5m"})
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(5))

    with pytest.raises(UnsupportedMarketError):
        _bars(_market_data(config)).fetch(_now(0), _now(1))


# --------------------------------------------------------------------------
# maintenance.validate() never mutates an empty (or absent) store
# --------------------------------------------------------------------------


def test_maintenance_validate_on_empty_store_creates_nothing(tmp_path: Path) -> None:
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    result = _market_data(config).maintenance.validate()

    assert result.is_valid
    assert not config.state_dir.exists()
    assert not config.data_dir.exists()


def test_unaligned_sync_fails_before_provider_or_storage(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    bars = _bars(_market_data(config))

    with pytest.raises(InvalidRequestError, match="must align"):
        bars.sync(_now(0) + timedelta(minutes=30), _now(2))

    assert exchange.fetch_calls == []
    assert not config.state_dir.exists()
    assert not config.data_dir.exists()


def test_zero_publication_catalog_fault_rolls_back_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=[])
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    original = Catalog.record_ingestion_run

    def fail_terminal(self: Catalog, run) -> None:
        if run.status == "completed":
            raise CatalogError("injected terminal transaction failure")
        original(self, run)

    monkeypatch.setattr(Catalog, "record_ingestion_run", fail_terminal)
    bars = _bars(_market_data(config))

    with pytest.raises(CatalogError, match="terminal transaction"):
        bars.sync(_now(0), _now(3))

    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_coverage(DatasetKey.from_identity(bars.identity, timeframe="1h")) == ()


def test_sync_reinserts_run_deleted_by_rebuild_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    market_data = _market_data(config)
    original_gate = dataset.locking.catalog_gate
    gate_calls = 0

    def rebuild_before_publication_gate(state_dir: Path):
        nonlocal gate_calls
        gate_calls += 1
        if gate_calls == 2:
            market_data.maintenance.rebuild_catalog()
        return original_gate(state_dir)

    monkeypatch.setattr(dataset.locking, "catalog_gate", rebuild_before_publication_gate)
    result = _bars(market_data).sync(_now(0), _now(3))

    assert result.is_complete
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.list_ingestion_run_ids(result.dataset_key) == (result.run_id,)


def test_sync_fails_before_publication_when_rebuilt_run_identity_conflicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    market_data = _market_data(config)
    bars = _bars(market_data)
    key = DatasetKey.from_identity(bars.identity, timeframe="1h")
    original_gate = dataset.locking.catalog_gate
    gate_calls = 0

    def inject_conflict_before_publication(state_dir: Path):
        nonlocal gate_calls
        gate_calls += 1
        if gate_calls == 2:
            with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
                run_id = catalog.connection.execute(
                    "SELECT run_id FROM ingestion_runs WHERE status = 'running'"
                ).fetchone()["run_id"]
            market_data.maintenance.rebuild_catalog()
            with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
                catalog.record_ingestion_run(
                    IngestionRunMetadata(
                        run_id=run_id,
                        dataset_key=key,
                        requested_start=_now(0),
                        requested_end=_now(4),
                        started_at=_now(10),
                        schema_version=SCHEMA_VERSION,
                        status="running",
                    )
                )
        return original_gate(state_dir)

    monkeypatch.setattr(dataset.locking, "catalog_gate", inject_conflict_before_publication)

    with pytest.raises(CatalogError, match="different immutable identity"):
        bars.sync(_now(0), _now(3))
    assert not list(config.data_dir.rglob("*.parquet"))


def test_sync_refuses_managed_data_when_catalog_is_absent(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    orphan = config.data_dir / "orphan" / "data.parquet"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"managed evidence")
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)

    with pytest.raises(CatalogError, match="managed storage evidence"):
        _bars(_market_data(config)).sync(_now(0), _now(3))

    assert exchange.fetch_calls == []
    assert not (config.state_dir / CATALOG_FILE_NAME).exists()


def test_scans_fail_closed_when_available_catalog_file_is_missing(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 3)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))
    bars = _bars(_market_data(config))
    bars.sync(_now(0), _now(3))
    next(config.data_dir.rglob("*.parquet")).unlink()

    with pytest.raises(CatalogError, match="missing canonical file"):
        bars.scan(_now(0), _now(3))
    with pytest.raises(CatalogError, match="missing canonical file"):
        bars.scan_partial(_now(0), _now(3))


def test_scans_refuse_ambiguous_managed_storage_when_catalog_is_absent(tmp_path: Path) -> None:
    config = _configure(tmp_path)
    ambiguous = config.data_dir / ".data.parquet.tmp-interrupted"
    ambiguous.parent.mkdir(parents=True)
    ambiguous.write_bytes(b"incomplete managed publication")
    bars = _bars(_market_data(config))

    with pytest.raises(
        CatalogError, match="catalog is absent while managed storage evidence exists"
    ):
        bars.scan(_now(0), _now(3))
    with pytest.raises(
        CatalogError, match="catalog is absent while managed storage evidence exists"
    ):
        bars.scan_partial(_now(0), _now(3))
    assert not config.state_dir.exists()


# --------------------------------------------------------------------------
# Gap coalescing (sparse rebuild revalidation)
# --------------------------------------------------------------------------


def _gap(start_hour: int, end_hour: int) -> CoverageInterval:
    return CoverageInterval(_now(start_hour), _now(end_hour), CoverageStatus.MISSING)


def test_coalesce_empty() -> None:
    bar = TimeBar.parse("1h")
    assert _coalesce_fetch_ranges([], bar) == ()


def test_coalesce_single_gap() -> None:
    bar = TimeBar.parse("1h")
    gaps = [_gap(2, 3)]
    result = _coalesce_fetch_ranges(gaps, bar)
    assert len(result) == 1
    assert result[0].start == _now(2)
    assert result[0].end == _now(3)
    assert len(result[0].gaps) == 1


def test_coalesce_adjacent_gaps() -> None:
    bar = TimeBar.parse("1h")
    gaps = [_gap(1, 2), _gap(3, 4), _gap(5, 6)]
    result = _coalesce_fetch_ranges(gaps, bar, max_window_bars=10)
    assert len(result) == 1
    assert result[0].start == _now(1)
    assert result[0].end == _now(6)
    assert len(result[0].gaps) == 3


def test_coalesce_respects_max_window() -> None:
    bar = TimeBar.parse("1h")
    gaps = [_gap(0, 1), _gap(5, 6), _gap(10, 11)]
    result = _coalesce_fetch_ranges(gaps, bar, max_window_bars=6)
    assert len(result) == 2
    assert result[0].start == _now(0)
    assert result[0].end == _now(6)
    assert result[1].start == _now(10)
    assert result[1].end == _now(11)


def test_coalesce_exact_limit() -> None:
    bar = TimeBar.parse("1h")
    gaps = [_gap(0, 1), _gap(5, 6)]
    result = _coalesce_fetch_ranges(gaps, bar, max_window_bars=6)
    assert len(result) == 1
    assert result[0].end == _now(6)


def test_coalesce_keeps_oversized_single_gap() -> None:
    bar = TimeBar.parse("1h")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    gap = CoverageInterval(base, base + timedelta(hours=100), CoverageStatus.MISSING)
    result = _coalesce_fetch_ranges([gap], bar, max_window_bars=10)
    assert len(result) == 1
    assert result[0].start == base
    assert result[0].end == base + timedelta(hours=100)


def test_sparse_rebuild_resync_fewer_fetch_calls(tmp_path: Path) -> None:
    """After rebuild, sparse gaps are coalesced into fewer provider requests."""
    config = _configure(tmp_path)
    # Sparse: even hours only (0, 2, 4, 6, 8)
    exchange = FakeExchange(candles=_hourly_candles(range(0, 10, 2)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(20))
    bars = _bars(_market_data(config))

    # Initial sync
    result = bars.sync(_now(0), _now(10))
    assert result.fetched_rows == 5

    # Rebuild — loses UNAVAILABLE
    md = _market_data(config)
    md.maintenance.rebuild_catalog()

    # Re-sync — should coalesce gaps
    exchange.fetch_calls.clear()
    result2 = bars.sync(_now(0), _now(10))
    coalesced_calls = len(exchange.fetch_calls)

    # Without coalescing: 5 gaps → 5 fetch calls
    # With coalescing: 1 wide request [1, 10) → 1 fetch call
    assert coalesced_calls == 1, f"expected 1 coalesced call, got {coalesced_calls}"
    # Provider returns 4 bridging rows (hours 2,4,6,8) in the wide window
    assert result2.fetched_rows == 4


def test_coalesced_resync_preserves_available_rows(tmp_path: Path) -> None:
    """Coalesced re-sync must NOT overwrite existing AVAILABLE row values."""
    config = _configure(tmp_path)
    # Initial: even hours with price=100
    exchange = FakeExchange(candles=_hourly_candles(range(0, 10, 2)))
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(20))
    bars = _bars(_market_data(config))
    bars.sync(_now(0), _now(10))

    # Read original Parquet values

    key = DatasetKey.from_identity(
        MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot"),
        timeframe="1h",
    )
    ym = YearMonth(year=2024, month=1)
    parquet_path = storage_paths.month_file_path(config.data_dir, key, ym)
    original = read_month_file(parquet_path)
    original_closes = dict(
        zip(
            original.get_column("timestamp").to_list(),
            original.get_column("close").to_list(),
            strict=True,
        )
    )

    # Rebuild
    md = _market_data(config)
    md.maintenance.rebuild_catalog()

    # Change provider to return DIFFERENT prices for existing bars
    exchange._candles = sorted(
        [_row(h, price=999.0) for h in range(0, 10, 2)]
        + [_row(h, price=50.0) for h in range(1, 10, 2)],
        key=lambda row: row[0],
    )

    # Re-sync with coalescing
    bars.sync(_now(0), _now(10))

    # Verify: original AVAILABLE rows must keep their original values
    updated = read_month_file(parquet_path)
    updated_closes = dict(
        zip(
            updated.get_column("timestamp").to_list(),
            updated.get_column("close").to_list(),
            strict=True,
        )
    )
    for ts, original_close in original_closes.items():
        assert updated_closes[ts] == original_close, (
            f"AVAILABLE row at {ts} was overwritten: {original_close} → {updated_closes[ts]}"
        )

    # New rows from previously MISSING gaps should exist with correct values
    assert updated.height == 10
    for hour in range(1, 10, 2):
        ts = _now(hour)
        assert updated_closes[ts] == 50.5, f"missing row at {ts} not stored correctly"


def test_coalesced_resync_does_not_promote_bridged_unavailable(
    tmp_path: Path,
) -> None:
    """Bridging UNAVAILABLE bars must not be promoted to AVAILABLE without storage."""
    config = _configure(tmp_path)
    exchange = FakeExchange(
        candles=[
            _row(0),
            _row(1, price=999.0),
            _row(2),
        ]
    )
    _register(exchange)
    provider_runtime._set_clock_override(lambda: _now(10))
    dataset._set_clock_override(lambda: _now(10))

    bars = _bars(_market_data(config))
    key = DatasetKey.from_identity(bars.identity, timeframe=bars.timeframe)

    # Seed only the middle interval as UNAVAILABLE.
    # [0,1) and [2,3) remain implicit MISSING (no coverage record).
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        catalog.apply_coverage(
            key,
            CoverageSegment(_now(1), _now(2), CoverageStatus.UNAVAILABLE),
        )

    result = bars.sync(_now(0), _now(3))

    # Two MISSING gaps coalesced into one wide request [0,3)
    assert len(exchange.fetch_calls) == 1
    assert result.fetched_rows == 3

    ym = YearMonth(year=2024, month=1)
    parquet_path = storage_paths.month_file_path(config.data_dir, key, ym)
    frame = read_month_file(parquet_path)
    timestamps = set(frame.get_column("timestamp").to_list())

    # Hours 0, 2 were MISSING → provider returned data → stored
    assert timestamps == {_now(0), _now(2)}

    # Hour 1 was UNAVAILABLE (bridging) → NOT stored, NOT promoted
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        covered, gaps = catalog.coverage_and_gaps(key, _now(0), _now(3))

    assert {(item.start, item.end) for item in covered} == {
        (_now(0), _now(1)),
        (_now(2), _now(3)),
    }
    assert {(gap.start, gap.end, gap.status) for gap in gaps} == {
        (_now(1), _now(2), CoverageStatus.UNAVAILABLE),
    }


def test_coalesce_rejects_nonpositive_window_budget() -> None:
    bar = TimeBar.parse("1h")
    with pytest.raises(ValueError, match="positive"):
        _coalesce_fetch_ranges([_gap(0, 1)], bar, max_window_bars=0)
    with pytest.raises(ValueError, match="positive"):
        _coalesce_fetch_ranges([_gap(0, 1)], bar, max_window_bars=-1)
