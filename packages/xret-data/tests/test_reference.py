from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from xret.data import MarketData
from xret.data.config import MarketDataConfig
from xret.data.errors import (
    CatalogError,
    CoverageError,
    InvalidRequestError,
    ProviderError,
    SyncError,
    UnsupportedMarketError,
)
from xret.data.models import ReferencePriceKind
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_REFERENCE_BAR_SCHEMA,
    ObservedWindow,
    ProviderDescriptor,
    ReferenceBarObservation,
    ReferenceBarRequest,
    ResolvedReferenceMarket,
)
from xret.data.providers.ccxt import CcxtProvider
from xret.data.providers.ccxt.reference_pagination import paginate_reference_history
from xret.data.schema import REFERENCE_BAR_SCHEMA
from xret.data.storage.catalog import Catalog
from xret.data.timeframe import TimeBar


def _at(day: int, hour: int = 0) -> datetime:
    return datetime(2024, 1, day, hour, tzinfo=UTC)


def _provider_frame(
    rows: list[tuple[datetime, float, float, float, float]],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": [row[0] for row in rows],
            "open": [row[1] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[3] for row in rows],
            "close": [row[4] for row in rows],
        },
        schema=PROVIDER_REFERENCE_BAR_SCHEMA,
    )


class ReferenceProvider:
    descriptor = ProviderDescriptor("reference-fixture", "1", PROVIDER_API_VERSION)

    def __init__(self, rows, *, observed: bool = True) -> None:
        self.rows = rows
        self.observed = observed
        self.resolve_calls = 0
        self.observe_calls = 0

    def resolve_reference_market(self, identity, kind):
        self.resolve_calls += 1
        if identity.settle is None:
            from dataclasses import replace

            identity = replace(identity, settle="USDT")
        pair_scoped = kind is ReferencePriceKind.INDEX
        return ResolvedReferenceMarket(
            identity,
            "BTC/USDT" if pair_scoped else "BTCUSDT",
            "BTC/USDT" if pair_scoped else "BTC/USDT:USDT",
            kind,
            frozenset({"1h"}),
            "pair" if pair_scoped else "contract",
        )

    def observe_reference_bars(
        self, request: ReferenceBarRequest, market: ResolvedReferenceMarket
    ) -> ReferenceBarObservation:
        self.observe_calls += 1
        rows = [row for row in self.rows if request.start <= row[0] < request.end]
        windows = (ObservedWindow(request.start, request.end),) if self.observed else ()
        return ReferenceBarObservation(_provider_frame(rows), windows)


def _dataset(tmp_path: Path, provider: object, *, kind: str = "mark"):
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    return (
        MarketData(config=config, provider=provider).reference_bars(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
            kind=kind,
            timeframe="1h",
        ),
        config,
    )


def test_binding_is_io_free_and_kind_is_closed(tmp_path: Path) -> None:
    provider = ReferenceProvider([])
    dataset, config = _dataset(tmp_path, provider, kind="premium_index")
    assert dataset.kind is ReferencePriceKind.PREMIUM_INDEX
    assert provider.resolve_calls == provider.observe_calls == 0
    assert not config.state_dir.exists() and not config.data_dir.exists()
    with pytest.raises(InvalidRequestError):
        _dataset(tmp_path, provider, kind="last")


def test_all_kinds_are_separate_no_volume_identities_and_signed_premium(tmp_path: Path) -> None:
    rows = [(_at(1), -0.1, 0.2, -0.2, 0.0)]
    premium, config = _dataset(tmp_path, ReferenceProvider(rows), kind="premium_index")
    result = premium.sync(_at(1), _at(1, 1)).require_complete()
    assert result.dataset_key.kind is ReferencePriceKind.PREMIUM_INDEX
    frame = premium.scan(_at(1), _at(1, 1)).collect()
    assert frame.schema == REFERENCE_BAR_SCHEMA and "volume" not in frame.columns
    assert frame.get_column("open").item() == -0.1

    for kind in ("mark", "index"):
        dataset, _ = _dataset(
            tmp_path,
            ReferenceProvider([(_at(1), 100.0, 101.0, 99.0, 100.5)]),
            kind=kind,
        )
        synced = dataset.sync(_at(1), _at(1, 1)).require_complete()
        assert synced.dataset_key.kind.value == kind
        assert dataset.scan(_at(1), _at(1, 1)).collect().height == 1
    expected_paths = {
        f"_xret/reference-bars/{kind}/binance/perpetual/BTC-USDT-USDT/1h/"
        "year=2024/month=01/data.parquet"
        for kind in ("mark", "index", "premium_index")
    }
    actual_paths = {
        path.relative_to(config.data_dir).as_posix()
        for path in config.data_dir.rglob("data.parquet")
    }
    assert actual_paths == expected_paths
    for kind, scope, native_symbol in (
        ("mark", "contract", "BTC/USDT:USDT"),
        ("index", "pair", "BTC/USDT"),
        ("premium_index", "contract", "BTC/USDT:USDT"),
    ):
        path = config.data_dir / next(item for item in expected_paths if f"/{kind}/" in item)
        metadata = pl.read_parquet_metadata(path)
        assert metadata["reference_target_scope"] == scope
        assert metadata["native_symbol"] == native_symbol


def test_fetch_sync_and_reads_preserve_io_boundaries(tmp_path: Path) -> None:
    provider = ReferenceProvider([(_at(1), 100.0, 101.0, 99.0, 100.5)])
    dataset, config = _dataset(tmp_path, provider)

    dataset.fetch(_at(1), _at(1, 1)).require_complete()
    assert provider.resolve_calls == provider.observe_calls == 1
    assert not config.state_dir.exists() and not config.data_dir.exists()

    dataset.sync(_at(1), _at(1, 1)).require_complete()
    remote_calls = (provider.resolve_calls, provider.observe_calls)
    dataset.scan(_at(1), _at(1, 1)).collect()
    dataset.scan_partial(_at(1), _at(1, 1)).data.collect()
    assert dataset.sync(_at(1), _at(1, 1)).changed is False
    assert (provider.resolve_calls, provider.observe_calls) == remote_calls


def test_reference_validation_rejects_bad_values_relations_and_grid(tmp_path: Path) -> None:
    cases = [
        [(_at(1), 0.0, 1.0, 0.0, 0.5)],
        [(_at(1), 1.0, float("inf"), 0.5, 1.0)],
        [(_at(1), 1.0, 0.9, 0.5, 1.0)],
        [(_at(1) + timedelta(minutes=1), 1.0, 1.1, 0.9, 1.0)],
        [(_at(1), 1.0, 1.1, 0.9, 1.0), (_at(1), 1.0, 1.1, 0.9, 1.0)],
        [(_at(1, 1), 1.0, 1.1, 0.9, 1.0), (_at(1), 1.0, 1.1, 0.9, 1.0)],
    ]
    for rows in cases:
        dataset, _ = _dataset(tmp_path, ReferenceProvider(rows))
        with pytest.raises(ProviderError):
            dataset.fetch(_at(1), _at(1, 2))
    dataset, _ = _dataset(tmp_path, ReferenceProvider([]))
    with pytest.raises(Exception, match="align"):
        dataset.fetch(_at(1) + timedelta(minutes=1), _at(1, 1))


def test_grid_gap_partial_retry_idempotence_month_and_rebuild(tmp_path: Path) -> None:
    rows = [
        (datetime(2024, 1, 31, 23, tzinfo=UTC), 100.0, 101.0, 99.0, 100.0),
        (datetime(2024, 2, 1, 0, tzinfo=UTC), 101.0, 102.0, 100.0, 101.0),
    ]
    dataset, config = _dataset(tmp_path, ReferenceProvider(rows))
    start, end = rows[0][0], datetime(2024, 2, 1, 1, tzinfo=UTC)
    first = dataset.sync(start, end).require_complete()
    assert first.written_partitions == 2
    assert dataset.sync(start, end).changed is False
    assert dataset.scan(start, end).collect().height == 2

    db = config.state_dir / "catalog.sqlite3"
    for path in (db, db.with_name(db.name + "-wal"), db.with_name(db.name + "-shm")):
        path.unlink(missing_ok=True)
    MarketData(config=config).maintenance.rebuild_catalog()
    assert dataset.scan(start, end).collect().height == 2

    gapped, _ = _dataset(tmp_path / "gap", ReferenceProvider([rows[0]]))
    partial = gapped.sync(start, end)
    assert not partial.is_complete
    assert partial.gaps[0].status.value == "unavailable"
    with pytest.raises(CoverageError):
        gapped.scan(start, end)
    assert gapped.scan_partial(start, end).data.collect().height == 1


def test_failure_and_catalog_fault_publish_contract(tmp_path: Path, monkeypatch) -> None:
    class Failing(ReferenceProvider):
        def observe_reference_bars(self, request, market):
            raise RuntimeError("transport")

    dataset, config = _dataset(tmp_path, Failing([]))
    with pytest.raises(ProviderError):
        dataset.sync(_at(1), _at(1, 1))
    assert not tuple(config.data_dir.rglob("*.parquet"))

    dataset, config = _dataset(
        tmp_path / "catalog", ReferenceProvider([(_at(1), 1.0, 1.1, 0.9, 1.0)])
    )
    original = Catalog.record_file

    def fail(self, metadata, *, run_id=None):
        original(self, metadata, run_id=run_id)
        raise RuntimeError("catalog fault")

    monkeypatch.setattr(Catalog, "record_file", fail)
    with pytest.raises(SyncError, match="after publishing canonical reference"):
        dataset.sync(_at(1), _at(1, 1))
    assert tuple(config.data_dir.rglob("data.parquet"))


@pytest.mark.parametrize(
    ("kind", "capability", "method"),
    (
        ("mark", "fetchMarkOHLCV", "fetch_mark_ohlcv"),
        ("index", "fetchIndexOHLCV", "fetch_index_ohlcv"),
        ("premium_index", "fetchPremiumIndexOHLCV", "fetch_premium_index_ohlcv"),
    ),
)
def test_ccxt_uses_each_pinned_convenience_route(kind, capability, method, tmp_path: Path) -> None:
    class Exchange:
        id = "binanceusdm"
        has = {capability: True}
        markets = None
        precisionMode = 0
        timeframes = {"1h": "1h"}

        def __init__(self):
            self.calls = []

        def load_markets(self, reload=False):
            return {
                "BTC/USDT:USDT": {
                    "id": "BTCUSDT",
                    "symbol": "BTC/USDT:USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "settle": "USDT",
                    "swap": True,
                    "linear": True,
                    "inverse": False,
                    "contractSize": 1,
                }
            }

    exchange = Exchange()

    def fetch(symbol, timeframe="1m", since=None, limit=None, params=None):
        exchange.calls.append((symbol, timeframe, since, limit, params))
        if len(exchange.calls) == 1:
            values = (-0.1, 0.1, -0.2, 0.0) if kind == "premium_index" else (1, 2, 0.5, 1.5)
            return [[int(_at(1).timestamp() * 1000), *values, 0]]
        return []

    setattr(exchange, method, fetch)
    provider = CcxtProvider(
        exchange_factory=lambda _client_id: exchange,
        version_provider=lambda: "fixture",
        page_limit=100,
        sleep=lambda _seconds: None,
    )
    dataset, _ = _dataset(tmp_path, provider, kind=kind)
    result = dataset.fetch(_at(1), _at(1, 1)).require_complete()
    assert result.data.schema == REFERENCE_BAR_SCHEMA and "volume" not in result.data.columns
    expected_symbol = "BTC/USDT" if kind == "index" else "BTC/USDT:USDT"
    assert exchange.calls[0] == (
        expected_symbol,
        "1h",
        int(_at(1).timestamp() * 1000),
        100,
        {},
    )
    if kind == "index":
        assert result.sources[0].native_symbol == "BTC/USDT"
        assert result.sources[0].normalizations == ("reference_target_scope.pair",)
        assert result.sources[0].reference_target_scope == "pair"


def test_ccxt_unsupported_and_duplicate_fail_strictly(tmp_path: Path) -> None:
    class Exchange:
        id = "binanceusdm"
        has = {"fetchOHLCV": True}
        markets = None
        precisionMode = 0
        timeframes = {"1h": "1h"}

        def load_markets(self, reload=False):
            return {
                "BTC/USDT:USDT": {
                    "id": "BTCUSDT",
                    "symbol": "BTC/USDT:USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "settle": "USDT",
                    "swap": True,
                }
            }

    exchange = Exchange()
    provider = CcxtProvider(exchange_factory=lambda _: exchange, version_provider=lambda: "x")
    dataset, _ = _dataset(tmp_path, provider)
    with pytest.raises(UnsupportedMarketError, match="fetchMarkOHLCV"):
        dataset.fetch(_at(1), _at(1, 1))


def test_reference_pagination_rejects_malformed_duplicate_conflict_and_order() -> None:
    start, end = _at(1), _at(1, 2)
    timestamp = int(start.timestamp() * 1000)
    cases = (
        [[timestamp, 1.0]],
        [
            [timestamp, 1.0, 1.1, 0.9, 1.0, 0],
            [timestamp, 1.0, 1.1, 0.9, 1.0, 0],
        ],
        [
            [timestamp, 1.0, 1.1, 0.9, 1.0, 0],
            [timestamp, 1.0, 1.2, 0.9, 1.0, 0],
        ],
        [
            [timestamp + 3_600_000, 1.0, 1.1, 0.9, 1.0, 0],
            [timestamp, 1.0, 1.1, 0.9, 1.0, 0],
        ],
    )
    for page in cases:
        with pytest.raises(ProviderError):
            paginate_reference_history(
                exchange_id="fixture",
                time_bar=TimeBar.parse("1h"),
                start=start,
                end=end,
                page_limit=100,
                fetch_page=lambda *_args, page=page: page,
            )


def test_runtime_rejects_misaligned_reference_observed_windows(tmp_path: Path) -> None:
    class Misaligned(ReferenceProvider):
        def observe_reference_bars(self, request, market):
            del market
            return ReferenceBarObservation(
                _provider_frame([]),
                (ObservedWindow(request.start + timedelta(minutes=1), request.end),),
            )

    dataset, _ = _dataset(tmp_path, Misaligned([]))
    with pytest.raises(ProviderError, match="observed windows must align"):
        dataset.fetch(_at(1), _at(1, 1))


def test_reference_canonical_ahead_revision_fails_all_local_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ReferenceProvider(
        [
            (_at(1), 1.0, 1.1, 0.9, 1.0),
            (_at(1, 1), 2.0, 2.1, 1.9, 2.0),
        ]
    )
    dataset, _ = _dataset(tmp_path, provider)
    dataset.sync(_at(1), _at(1, 1)).require_complete()
    original = Catalog.record_file

    def fail(self, metadata, *, run_id=None):
        original(self, metadata, run_id=run_id)
        raise RuntimeError("catalog fault")

    monkeypatch.setattr(Catalog, "record_file", fail)
    with pytest.raises(SyncError, match="after publishing canonical reference"):
        dataset.sync(_at(1, 1), _at(1, 2))
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan(_at(1), _at(1, 1))
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan_partial(_at(1), _at(1, 1))
