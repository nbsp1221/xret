from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier

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
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_FUNDING_SCHEMA,
    FundingObservation,
    FundingRequest,
    ObservedWindow,
    ProviderDescriptor,
    ResolvedFundingMarket,
)
from xret.data.providers.ccxt import CcxtProvider, funding_pagination
from xret.data.providers.ccxt.funding_pagination import paginate_funding_history
from xret.data.providers.funding_runtime import FundingProviderRuntime
from xret.data.schema import SETTLED_FUNDING_SCHEMA
from xret.data.storage import catalog as catalog_storage
from xret.data.storage.catalog import (
    Catalog,
    IngestionRunMetadata,
    _CommitUncertainCatalogError,
)


def _at(day: int, hour: int = 0, millisecond: int = 0) -> datetime:
    return datetime(2024, 1, day, hour, 0, 0, millisecond * 1000, tzinfo=UTC)


def _provider_frame(
    rows: list[tuple[datetime, float, int | None, float | None]],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "effective_at": [row[0] for row in rows],
            "funding_rate": [row[1] for row in rows],
            "funding_interval_seconds": [row[2] for row in rows],
            "mark_price": [row[3] for row in rows],
        },
        schema=PROVIDER_FUNDING_SCHEMA,
    )


class FundingProvider:
    descriptor = ProviderDescriptor("funding-fixture", "1", PROVIDER_API_VERSION)

    def __init__(
        self,
        rows: list[tuple[datetime, float, int | None, float | None]],
        *,
        partial: bool = False,
    ) -> None:
        self.rows = rows
        self.partial = partial
        self.resolve_calls = 0
        self.observe_calls = 0

    def resolve_funding_market(self, identity):
        self.resolve_calls += 1
        if identity.settle is None:
            from dataclasses import replace

            identity = replace(identity, settle="USDT")
        return ResolvedFundingMarket(identity, "BTCUSDT", "BTC/USDT:USDT")

    def observe_funding(
        self, request: FundingRequest, market: ResolvedFundingMarket
    ) -> FundingObservation:
        self.observe_calls += 1
        end = request.start + (request.end - request.start) / 2 if self.partial else request.end
        rows = [row for row in self.rows if request.start <= row[0] < end]
        return FundingObservation(
            _provider_frame(rows),
            (ObservedWindow(request.start, end),),
        )


def _dataset(tmp_path: Path, provider: object):
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    return (
        MarketData(config=config, provider=provider).settled_funding(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
        ),
        config,
    )


def test_binding_is_io_free_and_funding_only_provider_is_admitted(tmp_path: Path) -> None:
    provider = FundingProvider([])
    dataset, config = _dataset(tmp_path, provider)
    assert provider.resolve_calls == provider.observe_calls == 0
    assert dataset.identity.settle == "USDT"
    assert not config.state_dir.exists()
    assert not config.data_dir.exists()


def test_fetch_round_trips_signed_rates_exact_timestamps_and_nullable_fields(
    tmp_path: Path,
) -> None:
    rows = [
        (_at(1, 1, 123), -0.0001, None, None),
        (_at(1, 7), 0.0, 14_400, 42_000.0),
        (_at(1, 19), 0.0002, 43_200, None),
    ]
    dataset, config = _dataset(tmp_path, FundingProvider(rows))
    result = dataset.fetch(_at(1), _at(2)).require_complete()
    assert result.data.schema == SETTLED_FUNDING_SCHEMA
    assert result.data.get_column("effective_at").to_list() == [row[0] for row in rows]
    assert result.data.get_column("funding_rate").to_list() == [row[1] for row in rows]
    assert result.data.get_column("funding_interval_seconds").to_list() == [
        None,
        14_400,
        43_200,
    ]
    assert result.data.get_column("mark_price").to_list() == [None, 42_000.0, None]
    assert len(result.sources) == 1
    assert not config.state_dir.exists() and not config.data_dir.exists()


def test_funding_observation_normalizations_are_strict_and_positional_compatible() -> None:
    observation = FundingObservation(_provider_frame([]), ())
    assert observation.normalizations == ()
    for invalid in (["x"], ("",), (1,)):
        with pytest.raises(InvalidRequestError, match="normalizations"):
            FundingObservation(
                _provider_frame([]),
                (),
                normalizations=invalid,  # type: ignore[arg-type]
            )


def test_narrow_funding_scan_ignores_unrelated_corrupt_month_and_eventless_month(
    tmp_path: Path,
) -> None:
    rows = [
        (datetime(2024, 1, 15, tzinfo=UTC), -0.1, None, None),
        (datetime(2024, 3, 15, tzinfo=UTC), 0.1, None, None),
    ]
    dataset, config = _dataset(tmp_path, FundingProvider(rows))
    dataset.sync(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC)
    ).require_complete()
    january = next(
        path
        for path in config.data_dir.rglob("data.parquet")
        if "year=2024/month=01" in path.as_posix()
    )
    january.write_bytes(b"corrupt unrelated month")

    assert (
        dataset.scan(datetime(2024, 2, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC))
        .collect()
        .is_empty()
    )
    march = dataset.scan(
        datetime(2024, 3, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC)
    ).collect()
    assert march.get_column("effective_at").to_list() == [datetime(2024, 3, 15, tzinfo=UTC)]
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC))


def test_partial_fetch_exposes_unproved_tail(tmp_path: Path) -> None:
    dataset, _ = _dataset(tmp_path, FundingProvider([], partial=True))
    result = dataset.fetch(_at(1), _at(2))
    assert not result.is_complete
    assert result.covered[0].start == _at(1)
    assert result.gaps[0].end == _at(2)
    with pytest.raises(ProviderError, match="did not fully complete"):
        result.require_complete()


def test_eventless_sync_is_complete_strict_empty_and_idempotent(tmp_path: Path) -> None:
    provider = FundingProvider([])
    dataset, _ = _dataset(tmp_path, provider)
    first = dataset.sync(_at(1), _at(2)).require_complete()
    assert first.changed and first.fetched_rows == first.written_partitions == 0
    assert dataset.scan(_at(1), _at(2)).collect().schema == SETTLED_FUNDING_SCHEMA
    second = dataset.sync(_at(1), _at(2)).require_complete()
    assert not second.changed
    assert second.sources == ()
    assert provider.observe_calls == 1


def test_provider_failure_publishes_nothing_and_range_remains_retryable(
    tmp_path: Path,
) -> None:
    class FailingFundingProvider(FundingProvider):
        def observe_funding(self, request, market):
            self.observe_calls += 1
            raise RuntimeError("transport failed")

    failing, config = _dataset(tmp_path, FailingFundingProvider([]))
    with pytest.raises(ProviderError, match="failed to observe settled funding"):
        failing.sync(_at(1), _at(2))
    assert not tuple(config.data_dir.rglob("*.parquet"))
    partial = failing.scan_partial(_at(1), _at(2))
    assert not partial.covered and len(partial.gaps) == 1

    recovered, _ = _dataset(tmp_path, FundingProvider([(_at(1, 1), 0.1, None, None)]))
    assert recovered.sync(_at(1), _at(2)).require_complete().fetched_rows == 1


def test_month_partition_strict_partial_and_rebuild_loses_funding_completeness(
    tmp_path: Path,
) -> None:
    rows = [
        (datetime(2024, 1, 31, 23, 59, 59, 999000, tzinfo=UTC), -0.1, None, None),
        (datetime(2024, 2, 1, 0, 0, 0, 1000, tzinfo=UTC), 0.1, None, 1.0),
    ]
    dataset, config = _dataset(tmp_path, FundingProvider(rows))
    start = datetime(2024, 1, 31, tzinfo=UTC)
    end = datetime(2024, 2, 2, tzinfo=UTC)
    result = dataset.sync(start, end).require_complete()
    assert result.written_partitions == 2
    assert dataset.scan(start, end).collect().height == 2
    partial = dataset.scan_partial(start, end)
    assert partial.is_complete and partial.data.collect().height == 2

    db_path = config.state_dir / "catalog.sqlite3"
    for path in (
        db_path,
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ):
        path.unlink(missing_ok=True)
    MarketData(config=config).maintenance.rebuild_catalog()
    with pytest.raises(CoverageError):
        dataset.scan(start, end)
    rebuilt = dataset.scan_partial(start, end)
    assert not rebuilt.is_complete
    assert rebuilt.data.collect().is_empty()


def test_provider_without_funding_fails_before_family_io(tmp_path: Path) -> None:
    class TradeOnly:
        descriptor = ProviderDescriptor("trade-only", "1", PROVIDER_API_VERSION)

        def resolve_market(self, identity):
            raise AssertionError("must not resolve")

        def observe_bars(self, request, market):
            raise AssertionError("must not observe")

    dataset, _ = _dataset(tmp_path, TradeOnly())
    with pytest.raises(UnsupportedMarketError, match="no settled-funding capability"):
        dataset.fetch(_at(1), _at(2))


def test_catalog_failure_after_funding_publication_is_canonical_ahead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset, config = _dataset(
        tmp_path,
        FundingProvider([(_at(1, 1), 0.1, None, None)]),
    )
    original = Catalog.record_file

    def fail(self, metadata, *, run_id=None):
        original(self, metadata, run_id=run_id)
        raise RuntimeError("catalog fault")

    monkeypatch.setattr(Catalog, "record_file", fail)
    with pytest.raises(SyncError, match="after publishing canonical funding"):
        dataset.sync(_at(1), _at(2))
    assert tuple(config.data_dir.rglob("data.parquet"))


def test_funding_uncertain_terminal_commit_uses_visibility_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset, _ = _dataset(
        tmp_path,
        FundingProvider([(_at(1, 1), 0.1, None, None)]),
    )
    original_transaction = Catalog.transaction
    original_record = Catalog.record_ingestion_run
    original_visibility = catalog_storage.terminal_commit_is_visible
    terminal_recorded = False
    visibility_checks = 0

    def mark_terminal(self: Catalog, run: IngestionRunMetadata) -> None:
        nonlocal terminal_recorded
        original_record(self, run)
        terminal_recorded |= run.status == "completed"

    @contextmanager
    def commit_then_report(self: Catalog):
        is_root = not self.connection.in_transaction
        with original_transaction(self):
            yield
        if is_root and terminal_recorded:
            raise _CommitUncertainCatalogError("injected post-commit failure")

    def count_visibility(db_path, run, expected_files):
        nonlocal visibility_checks
        visibility_checks += 1
        return original_visibility(db_path, run, expected_files)

    monkeypatch.setattr(Catalog, "record_ingestion_run", mark_terminal)
    monkeypatch.setattr(Catalog, "transaction", commit_then_report)
    monkeypatch.setattr(catalog_storage, "terminal_commit_is_visible", count_visibility)

    result = dataset.sync(_at(1), _at(2)).require_complete()
    assert visibility_checks == 1
    assert result.written_partitions == 1
    assert dataset.scan(_at(1), _at(2)).collect().height == 1


def test_ccxt_unified_adapter_resolves_capability_and_fetches_exact_history(
    tmp_path: Path,
) -> None:
    class Exchange:
        id = "binanceusdm"
        has = {"fetchOHLCV": False, "fetchFundingRateHistory": True}
        markets = None
        precisionMode = 0
        timeframes = None

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

        def fetch_funding_rate_history(self, symbol, since=None, limit=None, params=None):
            assert symbol == "BTC/USDT:USDT"
            assert params == {"until": int(_at(2).timestamp() * 1000) - 1}
            record = {
                "timestamp": int(_at(1, 3, 17).timestamp() * 1000),
                "fundingRate": "-0.0002",
                "info": {"markPrice": "", "fundingIntervalHours": "4"},
            }
            return [record, dict(record)]

    exchange = Exchange()
    provider = CcxtProvider(
        exchange_factory=lambda _client_id: exchange,
        version_provider=lambda: "fixture",
        page_limit=100,
        sleep=lambda _seconds: None,
    )
    dataset, _ = _dataset(tmp_path, provider)
    result = dataset.fetch(_at(1), _at(2)).require_complete()
    assert result.data.get_column("effective_at").to_list() == [_at(1, 3, 17)]
    assert result.data.get_column("funding_rate").to_list() == [-0.0002]
    assert result.data.get_column("funding_interval_seconds").to_list() == [14_400]
    assert result.data.get_column("mark_price").to_list() == [None]
    assert result.sources[0].normalizations == ("funding.identical_duplicate_dedup",)


def test_concurrent_ccxt_funding_fetches_keep_call_specific_dedupe_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Exchange:
        id = "binanceusdm"
        has = {"fetchOHLCV": False, "fetchFundingRateHistory": True}
        markets = None
        precisionMode = 0
        timeframes = None

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

    rendezvous = Barrier(2)

    class CoordinatedProvider(CcxtProvider):
        def observe_funding(self, request, market):
            observation = super().observe_funding(request, market)
            rendezvous.wait(timeout=5)
            return observation

    def paginate(*, start, end, **_kwargs):
        duplicate_count = 1 if start.day == 1 else 0
        return funding_pagination.FundingPaginationResult(
            (), (ObservedWindow(start, end),), duplicate_count
        )

    monkeypatch.setattr(funding_pagination, "paginate_funding_history", paginate)
    provider = CoordinatedProvider(
        exchange_factory=lambda _client_id: Exchange(),
        version_provider=lambda: "fixture",
        sleep=lambda _seconds: None,
    )
    dataset, _ = _dataset(tmp_path, provider)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {day: executor.submit(dataset.fetch, _at(day), _at(day + 1)) for day in (1, 2)}
        results = {day: future.result() for day, future in futures.items()}

    assert results[1].sources[0].normalizations == ("funding.identical_duplicate_dedup",)
    assert results[2].sources[0].normalizations == ()


def test_ccxt_pagination_deduplicates_overlap_and_preserves_no_schedule() -> None:
    calls: list[int] = []
    pages = [
        [
            {"timestamp": 1000, "fundingRate": "-0.1", "markPrice": ""},
            {"timestamp": 7000, "fundingRate": "0", "fundingIntervalSeconds": "6"},
        ],
        [
            {"timestamp": 7000, "fundingRate": "0", "fundingIntervalSeconds": "6"},
            {"timestamp": 19000, "fundingRate": "0.2", "markPrice": "10"},
        ],
        [],
    ]

    def fetch(since, limit, params):
        calls.append(since)
        assert limit == 2 and params == {"until": 19_999}
        return pages[len(calls) - 1]

    result = paginate_funding_history(
        exchange_id="binanceusdm",
        start=datetime.fromtimestamp(0, tz=UTC),
        end=datetime.fromtimestamp(20, tz=UTC),
        page_limit=2,
        fetch_page=fetch,
    )
    assert [row[0] for row in result.rows] == [1000, 7000, 19000]
    assert result.duplicate_count == 1
    assert result.observed == (
        ObservedWindow(datetime.fromtimestamp(0, tz=UTC), datetime.fromtimestamp(20, tz=UTC)),
    )
    assert calls == [0, 7001, 19001]


def test_ccxt_pagination_rejects_conflict_ignored_bounds_and_no_progress() -> None:
    start = datetime.fromtimestamp(0, tz=UTC)
    end = datetime.fromtimestamp(20, tz=UTC)

    def conflict(since, limit, params):
        if since == 0:
            return [{"timestamp": 1000, "fundingRate": "0.1"}]
        return [{"timestamp": 1000, "fundingRate": "0.2"}]

    with pytest.raises(ProviderError, match="conflicting duplicates"):
        paginate_funding_history(
            exchange_id="binanceusdm",
            start=start,
            end=end,
            page_limit=1,
            fetch_page=conflict,
        )

    with pytest.raises(ProviderError, match="ignored requested bounds"):
        paginate_funding_history(
            exchange_id="binanceusdm",
            start=start,
            end=end,
            page_limit=10,
            fetch_page=lambda *_: [{"timestamp": 20_000, "fundingRate": "0.1"}],
        )

    with pytest.raises(ProviderError, match="made no progress"):
        paginate_funding_history(
            exchange_id="binanceusdm",
            start=start,
            end=end,
            page_limit=1,
            fetch_page=lambda *_: [{"timestamp": 1000, "fundingRate": "0.1"}],
        )


@pytest.mark.parametrize(
    "record",
    (
        {"timestamp": 1000, "fundingRate": float("nan")},
        {"timestamp": 1000, "fundingRate": float("inf")},
        {"timestamp": 1000, "fundingRate": "0.1", "markPrice": float("nan")},
        {"timestamp": 1000, "fundingRate": "0.1", "markPrice": 0},
        {"timestamp": 1000, "fundingRate": "0.1", "fundingIntervalSeconds": 0},
        {"timestamp": 1000, "fundingRate": "0.1", "fundingIntervalSeconds": 3600.5},
    ),
)
def test_ccxt_pagination_rejects_malformed_funding_values(record: dict[str, object]) -> None:
    with pytest.raises(ProviderError):
        paginate_funding_history(
            exchange_id="binanceusdm",
            start=datetime.fromtimestamp(0, tz=UTC),
            end=datetime.fromtimestamp(20, tz=UTC),
            page_limit=10,
            fetch_page=lambda *_: [record],
        )


def test_runtime_clips_funding_evidence_at_pre_call_instant() -> None:
    provider = FundingProvider([(_at(1, 13), 0.1, None, None)])
    dataset_identity = _dataset(Path("/tmp/unused-funding-runtime"), provider)[0].identity
    runtime = FundingProviderRuntime(provider, clock=lambda: _at(1, 12))
    request = FundingRequest(identity=dataset_identity, start=_at(1), end=_at(2))

    with pytest.raises(ProviderError, match="outside observed windows"):
        runtime.observe(request)

    empty = FundingProvider([])
    observation = FundingProviderRuntime(empty, clock=lambda: _at(1, 12)).observe(
        FundingRequest(identity=dataset_identity, start=_at(1), end=_at(2))
    )
    assert observation.observed == (ObservedWindow(_at(1), _at(1, 12)),)


def test_funding_canonical_ahead_revision_fails_all_local_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = FundingProvider([(_at(1, 1), 0.1, None, None), (_at(2, 1), 0.2, None, None)])
    dataset, _ = _dataset(tmp_path, provider)
    dataset.sync(_at(1), _at(2)).require_complete()
    original = Catalog.record_file

    def fail(self, metadata, *, run_id=None):
        original(self, metadata, run_id=run_id)
        raise RuntimeError("catalog fault")

    monkeypatch.setattr(Catalog, "record_file", fail)
    with pytest.raises(SyncError, match="after publishing canonical funding"):
        dataset.sync(_at(2), _at(3))
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan(_at(1), _at(2))
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan_partial(_at(1), _at(2))
