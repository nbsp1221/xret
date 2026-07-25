"""Recovery contracts: catalog is derived, Parquet is immutable."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from xret.data.config import MarketDataConfig
from xret.data.errors import CatalogError
from xret.data.models import (
    NONE_SETTLE_SENTINEL,
    CoverageStatus,
    DatasetKey,
    Market,
    YearMonth,
)
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage import parquet, paths
from xret.data.storage.catalog import (
    SCHEMA_VERSION as CATALOG_SCHEMA_VERSION,
)
from xret.data.storage.catalog import (
    Catalog,
    CoverageSegment,
    FileMetadata,
    IngestionRunMetadata,
)
from xret.data.storage.locking import catalog_gate
from xret.data.storage.recovery import (
    _available_segments,
    rebuild_catalog_state,
    validate_catalog_state,
)
from xret.data.timeframe import TimeBar


@dataclass(frozen=True, slots=True)
class _File:
    dataset_key: DatasetKey
    year_month: YearMonth
    relative_path: str
    absolute_path: Path
    row_count: int
    min_timestamp: datetime
    max_timestamp: datetime
    physical_hash: str
    schema_version: int = 4


@contextmanager
def _gate(_: Path):
    yield


def _key() -> DatasetKey:
    return DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.SPOT,
        settle=NONE_SETTLE_SENTINEL,
        timeframe="1h",
    )


def _file(
    tmp_path: Path,
    year_month: YearMonth | None = None,
    instant: datetime | None = None,
) -> _File:
    year_month = year_month or YearMonth(2024, 1)
    instant = instant or datetime(2024, 1, 1, tzinfo=UTC)
    frame = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["spot"],
            "settle": [None],
            "timeframe": ["1h"],
            "timestamp": [instant],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "volume": [10.0],
        },
        schema=OHLCV_SCHEMA,
    )
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(
            tmp_path / "data",
            _key(),
            year_month,
            frame,
            provider=parquet.ProviderIdentity("binance", "4.5.66", "BTCUSDT", "BTC/USDT"),
        )
    )
    return _File(
        committed.dataset_key,
        committed.year_month,
        committed.relative_path,
        committed.absolute_path,
        committed.row_count,
        committed.min_timestamp,
        committed.max_timestamp,
        committed.physical_hash,
    )


def _config(tmp_path: Path) -> MarketDataConfig:
    return MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")


def _record(db_path: Path, file: _File) -> None:
    with Catalog.open(db_path) as catalog:
        catalog.record_file(
            FileMetadata(
                file.dataset_key,
                file.relative_path,
                file.year_month.year,
                file.year_month.month,
                file.row_count,
                file.min_timestamp,
                file.max_timestamp,
                file.physical_hash,
                parquet.SCHEMA_VERSION,
            )
        )


def test_rebuild_recovers_after_sqlite_catalog_deletion_from_current_parquet(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    _record(db_path, file)
    parquet_before = file.absolute_path.read_bytes()

    db_path.unlink()
    result = rebuild_catalog_state(db_path, config, gate_factory=_gate)

    assert result.recovered_files == 1
    assert file.absolute_path.read_bytes() == parquet_before
    with Catalog.open(db_path) as catalog:
        recovered = catalog.list_files()
    assert recovered[0].relative_path == file.relative_path
    assert recovered[0].schema_version == 4


def test_rebuild_recovers_perpetual_metadata_first_identity_and_canonical_key(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    key = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.PERPETUAL,
        settle="USDT",
        timeframe="1h",
    )
    frame = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["perpetual"],
            "settle": ["USDT"],
            "timeframe": ["1h"],
            "timestamp": [datetime(2024, 1, 1, tzinfo=UTC)],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "volume": [10.0],
        },
        schema=OHLCV_SCHEMA,
    )
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(
            config.data_dir,
            key,
            YearMonth(2024, 1),
            frame,
            provider=parquet.ProviderIdentity("ccxt", "4.5.66", "BTCUSDT", "BTC/USDT:USDT"),
            derivative=parquet.DerivativeInterpretation(
                linear=True, inverse=False, contract_size="1"
            ),
        )
    )

    result = rebuild_catalog_state(db_path, config, gate_factory=_gate)

    assert result.recovered_files == 1
    assert committed.relative_path == (
        "binance/perpetual/BTC-USDT-USDT/1h/year=2024/month=01/data.parquet"
    )
    with Catalog.open(db_path) as catalog:
        recovered = catalog.list_files(key)
    assert len(recovered) == 1
    assert recovered[0].dataset_key == key
    assert recovered[0].relative_path == committed.relative_path


def test_validate_is_read_only_and_binds_to_fresh_committed_file_facts(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    _record(db_path, file)
    before = db_path.read_bytes()
    parquet_before = file.absolute_path.read_bytes()
    stale_source = replace(
        file,
        relative_path="stale/path.parquet",
        row_count=file.row_count + 1,
        min_timestamp=file.min_timestamp + timedelta(hours=1),
        physical_hash="stale",
        schema_version=999,
    )
    assert validate_catalog_state(db_path, config, file_source=lambda: [stale_source]).is_valid
    assert db_path.read_bytes() == before
    assert file.absolute_path.read_bytes() == parquet_before

    file.absolute_path.write_bytes(b"changed")
    parquet_before = file.absolute_path.read_bytes()
    with pytest.raises(CatalogError, match="failed to read"):
        validate_catalog_state(db_path, config, file_source=lambda: [file])
    assert db_path.read_bytes() == before
    assert file.absolute_path.read_bytes() == parquet_before


def test_validate_absent_catalog_does_not_create_catalog_or_lock(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    assert validate_catalog_state(db_path, config).is_valid
    assert not db_path.exists()
    assert not config.state_dir.exists()


@pytest.mark.parametrize(
    "relative_path",
    [
        f"{paths.TEMP_FILE_PREFIX}interrupted",
        "binance/BTC%2FUSDT/1h/year=2024/month=01/data.parquet",
        "binance/BTC%2FUSDT/invalid/none/1h/year=2024/month=01/data.parquet",
    ],
    ids=["temporary", "legacy", "malformed-canonical"],
)
def test_absent_catalog_refuses_ambiguous_managed_storage_without_mutation(
    tmp_path: Path, relative_path: str
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    evidence = config.data_dir / relative_path
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(b"owned storage evidence")
    before = evidence.read_bytes()

    assert paths.classify_managed_storage(config.data_dir) == "ambiguous"
    with pytest.raises(CatalogError, match="ambiguous"):
        validate_catalog_state(db_path, config, file_source=lambda: [])
    with pytest.raises(CatalogError, match="ambiguous"):
        rebuild_catalog_state(db_path, config, file_source=lambda: [], gate_factory=_gate)

    assert not db_path.exists()
    assert not config.state_dir.exists()
    assert evidence.read_bytes() == before


def test_rebuild_rejects_wrong_readable_slug_without_mutating_evidence(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    wrong_path = (
        config.data_dir
        / "binance"
        / "spot"
        / "WRONG-USDT"
        / "1h"
        / "year=2024"
        / "month=01"
        / paths.DATA_FILE_NAME
    )
    wrong_path.parent.mkdir(parents=True)
    file.absolute_path.replace(wrong_path)
    before = wrong_path.read_bytes()

    with pytest.raises(CatalogError, match="metadata-derived path"):
        rebuild_catalog_state(db_path, config, gate_factory=_gate)

    assert wrong_path.read_bytes() == before
    assert not db_path.exists()


def test_rebuild_rejects_unsupported_parquet_schema_without_mutating_evidence(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    frame = pl.read_parquet(file.absolute_path)
    metadata = pl.read_parquet_metadata(file.absolute_path) | {"schema_version": "3"}
    frame.write_parquet(file.absolute_path, metadata=metadata)
    before = file.absolute_path.read_bytes()

    with pytest.raises(CatalogError, match="unsupported schema version"):
        rebuild_catalog_state(db_path, config, gate_factory=_gate)

    assert file.absolute_path.read_bytes() == before
    assert not db_path.exists()


def test_rebuild_rejects_duplicate_evidence_without_mutating_parquet(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    before = file.absolute_path.read_bytes()

    with pytest.raises(CatalogError, match="duplicate canonical file path"):
        rebuild_catalog_state(db_path, config, file_source=lambda: [file, file], gate_factory=_gate)

    assert file.absolute_path.read_bytes() == before
    assert not db_path.exists()


def test_empty_managed_storage_is_valid_with_an_absent_catalog(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    config.data_dir.mkdir()

    assert paths.classify_managed_storage(config.data_dir) == "empty"
    result = validate_catalog_state(db_path, config, file_source=lambda: [])

    assert result.is_valid
    assert result.checked_datasets == ()
    assert result.issues == ()
    assert not db_path.exists()


def test_current_rebuild_resets_operational_rows_and_reconstructs_coverage(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    _record(db_path, file)
    with Catalog.open(db_path) as catalog:
        catalog.record_ingestion_run(
            IngestionRunMetadata(
                run_id="stale-run",
                dataset_key=file.dataset_key,
                requested_start=file.min_timestamp,
                requested_end=file.min_timestamp + timedelta(hours=2),
                started_at=file.min_timestamp,
                schema_version=CATALOG_SCHEMA_VERSION,
            )
        )
        catalog.set_coverage(
            file.dataset_key,
            (
                CoverageSegment(
                    file.min_timestamp,
                    file.min_timestamp + timedelta(hours=2),
                    CoverageStatus.UNAVAILABLE,
                ),
            ),
        )
    parquet_before = file.absolute_path.read_bytes()

    result = rebuild_catalog_state(db_path, config, file_source=lambda: [file], gate_factory=_gate)

    assert result.recovered_files == 1
    assert result.reset_datasets == ()
    assert result.warnings == ()
    assert file.absolute_path.read_bytes() == parquet_before
    with Catalog.open(db_path) as catalog:
        row = catalog.list_files()[0]
        assert row.physical_hash == file.physical_hash
        assert catalog.list_ingestion_run_ids(file.dataset_key) == ()
        assert catalog.get_coverage_segments(file.dataset_key) == (
            CoverageSegment(
                file.min_timestamp,
                file.min_timestamp + timedelta(hours=1),
                CoverageStatus.AVAILABLE,
            ),
        )


def test_rebuild_holds_catalog_gate_across_discovery_and_blocks_contending_mutation(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    _record(db_path, file)
    sqlite_before = db_path.read_bytes()
    parquet_before = file.absolute_path.read_bytes()
    events: list[str] = []

    @contextmanager
    def recording_gate(state_dir: Path):
        events.append("entered")
        with catalog_gate(state_dir):
            yield
        events.append("exited")

    def source() -> list[_File]:
        assert events == ["entered"]
        events.append("discovery")
        with (
            pytest.raises(CatalogError, match="timed out acquiring lock"),
            catalog_gate(config.state_dir, timeout=0, poll_interval=0),
        ):
            db_path.write_bytes(b"contended SQLite mutation")
            file.absolute_path.write_bytes(b"contended Parquet mutation")
        assert db_path.read_bytes() == sqlite_before
        assert file.absolute_path.read_bytes() == parquet_before
        return [file]

    result = rebuild_catalog_state(db_path, config, file_source=source, gate_factory=recording_gate)

    assert events == ["entered", "discovery", "exited"]
    assert result.recovered_files == 1
    assert file.absolute_path.read_bytes() == parquet_before


def test_rebuild_uses_canonical_file_facts_over_stale_source_metadata(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    file = _file(tmp_path)
    _record(db_path, file)
    parquet_before = file.absolute_path.read_bytes()
    bad = _File(
        file.dataset_key,
        file.year_month,
        file.relative_path,
        file.absolute_path,
        1,
        file.min_timestamp,
        file.max_timestamp,
        "0" * 64,
    )
    result = rebuild_catalog_state(db_path, config, file_source=lambda: [bad], gate_factory=_gate)

    assert result.recovered_files == 1
    with Catalog.open(db_path) as catalog:
        assert catalog.list_files(file.dataset_key)[0].physical_hash == file.physical_hash
    assert file.absolute_path.read_bytes() == parquet_before


def test_current_rebuild_rolls_back_after_later_catalog_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    first = _file(tmp_path)
    second = _file(
        tmp_path,
        YearMonth(2024, 2),
        datetime(2024, 2, 1, tzinfo=UTC),
    )
    _record(db_path, first)
    _record(db_path, second)
    with Catalog.open(db_path) as catalog:
        catalog.record_ingestion_run(
            IngestionRunMetadata(
                run_id="preserved-run",
                dataset_key=first.dataset_key,
                requested_start=first.min_timestamp,
                requested_end=second.max_timestamp + timedelta(hours=1),
                started_at=first.min_timestamp,
                schema_version=CATALOG_SCHEMA_VERSION,
            )
        )
        catalog.set_coverage(
            first.dataset_key,
            (
                CoverageSegment(
                    first.min_timestamp,
                    second.max_timestamp + timedelta(hours=1),
                    CoverageStatus.UNAVAILABLE,
                ),
            ),
        )
        datasets_before = catalog.list_datasets()
        files_before = catalog.list_files()
        ingestion_before = catalog.list_ingestion_run_ids(first.dataset_key)
        coverage_before = catalog.get_coverage_segments(first.dataset_key)
    parquet_before = {
        file.relative_path: file.absolute_path.read_bytes() for file in (first, second)
    }

    record_file = Catalog.record_file
    writes = 0

    def fail_second_record(catalog: Catalog, metadata: FileMetadata) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise CatalogError("injected later catalog write failure")
        record_file(catalog, metadata)

    monkeypatch.setattr(Catalog, "record_file", fail_second_record)

    with pytest.raises(CatalogError, match="injected later catalog write failure"):
        rebuild_catalog_state(
            db_path,
            config,
            file_source=lambda: [first, second],
            gate_factory=_gate,
        )

    assert writes == 2
    assert {
        file.relative_path: file.absolute_path.read_bytes() for file in (first, second)
    } == parquet_before
    with Catalog.open(db_path) as catalog:
        assert catalog.list_datasets() == datasets_before
        assert catalog.list_files() == files_before
        assert catalog.list_ingestion_run_ids(first.dataset_key) == ingestion_before
        assert catalog.get_coverage_segments(first.dataset_key) == coverage_before


def _key_1m() -> DatasetKey:
    return DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.SPOT,
        settle=NONE_SETTLE_SENTINEL,
        timeframe="1m",
    )


def _file_with_timestamps(
    tmp_path: Path,
    timestamps: list[datetime],
    timeframe: str = "1m",
) -> _File:
    """Create a committed Parquet file containing the given timestamps."""
    key = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.SPOT,
        settle=NONE_SETTLE_SENTINEL,
        timeframe=timeframe,
    )
    n = len(timestamps)
    year_month = YearMonth(timestamps[0].year, timestamps[0].month)
    frame = pl.DataFrame(
        {
            "exchange": ["binance"] * n,
            "symbol": ["BTC/USDT"] * n,
            "market": ["spot"] * n,
            "settle": [None] * n,
            "timeframe": [timeframe] * n,
            "timestamp": timestamps,
            "open": [1.0] * n,
            "high": [2.0] * n,
            "low": [0.5] * n,
            "close": [1.5] * n,
            "volume": [10.0] * n,
        },
        schema=OHLCV_SCHEMA,
    )
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(
            tmp_path / "data",
            key,
            year_month,
            frame,
            provider=parquet.ProviderIdentity("binance", "4.5.66", "BTCUSDT", "BTC/USDT"),
        )
    )
    return _File(
        committed.dataset_key,
        committed.year_month,
        committed.relative_path,
        committed.absolute_path,
        committed.row_count,
        committed.min_timestamp,
        committed.max_timestamp,
        committed.physical_hash,
    )


def _contiguous_timestamps(n: int, timeframe: str, start: datetime) -> list[datetime]:
    bar = TimeBar.parse(timeframe)
    result = []
    cursor = start
    for _ in range(n):
        result.append(cursor)
        cursor = bar.next_boundary(cursor)
    return result


def test_available_segments_coalesces_contiguous_bars(tmp_path: Path) -> None:
    """Contiguous 1m bars produce one coverage segment, not one per bar."""
    bar = TimeBar.parse("1m")
    timestamps = _contiguous_timestamps(10, "1m", datetime(2024, 1, 1, tzinfo=UTC))
    file = _file_with_timestamps(tmp_path, timestamps)

    segments = _available_segments(tmp_path / "data", file)

    assert len(segments) == 1
    assert segments[0].start == timestamps[0]
    assert segments[0].end == bar.next_boundary(timestamps[-1])
    assert segments[0].status == CoverageStatus.AVAILABLE


def test_available_segments_splits_on_gap(tmp_path: Path) -> None:
    """A gap in the middle produces two separate coverage segments."""
    bar = TimeBar.parse("1m")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    first_run = _contiguous_timestamps(5, "1m", base)
    # skip 2 minutes (00:05, 00:06 missing)
    second_start = bar.next_boundary(bar.next_boundary(first_run[-1]))
    second_start = bar.next_boundary(second_start)
    second_run = _contiguous_timestamps(5, "1m", second_start)
    timestamps = first_run + second_run
    file = _file_with_timestamps(tmp_path, timestamps)

    segments = _available_segments(tmp_path / "data", file)

    assert len(segments) == 2
    assert segments[0] == CoverageSegment(
        first_run[0], bar.next_boundary(first_run[-1]), CoverageStatus.AVAILABLE
    )
    assert segments[1] == CoverageSegment(
        second_run[0], bar.next_boundary(second_run[-1]), CoverageStatus.AVAILABLE
    )


def test_available_segments_multiple_gaps(tmp_path: Path) -> None:
    """Three contiguous runs separated by gaps produce three segments."""
    bar = TimeBar.parse("1m")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    runs = []
    cursor = base
    for _ in range(3):
        run = _contiguous_timestamps(3, "1m", cursor)
        runs.append(run)
        # skip 2 bars after each run
        cursor = bar.next_boundary(bar.next_boundary(bar.next_boundary(run[-1])))
    timestamps = [ts for run in runs for ts in run]
    file = _file_with_timestamps(tmp_path, timestamps)

    segments = _available_segments(tmp_path / "data", file)

    assert len(segments) == 3
    for seg, run in zip(segments, runs, strict=True):
        assert seg.start == run[0]
        assert seg.end == bar.next_boundary(run[-1])
        assert seg.status == CoverageStatus.AVAILABLE


def test_available_segments_single_bar(tmp_path: Path) -> None:
    """A single bar produces exactly one segment."""
    bar = TimeBar.parse("1m")
    timestamps = [datetime(2024, 1, 1, tzinfo=UTC)]
    file = _file_with_timestamps(tmp_path, timestamps)

    segments = _available_segments(tmp_path / "data", file)

    assert len(segments) == 1
    assert segments[0].start == timestamps[0]
    assert segments[0].end == bar.next_boundary(timestamps[0])
    assert segments[0].status == CoverageStatus.AVAILABLE


def test_rebuild_coverage_uses_coalesced_segments(tmp_path: Path) -> None:
    """Full rebuild produces coalesced coverage, not per-bar segments."""
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    timestamps = _contiguous_timestamps(10, "1m", datetime(2024, 1, 1, tzinfo=UTC))
    file = _file_with_timestamps(tmp_path, timestamps)
    _record(db_path, file)

    result = rebuild_catalog_state(db_path, config, file_source=lambda: [file], gate_factory=_gate)

    assert result.recovered_files == 1
    bar = TimeBar.parse("1m")
    with Catalog.open(db_path) as catalog:
        coverage = catalog.get_coverage_segments(file.dataset_key)
    assert coverage == (
        CoverageSegment(timestamps[0], bar.next_boundary(timestamps[-1]), CoverageStatus.AVAILABLE),
    )
