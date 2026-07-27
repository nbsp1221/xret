"""Focused contract tests for the SQLite operational catalog."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from xret.data.errors import CatalogError, SyncError
from xret.data.models import (
    NONE_SETTLE_SENTINEL,
    CoverageInterval,
    CoverageStatus,
    DatasetKey,
    Market,
    QualitySeverity,
)
from xret.data.storage import locking
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    SCHEMA_VERSION,
    Catalog,
    CoverageSegment,
    FileMetadata,
    IngestionRunMetadata,
    QualityEventMetadata,
    apply_update,
    connect,
    detect_incompatible_state,
    terminal_commit_is_visible,
)


def _dt(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour, tzinfo=UTC)


def _key(symbol: str = "BTC/USDT") -> DatasetKey:
    return DatasetKey(
        exchange="binance",
        symbol=symbol,
        market=Market.SPOT,
        settle=NONE_SETTLE_SENTINEL,
        timeframe="1h",
    )


@pytest.fixture
def catalog(tmp_path: Path):
    value = Catalog.open(tmp_path / CATALOG_FILE_NAME)
    yield value
    value.close()


@pytest.mark.parametrize(
    ("case", "expected"),
    (
        ("exact", True),
        ("identity_mismatch", False),
        ("hash_mismatch", False),
        ("absent_file", False),
        ("unreadable_catalog", False),
    ),
)
def test_terminal_commit_visibility_proof_is_fail_closed(
    tmp_path: Path, case: str, expected: bool
) -> None:
    db_path = tmp_path / CATALOG_FILE_NAME
    run = IngestionRunMetadata(
        run_id="run-1",
        dataset_key=_key(),
        requested_start=_dt(0),
        requested_end=_dt(2),
        started_at=_dt(0),
        schema_version=SCHEMA_VERSION,
        status="completed",
        completed_at=_dt(2),
    )
    with Catalog.open(db_path) as value:
        value.record_ingestion_run(run)
        value.record_file(
            FileMetadata(
                _key(),
                "2024/01/data.parquet",
                2024,
                1,
                2,
                _dt(0),
                _dt(1),
                "physical-hash",
                SCHEMA_VERSION,
            ),
            run_id=run.run_id,
        )

    proof_path = db_path
    proof_run = run
    expected_files = (("2024/01/data.parquet", "physical-hash"),)
    if case == "identity_mismatch":
        proof_run = replace(run, requested_end=_dt(3))
    elif case == "hash_mismatch":
        expected_files = (("2024/01/data.parquet", "different-hash"),)
    elif case == "absent_file":
        expected_files = (("2024/02/data.parquet", "physical-hash"),)
    elif case == "unreadable_catalog":
        proof_path = tmp_path / "missing.sqlite3"

    assert terminal_commit_is_visible(proof_path, proof_run, expected_files) is expected


def test_available_precedes_unavailable() -> None:
    available = CoverageSegment(_dt(1), _dt(3), CoverageStatus.AVAILABLE)
    unavailable = CoverageSegment(_dt(0), _dt(4), CoverageStatus.UNAVAILABLE)

    assert apply_update((unavailable,), available) == (
        CoverageSegment(_dt(0), _dt(1), CoverageStatus.UNAVAILABLE),
        available,
        CoverageSegment(_dt(3), _dt(4), CoverageStatus.UNAVAILABLE),
    )


def test_later_unavailable_update_does_not_downgrade_available_coverage() -> None:
    available = CoverageSegment(_dt(1), _dt(3), CoverageStatus.AVAILABLE)
    unavailable = CoverageSegment(_dt(0), _dt(4), CoverageStatus.UNAVAILABLE)

    assert apply_update((available,), unavailable) == (
        CoverageSegment(_dt(0), _dt(1), CoverageStatus.UNAVAILABLE),
        available,
        CoverageSegment(_dt(3), _dt(4), CoverageStatus.UNAVAILABLE),
    )


def test_missing_is_computed_not_persisted(catalog: Catalog) -> None:
    key = _key()
    catalog.apply_coverage(key, CoverageSegment(_dt(1), _dt(2), CoverageStatus.UNAVAILABLE))

    covered, gaps = catalog.coverage_and_gaps(key, _dt(0), _dt(3))
    assert covered == ()
    assert gaps == (
        CoverageInterval(_dt(0), _dt(1), CoverageStatus.MISSING),
        CoverageInterval(_dt(1), _dt(2), CoverageStatus.UNAVAILABLE),
        CoverageInterval(_dt(2), _dt(3), CoverageStatus.MISSING),
    )
    assert (
        catalog.connection.execute("SELECT status FROM coverage").fetchall()[0][0] == "unavailable"
    )


def test_apply_coverage_batch_matches_sequential_apply_coverage(catalog: Catalog) -> None:
    """Batch and sequential application produce identical stored coverage."""
    segments = [
        CoverageSegment(_dt(0), _dt(1), CoverageStatus.AVAILABLE),
        CoverageSegment(_dt(1), _dt(2), CoverageStatus.UNAVAILABLE),
        CoverageSegment(_dt(2), _dt(4), CoverageStatus.AVAILABLE),
    ]
    batch_key = _key()
    sequential_key = _key("ETH/USDT")

    catalog.apply_coverage_batch(batch_key, segments)
    for seg in segments:
        catalog.apply_coverage(sequential_key, seg)

    assert catalog.get_coverage_segments(batch_key) == catalog.get_coverage_segments(sequential_key)


def test_apply_coverage_batch_empty_is_noop(catalog: Catalog) -> None:
    key = _key()
    catalog.apply_coverage(key, CoverageSegment(_dt(0), _dt(2), CoverageStatus.AVAILABLE))
    before = catalog.get_coverage_segments(key)

    result = catalog.apply_coverage_batch(key, ())

    assert result == before
    assert catalog.get_coverage_segments(key) == before


def test_apply_coverage_batch_merges_with_existing_coverage(catalog: Catalog) -> None:
    key = _key()
    catalog.apply_coverage(key, CoverageSegment(_dt(0), _dt(2), CoverageStatus.AVAILABLE))

    result = catalog.apply_coverage_batch(
        key,
        [
            CoverageSegment(_dt(1), _dt(3), CoverageStatus.UNAVAILABLE),
            CoverageSegment(_dt(3), _dt(5), CoverageStatus.AVAILABLE),
        ],
    )

    assert result == (
        CoverageSegment(_dt(0), _dt(2), CoverageStatus.AVAILABLE),
        CoverageSegment(_dt(2), _dt(3), CoverageStatus.UNAVAILABLE),
        CoverageSegment(_dt(3), _dt(5), CoverageStatus.AVAILABLE),
    )
    assert catalog.get_coverage_segments(key) == result


def test_apply_coverage_batch_coalesces_adjacent_same_status(catalog: Catalog) -> None:
    result = catalog.apply_coverage_batch(
        _key(),
        [
            CoverageSegment(_dt(0), _dt(1), CoverageStatus.AVAILABLE),
            CoverageSegment(_dt(1), _dt(2), CoverageStatus.AVAILABLE),
            CoverageSegment(_dt(2), _dt(3), CoverageStatus.AVAILABLE),
        ],
    )

    assert result == (CoverageSegment(_dt(0), _dt(3), CoverageStatus.AVAILABLE),)


def test_coverage_segment_rejects_missing_status(catalog: Catalog) -> None:
    with pytest.raises(CatalogError):
        CoverageSegment(_dt(0), _dt(1), CoverageStatus.MISSING)


@pytest.mark.parametrize("status", ("missing", "retired"))
def test_direct_sql_rejects_unpersistable_coverage_statuses(catalog: Catalog, status: str) -> None:
    catalog.ensure_dataset(_key())
    with pytest.raises(sqlite3.IntegrityError):
        catalog.connection.execute(
            """
            INSERT INTO coverage (dataset_id, start_ts, end_ts, status, updated_at)
            VALUES (1, ?, ?, ?, ?)
            """,
            (_dt(0).isoformat(), _dt(1).isoformat(), status, _dt(1).isoformat()),
        )


def test_precurrent_schema_is_rejected(tmp_path: Path) -> None:
    db_path = tmp_path / CATALOG_FILE_NAME
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO schema_migrations VALUES (2)")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(CatalogError):
        connect(db_path)


def test_previous_v3_catalog_is_rejected_without_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / CATALOG_FILE_NAME
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO schema_migrations VALUES (3)")
        connection.execute(
            """
            CREATE TABLE datasets (
                id INTEGER PRIMARY KEY,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                settle TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (exchange, symbol, market, settle, timeframe)
            )
            """
        )
    before = db_path.read_bytes()

    assert detect_incompatible_state(db_path)
    with pytest.raises(CatalogError, match="incompatible"):
        Catalog.open(db_path)

    assert db_path.read_bytes() == before
    with sqlite3.connect(db_path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(datasets)")}
    assert "provider_name" not in columns


def test_run_identity_is_immutable_and_transaction_rolls_back(catalog: Catalog) -> None:
    run = IngestionRunMetadata(
        run_id="run-1",
        dataset_key=_key(),
        requested_start=_dt(0),
        requested_end=_dt(2),
        started_at=_dt(0),
        schema_version=SCHEMA_VERSION,
    )
    catalog.record_ingestion_run(run)
    catalog.record_ingestion_run(run)

    with pytest.raises(CatalogError):
        catalog.record_ingestion_run(
            IngestionRunMetadata(
                run_id="run-1",
                dataset_key=_key(),
                requested_start=_dt(1),
                requested_end=_dt(2),
                started_at=_dt(0),
                schema_version=SCHEMA_VERSION,
            )
        )

    with pytest.raises(RuntimeError), catalog.transaction():
        catalog.apply_coverage(_key(), CoverageSegment(_dt(0), _dt(1), CoverageStatus.AVAILABLE))
        raise RuntimeError("abort")
    assert catalog.get_coverage(_key()) == ()


def test_quality_event_is_linked_to_its_ingestion_run(catalog: Catalog) -> None:
    key = _key()
    catalog.record_ingestion_run(
        IngestionRunMetadata(
            run_id="run-1",
            dataset_key=key,
            requested_start=_dt(0),
            requested_end=_dt(2),
            started_at=_dt(0),
            schema_version=SCHEMA_VERSION,
        )
    )

    catalog.record_quality_event(
        key,
        QualityEventMetadata(
            severity=QualitySeverity.WARNING,
            code="coverage.timeframe_gap",
            message="one missing candle",
            run_id="run-1",
            created_at=_dt(2),
        ),
    )

    assert catalog.list_quality_events(key) == (
        QualityEventMetadata(
            severity=QualitySeverity.WARNING,
            code="coverage.timeframe_gap",
            message="one missing candle",
            run_id="run-1",
            created_at=_dt(2),
        ),
    )


def test_record_file_rejects_relative_path_reparenting(catalog: Catalog) -> None:
    key = _key()
    catalog.record_ingestion_run(
        IngestionRunMetadata(
            run_id="run-1",
            dataset_key=key,
            requested_start=_dt(0),
            requested_end=_dt(1),
            started_at=_dt(0),
            schema_version=SCHEMA_VERSION,
        )
    )
    metadata = FileMetadata(
        key, "shared.parquet", 2024, 1, 1, _dt(0), _dt(0), "hash", SCHEMA_VERSION
    )
    catalog.record_file(metadata, run_id="run-1")

    with pytest.raises(CatalogError, match="cannot re-parent existing file"):
        catalog.record_file(
            FileMetadata(
                _key("ETH/USDT"),
                "shared.parquet",
                2024,
                1,
                1,
                _dt(0),
                _dt(0),
                "hash",
                SCHEMA_VERSION,
            )
        )

    assert catalog.list_files() == (catalog.list_files(key)[0],)
    assert catalog.connection.execute("SELECT COUNT(*) FROM file_runs").fetchone()[0] == 1


def test_lock_setup_oserror_uses_configured_error_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        raise OSError(f"cannot create {self}")

    monkeypatch.setattr(Path, "mkdir", fail_mkdir)

    with pytest.raises(SyncError, match="failed to set up lock") as dataset_error:
        locking.dataset_lock(tmp_path, _key()).acquire()
    assert isinstance(dataset_error.value.__cause__, OSError)

    with pytest.raises(CatalogError, match="failed to set up lock") as gate_error:
        locking.catalog_gate(tmp_path).acquire()
    assert isinstance(gate_error.value.__cause__, OSError)


def test_catalog_gate_is_distinct_from_dataset_lock(tmp_path: Path) -> None:
    with locking.dataset_lock(tmp_path, _key()), locking.catalog_gate(tmp_path):
        pass

    gate = locking.catalog_gate(tmp_path)
    with gate, pytest.raises(CatalogError), locking.catalog_gate(tmp_path, timeout=0):
        pass

    dataset = locking.dataset_lock(tmp_path, _key())
    with dataset, pytest.raises(SyncError), locking.dataset_lock(tmp_path, _key(), timeout=0):
        pass


def test_snapshot_remains_stable_and_rejects_writes_after_concurrent_commit(
    catalog: Catalog, tmp_path: Path
) -> None:
    catalog.ensure_dataset(_key())
    with catalog.snapshot() as snapshot:
        assert snapshot.execute("SELECT COUNT(*) FROM datasets").fetchone()[0] == 1

        writer = Catalog.open(tmp_path / CATALOG_FILE_NAME)
        try:
            writer.ensure_dataset(_key("ETH/USDT"))
        finally:
            writer.close()

        assert snapshot.execute("SELECT COUNT(*) FROM datasets").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            snapshot.execute(
                "UPDATE datasets SET updated_at = ? WHERE id = 1",
                (_dt(2).isoformat(),),
            )
