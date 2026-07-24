"""SQLite operational catalog derived from canonical Parquet data.

The catalog indexes datasets and files, records normal synchronization
lifecycle state, and stores the two observed coverage facts.  SQLite is not
canonical: unavailable observations and run history cannot be reconstructed
from Parquet.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final

from xret.data.errors import CatalogError
from xret.data.models import (
    CoverageInterval,
    CoverageStatus,
    DatasetKey,
    Market,
    QualitySeverity,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "CATALOG_FILE_NAME",
    "SCHEMA_VERSION",
    "CoverageSegment",
    "FileMetadata",
    "IngestionRunMetadata",
    "QualityEventMetadata",
    "FileRow",
    "connect",
    "terminal_commit_is_visible",
    "normalize_segments",
    "apply_update",
    "covered_and_gaps",
    "detect_incompatible_state",
    "Catalog",
]

#: Current, incompatible catalog schema.  Older layouts are rejected rather
#: than migrated because their retry, generation, and provenance semantics
#: are retired.
SCHEMA_VERSION: Final[int] = 3
#: Name of the SQLite coverage/provenance index file under `state_dir`.
CATALOG_FILE_NAME: Final[str] = "catalog.sqlite3"


class _CommitUncertainCatalogError(CatalogError):
    """Private signal that SQLite could not confirm a terminal COMMIT."""


_BUSY_TIMEOUT_MS_DEFAULT: Final[int] = 5_000


# --------------------------------------------------------------------------
# Coverage algebra (pure, no I/O)
# --------------------------------------------------------------------------

#: Only observed states are persisted. ``MISSING`` is computed from absent
#: records and must never enter a catalog row.
_PRECEDENCE: Final[tuple[CoverageStatus, ...]] = (
    CoverageStatus.AVAILABLE,
    CoverageStatus.UNAVAILABLE,
)

_RANK: Final[dict[CoverageStatus, int]] = {status: i for i, status in enumerate(_PRECEDENCE)}


def _rank(status: CoverageStatus) -> int:
    try:
        return _RANK[status]
    except KeyError as exc:
        raise CatalogError(f"coverage status has no stored precedence: {status!r}") from exc


@dataclass(frozen=True, slots=True)
class CoverageSegment:
    """A persisted observed coverage interval."""

    start: datetime
    end: datetime
    status: CoverageStatus

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise CatalogError(
                f"coverage segment start must be before end: start={self.start!r} end={self.end!r}"
            )
        if self.status not in _RANK:
            raise CatalogError(f"coverage segment has an unstorable status: {self.status!r}")

    def to_interval(self) -> CoverageInterval:
        return CoverageInterval(self.start, self.end, self.status)


def normalize_segments(segments: Sequence[CoverageSegment]) -> tuple[CoverageSegment, ...]:
    """Normalize possibly-overlapping segments into sorted, disjoint ones.

    Overlaps are resolved by status precedence (`available` wins); on a tie,
    the later entry in `segments` wins. Adjacent equal-status segments are
    coalesced.
    """
    if not segments:
        return ()

    points = sorted({segment.start for segment in segments} | {segment.end for segment in segments})
    elementary: list[CoverageSegment] = []
    for lo, hi in zip(points, points[1:], strict=False):
        best: CoverageSegment | None = None
        for segment in segments:
            if segment.start > lo or segment.end < hi:
                continue
            if best is None or _rank(segment.status) <= _rank(best.status):
                best = segment
        if best is None:
            continue
        elementary.append(CoverageSegment(lo, hi, best.status))

    return _coalesce(elementary)


def _coalesce(segments: list[CoverageSegment]) -> tuple[CoverageSegment, ...]:
    if not segments:
        return ()
    merged = [segments[0]]
    for segment in segments[1:]:
        last = merged[-1]
        if last.end == segment.start and last.status is segment.status:
            merged[-1] = CoverageSegment(last.start, segment.end, last.status)
        else:
            merged.append(segment)
    return tuple(merged)


def apply_update(
    existing: Sequence[CoverageSegment], update: CoverageSegment
) -> tuple[CoverageSegment, ...]:
    """Merge `update` into `existing`, resolving overlaps by precedence."""
    return normalize_segments((*existing, update))


def covered_and_gaps(
    segments: Sequence[CoverageSegment], start: datetime, end: datetime
) -> tuple[tuple[CoverageInterval, ...], tuple[CoverageInterval, ...]]:
    """Split a request into available coverage and unavailable/missing gaps."""
    if start >= end:
        raise CatalogError(f"start must be before end: start={start!r} end={end!r}")

    normalized = normalize_segments(segments)
    covered: list[CoverageInterval] = []
    gaps: list[CoverageInterval] = []
    cursor = start
    for segment in normalized:
        if segment.end <= start or segment.start >= end:
            continue
        seg_start = max(segment.start, start)
        seg_end = min(segment.end, end)
        if seg_start > cursor:
            gaps.append(CoverageInterval(cursor, seg_start, CoverageStatus.MISSING))
        if segment.status is CoverageStatus.AVAILABLE:
            covered.append(CoverageInterval(seg_start, seg_end, CoverageStatus.AVAILABLE))
        else:
            gaps.append(CoverageInterval(seg_start, seg_end, segment.status))
        cursor = seg_end
    if cursor < end:
        gaps.append(CoverageInterval(cursor, end, CoverageStatus.MISSING))

    return tuple(covered), tuple(gaps)


# --------------------------------------------------------------------------
# Persistence-facing metadata records
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FileMetadata:
    """SQLite index metadata for one published physical Parquet file."""

    dataset_key: DatasetKey
    relative_path: str
    year: int
    month: int
    row_count: int
    min_timestamp: datetime
    max_timestamp: datetime
    physical_hash: str
    schema_version: int


@dataclass(frozen=True, slots=True)
class IngestionRunMetadata:
    """One synchronization run with immutable request identity."""

    run_id: str
    dataset_key: DatasetKey
    requested_start: datetime
    requested_end: datetime
    started_at: datetime
    schema_version: int
    status: str = "running"
    ccxt_version: str | None = None
    raw_market_id: str | None = None
    actual_start: datetime | None = None
    actual_end: datetime | None = None
    retrieved_at: datetime | None = None
    completed_at: datetime | None = None
    row_count: int = 0


@dataclass(frozen=True, slots=True)
class QualityEventMetadata:
    """One recorded warning or quality finding."""

    severity: QualitySeverity
    code: str
    message: str
    run_id: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class FileRow:
    """A file record as read back from the catalog."""

    dataset_key: DatasetKey
    relative_path: str
    year: int
    month: int
    row_count: int
    min_timestamp: datetime
    max_timestamp: datetime
    physical_hash: str
    schema_version: int


# --------------------------------------------------------------------------
# Connection lifecycle and migrations
# --------------------------------------------------------------------------

_DDL_V3: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS datasets (
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
    """,
    """
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY,
        dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
        relative_path TEXT NOT NULL UNIQUE,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        min_timestamp TEXT NOT NULL,
        max_timestamp TEXT NOT NULL,
        physical_hash TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_files_dataset ON files(dataset_id)",
    """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        id INTEGER PRIMARY KEY,
        run_id TEXT NOT NULL UNIQUE,
        dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
        requested_start TEXT NOT NULL,
        requested_end TEXT NOT NULL,
        started_at TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        ccxt_version TEXT,
        raw_market_id TEXT,
        actual_start TEXT,
        actual_end TEXT,
        retrieved_at TEXT,
        completed_at TEXT,
        row_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_runs_dataset ON ingestion_runs(dataset_id)",
    """
    CREATE TABLE IF NOT EXISTS file_runs (
        file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
        run_id INTEGER NOT NULL REFERENCES ingestion_runs(id) ON DELETE CASCADE,
        PRIMARY KEY (file_id, run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS coverage (
        id INTEGER PRIMARY KEY,
        dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
        start_ts TEXT NOT NULL,
        end_ts TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('available', 'unavailable')),
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_coverage_dataset ON coverage(dataset_id, start_ts)",
    """
    CREATE TABLE IF NOT EXISTS quality_events (
        id INTEGER PRIMARY KEY,
        dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
        run_id INTEGER REFERENCES ingestion_runs(id) ON DELETE SET NULL,
        severity TEXT NOT NULL,
        code TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_quality_dataset ON quality_events(dataset_id)",
)
_MIGRATIONS: Final[tuple[tuple[int, tuple[str, ...]], ...]] = ((SCHEMA_VERSION, _DDL_V3),)


def _open_read_only_connection(db_path: Path) -> sqlite3.Connection:
    """Open the live catalog read-only without creating SQLite artifacts."""
    connection = sqlite3.connect(
        f"{db_path.resolve().as_uri()}?mode=ro",
        uri=True,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _read_only_snapshot_connection(db_path: Path) -> sqlite3.Connection:
    """Copy a SQLite-owned coherent read snapshot into memory."""
    source = _open_read_only_connection(db_path)
    snapshot = sqlite3.connect(":memory:", isolation_level=None)
    try:
        source.execute("BEGIN")
        source.backup(snapshot)
    finally:
        if source.in_transaction:
            source.execute("ROLLBACK")
        source.close()
    snapshot.row_factory = sqlite3.Row
    snapshot.execute("PRAGMA query_only = ON")
    return snapshot


def terminal_commit_is_visible(
    db_path: Path,
    run: IngestionRunMetadata,
    expected_files: tuple[tuple[str, str], ...],
) -> bool:
    """Confirm a completed immutable run and its expected indexed file hashes."""
    connection: sqlite3.Connection | None = None
    try:
        connection = _read_only_snapshot_connection(db_path)
        row = connection.execute(
            """
            SELECT r.status, r.requested_start, r.requested_end, r.started_at,
                   r.schema_version, d.exchange, d.symbol, d.market, d.settle,
                   d.timeframe
            FROM ingestion_runs r
            JOIN datasets d ON d.id = r.dataset_id
            WHERE r.run_id = ?
            """,
            (run.run_id,),
        ).fetchone()
        expected_identity = (
            "completed",
            _iso(run.requested_start),
            _iso(run.requested_end),
            _iso(run.started_at),
            run.schema_version,
            run.dataset_key.exchange,
            run.dataset_key.symbol,
            run.dataset_key.market.value,
            run.dataset_key.settle,
            run.dataset_key.timeframe,
        )
        if row is None or tuple(row) != expected_identity:
            return False
        indexed = {
            item["relative_path"]: item["physical_hash"]
            for item in connection.execute(
                """
                SELECT f.relative_path, f.physical_hash
                FROM files f
                JOIN datasets d ON d.id = f.dataset_id
                WHERE d.exchange = ? AND d.symbol = ? AND d.market = ?
                  AND d.settle = ? AND d.timeframe = ?
                """,
                (
                    run.dataset_key.exchange,
                    run.dataset_key.symbol,
                    run.dataset_key.market.value,
                    run.dataset_key.settle,
                    run.dataset_key.timeframe,
                ),
            )
        }
        return all(
            indexed.get(relative_path) == physical_hash
            for relative_path, physical_hash in expected_files
        )
    except (OSError, sqlite3.DatabaseError):
        return False
    finally:
        if connection is not None:
            connection.close()


def detect_incompatible_state(db_path: Path) -> bool:
    """Whether `db_path` is a pre-v2 (or otherwise incompatible) catalog.

    Read-only and side-effect-free: briefly opens `db_path` (if it
    exists) to inspect `sqlite_master`/`schema_migrations`, then closes
    it -- never runs `connect()`'s migration path. Returns `False` when
    `db_path` does not exist (nothing to detect yet) or the current
    `SCHEMA_VERSION` is already recorded as applied; returns `True` for
    every other case, including a corrupt/unopenable file, a database
    with `datasets`-like content but no `schema_migrations` table, or a
    ledger whose most recent applied version predates `SCHEMA_VERSION`.

    This function only detects; it never deletes or migrates anything.
    `rebuild_catalog_state` uses it to select the validated replacement path
    for a missing, incompatible, or unreadable catalog. `connect()` and
    `Catalog.open()` refuse internally (via `_migrate`) rather than silently
    stamping the current version onto legacy tables, so callers still get a
    typed `CatalogError` before mutation.
    """
    if not db_path.is_file():
        return False
    minimum_sidecar_sizes = {"-wal": 32, "-shm": 32_768}
    for suffix, minimum_size in minimum_sidecar_sizes.items():
        sidecar = db_path.with_name(db_path.name + suffix)
        try:
            if sidecar.is_file() and 0 < sidecar.stat().st_size < minimum_size:
                return True
        except OSError:
            return True
    try:
        probe = _open_read_only_connection(db_path)
        probe.execute("BEGIN")
    except (OSError, sqlite3.DatabaseError):
        return True
    try:
        tables = {
            row[0]
            for row in probe.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "schema_migrations" not in tables:
            return True
        applied = {
            row[0] for row in probe.execute("SELECT version FROM schema_migrations").fetchall()
        }
        return SCHEMA_VERSION not in applied
    except sqlite3.DatabaseError:
        return True
    finally:
        if probe.in_transaction:
            probe.execute("ROLLBACK")
        probe.close()


def connect(
    db_path: Path, *, busy_timeout_ms: int = _BUSY_TIMEOUT_MS_DEFAULT
) -> sqlite3.Connection:
    """Open `db_path`, configure WAL/foreign-keys/busy-timeout, and migrate.

    `db_path` may be `:memory:`-like or a real file; the parent directory
    is created if missing. The returned connection has `row_factory` set
    to `sqlite3.Row` and autocommit disabled (callers use `Catalog`'s
    `transaction()` helper).

    Raises:
        CatalogError: `db_path` names an incompatible (pre-v2, or
            otherwise unrecognized) database -- see `_migrate`. This is
            the typed failure IR-4 requires: a normal `connect()` never
            silently mutates or misclassifies a legacy database, even if
            a caller forgot to check `detect_incompatible_state` first.
    """
    if str(db_path) != ":memory:":
        if db_path.exists() and detect_incompatible_state(db_path):
            raise CatalogError(
                f"catalog at {db_path} is incompatible; run maintenance.rebuild_catalog()"
            )
        db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_path), isolation_level=None, timeout=busy_timeout_ms / 1000)
    connection.row_factory = sqlite3.Row
    try:
        _migrate(connection)
        connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = NORMAL")
    except CatalogError:
        connection.close()
        raise
    except sqlite3.DatabaseError as exc:
        connection.close()
        raise CatalogError(f"failed to open catalog at {db_path}: {exc}") from exc
    return connection


def _migrate(connection: sqlite3.Connection) -> None:
    """Apply every not-yet-applied migration in `_MIGRATIONS`, in order.

    Refuses -- typed, before any DDL or ledger write -- rather than
    stamping `SCHEMA_VERSION` onto a database whose tables it did not create.
    An incompatible database is handled only by the exclusive catalog rebuild
    path, never by normal catalog opening.
    """
    existing_tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if existing_tables and "schema_migrations" not in existing_tables:
        raise CatalogError(
            "database has pre-existing tables but no schema_migrations ledger; "
            "refusing to stamp a schema version onto an incompatible catalog; "
            "run maintenance.rebuild_catalog()"
        )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )
    applied = {
        row[0] for row in connection.execute("SELECT version FROM schema_migrations").fetchall()
    }
    if applied and SCHEMA_VERSION not in applied:
        raise CatalogError(
            f"database's schema_migrations ledger records only {sorted(applied)!r}, "
            f"never the current version {SCHEMA_VERSION}; refusing to mutate an "
            "incompatible catalog; run maintenance.rebuild_catalog()"
        )

    for version, statements in _MIGRATIONS:
        if version in applied:
            continue
        connection.execute("BEGIN IMMEDIATE")
        try:
            for statement in statements:
                connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, datetime.now().astimezone().isoformat()),
            )
        except sqlite3.DatabaseError:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")


def _iso(value: datetime) -> str:
    return value.isoformat()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


# --------------------------------------------------------------------------
# Catalog
# --------------------------------------------------------------------------


class Catalog:
    """A single-file SQLite coverage/provenance index for one Xret root.

    `Catalog` never performs Parquet I/O; it only stores what
    `storage.parquet` and `recovery.py` tell it. All mutating methods
    run in their own transaction unless called inside an outer
    `transaction()` block.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    @classmethod
    def open(cls, db_path: Path, *, busy_timeout_ms: int = _BUSY_TIMEOUT_MS_DEFAULT) -> Catalog:
        return cls(connect(db_path, busy_timeout_ms=busy_timeout_ms))

    @classmethod
    def open_read_only(cls, db_path: Path) -> Catalog:
        """Open an existing current catalog as an enforceable read-only snapshot."""
        if not db_path.is_file() or detect_incompatible_state(db_path):
            raise CatalogError(f"catalog at {db_path} is absent or incompatible")
        try:
            connection = _read_only_snapshot_connection(db_path)
        except (OSError, sqlite3.DatabaseError) as exc:
            raise CatalogError(f"catalog at {db_path} is unreadable: {exc}") from exc
        return cls(connection)

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run a block of catalog writes atomically."""
        if self._connection.in_transaction:
            yield self._connection
            return
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.DatabaseError as exc:
            raise CatalogError(f"failed to begin catalog transaction: {exc}") from exc
        try:
            yield self._connection
        except BaseException:
            with suppress(sqlite3.DatabaseError):
                self._connection.execute("ROLLBACK")
            raise
        else:
            try:
                self._connection.execute("COMMIT")
            except sqlite3.DatabaseError as exc:
                raise _CommitUncertainCatalogError(
                    f"catalog commit outcome is uncertain: {exc}"
                ) from exc

    @contextmanager
    def snapshot(self) -> Iterator[sqlite3.Connection]:
        """Provide a consistent read snapshot without taking a write lock."""
        self._connection.execute("BEGIN")
        try:
            yield self._connection
        finally:
            self._connection.execute("ROLLBACK")

    # -- datasets ----------------------------------------------------

    def ensure_dataset(self, key: DatasetKey) -> int:
        """Return the dataset row id, inserting it if it does not exist."""
        with self.transaction():
            now = _iso(datetime.now().astimezone())
            existing = self._get_dataset_id(key)
            if existing is not None:
                return existing
            cursor = self._connection.execute(
                """
                INSERT INTO datasets
                    (exchange, symbol, market, settle, timeframe, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key.exchange,
                    key.symbol,
                    key.market.value,
                    key.settle,
                    key.timeframe,
                    now,
                    now,
                ),
            )
            dataset_id = cursor.lastrowid
            if dataset_id is None:
                raise CatalogError(f"failed to insert dataset row for {key!r}")
            return dataset_id

    def _get_dataset_id(self, key: DatasetKey) -> int | None:
        row = self._connection.execute(
            """
            SELECT id FROM datasets
            WHERE exchange = ? AND symbol = ? AND market = ? AND settle = ? AND timeframe = ?
            """,
            (key.exchange, key.symbol, key.market.value, key.settle, key.timeframe),
        ).fetchone()
        return row["id"] if row is not None else None

    def get_dataset_id(self, key: DatasetKey) -> int | None:
        return self._get_dataset_id(key)

    def list_datasets(self) -> tuple[DatasetKey, ...]:
        rows = self._connection.execute(
            "SELECT exchange, symbol, market, settle, timeframe FROM datasets ORDER BY id"
        ).fetchall()
        return tuple(
            DatasetKey(
                exchange=row["exchange"],
                symbol=row["symbol"],
                market=Market(row["market"]),
                settle=row["settle"],
                timeframe=row["timeframe"],
            )
            for row in rows
        )

    def delete_dataset(self, key: DatasetKey) -> None:
        """Remove a dataset and every row that references it (cascade)."""
        with self.transaction():
            self._connection.execute(
                "DELETE FROM datasets "
                "WHERE exchange = ? AND symbol = ? AND market = ? AND settle = ? AND timeframe = ?",
                (key.exchange, key.symbol, key.market.value, key.settle, key.timeframe),
            )

    # -- coverage ------------------------------------------------------

    def get_coverage_segments(self, key: DatasetKey) -> tuple[CoverageSegment, ...]:
        dataset_id = self._get_dataset_id(key)
        if dataset_id is None:
            return ()
        rows = self._connection.execute(
            "SELECT start_ts, end_ts, status FROM coverage WHERE dataset_id = ? ORDER BY start_ts",
            (dataset_id,),
        ).fetchall()
        return tuple(
            CoverageSegment(
                _parse_iso(row["start_ts"]),
                _parse_iso(row["end_ts"]),
                CoverageStatus(row["status"]),
            )
            for row in rows
        )

    def get_coverage(self, key: DatasetKey) -> tuple[CoverageInterval, ...]:
        return tuple(segment.to_interval() for segment in self.get_coverage_segments(key))

    def apply_coverage(
        self, key: DatasetKey, segment: CoverageSegment
    ) -> tuple[CoverageSegment, ...]:
        """Merge `segment` into the dataset's stored coverage (by precedence)."""
        with self.transaction():
            dataset_id = self.ensure_dataset(key)
            existing = self.get_coverage_segments(key)
            normalized = apply_update(existing, segment)
            self._replace_coverage(dataset_id, normalized)
            return normalized

    def apply_coverage_batch(
        self, key: DatasetKey, segments: Sequence[CoverageSegment]
    ) -> tuple[CoverageSegment, ...]:
        """Merge multiple `segments` into stored coverage in one pass.

        Equivalent to calling `apply_coverage` for each segment in order,
        but reads existing coverage once and normalizes once: O((e+k)²)
        instead of O(k³) for k new segments against e existing ones.
        """
        if not segments:
            return self.get_coverage_segments(key)
        with self.transaction():
            dataset_id = self.ensure_dataset(key)
            existing = self.get_coverage_segments(key)
            normalized = normalize_segments((*existing, *segments))
            self._replace_coverage(dataset_id, normalized)
            return normalized

    def set_coverage(self, key: DatasetKey, segments: Sequence[CoverageSegment]) -> None:
        """Replace a dataset's entire stored coverage with a normalized set."""
        with self.transaction():
            dataset_id = self.ensure_dataset(key)
            self._replace_coverage(dataset_id, normalize_segments(tuple(segments)))

    def _replace_coverage(self, dataset_id: int, segments: Sequence[CoverageSegment]) -> None:
        now = _iso(datetime.now().astimezone())
        self._connection.execute("DELETE FROM coverage WHERE dataset_id = ?", (dataset_id,))
        self._connection.executemany(
            """
            INSERT INTO coverage (dataset_id, start_ts, end_ts, status, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    dataset_id,
                    _iso(segment.start),
                    _iso(segment.end),
                    segment.status.value,
                    now,
                )
                for segment in segments
            ],
        )

    def coverage_and_gaps(
        self, key: DatasetKey, start: datetime, end: datetime
    ) -> tuple[tuple[CoverageInterval, ...], tuple[CoverageInterval, ...]]:
        return covered_and_gaps(self.get_coverage_segments(key), start, end)

    # -- files -----------------------------------------------------------

    def record_file(self, metadata: FileMetadata, *, run_id: str | None = None) -> None:
        """Upsert a published file and optionally link the publishing run."""
        with self.transaction():
            existing = self._connection.execute(
                "SELECT id, dataset_id FROM files WHERE relative_path = ?",
                (metadata.relative_path,),
            ).fetchone()
            if existing is not None and existing["dataset_id"] != self._get_dataset_id(
                metadata.dataset_key
            ):
                raise CatalogError(
                    f"cannot re-parent existing file to another dataset: {metadata.relative_path!r}"
                )
            dataset_id = self.ensure_dataset(metadata.dataset_key)
            run_row = None
            if run_id is not None:
                run_row = self._connection.execute(
                    "SELECT id, dataset_id FROM ingestion_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run_row is None:
                    raise CatalogError(f"cannot link file to unknown run {run_id!r}")
                if run_row["dataset_id"] != dataset_id:
                    raise CatalogError(f"cannot link file to run from another dataset: {run_id!r}")
            now = _iso(datetime.now().astimezone())
            if existing is None:
                cursor = self._connection.execute(
                    """
                    INSERT INTO files (
                        dataset_id, relative_path, year, month, row_count, min_timestamp,
                        max_timestamp, physical_hash, schema_version, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        dataset_id,
                        metadata.relative_path,
                        metadata.year,
                        metadata.month,
                        metadata.row_count,
                        _iso(metadata.min_timestamp),
                        _iso(metadata.max_timestamp),
                        metadata.physical_hash,
                        metadata.schema_version,
                        now,
                        now,
                    ),
                )
                file_id = cursor.lastrowid
                if file_id is None:
                    raise CatalogError(f"failed to insert file row for {metadata.relative_path!r}")
            else:
                file_id = existing["id"]
                self._connection.execute(
                    """
                    UPDATE files SET dataset_id = ?, year = ?, month = ?, row_count = ?,
                        min_timestamp = ?, max_timestamp = ?, physical_hash = ?,
                        schema_version = ?, updated_at = ? WHERE id = ?
                    """,
                    (
                        dataset_id,
                        metadata.year,
                        metadata.month,
                        metadata.row_count,
                        _iso(metadata.min_timestamp),
                        _iso(metadata.max_timestamp),
                        metadata.physical_hash,
                        metadata.schema_version,
                        now,
                        file_id,
                    ),
                )
            if run_row is not None:
                self._connection.execute(
                    "INSERT OR IGNORE INTO file_runs (file_id, run_id) VALUES (?, ?)",
                    (file_id, run_row["id"]),
                )

    def remove_file(self, relative_path: str) -> None:
        with self.transaction():
            self._connection.execute("DELETE FROM files WHERE relative_path = ?", (relative_path,))

    def list_files(self, key: DatasetKey | None = None) -> tuple[FileRow, ...]:
        if key is None:
            rows = self._connection.execute(
                """
                SELECT d.exchange, d.symbol, d.market, d.settle, d.timeframe, f.*
                FROM files f JOIN datasets d ON d.id = f.dataset_id
                ORDER BY f.relative_path
                """
            ).fetchall()
        else:
            dataset_id = self._get_dataset_id(key)
            if dataset_id is None:
                return ()
            rows = self._connection.execute(
                """
                SELECT d.exchange, d.symbol, d.market, d.settle, d.timeframe, f.*
                FROM files f JOIN datasets d ON d.id = f.dataset_id
                WHERE f.dataset_id = ?
                ORDER BY f.relative_path
                """,
                (dataset_id,),
            ).fetchall()

        return tuple(
            FileRow(
                dataset_key=DatasetKey(
                    exchange=row["exchange"],
                    symbol=row["symbol"],
                    market=Market(row["market"]),
                    settle=row["settle"],
                    timeframe=row["timeframe"],
                ),
                relative_path=row["relative_path"],
                year=row["year"],
                month=row["month"],
                row_count=row["row_count"],
                min_timestamp=_parse_iso(row["min_timestamp"]),
                max_timestamp=_parse_iso(row["max_timestamp"]),
                physical_hash=row["physical_hash"],
                schema_version=row["schema_version"],
            )
            for row in rows
        )

    # -- ingestion runs ----------------------------------------------------

    def record_ingestion_run(self, run: IngestionRunMetadata) -> None:
        """Create or update a run after verifying its immutable identity."""
        with self.transaction():
            dataset_id = self.ensure_dataset(run.dataset_key)
            existing = self._connection.execute(
                """
                SELECT dataset_id, requested_start, requested_end, started_at, schema_version
                FROM ingestion_runs WHERE run_id = ?
                """,
                (run.run_id,),
            ).fetchone()
            identity = (
                dataset_id,
                _iso(run.requested_start),
                _iso(run.requested_end),
                _iso(run.started_at),
                run.schema_version,
            )
            if existing is not None:
                stored_identity = tuple(
                    existing[column]
                    for column in (
                        "dataset_id",
                        "requested_start",
                        "requested_end",
                        "started_at",
                        "schema_version",
                    )
                )
                if stored_identity != identity:
                    raise CatalogError(f"run_id {run.run_id!r} has a different immutable identity")
                self._connection.execute(
                    """
                    UPDATE ingestion_runs SET status = ?, ccxt_version = ?, raw_market_id = ?,
                        actual_start = ?, actual_end = ?, retrieved_at = ?, completed_at = ?,
                        row_count = ? WHERE run_id = ?
                    """,
                    (
                        run.status,
                        run.ccxt_version,
                        run.raw_market_id,
                        _iso(run.actual_start) if run.actual_start is not None else None,
                        _iso(run.actual_end) if run.actual_end is not None else None,
                        _iso(run.retrieved_at) if run.retrieved_at is not None else None,
                        _iso(run.completed_at) if run.completed_at is not None else None,
                        run.row_count,
                        run.run_id,
                    ),
                )
                return
            self._connection.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, dataset_id, requested_start, requested_end, started_at,
                    schema_version, status, ccxt_version, raw_market_id, actual_start,
                    actual_end, retrieved_at, completed_at, row_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    *identity,
                    run.status,
                    run.ccxt_version,
                    run.raw_market_id,
                    _iso(run.actual_start) if run.actual_start is not None else None,
                    _iso(run.actual_end) if run.actual_end is not None else None,
                    _iso(run.retrieved_at) if run.retrieved_at is not None else None,
                    _iso(run.completed_at) if run.completed_at is not None else None,
                    run.row_count,
                    _iso(datetime.now().astimezone()),
                ),
            )

    def list_ingestion_run_ids(self, key: DatasetKey) -> tuple[str, ...]:
        dataset_id = self._get_dataset_id(key)
        if dataset_id is None:
            return ()
        rows = self._connection.execute(
            "SELECT run_id FROM ingestion_runs WHERE dataset_id = ? ORDER BY run_id", (dataset_id,)
        ).fetchall()
        return tuple(row["run_id"] for row in rows)

    # -- quality events ------------------------------------------------

    def record_quality_event(self, key: DatasetKey, event: QualityEventMetadata) -> None:
        with self.transaction():
            dataset_id = self.ensure_dataset(key)
            run_row_id = None
            if event.run_id is not None:
                run_row = self._connection.execute(
                    "SELECT id, dataset_id FROM ingestion_runs WHERE run_id = ?", (event.run_id,)
                ).fetchone()
                if run_row is None:
                    raise CatalogError(f"cannot link quality event to unknown run {event.run_id!r}")
                if run_row["dataset_id"] != dataset_id:
                    raise CatalogError(
                        f"cannot link quality event to run from another dataset: {event.run_id!r}"
                    )
                run_row_id = run_row["id"]
            created_at = _iso(event.created_at or datetime.now().astimezone())
            self._connection.execute(
                """
                INSERT INTO quality_events (dataset_id, run_id, severity, code, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    dataset_id,
                    run_row_id,
                    event.severity.value,
                    event.code,
                    event.message,
                    created_at,
                ),
            )

    def list_quality_events(self, key: DatasetKey) -> tuple[QualityEventMetadata, ...]:
        dataset_id = self._get_dataset_id(key)
        if dataset_id is None:
            return ()
        rows = self._connection.execute(
            """
            SELECT qe.severity, qe.code, qe.message, qe.created_at, ir.run_id AS run_id
            FROM quality_events qe
            LEFT JOIN ingestion_runs ir ON ir.id = qe.run_id
            WHERE qe.dataset_id = ?
            ORDER BY qe.id
            """,
            (dataset_id,),
        ).fetchall()
        return tuple(
            QualityEventMetadata(
                severity=QualitySeverity(row["severity"]),
                code=row["code"],
                message=row["message"],
                run_id=row["run_id"],
                created_at=_parse_iso(row["created_at"]),
            )
            for row in rows
        )

    # -- maintenance ---------------------------------------------------

    def checkpoint(self, mode: str = "TRUNCATE") -> None:
        """Force a WAL checkpoint (e.g. before closing for a file swap)."""
        if mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise CatalogError(f"invalid WAL checkpoint mode: {mode!r}")
        self._connection.execute(f"PRAGMA wal_checkpoint({mode})")


def iter_all_file_metadata(catalog: Catalog) -> Iterable[FileMetadata]:
    """Yield every indexed file as `FileMetadata`, e.g. for a diff/validate pass."""
    for row in catalog.list_files():
        yield FileMetadata(
            dataset_key=row.dataset_key,
            relative_path=row.relative_path,
            year=row.year,
            month=row.month,
            row_count=row.row_count,
            min_timestamp=row.min_timestamp,
            max_timestamp=row.max_timestamp,
            physical_hash=row.physical_hash,
            schema_version=row.schema_version,
        )
