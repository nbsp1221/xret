"""Read-only catalog validation and file-provable catalog recovery."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from xret.data.errors import CatalogError
from xret.data.models import (
    CatalogRebuildResult,
    CatalogValidationResult,
    CoverageStatus,
    DatasetKey,
    YearMonth,
)
from xret.data.storage import paths
from xret.data.storage.catalog import (
    Catalog,
    CoverageSegment,
    FileMetadata,
    detect_incompatible_state,
)
from xret.data.storage.locking import catalog_gate
from xret.data.storage.parquet import SCHEMA_VERSION as PARQUET_SCHEMA_VERSION
from xret.data.storage.parquet import read_committed_file, read_month_file
from xret.data.timeframe import TimeBar

if TYPE_CHECKING:
    from xret.data.config import MarketDataConfig

__all__ = [
    "CommittedFileLike",
    "FileSource",
    "GateFactory",
    "discover_committed_files",
    "default_gate_factory",
    "validate_catalog_state",
    "rebuild_catalog_state",
    "RecoveryService",
]


@runtime_checkable
class CommittedFileLike(Protocol):
    """The file facts recovery is permitted to persist."""

    dataset_key: DatasetKey
    year_month: YearMonth
    relative_path: str
    absolute_path: Path
    row_count: int
    min_timestamp: datetime
    max_timestamp: datetime
    physical_hash: str
    schema_version: int


FileSource = Callable[[], Iterable[CommittedFileLike]]
GateFactory = Callable[[Path], AbstractContextManager[object]]


def discover_committed_files(config: MarketDataConfig) -> Iterator[CommittedFileLike]:
    """Deeply validate every canonical current-layout Parquet file."""
    for path in paths.iter_canonical_files(config.data_dir):
        yield cast("CommittedFileLike", read_committed_file(config.data_dir, path))


def default_gate_factory(state_dir: Path) -> AbstractContextManager[object]:
    return catalog_gate(state_dir)


def _files(data_dir: Path, source: FileSource) -> list[CommittedFileLike]:
    try:
        supplied = list(source())
    except CatalogError:
        raise
    except Exception as exc:
        raise CatalogError(f"failed to discover committed files: {exc}") from exc
    files: list[CommittedFileLike] = []
    seen: set[str] = set()
    for supplied_file in supplied:
        try:
            file = read_committed_file(data_dir, supplied_file.absolute_path)
        except CatalogError:
            raise
        except Exception as exc:
            raise CatalogError(
                f"failed to read committed file: {supplied_file.absolute_path}"
            ) from exc
        if file.schema_version != PARQUET_SCHEMA_VERSION:
            raise CatalogError(f"unsupported Parquet schema for {file.relative_path}")
        if file.relative_path in seen:
            raise CatalogError(f"duplicate canonical file path: {file.relative_path}")
        seen.add(file.relative_path)
        files.append(cast("CommittedFileLike", file))
    return files


def _read_only_catalog(db_path: Path) -> Catalog:
    return Catalog.open_read_only(db_path)


def _state(db_path: Path) -> str:
    if not db_path.exists():
        return "absent"
    if not db_path.is_file():
        return "unreadable"
    if not detect_incompatible_state(db_path):
        return "current"
    return "incompatible"


def _validate(catalog: Catalog, files: list[CommittedFileLike]) -> CatalogValidationResult:
    indexed = {row.relative_path: row for row in catalog.list_files()}
    discovered = {file.relative_path: file for file in files}
    issues: list[str] = []
    keys: set[DatasetKey] = set()
    for relative_path, row in indexed.items():
        keys.add(row.dataset_key)
        file = discovered.get(relative_path)
        if file is None:
            issues.append(f"missing canonical file indexed in catalog: {relative_path}")
            continue
        if file.dataset_key != row.dataset_key:
            issues.append(f"dataset identity mismatch for {relative_path}")
        if file.physical_hash != row.physical_hash:
            issues.append(
                f"physical SHA mismatch for {relative_path}: "
                f"catalog={row.physical_hash} disk={file.physical_hash}"
            )
        if file.schema_version != row.schema_version:
            issues.append(
                f"schema version mismatch for {relative_path}: "
                f"catalog={row.schema_version} disk={file.schema_version}"
            )
        if file.row_count != row.row_count:
            issues.append(
                f"row count mismatch for {relative_path}: "
                f"catalog={row.row_count} disk={file.row_count}"
            )
        if file.min_timestamp != row.min_timestamp or file.max_timestamp != row.max_timestamp:
            issues.append(f"row bounds mismatch for {relative_path}")
        if file.year_month.year != row.year or file.year_month.month != row.month:
            issues.append(f"year/month mismatch for {relative_path}")
    for relative_path, file in discovered.items():
        keys.add(file.dataset_key)
        if relative_path not in indexed:
            issues.append(f"orphan canonical file not indexed in catalog: {relative_path}")
    return CatalogValidationResult(
        is_valid=not issues,
        checked_datasets=tuple(sorted(keys, key=_key_sort)),
        issues=tuple(issues),
    )


def _key_sort(key: DatasetKey) -> tuple[str, str, str, str, str]:
    return key.exchange, key.symbol, key.market.value, key.settle, key.timeframe


def validate_catalog_state(
    db_path: Path,
    config: MarketDataConfig,
    *,
    file_source: FileSource | None = None,
) -> CatalogValidationResult:
    """Compare current catalog facts with canonical Parquet without mutation."""
    state = _state(db_path)
    storage_state = paths.classify_managed_storage(config.data_dir)
    if storage_state == "ambiguous":
        raise CatalogError("managed storage evidence is ambiguous; refusing validation")
    files = _files(config.data_dir, file_source or (lambda: discover_committed_files(config)))
    if state == "absent":
        if files:
            return CatalogValidationResult(
                is_valid=False,
                checked_datasets=tuple(sorted(_group(files), key=_key_sort)),
                issues=(
                    "canonical files exist but the catalog is absent; "
                    "rebuild_catalog() is required",
                ),
            )
        return CatalogValidationResult(is_valid=True)
    if state != "current":
        return CatalogValidationResult(
            is_valid=False,
            issues=(f"catalog at {db_path} is {state}; rebuild_catalog() may replace it",),
        )
    catalog = _read_only_catalog(db_path)
    try:
        return _validate(catalog, files)
    finally:
        catalog.close()


def _group(files: Iterable[CommittedFileLike]) -> dict[DatasetKey, list[CommittedFileLike]]:
    grouped: dict[DatasetKey, list[CommittedFileLike]] = {}
    for file in files:
        grouped.setdefault(file.dataset_key, []).append(file)
    return grouped


def _available_segments(data_dir: Path, file: CommittedFileLike) -> tuple[CoverageSegment, ...]:
    committed = read_committed_file(data_dir, file.absolute_path)
    if (
        committed.dataset_key != file.dataset_key
        or committed.year_month != file.year_month
        or committed.relative_path != file.relative_path
        or committed.physical_hash != file.physical_hash
    ):
        raise CatalogError(f"canonical file changed while deriving coverage: {file.relative_path}")
    try:
        frame = read_month_file(file.absolute_path)
        if frame is None:
            raise CatalogError(
                f"canonical file disappeared while deriving coverage: {file.relative_path}"
            )
        timestamps = frame.get_column("timestamp").to_list()
    except CatalogError:
        raise
    except Exception as exc:
        raise CatalogError(f"failed to read canonical timestamps: {file.relative_path}") from exc
    if len(timestamps) != file.row_count:
        raise CatalogError(
            f"canonical file row count changed while deriving coverage: {file.relative_path}"
        )
    try:
        time_bar = TimeBar.parse(file.dataset_key.timeframe)
    except Exception as exc:
        raise CatalogError(
            f"cannot derive candle boundary from timeframe: {file.dataset_key.timeframe!r}"
        ) from exc
    segments: list[CoverageSegment] = []
    previous: datetime | None = None
    run_start: datetime | None = None
    for timestamp in timestamps:
        if not isinstance(timestamp, datetime):
            raise CatalogError(f"canonical file has invalid timestamp: {file.relative_path}")
        if previous is not None and timestamp <= previous:
            raise CatalogError(
                f"canonical file timestamps are not strictly increasing: {file.relative_path}"
            )
        if previous is None:
            run_start = timestamp
        else:
            expected_end = time_bar.next_boundary(previous)
            if timestamp != expected_end:
                assert run_start is not None
                segments.append(CoverageSegment(run_start, expected_end, CoverageStatus.AVAILABLE))
                run_start = timestamp
        previous = timestamp
    if run_start is not None and previous is not None:
        segments.append(
            CoverageSegment(run_start, time_bar.next_boundary(previous), CoverageStatus.AVAILABLE)
        )
    return tuple(segments)


def _replace_derived_state(catalog: Catalog, data_dir: Path, files: list[CommittedFileLike]) -> int:
    grouped = _group(files)
    with catalog.transaction():
        catalog.connection.execute("DELETE FROM datasets")
        for key, dataset_files in grouped.items():
            segments: list[CoverageSegment] = []
            for file in sorted(dataset_files, key=lambda item: item.relative_path):
                catalog.record_file(
                    FileMetadata(
                        key,
                        file.relative_path,
                        file.year_month.year,
                        file.year_month.month,
                        file.row_count,
                        file.min_timestamp,
                        file.max_timestamp,
                        file.physical_hash,
                        file.schema_version,
                    )
                )
                segments.extend(_available_segments(data_dir, file))
            catalog.set_coverage(key, segments)
    return len(files)


def _sidecars_present(db_path: Path) -> bool:
    return any(db_path.with_name(db_path.name + suffix).exists() for suffix in ("-wal", "-shm"))


def _build_temp_catalog(
    parent: Path, name: str, data_dir: Path, files: list[CommittedFileLike]
) -> Path:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{name}.rebuild-", dir=parent)
    os.close(fd)
    temp_path = Path(temporary_name)
    temp_path.unlink()
    catalog = Catalog.open(temp_path)
    try:
        _replace_derived_state(catalog, data_dir, files)
    finally:
        catalog.close()
    if _sidecars_present(temp_path):
        raise CatalogError("temporary catalog retained SQLite sidecars; refusing replacement")
    verify = _read_only_catalog(temp_path)
    try:
        result = _validate(verify, files)
    finally:
        verify.close()
    if not result.is_valid:
        raise CatalogError("rebuilt catalog failed self-validation: " + "; ".join(result.issues))
    return temp_path


def rebuild_catalog_state(
    db_path: Path,
    config: MarketDataConfig,
    *,
    file_source: FileSource | None = None,
    gate_factory: GateFactory | None = None,
) -> CatalogRebuildResult:
    """Replace only derived facts from deeply validated current Parquet files."""
    gate = gate_factory or default_gate_factory
    with gate(config.state_dir):
        sidecars_present = _sidecars_present(db_path)
        if sidecars_present and (not db_path.is_file() or detect_incompatible_state(db_path)):
            raise CatalogError(f"catalog at {db_path} has SQLite sidecars; refusing replacement")
        state = _state(db_path)
        if state != "current":
            if sidecars_present:
                raise CatalogError(
                    f"catalog at {db_path} has SQLite sidecars; refusing replacement"
                )
            storage_state = paths.classify_managed_storage(config.data_dir)
            if storage_state == "ambiguous":
                raise CatalogError("managed storage evidence is ambiguous; refusing rebuild")
        files = _files(config.data_dir, file_source or (lambda: discover_committed_files(config)))
        if state == "current":
            catalog = Catalog.open(db_path)
            try:
                recovered = _replace_derived_state(catalog, config.data_dir, files)
            finally:
                catalog.close()
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path: Path | None = None
            try:
                temp_path = _build_temp_catalog(
                    db_path.parent, db_path.name, config.data_dir, files
                )
                os.replace(temp_path, db_path)
                temp_path = None
                certified = _read_only_catalog(db_path)
                try:
                    result = _validate(certified, files)
                finally:
                    certified.close()
                if not result.is_valid:
                    raise CatalogError(
                        "replaced catalog failed certification: " + "; ".join(result.issues)
                    )
                recovered = len(files)
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
        return CatalogRebuildResult(
            rebuilt_datasets=tuple(sorted(_group(files), key=_key_sort)),
            recovered_files=recovered,
            reset_datasets=(),
            warnings=(),
        )


@dataclass(slots=True)
class RecoveryService:
    db_path: Path
    config: MarketDataConfig
    file_source: FileSource | None = None
    gate_factory: GateFactory | None = None

    def validate(self) -> CatalogValidationResult:
        return validate_catalog_state(self.db_path, self.config, file_source=self.file_source)

    def rebuild(self) -> CatalogRebuildResult:
        return rebuild_catalog_state(
            self.db_path, self.config, file_source=self.file_source, gate_factory=self.gate_factory
        )

    def reopen(self) -> Catalog:
        return Catalog.open(self.db_path)
