"""Self-describing, atomic monthly Parquet artifacts.

Parquet owns canonical rows and compact domain interpretation.  Operational
provenance, including run history and physical file hashes, belongs to SQLite.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import stat
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

import polars as pl
from xret.data.errors import CatalogError, InvalidRequestError, SyncError, XretDataError
from xret.data.models import NONE_SETTLE_SENTINEL, DatasetKey, Market, YearMonth
from xret.data.providers.contracts import DerivativeInterpretation
from xret.data.quality import enforce_canonical_ohlcv
from xret.data.schema import IDENTITY_COLUMNS, OHLCV_COLUMNS, OHLCV_SCHEMA
from xret.data.storage import paths

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from pathlib import Path

__all__ = [
    "SCHEMA_VERSION",
    "ProviderProvenance",
    "DerivativeInterpretation",
    "CommittedFile",
    "PreparedFile",
    "prepare_month",
    "publish_prepared_file",
    "discard_prepared_file",
    "read_month_file",
    "read_committed_file",
    "compute_content_hash",
    "cleanup_stale_temp_files",
    "split_by_year_month",
]

SCHEMA_VERSION: Final[int] = 5
_HASH_CHUNK_SIZE: Final[int] = 1024 * 1024
_META_SCHEMA_VERSION: Final[str] = "schema_version"
_META_EXCHANGE: Final[str] = "exchange"
_META_SYMBOL: Final[str] = "symbol"
_META_MARKET: Final[str] = "market"
_META_SETTLE: Final[str] = "settle"
_META_TIMEFRAME: Final[str] = "timeframe"
_META_YEAR: Final[str] = "year"
_META_MONTH: Final[str] = "month"
_META_ROW_COUNT: Final[str] = "row_count"
_META_MIN_TIMESTAMP: Final[str] = "min_timestamp"
_META_MAX_TIMESTAMP: Final[str] = "max_timestamp"
_META_PROVIDER_NAME: Final[str] = "provider_name"
_META_PROVIDER_VERSION: Final[str] = "provider_version"
_META_PROVIDER_API_VERSION: Final[str] = "provider_api_version"
_META_PROVIDER_MARKET_ID: Final[str] = "provider_market_id"
_META_NATIVE_SYMBOL: Final[str] = "native_symbol"
_META_LINEAR: Final[str] = "linear"
_META_INVERSE: Final[str] = "inverse"
_META_CONTRACT_SIZE: Final[str] = "contract_size"
_BASE_METADATA: Final[frozenset[str]] = frozenset(
    {
        _META_SCHEMA_VERSION,
        _META_EXCHANGE,
        _META_SYMBOL,
        _META_MARKET,
        _META_TIMEFRAME,
        _META_YEAR,
        _META_MONTH,
        _META_ROW_COUNT,
        _META_MIN_TIMESTAMP,
        _META_MAX_TIMESTAMP,
        _META_PROVIDER_NAME,
        _META_PROVIDER_VERSION,
        _META_PROVIDER_API_VERSION,
        _META_PROVIDER_MARKET_ID,
        _META_NATIVE_SYMBOL,
    }
)
_DERIVATIVE_METADATA: Final[frozenset[str]] = frozenset(
    {
        _META_LINEAR,
        _META_INVERSE,
        _META_CONTRACT_SIZE,
    }
)


@dataclass(frozen=True, slots=True)
class ProviderProvenance:
    """Provider snapshot for the latest publication of a canonical file."""

    name: str
    version: str
    api_version: int
    market_id: str
    native_symbol: str

    def __post_init__(self) -> None:
        for field in ("name", "version", "market_id", "native_symbol"):
            if not getattr(self, field):
                raise InvalidRequestError(f"provider {field} must not be empty")
        if not isinstance(self.api_version, int) or isinstance(self.api_version, bool):
            raise InvalidRequestError("provider api_version must be an integer")


@dataclass(frozen=True, slots=True)
class CommittedFile:
    dataset_key: DatasetKey
    year_month: YearMonth
    relative_path: str
    absolute_path: Path
    row_count: int
    min_timestamp: datetime
    max_timestamp: datetime
    physical_hash: str
    schema_version: int
    provider: ProviderProvenance


def _derivative_metadata(derivative: DerivativeInterpretation) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if derivative.linear is not None:
        metadata[_META_LINEAR] = str(derivative.linear).lower()
    if derivative.inverse is not None:
        metadata[_META_INVERSE] = str(derivative.inverse).lower()
    if derivative.contract_size is not None:
        metadata[_META_CONTRACT_SIZE] = derivative.contract_size
    return metadata


@dataclass(slots=True)
class PreparedFile:
    """A complete, deeply validated temp artifact awaiting publication."""

    committed_file: CommittedFile
    temp_path: Path
    data_dir: Path
    published: bool = False


def _invoke(hook: Callable[[str], None] | None, checkpoint: str) -> None:
    if hook is not None:
        hook(checkpoint)


def compute_content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_schema(
    frame: pl.DataFrame, *, source: str, error_cls: type[XretDataError] = SyncError
) -> None:
    if frame.schema != OHLCV_SCHEMA:
        raise error_cls(f"{source}: schema does not match OHLCV_SCHEMA: got {frame.schema!r}")


def read_month_file(path: Path) -> pl.DataFrame | None:
    if not path.is_file():
        return None
    frame = pl.read_parquet(path)
    _validate_schema(frame, source=str(path))
    return frame


def _row_bounds(
    frame: pl.DataFrame, *, error_cls: type[XretDataError] = SyncError
) -> tuple[int, datetime, datetime]:
    if frame.height == 0:
        raise error_cls("cannot commit an empty merged frame")
    return (
        frame.height,
        cast("datetime", frame.get_column("timestamp").min()),
        cast("datetime", frame.get_column("timestamp").max()),
    )


def _read_metadata(path: Path, *, error_cls: type[XretDataError]) -> dict[str, str]:
    try:
        return pl.read_parquet_metadata(path)
    except Exception as exc:  # noqa: BLE001
        raise error_cls(f"failed to read metadata for {path}: {exc}") from exc


def _identity_from_metadata(
    path: Path, metadata: dict[str, str]
) -> tuple[DatasetKey, YearMonth, int]:
    missing_base = _BASE_METADATA.difference(metadata)
    if missing_base:
        raise CatalogError(f"{path}: missing required metadata: {sorted(missing_base)!r}")
    try:
        market = Market(metadata[_META_MARKET])
    except ValueError as exc:
        raise CatalogError(f"{path}: invalid market metadata") from exc

    required = _BASE_METADATA | ({_META_SETTLE} if market is Market.PERPETUAL else set())
    allowed = required | (_DERIVATIVE_METADATA if market is Market.PERPETUAL else set())
    missing = required.difference(metadata)
    unknown = set(metadata).difference(allowed | {"ARROW:schema"})
    if missing:
        raise CatalogError(f"{path}: missing required metadata: {sorted(missing)!r}")
    if unknown:
        raise CatalogError(f"{path}: unsupported metadata keys: {sorted(unknown)!r}")
    try:
        schema_version = int(metadata[_META_SCHEMA_VERSION])
    except ValueError as exc:
        raise CatalogError(f"{path}: invalid schema version") from exc
    if schema_version != SCHEMA_VERSION:
        raise CatalogError(f"{path}: unsupported schema version")

    try:
        dataset_key = DatasetKey(
            exchange=metadata[_META_EXCHANGE],
            symbol=metadata[_META_SYMBOL],
            market=market,
            settle=metadata[_META_SETTLE] if market is Market.PERPETUAL else NONE_SETTLE_SENTINEL,
            timeframe=metadata[_META_TIMEFRAME],
        )
        year_month = YearMonth(year=int(metadata[_META_YEAR]), month=int(metadata[_META_MONTH]))
    except (KeyError, TypeError, ValueError, XretDataError) as exc:
        raise CatalogError(f"{path}: invalid identity metadata") from exc
    return dataset_key, year_month, schema_version


def _provider_from_metadata(
    path: Path,
    metadata: dict[str, str],
) -> ProviderProvenance:
    try:
        return ProviderProvenance(
            metadata[_META_PROVIDER_NAME],
            metadata[_META_PROVIDER_VERSION],
            int(metadata[_META_PROVIDER_API_VERSION]),
            metadata[_META_PROVIDER_MARKET_ID],
            metadata[_META_NATIVE_SYMBOL],
        )
    except (KeyError, TypeError, ValueError, XretDataError) as exc:
        raise CatalogError(f"{path}: invalid provider metadata") from exc


def _validate_frame_identity(
    dataset_key: DatasetKey,
    year_month: YearMonth,
    frame: pl.DataFrame,
    *,
    error_cls: type[XretDataError],
    source: str,
) -> None:
    expected_settle = None if dataset_key.market is Market.SPOT else dataset_key.settle
    for column, expected in (
        ("exchange", dataset_key.exchange),
        ("symbol", dataset_key.symbol),
        ("market", dataset_key.market.value),
        ("settle", expected_settle),
        ("timeframe", dataset_key.timeframe),
    ):
        if frame.get_column(column).unique().to_list() != [expected]:
            raise error_cls(f"{source}: column {column!r} must be uniformly {expected!r}")
    if frame.get_column("timestamp").null_count():
        raise error_cls(f"{source}: must not contain null timestamps")
    if frame.filter(
        (pl.col("timestamp").dt.year() != year_month.year)
        | (pl.col("timestamp").dt.month() != year_month.month)
    ).height:
        raise error_cls(f"{source}: contains rows outside {year_month}")


def _validate_storage_frame(
    dataset_key: DatasetKey,
    year_month: YearMonth,
    frame: pl.DataFrame,
    *,
    error_cls: type[XretDataError],
    source: str,
) -> None:
    """Composite storage validation: partition identity + canonical row invariants.

    ``_validate_frame_identity`` checks metadata/partition consistency only;
    ``enforce_canonical_ohlcv`` owns every row-level invariant including
    timestamp alignment.  Callsites must use this composite so the two
    validators are always paired.
    """
    _validate_frame_identity(dataset_key, year_month, frame, error_cls=error_cls, source=source)
    enforce_canonical_ohlcv(frame, dataset_key.timeframe, error_cls=error_cls)


def _validate_batch(dataset_key: DatasetKey, year_month: YearMonth, batch: pl.DataFrame) -> None:
    _validate_schema(batch, source="batch", error_cls=InvalidRequestError)
    if batch.height == 0:
        raise InvalidRequestError("batch must not be empty")
    _validate_storage_frame(
        dataset_key, year_month, batch, error_cls=InvalidRequestError, source="batch"
    )


def merge_frames(existing: pl.DataFrame | None, batch: pl.DataFrame) -> pl.DataFrame:
    parts = [existing, batch] if existing is not None else [batch]
    return (
        pl.concat(parts, how="vertical")
        .unique(subset=IDENTITY_COLUMNS, keep="last", maintain_order=True)
        .sort("timestamp")
        .select(OHLCV_COLUMNS)
    )


def split_by_year_month(batch: pl.DataFrame) -> list[tuple[YearMonth, pl.DataFrame]]:
    if batch.height == 0:
        return []
    tagged = batch.with_columns(
        year=pl.col("timestamp").dt.year(), month=pl.col("timestamp").dt.month()
    )
    return [
        (YearMonth(year=year, month=month), group.select(OHLCV_COLUMNS))
        for (year, month), group in tagged.group_by(["year", "month"], maintain_order=True)
    ]


def cleanup_stale_temp_files(directory: Path) -> list[Path]:
    removed: list[Path] = []
    for temp_path in paths.iter_temp_files(directory):
        temp_path.unlink(missing_ok=True)
        removed.append(temp_path)
    return removed


def _require_safe_managed_path(
    data_dir: Path, path: Path, *, error_cls: type[XretDataError]
) -> None:
    """Reject paths outside the data root or traversing managed symlinks."""
    try:
        if not paths.is_within(data_dir, path):
            raise error_cls(f"{path}: path is outside data directory {data_dir}")
        relative = path.absolute().relative_to(data_dir.absolute())
    except (OSError, RuntimeError) as exc:
        raise error_cls(f"failed to validate managed path {path}") from exc
    except ValueError as exc:
        raise error_cls(f"{path}: path is outside data directory {data_dir}") from exc

    current = data_dir.absolute()
    for part in relative.parts:
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise error_cls(f"failed to inspect managed path {current}") from exc
        else:
            if stat.S_ISLNK(mode):
                raise error_cls(f"{current}: managed path must not be a symlink")
        current /= part
    try:
        mode = os.lstat(current).st_mode
    except FileNotFoundError:
        return
    except OSError as exc:
        raise error_cls(f"failed to inspect managed path {current}") from exc
    if stat.S_ISLNK(mode):
        raise error_cls(f"{current}: managed path must not be a symlink")


def _fsync_directory(directory: Path) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _build_metadata(
    *,
    dataset_key: DatasetKey,
    year_month: YearMonth,
    row_count: int,
    min_timestamp: datetime,
    max_timestamp: datetime,
    provider: ProviderProvenance,
    derivative: DerivativeInterpretation | None,
) -> dict[str, str]:
    metadata = {
        _META_SCHEMA_VERSION: str(SCHEMA_VERSION),
        _META_EXCHANGE: dataset_key.exchange,
        _META_SYMBOL: dataset_key.symbol,
        _META_MARKET: dataset_key.market.value,
        _META_TIMEFRAME: dataset_key.timeframe,
        _META_YEAR: f"{year_month.year:04d}",
        _META_MONTH: f"{year_month.month:02d}",
        _META_ROW_COUNT: str(row_count),
        _META_MIN_TIMESTAMP: min_timestamp.isoformat(),
        _META_MAX_TIMESTAMP: max_timestamp.isoformat(),
        _META_PROVIDER_NAME: provider.name,
        _META_PROVIDER_VERSION: provider.version,
        _META_PROVIDER_API_VERSION: str(provider.api_version),
        _META_PROVIDER_MARKET_ID: provider.market_id,
        _META_NATIVE_SYMBOL: provider.native_symbol,
    }
    if dataset_key.market is Market.PERPETUAL:
        metadata[_META_SETTLE] = dataset_key.settle
        if derivative is not None:
            metadata.update(_derivative_metadata(derivative))
    elif derivative is not None:
        raise InvalidRequestError("derivative interpretation is only valid for perpetual markets")
    return metadata


def _derivative_metadata_for_rewrite(
    path: Path, derivative: DerivativeInterpretation | None
) -> dict[str, str]:
    existing: dict[str, str] = {}
    if path.is_file():
        existing = {
            key: value
            for key, value in _read_metadata(path, error_cls=SyncError).items()
            if key in _DERIVATIVE_METADATA and value
        }
    proposed = _derivative_metadata(derivative) if derivative is not None else {}
    for key, value in proposed.items():
        if key in existing and existing[key] != value:
            raise SyncError(f"{path}: conflicting derivative interpretation for {key!r}")
    return existing | proposed


def _validate_complete_artifact(
    path: Path,
    metadata: dict[str, str],
    row_count: int,
    min_timestamp: datetime,
    max_timestamp: datetime,
    *,
    dataset_key: DatasetKey,
    year_month: YearMonth,
) -> None:
    try:
        reopened = pl.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        raise SyncError(f"failed to reopen prepared artifact {path}: {exc}") from exc
    _validate_schema(reopened, source=str(path))
    _validate_storage_frame(
        dataset_key, year_month, reopened, error_cls=SyncError, source=str(path)
    )
    actual_count, actual_min, actual_max = _row_bounds(reopened)
    if (actual_count, actual_min, actual_max) != (row_count, min_timestamp, max_timestamp):
        raise SyncError(f"reopen validation failed for {path}: row bounds differ")
    actual_metadata = _read_metadata(path, error_cls=SyncError)
    for key, value in metadata.items():
        if actual_metadata.get(key) != value:
            raise SyncError(f"reopen validation failed for {path}: metadata {key!r} differs")


def prepare_month(
    data_dir: Path,
    dataset_key: DatasetKey,
    year_month: YearMonth,
    batch: pl.DataFrame,
    *,
    provider: ProviderProvenance,
    derivative: DerivativeInterpretation | None = None,
    crash_hook: Callable[[str], None] | None = None,
) -> PreparedFile:
    """Write, flush, fsync, reopen and deeply validate a same-directory temp file."""
    _validate_batch(dataset_key, year_month, batch)
    directory = paths.month_dir(data_dir, dataset_key, year_month)
    _require_safe_managed_path(data_dir, directory, error_cls=SyncError)
    directory.mkdir(parents=True, exist_ok=True)
    cleanup_stale_temp_files(directory)
    final_path = paths.month_file_path(data_dir, dataset_key, year_month)
    _require_safe_managed_path(data_dir, final_path, error_cls=SyncError)
    existing = None
    if final_path.is_file():
        committed = read_committed_file(data_dir, final_path)
        if committed.provider.name != provider.name:
            raise SyncError(
                f"{final_path}: source lineage is {committed.provider.name!r}, "
                f"not {provider.name!r}"
            )
        existing = read_month_file(final_path)
    if dataset_key.market is Market.SPOT and derivative is not None:
        raise InvalidRequestError("derivative interpretation is only valid for perpetual markets")
    derivative_metadata = _derivative_metadata_for_rewrite(final_path, derivative)
    merged = merge_frames(existing, batch)
    _validate_storage_frame(
        dataset_key, year_month, merged, error_cls=SyncError, source="merged artifact"
    )
    row_count, min_timestamp, max_timestamp = _row_bounds(merged)
    metadata = _build_metadata(
        dataset_key=dataset_key,
        year_month=year_month,
        row_count=row_count,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
        provider=provider,
        derivative=None,
    )
    metadata.update(derivative_metadata)
    temp_path = paths.new_temp_path(directory)
    _require_safe_managed_path(data_dir, temp_path, error_cls=SyncError)
    try:
        _invoke(crash_hook, "before_temp_write")
        _require_safe_managed_path(data_dir, temp_path, error_cls=SyncError)
        with temp_path.open("wb") as handle:
            merged.write_parquet(handle, metadata=metadata)
            try:
                handle.flush()
                os.fsync(handle.fileno())
            except OSError as exc:
                raise SyncError(f"failed to flush prepared artifact {temp_path}") from exc
        _invoke(crash_hook, "after_temp_write")
        _validate_complete_artifact(
            temp_path,
            metadata,
            row_count,
            min_timestamp,
            max_timestamp,
            dataset_key=dataset_key,
            year_month=year_month,
        )
        physical_hash = compute_content_hash(temp_path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return PreparedFile(
        CommittedFile(
            dataset_key,
            year_month,
            paths.relative_month_file_path(data_dir, dataset_key, year_month),
            final_path,
            row_count,
            min_timestamp,
            max_timestamp,
            physical_hash,
            SCHEMA_VERSION,
            provider,
        ),
        temp_path,
        data_dir,
    )


def publish_prepared_file(
    prepared: PreparedFile, *, crash_hook: Callable[[str], None] | None = None
) -> CommittedFile:
    """Atomically publish a prepared artifact; successful replace is irreversible."""
    if prepared.published:
        raise SyncError("prepared artifact has already been published")
    _require_safe_managed_path(prepared.data_dir, prepared.temp_path, error_cls=SyncError)
    _require_safe_managed_path(
        prepared.data_dir, prepared.committed_file.absolute_path, error_cls=SyncError
    )
    _invoke(crash_hook, "before_replace")
    os.replace(prepared.temp_path, prepared.committed_file.absolute_path)
    prepared.published = True
    _invoke(crash_hook, "after_replace")
    _fsync_directory(prepared.committed_file.absolute_path.parent)
    return prepared.committed_file


def discard_prepared_file(prepared: PreparedFile) -> None:
    """Discard only this unpublished direct-child owned temp artifact."""
    if prepared.published:
        return
    path = prepared.temp_path
    if path.parent != prepared.committed_file.absolute_path.parent or not paths.is_temp_file(path):
        raise SyncError("refusing to discard a non-owned temporary path")
    path.unlink(missing_ok=True)


def read_committed_file(data_dir: Path, path: Path) -> CommittedFile:
    """Read a current canonical file, deriving identity solely from its metadata."""
    _require_safe_managed_path(data_dir, path, error_cls=CatalogError)
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        frame = pl.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        raise CatalogError(f"failed to read {path}: {exc}") from exc
    _validate_schema(frame, source=str(path), error_cls=CatalogError)
    metadata = _read_metadata(path, error_cls=CatalogError)
    dataset_key, year_month, schema_version = _identity_from_metadata(path, metadata)
    provider = _provider_from_metadata(path, metadata)
    expected_path = paths.month_file_path(data_dir, dataset_key, year_month)
    _require_safe_managed_path(data_dir, expected_path, error_cls=CatalogError)
    if path.resolve() != expected_path.resolve():
        raise CatalogError(
            f"{path}: does not match canonical metadata-derived path {expected_path}"
        )
    _validate_storage_frame(
        dataset_key, year_month, frame, error_cls=CatalogError, source=str(path)
    )
    row_count, min_timestamp, max_timestamp = _row_bounds(frame, error_cls=CatalogError)
    expected = _build_metadata(
        dataset_key=dataset_key,
        year_month=year_month,
        row_count=row_count,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
        provider=provider,
        derivative=DerivativeInterpretation(
            metadata.get(_META_LINEAR) == "true" if _META_LINEAR in metadata else None,
            metadata.get(_META_INVERSE) == "true" if _META_INVERSE in metadata else None,
            metadata.get(_META_CONTRACT_SIZE),
        )
        if dataset_key.market is Market.PERPETUAL
        else None,
    )
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise CatalogError(f"{path}: metadata {key!r} does not match file rows")
    return CommittedFile(
        dataset_key,
        year_month,
        paths.relative_month_file_path(data_dir, dataset_key, year_month),
        path,
        row_count,
        min_timestamp,
        max_timestamp,
        compute_content_hash(path),
        schema_version,
        provider,
    )
