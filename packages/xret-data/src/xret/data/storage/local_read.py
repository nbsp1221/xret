"""Concrete, local-only facts used by dataset read verbs.

This module owns catalog snapshots, locally resolvable perpetual settlement,
and canonical Parquet selection.  It never applies strict or partial-read
policy, acquires locks, mutates storage, or contacts a provider.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path, PurePosixPath

import polars as pl
from xret.data.errors import CatalogError, InvalidRequestError
from xret.data.models import (
    CoverageInterval,
    CoverageStatus,
    DatasetFamily,
    DatasetKey,
    Market,
    MarketIdentity,
    StorageKey,
    YearMonth,
    storage_identity,
)
from xret.data.schema import (
    OHLCV_SCHEMA,
    OPEN_INTEREST_SCHEMA,
    REFERENCE_BAR_SCHEMA,
    SETTLED_FUNDING_SCHEMA,
)
from xret.data.storage import paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    Catalog,
    detect_incompatible_state,
)
from xret.data.storage.parquet import _require_safe_managed_path
from xret.data.timeframe import TimeBar


@dataclass(frozen=True, slots=True)
class IndexedFileFacts:
    relative_path: str
    year_month: YearMonth
    physical_hash: str
    row_count: int = 1


@dataclass(frozen=True, slots=True)
class LocalReadFacts:
    """Immutable local coverage facts for one resolved read request."""

    dataset_key: StorageKey
    start: datetime
    end: datetime
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...]
    indexed_files: tuple[IndexedFileFacts, ...] = ()


def read_local_facts(
    state_dir: Path,
    data_dir: Path,
    identity: MarketIdentity,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> LocalReadFacts:
    """Resolve a local dataset identity and return its catalog coverage facts."""
    if identity.market is Market.PERPETUAL and identity.settle is None:
        identity = replace(
            identity,
            settle=_resolve_local_perpetual_settle(state_dir, identity, timeframe),
        )
    return read_local_facts_for_key(
        state_dir,
        data_dir,
        DatasetKey.from_identity(identity, timeframe=timeframe),
        start,
        end,
    )


def read_local_facts_for_key(
    state_dir: Path,
    data_dir: Path,
    dataset_key: StorageKey,
    start: datetime,
    end: datetime,
) -> LocalReadFacts:
    """Return coverage facts without creating or repairing local state."""
    db_path = state_dir / CATALOG_FILE_NAME
    if not db_path.is_file():
        if paths.classify_managed_storage(data_dir) != "empty":
            raise CatalogError("catalog is absent while managed storage evidence exists")
        return LocalReadFacts(
            dataset_key,
            start,
            end,
            (),
            (CoverageInterval(start, end, CoverageStatus.MISSING),),
        )
    if detect_incompatible_state(db_path):
        raise CatalogError(f"incompatible catalog state: {db_path}")
    catalog = Catalog.open_read_only(db_path)
    try:
        with catalog.snapshot():
            covered, gaps = catalog.coverage_and_gaps(dataset_key, start, end)
            indexed_files = tuple(
                IndexedFileFacts(
                    row.relative_path,
                    YearMonth(row.year, row.month),
                    row.physical_hash,
                    row.row_count,
                )
                for row in catalog.list_files(dataset_key)
            )
    finally:
        catalog.close()
    return LocalReadFacts(dataset_key, start, end, covered, gaps, indexed_files)


def _first_bar_start_at_or_after(time_bar: TimeBar, moment: datetime) -> datetime:
    """The earliest bar boundary that is not before `moment`."""
    floored = time_bar.floor(moment)
    return floored if floored == moment else time_bar.next_boundary(floored)


def _required_months(facts: LocalReadFacts) -> list[YearMonth]:
    """Month partitions that must hold rows for `facts.covered`.

    A partition is keyed by the month a bar *starts* in, so a month is only
    required when some bar actually starts inside it. A bar may span a month
    boundary -- a calendar week ending on 2024-02-05 starts on 2024-01-29 and
    is stored under January -- and then the covered interval reaches into a
    month that owns no bar and therefore has no file. Deriving requirements
    from elapsed time instead of bar starts demands that nonexistent file.
    """
    identity = storage_identity(facts.dataset_key)
    if identity.family is DatasetFamily.SETTLED_FUNDING:
        return []
    time_bar = TimeBar.parse(identity.timeframe)
    months: dict[tuple[int, int], YearMonth] = {}
    for interval in facts.covered:
        for year_month, slice_start, slice_end in paths.iter_month_slices(
            interval.start, interval.end
        ):
            if _first_bar_start_at_or_after(time_bar, slice_start) < slice_end:
                months[(year_month.year, year_month.month)] = year_month
    return [months[key] for key in sorted(months)]


#: Temporary join columns used to restrict rows to covered intervals.
_COVERED_START = "_covered_start"
_COVERED_END = "_covered_end"


def _family_schema_and_timestamp(facts: LocalReadFacts) -> tuple[pl.Schema, str]:
    family = storage_identity(facts.dataset_key).family
    if family is DatasetFamily.TRADE_BARS:
        return OHLCV_SCHEMA, "timestamp"
    if family is DatasetFamily.SETTLED_FUNDING:
        return SETTLED_FUNDING_SCHEMA, "effective_at"
    if family is DatasetFamily.REFERENCE_BARS:
        return REFERENCE_BAR_SCHEMA, "timestamp"
    return OPEN_INTEREST_SCHEMA, "timestamp"


def _restrict_to_covered(frame: pl.LazyFrame, facts: LocalReadFacts) -> pl.LazyFrame:
    """Keep only rows inside `facts.covered`.

    `Catalog.coverage_and_gaps` returns disjoint intervals in ascending order,
    clipped to the requested range, so an as-of join answers membership in one
    pass: the latest interval starting at or before a row decides it. A
    per-interval boolean union would instead cost one comparison pair per
    interval per row, which a sparse dataset makes prohibitive.

    Filtering by coverage rather than by the request keeps `data` consistent
    with the reported `covered` and `gaps`. A month file can hold rows the
    catalog does not currently cover, for example when a sync published
    Parquet and then failed before recording coverage.
    """
    schema, timestamp = _family_schema_and_timestamp(facts)
    bounds = pl.LazyFrame(
        {
            _COVERED_START: [interval.start for interval in facts.covered],
            _COVERED_END: [interval.end for interval in facts.covered],
        },
        schema={
            _COVERED_START: schema[timestamp],
            _COVERED_END: schema[timestamp],
        },
    )
    return (
        frame.sort(timestamp)
        .join_asof(bounds, left_on=timestamp, right_on=_COVERED_START, strategy="backward")
        .filter(pl.col(_COVERED_END).is_not_null() & (pl.col(timestamp) < pl.col(_COVERED_END)))
        .drop(_COVERED_START, _COVERED_END)
    )


def _verified_frame(data_dir: Path, facts: LocalReadFacts, item: IndexedFileFacts) -> pl.DataFrame:
    relative = PurePosixPath(item.relative_path)
    if (
        relative.is_absolute()
        or relative.as_posix() != item.relative_path
        or any(part in ("", ".", "..") for part in relative.parts)
    ):
        raise CatalogError(f"catalog contains an unsafe canonical path: {item.relative_path!r}")
    expected = paths.relative_month_file_path(data_dir, facts.dataset_key, item.year_month)
    if item.relative_path != expected:
        raise CatalogError(
            f"catalog path {item.relative_path!r} does not match canonical path {expected!r}"
        )
    path = data_dir.joinpath(*relative.parts)
    _require_safe_managed_path(data_dir, path, error_cls=CatalogError)
    if not path.is_file():
        raise CatalogError(f"catalog coverage references missing canonical file: {path}")
    try:
        with path.open("rb") as handle:
            digest = hashlib.sha256()
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
            if digest.hexdigest() != item.physical_hash:
                raise CatalogError(f"catalog physical hash differs from canonical file: {path}")
            handle.seek(0)
            frame = pl.read_parquet(handle)
            if frame.height != item.row_count:
                raise CatalogError(f"catalog row count differs from canonical file: {path}")
            return frame
    except CatalogError:
        raise
    except Exception as exc:
        raise CatalogError(f"failed to read canonical file {path}: {exc}") from exc


def lazy_frame_for_facts(data_dir: Path, facts: LocalReadFacts) -> pl.LazyFrame:
    """Build a sorted lazy query over verified, stable canonical snapshots."""
    schema, _ = _family_schema_and_timestamp(facts)
    family = storage_identity(facts.dataset_key).family
    if family is DatasetFamily.SETTLED_FUNDING:
        covered_months = {
            year_month
            for interval in facts.covered
            for year_month, _slice_start, _slice_end in paths.iter_month_slices(
                interval.start, interval.end
            )
        }
        required = tuple(item for item in facts.indexed_files if item.year_month in covered_months)
    else:
        required_months = set(_required_months(facts))
        required = tuple(item for item in facts.indexed_files if item.year_month in required_months)
        indexed_months = {item.year_month for item in required}
        missing_months = required_months - indexed_months
        if missing_months:
            missing = min(missing_months, key=lambda item: (item.year, item.month))
            raise CatalogError(
                "catalog coverage references missing canonical file: "
                f"{paths.month_file_path(data_dir, facts.dataset_key, missing)}"
            )
    if not required:
        return pl.DataFrame(schema=schema).lazy()
    empty_required = next((item for item in required if item.row_count == 0), None)
    if empty_required is not None:
        raise CatalogError(
            "catalog coverage references an empty canonical artifact: "
            f"{empty_required.relative_path}"
        )
    frames = [_verified_frame(data_dir, facts, item).lazy() for item in required]
    combined = pl.concat(frames, how="vertical")
    return _restrict_to_covered(combined, facts)


def _resolve_local_perpetual_settle(
    state_dir: Path, identity: MarketIdentity, timeframe: str
) -> str:
    """Resolve an omitted perpetual settlement from the local catalog only."""
    db_path = state_dir / CATALOG_FILE_NAME
    message = (
        f"settle is required to read perpetual dataset "
        f"{identity.exchange}/{identity.symbol} (timeframe {timeframe!r}) locally: "
    )
    if not db_path.is_file():
        raise InvalidRequestError(message + "no local catalog exists to infer it from")
    catalog = Catalog.open_read_only(db_path)
    try:
        candidates = {
            key.settle
            for key in catalog.list_datasets()
            if isinstance(key, DatasetKey)
            and key.exchange == identity.exchange
            and key.symbol == identity.symbol
            and key.market is Market.PERPETUAL
            and key.timeframe == timeframe
        }
    finally:
        catalog.close()
    if len(candidates) != 1:
        raise InvalidRequestError(
            message
            + f"{len(candidates)} locally known settlement(s) found; pass settle= explicitly"
        )
    return next(iter(candidates))
