"""Concrete, local-only facts used by dataset read verbs.

This module owns catalog snapshots, locally resolvable perpetual settlement,
and canonical Parquet selection.  It never applies strict or partial-read
policy, acquires locks, mutates storage, or contacts a provider.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

import polars as pl
from xret.data.errors import CatalogError, InvalidRequestError
from xret.data.models import (
    CoverageInterval,
    CoverageStatus,
    DatasetKey,
    Market,
    MarketIdentity,
    YearMonth,
)
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage import paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    Catalog,
    detect_incompatible_state,
)
from xret.data.timeframe import TimeBar


@dataclass(frozen=True, slots=True)
class LocalReadFacts:
    """Immutable local coverage facts for one resolved read request."""

    dataset_key: DatasetKey
    start: datetime
    end: datetime
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...]


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
    dataset_key: DatasetKey,
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
    finally:
        catalog.close()
    return LocalReadFacts(dataset_key, start, end, covered, gaps)


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
    time_bar = TimeBar.parse(facts.dataset_key.timeframe)
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
    bounds = pl.LazyFrame(
        {
            _COVERED_START: [interval.start for interval in facts.covered],
            _COVERED_END: [interval.end for interval in facts.covered],
        },
        schema={
            _COVERED_START: OHLCV_SCHEMA["timestamp"],
            _COVERED_END: OHLCV_SCHEMA["timestamp"],
        },
    )
    return (
        frame.sort("timestamp")
        .join_asof(bounds, left_on="timestamp", right_on=_COVERED_START, strategy="backward")
        .filter(pl.col(_COVERED_END).is_not_null() & (pl.col("timestamp") < pl.col(_COVERED_END)))
        .drop(_COVERED_START, _COVERED_END)
    )


def lazy_frame_for_facts(data_dir: Path, facts: LocalReadFacts) -> pl.LazyFrame:
    """Build the sorted lazy frame for catalog-covered canonical files."""
    required_paths = [
        paths.month_file_path(data_dir, facts.dataset_key, year_month)
        for year_month in _required_months(facts)
    ]
    missing = [path for path in required_paths if not path.is_file()]
    if missing:
        raise CatalogError(f"catalog coverage references missing canonical file: {missing[0]}")
    if not required_paths:
        return pl.DataFrame(schema=OHLCV_SCHEMA).lazy()
    combined = pl.concat([pl.scan_parquet(path) for path in required_paths], how="vertical")
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
            if key.exchange == identity.exchange
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
