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
)
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage import paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    Catalog,
    detect_incompatible_state,
)


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


def lazy_frame_for_facts(data_dir: Path, facts: LocalReadFacts) -> pl.LazyFrame:
    """Build the sorted lazy frame for catalog-covered canonical files."""
    required_months = {
        year_month
        for interval in facts.covered
        for year_month, _, _ in paths.iter_month_slices(interval.start, interval.end)
    }
    required_paths = [
        paths.month_file_path(data_dir, facts.dataset_key, year_month)
        for year_month in sorted(required_months, key=lambda value: (value.year, value.month))
    ]
    missing = [path for path in required_paths if not path.is_file()]
    if missing:
        raise CatalogError(f"catalog coverage references missing canonical file: {missing[0]}")
    if not required_paths:
        return pl.DataFrame(schema=OHLCV_SCHEMA).lazy()
    combined = pl.concat([pl.scan_parquet(path) for path in required_paths], how="vertical")
    return combined.filter(
        (pl.col("timestamp") >= facts.start) & (pl.col("timestamp") < facts.end)
    ).sort("timestamp")


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
