"""Canonical Polars schema and identity constants for OHLCV data.

This module defines the one on-disk/in-memory column contract shared by
the provider, storage and facade layers. Nothing here performs I/O.
"""

from __future__ import annotations

from typing import Final

import polars as pl

__all__ = [
    "OHLCV_SCHEMA",
    "SETTLED_FUNDING_SCHEMA",
    "REFERENCE_BAR_SCHEMA",
    "OPEN_INTEREST_SCHEMA",
    "OHLCV_COLUMNS",
    "IDENTITY_COLUMNS",
    "OHLC_COLUMNS",
]

#: Column order and dtypes for canonical trade-OHLCV Parquet files and frames
#: (IR-2). OHLC values summarize eligible executed trades, and `volume` is
#: base-asset quantity. `settle` is `null` exactly when `market="spot"`.
#: `timestamp` is the candle open time (inclusive interval start), millisecond
#: precision, UTC. There is no `run_id` row column (P-4, Decision 15):
#: operational provenance lives in `SyncResult`, file metadata and the catalog,
#: not mixed into the market observation schema.
OHLCV_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "exchange": pl.String(),
        "symbol": pl.String(),
        "market": pl.String(),
        "settle": pl.String(),
        "timeframe": pl.String(),
        "timestamp": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "open": pl.Float64(),
        "high": pl.Float64(),
        "low": pl.Float64(),
        "close": pl.Float64(),
        "volume": pl.Float64(),
    }
)

#: Column names in canonical order, derived from `OHLCV_SCHEMA`.
OHLCV_COLUMNS: Final[tuple[str, ...]] = tuple(OHLCV_SCHEMA.names())

#: Columns that jointly identify one candle row. A canonical dataset must
#: never contain two rows sharing all of these values. `settle` is `null`
#: for every `spot` row (IR-2): duplicate-identity checks over this tuple
#: must be null-aware, treating `null == null` as equal (verified against
#: Polars `unique` null-equality semantics, not assumed).
IDENTITY_COLUMNS: Final[tuple[str, ...]] = (
    "exchange",
    "symbol",
    "market",
    "settle",
    "timeframe",
    "timestamp",
)

#: The four price columns, in OHLC order, used by invariant checks
#: (e.g. `low <= open, close <= high`).

#: Exact canonical schemas for the three derivatives data families. These are
#: deliberately independent from trade OHLCV: funding is irregular, reference
#: bars have no volume, and open interest is a sampled gauge.
SETTLED_FUNDING_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "exchange": pl.String(),
        "symbol": pl.String(),
        "market": pl.String(),
        "settle": pl.String(),
        "effective_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "funding_rate": pl.Float64(),
        "funding_interval_seconds": pl.Int64(),
        "mark_price": pl.Float64(),
    }
)

REFERENCE_BAR_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "exchange": pl.String(),
        "symbol": pl.String(),
        "market": pl.String(),
        "settle": pl.String(),
        "timeframe": pl.String(),
        "timestamp": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "open": pl.Float64(),
        "high": pl.Float64(),
        "low": pl.Float64(),
        "close": pl.Float64(),
    }
)

OPEN_INTEREST_SCHEMA: Final[pl.Schema] = pl.Schema(
    {
        "exchange": pl.String(),
        "symbol": pl.String(),
        "market": pl.String(),
        "settle": pl.String(),
        "timeframe": pl.String(),
        "timestamp": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "open_interest_amount": pl.Float64(),
        "open_interest_value": pl.Float64(),
    }
)
OHLC_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close")
