"""Canonical reference-price bar validation."""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl
from xret.data.errors import InvalidRequestError, XretDataError
from xret.data.models import ReferenceBarKey, ReferencePriceKind
from xret.data.schema import REFERENCE_BAR_SCHEMA
from xret.data.timeframe import TimeBar

if TYPE_CHECKING:
    from collections.abc import Sequence


def enforce_reference_bars(
    frame: pl.DataFrame,
    key: ReferenceBarKey,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    error_cls: type[XretDataError] = InvalidRequestError,
) -> None:
    """Fail unless ``frame`` is exact canonical data for ``key``."""
    if frame.schema != REFERENCE_BAR_SCHEMA:
        raise error_cls(f"schema does not match REFERENCE_BAR_SCHEMA: got {frame.schema!r}")
    identity = key.identity
    for column, expected in (
        ("exchange", identity.exchange),
        ("symbol", identity.symbol),
        ("market", identity.market.value),
        ("settle", identity.settle),
        ("timeframe", key.timeframe),
    ):
        if frame.get_column(column).unique().to_list() not in ([], [expected]):
            raise error_cls(f"column {column!r} must be uniformly {expected!r}")
    if frame.null_count().sum_horizontal().item():
        raise error_cls("reference bars must not contain nulls")
    timestamps: Sequence[datetime] = frame.get_column("timestamp").to_list()
    if any(left >= right for left, right in zip(timestamps, timestamps[1:], strict=False)):
        raise error_cls("reference timestamps must be strictly increasing")
    time_bar = TimeBar.parse(key.timeframe)
    if any(time_bar.floor(value) != value for value in timestamps):
        raise error_cls(f"reference timestamps must align to {key.timeframe}")
    if start is not None and any(value < start for value in timestamps):
        raise error_cls("reference bars contain rows before the request start")
    if end is not None and any(value >= end for value in timestamps):
        raise error_cls("reference bars contain rows at or after the request end")
    rows = frame.select("open", "high", "low", "close").iter_rows()
    positive = key.kind is not ReferencePriceKind.PREMIUM_INDEX
    for open_, high, low, close in rows:
        values = (open_, high, low, close)
        if any(not math.isfinite(value) for value in values):
            raise error_cls("reference OHLC values must be finite")
        if positive and any(value <= 0 for value in values):
            raise error_cls("mark and index OHLC values must be positive")
        if low > open_ or low > close or high < open_ or high < close or low > high:
            raise error_cls("reference OHLC ordering invariant is violated")
