"""Canonical historical open-interest validation."""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl
from xret.data.errors import InvalidRequestError, XretDataError
from xret.data.models import OpenInterestKey
from xret.data.schema import OPEN_INTEREST_SCHEMA
from xret.data.timeframe import TimeBar

if TYPE_CHECKING:
    from collections.abc import Sequence


def enforce_open_interest(
    frame: pl.DataFrame,
    key: OpenInterestKey,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    error_cls: type[XretDataError] = InvalidRequestError,
) -> None:
    """Fail unless ``frame`` is exact canonical sampled open interest."""
    if frame.schema != OPEN_INTEREST_SCHEMA:
        raise error_cls(f"schema does not match OPEN_INTEREST_SCHEMA: got {frame.schema!r}")
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
    if frame.select(pl.all().exclude("open_interest_value").null_count()).sum_horizontal().item():
        raise error_cls("open-interest required columns must not contain nulls")
    timestamps: Sequence[datetime] = frame.get_column("timestamp").to_list()
    if any(left >= right for left, right in zip(timestamps, timestamps[1:], strict=False)):
        raise error_cls("open-interest timestamps must be strictly increasing")
    time_bar = TimeBar.parse(key.timeframe)
    if any(time_bar.floor(value) != value for value in timestamps):
        raise error_cls(f"open-interest timestamps must align to {key.timeframe}")
    if start is not None and any(value < start for value in timestamps):
        raise error_cls("open interest contains rows before the request start")
    if end is not None and any(value >= end for value in timestamps):
        raise error_cls("open interest contains rows at or after the request end")
    for column in ("open_interest_amount", "open_interest_value"):
        values = frame.get_column(column).drop_nulls()
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise error_cls(f"{column} must be finite and nonnegative")
