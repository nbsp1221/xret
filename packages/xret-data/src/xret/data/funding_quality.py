"""Canonical settled-funding row validation."""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING

import polars as pl
from xret.data.errors import InvalidRequestError, XretDataError
from xret.data.models import SettledFundingKey
from xret.data.schema import SETTLED_FUNDING_SCHEMA

if TYPE_CHECKING:
    from collections.abc import Sequence


def enforce_settled_funding(
    frame: pl.DataFrame,
    key: SettledFundingKey,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    error_cls: type[XretDataError] = InvalidRequestError,
) -> None:
    """Fail unless ``frame`` is exact, ordered canonical settled funding."""
    if frame.schema != SETTLED_FUNDING_SCHEMA:
        raise error_cls(f"schema does not match SETTLED_FUNDING_SCHEMA: got {frame.schema!r}")
    _identity(frame, key, error_cls)
    if (
        frame.select(pl.all().exclude("funding_interval_seconds", "mark_price").null_count())
        .sum_horizontal()
        .item()
    ):
        raise error_cls("settled funding required columns must not contain nulls")
    timestamps: Sequence[datetime] = frame.get_column("effective_at").to_list()
    if any(left >= right for left, right in zip(timestamps, timestamps[1:], strict=False)):
        raise error_cls("settled funding effective_at values must be strictly increasing")
    if start is not None and any(value < start for value in timestamps):
        raise error_cls("settled funding contains rows before the request start")
    if end is not None and any(value >= end for value in timestamps):
        raise error_cls("settled funding contains rows at or after the request end")
    if any(not math.isfinite(value) for value in frame.get_column("funding_rate")):
        raise error_cls("funding_rate must be finite")
    intervals = frame.get_column("funding_interval_seconds").drop_nulls()
    if any(value <= 0 for value in intervals):
        raise error_cls("funding_interval_seconds must be positive when present")
    marks = frame.get_column("mark_price").drop_nulls()
    if any(not math.isfinite(value) or value <= 0 for value in marks):
        raise error_cls("mark_price must be positive and finite when present")


def _identity(frame: pl.DataFrame, key: SettledFundingKey, error_cls: type[XretDataError]) -> None:
    identity = key.identity
    for column, expected in (
        ("exchange", identity.exchange),
        ("symbol", identity.symbol),
        ("market", identity.market.value),
        ("settle", identity.settle),
    ):
        if frame.get_column(column).unique().to_list() not in ([], [expected]):
            raise error_cls(f"column {column!r} must be uniformly {expected!r}")
