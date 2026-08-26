"""Strict row-driven pagination for CCXT historical reference bars."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from xret.data.errors import ProviderError
from xret.data.providers.contracts import ObservedWindow
from xret.data.timeframe import TimeBar

ReferencePage = Callable[[int | None, int | None, dict[str, int | str]], list[list[float]]]


@dataclass(frozen=True, slots=True)
class ReferencePaginationResult:
    rows: tuple[tuple[float, float, float, float, float], ...]
    observed: tuple[ObservedWindow, ...]


def _row(raw: Sequence[object], *, exchange_id: str) -> tuple[float, float, float, float, float]:
    try:
        if len(raw) < 5:
            raise ValueError("expected timestamp and OHLC")
        values = tuple(float(cast("Any", value)) for value in raw[:5])
        timestamp = int(values[0])
        if values[0] != timestamp or not all(math.isfinite(value) for value in values):
            raise ValueError("non-finite or fractional timestamp")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError(
            f"reference OHLCV route returned a malformed candle on {exchange_id}"
        ) from exc
    return (values[0], values[1], values[2], values[3], values[4])


def paginate_reference_history(
    *,
    exchange_id: str,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    page_limit: int,
    fetch_page: ReferencePage,
    max_pages: int = 10_000,
) -> ReferencePaginationResult:
    """Traverse a since-based route conservatively; returned intervals alone are evidence."""
    if page_limit <= 0 or max_pages <= 0:
        raise ProviderError("reference pagination limits must be positive")
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    cursor = start_ms
    rows: list[tuple[float, float, float, float, float]] = []
    seen: set[int] = set()
    previous: int | None = None
    exhausted = False
    for _ in range(max_pages):
        batch = fetch_page(cursor, page_limit, {})
        if not isinstance(batch, list):
            raise ProviderError("reference OHLCV route must return a list")
        if not batch:
            exhausted = True
            break
        page_progress = False
        for raw in batch:
            row = _row(raw, exchange_id=exchange_id)
            timestamp = int(row[0])
            if previous is not None and timestamp <= previous:
                message = "duplicate" if timestamp == previous else "non-ascending"
                raise ProviderError(
                    f"reference OHLCV route returned {message} candles on {exchange_id}"
                )
            previous = timestamp
            if timestamp < cursor:
                raise ProviderError(f"reference OHLCV route ignored since on {exchange_id}")
            if timestamp >= end_ms:
                exhausted = True
                continue
            if timestamp < start_ms:
                raise ProviderError("reference OHLCV route returned a candle before the request")
            moment = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
            if time_bar.floor(moment) != moment:
                raise ProviderError(
                    f"reference OHLCV route returned a timestamp not aligned to {time_bar}"
                )
            if timestamp in seen:
                raise ProviderError(
                    f"reference OHLCV route returned duplicate candles on {exchange_id}"
                )
            seen.add(timestamp)
            rows.append(row)
            page_progress = True
        if exhausted:
            break
        if not page_progress:
            raise ProviderError(f"reference pagination made no progress on {exchange_id}")
        latest = int(rows[-1][0])
        cursor = int(
            time_bar.next_boundary(datetime.fromtimestamp(latest / 1000, tz=UTC)).timestamp() * 1000
        )
        if cursor >= end_ms:
            exhausted = True
            break
    if not exhausted:
        raise ProviderError(f"reference pagination exceeded its page budget on {exchange_id}")
    windows: list[ObservedWindow] = []
    for row in rows:
        timestamp = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
        boundary = time_bar.next_boundary(timestamp)
        if windows and windows[-1].end == timestamp:
            windows[-1] = ObservedWindow(windows[-1].start, boundary)
        else:
            windows.append(ObservedWindow(timestamp, boundary))
    return ReferencePaginationResult(tuple(rows), tuple(windows))
