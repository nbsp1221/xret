"""Deterministic pagination for CCXT unified settled-funding history."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from xret.data.errors import ProviderError
from xret.data.providers.contracts import ObservedWindow


@dataclass(frozen=True, slots=True)
class FundingPaginationResult:
    rows: tuple[tuple[int, float, int | None, float | None], ...]
    observed: tuple[ObservedWindow, ...]
    duplicate_count: int = 0


def _epoch_ms_ceiling(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    microseconds = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return -(-microseconds // 1000)


def _number(value: object, *, field: str, nullable: bool = False) -> float | None:
    if value is None or value == "":
        if nullable:
            return None
        raise ProviderError(f"CCXT funding record has no {field}")
    if isinstance(value, bool):
        raise ProviderError(f"CCXT funding record has malformed {field}")
    try:
        result = float(str(value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError(f"CCXT funding record has malformed {field}") from exc
    if not math.isfinite(result):
        raise ProviderError(f"CCXT funding record has non-finite {field}")
    return result


def _interval_seconds(record: Mapping[str, Any]) -> int | None:
    direct = record.get("fundingIntervalSeconds")
    info = record.get("info")
    if direct is None and isinstance(info, Mapping):
        direct = info.get("fundingIntervalSeconds")
    hours = None
    if direct is None:
        hours = record.get("fundingIntervalHours")
        if hours is None and isinstance(info, Mapping):
            hours = info.get("fundingIntervalHours")
    if direct in (None, "") and hours in (None, ""):
        return None
    if direct in (None, ""):
        try:
            hours_value = int(str(hours))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ProviderError("CCXT funding record has malformed funding interval") from exc
        direct = hours_value * 3600
    if isinstance(direct, bool):
        raise ProviderError("CCXT funding record has malformed funding interval")
    try:
        value = int(str(direct))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError("CCXT funding record has malformed funding interval") from exc
    if value <= 0:
        raise ProviderError("CCXT funding record has nonpositive funding interval")
    return value


def _record(raw: object) -> tuple[int, float, int | None, float | None]:
    if not isinstance(raw, Mapping):
        raise ProviderError("CCXT funding history rows must be mappings")
    record = cast("Mapping[str, Any]", raw)
    timestamp = record.get("timestamp")
    if isinstance(timestamp, bool):
        raise ProviderError("CCXT funding record has malformed timestamp")
    try:
        timestamp_ms = int(str(timestamp))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError("CCXT funding record has malformed timestamp") from exc
    rate = _number(record.get("fundingRate"), field="fundingRate")
    info = record.get("info")
    mark_value = record.get("markPrice")
    if mark_value is None and isinstance(info, Mapping):
        mark_value = info.get("markPrice")
    mark = _number(mark_value, field="markPrice", nullable=True)
    assert rate is not None
    if mark is not None and mark <= 0:
        raise ProviderError("CCXT funding record has nonpositive markPrice")
    return timestamp_ms, rate, _interval_seconds(record), mark


def paginate_funding_history(
    *,
    exchange_id: str,
    start: datetime,
    end: datetime,
    page_limit: int,
    fetch_page: Callable[[int, int, dict[str, int | str]], list[dict[str, Any]]],
) -> FundingPaginationResult:
    """Traverse ascending unified history, reconciling inclusive overlaps.

    Binance's unified history route accepts an inclusive ``until`` bound and
    empty/short terminal pages exhaust the requested span. Other CCXT routes
    conservatively prove only the span from the request start through one
    millisecond after their greatest returned event.
    """
    if page_limit <= 0:
        raise ProviderError("funding page limit must be positive")
    start_ms, end_ms = _epoch_ms_ceiling(start), _epoch_ms_ceiling(end)
    cursor = start_ms
    by_timestamp: dict[int, tuple[int, float, int | None, float | None]] = {}
    duplicates = 0
    calls = 0
    terminal = False
    while cursor < end_ms:
        calls += 1
        if calls > 1_000_000:
            raise ProviderError("CCXT funding pagination exceeded its request bound")
        try:
            raw_page = fetch_page(cursor, page_limit, {"until": end_ms - 1})
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError(f"fetchFundingRateHistory failed on {exchange_id}: {exc}") from exc
        if not isinstance(raw_page, list):
            raise ProviderError("CCXT funding history page must be a list")
        page = [_record(item) for item in raw_page]
        if any(left[0] > right[0] for left, right in zip(page, page[1:], strict=False)):
            raise ProviderError("CCXT funding history page is not ascending")
        maximum: int | None = None
        for row in page:
            timestamp = row[0]
            if timestamp < start_ms or timestamp >= end_ms:
                raise ProviderError("CCXT funding history ignored requested bounds")
            existing = by_timestamp.get(timestamp)
            if existing is not None:
                if existing != row:
                    raise ProviderError("CCXT funding history contains conflicting duplicates")
                duplicates += 1
            else:
                by_timestamp[timestamp] = row
            maximum = timestamp if maximum is None else max(maximum, timestamp)
        if len(page) < page_limit:
            terminal = True
            break
        if maximum is None or maximum < cursor:
            raise ProviderError("CCXT funding pagination made no progress")
        next_cursor = maximum + 1
        if next_cursor <= cursor:
            raise ProviderError("CCXT funding pagination made no progress")
        cursor = next_cursor
    rows = tuple(by_timestamp[key] for key in sorted(by_timestamp))
    if exchange_id in {"binance", "binanceusdm"} and (terminal or cursor >= end_ms):
        observed = (ObservedWindow(start, end),)
    elif rows:
        observed_end_ms = min(end_ms, rows[-1][0] + 1)
        observed_end = datetime.fromtimestamp(observed_end_ms / 1000, tz=start.tzinfo)
        observed = (ObservedWindow(start, observed_end),)
    else:
        observed = ()
    return FundingPaginationResult(rows, observed, duplicates)
