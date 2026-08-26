"""Deterministic pagination and lossless normalization for CCXT open interest."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from xret.data.errors import ProviderError
from xret.data.providers.contracts import ObservedWindow
from xret.data.timeframe import TimeBar

OpenInterestPage = Callable[[str, int, int, dict[str, int | str]], list[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class OpenInterestPaginationResult:
    rows: tuple[tuple[int, float, float | None], ...]
    observed: tuple[ObservedWindow, ...]
    duplicate_count: int = 0


def _decimal(value: object, *, field: str, nullable: bool = False) -> Decimal | None:
    if value is None or value == "":
        if nullable:
            return None
        raise ProviderError(f"CCXT open-interest record has no {field}")
    if isinstance(value, bool):
        raise ProviderError(f"CCXT open-interest record has malformed {field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ProviderError(f"CCXT open-interest record has malformed {field}") from exc
    if not result.is_finite() or result < 0:
        raise ProviderError(f"CCXT open-interest record has invalid {field}")
    return result


def _float_once(value: Decimal, *, field: str) -> float:
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise ProviderError(f"CCXT open-interest {field} overflows Float64") from exc
    if not math.isfinite(result):
        raise ProviderError(f"CCXT open-interest {field} overflows Float64")
    return 0.0 if result == 0.0 else result


def _record(raw: object) -> tuple[int, float, float | None]:
    if not isinstance(raw, Mapping):
        raise ProviderError("CCXT open-interest history rows must be mappings")
    record = cast("Mapping[str, Any]", raw)
    timestamp = record.get("timestamp")
    if isinstance(timestamp, bool):
        raise ProviderError("CCXT open-interest record has malformed timestamp")
    try:
        timestamp_ms = int(str(timestamp))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError("CCXT open-interest record has malformed timestamp") from exc
    amount = _decimal(record.get("openInterestAmount"), field="openInterestAmount")
    value = _decimal(record.get("openInterestValue"), field="openInterestValue", nullable=True)
    assert amount is not None
    return (
        timestamp_ms,
        _float_once(amount, field="base amount"),
        None if value is None else _float_once(value, field="quote value"),
    )


def paginate_open_interest_history(
    *,
    exchange_id: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    page_limit: int,
    fetch_page: OpenInterestPage,
    max_pages: int = 10_000,
) -> OpenInterestPaginationResult:
    """Traverse unified OI history while proving only returned sample slots.

    Empty/short pages and old ranges may be caused by retention, so they never
    prove absent samples. Returned rows prove only their own grid slots.
    """
    if page_limit <= 0 or max_pages <= 0:
        raise ProviderError("open-interest pagination limits must be positive")
    time_bar = TimeBar.parse(timeframe)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    cursor = start_ms
    rows: dict[int, tuple[int, float, float | None]] = {}
    duplicates = 0
    exhausted = False
    for _ in range(max_pages):
        if cursor >= end_ms:
            exhausted = True
            break
        try:
            raw_page = fetch_page(timeframe, cursor, page_limit, {"until": end_ms - 1})
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError(f"fetchOpenInterestHistory failed on {exchange_id}: {exc}") from exc
        if not isinstance(raw_page, list):
            raise ProviderError("CCXT open-interest history page must be a list")
        if not raw_page:
            exhausted = True
            break
        page = [_record(raw) for raw in raw_page]
        if any(left[0] > right[0] for left, right in zip(page, page[1:], strict=False)):
            raise ProviderError("CCXT open-interest history page is not ascending")
        maximum: int | None = None
        for row in page:
            timestamp = row[0]
            if timestamp < start_ms or timestamp >= end_ms:
                raise ProviderError("CCXT open-interest history ignored requested bounds")
            moment = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
            if time_bar.floor(moment) != moment:
                raise ProviderError(f"CCXT open-interest timestamp does not align to {timeframe}")
            previous = rows.get(timestamp)
            if previous is not None:
                if previous != row:
                    raise ProviderError(
                        "CCXT open-interest history contains conflicting duplicates"
                    )
                duplicates += 1
            else:
                rows[timestamp] = row
            maximum = timestamp if maximum is None else max(maximum, timestamp)
        if len(page) < page_limit:
            exhausted = True
            break
        if maximum is None or maximum < cursor:
            raise ProviderError("CCXT open-interest pagination made no progress")
        next_cursor = maximum + 1
        if next_cursor <= cursor:
            raise ProviderError("CCXT open-interest pagination made no progress")
        cursor = next_cursor
    if not exhausted:
        raise ProviderError("CCXT open-interest pagination exceeded its page budget")
    ordered = tuple(rows[key] for key in sorted(rows))
    observed = tuple(
        ObservedWindow(
            datetime.fromtimestamp(row[0] / 1000, tz=UTC),
            time_bar.next_boundary(datetime.fromtimestamp(row[0] / 1000, tz=UTC)),
        )
        for row in ordered
    )
    return OpenInterestPaginationResult(ordered, observed, duplicates)
