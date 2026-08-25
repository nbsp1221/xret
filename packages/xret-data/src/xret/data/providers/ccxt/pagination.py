"""CCXT OHLCV pagination with explicit observation evidence.

Returned candles and observed time are different facts.  A successful bounded
page proves its half-open request window was observed even when it contains no
candles; rows alone never prove an arbitrary tail was observed.

Known endpoint policies produce exhaustive bounded evidence. Unknown endpoint
families use a conservative row-driven strategy: validated rows prove only
their own bar intervals, never absent time around them.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from xret.data.errors import ProviderError
from xret.data.providers.ccxt.compatibility import ObservationProfile, WindowParameterFormat
from xret.data.providers.contracts import ObservedWindow
from xret.data.timeframe import TimeBar

RawOHLCVRow = Sequence[float]
WindowParameter = int | str
PageFetcher = Callable[[int | None, int | None, dict[str, WindowParameter]], list[list[float]]]


@dataclass(frozen=True, slots=True)
class PaginationResult:
    """Raw rows plus the exact windows proved by successful provider calls."""

    rows: tuple[tuple[float, ...], ...]
    observed: tuple[ObservedWindow, ...]


DEFAULT_GENERIC_MAX_PAGES = 10_000
DEFAULT_GENERIC_PAGE_LIMIT = 1000


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _rfc3339_milliseconds(value: int) -> str:
    return (
        datetime.fromtimestamp(value / 1000, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _request_window(
    profile: ObservationProfile,
    *,
    start_ms: int,
    end_ms: int,
) -> tuple[int | None, dict[str, WindowParameter]]:
    native = profile.native_window_parameters
    if native is not None:
        if native.format is WindowParameterFormat.RFC3339_MILLISECONDS:
            return None, {
                native.start: _rfc3339_milliseconds(start_ms),
                native.end: _rfc3339_milliseconds(end_ms - 1),
            }
        raise ProviderError(f"unsupported native CCXT window format: {native.format!r}")

    params: dict[str, WindowParameter] = {}
    if profile.send_unified_until:
        params["until"] = end_ms - 1 if profile.until_inclusive else end_ms
    return start_ms, params


def _advance(
    time_bar: TimeBar,
    start: datetime,
    bars: int,
    end: datetime,
    max_span: timedelta | None,
) -> tuple[datetime, int]:
    cursor = start
    traversed = 0
    for _ in range(bars):
        next_cursor = time_bar.next_boundary(cursor)
        if max_span is not None and next_cursor - start > max_span:
            break
        cursor = next_cursor
        traversed += 1
        if cursor >= end:
            return end, traversed
    return cursor, traversed


def _validate_page(
    rows: Sequence[RawOHLCVRow],
    *,
    native_symbol: str,
    exchange_id: str,
    window_start_ms: int,
    window_end_ms: int,
    accept_end_boundary: bool,
) -> None:
    """Reject a page that does not honor the bounded window it answers.

    Shape comes first: a value that cannot be read as a timestamp cannot be
    ordered or range-checked. The timestamp is coerced once and reused for both
    later checks, so every conversion failure is attributed as a malformed
    candle instead of escaping as a raw ``ValueError`` or ``OverflowError``.
    Coercion matches what this module already applies to collected rows, so the
    validator and the collector agree on what a timestamp is.

    A row outside the window proves the venue ignored ``until``, and a response
    that spans more than the window cannot prove the window was observed
    exhaustively, so the traversal must not treat it as evidence.
    """
    previous: int | None = None
    for row in rows:
        try:
            if len(row) < 6:
                raise ValueError(f"expected at least 6 OHLCV fields, got {len(row)}")
            timestamp_ms = int(float(row[0]))
            for value in row[1:6]:
                float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ProviderError(
                f"fetchOHLCV returned a malformed candle for {native_symbol} on {exchange_id}"
            ) from exc
        if previous is not None and timestamp_ms < previous:
            raise ProviderError(
                f"fetchOHLCV returned non-ascending candles for {native_symbol} on {exchange_id}"
            )
        within_window = window_start_ms <= timestamp_ms < window_end_ms
        is_known_end_boundary = accept_end_boundary and timestamp_ms == window_end_ms
        if not within_window and not is_known_end_boundary:
            raise ProviderError(
                f"fetchOHLCV returned a candle outside the requested window for "
                f"{native_symbol} on {exchange_id}: "
                f"{_describe_ms(timestamp_ms)} not in "
                f"[{_describe_ms(window_start_ms)}, {_describe_ms(window_end_ms)})"
            )
        previous = timestamp_ms


def _describe_ms(value: int) -> str:
    """Epoch milliseconds, with an ISO rendering when one exists.

    A venue emitting microsecond or nanosecond epochs is a real vendor bug, and
    formatting must not replace the attribution with a `datetime` range error.
    """
    try:
        return f"{datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()} ({value}ms)"
    except (OverflowError, OSError, ValueError):
        return f"{value}ms"


def _bounded_ohlcv(
    *,
    profile: ObservationProfile,
    exchange_id: str,
    native_symbol: str,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    requested_limit: int,
    fetch_page: PageFetcher,
) -> PaginationResult:
    """Traverse every configured bounded page in ``[start, end)``.

    The cursor advances by the provider window, never by the last returned
    candle.  Therefore an empty successful page is evidence for that page and
    cannot hide later data.
    """
    if requested_limit <= 0:
        raise ProviderError(f"page limit must be positive, got {requested_limit!r}")
    effective_limit = min(requested_limit, profile.max_bars)
    cursor = start
    collected: list[tuple[float, ...]] = []
    observed: list[ObservedWindow] = []

    while cursor < end:
        page_end, page_bars = _advance(
            time_bar,
            cursor,
            effective_limit,
            end,
            profile.max_span,
        )
        if page_end <= cursor:
            raise ProviderError(
                f"pagination made no progress for {native_symbol} on {exchange_id}: "
                f"cursor={cursor.isoformat()}"
            )
        start_ms = _epoch_ms(cursor)
        end_ms = _epoch_ms(page_end)
        # CCXT's unified `until` denotes the latest candle to fetch, but native
        # adapters disagree on whether it is inclusive. Translate Xret's
        # half-open page using the endpoint policy so a full page
        # contains at most `effective_limit` candle boundaries.
        request_since, params = _request_window(profile, start_ms=start_ms, end_ms=end_ms)
        request_limit = (
            min(profile.max_bars, page_bars + int(profile.accept_end_boundary))
            if profile.send_page_limit
            else None
        )
        batch = fetch_page(request_since, request_limit, params)
        _validate_page(
            batch,
            native_symbol=native_symbol,
            exchange_id=exchange_id,
            window_start_ms=start_ms,
            window_end_ms=end_ms,
            accept_end_boundary=profile.accept_end_boundary,
        )
        collected.extend(
            tuple(float(value) for value in row[:6]) for row in batch if int(float(row[0])) < end_ms
        )
        observed.append(ObservedWindow(cursor, page_end))
        cursor = page_end

    return PaginationResult(rows=tuple(collected), observed=tuple(observed))


def _coerce_row(
    row: RawOHLCVRow,
    *,
    native_symbol: str,
    exchange_id: str,
) -> tuple[float, ...]:
    try:
        if len(row) < 6:
            raise ValueError(f"expected at least 6 OHLCV fields, got {len(row)}")
        values = tuple(float(value) for value in row[:6])
        int(values[0])
        return values
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProviderError(
            f"fetchOHLCV returned a malformed candle for {native_symbol} on {exchange_id}"
        ) from exc


def _row_windows(
    rows: tuple[tuple[float, ...], ...],
    *,
    time_bar: TimeBar,
) -> tuple[ObservedWindow, ...]:
    windows: list[ObservedWindow] = []
    for row in rows:
        timestamp = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
        if time_bar.floor(timestamp) != timestamp:
            raise ProviderError(
                f"fetchOHLCV returned a timestamp not aligned to {time_bar}: "
                f"{timestamp.isoformat()}"
            )
        end = time_bar.next_boundary(timestamp)
        if windows and windows[-1].end == timestamp:
            windows[-1] = ObservedWindow(windows[-1].start, end)
        else:
            windows.append(ObservedWindow(timestamp, end))
    return tuple(windows)


def _generic_ohlcv(
    *,
    exchange_id: str,
    native_symbol: str,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    requested_limit: int,
    fetch_page: PageFetcher,
    provider_limit: int | None,
    max_pages: int,
) -> PaginationResult:
    if requested_limit <= 0:
        raise ProviderError(f"page limit must be positive, got {requested_limit!r}")
    if max_pages <= 0:
        raise ProviderError(f"generic max pages must be positive, got {max_pages!r}")
    effective_limit = min(
        requested_limit,
        provider_limit if provider_limit is not None else DEFAULT_GENERIC_PAGE_LIMIT,
    )
    if effective_limit <= 0:
        raise ProviderError(f"provider page limit must be positive, got {provider_limit!r}")
    request_bars = _bounded_bar_count(
        time_bar,
        start,
        end,
        cap=max_pages * effective_limit,
    )
    page_budget = min(
        (request_bars + effective_limit - 1) // effective_limit + 2,
        max_pages,
    )

    start_ms = _epoch_ms(start)
    end_ms = _epoch_ms(end)
    cursor_ms = start_ms
    previous_page_max: int | None = None
    collected: dict[int, tuple[float, ...]] = {}
    first_page = True

    for _page_number in range(page_budget):
        batch = fetch_page(cursor_ms, effective_limit, {})
        if not batch:
            if first_page:
                latest = fetch_page(None, 1, {})
                latest_in_range: list[int] = []
                latest_previous: int | None = None
                for raw in latest:
                    row = _coerce_row(
                        raw,
                        native_symbol=native_symbol,
                        exchange_id=exchange_id,
                    )
                    timestamp_ms = int(row[0])
                    if latest_previous is not None and timestamp_ms < latest_previous:
                        raise ProviderError(
                            f"fetchOHLCV returned non-ascending candles for "
                            f"{native_symbol} on {exchange_id}"
                        )
                    latest_previous = timestamp_ms
                    if start_ms <= timestamp_ms < end_ms:
                        existing = collected.get(timestamp_ms)
                        if existing is not None and existing != row:
                            raise ProviderError(
                                f"fetchOHLCV returned conflicting candles at "
                                f"{_describe_ms(timestamp_ms)} for "
                                f"{native_symbol} on {exchange_id}"
                            )
                        collected[timestamp_ms] = row
                        latest_in_range.append(timestamp_ms)
                if latest_in_range:
                    cursor_ms = min(latest_in_range)
                    first_page = False
                    continue
            break

        page_rows: list[tuple[float, ...]] = []
        page_previous: int | None = None
        reached_end = False
        for raw in batch:
            row = _coerce_row(
                raw,
                native_symbol=native_symbol,
                exchange_id=exchange_id,
            )
            timestamp_ms = int(row[0])
            if page_previous is not None and timestamp_ms < page_previous:
                raise ProviderError(
                    f"fetchOHLCV returned non-ascending candles for "
                    f"{native_symbol} on {exchange_id}"
                )
            page_previous = timestamp_ms
            allowed_overlap = previous_page_max is not None and timestamp_ms == previous_page_max
            if timestamp_ms < cursor_ms and not allowed_overlap:
                raise ProviderError(
                    f"fetchOHLCV ignored since for {native_symbol} on {exchange_id}: "
                    f"{_describe_ms(timestamp_ms)} < {_describe_ms(cursor_ms)}"
                )
            if timestamp_ms >= end_ms:
                reached_end = True
                continue
            if timestamp_ms < start_ms:
                raise ProviderError(
                    f"fetchOHLCV returned a candle before the request for "
                    f"{native_symbol} on {exchange_id}"
                )
            existing = collected.get(timestamp_ms)
            if existing is not None and existing != row:
                raise ProviderError(
                    f"fetchOHLCV returned conflicting candles at "
                    f"{_describe_ms(timestamp_ms)} for {native_symbol} on {exchange_id}"
                )
            collected[timestamp_ms] = row
            page_rows.append(row)

        if not page_rows:
            if reached_end:
                break
            raise ProviderError(
                f"pagination made no progress for {native_symbol} on {exchange_id}: "
                f"cursor={_describe_ms(cursor_ms)}"
            )
        page_max = max(int(row[0]) for row in page_rows)
        next_timestamp = time_bar.next_boundary(datetime.fromtimestamp(page_max / 1000, tz=UTC))
        next_cursor_ms = _epoch_ms(next_timestamp)
        if next_cursor_ms <= cursor_ms:
            raise ProviderError(
                f"pagination made no progress for {native_symbol} on {exchange_id}: "
                f"cursor={_describe_ms(cursor_ms)}"
            )
        previous_page_max = page_max
        cursor_ms = next_cursor_ms
        first_page = False
        if reached_end or cursor_ms >= end_ms:
            break

    rows = tuple(collected[timestamp] for timestamp in sorted(collected))
    return PaginationResult(rows=rows, observed=_row_windows(rows, time_bar=time_bar))


def _bounded_bar_count(
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    *,
    cap: int,
) -> int:
    """Count request intervals without materializing or traversing beyond a safety cap."""
    count = 0
    cursor = start
    while cursor < end and count < cap:
        cursor = time_bar.next_boundary(cursor)
        count += 1
    return count


def paginate_ohlcv(
    *,
    profile: ObservationProfile | None,
    exchange_id: str,
    native_symbol: str,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    requested_limit: int,
    fetch_page: PageFetcher,
    provider_limit: int | None = None,
    generic_max_pages: int = DEFAULT_GENERIC_MAX_PAGES,
) -> PaginationResult:
    """Use exhaustive bounded pagination when known, otherwise presence-only pagination."""
    if profile is not None:
        return _bounded_ohlcv(
            profile=profile,
            exchange_id=exchange_id,
            native_symbol=native_symbol,
            time_bar=time_bar,
            start=start,
            end=end,
            requested_limit=requested_limit,
            fetch_page=fetch_page,
        )
    return _generic_ohlcv(
        exchange_id=exchange_id,
        native_symbol=native_symbol,
        time_bar=time_bar,
        start=start,
        end=end,
        requested_limit=requested_limit,
        fetch_page=fetch_page,
        provider_limit=provider_limit,
        max_pages=generic_max_pages,
    )
