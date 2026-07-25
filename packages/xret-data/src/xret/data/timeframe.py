"""Single calendar-aware timeframe grammar and time-input parsing (P-8).

Internal module. `TimeBar` consolidates the three divergent parsers that
previously existed in `provider.parse_timeframe` (30-day `M`
approximation), `recovery._timeframe_duration` (could not parse `M`), and
`quality._check_timeframe_gaps` (fixed-duration diffs). Public APIs convert the
`timeframe="1h"` convenience string to `TimeBar` internally; `TimeBar` itself is
not a top-level export.

Units are case-sensitive (Decision 11):

- `s`: second
- `m`: minute
- `h`: hour
- `d`: day
- `w`: calendar week (Monday 00:00 UTC anchored)
- `M`: calendar month (1st-of-month 00:00 UTC anchored)

`w` and `M` are true calendar units, not fixed-duration approximations:
`floor`/`next_boundary` honor real week/month boundaries (including
variable month length and year rollover), never a fixed 7- or 30-day
duration.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from xret.data.errors import InvalidRequestError

__all__: list[str] = []

#: Canonical `<amount><unit>` grammar. Case-sensitive; no word aliases
#: (`1min`), no arbitrary casing (`1H`), no bare numbers (`60`).
_TIMEFRAME_PATTERN = re.compile(r"^(?P<amount>[1-9][0-9]*)(?P<unit>[smhdwM])$")

#: Fixed-duration units, in seconds. `w`/`M` are calendar units and are
#: handled separately by `TimeBar.floor`/`_step`.
_FIXED_UNIT_SECONDS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}

_CALENDAR_UNITS = frozenset({"w", "M"})
_VALID_UNITS = frozenset(_FIXED_UNIT_SECONDS) | _CALENDAR_UNITS

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _ensure_utc(value: datetime, *, field_name: str = "datetime") -> datetime:
    """Reject naive datetimes; normalize offset-bearing ones to UTC (Decision 10)."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidRequestError(f"{field_name} must be timezone-aware: {value!r}")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class TimeBar:
    """One canonical `<amount><unit>` time-bar specification.

    Construct via `TimeBar.parse("1h")` (or `TimeBar(amount=1, unit="h")`
    directly). Calendar units (`w`, `M`) only support `amount=1`: `2w` or
    `3M` are not part of the canonical grammar.
    """

    amount: int
    unit: str

    def __post_init__(self) -> None:
        if self.unit not in _VALID_UNITS:
            raise InvalidRequestError(
                f"unrecognized timeframe unit: {self.unit!r}; expected one of "
                f"{sorted(_VALID_UNITS)!r} (case-sensitive)"
            )
        if self.amount <= 0:
            raise InvalidRequestError(f"timeframe amount must be positive: {self.amount!r}")
        if self.unit in _CALENDAR_UNITS and self.amount != 1:
            raise InvalidRequestError(
                f"calendar timeframes only support amount=1: {self.amount}{self.unit}"
            )

    @classmethod
    def parse(cls, timeframe: str) -> TimeBar:
        """Parse a canonical `<amount><unit>` string, e.g. `"1m"`, `"4h"`, `"1M"`."""
        match = _TIMEFRAME_PATTERN.match(timeframe)
        if match is None:
            raise InvalidRequestError(
                f"invalid timeframe syntax: {timeframe!r}; expected "
                "'<amount><unit>' with unit in s/m/h/d/w/M (case-sensitive)"
            )
        return cls(amount=int(match.group("amount")), unit=match.group("unit"))

    def __str__(self) -> str:
        return f"{self.amount}{self.unit}"

    @property
    def is_calendar(self) -> bool:
        """Whether this timeframe uses true calendar (variable-length) boundaries."""
        return self.unit in _CALENDAR_UNITS

    @property
    def fixed_step_ms(self) -> int | None:
        """Step in milliseconds for fixed-duration units; None for calendar units."""
        if self.unit in _FIXED_UNIT_SECONDS:
            return self.amount * _FIXED_UNIT_SECONDS[self.unit] * 1000
        return None

    def floor(self, value: datetime) -> datetime:
        """The latest boundary at or before `value` (UTC)."""
        value = _ensure_utc(value)
        if self.unit == "M":
            return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if self.unit == "w":
            midnight = value.replace(hour=0, minute=0, second=0, microsecond=0)
            return midnight - timedelta(days=midnight.weekday())
        step = timedelta(seconds=self.amount * _FIXED_UNIT_SECONDS[self.unit])
        elapsed_steps = (value - _EPOCH) // step
        return _EPOCH + elapsed_steps * step

    def next_boundary(self, value: datetime) -> datetime:
        """The earliest boundary strictly after `value` (UTC).

        Equivalent to `floor(value)` advanced by one step: if `value` is
        already exactly on a boundary, this returns the *next* one, never
        `value` itself.
        """
        return self._step(self.floor(value))

    def _step(self, boundary: datetime) -> datetime:
        """Advance an aligned `boundary` by exactly one bar."""
        if self.unit == "M":
            if boundary.month == 12:
                return boundary.replace(year=boundary.year + 1, month=1)
            return boundary.replace(month=boundary.month + 1)
        if self.unit == "w":
            return boundary + timedelta(days=7)
        return boundary + timedelta(seconds=self.amount * _FIXED_UNIT_SECONDS[self.unit])

    def iter_intervals(self, start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
        """Yield successive half-open `[bar_start, bar_end)` pairs covering `[start, end)`.

        `start` must already sit on a boundary of this `TimeBar`; callers
        that only have an arbitrary `start` should `floor` it first if
        alignment is not otherwise guaranteed.
        """
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        if start >= end:
            raise InvalidRequestError(
                f"start must be strictly before end: start={start!r} end={end!r}"
            )
        floored = self.floor(start)
        if floored != start:
            raise InvalidRequestError(
                f"start is not aligned to a {self} boundary: {start!r} (nearest floor: {floored!r})"
            )
        intervals: list[tuple[datetime, datetime]] = []
        cursor = start
        while cursor < end:
            nxt = self._step(cursor)
            intervals.append((cursor, nxt))
            cursor = nxt
        return intervals


def validate_range(start: datetime, end: datetime) -> None:
    """Validate a half-open `[start, end)` range (Decision 10): both bounds
    UTC-aware (naive rejected), `start` strictly before `end`.
    """
    _ensure_utc(start, field_name="start")
    _ensure_utc(end, field_name="end")
    if start >= end:
        raise InvalidRequestError(f"start must be strictly before end: start={start!r} end={end!r}")


def parse_time_input(value: str | datetime) -> datetime:
    """Parse a `start`/`end` time input (Decision 10).

    Accepts a timezone-aware `datetime` (normalized to UTC; naive is
    rejected) or a string: a plain ISO date (`"2024-01-01"`, meaning UTC
    midnight) or an offset-bearing ISO timestamp (normalized to UTC).
    """
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, str):
        return _parse_time_string(value)
    raise InvalidRequestError(f"time input must be a str or datetime, got {value!r}")


def _parse_time_string(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise InvalidRequestError(f"invalid time string: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidRequestError(
            f"time string must be a plain ISO date or an offset-bearing timestamp, "
            f"not a naive datetime string: {value!r}"
        )
    return parsed.astimezone(UTC)
