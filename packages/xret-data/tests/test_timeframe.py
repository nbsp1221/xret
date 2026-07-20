"""Deterministic unit tests for the calendar-aware `TimeBar` grammar (P-8)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from xret.data.errors import InvalidRequestError
from xret.data.timeframe import TimeBar, parse_time_input, validate_range

# --------------------------------------------------------------------------
# Grammar / parsing
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("timeframe", "amount", "unit"),
    [
        ("1s", 1, "s"),
        ("1m", 1, "m"),
        ("5m", 5, "m"),
        ("4h", 4, "h"),
        ("1d", 1, "d"),
        ("1w", 1, "w"),
        ("1M", 1, "M"),
    ],
)
def test_parses_canonical_timeframes(timeframe: str, amount: int, unit: str) -> None:
    bar = TimeBar.parse(timeframe)

    assert bar.amount == amount
    assert bar.unit == unit
    assert str(bar) == timeframe


def test_1m_and_1M_are_distinct() -> None:
    minute = TimeBar.parse("1m")
    month = TimeBar.parse("1M")

    assert minute != month
    assert minute.unit != month.unit


@pytest.mark.parametrize(
    "timeframe",
    [
        "1min",  # word alias
        "1H",  # wrong casing
        "60",  # bare number, no unit
        "m1",  # reversed
        "1",  # missing unit
        "s",  # missing amount
        "0m",  # non-positive amount
        "-1m",  # negative
        "1x",  # unrecognized unit
        "",  # empty
        "1 m",  # whitespace
    ],
)
def test_rejects_non_canonical_timeframe_syntax(timeframe: str) -> None:
    with pytest.raises(InvalidRequestError):
        TimeBar.parse(timeframe)


@pytest.mark.parametrize("timeframe", ["2w", "3M"])
def test_calendar_units_reject_amount_other_than_one(timeframe: str) -> None:
    with pytest.raises(InvalidRequestError):
        TimeBar.parse(timeframe)


def test_direct_construction_validates_same_as_parse() -> None:
    with pytest.raises(InvalidRequestError):
        TimeBar(amount=1, unit="H")


# --------------------------------------------------------------------------
# floor / next_boundary: fixed-duration units
# --------------------------------------------------------------------------


def test_minute_floor_truncates_to_boundary() -> None:
    bar = TimeBar.parse("5m")
    value = datetime(2024, 1, 1, 0, 7, 30, tzinfo=UTC)

    assert bar.floor(value) == datetime(2024, 1, 1, 0, 5, tzinfo=UTC)


def test_minute_next_boundary_after_exact_boundary_advances() -> None:
    bar = TimeBar.parse("5m")
    on_boundary = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)

    assert bar.next_boundary(on_boundary) == datetime(2024, 1, 1, 0, 10, tzinfo=UTC)


def test_hour_floor_and_next_boundary() -> None:
    bar = TimeBar.parse("1h")
    value = datetime(2024, 1, 1, 13, 45, tzinfo=UTC)

    assert bar.floor(value) == datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
    assert bar.next_boundary(value) == datetime(2024, 1, 1, 14, 0, tzinfo=UTC)


# --------------------------------------------------------------------------
# floor / next_boundary: calendar week
# --------------------------------------------------------------------------


def test_week_floor_is_monday_midnight_utc() -> None:
    bar = TimeBar.parse("1w")
    # 2024-01-04 is a Thursday.
    value = datetime(2024, 1, 4, 15, 30, tzinfo=UTC)

    assert bar.floor(value) == datetime(2024, 1, 1, tzinfo=UTC)  # Monday


def test_week_next_boundary_crosses_month_end() -> None:
    bar = TimeBar.parse("1w")
    # 2024-01-29 is a Monday; the following boundary is 2024-02-05.
    value = datetime(2024, 1, 29, tzinfo=UTC)

    assert bar.next_boundary(value) == datetime(2024, 2, 5, tzinfo=UTC)


def test_week_boundary_is_exactly_seven_days() -> None:
    bar = TimeBar.parse("1w")
    start = datetime(2024, 1, 1, tzinfo=UTC)

    assert bar.next_boundary(start) - start == timedelta(days=7)


# --------------------------------------------------------------------------
# floor / next_boundary: calendar month (no 30-day approximation)
# --------------------------------------------------------------------------


def test_month_floor_is_first_of_month_midnight_utc() -> None:
    bar = TimeBar.parse("1M")
    value = datetime(2024, 2, 15, 12, 0, tzinfo=UTC)

    assert bar.floor(value) == datetime(2024, 2, 1, tzinfo=UTC)


def test_february_leap_year_month_boundary_is_29_days() -> None:
    bar = TimeBar.parse("1M")
    start = datetime(2024, 2, 1, tzinfo=UTC)  # 2024 is a leap year

    assert bar.next_boundary(start) == datetime(2024, 3, 1, tzinfo=UTC)
    assert bar.next_boundary(start) - start == timedelta(days=29)


def test_february_non_leap_year_month_boundary_is_28_days() -> None:
    bar = TimeBar.parse("1M")
    start = datetime(2023, 2, 1, tzinfo=UTC)

    assert bar.next_boundary(start) == datetime(2023, 3, 1, tzinfo=UTC)
    assert bar.next_boundary(start) - start == timedelta(days=28)


def test_31_day_month_boundary() -> None:
    bar = TimeBar.parse("1M")
    start = datetime(2024, 1, 1, tzinfo=UTC)

    assert bar.next_boundary(start) - start == timedelta(days=31)


def test_month_boundary_crosses_year_rollover() -> None:
    bar = TimeBar.parse("1M")
    start = datetime(2024, 12, 1, tzinfo=UTC)

    assert bar.next_boundary(start) == datetime(2025, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------
# iter_intervals
# --------------------------------------------------------------------------


def test_iter_intervals_minute_bars_are_half_open_and_contiguous() -> None:
    bar = TimeBar.parse("1m")
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 0, 3, tzinfo=UTC)

    intervals = bar.iter_intervals(start, end)

    assert intervals == [
        (datetime(2024, 1, 1, 0, 0, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC)),
        (datetime(2024, 1, 1, 0, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 2, tzinfo=UTC)),
        (datetime(2024, 1, 1, 0, 2, tzinfo=UTC), datetime(2024, 1, 1, 0, 3, tzinfo=UTC)),
    ]
    # Half-open: end is never itself the start of an emitted interval.
    assert all(bar_end <= end for _, bar_end in intervals)


def test_iter_intervals_month_bars_across_calendar_boundaries() -> None:
    bar = TimeBar.parse("1M")
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 4, 1, tzinfo=UTC)

    intervals = bar.iter_intervals(start, end)

    assert intervals == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)),
        (datetime(2024, 2, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)),
        (datetime(2024, 3, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC)),
    ]


def test_iter_intervals_requires_start_less_than_end() -> None:
    bar = TimeBar.parse("1h")
    same = datetime(2024, 1, 1, tzinfo=UTC)

    with pytest.raises(InvalidRequestError):
        bar.iter_intervals(same, same)


def test_iter_intervals_requires_start_aligned_to_boundary() -> None:
    bar = TimeBar.parse("1h")
    misaligned = datetime(2024, 1, 1, 0, 30, tzinfo=UTC)
    end = datetime(2024, 1, 1, 2, 0, tzinfo=UTC)

    with pytest.raises(InvalidRequestError):
        bar.iter_intervals(misaligned, end)


def test_iter_intervals_rejects_naive_datetimes() -> None:
    bar = TimeBar.parse("1h")
    naive = datetime(2024, 1, 1, 0, 0)  # noqa: DTZ001 - intentionally naive
    end = datetime(2024, 1, 1, 2, 0, tzinfo=UTC)

    with pytest.raises(InvalidRequestError):
        bar.iter_intervals(naive, end)


# --------------------------------------------------------------------------
# validate_range
# --------------------------------------------------------------------------


def test_validate_range_accepts_half_open_start_before_end() -> None:
    validate_range(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC))


def test_validate_range_rejects_start_equal_to_end() -> None:
    same = datetime(2024, 1, 1, tzinfo=UTC)

    with pytest.raises(InvalidRequestError):
        validate_range(same, same)


def test_validate_range_rejects_start_after_end() -> None:
    with pytest.raises(InvalidRequestError):
        validate_range(datetime(2024, 1, 2, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC))


def test_validate_range_rejects_naive_start() -> None:
    naive = datetime(2024, 1, 1)  # noqa: DTZ001 - intentionally naive
    end = datetime(2024, 1, 2, tzinfo=UTC)

    with pytest.raises(InvalidRequestError):
        validate_range(naive, end)


def test_validate_range_rejects_naive_end() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    naive_end = datetime(2024, 1, 2)  # noqa: DTZ001 - intentionally naive

    with pytest.raises(InvalidRequestError):
        validate_range(start, naive_end)


# --------------------------------------------------------------------------
# parse_time_input
# --------------------------------------------------------------------------


def test_parse_time_input_date_string_means_utc_midnight() -> None:
    parsed = parse_time_input("2024-01-01")

    assert parsed == datetime(2024, 1, 1, tzinfo=UTC)
    assert parsed.tzinfo is UTC


def test_parse_time_input_offset_bearing_string_normalizes_to_utc() -> None:
    parsed = parse_time_input("2024-01-01T05:00:00+05:00")

    assert parsed == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def test_parse_time_input_z_suffix_normalizes_to_utc() -> None:
    parsed = parse_time_input("2024-01-01T12:00:00Z")

    assert parsed == datetime(2024, 1, 1, 12, 0, tzinfo=UTC)


def test_parse_time_input_accepts_tz_aware_datetime() -> None:
    aware = datetime(2024, 1, 1, 9, tzinfo=timezone(timedelta(hours=9)))

    parsed = parse_time_input(aware)

    assert parsed == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def test_parse_time_input_rejects_naive_datetime() -> None:
    naive = datetime(2024, 1, 1)  # noqa: DTZ001 - intentionally naive

    with pytest.raises(InvalidRequestError):
        parse_time_input(naive)


def test_parse_time_input_rejects_naive_datetime_string() -> None:
    with pytest.raises(InvalidRequestError):
        parse_time_input("2024-01-01T00:00:00")


def test_parse_time_input_rejects_garbage_string() -> None:
    with pytest.raises(InvalidRequestError):
        parse_time_input("not-a-date")


def test_parse_time_input_rejects_non_str_non_datetime() -> None:
    with pytest.raises(InvalidRequestError):
        parse_time_input(12345)  # type: ignore[arg-type]
