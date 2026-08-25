"""Coverage facts derived from provider observation evidence."""

from __future__ import annotations

from datetime import UTC, datetime

from xret.data.models import CoverageStatus
from xret.data.observation_coverage import evaluate_observation_coverage
from xret.data.providers import ObservedWindow
from xret.data.timeframe import TimeBar


def _at(minute: int) -> datetime:
    return datetime(2026, 1, 1, 0, minute, tzinfo=UTC)


def test_partial_observation_never_turns_unproved_time_into_unavailable() -> None:
    result = evaluate_observation_coverage(
        time_bar=TimeBar.parse("1m"),
        start=_at(0),
        end=_at(5),
        finalizable_end=_at(5),
        timestamps=(_at(1),),
        observed=(ObservedWindow(_at(1), _at(2)),),
    )

    assert [(item.start, item.end, item.status) for item in result.covered] == [
        (_at(1), _at(2), CoverageStatus.AVAILABLE)
    ]
    assert [(item.start, item.end, item.status) for item in result.gaps] == [
        (_at(0), _at(1), CoverageStatus.MISSING),
        (_at(2), _at(5), CoverageStatus.MISSING),
    ]


def test_exhaustive_window_can_prove_sparse_unavailable_bars() -> None:
    result = evaluate_observation_coverage(
        time_bar=TimeBar.parse("1m"),
        start=_at(0),
        end=_at(4),
        finalizable_end=_at(4),
        timestamps=(_at(0), _at(3)),
        observed=(ObservedWindow(_at(0), _at(4)),),
    )

    assert [(item.start, item.end, item.status) for item in result.covered] == [
        (_at(0), _at(1), CoverageStatus.AVAILABLE),
        (_at(3), _at(4), CoverageStatus.AVAILABLE),
    ]
    assert [(item.start, item.end, item.status) for item in result.gaps] == [
        (_at(1), _at(3), CoverageStatus.UNAVAILABLE)
    ]


def test_unfinalized_tail_remains_missing() -> None:
    result = evaluate_observation_coverage(
        time_bar=TimeBar.parse("1m"),
        start=_at(0),
        end=_at(3),
        finalizable_end=_at(2),
        timestamps=(_at(0), _at(1)),
        observed=(ObservedWindow(_at(0), _at(3)),),
    )

    assert result.gaps[0].status is CoverageStatus.MISSING
    assert (result.gaps[0].start, result.gaps[0].end) == (_at(2), _at(3))
