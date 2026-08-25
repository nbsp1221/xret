"""Provider-independent conversion of observation evidence into coverage facts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from xret.data.models import CoverageInterval, CoverageStatus
from xret.data.providers.contracts import ObservedWindow
from xret.data.timeframe import TimeBar

__all__ = ["ObservationCoverage", "evaluate_observation_coverage"]


@dataclass(frozen=True, slots=True)
class ObservationCoverage:
    """Available intervals and unavailable or unproved gaps for one request."""

    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...]

    @property
    def is_complete(self) -> bool:
        return not self.gaps


def _coalesce(intervals: list[CoverageInterval]) -> list[CoverageInterval]:
    if not intervals:
        return []
    merged = [intervals[0]]
    for interval in intervals[1:]:
        previous = merged[-1]
        if previous.end == interval.start and previous.status is interval.status:
            merged[-1] = CoverageInterval(previous.start, interval.end, previous.status)
        else:
            merged.append(interval)
    return merged


def evaluate_observation_coverage(
    *,
    time_bar: TimeBar,
    start: datetime,
    end: datetime,
    finalizable_end: datetime,
    timestamps: Iterable[datetime],
    observed: tuple[ObservedWindow, ...],
) -> ObservationCoverage:
    """Translate positive rows and exhaustive windows into honest coverage.

    A present row is available. An absent bar is unavailable only inside an
    observed window. Every other portion of the request remains missing.
    """
    effective_end = min(end, max(start, finalizable_end))
    present = set(timestamps)
    facts: list[CoverageInterval] = []
    for window in observed:
        window_start = max(start, window.start)
        window_end = min(effective_end, window.end)
        if window_start >= window_end:
            continue
        for bar_start, bar_end in time_bar.iter_intervals(window_start, window_end):
            facts.append(
                CoverageInterval(
                    max(window_start, bar_start),
                    min(window_end, bar_end),
                    (
                        CoverageStatus.AVAILABLE
                        if bar_start in present
                        else CoverageStatus.UNAVAILABLE
                    ),
                )
            )
    facts = _coalesce(facts)

    covered: list[CoverageInterval] = []
    gaps: list[CoverageInterval] = []
    cursor = start
    for fact in facts:
        if cursor < fact.start:
            gaps.append(CoverageInterval(cursor, fact.start, CoverageStatus.MISSING))
        if fact.status is CoverageStatus.AVAILABLE:
            covered.append(fact)
        else:
            gaps.append(fact)
        cursor = fact.end
    if cursor < end:
        gaps.append(CoverageInterval(cursor, end, CoverageStatus.MISSING))
    return ObservationCoverage(tuple(_coalesce(covered)), tuple(_coalesce(gaps)))
