"""Deterministic result-warning normalization."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

from xret.data.models import DataWarning


def normalized_warnings(values: Iterable[DataWarning]) -> tuple[DataWarning, ...]:
    """Deduplicate and order structured warnings for stable caller behavior."""
    unique = {(value.code, value.message, value.start, value.end): value for value in values}
    minimum = datetime.min.replace(tzinfo=UTC)
    return tuple(
        sorted(
            unique.values(),
            key=lambda value: (
                value.start or minimum,
                value.end or minimum,
                value.code,
                value.message,
            ),
        )
    )
