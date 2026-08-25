"""Public warning categories emitted by Xret remote provider operations."""

from __future__ import annotations

__all__ = ["UnverifiedProviderWarning"]


class UnverifiedProviderWarning(UserWarning):
    """An available provider scope lacks current Xret qualification evidence."""
