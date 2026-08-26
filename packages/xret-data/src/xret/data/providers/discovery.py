"""Lazy resolution of directly supplied and installed providers."""

from __future__ import annotations

import threading
from importlib import metadata

from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.providers.ccxt import CcxtProvider
from xret.data.providers.runtime import validate_provider_descriptor

_CAPABILITY_METHOD_PAIRS: tuple[tuple[str, str], ...] = (
    ("resolve_market", "observe_bars"),
    ("resolve_funding_market", "observe_funding"),
    ("resolve_reference_market", "observe_reference_bars"),
    ("resolve_open_interest_market", "observe_open_interest"),
)

ENTRY_POINT_GROUP = "xret.data.providers"


def _validate_provider_object(
    provider: object,
    *,
    expected_name: str | None,
) -> object:
    try:
        descriptor = validate_provider_descriptor(provider)
    except ProviderError:
        raise
    except Exception as exc:
        raise ProviderError(f"invalid provider object: {exc}") from exc
    if expected_name is not None and descriptor.name != expected_name:
        raise ProviderError(
            f"provider entry point {expected_name!r} returned descriptor {descriptor.name!r}"
        )
    complete = 0
    for first, second in _CAPABILITY_METHOD_PAIRS:
        first_present = callable(getattr(provider, first, None))
        second_present = callable(getattr(provider, second, None))
        if first_present != second_present:
            missing = second if first_present else first
            raise ProviderError(
                f"provider {descriptor.name!r} has partial capability pair "
                f"{first}()/{second}(); missing callable {missing}()"
            )
        complete += int(first_present and second_present)
    if not complete:
        raise ProviderError(f"provider {descriptor.name!r} has no callable resolve_market()")
    return provider


def load_installed_provider(name: str) -> object:
    """Load one explicitly named provider factory from installed metadata."""
    try:
        candidates = tuple(metadata.entry_points(group=ENTRY_POINT_GROUP))
    except Exception as exc:
        raise ProviderError(f"failed to discover installed providers: {exc}") from exc
    matches = tuple(entry_point for entry_point in candidates if entry_point.name == name)
    if not matches:
        raise ProviderError(
            f"unknown installed provider {name!r}; no {ENTRY_POINT_GROUP!r} entry point found"
        )
    if len(matches) != 1:
        raise ProviderError(f"duplicate installed provider entry points for {name!r}")
    entry_point = matches[0]
    try:
        factory = entry_point.load()
    except Exception as exc:
        raise ProviderError(f"failed to import provider {name!r}: {exc}") from exc
    if not callable(factory):
        raise ProviderError(f"provider entry point {name!r} must reference a callable factory")
    try:
        provider = factory()
    except Exception as exc:
        raise ProviderError(f"provider factory {name!r} failed: {exc}") from exc
    return _validate_provider_object(provider, expected_name=name)


class ProviderHandle:
    """One per-`MarketData` lazy provider binding."""

    def __init__(self, selection: object | None) -> None:
        if isinstance(selection, str) and not selection:
            raise InvalidRequestError("provider name must not be empty")
        self._selection = selection
        self._resolved: object | None = None
        self._lock = threading.Lock()

    def get(self) -> object:
        resolved = self._resolved
        if resolved is not None:
            return resolved
        with self._lock:
            resolved = self._resolved
            if resolved is None:
                selection = self._selection
                if selection is None:
                    resolved = _validate_provider_object(CcxtProvider(), expected_name=None)
                elif isinstance(selection, str):
                    resolved = load_installed_provider(selection)
                else:
                    resolved = _validate_provider_object(selection, expected_name=None)
                self._resolved = resolved
            return resolved
