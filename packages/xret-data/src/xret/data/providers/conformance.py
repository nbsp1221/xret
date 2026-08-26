"""I/O-free structural validation for market-data provider implementations."""

from __future__ import annotations

from xret.data.errors import ProviderError
from xret.data.providers.contracts import OpenInterestSyncPolicy, ProviderDescriptor
from xret.data.providers.runtime import validate_provider_descriptor

_CAPABILITY_METHOD_PAIRS: tuple[tuple[str, str], ...] = (
    ("resolve_market", "observe_bars"),
    ("resolve_funding_market", "observe_funding"),
    ("resolve_reference_market", "observe_reference_bars"),
    ("resolve_open_interest_market", "observe_open_interest"),
)


def validate_provider_conformance(provider: object) -> ProviderDescriptor:
    """Validate a descriptor and every recognized capability pair without provider I/O."""
    try:
        descriptor = validate_provider_descriptor(provider)
    except ProviderError:
        raise
    except Exception as exc:
        raise ProviderError(f"invalid provider object: {exc}") from exc
    complete = 0
    for resolver, observer in _CAPABILITY_METHOD_PAIRS:
        resolver_present = callable(getattr(provider, resolver, None))
        observer_present = callable(getattr(provider, observer, None))
        if resolver_present != observer_present:
            missing = observer if resolver_present else resolver
            raise ProviderError(
                f"provider {descriptor.name!r} has partial capability pair "
                f"{resolver}()/{observer}(); missing callable {missing}()"
            )
        complete += int(resolver_present and observer_present)
    if not complete:
        raise ProviderError(
            f"provider {descriptor.name!r} has no complete recognized capability pair"
        )
    has_open_interest = all(
        callable(getattr(provider, method, None))
        for method in ("resolve_open_interest_market", "observe_open_interest")
    )
    if has_open_interest:
        sync_policy = getattr(
            provider,
            "open_interest_sync_policy",
            OpenInterestSyncPolicy.MISSING_ONLY,
        )
        if not isinstance(sync_policy, OpenInterestSyncPolicy):
            raise ProviderError(
                f"provider {descriptor.name!r} open_interest_sync_policy must be "
                "an OpenInterestSyncPolicy"
            )
    return descriptor
