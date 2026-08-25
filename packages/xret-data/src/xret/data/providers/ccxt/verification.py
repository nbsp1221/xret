"""Xret qualification evidence for exact CCXT operation scopes.

Evidence affects reported confidence only. It is never consulted to authorize
historical fetches or live subscriptions.
"""

from __future__ import annotations

from datetime import date

from xret.data.models import Verification, VerificationStatus
from xret.data.providers.ccxt import compatibility

__all__ = ["historical", "live"]

_EXACT_SCOPE_REQUALIFIED_ON = date(2026, 8, 24)
_LIVE_REQUALIFIED_ON = date(2026, 8, 24)


def historical(
    client_id: str,
    market_family: str,
    settle: str | None,
) -> Verification:
    """Return exact historical qualification evidence without gating use."""
    family = compatibility._validated_market_family(market_family)
    scope = compatibility.EndpointScope(client_id, family, settle)
    if scope in compatibility._OBSERVATION_PROFILES:
        return Verification(
            VerificationStatus.VERIFIED,
            _EXACT_SCOPE_REQUALIFIED_ON,
        )
    return Verification(VerificationStatus.UNVERIFIED)


def live(
    client_id: str,
    market_family: str,
    settle: str | None,
    timeframe: str,
) -> Verification:
    """Return exact live qualification evidence without gating use."""
    family = compatibility._validated_market_family(market_family)
    scope = compatibility.LiveEndpointScope(client_id, family, settle, timeframe)
    if scope in compatibility._LIVE_ENDPOINT_SCOPES:
        return Verification(VerificationStatus.VERIFIED, _LIVE_REQUALIFIED_ON)
    return Verification(VerificationStatus.UNVERIFIED)
