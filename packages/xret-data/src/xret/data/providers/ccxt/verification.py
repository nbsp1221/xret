"""Xret qualification evidence for exact CCXT operation scopes.

Evidence affects reported confidence only. It is never consulted to authorize
historical fetches or live subscriptions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final, Literal

from xret.data.models import Verification, VerificationStatus
from xret.data.providers.ccxt import compatibility

__all__ = ["historical", "live"]

_EXACT_SCOPE_REQUALIFIED_ON = date(2026, 8, 24)
_LIVE_REQUALIFIED_ON = date(2026, 8, 24)


@dataclass(frozen=True, slots=True)
class QualificationScope:
    """Exact operation scope covered by durable qualification evidence."""

    client_id: str
    market_family: Literal["spot", "perpetual"]
    settle: str | None
    timeframe: str | None = None


_LIVE_SCOPES: Final[frozenset[QualificationScope]] = frozenset(
    {
        QualificationScope("binance", "spot", None, "1m"),
        QualificationScope("binanceusdm", "perpetual", "USDT", "1m"),
        QualificationScope("bybit", "spot", None, "1m"),
        QualificationScope("bybit", "perpetual", "USDT", "1m"),
        QualificationScope("okx", "spot", None, "1m"),
        QualificationScope("okx", "perpetual", "USDT", "1m"),
    }
)


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
    scope = QualificationScope(client_id, family, settle, timeframe)
    if scope in _LIVE_SCOPES:
        return Verification(VerificationStatus.VERIFIED, _LIVE_REQUALIFIED_ON)
    return Verification(VerificationStatus.UNVERIFIED)
