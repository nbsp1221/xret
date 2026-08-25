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


def _timeframes(value: str) -> frozenset[str]:
    return frozenset(value.split())


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

# Exact timeframe sets from the 2026-08-24 qualification evidence. Keep the
# public verified-support matrix synchronized when this registry changes.
_HISTORICAL_TIMEFRAMES: Final[dict[compatibility.EndpointScope, frozenset[str]]] = {
    compatibility.EndpointScope("apex", "perpetual", "USDT"): _timeframes(
        "1m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("aster", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 2h 4h 6h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("aster", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 2h 4h 6h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("binance", "spot"): _timeframes(
        "1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("binanceusdm", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 1M"
    ),
    compatibility.EndpointScope("bingx", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w"
    ),
    compatibility.EndpointScope("bitfinex", "spot"): _timeframes(
        "1m 5m 15m 30m 1h 3h 4h 6h 12h 1d 1M"
    ),
    compatibility.EndpointScope("bitfinex", "perpetual", "USDT"): _timeframes(
        "1m 5m 15m 30m 1h 3h 4h 6h 12h 1d 1M"
    ),
    compatibility.EndpointScope("bitget", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 4h 6h 12h 1d 3d 1w 1M"
    ),
    compatibility.EndpointScope("bitget", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 3d 1w 1M"
    ),
    compatibility.EndpointScope("bitrue", "spot"): _timeframes("1m 5m 15m 30m 1h 2h 4h"),
    compatibility.EndpointScope("bitso", "spot"): _timeframes("1m 5m 15m 30m 1h 4h 12h"),
    compatibility.EndpointScope("bitstamp", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d"
    ),
    compatibility.EndpointScope("bitvavo", "spot"): _timeframes(
        "1m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d"
    ),
    compatibility.EndpointScope("btcturk", "spot"): _timeframes("1m 15m 30m 1h 4h 1d"),
    compatibility.EndpointScope("bybit", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("bybit", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("coinbase", "spot"): _timeframes("1m 5m 15m 30m 1h 2h 6h 1d"),
    compatibility.EndpointScope("coinbase", "perpetual", "USDC"): _timeframes(
        "1m 5m 15m 30m 1h 2h 6h 1d"
    ),
    compatibility.EndpointScope("cryptocom", "spot"): _timeframes(
        "1m 5m 15m 30m 1h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("cryptocom", "perpetual", "USD"): _timeframes(
        "1m 5m 15m 30m 1h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("deribit", "spot"): _timeframes("1m 3m 5m 10m 15m 30m 1h 2h"),
    compatibility.EndpointScope("deribit", "perpetual", "USDC"): _timeframes(
        "1m 3m 5m 10m 15m 30m 1h 2h"
    ),
    compatibility.EndpointScope("dydx", "perpetual", "USDC"): _timeframes("1m 5m 15m 30m 1h 4h 1d"),
    compatibility.EndpointScope("hashkey", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("hashkey", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("htx", "spot"): _timeframes("1m 5m 15m 30m 1h 4h"),
    compatibility.EndpointScope("htx", "perpetual", "USDT"): _timeframes("1m 5m 15m 30m 1h 4h"),
    compatibility.EndpointScope("hyperliquid", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 8h 12h 1d 3d"
    ),
    compatibility.EndpointScope("hyperliquid", "perpetual", "USDC"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 8h 12h 1d 3d"
    ),
    compatibility.EndpointScope("krakenfutures", "perpetual", "USD"): _timeframes(
        "1m 5m 15m 30m 1h 4h 12h 1d"
    ),
    compatibility.EndpointScope("kucoin", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 1M"
    ),
    compatibility.EndpointScope("kucoinfutures", "perpetual", "USDT"): _timeframes(
        "1m 5m 15m 30m 1h 2h 4h 8h 12h 1d 1w"
    ),
    compatibility.EndpointScope("mexc", "spot"): _timeframes("1m 5m 15m 30m 1h 4h 1d"),
    compatibility.EndpointScope("mexc", "perpetual", "USDT"): _timeframes(
        "1m 5m 15m 30m 1h 4h 8h 1d 1w 1M"
    ),
    compatibility.EndpointScope("okx", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("okx", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("pacifica", "perpetual", "USDC"): _timeframes(
        "1m 3m 5m 15m 30m 2h 4h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("phemex", "perpetual", "USDT"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 3h 4h 6h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("toobit", "spot"): _timeframes(
        "1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 1w 1M"
    ),
    compatibility.EndpointScope("upbit", "spot"): _timeframes(
        "1s 1m 3m 5m 10m 15m 30m 1h 4h 1d 1w 1M"
    ),
    compatibility.EndpointScope("woo", "spot"): _timeframes("1m 5m 15m 30m"),
    compatibility.EndpointScope("woo", "perpetual", "USDT"): _timeframes("1m 5m 15m 30m 1h"),
    compatibility.EndpointScope("xt", "spot"): _timeframes("1m 5m 15m 30m 1h 2h 4h 6h 8h 1d 1w 1M"),
    compatibility.EndpointScope("xt", "perpetual", "USDT"): _timeframes(
        "1m 5m 15m 30m 1h 2h 4h 6h 8h 1d 1w 1M"
    ),
}


def historical(
    client_id: str,
    market_family: str,
    settle: str | None,
    timeframe: str,
) -> Verification:
    """Return exact historical qualification evidence without gating use."""
    family = compatibility._validated_market_family(market_family)
    scope = compatibility.EndpointScope(client_id, family, settle)
    if timeframe in _HISTORICAL_TIMEFRAMES.get(scope, frozenset()):
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
