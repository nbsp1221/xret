"""Interpret CCXT capability metadata without turning it into authorization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.timeframe import TimeBar


@dataclass(frozen=True, slots=True)
class LiveOHLCVCapability:
    """Static CCXT Pro OHLCV facts discovered without opening a connection."""

    available: bool
    timeframes: frozenset[str] | None


class LiveCapabilityProvider(Protocol):
    def __call__(self, client_id: str) -> LiveOHLCVCapability: ...


def canonical_timeframes(value: object) -> frozenset[str] | None:
    if not isinstance(value, Mapping):
        return None
    canonical: set[str] = set()
    for raw in value:
        timeframe = str(raw)
        try:
            TimeBar.parse(timeframe)
        except InvalidRequestError:
            continue
        canonical.add(timeframe)
    return frozenset(canonical)


def installed_live_ohlcv(client_id: str) -> LiveOHLCVCapability:
    """Inspect an installed CCXT Pro class without network or socket I/O."""
    try:
        import ccxt.pro as ccxtpro

        exchange_type = getattr(ccxtpro, client_id, None)
        if exchange_type is None:
            return LiveOHLCVCapability(False, None)
        exchange = exchange_type({"enableRateLimit": True})
        available = bool(exchange.has.get("watchOHLCV"))
        return LiveOHLCVCapability(
            available,
            canonical_timeframes(getattr(exchange, "timeframes", None)),
        )
    except Exception as exc:
        raise ProviderError(
            f"failed to inspect CCXT Pro capability for {client_id!r}: {exc}"
        ) from exc


def ohlcv_page_limit(
    exchange: object,
    *,
    market_family: str,
    metadata: Mapping[str, Any],
) -> int | None:
    """Return CCXT's advertised unified page limit when it is unambiguous."""
    features = getattr(exchange, "features", None)
    if not isinstance(features, Mapping):
        return None
    family = "spot" if market_family == "spot" else "swap"
    selected = features.get(family)
    if not isinstance(selected, Mapping):
        return None
    if family == "swap":
        direction = "linear" if metadata.get("linear") is True else "inverse"
        directional = selected.get(direction)
        if isinstance(directional, Mapping):
            selected = directional
    feature = selected.get("fetchOHLCV")
    if not isinstance(feature, Mapping):
        return None
    value = feature.get("limit")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value
