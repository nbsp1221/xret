"""Canonical crypto identity resolution against CCXT market metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from xret.data.errors import InvalidRequestError, UnsupportedMarketError
from xret.data.models import Market, MarketIdentity
from xret.data.providers.ccxt.client import CCXTExchange
from xret.data.providers.contracts import DerivativeInterpretation
from xret.data.timeframe import TimeBar

_PERPETUAL_CLIENT_IDS: Final[dict[str, str]] = {
    "binance": "binanceusdm",
    "kraken": "krakenfutures",
    "kucoin": "kucoinfutures",
}


@dataclass(frozen=True, slots=True)
class CcxtMarket:
    """One canonical crypto market resolved to native CCXT metadata."""

    native_market_id: str
    native_symbol: str
    settle: str | None
    metadata: dict[str, Any]


def client_id(identity: MarketIdentity) -> str:
    if identity.market is Market.PERPETUAL:
        return _PERPETUAL_CLIENT_IDS.get(identity.exchange, identity.exchange)
    return identity.exchange


def _native_symbol(symbol: str, market: dict[str, Any]) -> str:
    native = market.get("symbol")
    return native if isinstance(native, str) and native else symbol


def _resolved_market(
    symbol: str,
    settle: str | None,
    market: dict[str, Any],
) -> CcxtMarket:
    native_market_id = market.get("id")
    if not isinstance(native_market_id, str) or not native_market_id:
        raise UnsupportedMarketError(f"{symbol!r} has no native CCXT market id")
    return CcxtMarket(
        native_market_id=native_market_id,
        native_symbol=_native_symbol(symbol, market),
        settle=settle,
        metadata=market,
    )


def _spot(identity: MarketIdentity, markets: dict[str, Any]) -> CcxtMarket:
    market = markets.get(identity.symbol)
    if market is None or not market.get("spot"):
        raise UnsupportedMarketError(f"{identity.symbol!r} is not a listed spot market")
    return _resolved_market(identity.symbol, None, market)


def _perpetual_candidates(
    identity: MarketIdentity,
    markets: dict[str, Any],
) -> dict[str, Any]:
    base, quote = identity.symbol.split("/")
    return {
        symbol: market
        for symbol, market in markets.items()
        if market.get("base") == base and market.get("quote") == quote and market.get("swap")
    }


def _perpetual(identity: MarketIdentity, markets: dict[str, Any]) -> CcxtMarket:
    candidates = _perpetual_candidates(identity, markets)
    if identity.settle is not None:
        matches = {
            symbol: market
            for symbol, market in candidates.items()
            if market.get("settle") == identity.settle
        }
        if not matches:
            raise UnsupportedMarketError(
                f"no perpetual market for {identity.symbol!r} settling in "
                f"{identity.settle!r} is listed"
            )
        if len(matches) != 1:
            raise UnsupportedMarketError(
                f"ambiguous perpetual market for {identity.symbol!r} settling in "
                f"{identity.settle!r}"
            )
        symbol, market = next(iter(matches.items()))
        return _resolved_market(symbol, identity.settle, market)

    settlements = sorted(
        {market["settle"] for market in candidates.values() if market.get("settle")}
    )
    if not settlements:
        raise UnsupportedMarketError(
            f"no perpetual settlement candidates found for {identity.symbol!r}; "
            "pass settle= explicitly"
        )
    if len(settlements) > 1:
        raise UnsupportedMarketError(
            f"ambiguous perpetual settlement for {identity.symbol!r}: "
            f"candidates={settlements!r}; pass settle= explicitly"
        )
    settlement = settlements[0]
    matches = {
        symbol: market
        for symbol, market in candidates.items()
        if market.get("settle") == settlement
    }
    if len(matches) != 1:
        raise UnsupportedMarketError(
            f"ambiguous perpetual market for {identity.symbol!r} settling in {settlement!r}"
        )
    symbol, market = next(iter(matches.items()))
    return _resolved_market(symbol, settlement, market)


def resolve(identity: MarketIdentity, exchange: CCXTExchange) -> CcxtMarket:
    if not exchange.has.get("fetchOHLCV"):
        raise UnsupportedMarketError(f"{exchange.id} does not support fetchOHLCV")
    native_markets = exchange.load_markets()
    return (
        _spot(identity, native_markets)
        if identity.market is Market.SPOT
        else _perpetual(identity, native_markets)
    )


def supported_timeframes(exchange: CCXTExchange) -> frozenset[str]:
    """Timeframes Xret can request from `exchange`, in canonical vocabulary.

    CCXT advertises each venue's own catalog, which legitimately contains
    entries outside Xret's canonical grammar (`3M`, `1y`, bare `15`). Those
    are true facts about the venue, not contract violations, so they are
    excluded here rather than rejected: a venue must not become unusable
    because it offers a bar type Xret has no vocabulary for. Requesting an
    excluded timeframe still fails explicitly -- `BarRequest` rejects
    non-canonical input, and `ProviderRuntime` raises
    `UnsupportedMarketError` for a canonical timeframe this venue omits.
    """
    timeframes = getattr(exchange, "timeframes", None)
    if not isinstance(timeframes, Mapping):
        return frozenset()
    canonical: set[str] = set()
    for key in timeframes:
        candidate = str(key)
        try:
            TimeBar.parse(candidate)
        except InvalidRequestError:
            continue
        canonical.add(candidate)
    return frozenset(canonical)


def derivative_interpretation(market: CcxtMarket) -> DerivativeInterpretation:
    contract_size = market.metadata.get("contractSize")
    return DerivativeInterpretation(
        linear=(
            market.metadata.get("linear")
            if isinstance(market.metadata.get("linear"), bool)
            else None
        ),
        inverse=(
            market.metadata.get("inverse")
            if isinstance(market.metadata.get("inverse"), bool)
            else None
        ),
        contract_size=str(contract_size) if contract_size is not None else None,
    )
