"""Deterministic unit tests for `MarketIdentity` construction and validation.

No I/O, no provider: `MarketIdentity` is pure identity validation
(Decisions 4-9). Provider-resolution tests cover settlement inference from
provider metadata.
"""

from __future__ import annotations

import pytest
from xret.data.errors import InvalidRequestError, UnsupportedMarketError
from xret.data.models import NONE_SETTLE_SENTINEL, DatasetKey, Market, MarketIdentity


def test_spot_identity_without_settle_is_valid() -> None:
    identity = MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot")

    assert identity.exchange == "binance"
    assert identity.symbol == "BTC/USDT"
    assert identity.market is Market.SPOT
    assert identity.settle is None


def test_perpetual_identity_with_explicit_settle_is_valid() -> None:
    identity = MarketIdentity(
        exchange="binance", symbol="BTC/USDT", market="perpetual", settle="USDT"
    )

    assert identity.market is Market.PERPETUAL
    assert identity.settle == "USDT"


def test_perpetual_identity_without_settle_is_valid_construction() -> None:
    # Omitted settle is legal at the identity layer; ambiguous/absent
    # inference is a resolution-time (fetch/sync) concern, not identity
    # construction (Decision 9).
    identity = MarketIdentity(exchange="binance", symbol="BTC/USDT", market="perpetual")

    assert identity.settle is None


def test_market_accepts_enum_member_directly() -> None:
    identity = MarketIdentity(exchange="binance", symbol="BTC/USDT", market=Market.PERPETUAL)

    assert identity.market is Market.PERPETUAL


def test_identity_construction_is_keyword_only() -> None:
    with pytest.raises(TypeError):
        MarketIdentity("binance", "BTC/USDT", "spot")  # type: ignore[misc]


def test_identity_is_frozen() -> None:
    identity = MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot")

    with pytest.raises(AttributeError):
        identity.exchange = "okx"  # type: ignore[misc]


@pytest.mark.parametrize("exchange", ["Binance", "BINANCE", " binance", "binance ", "", "bin ance"])
def test_rejects_non_canonical_exchange_slug(exchange: str) -> None:
    with pytest.raises(InvalidRequestError):
        MarketIdentity(exchange=exchange, symbol="BTC/USDT", market="spot")


@pytest.mark.parametrize(
    "symbol",
    [
        "BTCUSDT",  # no separator
        "BTC/USDT/EXTRA",  # too many components
        "/USDT",  # empty base
        "BTC/",  # empty quote
        "",
    ],
)
def test_rejects_malformed_symbol_boundary(symbol: str) -> None:
    with pytest.raises(InvalidRequestError):
        MarketIdentity(exchange="binance", symbol=symbol, market="spot")


@pytest.mark.parametrize("market", ["swap", "perp", "spots", "Spot", "PERPETUAL", "futures", ""])
def test_rejects_provider_terms_and_non_canonical_casing(market: str) -> None:
    with pytest.raises(InvalidRequestError):
        MarketIdentity(exchange="binance", symbol="BTC/USDT", market=market)


@pytest.mark.parametrize("market", ["future", "option"])
def test_vocabulary_valid_but_unsupported_markets_raise_unsupported(market: str) -> None:
    # P-2: vocabulary-valid but structurally ambiguous without
    # contract-attribute fields; always rejected in V1, never
    # InvalidRequestError (the string itself is not malformed).
    with pytest.raises(UnsupportedMarketError):
        MarketIdentity(exchange="binance", symbol="BTC/USDT", market=market)


def test_settle_with_spot_market_is_invalid_request() -> None:
    with pytest.raises(InvalidRequestError):
        MarketIdentity(exchange="binance", symbol="BTC/USDT", market="spot", settle="USDT")


@pytest.mark.parametrize("settle", ["USD/T", ""])
def test_rejects_malformed_settle_component(settle: str) -> None:
    with pytest.raises(InvalidRequestError):
        MarketIdentity(exchange="binance", symbol="BTC/USDT", market="perpetual", settle=settle)


@pytest.mark.parametrize(
    ("symbol", "settle"),
    [
        ("btc/usdt", "usdt"),
        ("老板/김치", "老板"),
        ("$TRDL/100¥", "💵"),
        ("Café/USDT", "Café"),
        ("DOGE-1/USDT", "-"),
    ],
)
def test_symbol_and_settle_preserve_safe_unicode_components_with_nfc(
    symbol: str, settle: str
) -> None:
    identity = MarketIdentity(exchange="binance", symbol=symbol, market="perpetual", settle=settle)

    assert identity.symbol == symbol.replace("é", "é")
    assert identity.settle == settle.replace("é", "é")


def test_future_rejection_happens_before_settle_validation() -> None:
    with pytest.raises(UnsupportedMarketError):
        MarketIdentity(exchange="binance", symbol="BTC/USDT", market="future", settle="USD/T")


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        (
            {
                "exchange": "Binance",
                "symbol": "BTC/USDT",
                "market": "spot",
                "settle": NONE_SETTLE_SENTINEL,
                "timeframe": "1m",
            },
            InvalidRequestError,
        ),
        (
            {
                "exchange": "binance",
                "symbol": "BTC-USDT",
                "market": "spot",
                "settle": NONE_SETTLE_SENTINEL,
                "timeframe": "1m",
            },
            InvalidRequestError,
        ),
        (
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "market": "future",
                "settle": "USDT",
                "timeframe": "1m",
            },
            UnsupportedMarketError,
        ),
        (
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "market": "perpetual",
                "settle": NONE_SETTLE_SENTINEL,
                "timeframe": "1m",
            },
            InvalidRequestError,
        ),
        (
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "market": "spot",
                "settle": NONE_SETTLE_SENTINEL,
                "timeframe": "1minute",
            },
            InvalidRequestError,
        ),
    ],
)
def test_direct_dataset_key_construction_enforces_canonical_identity(
    kwargs: dict[str, str], error: type[Exception]
) -> None:
    with pytest.raises(error):
        DatasetKey(**kwargs)
