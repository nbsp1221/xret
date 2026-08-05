"""Provider-neutral market-definition discovery contracts."""

from __future__ import annotations

import inspect
from decimal import Decimal
from pathlib import Path

import pytest
from xret.data.config import MarketDataConfig
from xret.data.errors import InvalidRequestError, ProviderError, UnsupportedMarketError
from xret.data.market_data import MarketData
from xret.data.providers import (
    PROVIDER_API_VERSION,
    BarObservation,
    BarRequest,
    DerivativeInterpretation,
    Market,
    MarketDefinition,
    MarketDefinitionProvider,
    MarketIdentity,
    ProviderDescriptor,
    ResolvedBarMarket,
)


def _spot_definition(**changes: object) -> MarketDefinition:
    values: dict[str, object] = {
        "identity": MarketIdentity(exchange="coinbase", symbol="ETH/USD", market="spot"),
        "active": True,
        "timeframes": frozenset({"1m", "1h"}),
        "tick_size": Decimal("0.01"),
        "size_increment": Decimal("0.0001"),
    }
    values.update(changes)
    return MarketDefinition(**values)  # type: ignore[arg-type]


def _perpetual_definition() -> MarketDefinition:
    return MarketDefinition(
        identity=MarketIdentity(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
        ),
        active=True,
        timeframes=frozenset({"1m", "1h"}),
        tick_size=Decimal("0.1"),
        size_increment=Decimal("0.001"),
        derivative=DerivativeInterpretation(
            linear=True,
            inverse=False,
            contract_size="1",
        ),
    )


class DefinitionProvider:
    def __init__(self, definitions: tuple[MarketDefinition, ...]) -> None:
        self.definitions = definitions
        self.calls: list[tuple[str, Market]] = []

    @property
    def descriptor(self) -> ProviderDescriptor:
        return ProviderDescriptor("definition-provider", "1", PROVIDER_API_VERSION)

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]:
        self.calls.append((exchange, market))
        return self.definitions

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
        raise AssertionError("market-definition discovery must not resolve a bar market")

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation:
        raise AssertionError("market-definition discovery must not observe bars")


def test_market_definition_is_an_immutable_domain_value() -> None:
    definition = _perpetual_definition()

    assert definition.identity.symbol == "BTC/USDT"
    assert definition.tick_size == Decimal("0.1")
    assert definition.derivative == DerivativeInterpretation(
        linear=True,
        inverse=False,
        contract_size="1",
    )
    with pytest.raises(AttributeError):
        definition.active = False  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("linear", 1),
        ("inverse", "false"),
        ("contract_size", object()),
        ("contract_size", ""),
    ],
)
def test_derivative_interpretation_validates_field_types(field: str, value: object) -> None:
    with pytest.raises(InvalidRequestError, match=field):
        DerivativeInterpretation(**{field: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("derivative", ["invalid", object(), 1])
def test_perpetual_definition_rejects_invalid_derivative_type(derivative: object) -> None:
    identity = _perpetual_definition().identity

    with pytest.raises(InvalidRequestError, match="market definition derivative"):
        MarketDefinition(
            identity=identity,
            active=True,
            timeframes=frozenset({"1h"}),
            tick_size=Decimal("0.1"),
            size_increment=Decimal("0.001"),
            derivative=derivative,  # type: ignore[arg-type]
        )


def test_resolved_market_rejects_invalid_derivative_type() -> None:
    identity = _perpetual_definition().identity

    with pytest.raises(InvalidRequestError, match="resolved market derivative"):
        ResolvedBarMarket(
            identity=identity,
            native_market_id="BTCUSDT",
            native_symbol="BTC/USDT:USDT",
            timeframes=frozenset({"1h"}),
            derivative="invalid",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("active", [0, 1, "true", object()])
def test_market_definition_rejects_non_boolean_active(active: object) -> None:
    with pytest.raises(InvalidRequestError, match="active"):
        _spot_definition(active=active)


def test_market_definition_allows_unknown_active() -> None:
    assert _spot_definition(active=None).active is None


@pytest.mark.parametrize("field", ["tick_size", "size_increment"])
@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0.01, id="float"),
        pytest.param(1, id="integer"),
        pytest.param(Decimal("0"), id="zero"),
        pytest.param(Decimal("-0.01"), id="negative"),
        pytest.param(Decimal("NaN"), id="nan"),
        pytest.param(Decimal("Infinity"), id="infinity"),
    ],
)
def test_market_definition_requires_positive_finite_decimal_increments(
    field: str,
    value: object,
) -> None:
    with pytest.raises(InvalidRequestError, match=field):
        _spot_definition(**{field: value})


def test_market_definition_allows_unknown_increments() -> None:
    definition = _spot_definition(tick_size=None, size_increment=None)

    assert definition.tick_size is None
    assert definition.size_increment is None


def test_market_definition_requires_an_immutable_canonical_timeframe_set() -> None:
    with pytest.raises(InvalidRequestError, match="frozenset"):
        _spot_definition(timeframes={"1h"})
    with pytest.raises(InvalidRequestError, match="timeframe"):
        _spot_definition(timeframes=frozenset({"hourly"}))


def test_market_definition_rejects_derivative_metadata_for_spot() -> None:
    with pytest.raises(InvalidRequestError, match="spot"):
        _spot_definition(derivative=DerivativeInterpretation(linear=True))


def test_market_definition_requires_settlement_for_perpetual() -> None:
    with pytest.raises(InvalidRequestError, match="settle"):
        MarketDefinition(
            identity=MarketIdentity(
                exchange="binance",
                symbol="BTC/USDT",
                market="perpetual",
            ),
            active=True,
            timeframes=frozenset({"1h"}),
            tick_size=Decimal("0.1"),
            size_increment=Decimal("0.001"),
        )


def test_market_definition_provider_is_structural() -> None:
    provider = DefinitionProvider((_spot_definition(),))
    structurally_typed: MarketDefinitionProvider = provider

    assert (
        structurally_typed.fetch_markets(
            exchange="coinbase",
            market=Market.SPOT,
        )
        == provider.definitions
    )


def test_fetch_markets_has_only_the_approved_query_scope() -> None:
    signature = inspect.signature(MarketData.fetch_markets)

    assert tuple(signature.parameters) == ("self", "exchange", "market")
    assert signature.parameters["exchange"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["market"].kind is inspect.Parameter.KEYWORD_ONLY


def test_fetch_markets_uses_optional_provider_capability_without_storage_side_effects(
    tmp_path: Path,
) -> None:
    definition = _spot_definition()
    provider = DefinitionProvider((definition,))
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    result = MarketData(config=config, provider=provider).fetch_markets(
        exchange="coinbase",
        market="spot",
    )

    assert result == (definition,)
    assert provider.calls == [("coinbase", Market.SPOT)]
    assert not config.state_dir.exists()
    assert not config.data_dir.exists()


def test_fetch_markets_validates_scope_before_resolving_provider(tmp_path: Path) -> None:
    class ExplodingProvider(DefinitionProvider):
        @property
        def descriptor(self) -> ProviderDescriptor:
            raise AssertionError("invalid caller input must fail before provider access")

    provider = ExplodingProvider(())
    market_data = MarketData(
        config=MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data"),
        provider=provider,
    )

    with pytest.raises(InvalidRequestError, match="exchange"):
        market_data.fetch_markets(exchange="Coinbase", market="spot")
    with pytest.raises(InvalidRequestError, match="unrecognized market"):
        market_data.fetch_markets(exchange="coinbase", market="swap")
    with pytest.raises(UnsupportedMarketError, match="not supported in V1"):
        market_data.fetch_markets(exchange="coinbase", market="future")


def test_fetch_markets_rejects_provider_without_optional_capability(tmp_path: Path) -> None:
    class HistoricalOnlyProvider:
        @property
        def descriptor(self) -> ProviderDescriptor:
            return ProviderDescriptor("historical-only", "1", PROVIDER_API_VERSION)

        def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
            raise AssertionError

        def observe_bars(
            self,
            request: BarRequest,
            market: ResolvedBarMarket,
        ) -> BarObservation:
            raise AssertionError

    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    with pytest.raises(UnsupportedMarketError, match="market-definition"):
        MarketData(config=config, provider=HistoricalOnlyProvider()).fetch_markets(
            exchange="coinbase",
            market="spot",
        )


def test_fetch_markets_chains_capability_access_failure(tmp_path: Path) -> None:
    class BrokenCapabilityProvider:
        @property
        def descriptor(self) -> ProviderDescriptor:
            return ProviderDescriptor("broken-capability", "1", PROVIDER_API_VERSION)

        @property
        def fetch_markets(self) -> object:
            raise RuntimeError("capability descriptor exploded")

        def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
            raise AssertionError

        def observe_bars(
            self,
            request: BarRequest,
            market: ResolvedBarMarket,
        ) -> BarObservation:
            raise AssertionError

    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    with pytest.raises(ProviderError, match="capability access failed") as captured:
        MarketData(
            config=config,
            provider=BrokenCapabilityProvider(),  # type: ignore[arg-type]
        ).fetch_markets(exchange="coinbase", market="spot")

    assert isinstance(captured.value.__cause__, RuntimeError)


def test_fetch_markets_rejects_non_tuple_provider_result(tmp_path: Path) -> None:
    provider = DefinitionProvider((_spot_definition(),))
    provider.definitions = [_spot_definition()]  # type: ignore[assignment]
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    with pytest.raises(ProviderError, match="tuple"):
        MarketData(config=config, provider=provider).fetch_markets(
            exchange="coinbase",
            market="spot",
        )


def test_fetch_markets_rejects_definition_outside_requested_scope(tmp_path: Path) -> None:
    provider = DefinitionProvider((_perpetual_definition(),))
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    with pytest.raises(ProviderError, match="outside requested scope"):
        MarketData(config=config, provider=provider).fetch_markets(
            exchange="coinbase",
            market="spot",
        )


def test_fetch_markets_rejects_duplicate_canonical_identity(tmp_path: Path) -> None:
    definition = _spot_definition()
    provider = DefinitionProvider((definition, definition))
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    with pytest.raises(ProviderError, match="duplicate canonical identity"):
        MarketData(config=config, provider=provider).fetch_markets(
            exchange="coinbase",
            market="spot",
        )


def test_fetch_markets_chains_unknown_provider_failure(tmp_path: Path) -> None:
    class BrokenProvider(DefinitionProvider):
        def fetch_markets(
            self,
            *,
            exchange: str,
            market: Market,
        ) -> tuple[MarketDefinition, ...]:
            raise RuntimeError("metadata endpoint exploded")

    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    with pytest.raises(ProviderError, match="failed to fetch market definitions") as captured:
        MarketData(config=config, provider=BrokenProvider(())).fetch_markets(
            exchange="coinbase",
            market="spot",
        )

    assert isinstance(captured.value.__cause__, RuntimeError)
