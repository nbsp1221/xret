"""Provider-author contracts and provider-independent runtime behavior."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from xret.data.config import MarketDataConfig
from xret.data.errors import (
    CatalogError,
    InvalidRequestError,
    ProviderError,
    UnsupportedMarketError,
)
from xret.data.market_data import MarketData
from xret.data.models import BarRequest, CoverageStatus, MarketIdentity
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    BarObservation,
    HistoricalBarProvider,
    ObservedWindow,
    ProviderDescriptor,
    ResolvedBarMarket,
    discovery,
    runtime,
)
from xret.data.providers.runtime import ProviderRuntime
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage.catalog import CATALOG_FILE_NAME, Catalog

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 1, 1, 3, tzinfo=UTC)
COMPLETED = datetime(2024, 1, 1, 10, tzinfo=UTC)
IDENTITY = MarketIdentity(exchange="coinbase", symbol="ETH/USD", market="spot")
REQUEST = BarRequest(identity=IDENTITY, timeframe="1h", start=START, end=END)


def _provider_frame(hours: tuple[int, ...] = (0, 1, 2)) -> pl.DataFrame:
    timestamps = [datetime(2024, 1, 1, hour, tzinfo=UTC) for hour in hours]
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0 + hour for hour in hours],
            "high": [101.0 + hour for hour in hours],
            "low": [99.0 + hour for hour in hours],
            "close": [100.5 + hour for hour in hours],
            "volume": [10.0 + hour for hour in hours],
        },
        schema=PROVIDER_BAR_SCHEMA,
    )


class FakeProvider:
    def __init__(
        self,
        *,
        observation: BarObservation | None = None,
        descriptor: ProviderDescriptor | None = None,
        market: ResolvedBarMarket | None = None,
    ) -> None:
        self._descriptor = descriptor or ProviderDescriptor(
            name="test-provider",
            version="build-abc123",
            api_version=PROVIDER_API_VERSION,
        )
        self.market = market or ResolvedBarMarket(
            identity=IDENTITY,
            native_market_id="ETH-USD",
            native_symbol="ETH-USD",
            timeframes=frozenset({"1h"}),
        )
        self.observation = observation or BarObservation(
            frame=_provider_frame(),
            observed=(ObservedWindow(START, END),),
        )
        self.descriptor_calls = 0
        self.resolve_calls = 0
        self.observe_calls = 0

    @property
    def descriptor(self) -> ProviderDescriptor:
        self.descriptor_calls += 1
        return self._descriptor

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
        self.resolve_calls += 1
        return self.market

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation:
        self.observe_calls += 1
        return self.observation


@pytest.fixture(autouse=True)
def _fixed_runtime_clock():
    runtime._set_clock_override(lambda: COMPLETED)
    yield
    runtime._set_clock_override(None)


def test_descriptor_accepts_non_pep440_audit_version() -> None:
    descriptor = ProviderDescriptor(
        name="internal-feed",
        version="git:deadbeef/service:v7",
        api_version=PROVIDER_API_VERSION,
    )

    assert descriptor.version == "git:deadbeef/service:v7"


@pytest.mark.parametrize("name", ["", "Coinbase", "coinbase_native", "-coinbase"])
def test_descriptor_rejects_invalid_provider_name(name: str) -> None:
    with pytest.raises(InvalidRequestError, match="provider name"):
        ProviderDescriptor(name=name, version="1", api_version=PROVIDER_API_VERSION)


def test_observed_window_validates_only_utc_and_nonempty() -> None:
    window = ObservedWindow(START, END)

    assert window == ObservedWindow(START, END)
    with pytest.raises(ProviderError, match="UTC-aware"):
        ObservedWindow(START.replace(tzinfo=None), END)
    with pytest.raises(ProviderError, match="nonempty"):
        ObservedWindow(END, END)


def test_runtime_accepts_a_structural_provider_without_inheritance() -> None:
    provider = FakeProvider()
    structurally_typed: HistoricalBarProvider = provider

    result = ProviderRuntime(structurally_typed).observe(REQUEST)

    assert result.frame.schema == OHLCV_SCHEMA
    assert result.frame["exchange"].to_list() == ["coinbase"] * 3
    assert result.frame["symbol"].to_list() == ["ETH/USD"] * 3
    assert result.source.descriptor.name == "test-provider"
    assert result.source.native_market_id == "ETH-USD"
    assert result.evidence_at == COMPLETED
    assert result.completed_at == COMPLETED
    assert provider.descriptor_calls == 1
    assert provider.resolve_calls == 1
    assert provider.observe_calls == 1


def test_runtime_rejects_wrong_spi_major_before_provider_io() -> None:
    provider = FakeProvider(
        descriptor=ProviderDescriptor(
            name="old-provider",
            version="1",
            api_version=PROVIDER_API_VERSION + 1,
        )
    )

    with pytest.raises(ProviderError, match="API version"):
        ProviderRuntime(provider).observe(REQUEST)

    assert provider.resolve_calls == 0
    assert provider.observe_calls == 0


def test_runtime_rejects_provider_changing_canonical_identity() -> None:
    provider = FakeProvider(
        market=ResolvedBarMarket(
            identity=replace(IDENTITY, symbol="BTC/USD"),
            native_market_id="BTC-USD",
            native_symbol="BTC-USD",
            timeframes=frozenset({"1h"}),
        )
    )

    with pytest.raises(ProviderError, match="changed canonical"):
        ProviderRuntime(provider).observe(REQUEST)


def test_runtime_rejects_unsupported_market_timeframe_before_observation() -> None:
    provider = FakeProvider(
        market=ResolvedBarMarket(
            identity=IDENTITY,
            native_market_id="ETH-USD",
            native_symbol="ETH-USD",
            timeframes=frozenset({"5m"}),
        )
    )

    with pytest.raises(UnsupportedMarketError, match="does not support timeframe"):
        ProviderRuntime(provider).observe(REQUEST)

    assert provider.observe_calls == 0


def test_runtime_rejects_incomplete_observation_evidence() -> None:
    provider = FakeProvider(
        observation=BarObservation(
            frame=_provider_frame((0,)),
            observed=(ObservedWindow(START, datetime(2024, 1, 1, 1, tzinfo=UTC)),),
        )
    )

    with pytest.raises(ProviderError, match="incomplete provider observation"):
        ProviderRuntime(provider).observe(REQUEST)


def test_runtime_rejects_mutable_observation_window_collection() -> None:
    provider = FakeProvider()
    provider.observation = BarObservation(
        frame=_provider_frame(),
        observed=[ObservedWindow(START, END)],  # type: ignore[arg-type]
    )

    with pytest.raises(ProviderError, match="must be a tuple"):
        ProviderRuntime(provider).observe(REQUEST)


def test_runtime_rejects_null_timestamp_as_provider_error() -> None:
    frame = _provider_frame((0,)).with_columns(
        pl.lit(None, dtype=pl.Datetime("ms", "UTC")).alias("timestamp")
    )
    provider = FakeProvider(
        observation=BarObservation(
            frame=frame,
            observed=(ObservedWindow(START, END),),
        )
    )

    with pytest.raises(ProviderError, match="null timestamps"):
        ProviderRuntime(provider).observe(REQUEST)


def test_runtime_separates_evidence_and_completion_times() -> None:
    provider = FakeProvider(
        observation=BarObservation(
            frame=pl.DataFrame(schema=PROVIDER_BAR_SCHEMA),
            observed=(ObservedWindow(START, END),),
        )
    )
    samples = iter(
        (
            datetime(2024, 1, 1, 1, 0, 4, tzinfo=UTC),
            datetime(2024, 1, 1, 1, 0, 6, tzinfo=UTC),
        )
    )
    runtime._set_clock_override(lambda: next(samples))

    result = ProviderRuntime(provider).observe(REQUEST)

    assert result.evidence_at == datetime(2024, 1, 1, 1, 0, 4, tzinfo=UTC)
    assert result.completed_at == datetime(2024, 1, 1, 1, 0, 6, tzinfo=UTC)
    assert result.frame.is_empty()


def test_bar_becoming_final_during_observation_remains_missing_without_lineage(
    tmp_path: Path,
) -> None:
    provider = FakeProvider(
        descriptor=ProviderDescriptor(
            name="first-provider",
            version="1",
            api_version=PROVIDER_API_VERSION,
        ),
        observation=BarObservation(
            frame=pl.DataFrame(schema=PROVIDER_BAR_SCHEMA),
            observed=(ObservedWindow(START, END),),
        ),
    )
    samples = iter(
        (
            datetime(2024, 1, 1, 1, 0, 4, tzinfo=UTC),
            datetime(2024, 1, 1, 1, 0, 6, tzinfo=UTC),
        )
    )
    runtime._set_clock_override(lambda: next(samples))
    config = MarketDataConfig(
        state_dir=tmp_path / "state",
        data_dir=tmp_path / "data",
    )
    bars = MarketData(config=config, provider=provider).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )

    result = bars.sync(START, END)

    assert not result.changed
    assert len(result.gaps) == 1
    assert result.gaps[0].status is CoverageStatus.MISSING
    assert result.gaps[0].start == START
    assert result.gaps[0].end == END
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_coverage(result.dataset_key) == ()
        assert catalog.get_source_lineage(result.dataset_key) is None
    assert not list(config.data_dir.rglob("*.parquet"))

    runtime._set_clock_override(lambda: COMPLETED)
    replacement = FakeProvider(
        descriptor=ProviderDescriptor(
            name="replacement-provider",
            version="1",
            api_version=PROVIDER_API_VERSION,
        )
    )
    replacement_bars = MarketData(config=config, provider=replacement).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )

    replacement_result = replacement_bars.sync(START, END)

    assert replacement_result.is_complete
    assert replacement_result.fetched_rows == 3
    assert replacement.observe_calls == 1
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_source_lineage(replacement_result.dataset_key) == "replacement-provider"


def test_runtime_accepts_exact_schema_empty_exhaustive_observation() -> None:
    provider = FakeProvider(
        observation=BarObservation(
            frame=pl.DataFrame(schema=PROVIDER_BAR_SCHEMA),
            observed=(ObservedWindow(START, END),),
        )
    )

    result = ProviderRuntime(provider).observe(REQUEST)

    assert result.frame.schema == OHLCV_SCHEMA
    assert result.frame.is_empty()


def test_runtime_rejects_identity_columns_in_provider_frame() -> None:
    provider = FakeProvider(
        observation=BarObservation(
            frame=_provider_frame().with_columns(pl.lit("spoofed").alias("symbol")),
            observed=(ObservedWindow(START, END),),
        )
    )

    with pytest.raises(ProviderError, match="schema mismatch"):
        ProviderRuntime(provider).observe(REQUEST)


def test_runtime_chains_unknown_provider_failure() -> None:
    class BrokenProvider(FakeProvider):
        def observe_bars(
            self,
            request: BarRequest,
            market: ResolvedBarMarket,
        ) -> BarObservation:
            raise RuntimeError("native client exploded")

    with pytest.raises(ProviderError, match="native client exploded") as captured:
        ProviderRuntime(BrokenProvider()).observe(REQUEST)

    assert isinstance(captured.value.__cause__, RuntimeError)


def test_runtime_chains_direct_provider_descriptor_failure() -> None:
    class BrokenDescriptorProvider:
        @property
        def descriptor(self):
            raise RuntimeError("descriptor exploded")

    with pytest.raises(ProviderError, match="descriptor access failed") as captured:
        ProviderRuntime(BrokenDescriptorProvider()).observe(REQUEST)  # type: ignore[arg-type]

    assert isinstance(captured.value.__cause__, RuntimeError)


def test_direct_provider_fetch_uses_public_api_without_storage_side_effects(tmp_path) -> None:
    provider = FakeProvider()
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    bars = MarketData(config=config, provider=provider).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )

    frame = bars.fetch(START, END)

    assert frame.height == 3
    assert frame.schema == OHLCV_SCHEMA
    assert provider.resolve_calls == 1
    assert provider.observe_calls == 1
    assert not config.state_dir.exists()
    assert not config.data_dir.exists()


def test_local_partial_scan_never_resolves_bound_provider(tmp_path) -> None:
    class ExplodingProvider:
        @property
        def descriptor(self):
            raise AssertionError("local scan must not inspect provider")

    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    bars = MarketData(config=config, provider=ExplodingProvider()).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )

    result = bars.scan_partial(START, END)

    assert result.data.collect().is_empty()
    assert len(result.gaps) == 1
    assert result.gaps[0].status is CoverageStatus.MISSING


class FakeEntryPoint:
    def __init__(self, name: str, target: object) -> None:
        self.name = name
        self._target = target

    def load(self) -> object:
        if isinstance(self._target, Exception):
            raise self._target
        return self._target


def _installed_entry_points(
    monkeypatch: pytest.MonkeyPatch,
    *entry_points: FakeEntryPoint,
) -> list[str]:
    calls: list[str] = []

    def discover(*, group: str):
        calls.append(group)
        return entry_points

    monkeypatch.setattr(discovery.metadata, "entry_points", discover)
    return calls


def test_named_provider_discovery_is_lazy_and_cached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = FakeProvider(descriptor=ProviderDescriptor("installed", "1", PROVIDER_API_VERSION))
    calls = _installed_entry_points(
        monkeypatch,
        FakeEntryPoint("installed", lambda: provider),
    )
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    bars = MarketData(config=config, provider="installed").bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )

    assert calls == []
    assert bars.scan_partial(START, END).data.collect().is_empty()
    assert calls == []
    assert bars.fetch(START, END).height == 3
    assert bars.fetch(START, END).height == 3
    assert calls == [discovery.ENTRY_POINT_GROUP]


def test_direct_provider_bypasses_installed_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _installed_entry_points(monkeypatch)
    provider = FakeProvider()
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    frame = (
        MarketData(config=config, provider=provider)
        .bars(
            exchange="coinbase",
            symbol="ETH/USD",
            market="spot",
            timeframe="1h",
        )
        .fetch(START, END)
    )

    assert frame.height == 3
    assert calls == []


def test_named_provider_rejects_unknown_and_duplicate_entry_points(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")

    _installed_entry_points(monkeypatch)
    unknown = MarketData(config=config, provider="unknown").bars(
        exchange="coinbase", symbol="ETH/USD", market="spot", timeframe="1h"
    )
    with pytest.raises(ProviderError, match="unknown installed provider"):
        unknown.fetch(START, END)

    _installed_entry_points(
        monkeypatch,
        FakeEntryPoint("duplicate", lambda: FakeProvider()),
        FakeEntryPoint("duplicate", lambda: FakeProvider()),
    )
    duplicate = MarketData(config=config, provider="duplicate").bars(
        exchange="coinbase", symbol="ETH/USD", market="spot", timeframe="1h"
    )
    with pytest.raises(ProviderError, match="duplicate installed provider"):
        duplicate.fetch(START, END)


@pytest.mark.parametrize(
    ("target", "message", "cause_type"),
    [
        (RuntimeError("import exploded"), "failed to import", RuntimeError),
        (lambda: (_ for _ in ()).throw(ValueError("factory exploded")), "factory", ValueError),
    ],
)
def test_named_provider_chains_load_and_factory_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: object,
    message: str,
    cause_type: type[Exception],
) -> None:
    _installed_entry_points(monkeypatch, FakeEntryPoint("broken", target))
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    bars = MarketData(config=config, provider="broken").bars(
        exchange="coinbase", symbol="ETH/USD", market="spot", timeframe="1h"
    )

    with pytest.raises(ProviderError, match=message) as captured:
        bars.fetch(START, END)

    assert isinstance(captured.value.__cause__, cause_type)


@pytest.mark.parametrize(
    ("target", "message"),
    [
        (object(), "callable factory"),
        (
            lambda: FakeProvider(descriptor=ProviderDescriptor("other", "1", PROVIDER_API_VERSION)),
            "returned descriptor",
        ),
        (
            lambda: FakeProvider(
                descriptor=ProviderDescriptor("installed", "1", PROVIDER_API_VERSION + 1)
            ),
            "API version",
        ),
        (
            lambda: type(
                "IncompleteProvider",
                (),
                {"descriptor": ProviderDescriptor("installed", "1", PROVIDER_API_VERSION)},
            )(),
            "no callable resolve_market",
        ),
    ],
)
def test_named_provider_validates_factory_contract_before_market_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: object,
    message: str,
) -> None:
    _installed_entry_points(monkeypatch, FakeEntryPoint("installed", target))
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    bars = MarketData(config=config, provider="installed").bars(
        exchange="coinbase", symbol="ETH/USD", market="spot", timeframe="1h"
    )

    with pytest.raises(ProviderError, match=message):
        bars.fetch(START, END)


class RangeProvider:
    def __init__(
        self,
        name: str,
        version: str,
        *,
        empty: bool = False,
        market_id: str = "ETH-USD",
        native_symbol: str = "ETH-USD",
    ) -> None:
        self._descriptor = ProviderDescriptor(name, version, PROVIDER_API_VERSION)
        self.empty = empty
        self.market_id = market_id
        self.native_symbol = native_symbol
        self.observe_calls = 0

    @property
    def descriptor(self) -> ProviderDescriptor:
        return self._descriptor

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket:
        return ResolvedBarMarket(
            identity=identity,
            native_market_id=self.market_id,
            native_symbol=self.native_symbol,
            timeframes=frozenset({"1h"}),
        )

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation:
        self.observe_calls += 1
        if self.empty:
            frame = pl.DataFrame(schema=PROVIDER_BAR_SCHEMA)
        else:
            timestamps = []
            cursor = request.start
            while cursor < request.end:
                timestamps.append(cursor)
                cursor = cursor.replace(hour=cursor.hour + 1)
            n = len(timestamps)
            frame = pl.DataFrame(
                {
                    "timestamp": timestamps,
                    "open": [100.0] * n,
                    "high": [101.0] * n,
                    "low": [99.0] * n,
                    "close": [100.5] * n,
                    "volume": [10.0] * n,
                },
                schema=PROVIDER_BAR_SCHEMA,
            )
        return BarObservation(frame, (ObservedWindow(request.start, request.end),))


def _custom_bars(tmp_path: Path, provider: RangeProvider):
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    bars = MarketData(config=config, provider=provider).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )
    return config, bars


def test_custom_provider_sync_persists_generic_provenance_and_repeats_as_noop(
    tmp_path: Path,
) -> None:
    provider = RangeProvider("acme", "git:abc")
    config, bars = _custom_bars(tmp_path, provider)

    first = bars.sync(START, END)
    second = bars.sync(START, END)

    assert first.is_complete
    assert first.fetched_rows == 3
    assert first.written_partitions == 1
    assert not second.changed
    assert provider.observe_calls == 1
    metadata = pl.read_parquet_metadata(next(config.data_dir.rglob("*.parquet")))
    assert metadata["provider_name"] == "acme"
    assert metadata["provider_version"] == "git:abc"
    assert metadata["provider_api_version"] == str(PROVIDER_API_VERSION)
    assert "ccxt_version" not in metadata
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_source_lineage(first.dataset_key) == "acme"


def test_same_source_lineage_allows_provider_version_change(tmp_path: Path) -> None:
    first_provider = RangeProvider("acme", "build-1")
    config, first_bars = _custom_bars(tmp_path, first_provider)
    first_bars.sync(START, datetime(2024, 1, 1, 2, tzinfo=UTC))

    second_provider = RangeProvider("acme", "build-2")
    second_bars = MarketData(config=config, provider=second_provider).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )
    result = second_bars.sync(START, END)

    assert result.is_complete
    assert result.fetched_rows == 1
    assert second_bars.scan(START, END).collect().height == 3
    metadata = pl.read_parquet_metadata(next(config.data_dir.rglob("*.parquet")))
    assert metadata["provider_version"] == "build-2"
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_source_lineage(result.dataset_key) == "acme"
        versions = {
            row["provider_version"]
            for row in catalog.connection.execute(
                """
                SELECT provider_version
                FROM ingestion_runs
                WHERE provider_version IS NOT NULL
                """
            )
        }
        assert versions == {"build-1", "build-2"}


def test_same_source_native_identity_change_updates_latest_publication_snapshot(
    tmp_path: Path,
) -> None:
    first_provider = RangeProvider(
        "acme",
        "build-1",
        market_id="ETH-USD-v1",
        native_symbol="ETH/USD:v1",
    )
    config, first_bars = _custom_bars(tmp_path, first_provider)
    first_bars.sync(START, datetime(2024, 1, 1, 2, tzinfo=UTC))

    second_provider = RangeProvider(
        "acme",
        "build-2",
        market_id="ETH-USD-v2",
        native_symbol="ETH/USD:v2",
    )
    second_bars = MarketData(config=config, provider=second_provider).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )
    result = second_bars.sync(START, END)

    metadata = pl.read_parquet_metadata(next(config.data_dir.rglob("*.parquet")))
    assert metadata["provider_market_id"] == "ETH-USD-v2"
    assert metadata["native_symbol"] == "ETH/USD:v2"
    with Catalog.open(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.get_source_lineage(result.dataset_key) == "acme"
        snapshots = {
            (row["provider_market_id"], row["native_symbol"])
            for row in catalog.connection.execute(
                """
                SELECT provider_market_id, native_symbol
                FROM ingestion_runs
                WHERE provider_market_id IS NOT NULL
                """
            )
        }
        assert snapshots == {
            ("ETH-USD-v1", "ETH/USD:v1"),
            ("ETH-USD-v2", "ETH/USD:v2"),
        }


def test_different_source_lineage_fails_before_new_parquet_publication(
    tmp_path: Path,
) -> None:
    first_provider = RangeProvider("source-a", "1")
    config, first_bars = _custom_bars(tmp_path, first_provider)
    first_bars.sync(START, datetime(2024, 1, 1, 2, tzinfo=UTC))
    parquet_path = next(config.data_dir.rglob("*.parquet"))
    before = parquet_path.read_bytes()

    second_provider = RangeProvider("source-b", "1")
    second_bars = MarketData(config=config, provider=second_provider).bars(
        exchange="coinbase",
        symbol="ETH/USD",
        market="spot",
        timeframe="1h",
    )

    with pytest.raises(CatalogError, match="source lineage"):
        second_bars.sync(START, END)

    assert second_provider.observe_calls == 0
    assert parquet_path.read_bytes() == before
    partial = second_bars.scan_partial(START, END)
    assert partial.data.collect().height == 2
    assert partial.gaps[0].status is CoverageStatus.MISSING


def test_unavailable_only_lineage_is_operational_and_not_rebuilt(
    tmp_path: Path,
) -> None:
    provider = RangeProvider("empty-source", "1", empty=True)
    config, bars = _custom_bars(tmp_path, provider)
    result = bars.sync(START, END)
    catalog_path = config.state_dir / CATALOG_FILE_NAME

    with Catalog.open(catalog_path) as catalog:
        assert catalog.get_source_lineage(result.dataset_key) == "empty-source"
        assert {segment.status for segment in catalog.get_coverage(result.dataset_key)} == {
            CoverageStatus.UNAVAILABLE
        }
    assert not list(config.data_dir.rglob("*.parquet"))

    catalog_path.unlink()
    MarketData(config=config, provider=provider).maintenance.rebuild_catalog()

    with Catalog.open(catalog_path) as catalog:
        assert catalog.get_source_lineage(result.dataset_key) is None
    rebuilt = bars.scan_partial(START, END)
    assert rebuilt.gaps == (replace(result.gaps[0], status=CoverageStatus.MISSING),)
