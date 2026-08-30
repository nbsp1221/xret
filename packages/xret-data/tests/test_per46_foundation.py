from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from xret.data import ProviderEvidence
from xret.data.config import MarketDataConfig
from xret.data.errors import CatalogError, InvalidRequestError, ProviderError
from xret.data.funding_quality import enforce_settled_funding
from xret.data.models import (
    DatasetFamily,
    DatasetKey,
    MarketIdentity,
    OpenInterestKey,
    ReferenceBarKey,
    ReferencePriceKind,
    SettledFundingKey,
    YearMonth,
)
from xret.data.open_interest_quality import enforce_open_interest
from xret.data.providers import PROVIDER_API_VERSION, ProviderDescriptor
from xret.data.providers.discovery import ProviderHandle
from xret.data.reference_quality import enforce_reference_bars
from xret.data.schema import OPEN_INTEREST_SCHEMA, REFERENCE_BAR_SCHEMA, SETTLED_FUNDING_SCHEMA
from xret.data.storage import paths
from xret.data.storage.catalog import CATALOG_FILE_NAME, Catalog, detect_incompatible_state
from xret.data.storage.family_parquet import family_metadata
from xret.data.storage.local_read import lazy_frame_for_facts, read_local_facts_for_key
from xret.data.storage.parquet import ProviderProvenance, read_committed_file
from xret.data.storage.recovery import rebuild_catalog_state


def _identity() -> MarketIdentity:
    return MarketIdentity(
        exchange="binance",
        symbol="BTC/USDT",
        market="perpetual",
        settle="USDT",
    )


def _timestamp(minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, 0, minute, tzinfo=UTC)


def test_family_keys_paths_and_locks_are_collision_free_and_trade_is_unchanged(
    tmp_path: Path,
) -> None:
    identity = _identity()
    trade = DatasetKey.from_identity(identity, timeframe="5m")
    funding = SettledFundingKey(identity=identity)
    mark = ReferenceBarKey(identity=identity, kind=ReferencePriceKind.MARK, timeframe="5m")
    oi = OpenInterestKey(identity=identity, timeframe="5m")
    month = YearMonth(2024, 1)

    assert paths.relative_month_file_path(tmp_path, trade, month) == (
        "binance/perpetual/BTC-USDT-USDT/5m/year=2024/month=01/data.parquet"
    )
    assert paths.relative_month_file_path(tmp_path, funding, month) == (
        "_xret/settled-funding/binance/perpetual/BTC-USDT-USDT/year=2024/month=01/data.parquet"
    )
    assert paths.relative_month_file_path(tmp_path, mark, month) == (
        "_xret/reference-bars/mark/binance/perpetual/BTC-USDT-USDT/5m/"
        "year=2024/month=01/data.parquet"
    )
    assert paths.relative_month_file_path(tmp_path, oi, month) == (
        "_xret/open-interest/binance/perpetual/BTC-USDT-USDT/5m/year=2024/month=01/data.parquet"
    )
    assert paths.lock_file_path(tmp_path, trade) == (
        tmp_path / "locks/binance__perpetual__BTC-USDT-USDT__5m.lock"
    )
    assert paths.lock_file_path(tmp_path, mark) == (
        tmp_path / "locks/v2/reference_bars/mark/binance__perpetual__BTC-USDT-USDT__5m.lock"
    )


def test_malformed_reserved_paths_are_ambiguous(tmp_path: Path) -> None:
    malformed = tmp_path / "_xret/unknown/data.parquet"
    malformed.parent.mkdir(parents=True)
    malformed.touch()
    assert paths.classify_managed_storage(tmp_path) == "ambiguous"
    assert not paths.is_canonical_month_file_path(tmp_path, malformed)


@pytest.mark.parametrize(
    ("first", "second"),
    (
        ("resolve_market", "observe_bars"),
        ("resolve_funding_market", "observe_funding"),
        ("resolve_reference_market", "observe_reference_bars"),
        ("resolve_open_interest_market", "observe_open_interest"),
    ),
)
def test_provider_admission_rejects_every_partial_capability_pair(first: str, second: str) -> None:
    provider = type(
        "PartialProvider",
        (),
        {
            "descriptor": ProviderDescriptor("partial", "1", PROVIDER_API_VERSION),
            first: lambda self: None,
        },
    )()
    with pytest.raises(ProviderError, match=f"partial capability pair.*{second}"):
        ProviderHandle(provider).get()


def test_provider_admission_accepts_an_open_interest_only_provider() -> None:
    provider = type(
        "OpenInterestOnlyProvider",
        (),
        {
            "descriptor": ProviderDescriptor("oi-only", "1", PROVIDER_API_VERSION),
            "resolve_open_interest_market": lambda self: None,
            "observe_open_interest": lambda self: None,
        },
    )()
    assert ProviderHandle(provider).get() is provider


def test_exact_family_schemas_and_validators() -> None:
    identity = _identity()
    funding_key = SettledFundingKey(identity=identity)
    funding = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["perpetual"],
            "settle": ["USDT"],
            "effective_at": [_timestamp(1)],
            "funding_rate": [-0.0001],
            "funding_interval_seconds": [None],
            "mark_price": [None],
        },
        schema=SETTLED_FUNDING_SCHEMA,
    )
    enforce_settled_funding(funding, funding_key)

    premium_key = ReferenceBarKey(
        identity=identity,
        kind=ReferencePriceKind.PREMIUM_INDEX,
        timeframe="5m",
    )
    premium = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["perpetual"],
            "settle": ["USDT"],
            "timeframe": ["5m"],
            "timestamp": [_timestamp()],
            "open": [-0.1],
            "high": [0.1],
            "low": [-0.2],
            "close": [0.0],
        },
        schema=REFERENCE_BAR_SCHEMA,
    )
    enforce_reference_bars(premium, premium_key)
    with pytest.raises(InvalidRequestError, match="positive"):
        enforce_reference_bars(
            premium,
            ReferenceBarKey(
                identity=identity,
                kind=ReferencePriceKind.MARK,
                timeframe="5m",
            ),
        )

    oi_key = OpenInterestKey(identity=identity, timeframe="5m")
    oi = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["perpetual"],
            "settle": ["USDT"],
            "timeframe": ["5m"],
            "timestamp": [_timestamp()],
            "open_interest_amount": [0.0],
            "open_interest_value": [None],
        },
        schema=OPEN_INTEREST_SCHEMA,
    )
    enforce_open_interest(oi, oi_key)


def test_empty_family_artifacts_remain_forbidden_outside_oi_revision_path() -> None:
    identity = _identity()
    provider = ProviderProvenance("fixture", "1", 1, "BTCUSDT", "BTC/USDT:USDT")
    cases = (
        (SettledFundingKey(identity=identity), pl.DataFrame(schema=SETTLED_FUNDING_SCHEMA)),
        (
            ReferenceBarKey(
                identity=identity,
                kind=ReferencePriceKind.MARK,
                timeframe="5m",
            ),
            pl.DataFrame(schema=REFERENCE_BAR_SCHEMA),
        ),
        (
            OpenInterestKey(identity=identity, timeframe="5m"),
            pl.DataFrame(schema=OPEN_INTEREST_SCHEMA),
        ),
    )
    for key, frame in cases:
        with pytest.raises(InvalidRequestError, match="empty family artifact"):
            family_metadata(key, YearMonth(2024, 1), frame, provider)

    contributor = ProviderEvidence(
        provider_name="fixture",
        provider_version="1",
        provider_api_version=1,
        native_market_id="BTCUSDT",
        native_symbol="BTC/USDT:USDT",
        contributed_start=_timestamp(),
        contributed_end=datetime(2024, 2, 1, tzinfo=UTC),
        source_rows=0,
        canonical_rows=0,
        duplicate_rows=0,
    )
    with pytest.raises(InvalidRequestError, match="empty family artifact"):
        family_metadata(
            OpenInterestKey(identity=identity, timeframe="5m"),
            YearMonth(2024, 1),
            pl.DataFrame(schema=OPEN_INTEREST_SCHEMA),
            provider,
            (contributor,),
        )


def _write_family_file(
    data_dir: Path,
    key: ReferenceBarKey | SettledFundingKey,
    frame: pl.DataFrame,
) -> None:
    month = YearMonth(2024, 1)
    path = paths.month_file_path(data_dir, key, month)
    path.parent.mkdir(parents=True)
    metadata = family_metadata(
        key,
        month,
        frame,
        ProviderProvenance(
            "fixture",
            "1",
            1,
            "BTCUSDT",
            "BTC/USDT:USDT",
            reference_target_scope=("contract" if isinstance(key, ReferenceBarKey) else ""),
        ),
    )
    frame.write_parquet(path, metadata=metadata)


def test_family_artifact_dispatch_recovery_and_local_read(tmp_path: Path) -> None:
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    identity = _identity()
    key = ReferenceBarKey(identity=identity, kind=ReferencePriceKind.INDEX, timeframe="5m")
    frame = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["perpetual"],
            "settle": ["USDT"],
            "timeframe": ["5m"],
            "timestamp": [_timestamp()],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
        },
        schema=REFERENCE_BAR_SCHEMA,
    )
    _write_family_file(config.data_dir, key, frame)
    committed = read_committed_file(
        config.data_dir, paths.month_file_path(config.data_dir, key, YearMonth(2024, 1))
    )
    assert committed.dataset_key == key

    result = rebuild_catalog_state(config.state_dir / CATALOG_FILE_NAME, config)
    assert result.rebuilt_datasets == (key,)
    facts = read_local_facts_for_key(
        config.state_dir,
        config.data_dir,
        key,
        _timestamp(),
        _timestamp(5),
    )
    assert facts.gaps == ()
    assert lazy_frame_for_facts(config.data_dir, facts).collect().equals(frame)


def test_funding_rebuild_restores_files_but_no_completeness(tmp_path: Path) -> None:
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    key = SettledFundingKey(identity=_identity())
    frame = pl.DataFrame(
        {
            "exchange": ["binance"],
            "symbol": ["BTC/USDT"],
            "market": ["perpetual"],
            "settle": ["USDT"],
            "effective_at": [_timestamp(1)],
            "funding_rate": [0.0],
            "funding_interval_seconds": [28_800],
            "mark_price": [100.0],
        },
        schema=SETTLED_FUNDING_SCHEMA,
    )
    _write_family_file(config.data_dir, key, frame)
    rebuild_catalog_state(config.state_dir / CATALOG_FILE_NAME, config)
    with Catalog.open_read_only(config.state_dir / CATALOG_FILE_NAME) as catalog:
        assert catalog.list_files(key)
        assert catalog.get_coverage(key) == ()


def test_previous_v4_catalog_is_rejected_without_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / CATALOG_FILE_NAME
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE schema_info "
            "(singleton INTEGER PRIMARY KEY, version INTEGER, created_at TEXT)"
        )
        connection.execute("INSERT INTO schema_info VALUES (1, 4, '2024-01-01T00:00:00+00:00')")
    before = db_path.read_bytes()
    assert detect_incompatible_state(db_path)
    with pytest.raises(CatalogError, match="incompatible"):
        Catalog.open(db_path)
    assert db_path.read_bytes() == before


def test_dataset_families_are_closed() -> None:
    assert tuple(family.value for family in DatasetFamily) == (
        "trade_bars",
        "settled_funding",
        "reference_bars",
        "open_interest",
    )
