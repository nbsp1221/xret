"""Focused filesystem contracts for self-describing Parquet artifacts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from xret.data.config import MarketDataConfig
from xret.data.errors import CatalogError, InvalidRequestError, SyncError
from xret.data.models import NONE_SETTLE_SENTINEL, DatasetKey, Market, YearMonth
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage import parquet, paths
from xret.data.storage.recovery import discover_committed_files

DATASET_KEY = DatasetKey(
    exchange="binance",
    symbol="BTC/USDT",
    market=Market.SPOT,
    settle=NONE_SETTLE_SENTINEL,
    timeframe="1m",
)
YEAR_MONTH = YearMonth(year=2024, month=1)
PROVIDER = parquet.ProviderIdentity("ccxt", "4.4.0", "BTCUSDT", "BTC/USDT")


def test_readable_paths_project_spot_and_perpetual_identity_exactly(tmp_path: Path) -> None:
    perpetual = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.PERPETUAL,
        settle="USDT",
        timeframe="1h",
    )

    assert paths.relative_month_file_path(tmp_path, DATASET_KEY, YEAR_MONTH) == (
        "binance/spot/BTC-USDT/1m/year=2024/month=01/data.parquet"
    )
    assert paths.relative_month_file_path(tmp_path, perpetual, YEAR_MONTH) == (
        "binance/perpetual/BTC-USDT-USDT/1h/year=2024/month=01/data.parquet"
    )
    assert "%2F" not in str(paths.dataset_dir(tmp_path, DATASET_KEY))
    assert "/none/" not in str(paths.dataset_dir(tmp_path, DATASET_KEY))


@pytest.mark.parametrize(
    ("component", "encoded"),
    [
        ("-", "%2D"),
        ("%", "%25"),
        ("a b", "a%20b"),
        ("\n", "%0A"),
        ("\u200e", "%E2%80%8E"),
        ("💵", "💵"),
        ("老板", "老板"),
        (".ABC", "%2EABC"),
        ("ABC.", "ABC%2E"),
        ("A.B", "A.B"),
    ],
)
def test_instrument_slug_escapes_only_collision_or_invisible_components(
    component: str, encoded: str
) -> None:
    assert paths.encode_slug_component(component) == encoded


def test_instrument_slug_is_nfc_normalized_and_collision_safe() -> None:
    left = DatasetKey(
        exchange="binance",
        symbol="DOGE-1/USDT",
        market=Market.SPOT,
        settle=NONE_SETTLE_SENTINEL,
        timeframe="1m",
    )
    right = DatasetKey(
        exchange="binance",
        symbol="DOGE/1-USDT",
        market=Market.SPOT,
        settle=NONE_SETTLE_SENTINEL,
        timeframe="1m",
    )
    unicode_key = DatasetKey(
        exchange="binance",
        symbol="Café/김치",
        market=Market.PERPETUAL,
        settle="$TRDL",
        timeframe="1m",
    )

    assert paths.instrument_slug(left) == "DOGE%2D1-USDT"
    assert paths.instrument_slug(right) == "DOGE-1%2DUSDT"
    assert paths.instrument_slug(left) != paths.instrument_slug(right)
    assert paths.instrument_slug(unicode_key) == "Café-김치-$TRDL"


@pytest.mark.parametrize(
    ("component", "encoded"),
    [
        ("\\", "%5C"),
        (".", "%2E"),
        ("..", "%2E%2E"),
        ("...", "%2E.%2E"),
    ],
    ids=["backslash", "dot", "dot-dot", "all-dots"],
)
def test_instrument_slug_escapes_backslashes_and_traversal_like_components(
    component: str, encoded: str
) -> None:
    assert paths.encode_slug_component(component) == encoded


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (
            datetime(2024, 1, 31, 23, tzinfo=UTC),
            datetime(2024, 2, 1, 1, tzinfo=UTC),
            [
                (
                    YearMonth(2024, 1),
                    datetime(2024, 1, 31, 23, tzinfo=UTC),
                    datetime(2024, 2, 1, tzinfo=UTC),
                ),
                (
                    YearMonth(2024, 2),
                    datetime(2024, 2, 1, tzinfo=UTC),
                    datetime(2024, 2, 1, 1, tzinfo=UTC),
                ),
            ],
        ),
        (
            datetime(2024, 12, 31, 23, tzinfo=UTC),
            datetime(2025, 1, 1, 1, tzinfo=UTC),
            [
                (
                    YearMonth(2024, 12),
                    datetime(2024, 12, 31, 23, tzinfo=UTC),
                    datetime(2025, 1, 1, tzinfo=UTC),
                ),
                (
                    YearMonth(2025, 1),
                    datetime(2025, 1, 1, tzinfo=UTC),
                    datetime(2025, 1, 1, 1, tzinfo=UTC),
                ),
            ],
        ),
    ],
    ids=["month-boundary", "year-boundary"],
)
def test_iter_month_slices_clips_calendar_boundaries(
    start: datetime,
    end: datetime,
    expected: list[tuple[YearMonth, datetime, datetime]],
) -> None:
    assert list(paths.iter_month_slices(start, end)) == expected


def make_batch(
    n: int, *, start_minute: int = 0, dataset_key: DatasetKey = DATASET_KEY
) -> pl.DataFrame:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    timestamps = [base + timedelta(minutes=start_minute + index) for index in range(n)]
    return pl.DataFrame(
        {
            "exchange": [dataset_key.exchange] * n,
            "symbol": [dataset_key.symbol] * n,
            "market": [dataset_key.market.value] * n,
            "settle": [None if dataset_key.market is Market.SPOT else dataset_key.settle] * n,
            "timeframe": [dataset_key.timeframe] * n,
            "timestamp": timestamps,
            "open": [1.0 + index for index in range(n)],
            "high": [2.0 + index for index in range(n)],
            "low": [0.5 + index for index in range(n)],
            "close": [1.5 + index for index in range(n)],
            "volume": [10.0 + index for index in range(n)],
        },
        schema=OHLCV_SCHEMA,
    )


def rewrite_metadata(path: Path, **updates: str) -> None:
    frame = pl.read_parquet(path)
    frame.write_parquet(path, metadata=pl.read_parquet_metadata(path) | updates)


def test_prepared_file_has_only_domain_self_description(tmp_path: Path) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(3), provider=PROVIDER
    )
    metadata = pl.read_parquet_metadata(prepared.temp_path)

    required = {
        "schema_version",
        "exchange",
        "symbol",
        "market",
        "timeframe",
        "year",
        "month",
        "row_count",
        "min_timestamp",
        "max_timestamp",
        "provider_name",
        "ccxt_version",
        "provider_market_id",
        "native_symbol",
    }
    forbidden = {
        "run_id",
        "run_ids",
        "content_hash",
        "physical_hash",
        "requested_start",
        "requested_end",
        "created_at",
        "updated_at",
        "completed_at",
        "warnings",
        "quality",
    }
    assert required <= metadata.keys()
    assert "settle" not in metadata
    assert not forbidden.intersection(metadata)
    assert set(metadata).difference(required | {"ARROW:schema"}) == set()
    assert prepared.committed_file.physical_hash not in metadata.values()
    parquet.discard_prepared_file(prepared)


def test_perpetual_metadata_includes_explicit_derivative_interpretation(tmp_path: Path) -> None:
    key = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.PERPETUAL,
        settle="USDT",
        timeframe="1m",
    )
    provider = parquet.ProviderIdentity("ccxt", "4.4.0", "BTCUSDT", "BTC/USDT:USDT")
    prepared = parquet.prepare_month(
        tmp_path,
        key,
        YEAR_MONTH,
        make_batch(2, dataset_key=key),
        provider=provider,
        derivative=parquet.DerivativeInterpretation(linear=True, inverse=False, contract_size="1"),
    )
    metadata = pl.read_parquet_metadata(prepared.temp_path)
    assert metadata["settle"] == "USDT"
    assert {"linear": "true", "inverse": "false", "contract_size": "1"}.items() <= metadata.items()
    parquet.discard_prepared_file(prepared)


def test_committed_perpetual_file_reconstructs_metadata_first_identity_and_path(
    tmp_path: Path,
) -> None:
    key = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.PERPETUAL,
        settle="USDT",
        timeframe="1m",
    )
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(
            tmp_path,
            key,
            YEAR_MONTH,
            make_batch(2, dataset_key=key),
            provider=parquet.ProviderIdentity("ccxt", "4.4.0", "BTCUSDT", "BTC/USDT:USDT"),
            derivative=parquet.DerivativeInterpretation(
                linear=True, inverse=False, contract_size="1"
            ),
        )
    )

    reconstructed = parquet.read_committed_file(tmp_path, committed.absolute_path)

    assert reconstructed.dataset_key == key
    assert reconstructed.absolute_path == paths.month_file_path(tmp_path, key, YEAR_MONTH)
    assert reconstructed.relative_path == (
        "binance/perpetual/BTC-USDT-USDT/1m/year=2024/month=01/data.parquet"
    )
    metadata = pl.read_parquet_metadata(committed.absolute_path)
    assert metadata["market"] == "perpetual"
    assert metadata["settle"] == "USDT"
    assert {"linear": "true", "inverse": "false", "contract_size": "1"}.items() <= metadata.items()


def test_conflicting_nonempty_derivative_interpretation_fails_closed(tmp_path: Path) -> None:
    key = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.PERPETUAL,
        settle="USDT",
        timeframe="1m",
    )
    provider = parquet.ProviderIdentity("ccxt", "4.4.0", "BTCUSDT", "BTC/USDT:USDT")
    first = parquet.prepare_month(
        tmp_path,
        key,
        YEAR_MONTH,
        make_batch(2, dataset_key=key),
        provider=provider,
        derivative=parquet.DerivativeInterpretation(linear=True, contract_size="1"),
    )
    parquet.publish_prepared_file(first)

    canonical_bytes = first.committed_file.absolute_path.read_bytes()
    canonical_hash = first.committed_file.physical_hash
    canonical_metadata = pl.read_parquet_metadata(first.committed_file.absolute_path)

    with pytest.raises(SyncError, match="conflicting derivative"):
        parquet.prepare_month(
            tmp_path,
            key,
            YEAR_MONTH,
            make_batch(1, dataset_key=key),
            provider=provider,
            derivative=parquet.DerivativeInterpretation(linear=False, contract_size="1"),
        )

    assert first.committed_file.absolute_path.read_bytes() == canonical_bytes
    assert parquet.compute_content_hash(first.committed_file.absolute_path) == canonical_hash
    assert pl.read_parquet_metadata(first.committed_file.absolute_path) == canonical_metadata
    assert list(paths.iter_temp_files(first.committed_file.absolute_path.parent)) == []


def test_prepare_deep_validates_then_publish_preserves_physical_digest(tmp_path: Path) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(4), provider=PROVIDER
    )
    assert pl.read_parquet(prepared.temp_path).schema == OHLCV_SCHEMA
    assert parquet.compute_content_hash(prepared.temp_path) == prepared.committed_file.physical_hash

    committed = parquet.publish_prepared_file(prepared)
    assert prepared.published
    assert committed.absolute_path.is_file()
    assert parquet.compute_content_hash(committed.absolute_path) == committed.physical_hash
    assert committed.physical_hash == prepared.committed_file.physical_hash


def test_prepare_month_rejects_off_timeframe_timestamp_before_publication(tmp_path: Path) -> None:
    batch = make_batch(1).with_columns(
        (pl.col("timestamp") + pl.duration(seconds=30)).alias("timestamp")
    )

    with pytest.raises(InvalidRequestError, match=r"off '1m' boundaries"):
        parquet.prepare_month(tmp_path, DATASET_KEY, YEAR_MONTH, batch, provider=PROVIDER)

    directory = paths.month_dir(tmp_path, DATASET_KEY, YEAR_MONTH)
    assert not paths.month_file_path(tmp_path, DATASET_KEY, YEAR_MONTH).exists()
    assert list(paths.iter_temp_files(directory)) == []


def test_prepare_flush_failure_raises_sync_error_and_removes_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_fsync(_: int) -> None:
        raise OSError("disk failure")

    monkeypatch.setattr(parquet.os, "fsync", fail_fsync)

    with pytest.raises(SyncError, match="failed to flush prepared artifact"):
        parquet.prepare_month(tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(1), provider=PROVIDER)

    directory = paths.month_dir(tmp_path, DATASET_KEY, YEAR_MONTH)
    assert list(paths.iter_temp_files(directory)) == []


def test_prepare_merges_month_rows_without_operational_provenance(tmp_path: Path) -> None:
    first = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(4), provider=PROVIDER
    )
    parquet.publish_prepared_file(first)
    second = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(3, start_minute=2), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(second)

    frame = pl.read_parquet(committed.absolute_path)
    assert frame.height == 5
    assert frame.get_column("timestamp").is_sorted()
    assert (
        frame.select(["exchange", "symbol", "market", "settle", "timeframe", "timestamp"])
        .is_duplicated()
        .sum()
        == 0
    )
    assert "run_ids" not in pl.read_parquet_metadata(committed.absolute_path)


def test_discard_only_removes_unpublished_owned_temp(tmp_path: Path) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(2), provider=PROVIDER
    )
    foreign = prepared.temp_path.parent / "foreign.txt"
    foreign.write_text("keep")
    parquet.discard_prepared_file(prepared)

    assert not prepared.temp_path.exists()
    assert not prepared.committed_file.absolute_path.exists()
    assert foreign.read_text() == "keep"


def test_discard_after_publish_preserves_canonical_artifact_and_leaks_no_temp(
    tmp_path: Path,
) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(2), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(prepared)
    canonical_bytes = committed.absolute_path.read_bytes()
    canonical_metadata = pl.read_parquet_metadata(committed.absolute_path)

    parquet.discard_prepared_file(prepared)

    assert committed.absolute_path.read_bytes() == canonical_bytes
    assert parquet.compute_content_hash(committed.absolute_path) == committed.physical_hash
    assert pl.read_parquet_metadata(committed.absolute_path) == canonical_metadata
    assert list(paths.iter_temp_files(committed.absolute_path.parent)) == []


def test_before_replace_fault_leaves_no_canonical_file_and_temp_can_be_discarded(
    tmp_path: Path,
) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(2), provider=PROVIDER
    )

    def fail(checkpoint: str) -> None:
        if checkpoint == "before_replace":
            raise RuntimeError("fault")

    with pytest.raises(RuntimeError, match="fault"):
        parquet.publish_prepared_file(prepared, crash_hook=fail)
    assert not prepared.published
    assert not prepared.committed_file.absolute_path.exists()
    parquet.discard_prepared_file(prepared)
    assert not prepared.temp_path.exists()


def test_after_replace_fault_marks_published_at_irreversible_boundary(tmp_path: Path) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(2), provider=PROVIDER
    )

    def fail(checkpoint: str) -> None:
        if checkpoint == "after_replace":
            raise RuntimeError("post-publication fault")

    with pytest.raises(RuntimeError, match="post-publication"):
        parquet.publish_prepared_file(prepared, crash_hook=fail)
    assert prepared.published
    assert prepared.committed_file.absolute_path.is_file()
    assert (
        parquet.compute_content_hash(prepared.committed_file.absolute_path)
        == prepared.committed_file.physical_hash
    )


def test_read_committed_file_deeply_checks_metadata_against_rows_and_path(tmp_path: Path) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(3), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(prepared)
    reread = parquet.read_committed_file(tmp_path, committed.absolute_path)
    assert reread == committed
    assert committed.absolute_path == paths.month_file_path(
        tmp_path, reread.dataset_key, reread.year_month
    )


def test_off_timeframe_committed_file_is_rejected_for_canonical_and_rebuild_evidence(
    tmp_path: Path,
) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(1), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(prepared)
    timestamp = datetime(2024, 1, 1, 0, 0, 30, tzinfo=UTC)
    frame = pl.read_parquet(committed.absolute_path).with_columns(
        pl.lit(timestamp).cast(OHLCV_SCHEMA["timestamp"]).alias("timestamp")
    )
    metadata = pl.read_parquet_metadata(committed.absolute_path) | {
        "min_timestamp": timestamp.isoformat(),
        "max_timestamp": timestamp.isoformat(),
    }
    frame.write_parquet(committed.absolute_path, metadata=metadata)

    with pytest.raises(CatalogError, match=r"off '1m' boundaries"):
        parquet.read_committed_file(tmp_path, committed.absolute_path)

    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path)
    with pytest.raises(CatalogError, match=r"off '1m' boundaries"):
        list(discover_committed_files(config))


@pytest.mark.parametrize(
    ("defect", "finding"),
    [
        ("broken_ohlc", "ohlc.invariant_violation"),
        ("null_price", "null.disallowed"),
        ("nonfinite_price", "price.non_finite"),
        ("nonpositive_price", "price.non_positive"),
        ("negative_volume", "volume.negative"),
        ("duplicate_identity", "identity.duplicate"),
        ("unordered_timestamp", "timestamp.unordered"),
    ],
)
def test_read_committed_file_rejects_malformed_canonical_rows(
    tmp_path: Path, defect: str, finding: str
) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(2), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(prepared)
    frame = pl.read_parquet(committed.absolute_path)
    if defect == "broken_ohlc":
        frame = frame.with_columns(pl.lit(0.25).alias("high"))
    elif defect == "null_price":
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("open"))
    elif defect == "nonfinite_price":
        frame = frame.with_columns(pl.lit(float("inf")).alias("open"))
    elif defect == "nonpositive_price":
        frame = frame.with_columns(pl.lit(0.0).alias("open"))
    elif defect == "negative_volume":
        frame = frame.with_columns(pl.lit(-1.0).alias("volume"))
    elif defect == "duplicate_identity":
        frame = pl.concat([frame, frame.head(1)])
    else:
        frame = frame.reverse()

    frame.write_parquet(
        committed.absolute_path, metadata=pl.read_parquet_metadata(committed.absolute_path)
    )

    with pytest.raises(CatalogError, match=finding):
        parquet.read_committed_file(tmp_path, committed.absolute_path)


@pytest.mark.parametrize(
    ("metadata_key", "metadata_value"),
    [
        ("row_count", "4"),
        ("min_timestamp", datetime(2024, 1, 1, 0, 1, tzinfo=UTC).isoformat()),
        ("max_timestamp", datetime(2024, 1, 1, 0, 1, tzinfo=UTC).isoformat()),
        ("exchange", "tampered"),
    ],
    ids=["row-count", "min-timestamp", "max-timestamp", "dataset-identity"],
)
def test_read_committed_file_rejects_tampered_metadata(
    tmp_path: Path, metadata_key: str, metadata_value: str
) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(3), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(prepared)
    rewrite_metadata(committed.absolute_path, **{metadata_key: metadata_value})

    with pytest.raises(CatalogError, match="does not match"):
        parquet.read_committed_file(tmp_path, committed.absolute_path)


@pytest.mark.parametrize(
    ("metadata_key", "metadata_value"),
    [("market", "spot"), ("settle", "BUSD")],
    ids=["market", "settle"],
)
def test_read_committed_file_rejects_tampered_perpetual_identity_metadata(
    tmp_path: Path, metadata_key: str, metadata_value: str
) -> None:
    key = DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.PERPETUAL,
        settle="USDT",
        timeframe="1m",
    )
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(
            tmp_path,
            key,
            YEAR_MONTH,
            make_batch(2, dataset_key=key),
            provider=parquet.ProviderIdentity("ccxt", "4.4.0", "BTCUSDT", "BTC/USDT:USDT"),
            derivative=parquet.DerivativeInterpretation(linear=True, contract_size="1"),
        )
    )
    rewrite_metadata(committed.absolute_path, **{metadata_key: metadata_value})

    with pytest.raises(CatalogError):
        parquet.read_committed_file(tmp_path, committed.absolute_path)


def test_read_committed_file_rejects_month_path_mismatch(tmp_path: Path) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(3), provider=PROVIDER
    )
    committed = parquet.publish_prepared_file(prepared)
    mismatched_path = paths.month_file_path(tmp_path, DATASET_KEY, YearMonth(year=2024, month=2))
    mismatched_path.parent.mkdir(parents=True)
    committed.absolute_path.replace(mismatched_path)

    with pytest.raises(CatalogError, match="metadata-derived path"):
        parquet.read_committed_file(tmp_path, mismatched_path)


@pytest.mark.parametrize("fault", ["open", "fsync", "close"])
def test_publish_tolerates_best_effort_directory_durability_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    prepared = parquet.prepare_month(
        tmp_path, DATASET_KEY, YEAR_MONTH, make_batch(1), provider=PROVIDER
    )

    monkeypatch.setattr(
        parquet.os,
        fault,
        lambda *_: (_ for _ in ()).throw(OSError(f"directory {fault} failure")),
    )

    committed = parquet.publish_prepared_file(prepared)

    assert committed.absolute_path.is_file()
    assert parquet.read_committed_file(tmp_path, committed.absolute_path) == committed


def test_symlinked_managed_ancestor_is_rejected_before_canonical_read_and_write(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(data_dir, DATASET_KEY, YEAR_MONTH, make_batch(1), provider=PROVIDER)
    )
    managed_ancestor = data_dir / DATASET_KEY.exchange
    escaped_ancestor = tmp_path / "escaped"
    managed_ancestor.replace(escaped_ancestor)
    managed_ancestor.symlink_to(escaped_ancestor, target_is_directory=True)

    with pytest.raises(CatalogError, match="symlink"):
        parquet.read_committed_file(data_dir, committed.absolute_path)
    with pytest.raises(SyncError, match="symlink"):
        parquet.prepare_month(data_dir, DATASET_KEY, YEAR_MONTH, make_batch(1), provider=PROVIDER)


@pytest.mark.parametrize(
    "batch",
    [
        make_batch(1).with_columns(pl.lit("tampered").alias("exchange")),
        pl.concat([make_batch(1), make_batch(1)]),
        make_batch(1).with_columns((pl.col("timestamp") + pl.duration(days=31)).alias("timestamp")),
    ],
    ids=["malformed-identity", "duplicate-identity", "out-of-month"],
)
def test_prepare_month_rejects_invalid_batch_before_publication(
    tmp_path: Path, batch: pl.DataFrame
) -> None:
    with pytest.raises(InvalidRequestError):
        parquet.prepare_month(tmp_path, DATASET_KEY, YEAR_MONTH, batch, provider=PROVIDER)

    directory = paths.month_dir(tmp_path, DATASET_KEY, YEAR_MONTH)
    assert not paths.month_file_path(tmp_path, DATASET_KEY, YEAR_MONTH).exists()
    assert list(paths.iter_temp_files(directory)) == []


def test_read_committed_file_rejects_missing_self_description(tmp_path: Path) -> None:
    directory = paths.month_dir(tmp_path, DATASET_KEY, YEAR_MONTH)
    directory.mkdir(parents=True)
    make_batch(2).write_parquet(directory / paths.DATA_FILE_NAME)
    with pytest.raises(CatalogError, match="missing required metadata"):
        parquet.read_committed_file(tmp_path, directory / paths.DATA_FILE_NAME)
