from __future__ import annotations

import csv
import hashlib
import io
import math
import urllib.error
import zipfile
from datetime import UTC, datetime, timedelta

import pytest
from xret.data import MarketData
from xret.data.config import MarketDataConfig
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.providers.binance_data_vision import (
    BinanceDataVisionProvider,
)
from xret.data.providers.binance_data_vision import (
    provider as data_vision,
)
from xret.data.providers.discovery import load_installed_provider

_HEADER = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)
_DAY = datetime(2024, 1, 2, tzinfo=UTC)


def _archive(
    rows: list[tuple[str, str, str, str]],
    *,
    day: datetime = _DAY,
    header: tuple[str, ...] = _HEADER,
    member: str | None = None,
) -> tuple[bytes, bytes]:
    symbol = "BTCUSDT"
    stem = f"{symbol}-metrics-{day.date().isoformat()}"
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(header)
    for timestamp, amount, value, native_symbol in rows:
        writer.writerow((timestamp, native_symbol, amount, value, "1", "1", "1", "1"))
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(member or f"{stem}.csv", output.getvalue())
    payload = archive.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    return payload, f"{digest}  {stem}.zip\n".encode()


def _transport(archive: bytes, checksum: bytes):
    def fetch(url: str, limit: int) -> bytes:
        payload = checksum if url.endswith(".CHECKSUM") else archive
        assert len(payload) <= limit
        return payload

    return fetch


def _dataset(tmp_path, provider):
    return MarketData(
        config=MarketDataConfig(tmp_path / "state", tmp_path / "data"), provider=provider
    ).open_interest(
        exchange="binance",
        symbol="BTC/USDT",
        market="perpetual",
        settle="USDT",
        timeframe="5m",
    )


def _rows(count: int = 288):
    return [
        (
            (_DAY + timedelta(minutes=5 * index)).strftime("%Y-%m-%d %H:%M:%S"),
            str(index),
            str(index * 2),
            "BTCUSDT",
        )
        for index in range(count)
    ]


def test_valid_complete_day_and_installed_entry_point(tmp_path) -> None:
    archive, checksum = _archive(_rows())
    result = (
        _dataset(tmp_path, BinanceDataVisionProvider(transport=_transport(archive, checksum)))
        .fetch(_DAY, _DAY + timedelta(days=1))
        .require_complete()
    )
    assert result.data.height == 288
    assert result.data[0, "open_interest_amount"] == 0.0
    assert math.copysign(1.0, result.data[0, "open_interest_amount"]) == 1.0
    installed = load_installed_provider("binance-data-vision")
    assert isinstance(installed, BinanceDataVisionProvider)


def test_exact_duplicates_and_unsorted_rows_are_stably_normalized(tmp_path) -> None:
    rows = list(reversed(_rows()))
    archive, checksum = _archive(rows + rows)
    result = (
        _dataset(tmp_path, BinanceDataVisionProvider(transport=_transport(archive, checksum)))
        .fetch(_DAY, _DAY + timedelta(days=1))
        .require_complete()
    )
    assert result.data.height == 288
    assert result.data.get_column("timestamp").is_sorted()
    assert result.sources[0].normalizations == (
        "open_interest.decimal_to_float64",
        "open_interest.signed_zero_to_positive",
        "open_interest.stable_timestamp_sort",
        "open_interest.identical_duplicate_dedup",
    )


def test_conflict_missing_slot_and_publication_lag_are_not_complete(tmp_path) -> None:
    conflict = _rows()
    conflict.append((conflict[0][0], "9", "0", "BTCUSDT"))
    archive, checksum = _archive(conflict)
    with pytest.raises(ProviderError, match="conflicting duplicates"):
        _dataset(
            tmp_path, BinanceDataVisionProvider(transport=_transport(archive, checksum))
        ).fetch(_DAY, _DAY + timedelta(days=1))

    archive, checksum = _archive(_rows(287))
    partial = _dataset(
        tmp_path, BinanceDataVisionProvider(transport=_transport(archive, checksum))
    ).fetch(_DAY, _DAY + timedelta(days=1))
    assert partial.data.height == 287
    assert len(partial.gaps) == 1 and partial.gaps[0].status.value == "missing"

    def missing(url: str, limit: int) -> bytes:
        del limit
        raise urllib.error.HTTPError(url, 404, "not found", {}, None)

    late = _dataset(tmp_path, BinanceDataVisionProvider(transport=missing)).fetch(
        _DAY, _DAY + timedelta(days=1)
    )
    assert late.data.is_empty()
    assert len(late.gaps) == 1 and late.gaps[0].status.value == "missing"
    assert late.warnings[0].code == "source.publication_lag"


@pytest.mark.parametrize("defect", ["checksum", "member", "header", "day", "symbol"])
def test_strict_archive_validation(tmp_path, defect: str) -> None:
    day = _DAY + timedelta(days=1) if defect == "day" else _DAY
    rows = _rows()
    if defect == "symbol":
        rows[0] = (*rows[0][:3], "ETHUSDT")
    header = (*_HEADER[:-1], "wrong") if defect == "header" else _HEADER
    member = "wrong.csv" if defect == "member" else None
    archive, checksum = _archive(rows, day=day, header=header, member=member)
    if defect == "checksum":
        checksum = b"0" * 64 + checksum[64:]
    with pytest.raises(ProviderError):
        _dataset(
            tmp_path, BinanceDataVisionProvider(transport=_transport(archive, checksum))
        ).fetch(_DAY, _DAY + timedelta(days=1))


def test_provider_has_only_descriptor_and_historical_oi_pair(tmp_path) -> None:
    provider = BinanceDataVisionProvider(transport=lambda *_: b"")
    for method in (
        "resolve_market",
        "observe_bars",
        "resolve_funding_market",
        "observe_funding",
        "resolve_reference_market",
        "observe_reference_bars",
        "open_live_bars",
        "fetch_markets",
    ):
        assert not hasattr(provider, method)
    with pytest.raises(UnsupportedMarketError, match="open-interest timeframe"):
        MarketData(
            config=MarketDataConfig(tmp_path / "state", tmp_path / "data"), provider=provider
        ).open_interest(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
            timeframe="1h",
        ).fetch(_DAY, _DAY + timedelta(days=1))

    calls = 0

    def unexpected_transport(url: str, limit: int) -> bytes:
        nonlocal calls
        calls += 1
        raise AssertionError((url, limit))

    with pytest.raises(UnsupportedMarketError, match="qualified"):
        MarketData(
            config=MarketDataConfig(tmp_path / "state-eth", tmp_path / "data-eth"),
            provider=BinanceDataVisionProvider(transport=unexpected_transport),
        ).open_interest(
            exchange="binance",
            symbol="ETH/USDT",
            market="perpetual",
            settle="USDT",
            timeframe="5m",
        ).fetch(_DAY, _DAY + timedelta(days=1))
    assert calls == 0


def test_revision_set_replace_preserves_disjoint_provider_rows_and_rebuilds(
    tmp_path, monkeypatch
) -> None:
    from dataclasses import replace

    import polars as pl
    from xret.data.errors import CatalogError, SyncError
    from xret.data.providers import (
        PROVIDER_API_VERSION,
        PROVIDER_OPEN_INTEREST_SCHEMA,
        DerivativeInterpretation,
        ObservedWindow,
        OpenInterestObservation,
        ProviderDescriptor,
        ResolvedOpenInterestMarket,
    )
    from xret.data.storage.catalog import Catalog

    current = {}

    def archive_transport(url: str, limit: int) -> bytes:
        payload = current["checksum"] if url.endswith(".CHECKSUM") else current["archive"]
        assert len(payload) <= limit
        return payload

    class RecentProvider:
        descriptor = ProviderDescriptor("ccxt", "fixture", PROVIDER_API_VERSION)

        def __init__(self) -> None:
            self.observe_calls = 0

        def resolve_open_interest_market(self, identity):
            return ResolvedOpenInterestMarket(
                replace(identity, settle="USDT"),
                "BTCUSDT",
                "BTC/USDT:USDT",
                frozenset({"5m"}),
                DerivativeInterpretation(True, False, "1"),
            )

        def observe_open_interest(self, request, market):
            del market
            self.observe_calls += 1
            timestamps = [request.start + timedelta(minutes=5 * index) for index in range(288)]
            return OpenInterestObservation(
                pl.DataFrame(
                    {
                        "timestamp": timestamps,
                        "open_interest_amount": [float(index) for index in range(288)],
                        "open_interest_value": [float(index * 2) for index in range(288)],
                    },
                    schema=PROVIDER_OPEN_INTEREST_SCHEMA,
                ),
                (ObservedWindow(request.start, request.end),),
            )

    config = MarketDataConfig(tmp_path / "state", tmp_path / "data")
    initial_archive, initial_checksum = _archive(_rows())
    current.update(archive=initial_archive, checksum=initial_checksum)
    archive_data = MarketData(
        config=config,
        provider=BinanceDataVisionProvider(transport=archive_transport),
    ).open_interest(
        exchange="binance",
        symbol="BTC/USDT",
        market="perpetual",
        settle="USDT",
        timeframe="5m",
    )
    archive_data.sync(_DAY, _DAY + timedelta(days=1)).require_complete()

    recent_provider = RecentProvider()
    recent_data = MarketData(config=config, provider=recent_provider).open_interest(
        exchange="binance",
        symbol="BTC/USDT",
        market="perpetual",
        settle="USDT",
        timeframe="5m",
    )
    next_day = _DAY + timedelta(days=1)
    recent_data.sync(next_day, next_day + timedelta(days=1)).require_complete()
    assert recent_data.scan(next_day, next_day + timedelta(days=1)).collect().height == 288

    with pytest.raises(CatalogError, match="owned"):
        recent_data.sync(_DAY, _DAY + timedelta(days=1))
    assert recent_provider.observe_calls == 1

    current["checksum"] = b"0" * 64 + initial_checksum[64:]
    with pytest.raises(ProviderError, match="checksum mismatch"):
        archive_data.sync(_DAY, _DAY + timedelta(days=1))
    assert archive_data.scan(_DAY, _DAY + timedelta(days=1)).collect().height == 288
    assert recent_data.scan(next_day, next_day + timedelta(days=1)).collect().height == 288

    revised_rows = _rows()
    del revised_rows[100]
    revised_archive, revised_checksum = _archive(revised_rows)
    current.update(archive=revised_archive, checksum=revised_checksum)
    revised = archive_data.sync(_DAY, _DAY + timedelta(days=1))
    assert not revised.is_complete
    assert archive_data.scan_partial(_DAY, _DAY + timedelta(days=1)).data.collect().height == 287
    assert recent_data.scan(next_day, next_day + timedelta(days=1)).collect().height == 288

    parquet_path = next(config.data_dir.rglob("data.parquet"))
    metadata = pl.read_parquet_metadata(parquet_path)
    assert initial_checksum[:64].decode() not in metadata["contributors"]
    assert revised_checksum[:64].decode() in metadata["contributors"]
    assert '"provider_name":"ccxt"' in metadata["contributors"]

    fault_rows = _rows()
    del fault_rows[100:102]
    fault_archive, fault_checksum = _archive(fault_rows)
    current.update(archive=fault_archive, checksum=fault_checksum)
    original_record_file = Catalog.record_file

    def fail_after_record(self, file_metadata, *, run_id=None):
        original_record_file(self, file_metadata, run_id=run_id)
        raise RuntimeError("injected catalog fault")

    with monkeypatch.context() as patch:
        patch.setattr(Catalog, "record_file", fail_after_record)
        with pytest.raises(SyncError, match="after publishing canonical open-interest"):
            archive_data.sync(_DAY, _DAY + timedelta(days=1))
    assert not MarketData(config=config).maintenance.validate().is_valid
    with pytest.raises(CatalogError, match="physical hash differs"):
        recent_data.scan(next_day, next_day + timedelta(days=1))
    MarketData(config=config).maintenance.rebuild_catalog()
    assert MarketData(config=config).maintenance.validate().is_valid

    db_path = config.state_dir / "catalog.sqlite3"
    with Catalog.open_read_only(db_path) as catalog:
        ownership = catalog.list_source_ownership(revised.dataset_key)
        assert [(item[0], item[1], item[2]) for item in ownership] == [
            (_DAY, _DAY + timedelta(days=1), "binance-data-vision"),
            (next_day, next_day + timedelta(days=1), "ccxt"),
        ]
        assert (
            len(
                catalog.file_contributor_evidence(
                    parquet_path.relative_to(config.data_dir).as_posix()
                )
            )
            == 2
        )

    for path in (
        db_path,
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ):
        path.unlink(missing_ok=True)
    MarketData(config=config).maintenance.rebuild_catalog()
    assert MarketData(config=config).maintenance.validate().is_valid
    with Catalog.open_read_only(db_path) as catalog:
        assert len(catalog.list_source_ownership(revised.dataset_key)) == 2


def test_fetch_only_cross_provider_overlap_agrees_or_fails_without_publication(tmp_path) -> None:
    from dataclasses import replace

    import polars as pl
    from xret.data.open_interest import _qualify_fetch_overlap

    archive, checksum = _archive(_rows())
    dataset = _dataset(tmp_path, BinanceDataVisionProvider(transport=_transport(archive, checksum)))
    left = dataset.fetch(_DAY, _DAY + timedelta(days=1)).require_complete()
    right_data = left.data.with_columns(
        pl.when(pl.col("timestamp") == _DAY)
        .then(pl.lit(-0.0))
        .otherwise(pl.col("open_interest_amount"))
        .alias("open_interest_amount")
    )
    assert _qualify_fetch_overlap(left, replace(left, data=right_data)) == 288
    conflict = right_data.with_columns(
        pl.when(pl.col("timestamp") == _DAY)
        .then(pl.lit(1.0))
        .otherwise(pl.col("open_interest_amount"))
        .alias("open_interest_amount")
    )
    with pytest.raises(ProviderError, match="qualification conflict"):
        _qualify_fetch_overlap(left, replace(left, data=conflict))
    assert not (tmp_path / "state").exists() and not (tmp_path / "data").exists()


def test_empty_revision_retires_month_and_rebuild_cannot_resurrect_rows(tmp_path) -> None:
    current: dict[str, bytes] = {}

    def transport(url: str, limit: int) -> bytes:
        payload = current["checksum"] if url.endswith(".CHECKSUM") else current["archive"]
        assert len(payload) <= limit
        return payload

    initial_archive, initial_checksum = _archive(_rows())
    current.update(archive=initial_archive, checksum=initial_checksum)
    config = MarketDataConfig(tmp_path / "state", tmp_path / "data")
    dataset = MarketData(
        config=config,
        provider=BinanceDataVisionProvider(transport=transport),
    ).open_interest(
        exchange="binance",
        symbol="BTC/USDT",
        market="perpetual",
        settle="USDT",
        timeframe="5m",
    )
    dataset.sync(_DAY, _DAY + timedelta(days=1)).require_complete()

    empty_archive, empty_checksum = _archive([])
    current.update(archive=empty_archive, checksum=empty_checksum)
    revised = dataset.sync(_DAY, _DAY + timedelta(days=1))
    assert not revised.is_complete
    assert not tuple(config.data_dir.rglob("data.parquet"))
    assert dataset.scan_partial(_DAY, _DAY + timedelta(days=1)).data.collect().is_empty()

    db_path = config.state_dir / "catalog.sqlite3"
    for path in (
        db_path,
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ):
        path.unlink(missing_ok=True)
    MarketData(config=config).maintenance.rebuild_catalog()
    assert dataset.scan_partial(_DAY, _DAY + timedelta(days=1)).data.collect().is_empty()


def _checksum_for(payload: bytes) -> bytes:
    filename = f"BTCUSDT-metrics-{_DAY.date().isoformat()}.zip"
    return f"{hashlib.sha256(payload).hexdigest()}  {filename}\n".encode()


def test_transport_and_uncompressed_size_limits_fail_closed(tmp_path, monkeypatch) -> None:
    archive, checksum = _archive(_rows(1))
    monkeypatch.setattr(data_vision, "_MAX_OBJECT_BYTES", len(archive) - 1)
    with pytest.raises(ProviderError, match="archive response exceeds"):
        _dataset(
            tmp_path / "object",
            BinanceDataVisionProvider(
                transport=lambda url, limit: checksum if url.endswith(".CHECKSUM") else archive
            ),
        ).fetch(_DAY, _DAY + timedelta(days=1))

    monkeypatch.setattr(data_vision, "_MAX_OBJECT_BYTES", 32 * 1024 * 1024)
    monkeypatch.setattr(data_vision, "_MAX_UNCOMPRESSED_BYTES", 10)
    with pytest.raises(ProviderError, match="uncompressed size"):
        _dataset(
            tmp_path / "uncompressed",
            BinanceDataVisionProvider(transport=_transport(archive, checksum)),
        ).fetch(_DAY, _DAY + timedelta(days=1))


def test_zip_structure_ratio_encryption_crc_and_checksum_syntax(tmp_path) -> None:
    archive, checksum = _archive(_rows(1))

    for suffix in (b" ../wrong.zip\n", b"  directory/wrong.zip\n", b"  WRONG.zip"):
        malformed = checksum[:64] + suffix
        with pytest.raises(ProviderError, match="checksum file"):
            _dataset(
                tmp_path / hashlib.sha256(suffix).hexdigest()[:8],
                BinanceDataVisionProvider(transport=_transport(archive, malformed)),
            ).fetch(_DAY, _DAY + timedelta(days=1))

    multi = io.BytesIO(archive)
    with zipfile.ZipFile(multi, "a") as bundle:
        bundle.writestr("extra.csv", "x")
    multi_payload = multi.getvalue()
    with pytest.raises(ProviderError, match="exactly its expected CSV member"):
        _dataset(
            tmp_path / "multiple",
            BinanceDataVisionProvider(
                transport=_transport(multi_payload, _checksum_for(multi_payload))
            ),
        ).fetch(_DAY, _DAY + timedelta(days=1))

    ratio = io.BytesIO()
    member = f"BTCUSDT-metrics-{_DAY.date().isoformat()}.csv"
    with zipfile.ZipFile(ratio, "w", zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(member, "A" * 100_000)
    ratio_payload = ratio.getvalue()
    with pytest.raises(ProviderError, match="compression-ratio"):
        _dataset(
            tmp_path / "ratio",
            BinanceDataVisionProvider(
                transport=_transport(ratio_payload, _checksum_for(ratio_payload))
            ),
        ).fetch(_DAY, _DAY + timedelta(days=1))

    encrypted = bytearray(archive)
    local = encrypted.index(b"PK\x03\x04")
    central = encrypted.index(b"PK\x01\x02")
    encrypted[local + 6] |= 1
    encrypted[central + 8] |= 1
    encrypted_payload = bytes(encrypted)
    with pytest.raises(ProviderError, match="valid bounded ZIP"):
        _dataset(
            tmp_path / "encrypted",
            BinanceDataVisionProvider(
                transport=_transport(encrypted_payload, _checksum_for(encrypted_payload))
            ),
        ).fetch(_DAY, _DAY + timedelta(days=1))

    stored = io.BytesIO()
    with zipfile.ZipFile(stored, "w", zipfile.ZIP_STORED) as bundle:
        bundle.writestr(member, b"create_time,symbol\n")
    corrupted = bytearray(stored.getvalue())
    payload_offset = corrupted.index(b"create_time")
    corrupted[payload_offset] ^= 1
    corrupted_payload = bytes(corrupted)
    with pytest.raises(ProviderError, match="valid bounded ZIP"):
        _dataset(
            tmp_path / "crc",
            BinanceDataVisionProvider(
                transport=_transport(corrupted_payload, _checksum_for(corrupted_payload))
            ),
        ).fetch(_DAY, _DAY + timedelta(days=1))
