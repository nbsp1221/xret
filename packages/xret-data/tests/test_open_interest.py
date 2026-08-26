from __future__ import annotations

import inspect
import math
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from xret.data import MarketData, OpenInterestDataset
from xret.data.config import MarketDataConfig
from xret.data.errors import (
    CatalogError,
    InvalidRequestError,
    ProviderError,
    SyncError,
    UnsupportedMarketError,
)
from xret.data.open_interest_quality import enforce_open_interest
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_OPEN_INTEREST_SCHEMA,
    DerivativeInterpretation,
    ObservedWindow,
    OpenInterestObservation,
    OpenInterestRequest,
    OpenInterestSourceEvidence,
    ProviderDescriptor,
    ResolvedOpenInterestMarket,
)
from xret.data.providers.ccxt import CcxtProvider
from xret.data.providers.ccxt.oi_pagination import paginate_open_interest_history
from xret.data.schema import OPEN_INTEREST_SCHEMA
from xret.data.storage import catalog as catalog_storage
from xret.data.storage.catalog import Catalog, IngestionRunMetadata, _CommitUncertainCatalogError


def _at(minute: int) -> datetime:
    return datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=minute)


def _provider_frame(
    rows: list[tuple[datetime, float, float | None]],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": [row[0] for row in rows],
            "open_interest_amount": [row[1] for row in rows],
            "open_interest_value": [row[2] for row in rows],
        },
        schema=PROVIDER_OPEN_INTEREST_SCHEMA,
    )


class OpenInterestProvider:
    descriptor = ProviderDescriptor("oi-fixture", "1", PROVIDER_API_VERSION)

    def __init__(
        self,
        rows: list[tuple[datetime, float, float | None]],
        *,
        observed: tuple[ObservedWindow, ...] | None = None,
        contract_size: str = "1",
    ) -> None:
        self.rows = rows
        self.observed = observed
        self.contract_size = contract_size
        self.resolve_calls = 0
        self.observe_calls = 0

    def resolve_open_interest_market(self, identity):
        self.resolve_calls += 1
        if identity.settle is None:
            from dataclasses import replace

            identity = replace(identity, settle="USDT")
        return ResolvedOpenInterestMarket(
            identity,
            "BTCUSDT",
            "BTC/USDT:USDT",
            frozenset({"5m"}),
            DerivativeInterpretation(True, False, self.contract_size),
        )

    def observe_open_interest(
        self, request: OpenInterestRequest, market: ResolvedOpenInterestMarket
    ) -> OpenInterestObservation:
        self.observe_calls += 1
        rows = [row for row in self.rows if request.start <= row[0] < request.end]
        observed = self.observed
        if observed is None:
            observed = (ObservedWindow(request.start, request.end),)
        return OpenInterestObservation(_provider_frame(rows), observed)


def _dataset(tmp_path: Path, provider: object) -> tuple[OpenInterestDataset, MarketDataConfig]:
    config = MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")
    return (
        MarketData(config=config, provider=provider).open_interest(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
            timeframe="5m",
        ),
        config,
    )


def test_binding_has_exact_signature_is_io_free_and_names_a_gauge(tmp_path: Path) -> None:
    signature = inspect.signature(MarketData.open_interest)
    assert str(signature) == (
        "(self, *, exchange: 'str', symbol: 'str', market: 'str', "
        "settle: 'str | None' = None, timeframe: 'str') -> 'OpenInterestDataset'"
    )
    provider = OpenInterestProvider([])
    dataset, config = _dataset(tmp_path, provider)
    assert provider.resolve_calls == provider.observe_calls == 0
    assert dataset.timeframe == "5m"
    assert not hasattr(dataset, "direction") and not hasattr(dataset, "flow")
    assert not config.state_dir.exists() and not config.data_dir.exists()


def test_fetch_round_trips_zero_nonnegative_nullable_values_without_local_io(
    tmp_path: Path,
) -> None:
    rows = [(_at(0), 0.0, None), (_at(5), 2.5, 100.0)]
    dataset, config = _dataset(tmp_path, OpenInterestProvider(rows))
    result = dataset.fetch(_at(0), _at(10)).require_complete()
    assert result.data.schema == OPEN_INTEREST_SCHEMA
    assert result.data.get_column("open_interest_amount").to_list() == [0.0, 2.5]
    assert result.data.get_column("open_interest_value").to_list() == [None, 100.0]
    assert not config.state_dir.exists() and not config.data_dir.exists()


@pytest.mark.parametrize("column", ["open_interest_amount", "open_interest_value"])
@pytest.mark.parametrize("value", [-1.0, float("nan"), float("inf")])
def test_quality_rejects_negative_and_nonfinite_values(column: str, value: float) -> None:
    data = {
        "exchange": ["binance"],
        "symbol": ["BTC/USDT"],
        "market": ["perpetual"],
        "settle": ["USDT"],
        "timeframe": ["5m"],
        "timestamp": [_at(0)],
        "open_interest_amount": [1.0],
        "open_interest_value": [1.0],
    }
    data[column] = [value]
    from xret.data.models import MarketIdentity, OpenInterestKey

    key = OpenInterestKey(
        identity=MarketIdentity(
            exchange="binance", symbol="BTC/USDT", market="perpetual", settle="USDT"
        ),
        timeframe="5m",
    )
    with pytest.raises(InvalidRequestError, match="finite and nonnegative"):
        enforce_open_interest(pl.DataFrame(data, schema=OPEN_INTEREST_SCHEMA), key)


def test_runtime_rejects_unaligned_duplicate_and_conflicting_provider_rows(tmp_path: Path) -> None:
    for rows, message in (
        ([(_at(1), 1.0, None)], "align"),
        ([(_at(0), 1.0, None), (_at(0), 1.0, None)], "strictly increasing"),
        ([(_at(0), 1.0, None), (_at(0), 2.0, None)], "strictly increasing"),
    ):
        dataset, _ = _dataset(tmp_path, OpenInterestProvider(rows))
        with pytest.raises(ProviderError, match=message):
            dataset.fetch(_at(0), _at(5))


def test_missing_grid_slot_is_unavailable_only_with_exhaustive_provider_evidence(
    tmp_path: Path,
) -> None:
    exhaustive, _ = _dataset(tmp_path, OpenInterestProvider([(_at(0), 1.0, None)]))
    result = exhaustive.fetch(_at(0), _at(10))
    assert not result.is_complete
    assert result.gaps[0].status.value == "unavailable"

    presence_only, _ = _dataset(
        tmp_path,
        OpenInterestProvider(
            [(_at(0), 1.0, None)],
            observed=(ObservedWindow(_at(0), _at(5)),),
        ),
    )
    result = presence_only.fetch(_at(0), _at(10))
    assert result.gaps[0].status.value == "missing"


def test_sync_strict_partial_idempotence_and_rebuild_preserve_only_row_grid(
    tmp_path: Path,
) -> None:
    provider = OpenInterestProvider([(_at(0), 1.0, None), (_at(5), 2.0, 20.0)])
    dataset, config = _dataset(tmp_path, provider)
    first = dataset.sync(_at(0), _at(10)).require_complete()
    assert first.written_partitions == 1 and first.fetched_rows == 2
    assert dataset.scan(_at(0), _at(10)).collect().height == 2
    assert dataset.scan_partial(_at(0), _at(10)).is_complete
    second = dataset.sync(_at(0), _at(10)).require_complete()
    metadata = pl.read_parquet_metadata(next(config.data_dir.rglob("data.parquet")))
    assert metadata["derivative_linear"] == "true"
    assert metadata["derivative_inverse"] == "false"
    assert metadata["contract_size"] == "1"
    assert metadata["source_field_mapping"] == ""
    assert not second.changed and second.sources == () and provider.observe_calls == 1

    db_path = config.state_dir / "catalog.sqlite3"
    for path in (
        db_path,
        db_path.with_name(db_path.name + "-wal"),
        db_path.with_name(db_path.name + "-shm"),
    ):
        path.unlink(missing_ok=True)
    MarketData(config=config).maintenance.rebuild_catalog()
    assert dataset.scan(_at(0), _at(10)).collect().height == 2


def test_sync_rejects_changed_conversion_provenance_for_existing_partition(
    tmp_path: Path,
) -> None:
    provider = OpenInterestProvider([(_at(0), 1.0, None), (_at(5), 2.0, None)], contract_size="1")
    dataset, _ = _dataset(tmp_path, provider)
    dataset.sync(_at(0), _at(5)).require_complete()

    provider.contract_size = "0.001"
    with pytest.raises(SyncError, match="conflicting open-interest provenance.*contract_size"):
        dataset.sync(_at(5), _at(10))

    assert dataset.scan(_at(0), _at(5)).collect().height == 1
    assert dataset.scan_partial(_at(5), _at(10)).data.collect().is_empty()


def test_provider_evidence_counts_rows_per_contributed_range(tmp_path: Path) -> None:
    provider = OpenInterestProvider(
        [(_at(0), 1.0, None), (_at(10), 2.0, None)],
        observed=(
            ObservedWindow(_at(0), _at(5)),
            ObservedWindow(_at(10), _at(15)),
        ),
    )
    dataset, _ = _dataset(tmp_path, provider)

    result = dataset.fetch(_at(0), _at(15))

    assert [source.source_rows for source in result.sources] == [1, 1]
    assert [source.canonical_rows for source in result.sources] == [1, 1]
    assert [source.duplicate_rows for source in result.sources] == [0, 0]


def test_runtime_rejects_contributor_count_that_disagrees_with_rows(tmp_path: Path) -> None:
    class IncorrectEvidenceProvider(OpenInterestProvider):
        def observe_open_interest(self, request, market):
            observation = super().observe_open_interest(request, market)
            return OpenInterestObservation(
                observation.frame,
                observation.observed,
                (
                    OpenInterestSourceEvidence(
                        start=request.start,
                        end=request.end,
                        source_route="fixture",
                        source_revision="fixture:1",
                        source_rows=0,
                        canonical_rows=0,
                        duplicate_rows=0,
                    ),
                ),
            )

    dataset, _ = _dataset(
        tmp_path,
        IncorrectEvidenceProvider([(_at(0), 1.0, None)]),
    )

    with pytest.raises(ProviderError, match="canonical row count does not match"):
        dataset.fetch(_at(0), _at(5))


def test_recovery_rejects_tampered_open_interest_contract_size(tmp_path: Path) -> None:
    dataset, config = _dataset(tmp_path, OpenInterestProvider([(_at(0), 1.0, None)]))
    dataset.sync(_at(0), _at(5)).require_complete()
    path = next(config.data_dir.rglob("data.parquet"))
    frame = pl.read_parquet(path)
    metadata = pl.read_parquet_metadata(path) | {"contract_size": "0"}
    frame.write_parquet(path, metadata=metadata)

    with pytest.raises(CatalogError, match="invalid provider metadata") as captured:
        MarketData(config=config).maintenance.validate()
    assert isinstance(captured.value.__cause__, CatalogError)
    assert "invalid contract-size provenance" in str(captured.value.__cause__)


def test_provider_failure_and_catalog_fault_do_not_claim_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Failing(OpenInterestProvider):
        def observe_open_interest(self, request, market):
            raise RuntimeError("transport failed")

    failing, config = _dataset(tmp_path, Failing([]))
    with pytest.raises(ProviderError, match="failed to observe open interest"):
        failing.sync(_at(0), _at(5))
    assert not tuple(config.data_dir.rglob("*.parquet"))
    assert failing.scan_partial(_at(0), _at(5)).gaps

    dataset, config = _dataset(tmp_path, OpenInterestProvider([(_at(0), 1.0, None)]))
    original = Catalog.record_file

    def fail(self, metadata, *, run_id=None):
        original(self, metadata, run_id=run_id)
        raise RuntimeError("catalog fault")

    monkeypatch.setattr(Catalog, "record_file", fail)
    with pytest.raises(SyncError, match="after publishing canonical open-interest"):
        dataset.sync(_at(0), _at(5))
    assert tuple(config.data_dir.rglob("data.parquet"))


def test_uncertain_terminal_commit_uses_visibility_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset, _ = _dataset(tmp_path, OpenInterestProvider([(_at(0), 1.0, None)]))
    original_transaction = Catalog.transaction
    original_record = Catalog.record_ingestion_run
    original_visibility = catalog_storage.terminal_commit_is_visible
    terminal_recorded = False
    visibility_checks = 0

    def mark_terminal(self: Catalog, run: IngestionRunMetadata) -> None:
        nonlocal terminal_recorded
        original_record(self, run)
        terminal_recorded |= run.status == "completed"

    @contextmanager
    def commit_then_report(self: Catalog):
        is_root = not self.connection.in_transaction
        with original_transaction(self):
            yield
        if is_root and terminal_recorded:
            raise _CommitUncertainCatalogError("injected post-commit failure")

    def count_visibility(db_path, run, expected_files):
        nonlocal visibility_checks
        visibility_checks += 1
        return original_visibility(db_path, run, expected_files)

    monkeypatch.setattr(Catalog, "record_ingestion_run", mark_terminal)
    monkeypatch.setattr(Catalog, "transaction", commit_then_report)
    monkeypatch.setattr(catalog_storage, "terminal_commit_is_visible", count_visibility)
    assert dataset.sync(_at(0), _at(5)).require_complete().written_partitions == 1
    assert visibility_checks == 1


def test_ccxt_unified_amount_signed_zero_and_nullable_value(tmp_path: Path) -> None:
    class Exchange:
        id = "binanceusdm"
        has = {"fetchOHLCV": False, "fetchOpenInterestHistory": True}
        markets = None
        precisionMode = 0
        timeframes = {}

        def load_markets(self, reload=False):
            return {
                "BTC/USDT:USDT": {
                    "id": "BTCUSDT",
                    "symbol": "BTC/USDT:USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "settle": "USDT",
                    "swap": True,
                    "linear": True,
                    "inverse": False,
                    "contractSize": "0.001",
                }
            }

        def fetch_open_interest_history(self, symbol, timeframe, since, limit, params):
            assert (symbol, timeframe, limit) == ("BTC/USDT:USDT", "5m", 3)
            assert params == {"until": int(_at(10).timestamp() * 1000) - 1}
            zero = {
                "timestamp": int(_at(0).timestamp() * 1000),
                "openInterestAmount": "-0.0",
                "openInterestValue": "-0.0",
            }
            nullable = {
                "timestamp": int(_at(5).timestamp() * 1000),
                "openInterestAmount": "1000",
                "openInterestValue": "",
            }
            return [zero, dict(zero), nullable] if since == int(_at(0).timestamp() * 1000) else []

    provider = CcxtProvider(
        exchange_factory=lambda _: Exchange(),
        version_provider=lambda: "fixture",
        page_limit=3,
        sleep=lambda _: None,
    )
    dataset, _ = _dataset(tmp_path, provider)
    result = dataset.fetch(_at(0), _at(10))
    assert result.data.get_column("open_interest_amount").to_list() == [0.0, 1000.0]
    assert result.data.get_column("open_interest_value").to_list() == [0.0, None]
    assert math.copysign(1.0, result.data[0, "open_interest_amount"]) == 1.0
    assert math.copysign(1.0, result.data[0, "open_interest_value"]) == 1.0
    assert not result.gaps
    assert result.sources[0].normalizations == ("open_interest.identical_duplicate_dedup",)
    assert result.sources[0].source_field_mapping == (
        "openInterestAmount->open_interest_amount;openInterestValue->open_interest_value"
    )


def test_ccxt_rejects_inverse_ambiguous_and_unsupported_timeframes(tmp_path: Path) -> None:
    class Exchange:
        id = "binanceusdm"
        has = {"fetchOHLCV": False, "fetchOpenInterestHistory": True}
        markets = None
        precisionMode = 0
        timeframes = {}

        def __init__(self, *, linear=True, inverse=False, quanto=False, contract_size="1"):
            self.linear = linear
            self.inverse = inverse
            self.quanto = quanto
            self.contract_size = contract_size

        def load_markets(self, reload=False):
            return {
                "BTC/USDT:USDT": {
                    "id": "BTCUSDT",
                    "symbol": "BTC/USDT:USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "settle": "USDT",
                    "swap": True,
                    "linear": self.linear,
                    "inverse": self.inverse,
                    "quanto": self.quanto,
                    "contractSize": self.contract_size,
                }
            }

    for exchange in (
        Exchange(linear=False, inverse=True),
        Exchange(linear=None, inverse=None),
        Exchange(quanto=True),
        Exchange(contract_size=None),
    ):
        dataset, _ = _dataset(
            tmp_path,
            CcxtProvider(
                exchange_factory=lambda _, exchange=exchange: exchange,
                version_provider=lambda: "fixture",
            ),
        )
        with pytest.raises(UnsupportedMarketError):
            dataset.fetch(_at(0), _at(5))


def test_ccxt_old_start_retention_rejection_remains_missing(tmp_path: Path) -> None:
    class BadRequest(Exception):
        pass

    class Exchange:
        id = "binanceusdm"
        has = {"fetchOHLCV": False, "fetchOpenInterestHistory": True}
        markets = None
        precisionMode = 0
        timeframes = {}

        def load_markets(self, reload=False):
            return {
                "BTC/USDT:USDT": {
                    "id": "BTCUSDT",
                    "symbol": "BTC/USDT:USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "settle": "USDT",
                    "swap": True,
                    "linear": True,
                    "inverse": False,
                    "contractSize": "1",
                }
            }

        def fetch_open_interest_history(self, symbol, timeframe, since, limit, params):
            raise BadRequest("parameter 'startTime' is invalid")

    dataset, _ = _dataset(
        tmp_path,
        CcxtProvider(
            exchange_factory=lambda _: Exchange(),
            version_provider=lambda: "fixture",
            sleep=lambda _: None,
        ),
    )
    result = dataset.fetch(_at(0), _at(10))
    assert result.data.is_empty()
    assert [(gap.status.value, gap.start, gap.end) for gap in result.gaps] == [
        ("missing", _at(0), _at(10))
    ]

    config = MarketDataConfig(state_dir=tmp_path / "s", data_dir=tmp_path / "d")
    dataset = MarketData(
        config=config,
        provider=CcxtProvider(
            exchange_factory=lambda _: Exchange(), version_provider=lambda: "fixture"
        ),
    ).open_interest(
        exchange="binance",
        symbol="BTC/USDT",
        market="perpetual",
        settle="USDT",
        timeframe="1m",
    )
    with pytest.raises(UnsupportedMarketError, match="timeframe"):
        dataset.fetch(_at(0), _at(5))


def test_ccxt_pagination_progress_conflict_bounds_alignment_and_overflow() -> None:
    kwargs = {
        "exchange_id": "binanceusdm",
        "timeframe": "5m",
        "start": _at(0),
        "end": _at(10),
        "page_limit": 1,
    }
    first = {"timestamp": int(_at(0).timestamp() * 1000), "openInterestAmount": "1000"}

    with pytest.raises(ProviderError, match="made no progress"):
        paginate_open_interest_history(fetch_page=lambda *_: [first], **kwargs)

    calls = 0

    def conflict(*_):
        nonlocal calls
        calls += 1
        return [first] if calls == 1 else [{**first, "openInterestAmount": "2000"}]

    with pytest.raises(ProviderError, match="conflicting duplicates"):
        paginate_open_interest_history(fetch_page=conflict, **kwargs)

    bad_records = (
        {**first, "timestamp": int(_at(1).timestamp() * 1000)},
        {**first, "timestamp": int(_at(10).timestamp() * 1000)},
        {**first, "openInterestAmount": "NaN"},
        {**first, "openInterestAmount": "1e10000"},
        {**first, "openInterestValue": "Infinity"},
    )
    for record in bad_records:
        with pytest.raises(ProviderError):
            paginate_open_interest_history(fetch_page=lambda *_, record=record: [record], **kwargs)


def test_runtime_rejects_misaligned_windows_and_unbound_contributors(tmp_path: Path) -> None:
    misaligned, _ = _dataset(
        tmp_path / "misaligned",
        OpenInterestProvider(
            [],
            observed=(ObservedWindow(_at(1), _at(5)),),
        ),
    )
    with pytest.raises(ProviderError, match="observed windows must align"):
        misaligned.fetch(_at(0), _at(5))

    class UnboundContributor(OpenInterestProvider):
        def observe_open_interest(self, request, market):
            del market
            return OpenInterestObservation(
                _provider_frame([(_at(0), 1.0, None)]),
                (ObservedWindow(request.start, request.end),),
                (
                    OpenInterestSourceEvidence(
                        request.start,
                        _at(5),
                        "fixture-route",
                        "fixture-revision",
                        source_rows=1,
                        canonical_rows=1,
                    ),
                ),
            )

    unbound, _ = _dataset(tmp_path / "contributors", UnboundContributor([]))
    with pytest.raises(ProviderError, match="contributors must cover"):
        unbound.fetch(_at(0), _at(10))


def test_oi_canonical_ahead_revision_fails_all_local_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = OpenInterestProvider([(_at(0), 1.0, None), (_at(5), 2.0, None)])
    dataset, _ = _dataset(tmp_path, provider)
    dataset.sync(_at(0), _at(5)).require_complete()
    original = Catalog.record_file

    def fail(self, metadata, *, run_id=None):
        original(self, metadata, run_id=run_id)
        raise RuntimeError("catalog fault")

    monkeypatch.setattr(Catalog, "record_file", fail)
    with pytest.raises(SyncError, match="after publishing canonical open-interest"):
        dataset.sync(_at(5), _at(10))
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan(_at(0), _at(5))
    with pytest.raises(CatalogError, match="physical hash differs"):
        dataset.scan_partial(_at(0), _at(5))
