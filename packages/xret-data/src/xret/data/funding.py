"""Settled public funding dataset and explicit remote/local operations."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from xret.data.config import resolve_config
from xret.data.errors import CatalogError, CoverageError, ProviderError, SyncError
from xret.data.models import (
    CoverageInterval,
    CoverageStatus,
    DataWarning,
    Market,
    MarketIdentity,
    ProviderEvidence,
    SettledFundingKey,
    YearMonth,
)
from xret.data.providers.contracts import FundingRequest, ObservedWindow
from xret.data.providers.discovery import ProviderHandle
from xret.data.providers.funding_runtime import FundingProviderRuntime, ValidatedFundingObservation
from xret.data.storage import catalog as catalog_storage
from xret.data.storage import local_read, locking, paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    Catalog,
    CoverageSegment,
    FileMetadata,
    IngestionRunMetadata,
    _CommitUncertainCatalogError,
)
from xret.data.storage.family_parquet import prepare_family_month, split_family_by_year_month
from xret.data.storage.parquet import (
    ProviderProvenance,
    discard_prepared_file,
    publish_prepared_file,
)
from xret.data.timeframe import parse_time_input, validate_range
from xret.data.warnings import normalized_warnings

if TYPE_CHECKING:
    from xret.data.config import MarketDataConfig


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingFetchResult:
    dataset_key: SettledFundingKey
    data: pl.DataFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    sources: tuple[ProviderEvidence, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps

    def require_complete(self) -> FundingFetchResult:
        if self.gaps:
            raise ProviderError(
                f"settled funding fetch did not fully complete: {len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingSyncResult:
    dataset_key: SettledFundingKey
    run_id: str
    changed: bool
    fetched_rows: int
    written_partitions: int
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    sources: tuple[ProviderEvidence, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps

    def require_complete(self) -> FundingSyncResult:
        if self.gaps:
            raise SyncError(
                f"settled funding sync did not fully complete: {len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingPartialScanResult:
    dataset_key: SettledFundingKey
    data: pl.LazyFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps


def _now() -> datetime:
    return datetime.now(UTC)


def _source(observation: ValidatedFundingObservation) -> ProviderEvidence:
    snapshot = observation.source
    return ProviderEvidence(
        provider_name=snapshot.descriptor.name,
        provider_version=snapshot.descriptor.version,
        provider_api_version=snapshot.descriptor.api_version,
        native_market_id=snapshot.native_market_id,
        native_symbol=snapshot.native_symbol,
        normalizations=snapshot.normalizations,
    )


def _coverage(
    observed: tuple[ObservedWindow, ...], start: datetime, end: datetime
) -> tuple[tuple[CoverageInterval, ...], tuple[CoverageInterval, ...]]:
    segments = [
        CoverageSegment(window.start, window.end, CoverageStatus.AVAILABLE) for window in observed
    ]
    from xret.data.storage.catalog import covered_and_gaps

    return covered_and_gaps(segments, start, end)


def _warnings(gaps: tuple[CoverageInterval, ...]) -> tuple[DataWarning, ...]:
    return normalized_warnings(
        DataWarning(
            "coverage.partial_observation",
            "provider observation did not prove this settled-funding span",
            gap.start,
            gap.end,
        )
        for gap in gaps
    )


def _filter(frame: pl.DataFrame, gaps: tuple[CoverageInterval, ...]) -> pl.DataFrame:
    if frame.is_empty() or not gaps:
        return frame if gaps else frame.head(0)
    expression = pl.lit(False)
    for gap in gaps:
        expression |= (pl.col("effective_at") >= gap.start) & (pl.col("effective_at") < gap.end)
    return frame.filter(expression)


@dataclass(frozen=True, slots=True, kw_only=True)
class SettledFundingDataset:
    """One immutable, I/O-free settled-funding binding."""

    identity: MarketIdentity
    _config: MarketDataConfig | None = field(default=None, init=False, repr=False, compare=False)
    _provider: ProviderHandle | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.identity.market is not Market.PERPETUAL:
            from xret.data.errors import InvalidRequestError

            raise InvalidRequestError("settled funding requires market='perpetual'")

    def _effective_config(self) -> MarketDataConfig:
        return self._config if self._config is not None else resolve_config()

    def _runtime(self) -> FundingProviderRuntime:
        return FundingProviderRuntime(
            (self._provider if self._provider is not None else ProviderHandle(None)).get()
        )

    @staticmethod
    def _range(start: str | datetime, end: str | datetime | None) -> tuple[datetime, datetime]:
        start_dt = parse_time_input(start)
        end_dt = _now() if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        return start_dt, end_dt

    def fetch(self, start: str | datetime, end: str | datetime | None = None) -> FundingFetchResult:
        start_dt, end_dt = self._range(start, end)
        runtime = self._runtime()
        observation = runtime.observe(
            FundingRequest(identity=self.identity, start=start_dt, end=end_dt)
        )
        covered, gaps = _coverage(observation.observed, start_dt, end_dt)
        return FundingFetchResult(
            dataset_key=SettledFundingKey(identity=observation.market.identity),
            data=observation.frame,
            covered=covered,
            gaps=gaps,
            sources=(_source(observation),),
            warnings=_warnings(gaps),
        )

    def _local_key(self) -> SettledFundingKey:
        if self.identity.settle is not None:
            return SettledFundingKey(identity=self.identity)
        config = self._effective_config()
        db_path = config.state_dir / CATALOG_FILE_NAME
        from xret.data.errors import InvalidRequestError

        if not db_path.is_file():
            raise InvalidRequestError("settle is required: no local funding catalog exists")
        with Catalog.open_read_only(db_path) as catalog:
            candidates = {
                key.identity.settle
                for key in catalog.list_datasets()
                if isinstance(key, SettledFundingKey)
                and key.identity.exchange == self.identity.exchange
                and key.identity.symbol == self.identity.symbol
            }
        if len(candidates) != 1:
            raise InvalidRequestError(
                f"settle is required: {len(candidates)} local funding settlements found"
            )
        return SettledFundingKey(identity=replace(self.identity, settle=next(iter(candidates))))

    def sync(self, start: str | datetime, end: str | datetime | None = None) -> FundingSyncResult:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        runtime: FundingProviderRuntime | None = None
        resolved = self.identity
        if resolved.settle is None:
            runtime = self._runtime()
            resolved = runtime.resolve_market(resolved).identity
        key = SettledFundingKey(identity=resolved)
        run_id = uuid.uuid4().hex
        db_path = config.state_dir / CATALOG_FILE_NAME
        started = _now()
        running = IngestionRunMetadata(
            run_id,
            key,
            start_dt,
            end_dt,
            started,
            1,
            status="running",
        )
        observations: list[tuple[CoverageInterval, ValidatedFundingObservation, pl.DataFrame]] = []
        prepared = []
        with locking.dataset_lock(config.state_dir, key):
            with locking.catalog_gate(config.state_dir):
                if (
                    not db_path.exists()
                    and paths.classify_managed_storage(config.data_dir) != "empty"
                ):
                    raise CatalogError(
                        "catalog is absent while managed storage evidence exists; rebuild required"
                    )
                with Catalog.open(db_path) as catalog:
                    _covered, gaps = catalog.coverage_and_gaps(key, start_dt, end_dt)
                    missing = tuple(gap for gap in gaps if gap.status is CoverageStatus.MISSING)
                    lineage = catalog.get_source_lineage(key)
                    catalog.record_ingestion_run(running)
            try:
                if missing:
                    runtime = runtime or self._runtime()
                    if lineage is not None and lineage != runtime.descriptor.name:
                        raise CatalogError(
                            "dataset source lineage is "
                            f"{lineage!r}, not {runtime.descriptor.name!r}"
                        )
                    market = runtime.resolve_market(resolved)
                    for gap in missing:
                        observation = runtime.observe(
                            FundingRequest(identity=resolved, start=gap.start, end=gap.end),
                            market=market,
                        )
                        observations.append((gap, observation, _filter(observation.frame, (gap,))))
                monthly: dict[YearMonth, list[pl.DataFrame]] = {}
                month_observation: dict[YearMonth, ValidatedFundingObservation] = {}
                for _gap, observation, rows in observations:
                    for month, frame in split_family_by_year_month(rows, timestamp="effective_at"):
                        monthly.setdefault(month, []).append(frame)
                        month_observation.setdefault(month, observation)
                for month, frames in monthly.items():
                    observation = month_observation[month]
                    prepared.append(
                        prepare_family_month(
                            config.data_dir,
                            key,
                            month,
                            pl.concat(frames, how="vertical").sort("effective_at"),
                            provider=ProviderProvenance(
                                observation.source.descriptor.name,
                                observation.source.descriptor.version,
                                observation.source.descriptor.api_version,
                                observation.source.native_market_id,
                                observation.source.native_symbol,
                            ),
                        )
                    )
            except Exception as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                self._fail_run(db_path, config, running, exc)
                raise
            terminal: IngestionRunMetadata | None = None
            try:
                with locking.catalog_gate(config.state_dir), Catalog.open(db_path) as catalog:
                    catalog.record_ingestion_run(running)
                    for artifact in prepared:
                        publish_prepared_file(artifact)
                    segments = [
                        CoverageSegment(window.start, window.end, CoverageStatus.AVAILABLE)
                        for _gap, observation, _rows in observations
                        for window in observation.observed
                    ]
                    terminal = replace(
                        running,
                        status="completed",
                        provider_name=(
                            observations[0][1].source.descriptor.name if observations else None
                        ),
                        provider_version=(
                            observations[0][1].source.descriptor.version if observations else None
                        ),
                        provider_api_version=(
                            observations[0][1].source.descriptor.api_version
                            if observations
                            else None
                        ),
                        provider_market_id=(
                            observations[0][1].source.native_market_id if observations else None
                        ),
                        native_symbol=(
                            observations[0][1].source.native_symbol if observations else None
                        ),
                        completed_at=_now(),
                        row_count=sum(item[1].frame.height for item in observations),
                    )
                    with catalog.transaction():
                        if observations and (segments or prepared):
                            catalog.bind_source_lineage(
                                key, observations[0][1].source.descriptor.name
                            )
                        if segments:
                            catalog.apply_coverage_batch(key, segments)
                        for artifact in prepared:
                            committed = artifact.committed_file
                            catalog.record_file(
                                FileMetadata(
                                    key,
                                    committed.relative_path,
                                    committed.year_month.year,
                                    committed.year_month.month,
                                    committed.row_count,
                                    committed.min_timestamp,
                                    committed.max_timestamp,
                                    committed.physical_hash,
                                    committed.schema_version,
                                ),
                                run_id=run_id,
                            )
                        catalog.record_ingestion_run(terminal)
                        covered, gaps = catalog.coverage_and_gaps(key, start_dt, end_dt)
            except CatalogError as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                if (
                    isinstance(exc, _CommitUncertainCatalogError)
                    and terminal is not None
                    and catalog_storage.terminal_commit_is_visible(
                        db_path,
                        terminal,
                        tuple(
                            (
                                artifact.committed_file.relative_path,
                                artifact.committed_file.physical_hash,
                            )
                            for artifact in prepared
                        ),
                    )
                ):
                    facts = local_read.read_local_facts_for_key(
                        config.state_dir,
                        config.data_dir,
                        key,
                        start_dt,
                        end_dt,
                    )
                    covered, gaps = facts.covered, facts.gaps
                elif any(artifact.published for artifact in prepared):
                    raise SyncError(
                        "catalog update failed after publishing canonical funding Parquet; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                else:
                    if not isinstance(exc, _CommitUncertainCatalogError):
                        self._fail_run(db_path, config, running, exc)
                    raise
            except Exception as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                if any(artifact.published for artifact in prepared):
                    raise SyncError(
                        "catalog update failed after publishing canonical funding Parquet; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                self._fail_run(db_path, config, running, exc)
                raise
        sources = tuple(dict.fromkeys(_source(item[1]) for item in observations))
        return FundingSyncResult(
            dataset_key=key,
            run_id=run_id,
            changed=bool(prepared)
            or any(observation.observed for _gap, observation, _rows in observations),
            fetched_rows=sum(item[1].frame.height for item in observations),
            written_partitions=len(prepared),
            covered=covered,
            gaps=gaps,
            sources=sources,
            warnings=_warnings(gaps),
        )

    @staticmethod
    def _fail_run(
        db_path: Path,
        config: MarketDataConfig,
        running: IngestionRunMetadata,
        original: Exception,
    ) -> None:
        try:
            with locking.catalog_gate(config.state_dir), Catalog.open(db_path) as catalog:
                catalog.record_ingestion_run(replace(running, status="failed", completed_at=_now()))
        except Exception as failure:
            original.add_note(f"also failed to record funding run failure: {failure}")

    def scan(self, start: str | datetime, end: str | datetime | None = None) -> pl.LazyFrame:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        key = self._local_key()
        facts = local_read.read_local_facts_for_key(
            config.state_dir, config.data_dir, key, start_dt, end_dt
        )
        if facts.gaps:
            raise CoverageError(
                f"scan requires complete local settled-funding coverage; "
                f"{len(facts.gaps)} gap(s) remain"
            )
        return local_read.lazy_frame_for_facts(config.data_dir, facts)

    def scan_partial(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> FundingPartialScanResult:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        key = (
            self._local_key()
            if self.identity.settle is None
            else SettledFundingKey(identity=self.identity)
        )
        facts = local_read.read_local_facts_for_key(
            config.state_dir, config.data_dir, key, start_dt, end_dt
        )
        return FundingPartialScanResult(
            dataset_key=key,
            data=local_read.lazy_frame_for_facts(config.data_dir, facts),
            covered=facts.covered,
            gaps=facts.gaps,
            warnings=tuple(
                DataWarning(
                    f"coverage.{gap.status.value}",
                    f"{gap.status.value} gap in local settled-funding coverage",
                    gap.start,
                    gap.end,
                )
                for gap in facts.gaps
            ),
        )
