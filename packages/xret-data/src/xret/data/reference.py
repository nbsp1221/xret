"""Reference-price bar dataset and explicit remote/local operations."""

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
    ReferenceBarKey,
    ReferencePriceKind,
    YearMonth,
)
from xret.data.observation_coverage import evaluate_observation_coverage
from xret.data.providers import runtime as provider_runtime
from xret.data.providers.contracts import ReferenceBarRequest
from xret.data.providers.discovery import ProviderHandle
from xret.data.providers.reference_runtime import (
    ReferenceBarProviderRuntime,
    ValidatedReferenceBarObservation,
)
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
from xret.data.timeframe import TimeBar, parse_time_input, validate_range
from xret.data.warnings import normalized_warnings

if TYPE_CHECKING:
    from xret.data.config import MarketDataConfig


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferenceBarFetchResult:
    dataset_key: ReferenceBarKey
    data: pl.DataFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    sources: tuple[ProviderEvidence, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps

    def require_complete(self) -> ReferenceBarFetchResult:
        if self.gaps:
            raise ProviderError(
                f"reference bars fetch did not fully complete: {len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferenceBarSyncResult:
    dataset_key: ReferenceBarKey
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

    def require_complete(self) -> ReferenceBarSyncResult:
        if self.gaps:
            raise SyncError(
                f"reference bars sync did not fully complete: {len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferenceBarPartialScanResult:
    dataset_key: ReferenceBarKey
    data: pl.LazyFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps


def _now() -> datetime:
    return datetime.now(UTC)


def _source(observation: ValidatedReferenceBarObservation) -> ProviderEvidence:
    snapshot = observation.source
    return ProviderEvidence(
        provider_name=snapshot.descriptor.name,
        provider_version=snapshot.descriptor.version,
        provider_api_version=snapshot.descriptor.api_version,
        native_market_id=snapshot.native_market_id,
        native_symbol=snapshot.native_symbol,
        normalizations=snapshot.normalizations,
        reference_target_scope=observation.market.reference_target_scope,
    )


def _coverage(
    observation: ValidatedReferenceBarObservation,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> tuple[tuple[CoverageInterval, ...], tuple[CoverageInterval, ...]]:
    time_bar = TimeBar.parse(timeframe)
    finalizable_end = min(
        end, time_bar.floor(observation.evidence_at - provider_runtime.DEFAULT_FINALITY_GRACE)
    )
    coverage = evaluate_observation_coverage(
        time_bar=time_bar,
        start=start,
        end=end,
        finalizable_end=finalizable_end,
        timestamps=observation.frame.get_column("timestamp").to_list(),
        observed=observation.observed,
    )
    return coverage.covered, coverage.gaps


def _segments(
    observation: ValidatedReferenceBarObservation,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> list[CoverageSegment]:
    covered, gaps = _coverage(observation, start, end, timeframe)
    intervals = (
        *covered,
        *(gap for gap in gaps if gap.status is not CoverageStatus.MISSING),
    )
    return [CoverageSegment(item.start, item.end, item.status) for item in intervals]


def _warnings(gaps: tuple[CoverageInterval, ...]) -> tuple[DataWarning, ...]:
    return normalized_warnings(
        DataWarning(
            "coverage.partial_observation",
            "provider observation did not prove this reference-bar span",
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
        expression |= (pl.col("timestamp") >= gap.start) & (pl.col("timestamp") < gap.end)
    return frame.filter(expression)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReferenceBarDataset:
    """One immutable, I/O-free reference-bar binding."""

    identity: MarketIdentity
    kind: ReferencePriceKind
    timeframe: str
    _config: MarketDataConfig | None = field(default=None, init=False, repr=False, compare=False)
    _provider: ProviderHandle | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.identity.market is not Market.PERPETUAL:
            from xret.data.errors import InvalidRequestError

            raise InvalidRequestError("reference bars require market='perpetual'")
        try:
            object.__setattr__(self, "kind", ReferencePriceKind(self.kind))
        except (TypeError, ValueError) as exc:
            from xret.data.errors import InvalidRequestError

            raise InvalidRequestError(f"unrecognized reference price kind: {self.kind!r}") from exc
        TimeBar.parse(self.timeframe)

    def _effective_config(self) -> MarketDataConfig:
        return self._config if self._config is not None else resolve_config()

    def _runtime(self) -> ReferenceBarProviderRuntime:
        return ReferenceBarProviderRuntime(
            (self._provider if self._provider is not None else ProviderHandle(None)).get()
        )

    def _range(
        self, start: str | datetime, end: str | datetime | None
    ) -> tuple[datetime, datetime]:
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = provider_runtime.default_end(time_bar) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        if time_bar.floor(start_dt) != start_dt or time_bar.floor(end_dt) != end_dt:
            from xret.data.errors import InvalidRequestError

            raise InvalidRequestError(
                f"reference bar ranges must align to {self.timeframe}: "
                f"[{start_dt.isoformat()}, {end_dt.isoformat()})"
            )
        return start_dt, end_dt

    def _request(
        self, identity: MarketIdentity, start: datetime, end: datetime
    ) -> ReferenceBarRequest:
        return ReferenceBarRequest(
            identity=identity,
            kind=self.kind,
            timeframe=self.timeframe,
            start=start,
            end=end,
        )

    def fetch(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> ReferenceBarFetchResult:
        start_dt, end_dt = self._range(start, end)
        runtime = self._runtime()
        observation = runtime.observe(self._request(self.identity, start_dt, end_dt))
        covered, gaps = _coverage(observation, start_dt, end_dt, self.timeframe)
        return ReferenceBarFetchResult(
            dataset_key=ReferenceBarKey(
                identity=observation.market.identity,
                kind=self.kind,
                timeframe=self.timeframe,
            ),
            data=observation.frame,
            covered=covered,
            gaps=gaps,
            sources=(_source(observation),),
            warnings=_warnings(gaps),
        )

    def _local_key(self) -> ReferenceBarKey:
        if self.identity.settle is not None:
            return ReferenceBarKey(identity=self.identity, kind=self.kind, timeframe=self.timeframe)
        config = self._effective_config()
        db_path = config.state_dir / CATALOG_FILE_NAME
        from xret.data.errors import InvalidRequestError

        if not db_path.is_file():
            raise InvalidRequestError("settle is required: no local reference catalog exists")
        with Catalog.open_read_only(db_path) as catalog:
            candidates = {
                key.identity.settle
                for key in catalog.list_datasets()
                if isinstance(key, ReferenceBarKey)
                and key.identity.exchange == self.identity.exchange
                and key.identity.symbol == self.identity.symbol
                and key.kind is self.kind
                and key.timeframe == self.timeframe
            }
        if len(candidates) != 1:
            raise InvalidRequestError(
                f"settle is required: {len(candidates)} local reference settlements found"
            )
        return ReferenceBarKey(
            identity=replace(self.identity, settle=next(iter(candidates))),
            kind=self.kind,
            timeframe=self.timeframe,
        )

    def sync(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> ReferenceBarSyncResult:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        runtime: ReferenceBarProviderRuntime | None = None
        resolved = self.identity
        request = self._request(resolved, start_dt, end_dt)
        if resolved.settle is None:
            runtime = self._runtime()
            market = runtime.resolve_market(request)
            resolved = market.identity
            request = self._request(resolved, start_dt, end_dt)
        key = ReferenceBarKey(identity=resolved, kind=self.kind, timeframe=self.timeframe)
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
        observations: list[
            tuple[CoverageInterval, ValidatedReferenceBarObservation, pl.DataFrame]
        ] = []
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
                    market = runtime.resolve_market(request)
                    for gap in missing:
                        observation = runtime.observe(
                            self._request(resolved, gap.start, gap.end),
                            market=market,
                        )
                        observations.append((gap, observation, _filter(observation.frame, (gap,))))
                monthly: dict[YearMonth, list[pl.DataFrame]] = {}
                month_observation: dict[YearMonth, ValidatedReferenceBarObservation] = {}
                for _gap, observation, rows in observations:
                    for month, frame in split_family_by_year_month(rows, timestamp="timestamp"):
                        monthly.setdefault(month, []).append(frame)
                        month_observation.setdefault(month, observation)
                for month, frames in monthly.items():
                    observation = month_observation[month]
                    prepared.append(
                        prepare_family_month(
                            config.data_dir,
                            key,
                            month,
                            pl.concat(frames, how="vertical").sort("timestamp"),
                            provider=ProviderProvenance(
                                observation.source.descriptor.name,
                                observation.source.descriptor.version,
                                observation.source.descriptor.api_version,
                                observation.source.native_market_id,
                                observation.source.native_symbol,
                                observation.market.reference_target_scope,
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
                        segment
                        for requested_gap, observation, _rows in observations
                        for segment in _segments(
                            observation,
                            requested_gap.start,
                            requested_gap.end,
                            self.timeframe,
                        )
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
                        "catalog update failed after publishing canonical reference Parquet; "
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
                        "catalog update failed after publishing canonical reference Parquet; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                self._fail_run(db_path, config, running, exc)
                raise
        sources = tuple(dict.fromkeys(_source(item[1]) for item in observations))
        return ReferenceBarSyncResult(
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
            original.add_note(f"also failed to record reference run failure: {failure}")

    def scan(self, start: str | datetime, end: str | datetime | None = None) -> pl.LazyFrame:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        key = self._local_key()
        facts = local_read.read_local_facts_for_key(
            config.state_dir, config.data_dir, key, start_dt, end_dt
        )
        if facts.gaps:
            raise CoverageError(
                f"scan requires complete local reference-bar coverage; "
                f"{len(facts.gaps)} gap(s) remain"
            )
        return local_read.lazy_frame_for_facts(config.data_dir, facts)

    def scan_partial(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> ReferenceBarPartialScanResult:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        key = (
            self._local_key()
            if self.identity.settle is None
            else ReferenceBarKey(identity=self.identity, kind=self.kind, timeframe=self.timeframe)
        )
        facts = local_read.read_local_facts_for_key(
            config.state_dir, config.data_dir, key, start_dt, end_dt
        )
        return ReferenceBarPartialScanResult(
            dataset_key=key,
            data=local_read.lazy_frame_for_facts(config.data_dir, facts),
            covered=facts.covered,
            gaps=facts.gaps,
            warnings=tuple(
                DataWarning(
                    f"coverage.{gap.status.value}",
                    f"{gap.status.value} gap in local reference-bar coverage",
                    gap.start,
                    gap.end,
                )
                for gap in facts.gaps
            ),
        )
