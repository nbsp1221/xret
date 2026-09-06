"""Historical open-interest dataset and explicit remote/local operations."""

from __future__ import annotations

import struct
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
    OpenInterestKey,
    ProviderEvidence,
    YearMonth,
)
from xret.data.observation_coverage import evaluate_observation_coverage
from xret.data.providers.contracts import OpenInterestRequest, OpenInterestSyncPolicy
from xret.data.providers.discovery import ProviderHandle
from xret.data.providers.oi_runtime import (
    OpenInterestProviderRuntime,
    ValidatedOpenInterestObservation,
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
from xret.data.storage.family_parquet import (
    prepare_family_month,
    prepare_open_interest_revision_month,
    split_family_by_year_month,
)
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
class OpenInterestFetchResult:
    dataset_key: OpenInterestKey
    data: pl.DataFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    sources: tuple[ProviderEvidence, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps

    def require_complete(self) -> OpenInterestFetchResult:
        if self.gaps:
            raise ProviderError(
                f"open interest fetch did not fully complete: {len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenInterestSyncResult:
    dataset_key: OpenInterestKey
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

    def require_complete(self) -> OpenInterestSyncResult:
        if self.gaps:
            raise SyncError(
                f"open interest sync did not fully complete: {len(self.gaps)} gap(s) remain"
            )
        return self


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenInterestPartialScanResult:
    dataset_key: OpenInterestKey
    data: pl.LazyFrame
    covered: tuple[CoverageInterval, ...]
    gaps: tuple[CoverageInterval, ...] = ()
    warnings: tuple[DataWarning, ...] = ()

    @property
    def is_complete(self) -> bool:
        return not self.gaps


def _now() -> datetime:
    return datetime.now(UTC)


def _source(observation: ValidatedOpenInterestObservation) -> ProviderEvidence:
    snapshot = observation.source
    return ProviderEvidence(
        provider_name=snapshot.descriptor.name,
        provider_version=snapshot.descriptor.version,
        provider_api_version=snapshot.descriptor.api_version,
        native_market_id=snapshot.native_market_id,
        native_symbol=snapshot.native_symbol,
        normalizations=snapshot.normalizations,
        derivative_linear=observation.market.derivative.linear,
        derivative_inverse=observation.market.derivative.inverse,
        contract_size=observation.market.derivative.contract_size,
        source_field_mapping=observation.source_field_mapping,
    )


def _sources(observation: ValidatedOpenInterestObservation) -> tuple[ProviderEvidence, ...]:
    base = _source(observation)
    return tuple(
        replace(
            base,
            normalizations=contributor.normalizations or base.normalizations,
            contributed_start=contributor.start,
            contributed_end=contributor.end,
            source_route=contributor.source_route,
            object_key=contributor.object_key or None,
            checksum_algorithm=contributor.checksum_algorithm or None,
            checksum_value=contributor.checksum_value or None,
            source_revision=contributor.source_revision,
            retrieved_at=contributor.retrieved_at,
            source_rows=contributor.source_rows,
            canonical_rows=contributor.canonical_rows,
            duplicate_rows=contributor.duplicate_rows,
        )
        for contributor in observation.contributors
    )


def _month_source(
    source: ProviderEvidence,
    frame: pl.DataFrame,
    start: datetime,
    end: datetime,
) -> ProviderEvidence:
    assert source.contributed_start is not None
    assert source.contributed_end is not None
    if start == source.contributed_start and end == source.contributed_end:
        return source
    if source.duplicate_rows:
        raise ProviderError(
            "cross-month OI source duplicate rows cannot be attributed to monthly artifacts"
        )
    canonical_rows = frame.filter(
        (pl.col("timestamp") >= start) & (pl.col("timestamp") < end)
    ).height
    return replace(
        source,
        contributed_start=start,
        contributed_end=end,
        source_rows=canonical_rows,
        canonical_rows=canonical_rows,
        duplicate_rows=0,
    )


def _coverage(
    observation: ValidatedOpenInterestObservation,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> tuple[tuple[CoverageInterval, ...], tuple[CoverageInterval, ...]]:
    time_bar = TimeBar.parse(timeframe)
    coverage = evaluate_observation_coverage(
        time_bar=time_bar,
        start=start,
        end=end,
        finalizable_end=end,
        timestamps=observation.frame.get_column("timestamp").to_list(),
        observed=observation.observed,
    )
    return coverage.covered, coverage.gaps


def _segments(
    observation: ValidatedOpenInterestObservation,
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


def _warnings(
    gaps: tuple[CoverageInterval, ...], *, sync_policy: OpenInterestSyncPolicy
) -> tuple[DataWarning, ...]:
    revisioned_archive = sync_policy is OpenInterestSyncPolicy.REVISIONED_ARCHIVE
    code = "source.publication_lag" if revisioned_archive else "coverage.partial_observation"
    message = (
        "revisioned archive object is absent or does not certify this open-interest span"
        if revisioned_archive
        else "provider observation did not prove this open-interest span"
    )
    return normalized_warnings(DataWarning(code, message, gap.start, gap.end) for gap in gaps)


def _filter(frame: pl.DataFrame, gaps: tuple[CoverageInterval, ...]) -> pl.DataFrame:
    if frame.is_empty() or not gaps:
        return frame if gaps else frame.head(0)
    expression = pl.lit(False)
    for gap in gaps:
        expression |= (pl.col("timestamp") >= gap.start) & (pl.col("timestamp") < gap.end)
    return frame.filter(expression)


def _qualify_fetch_overlap(left: OpenInterestFetchResult, right: OpenInterestFetchResult) -> int:
    """Compare same-key fetched rows bitwise; never selects or publishes a winner."""
    if left.dataset_key != right.dataset_key:
        raise ProviderError("OI qualification requires the same canonical dataset")
    columns = ("open_interest_amount", "open_interest_value")
    left_rows = {row["timestamp"]: row for row in left.data.rows(named=True)}
    right_rows = {row["timestamp"]: row for row in right.data.rows(named=True)}
    overlap = sorted(left_rows.keys() & right_rows.keys())
    for timestamp in overlap:
        for column in columns:
            first = left_rows[timestamp][column]
            second = right_rows[timestamp][column]
            if first is None or second is None:
                equal = first is second
            else:
                first_value = 0.0 if first == 0.0 else first
                second_value = 0.0 if second == 0.0 else second
                equal = struct.pack("!d", first_value) == struct.pack("!d", second_value)
            if not equal:
                raise ProviderError(
                    "cross-provider OI qualification conflict at "
                    f"{timestamp.isoformat()} for {column}"
                )
    return len(overlap)


@dataclass(frozen=True, slots=True, kw_only=True)
class OpenInterestDataset:
    """One immutable, I/O-free open-interest binding."""

    identity: MarketIdentity
    timeframe: str
    _config: MarketDataConfig | None = field(default=None, init=False, repr=False, compare=False)
    _provider: ProviderHandle | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.identity.market is not Market.PERPETUAL:
            from xret.data.errors import InvalidRequestError

            raise InvalidRequestError("open interest requires market='perpetual'")
        TimeBar.parse(self.timeframe)

    def _effective_config(self) -> MarketDataConfig:
        return self._config if self._config is not None else resolve_config()

    def _runtime(self) -> OpenInterestProviderRuntime:
        return OpenInterestProviderRuntime(
            (self._provider if self._provider is not None else ProviderHandle(None)).get()
        )

    def _range(
        self, start: str | datetime, end: str | datetime | None
    ) -> tuple[datetime, datetime]:
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = time_bar.floor(_now()) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        if time_bar.floor(start_dt) != start_dt or time_bar.floor(end_dt) != end_dt:
            from xret.data.errors import InvalidRequestError

            raise InvalidRequestError(
                f"open interest ranges must align to {self.timeframe}: "
                f"[{start_dt.isoformat()}, {end_dt.isoformat()})"
            )
        return start_dt, end_dt

    def _request(
        self, identity: MarketIdentity, start: datetime, end: datetime
    ) -> OpenInterestRequest:
        return OpenInterestRequest(
            identity=identity,
            timeframe=self.timeframe,
            start=start,
            end=end,
        )

    def fetch(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> OpenInterestFetchResult:
        start_dt, end_dt = self._range(start, end)
        runtime = self._runtime()
        observation = runtime.observe(self._request(self.identity, start_dt, end_dt))
        covered, gaps = _coverage(observation, start_dt, end_dt, self.timeframe)
        return OpenInterestFetchResult(
            dataset_key=OpenInterestKey(
                identity=observation.market.identity,
                timeframe=self.timeframe,
            ),
            data=observation.frame,
            covered=covered,
            gaps=gaps,
            sources=_sources(observation),
            warnings=_warnings(gaps, sync_policy=observation.market.sync_policy),
        )

    def _local_key(self) -> OpenInterestKey:
        if self.identity.settle is not None:
            return OpenInterestKey(identity=self.identity, timeframe=self.timeframe)
        config = self._effective_config()
        db_path = config.state_dir / CATALOG_FILE_NAME
        from xret.data.errors import InvalidRequestError

        if not db_path.is_file():
            raise InvalidRequestError("settle is required: no local open-interest catalog exists")
        with Catalog.open_read_only(db_path) as catalog:
            candidates = {
                key.identity.settle
                for key in catalog.list_datasets()
                if isinstance(key, OpenInterestKey)
                and key.identity.exchange == self.identity.exchange
                and key.identity.symbol == self.identity.symbol
                and key.timeframe == self.timeframe
            }
        if len(candidates) != 1:
            raise InvalidRequestError(
                f"settle is required: {len(candidates)} local open-interest settlements found"
            )
        return OpenInterestKey(
            identity=replace(self.identity, settle=next(iter(candidates))),
            timeframe=self.timeframe,
        )

    def sync(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> OpenInterestSyncResult:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        runtime: OpenInterestProviderRuntime | None = None
        resolved = self.identity
        if resolved.settle is None:
            runtime = self._runtime()
            market = runtime.resolve_market(resolved)
            resolved = market.identity
        key = OpenInterestKey(identity=resolved, timeframe=self.timeframe)
        runtime = runtime or self._runtime()
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
            tuple[CoverageInterval, ValidatedOpenInterestObservation, pl.DataFrame]
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
                    catalog.validate_provider_request(
                        key, start_dt, end_dt, runtime.descriptor.name
                    )
                    _covered, gaps = catalog.coverage_and_gaps(key, start_dt, end_dt)
                    missing = tuple(gap for gap in gaps if gap.status is CoverageStatus.MISSING)
                    catalog.record_ingestion_run(running)
            try:
                runtime = runtime or self._runtime()
                if runtime.sync_policy is OpenInterestSyncPolicy.REVISIONED_ARCHIVE:
                    missing = (CoverageInterval(start_dt, end_dt, CoverageStatus.MISSING),)
                if missing:
                    market = runtime.resolve_market(resolved)
                    for gap in missing:
                        observation = runtime.observe(
                            self._request(resolved, gap.start, gap.end),
                            market=market,
                        )
                        observations.append((gap, observation, _filter(observation.frame, (gap,))))
                monthly: dict[YearMonth, list[pl.DataFrame]] = {}
                month_observation: dict[YearMonth, ValidatedOpenInterestObservation] = {}
                month_contributors: dict[YearMonth, list[ProviderEvidence]] = {}
                for _gap, observation, rows in observations:
                    for month, frame in split_family_by_year_month(rows, timestamp="timestamp"):
                        monthly.setdefault(month, []).append(frame)
                        month_observation.setdefault(month, observation)
                    for source in _sources(observation):
                        assert source.contributed_start is not None
                        assert source.contributed_end is not None
                        for month, month_start, month_end in paths.iter_month_slices(
                            source.contributed_start, source.contributed_end
                        ):
                            monthly.setdefault(month, [])
                            month_observation.setdefault(month, observation)
                            clipped = _month_source(source, rows, month_start, month_end)
                            if clipped not in month_contributors.setdefault(month, []):
                                month_contributors[month].append(clipped)
                for month, frames in monthly.items():
                    observation = month_observation[month]
                    batch = (
                        pl.concat(frames, how="vertical").sort("timestamp")
                        if frames
                        else observation.frame.head(0)
                    )
                    contributors = tuple(
                        sorted(
                            month_contributors[month],
                            key=lambda item: item.contributed_start,
                        )
                    )
                    provenance = ProviderProvenance(
                        observation.source.descriptor.name,
                        observation.source.descriptor.version,
                        observation.source.descriptor.api_version,
                        observation.source.native_market_id,
                        observation.source.native_symbol,
                        derivative_linear=observation.market.derivative.linear,
                        derivative_inverse=observation.market.derivative.inverse,
                        contract_size=observation.market.derivative.contract_size or "",
                        source_field_mapping=observation.source_field_mapping or "",
                    )
                    if observation.market.sync_policy is OpenInterestSyncPolicy.REVISIONED_ARCHIVE:
                        change = prepare_open_interest_revision_month(
                            config.data_dir,
                            key,
                            month,
                            batch,
                            provider=provenance,
                            contributors=contributors,
                        )
                    elif batch.is_empty():
                        change = None
                    else:
                        change = prepare_family_month(
                            config.data_dir,
                            key,
                            month,
                            batch,
                            provider=provenance,
                            contributors=contributors,
                        )
                    if change is not None:
                        prepared.append(change)
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
                        catalog.validate_source_ownership(key, artifact.committed_file.contributors)
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
                        if (
                            observations
                            and observations[0][1].market.sync_policy
                            is OpenInterestSyncPolicy.REVISIONED_ARCHIVE
                        ):
                            for _requested, observation, _rows in observations:
                                for contributor in observation.contributors:
                                    replacements = tuple(
                                        CoverageSegment(
                                            max(segment.start, contributor.start),
                                            min(segment.end, contributor.end),
                                            segment.status,
                                        )
                                        for segment in segments
                                        if segment.start < contributor.end
                                        and segment.end > contributor.start
                                    )
                                    catalog.replace_coverage_range(
                                        key,
                                        contributor.start,
                                        contributor.end,
                                        replacements,
                                    )
                        elif segments:
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
                                    committed.contributors,
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
                        "catalog update failed after publishing canonical open-interest Parquet; "
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
                        "catalog update failed after publishing canonical open-interest Parquet; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                self._fail_run(db_path, config, running, exc)
                raise
        sources = tuple(
            dict.fromkeys(
                source
                for _gap, observation, _rows in observations
                for source in _sources(observation)
            )
        )
        return OpenInterestSyncResult(
            dataset_key=key,
            run_id=run_id,
            changed=bool(prepared)
            or any(observation.observed for _gap, observation, _rows in observations),
            fetched_rows=sum(item[1].frame.height for item in observations),
            written_partitions=len(prepared),
            covered=covered,
            gaps=gaps,
            sources=sources,
            warnings=_warnings(gaps, sync_policy=runtime.sync_policy),
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
            original.add_note(f"also failed to record open-interest run failure: {failure}")

    def scan(self, start: str | datetime, end: str | datetime | None = None) -> pl.LazyFrame:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        key = self._local_key()
        facts = local_read.read_local_facts_for_key(
            config.state_dir, config.data_dir, key, start_dt, end_dt
        )
        if facts.gaps:
            raise CoverageError(
                f"scan requires complete local open-interest coverage; "
                f"{len(facts.gaps)} gap(s) remain"
            )
        return local_read.lazy_frame_for_facts(config.data_dir, facts)

    def scan_partial(
        self, start: str | datetime, end: str | datetime | None = None
    ) -> OpenInterestPartialScanResult:
        start_dt, end_dt = self._range(start, end)
        config = self._effective_config()
        key = (
            self._local_key()
            if self.identity.settle is None
            else OpenInterestKey(identity=self.identity, timeframe=self.timeframe)
        )
        facts = local_read.read_local_facts_for_key(
            config.state_dir, config.data_dir, key, start_dt, end_dt
        )
        return OpenInterestPartialScanResult(
            dataset_key=key,
            data=local_read.lazy_frame_for_facts(config.data_dir, facts),
            covered=facts.covered,
            gaps=facts.gaps,
            warnings=tuple(
                DataWarning(
                    f"coverage.{gap.status.value}",
                    f"{gap.status.value} gap in local open-interest coverage",
                    gap.start,
                    gap.end,
                )
                for gap in facts.gaps
            ),
        )
