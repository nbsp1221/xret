# Data lifecycle

Xret separates remote observation, canonical mutation, and local analysis for every historical dataset family. Trade bars, settled funding, reference bars, and open interest share explicit I/O boundaries without sharing a falsely generic schema or completeness model.

## Dataset identity

Every family includes provider-independent exchange, `BASE/QUOTE` symbol, perpetual settlement where applicable, and a closed family discriminator. Trade bars add timeframe; settled funding adds no timeframe because events are irregular; reference bars add `kind` (`mark`, `index`, or `premium_index`) and timeframe; open interest adds sampling timeframe. Xret operates spot and perpetual trade bars, while the three derivatives families require perpetual markets.

For remote operations, an omitted perpetual settlement is inferred only when provider metadata has exactly one nonempty settlement value and exactly one listed market matching the base, quote, and settlement. Local reads infer it only from exactly one locally known candidate.

## Remote observation: `fetch`

`fetch` always invokes the selected provider and returns a family-specific result containing an eager Polars `DataFrame`, coverage, gaps, provider evidence, and warnings. It does not read or change canonical local state. Trade bars require an explicit `BarFetchMode`: `LATEST` preserves the provider's current observation, including forming or recently closed rows when returned, while `FINAL` applies Xret's finality grace. A `MarketData(provider=...)` selection is explicit for the whole operation; Xret never chains providers or falls back to CCXT.

Returned rows and observation evidence are separate facts. Latest trade-bar rows may still change; final trade and reference bars use completed aligned intervals, OI uses provider-labeled sample timestamps, and funding uses exact final event timestamps inside arbitrary observed spans. Valid rows prove only their own facts unless the provider supplies stronger exhaustive `ObservedWindow` evidence. Every unproved remainder remains `missing`; qualification is maintained outside this runtime lifecycle as historical QA evidence.

## Canonical reconciliation: `sync`

`sync` reads local coverage, fetches only implicit `missing` intervals except when a revision-aware archive must re-observe its owned range, validates received values and evidence, and publishes each canonical monthly Parquet file by atomic replacement. A fully covered sync still records ingestion-run provenance, while its canonical data result is a visible no-op.

Coverage records facts. Persisted trade, reference, and OI rows establish `available` timestamps or intervals; funding availability comes from provider-observed spans rather than inferred event spacing. `unavailable` is recorded only from successful exhaustive negative evidence. Provider, validation, incomplete traversal, or storage failures never create negative coverage.

Ordinary datasets bind one stable provider-name lineage. OI may instead contain ordered, non-overlapping provider-owned contributor ranges so qualified deep archive and recent-history sources can coexist without overlap, fallback, or row-level winner selection.

## Transient live observation

A live `BarUpdate` is an in-memory observation and carries explicit time-based finality. `FORMING` is still inside its interval, `PROVISIONAL` is time-closed but inside Xret's finality grace, and `FINAL` has passed that grace at Xret's receipt time. None of these states means the value is canonical or persisted.

Live subscription and historical fetch are independent operations. Xret does not fetch or merge historical rows while subscribing and does not promise an atomic boundary between the two streams. An application that needs continuity owns overlap replacement, deduplication, recent-range reconciliation, and reconnect recovery. A later explicit `sync()` independently reacquires and validates a timestamp before it can become canonical.

## Local analysis: `scan` and `scan_partial`

`scan` is local-only and strict: it returns a lazy Polars query only when the requested range is fully available, otherwise it raises `CoverageError`.

`scan_partial` is also local-only. It returns available rows plus explicit covered and gap intervals, including `missing` or observed `unavailable` gaps. Use it only when incomplete local data is intentional and visible.

## Storage and coordination

Monthly Parquet files are canonical. Trade-bar paths remain stable; family-specific artifacts use the reserved `_xret/settled-funding`, `_xret/reference-bars`, and `_xret/open-interest` trees with explicit family and artifact-version metadata. Unknown `_xret` entries are errors, not application extension points. SQLite catalog v6 is a rebuildable operational index for family identity, coverage, file hashes, ingestion runs, ordinary lineage, and OI contributor ownership. Local reads treat every indexed path as untrusted: it must exactly match the identity-derived canonical relative path, remain inside the managed tree without symlinks, and match the indexed physical hash. A read materializes each verified file from the same open descriptor before returning its lazy query, so publication cannot substitute different bytes after verification; canonical-ahead faults fail closed until explicit validation and rebuild.

A sync serializes work for its exact dataset, while different datasets can overlap provider and temporary-file work. Catalog rebuild is the exclusive migration and recovery operation. It never changes Parquet and restores only facts provable from current artifacts: contiguous row grids for trade/reference/OI, ordinary lineage, and OI contributor ownership. Previous unavailable evidence and missing slots return to `missing`; funding files are indexed but funding completeness is not reconstructed because irregular event rows do not prove observed spans. Ingestion runs, warnings, quality events, and unavailable-only datasets are also non-rebuildable.

Catalog schemas other than exactly v6 are rejected without mutation. Operators back up state and data together, stop writers, and explicitly rebuild; older releases must not open a store after `_xret` artifacts have been written. See the [catalog v6 migration guide](../guides/catalog-v6-migration.md). Xret does not automatically repair data, synthesize values, or provide in-place or down-migration.

See [Synchronize and read market data](../guides/synchronization.md) and the [API reference](../reference/api.md).
