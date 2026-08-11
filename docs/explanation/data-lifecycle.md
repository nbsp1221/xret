# Data lifecycle

Xret separates remote observation, canonical mutation, and local analysis for one `MarketData.bars(...)` dataset.

## Dataset identity

A dataset is identified by exchange, `BASE/QUOTE` symbol, market family, settlement, and timeframe. Xret operates `spot` and `perpetual` markets. Spot has no settlement; perpetual uses its settlement currency. For `fetch` and `sync`, an omitted perpetual settlement is inferred only when provider metadata has exactly one nonempty settlement value and exactly one listed perpetual market matching the base, quote, and settlement. For local reads, it is inferred only from exactly one locally known dataset candidate.

## Remote observation: `fetch`

`fetch` gets completed bars from the provider and returns an eager Polars `DataFrame`. It does not read or change canonical local state.

Returned rows and observation evidence are separate facts. Xret traverses qualified, bounded provider windows across the requested range. A successful empty window proves only that window contains no returned candles; it does not terminate traversal or imply that later history is empty. If exhaustive window semantics are not qualified for an endpoint family, the remote operation fails closed.

## Canonical reconciliation: `sync`

`sync` reads local coverage, fetches only implicit `missing` intervals, validates received bars, and publishes each canonical monthly Parquet file by atomic replacement. A fully covered sync still records ingestion-run provenance, while its data result is a visible no-op.

Coverage records facts: persisted `available` intervals have canonical bars. An `unavailable` interval is recorded only when a successfully and exhaustively observed bounded window omits its completed bar boundary. `missing` is implicit when neither fact exists. Provider, validation, incomplete traversal, or storage failures never turn an interval into `unavailable`.

## Transient live observation

A live `BarUpdate` is an in-memory observation and carries explicit time-based finality. `FORMING` is still inside its interval, `PROVISIONAL` is time-closed but inside Xret's finality grace, and `FINAL` has passed that grace at Xret's receipt time. None of these states means the value is canonical or persisted.

An opt-in live bootstrap observes a small recent closed window and merges it with updates buffered after the live stream activates. This bridges the initial historical-to-live boundary without weakening `fetch` or `sync` and without opening the catalog or storage. A later explicit `sync()` independently reacquires and validates the timestamp before it can become canonical.

## Local analysis: `scan` and `scan_partial`

`scan` is local-only and strict: it returns a lazy Polars query only when the requested range is fully available, otherwise it raises `CoverageError`.

`scan_partial` is also local-only. It returns available rows plus explicit covered and gap intervals, including `missing` or observed `unavailable` gaps. Use it only when incomplete local data is intentional and visible.

## Storage and coordination

Monthly Parquet files are canonical. Their metadata describes the dataset and file interpretation; it does not carry operational history. SQLite is a rebuildable operational index that records coverage, file locations and physical SHA values, and ingestion runs. SQLite uses WAL with short transactions.

A sync serializes work for its dataset, while different datasets can overlap provider and temporary-file work. Catalog rebuild is the exclusive maintenance operation. It restores only dataset, file, and `available` coverage facts provable from canonical Parquet evidence; it drops unavailable observations, ingestion runs, warnings, and quality events. Rebuild never changes Parquet and fails closed when that evidence is insufficient. Xret does not automatically repair data, assign failure causes, synthesize bars, or provide forensic recovery.

See [Synchronize and read bars](../guides/synchronization.md) and the [API reference](../reference/api.md).
