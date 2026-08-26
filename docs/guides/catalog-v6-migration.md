# Migrate a local store to catalog v6

Catalog v6 indexes the four closed dataset families (`trade_bars`, `settled_funding`, `reference_bars`, and `open_interest`) and adds contributor ownership for open-interest ranges. Xret does not mutate an older catalog in place. Current canonical Parquet remains the source of truth; the supported transition is an explicit catalog rebuild.

## Before upgrading

Stop every process that can call `sync` or maintenance against the store. Back up both `state_dir` and `data_dir` together. Do not copy only `catalog.sqlite3`: catalog-only facts such as ingestion runs, warnings, quality events, observed-unavailable coverage, and funding completeness cannot be recovered from Parquet.

A v6 reader rejects an older, newer, corrupt, or unrecognized catalog without changing it. If `catalog.sqlite3-wal` or `catalog.sqlite3-shm` remains beside an incompatible catalog, rebuild also refuses replacement because an uncheckpointed writer may exist. Close the old process cleanly and preserve the database plus sidecars for rollback or diagnosis; do not delete sidecars to force migration.

## Rebuild the catalog

Use the same explicit configuration that points at the store, validate first, then rebuild:

```python
from pathlib import Path

from xret.data import MarketData, MarketDataConfig

config = MarketDataConfig(
    state_dir=Path("/path/to/xret-state"),
    data_dir=Path("/path/to/xret-data"),
)
md = MarketData(config=config)

before = md.maintenance.validate()
print(before.is_valid, before.issues)

result = md.maintenance.rebuild_catalog()
print(result.recovered_files, result.rebuilt_datasets)

after = md.maintenance.validate()
if not after.is_valid:
    raise RuntimeError(after.issues)
```

`rebuild_catalog()` deeply validates current-layout canonical files and builds v6 state from their metadata. It never rewrites Parquet. Unknown files, malformed reserved paths, unsupported artifact versions, identity/path mismatches, conflicting lineage, overlapping contributor ranges, or insufficient metadata fail closed.

Trade-bar paths remain unchanged. New family artifacts use independent artifact schema version 1 and live under the reserved `_xret` root:

```text
_xret/settled-funding/<exchange>/perpetual/<instrument>/year=<YYYY>/month=<MM>/data.parquet
_xret/reference-bars/<mark|index|premium_index>/<exchange>/perpetual/<instrument>/<timeframe>/year=<YYYY>/month=<MM>/data.parquet
_xret/open-interest/<exchange>/perpetual/<instrument>/<timeframe>/year=<YYYY>/month=<MM>/data.parquet
```

Do not place application files or future family names under `_xret`. Unknown reserved entries make managed storage ambiguous and block validation and rebuild. They also block synchronization when no catalog exists and Xret must classify the managed tree; with a current catalog, synchronization operates only on exact canonical paths and does not scan unrelated reserved entries.

## Understand rebuilt coverage

Rebuild restores file identity, hashes, provider evidence, source ownership, and only coverage directly provable from canonical rows.

- Trade, reference-bar, and open-interest files reconstruct `available` segments from contiguous exact timestamp grids. Missing slots remain `missing`; rebuild never recreates prior `unavailable` evidence.
- Settled funding is an irregular event series. Rows prove events, not that the surrounding interval was exhaustively observed, so rebuild restores its files but no complete funding coverage. Re-run `sync` for required funding ranges before strict `scan` can succeed.
- Open-interest contributor manifests reconstruct provider-owned, ordered, non-overlapping ranges. A range owned by `binance-data-vision` cannot be synchronized by `ccxt`, or vice versa, even if both providers can observe it.
- Ingestion runs, warnings, quality events, unavailable-only datasets, and catalog-only lineage disappear on rebuild because canonical files cannot prove them.

After rebuild, run the strict scans your application depends on. Use `scan_partial` to inspect any coverage that intentionally returned to `missing`, then re-synchronize those ranges with the same owning provider.

## Roll back safely

Catalog v6 and `_xret` artifacts are not backward-compatible promises. To roll back, stop all Xret processes and restore the pre-upgrade `state_dir` and `data_dir` backup as one unit. Do not point an older release at a store after a v6 process has synchronized `_xret` artifacts or updated contributor manifests, and do not combine an old catalog with newer Parquet.

If no post-upgrade canonical writes occurred, restoring the old state and data snapshot is sufficient. If canonical writes did occur, retain the upgraded store and export or reacquire data through a compatible release rather than deleting `_xret` files or editing Parquet metadata manually. Xret provides no down-migration and no automatic compatibility shim.