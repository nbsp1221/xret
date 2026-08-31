# Synchronize and read market data

Use explicit verbs to control provider access, canonical storage, and local coverage. Trade bars, settled funding, reference bars, and open interest have distinct schemas and identities but share the same I/O boundary: binding is I/O-free, `fetch` is remote-only, `sync` reconciles canonical state, and `scan`/`scan_partial` are local-only.

## Bind one dataset

```python
from xret.data import BarFetchMode, MarketData

bars = MarketData().bars(
    exchange="binance",
    symbol="ETH/USDT",
    market="perpetual",
    settle="USDT",
    timeframe="5m",
)
```

`bars()` performs no provider or filesystem I/O. Spot datasets use `market="spot"` and omit `settle`; perpetual datasets use a settlement currency. Remote operations can infer an omitted perpetual settlement only when provider metadata has exactly one nonempty settlement value and exactly one listed perpetual market matching the base/quote and that settlement.

Bind derivative families separately; none adds columns to trade OHLCV:

```python
md = MarketData()

funding = md.settled_funding(
    exchange="binance", symbol="BTC/USDT", market="perpetual", settle="USDT",
)
mark = md.reference_bars(
    exchange="binance", symbol="BTC/USDT", market="perpetual",
    settle="USDT", kind="mark", timeframe="5m",
)
oi = md.open_interest(
    exchange="binance", symbol="BTC/USDT", market="perpetual",
    settle="USDT", timeframe="5m",
)
```

Funding accepts arbitrary UTC half-open bounds. Reference and OI bounds must align to their timeframe. Mark/index/premium-index bars contain OHLC but no volume; OI is a sampled gauge and is never forward-filled.

## Fetch without storing

```python
remote = bars.fetch(
    start="2025-01-01",
    end="2025-01-02",
    mode=BarFetchMode.FINAL,
)
frame = remote.data
print(remote.source)
print(remote.gaps)
```

`fetch` always uses the selected provider and never reads or changes canonical local state. Trade-bar callers must choose `BarFetchMode.LATEST` for the provider's current observation, including a forming or recently closed bar when returned, or `BarFetchMode.FINAL` for rows beyond Xret's finality grace. The derivative families retain their family-specific finality contracts and corresponding funding, reference-bar, or OI fetch result. Every result contains an eager Polars frame plus coverage, gaps, provider evidence, warnings, `is_complete`, and `require_complete()`. Exhaustive observed windows can prove absence; otherwise only validated returned facts are covered and every unproved remainder is an explicit `missing` gap.

Require completeness explicitly when the application needs the entire interval:

```python
remote.require_complete()
```

## Synchronize canonical data

```python
result = bars.sync(start="2025-01-01", end="2025-02-01")
result.require_complete()

print(result.changed)
print(result.fetched_rows)
print(result.written_partitions)
```

Ordinary `sync` fetches only implicit `missing` intervals and commits validated monthly canonical Parquet files. Revision-aware Data Vision OI syncs instead re-observe requested archive days that the same provider already owns so checksum revisions can be detected. Persisted `available` means canonical facts exist. For aligned families, a successful exhaustive provider window records each absent expected timestamp as `unavailable`; unobserved ranges remain `missing`, and failures never create negative coverage. Funding completeness follows observed spans rather than expected timestamps. Repeating an ordinary fully covered request is a canonical data/coverage no-op with `changed=False`, `fetched_rows=0`, and `written_partitions=0`, while still recording operational ingestion-run provenance.

Remote `fetch` and `sync` attempt provider-advertised CCXT scopes when Xret can represent their semantics without known loss. Prior qualification is not consulted. Results may remain incomplete when the provider cannot prove exhaustive bounds, so call `require_complete()` when incomplete remote or synchronized coverage is unacceptable. Unsupported capability, known exact incompatibility, malformed data, conflicting duplicates, ignored bounds, and non-progress fail rather than entering canonical storage.

Completeness follows each family's economic clock. Reference and OI use aligned expected timestamps. Funding uses provider-observed spans, never an assumed event cadence. A catalog rebuild can reconstruct contiguous available reference/OI grids from rows but returns missing slots and prior unavailable evidence to `missing`; funding files rebuild with no completeness spans, so strict funding scans require re-synchronization.

Syncs serialize per dataset. Different families and identities may overlap provider and temporary-file work.

## Synchronize deep Binance open interest

Select the official archive provider explicitly; Xret never falls back or switches providers inside an operation:

```python
archive = MarketData(provider="binance-data-vision").open_interest(
    exchange="binance", symbol="BTC/USDT", market="perpetual",
    settle="USDT", timeframe="5m",
)
archive.sync("2024-01-01", "2024-02-01").require_complete()
```

Data Vision ranges use whole UTC days. Canonical contributor ownership is immutable and non-overlapping: use the default CCXT provider only for a disjoint recent range. Archive checksum revisions replace only their previously owned certified interval, so deleted samples become gaps without changing disjoint CCXT-owned rows. Use `fetch` rather than `sync` for cross-provider qualification overlap.

## Require complete local coverage

```python
lazy = bars.scan(start="2025-01-01", end="2025-02-01")
frame = lazy.collect()
```

`scan` never calls the provider and does not change local state. It raises `CoverageError` for any missing or observed-unavailable interval.

## Inspect partial local coverage

```python
partial = bars.scan_partial(start="2025-01-01", end="2025-02-01")
frame = partial.data.collect()

print(partial.covered)
print(partial.gaps)
```

`scan_partial` is local-only and returns available rows with explicit coverage and gap intervals. It is the deliberate choice for incomplete local coverage.

See the [API reference](../reference/api.md) for signatures and result fields and [Provider support and trust](../explanation/provider-support.md) for the difference between capability, runtime validation, coverage, and separate qualification evidence.
