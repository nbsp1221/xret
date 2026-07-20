# Xret

Xret is an AI-native quantitative research and execution ecosystem.

The initial distribution is `xret-data`, imported in Python as `xret.data`.

## xret-data

```python
from xret.data import MarketData

market_data = MarketData()
bars = market_data.bars(
    exchange="binance", symbol="BTC/USDT", market="perpetual",
    settle="USDT", timeframe="1h",
)

bars.sync("2024-01-01", "2024-02-01")
frame = bars.scan("2024-01-01", "2024-02-01")
```

`MarketData(config=None)` is the public entry point (`from xret.data import
MarketData`). `config` defaults to `resolve_config()`; passing an explicit
`MarketDataConfig` bypasses resolution entirely. `market_data.bars(...)`
binds one canonical market identity plus timeframe and performs no I/O;
identity resolution against the provider happens lazily inside
`fetch`/`sync`.

Market identity is provider-independent: `exchange` is a lowercase canonical
slug (`"binance"`), `symbol` is an NFC-normalized `BASE/QUOTE` pair with one
structural `/` and nonempty Unicode components, and `market` is `"spot"` or
`"perpetual"`. A resolved perpetual identity always has a nonempty `settle`
component; spot omits `settle`. For `fetch`/`sync`, an omitted perpetual
`settle` is inferred only when provider metadata has exactly one nonempty
settlement value and exactly one listed perpetual market matching the
base/quote and that settlement. Local reads infer an omitted `settle` only from
exactly one locally known dataset candidate.

Every `BarDataset` verb takes a `start` and an optional `end` (an ISO date
string, an offset-bearing ISO timestamp, or a timezone-aware `datetime`)
describing a UTC-aware, half-open `[start, end)` range. Both bounds must align
to the dataset timeframe.

### Verb semantics

- `bars.fetch(start, end=None) -> pl.DataFrame` — provider network call
  only; never reads or writes canonical local state. Returns completed
  bars only. When `end` is omitted, it resolves to the end of the latest
  completed bar at call time.
- `bars.sync(start, end=None) -> SyncResult` — reconciles implicit `missing`
  coverage against the provider and commits validated data. A successful
  provider observation records each absent completed bar boundary as
  `unavailable`; provider failures leave coverage `missing` and raise a domain
  error. A fully covered request is a canonical data/coverage no-op but still
  records operational ingestion-run provenance.
- `bars.scan(start, end=None) -> pl.LazyFrame` — local-only; raises
  `CoverageError` unless the requested range has full local coverage.
  When `end` is omitted, it resolves to the local timeframe boundary at
  call time (no provider finalization grace).
- `bars.scan_partial(start, end=None) -> PartialScanResult` — local-only;
  returns available rows plus structured `covered`/`gaps` intervals without
  weakening strict `scan`.

`fetch` always uses the provider; `sync` uses it only for implicit `missing`
intervals. `scan` and `scan_partial` never use the provider.

### Maintenance

- `market_data.maintenance.validate() -> CatalogValidationResult` —
  compares the SQLite operational index against canonical Parquet without
  mutating either.
- `market_data.maintenance.rebuild_catalog() -> CatalogRebuildResult` —
  exclusively rebuilds SQLite from validated canonical Parquet metadata.
  Parquet is never changed. Operational history that Parquet cannot prove is
  intentionally reset; unreadable or insufficient evidence fails closed.

### Config

`resolve_config()` resolves the highest-precedence source: `XRET_CONFIG`
environment variable (must name an existing TOML file) >
`~/.xret/config.toml` (used only if present) > built-in defaults
(`~/.xret` for `state_dir`, `<state_dir>/data` for `data_dir`). No I/O
happens at import time; `MarketData(config=...)` bypasses resolution
entirely when given an explicit `MarketDataConfig`.

### Errors

Every public domain or operational error raised by `xret.data` inherits from
`xret.data.errors.XretDataError`: `ConfigurationError`, `InvalidRequestError`,
`UnsupportedMarketError`, `ProviderError`, `CoverageError`, `SyncError`,
`CatalogError`. Errors chain (`raise ... from exc`) through the underlying
provider, SQLite, or filesystem exception where applicable.

See the [documentation](docs/index.md) for guides, reference, data lifecycle,
configuration, errors, and verified support.

## Development

```bash
uv sync --locked --all-packages
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest
uv build --package xret-data
```

## License

Xret is licensed under the MIT License.
