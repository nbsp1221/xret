# Market-data providers

The experimental provider-author API lets an application or separately installed
package acquire historical time bars through Xret without changing Xret itself.
CCXT is the built-in implementation, not part of the provider contract.

Xret owns canonical identity, finality, schema validation, coverage, storage,
locking, and recovery. A provider owns native market resolution, optional
market-definition snapshots, and network observation. Providers never write
Xret's Parquet files or SQLite catalog.

## Architecture

The provider package is organized around a stable provider-independent core and
one directory per implementation:

```text
xret/data/providers/
├── __init__.py          # provider-author public exports
├── contracts.py         # immutable SPI values and protocol
├── runtime.py           # validation and canonical normalization
├── discovery.py         # lazy direct/installed provider binding
└── ccxt/                # built-in crypto implementation
    ├── __init__.py      # CcxtProvider export
    ├── provider.py      # implementation orchestration
    ├── client.py        # CCXT construction, retry, and transport
    ├── markets.py       # crypto market resolution and definition translation
    └── pagination.py    # qualified exhaustive observation windows
```

An additional built-in provider would be a sibling implementation package, not
another branch inside `runtime.py` or the CCXT package. Separately distributed
providers implement the public contract in their own package and use direct
injection or the installed-provider entry point. The current contract is
deliberately limited to crypto spot and perpetual historical bars; it does not
claim that non-crypto asset identity or session semantics have been designed.

## Public provider API

Provider authors import from `xret.data.providers`:

```python
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    BarObservation,
    BarRequest,
    DerivativeInterpretation,
    HistoricalBarProvider,
    Market,
    MarketDefinition,
    MarketDefinitionProvider,
    MarketIdentity,
    ObservedWindow,
    ProviderDescriptor,
    ResolvedBarMarket,
)
```

This namespace is self-contained for provider authoring; provider packages do
not import domain values from implementation modules such as
`xret.data.models`.

The initial protocol covers synchronous historical OHLCV time bars for spot and
perpetual markets. It does not define streaming, trades, quotes, fundamentals,
provider-specific columns, fallback, or synthetic timeframes.

`HistoricalBarProvider` is a structural protocol. Inheritance is optional; an
implementation supplies:

```python
class HistoricalBarProvider(Protocol):
    @property
    def descriptor(self) -> ProviderDescriptor: ...

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket: ...

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation: ...
```

## Optional market-definition capability

Market-definition discovery is a separate structural protocol; adding it does
not change `HistoricalBarProvider` SPI v1 or require existing providers to
implement it.

```python
class MarketDefinitionProvider(Protocol):
    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]: ...
```

A provider used through `MarketData` still implements `HistoricalBarProvider`.
It may additionally implement `MarketDefinitionProvider`. Calling
`MarketData.fetch_markets(...)` against a provider without this optional
capability raises `UnsupportedMarketError`; Xret never falls back to CCXT after
an explicitly selected provider lacks or fails the operation.

Every returned definition must belong to the requested canonical exchange and
market family, and canonical identities must be unique. Xret rejects mutable
collections, wrong value types, out-of-scope definitions, and duplicate
identities as provider contract failures.

`MarketDefinition` is immutable and contains canonical identity, nullable
provider-advertised active status, canonical provider-advertised timeframes,
optional exact `tick_size` and `size_increment`, and optional derivative
interpretation. Its timeframes do not assert exhaustive historical pagination
or Xret verification. Search, filtering, ordering, and result caching remain
application responsibilities.

The built-in CCXT adapter translates only entries safely expressible with the
requested spot or perpetual identity. Unrelated native instrument families,
unknown optional fields, and native timeframe names outside Xret's grammar do
not reject the venue. Canonical identity collisions are excluded rather than
resolved by exposing or arbitrarily selecting a provider-native symbol. CCXT
precision values become increments only in `TICK_SIZE` mode; limits and other
precision modes are not guessed into fixed increments.

## Descriptor and market resolution

`ProviderDescriptor` contains:

- `name`: lowercase stable source-lineage slug;
- `version`: nonempty audit string, not required to follow PEP 440;
- `api_version`: exact provider SPI major implemented by the provider.

The provider name identifies the implementation lineage, not the exchange. For
example, a Coinbase-native implementation might be named `coinbase-advanced`
while resolving the canonical venue `coinbase`.

`resolve_market` receives Xret's provider-independent `MarketIdentity`. It returns
the same canonical identity, provider-native IDs used for provenance, and the
timeframes supported for that resolved market. A provider may resolve an omitted
perpetual settlement only when it can do so unambiguously. It must not relabel the
canonical venue, symbol, or market family.

`timeframes` declares what Xret may request from that market, so every entry must
be a canonical Xret timeframe. A venue legitimately offers bar types outside that
vocabulary; exclude them instead of passing them through. A venue must not become
unresolvable because it offers a bar type Xret cannot express.

Excluding an entry is not a silent fallback. Requesting a non-canonical timeframe
raises `InvalidRequestError` before any provider call, because the timeframe
grammar rejects it. Requesting a canonical timeframe this venue does not offer
raises `UnsupportedMarketError`. Neither case substitutes another bar type.

## Observation contract

`observe_bars` receives a UTC-aware, aligned, half-open `BarRequest`. It returns:

- a Polars `DataFrame` with exactly `PROVIDER_BAR_SCHEMA`;
- one or more `ObservedWindow` values proving where absence is meaningful.

The provider frame contains only:

```text
timestamp, open, high, low, close, volume
```

Identity columns are deliberately absent. Xret adds canonical identity after
validating the returned value frame. An empty frame is valid when the provider
exhaustively observed the requested window.

Observation evidence is stronger than returned rows. In the current SPI major,
ordered windows must align to the requested timeframe and contiguously cover the
entire request. A provider that cannot prove exhaustive coverage must raise an
error; it must not return a partial observation as success. Immediately before
calling the provider, Xret records a conservative evidence time and records
completion separately after the call returns. The completed-bar gate and negative
coverage use only the pre-call evidence time, so a bar becoming final during a
slow request remains `missing` for the next sync. Xret also rejects rows outside
the request or evidence and enforces canonical OHLCV invariants.

This distinction prevents a temporary empty native page from turning an unqueried
tail into false `unavailable` coverage:

```text
no returned row != proof that the entire remaining range was observed empty
```

## Direct injection

Direct injection is the primary integration path. It supports application-owned
credentials, sessions, clients, and lifecycle without a global registry:

```python
from xret.data import MarketData

provider = MyHistoricalBarProvider(...)
market_data = MarketData(provider=provider)
```

Construction and `bars(...)` do not inspect the provider. `scan`,
`scan_partial`, `maintenance.validate`, and `maintenance.rebuild_catalog` remain
local-only and never resolve it.

## Installed provider packages

A distribution may expose a zero-argument factory through the
`xret.data.providers` entry-point group:

```toml
[project.entry-points."xret.data.providers"]
acme = "acme_xret:create_provider"
```

```python
def create_provider():
    return AcmeProvider(...)
```

Consumers select that exact name:

```python
market_data = MarketData(provider="acme")
```

Discovery, import, and factory execution are lazy and cached per `MarketData`
instance. Unknown or duplicate names, import or factory failures, descriptor-name
mismatch, incompatible API versions, and missing protocol methods raise
`ProviderError`. Xret never falls back to CCXT after an explicitly selected
provider fails.

A zero-argument factory is suitable for providers configured from their own
environment or configuration files. Providers needing application-owned runtime
objects should use direct injection.

## Source lineage and recovery

The first `sync` that commits provider-derived canonical facts binds a canonical
dataset to `ProviderDescriptor.name`. Those facts may be available rows published
to Parquet or unavailable coverage for a finalized range. A successful remote
observation that produces neither does not bind lineage. A newer implementation
version with the same name may continue the history. A different name cannot
silently append or rewrite it.

This is a lineage constraint, not part of canonical market identity: `exchange`,
`symbol`, `market`, `settle`, and `timeframe` still identify the dataset.
Provider name, version, API version, native market ID, and native symbol are
operational provenance stored in canonical Parquet metadata and ingestion state.
Parquet metadata describes the provider snapshot that most recently published the
current physical monthly file; it is not row-level acquisition history for every
bar merged into that file. While the catalog exists, ingestion runs retain the
provider snapshot for each remote operation. Rebuild can recover only the latest
file snapshot and the stable provider-name lineage from canonical Parquet.
Provider version and native IDs are audit facts, not lineage equality fields;
changes under the same provider name update the latest publication snapshot and
remain visible in ingestion runs while the catalog exists.

Available lineage can be rebuilt from canonical Parquet. An exhaustive empty
observation can create unavailable coverage and lineage without producing a
Parquet file. Those catalog-only facts are intentionally non-rebuildable: after
catalog loss, rebuild returns them to `missing` with no source binding.

## Errors and qualification

Provider-native exceptions should be allowed to propagate from provider methods;
Xret wraps unknown failures in `ProviderError` and chains the original cause.
Providers should use `UnsupportedMarketError` when a requested market or timeframe
cannot be operated safely.

Conformance to these protocols means Xret can validate and orchestrate the implementation.
It does not make a third-party provider an Xret-verified source. Verified claims
require the separate live-provider qualification policy in
[Verified support](../quality/verified-support.md).
