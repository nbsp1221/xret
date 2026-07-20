# Errors

Every public domain or operational error inherits from `xret.data.errors.XretDataError`. Catch the base class at a package boundary or a specific subclass when the caller has a defined response.

| Exception | Meaning |
|---|---|
| `XretDataError` | Base class for public Xret data errors. |
| `ConfigurationError` | Configuration paths or TOML values are invalid. |
| `InvalidRequestError` | A caller value violates the documented request contract. |
| `UnsupportedMarketError` | The market, symbol, timeframe, or settlement cannot be operated safely. |
| `ProviderError` | A provider call failed or `fetch` fetched-data quality validation failed. |
| `CoverageError` | A strict local scan found missing or observed-unavailable coverage. |
| `SyncError` | Synchronization could not complete safely, including fetched-batch validation failure. |
| `CatalogError` | Catalog validation, rebuilding, locking, or persistence failed. |

Underlying CCXT, SQLite, and filesystem failures are chained as causes where applicable. A provider or validation failure does not mark coverage unavailable.

```python
from xret.data.errors import CoverageError

try:
    frame = bars.scan(start="2025-01-01", end="2025-02-01")
except CoverageError:
    result = bars.sync(start="2025-01-01", end="2025-02-01")
    result.require_complete()
```

Use `scan_partial(...)` instead of catching `CoverageError` when an incomplete local result is intentionally acceptable.
