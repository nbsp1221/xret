# Reference

This section defines the exact public contracts of the `xret-data` distribution, imported as `xret.data`.

- [Market data API](api.md) — exports, market identity, time ranges, verbs, result types, maintenance, and canonical schema.
- [Market-data providers](providers.md) — historical-bar SPI, optional market definitions, observation evidence, selection, and source lineage.
- [Configuration](configuration.md) — explicit configuration and discovery precedence.
- [Errors](errors.md) — public exception hierarchy and failure categories.

## Public API policy

The stable top-level import surface is the `xret.data.__all__` list documented here. Exception classes are public from `xret.data.errors`. The experimental, versioned provider-author surface is public from `xret.data.providers`. Other modules and objects are implementation details unless this reference explicitly states otherwise.

Reference pages describe behavior precisely. Task-oriented examples belong in [guides](../guides/synchronization.md), while design and lifecycle concepts belong in [explanation](../explanation/data-lifecycle.md).
