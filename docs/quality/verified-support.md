# Verified support

`verified` means that Xret has been exercised as an external user would use it against the real provider, beyond repository-controlled automated tests.

## Verification criterion

A provider combination is verified only after human-style dogfooding succeeds in a fresh project outside the repository. The evaluator installs the built `xret-data` distribution through uv, uses the public API against the real network, inspects results, and adapts the investigation when behavior warrants additional checks.

The evaluation must be broad enough to exercise the material risks of the provider combination without pretending to test every symbol. It covers representative symbols and the publicly claimed timeframes across realistic short and multi-year ranges, and separately proves the [canonical trade time-bar semantics](../reference/time-bars.md), including as applicable:

- initial acquisition and canonical storage;
- exact strict reads and partial reads;
- incremental extension;
- an identical no-op synchronization;
- partition and calendar boundaries;
- catalog validation and file-derived catalog/strict-read equivalence after rebuild;
- concurrent synchronization of the same dataset; and
- behavior at listing, availability, or other relevant provider boundaries.

The evaluator must also prove that OHLC comes from the eligible executed-trade universe, `volume` is exact base-asset quantity, and every native interval's origin, timezone, label, and finality match Xret. Derivative semantic evidence is settlement-scoped: a USDT witness does not approve a USDC, USD, or inverse BTC endpoint. The evaluator must find no unexplained gaps, duplicate timestamps, out-of-range rows, incomplete final bars, schema violations, invalid OHLC relationships, rebuild mismatches, or unresolved warnings.

Automated unit, integration, and heavy E2E tests are engineering prerequisites. Their definitions and results are already represented by test configuration, local command output, and CI logs; they are not repeated as verified promotion criteria.

## Verification granularity

Verification applies to a specific combination of:

```text
provider adapter
+ venue endpoint family
+ market family
+ bar type
+ exact timeframe
```

Representative symbols exercise shared adapter behavior. Verification does not mean that only those exact symbols are supported, and evidence from one endpoint or contract family is never generalized to an entire exchange.

This matrix is the public trust statement for historical bars. CCXT market discovery may expose a venue or market that is absent here, but historical `fetch` and `sync` fail closed unless the exact endpoint family and derivative settlement have a qualified observation profile. Absence does not assert that the venue itself is untrustworthy; it means Xret has not yet established an exhaustive historical acquisition contract for that scope. Successfully discovering a market definition, `active=True`, or an advertised timeframe is not sufficient evidence. Market-definition availability, historical-bar operability, and verified support remain separate facts.

## Qualification status at a glance

The complete canonical historical trust set is listed below. Every entry passed the maintained live-network gate with all mandatory edge cases, 135 additional deterministic observations at the declared 99.9% confidence target, contract-correct strict and partial coverage behavior, and an independent public-trade/candle semantic witness for every available market family.

The final 2026-08-23 promotion run installed the built `xret-data` 0.4.0 wheel in a fresh uv project outside the repository and ran against CCXT 4.5.75. The eleven full-venue scopes completed together in 352.49 seconds with 9,306 OHLCV calls and 767,761 returned rows; the independently scoped BingX perpetual and Bitrue spot runs also passed in 195.50 and 103.14 seconds respectively.

The 2026-08-23 bounded-window campaign used the same fresh-wheel and CCXT 4.5.75 procedure for Upbit spot, Crypto.com spot and USD perpetual, KuCoin spot and USDT perpetual, BTCTurk spot, HTX spot, Bitstamp spot, Kraken Futures USD perpetual, and Phemex USDT perpetual. All eight venue runs passed their exact endpoint scopes, totaling 1,792.23 seconds of summed venue runtime, 6,112 OHLCV calls, and 437,107 returned rows.

The 2026-08-23 native-history campaign then qualified Aster spot and USDT perpetual plus WOO spot and USDT perpetual in the same fresh-wheel environment. The joint run passed in 214.72 seconds with 1,499 OHLCV calls and 95,324 returned rows after excluding only native intervals with reproducible contract violations.

The same campaign qualified XT spot and USDT perpetual after translating its native end-boundary convention and converting perpetual contract-count volume through CCXT's exact `contractSize`. The final fresh-wheel run passed in 167.93 seconds with 992 OHLCV calls and 77,193 returned rows; its spot and perpetual candles also matched independently aggregated public trades exactly.

The 2026-08-24 compatibility campaign qualified Bitfinex spot and USDT perpetual, dYdX USDC perpetual, and Pacifica USDC perpetual from isolated Python 3.12.11 consumers with CCXT 4.5.75 and the built wheel. The passing runs completed in 6,272.72 summed seconds with 1,357 OHLCV calls and 158,148 returned rows. Bitfinex uses its documented candle-request pace, dYdX emits the official exact `fromISO`/`toISO` window parameters instead of the broken CCXT casing, and Pacifica honors its anonymous-IP credit budget while excluding its reproducibly malformed native `1h` history.

| Status | Venues |
|---|---|
| Complete canonical historical qualification | ApeX, Aster, Binance, BingX perpetual, Bitfinex, Bitget, Bitrue spot, Bitstamp spot, Bitvavo, BTCTurk spot, Bybit, Coinbase, Crypto.com, Deribit, dYdX USDC perpetual, HashKey, HTX spot, Hyperliquid, Kraken Futures USD perpetual, KuCoin, MEXC, OKX, Pacifica USDC perpetual, Phemex USDT perpetual, Toobit spot, Upbit spot, WOO, XT |
| Not supported under the arbitrary-history contract | Gate |

Live-bar verification is separate and appears under [Currently verified live bars](#currently-verified-live-bars). A historical qualification never implies live support.

## Complete canonical historical qualification

| Venue | Market families | Qualified timeframes |
|---|---|---|
| ApeX | USDT-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Aster | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` |
| Binance | Spot; USDT-settled linear perpetual | Spot: `1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M`; perpetual: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1M` |
| BingX | USDT-settled linear perpetual only | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `3d`, `1w` |
| Bitfinex | Spot; USDT-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `3h`, `4h`, `6h`, `12h`, `1d`, `1M` |
| Bitget | Spot; USDT-settled linear perpetual | Spot: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `4h`, `6h`, `12h`, `1d`, `3d`, `1w`, `1M`; perpetual additionally includes `2h` |
| Bitrue | Spot only | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h` |
| Bitstamp | Spot only | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d` |
| Bitvavo | Spot | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d` |
| BTCTurk | Spot only | `1m`, `15m`, `30m`, `1h`, `4h`, `1d` |
| Bybit | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Coinbase | Spot; USDC-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `6h`, `1d` |
| Crypto.com | Spot; USD-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Deribit | Spot; USDC-settled linear perpetual | `1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `1h`, `2h` |
| dYdX | USDC-settled linear perpetual only | `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d` |
| HashKey | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` |
| HTX | Spot only | `1m`, `5m`, `15m`, `30m`, `1h`, `4h` |
| Hyperliquid | Spot; USDC-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d` |
| Kraken Futures | USD-settled linear perpetual only | `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `12h`, `1d` |
| KuCoin | Spot; USDT-settled linear perpetual | Spot: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1M`; perpetual: `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `1w` |
| MEXC | Spot; USDT-settled linear perpetual | Spot: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1w`; perpetual additionally includes `8h`, `1M` |
| OKX | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Pacifica | USDC-settled linear perpetual only | `1m`, `3m`, `5m`, `15m`, `30m`, `2h`, `4h`, `8h`, `12h`, `1d`, `1w`, `1M` |
| Phemex | USDT-settled linear perpetual only | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `3h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Toobit | Spot only | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` |
| Upbit | Spot only | `1s`, `1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1w`, `1M` |
| WOO | Spot; USDT-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h` |
| XT | Spot; USDT-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `1d`, `1w`, `1M` |

The qualification excludes only combinations that cannot satisfy the canonical contract losslessly. Aster `1h`/`3d`, Binance `3d`, Binance USD-M `1s`/`3d`/`1w`, BingX perpetual `1M`, Bitfinex `1w`, Bitget spot `2h`, Bitstamp spot `1w`, BTCTurk spot `1w`, Deribit `3h`/`6h`/`12h`/`1d`, HTX spot `1d`/`1w`/`1M`, Hyperliquid `1w`/`1M`, Kraken Futures `1w`, KuCoin spot `1w`, KuCoin Futures `3m`/`6h`/`1M`, MEXC spot `8h`/`1M`, Pacifica perpetual `1h`, WOO `4h`/`12h`/`1d`/`1w`/`1M`, and XT `3d` are therefore not advertised by Xret. Aster's official spot and futures endpoints reproduce invalid historical `1h` OHLC rows, while its advertised `3d` label is outside Xret's canonical grammar. Pacifica's official endpoint reproduces a historical `1h` candle whose open exceeds its high, so the entire endpoint timeframe is excluded rather than adding a symbol/date waiver. WOO's official historical endpoint reproduces conflicting rows with identical timestamps on every excluded native interval. Binance USD-M `1w` is excluded because the official `BCHUSDT` weekly candle beginning 2020-01-13 violates the OHLC invariant; Xret does not add a symbol/date exception for malformed provider history. BingX spot remains unqualified because its long-interval anchors and candle volume do not match the canonical contract. Exact base volume is reconstructed from verified contract metadata for qualified linear ApeX, HashKey, MEXC, and XT perpetuals; inverse contracts whose aggregate candle volume cannot be converted exactly are excluded.

Native no-trade, pre-listing, and retention gaps remain explicit unavailable coverage. No venue-specific storage exception, synthetic bar, or gap waiver is present in Xret.

## Scopes not promoted by the current gate

Gate is explicitly unsupported for historical bars under Xret's current arbitrary-history contract. Its official endpoints impose an approximately 10,000-candle rolling horizon, and narrower successful evidence cannot prove exhaustive acquisition outside that horizon. Xret will not introduce a horizon-bounded public capability or a Gate-specific coverage exception.

Bitrue perpetuals were not promoted by the current gate; that missing futures scope remains explicit rather than being inferred from the now-qualified spot endpoint. Toobit USDT perpetual is also excluded: its historical lifecycle passed, but a fully observed CCXT Pro public-trade minute for `APT/USDT` reproduced OHLC while base volume disagreed with the native candle, so the spot approval cannot promote its sibling derivative scope. HTX USDT perpetual returned recent rows for old bounded requests. Kraken spot cannot traverse beyond its provider-defined latest-candle window, and Kraken Futures BTC-settled inverse perpetual did not yield a complete public-trade minute for exact base-volume adjudication. Pacifica spot is not inferred from the qualified USDC perpetual endpoint. Phemex spot lacks a reliable bounded `since` path in the current CCXT adapter. Bitso spot's native endpoint returned rows beyond a supplied old `end` bound. Bithumb exposes only a recent-count candle contract. LBank has reproducible native OHLC violations across core intervals, while Lighter, Extended, and Paradex failed the independent executed-trade candle semantics required by Xret. None of these scopes is inferred from a passing sibling.

## Currently verified live bars

The live-bar matrix below was requalified on 2026-08-11 from the built `xret-data` 0.3.0 wheel in a fresh external uv project with CCXT 4.5.71. Each combination opened through the public API, completed an initial bootstrap with two recent closed bars followed by the current forming bar in ascending, duplicate-free timestamp order, and closed cleanly without creating canonical state or catalog paths.

| Provider | Venue | Market family | Bar type | Timeframe | Representative symbol |
|---|---|---|---|---|---|
| CCXT Pro | Binance | Spot | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | Binance USD-M | USDT-settled linear perpetual | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | Bybit | Spot | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | Bybit | USDT-settled linear perpetual | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | OKX | Spot | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | OKX | USDT-settled linear perpetual | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |

The Binance USD-M qualification additionally exercised the handoff immediately after a one-minute boundary, while the just-closed bar remained provisional under the finality grace period. It produced consecutive `FINAL`, `PROVISIONAL`, and `FORMING` observations without touching canonical storage.

This is a connectivity, structural normalization, initial-handoff, multiplexing, and lifecycle claim. It does not independently prove the executed-trade event universe or base-asset volume unit, and it does not prove uninterrupted continuity, exhaustive delivery, canonical persistence of live observations, reconnect behavior, or long-running stability. Xret exposes disconnects and overflow as terminal failures rather than extending this matrix into those claims.

## Re-verification

Human-style re-verification is required when a change can materially alter real provider behavior, including:

- provider adapter or endpoint-family changes;
- market or symbol resolution changes;
- pagination changes;
- timestamp, timeframe, or finalization changes;
- canonical quality or normalization changes;
- commit, coverage, catalog, locking, or recovery changes;
- a provider dependency major upgrade; or
- a venue migration to a materially different endpoint.

Documentation-only changes and internal refactors that cannot affect these boundaries do not require renewed dogfooding.

## Evidence retention

The stable criterion and verified combinations are tracked here. Ad hoc scripts, temporary projects, raw network output, intermediate failures, local paths, large data stores, and detailed QA reports are transient internal evidence and are not committed to Git.
