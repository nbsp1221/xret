# CCXT qualification gate

`tools/ccxt_qualification_gate.py` is the maintained manual qualification harness for the Xret CCXT provider. It is deliberately excluded from CI: it performs live exchange requests, consumes rate limits, and can run for minutes or hours. The script and this policy are tracked; run artifacts, raw responses, logs, and temporary Xret state are not.

## What the gate claims

The gate does not prove every symbol and timestamp. It proves that the selected exchange has no unexplained violations of Xret's canonical invariants across a declared, reproducible sample design. The design has two independent layers:

1. Mandatory history-derived cases run regardless of the statistical budget. They cover every CCXT-advertised timeframe that Xret's canonical grammar can express, one- and two-bar ranges, the current closed-bar boundary, the qualified provider page limit at `N-1`, `N`, `N+1`, and `2N+1`, explicit month and year boundaries, one- and three-year history, left/right incremental extension, identical no-op synchronization, concurrent synchronization of one dataset, invalid zero-length and misaligned ranges, strict/partial reads, and catalog validation/rebuild equivalence.
2. Additional symbol, timeframe, length, and historical-offset combinations are deterministically stratified and sampled. The default is 135 statistical cases per exchange, the minimum zero-failure sample for a one-sided 99.9% upper bound below a 5% violation rate under the declared sampling model. Mandatory cases never count toward those 135 observations.

The confidence statement applies to the sampled invariant-violation rate under the sampling assumptions. It is not a probability that an exchange is universally reliable. Cases sharing one endpoint, symbol, or contiguous page sequence are correlated, so the mathematical bound is a declared sampling target rather than a proof of universal independence. The mandatory suite addresses known low-frequency, high-impact failure modes that random sampling would discover inefficiently.

## Invariants exercised

The harness checks canonical market identity, timeframe alignment, in-range and strictly increasing timestamps, uniqueness, finite OHLCV values, OHLC relationships, non-negative volume, bounded acquisition, `fetch`/`sync` consistency, incremental and idempotent synchronization, concurrent same-dataset synchronization, strict/partial scan agreement, and catalog rebuild equivalence. A genuine provider-observed empty interval is reported as `coverage_review`; it is not silently filled or ignored. It must be independently classified as pre-listing, native no-trade, or a documented provider history boundary before promotion.

Malformed-row injection, publication failure, crash recovery, and other destructive fault injection remain deterministic repository-test prerequisites. They are not repeated against public exchange endpoints because a live venue cannot be instructed to return a controlled corrupt response or crash at an exact storage phase.

## Cost and parallelism policy

Cases within one exchange run sequentially because the exchange's rate limit is the primary bottleneck and concurrent requests can distort the evidence. Different exchanges may run in parallel. The default of four workers matches the current machine's four physical cores while avoiding unnecessary pressure on a single venue. Increase it only when the run contains several independent exchanges and the operator has confirmed rate-limit headroom.

The default plan uses six representative symbols, all expressible advertised timeframes, the mandatory risk suite, and 135 deterministic statistical cases drawn across 32-, 96-, and 257-bar windows ending in recent and older historical strata. Use `--plan-only` first to inspect mandatory/statistical counts and estimated pages; use a pilot run with one exchange before widening the venue set. The script records the confidence target, failure-rate threshold, seed, timeout, retry, effective provider page limit, worker count, request count, returned-row count, and elapsed time in its artifacts.

Do not reuse the earlier 95% pilot runtime as an estimate for this gate. The strengthened gate has a larger statistical budget and mandatory multi-page cases. Measure every campaign with `--plan-only`, retain the generated request metrics, and stop or revise the run if the projected traffic is disproportionate.

## Usage

Run from the repository with the workspace environment:

```bash
uv run python tools/ccxt_qualification_gate.py \
  --exchange binance \
  --output /tmp/xret-qualification/binance-$(date -u +%Y%m%dT%H%M%SZ) \
  --plan-only
```

After reviewing the plan, remove `--plan-only` for the live run:

```bash
uv run python tools/ccxt_qualification_gate.py \
  --exchange binance \
  --output /tmp/xret-qualification/binance-$(date -u +%Y%m%dT%H%M%SZ)
```

Multiple exchanges can be supplied by repeating `--exchange`; the default four workers run exchanges in parallel and cases within each exchange serially. `summary.json` reports `pass`, `candidate`, or `error`. `candidate` means coverage or another reviewable result remains; it is not an approval.

To run the same gate over every client exposed by the installed CCXT version, use `--all-ccxt`. This is a campaign-scale operation and should be run with an operator-selected output directory and conservative worker count:

```bash
uv run python tools/ccxt_qualification_gate.py \
  --all-ccxt \
  --output /tmp/xret-qualification/all-$(date -u +%Y%m%dT%H%M%SZ) \
  --workers 8
```

The all-client mode is an evidence campaign, not a blanket approval. Clients without a qualified Xret pagination contract will produce explicit contract failures; clients with a provider capability failure remain separate from those contract failures.

The harness must be run against a built Xret distribution whose CCXT pagination contract is already present. An exchange without a qualified provider pagination profile remains blocked by contract; this tool must not bypass that protection or add native REST fallbacks.

## Promotion rule

Promotion requires every mandatory case to pass or receive an independent native-coverage classification, all 135 statistical observations to complete without an invariant violation, and independent review of every `coverage_review`. A classified native listing, retention, or no-trade boundary remains visible evidence and narrows the promoted claim; it is never converted into synthetic coverage or an exchange-specific Xret exception. Spot and perpetual, settlement, historical, and live claims are promoted separately. Passing one endpoint family or one timeframe never promotes the whole exchange.
