# CCXT qualification gate

`tools/ccxt_qualification_gate.py` is the maintained manual qualification harness for the Xret CCXT provider. It is deliberately excluded from CI: it performs live exchange requests, consumes rate limits, and can run for minutes or hours. The script and this policy are tracked; run artifacts, raw responses, logs, and temporary Xret state are not.

## What the gate claims

The gate does not prove every symbol and timestamp. It proves that the selected exchange has no unexplained violations of Xret's canonical invariants across a declared, reproducible sample design. The design has two layers:

1. The union of every CCXT-advertised timeframe across the market-definition universe that Xret's canonical grammar can express is included in at least one smoke case for each market family. The smoke uses a real selected market that advertises that timeframe.
2. Additional symbol and period cases are stratified and sampled until the configured confidence sample count is reached. The default is 59 independent case targets, which is the minimum zero-failure sample for a one-sided 95% upper bound below a 5% violation rate under the declared sampling model.

The confidence statement applies to the sampled invariant-violation rate under the sampling assumptions. It is not a probability that an exchange is universally reliable. Cases sharing one endpoint, symbol, or contiguous page sequence are correlated and must not be counted as independent evidence during final review.

## Invariants exercised

The harness checks canonical market identity, timeframe alignment, in-range and strictly increasing timestamps, uniqueness, finite OHLCV values, OHLC relationships, non-negative volume, bounded acquisition, `fetch`/`sync` consistency, idempotent synchronization, strict/partial scan agreement, and catalog rebuild equivalence. A genuine provider-observed empty interval is reported as `coverage_review`; it is not silently filled or ignored. It must be independently classified as pre-listing, native no-trade, or a documented provider history boundary before promotion.

## Cost and parallelism policy

Cases within one exchange run sequentially because the exchange's rate limit is the primary bottleneck and concurrent requests can distort the evidence. Different exchanges may run in parallel. The default of four workers matches the current machine's four physical cores while avoiding unnecessary pressure on a single venue. Increase it only when the run contains several independent exchanges and the operator has confirmed rate-limit headroom.

The default plan uses six representative symbols, all expressible advertised timeframes, a short smoke range for each, and deterministic recent/boundary additions up to 59 planned cases per exchange. Coarser timeframes use longer windows than minute bars. Use `--plan-only` first to inspect case counts; use a pilot run with one exchange before widening the venue set. The script records the seed, timeout, retry, page-limit, worker, and sampling settings in every artifact so a run can be reproduced.

On the current machine, a Binance pilot with two symbols and the all-timeframe smoke set produced 36 cases and 54 estimated pages and completed in about 25 seconds with one exchange worker. The pilot intentionally exposed three real qualification issues (a perpetual `1s` invalid interval and `3d` boundary mismatches) rather than treating every advertised timeframe as automatically supported. A full default plan is expected to be measured from `--plan-only` before execution; the operator should stop and revise the strata if estimated pages or projected runtime are disproportionate.

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

The harness must be run against a built Xret distribution whose CCXT pagination contract is already present. An exchange without a qualified provider pagination profile remains blocked by contract; this tool must not bypass that protection or add native REST fallbacks.

## Promotion rule

Promotion requires all advertised canonical timeframes to have structural smoke evidence, the declared combinatorial sample to be complete, zero unexplained invariant failures, and independent review of every `coverage_review`. Spot and perpetual, settlement, historical, and live claims are promoted separately. Passing one endpoint family or one timeframe never promotes the whole exchange.
