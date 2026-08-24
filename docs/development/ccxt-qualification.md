# CCXT qualification gate

`tools/ccxt_qualification_gate.py` is the maintained manual qualification harness for the Xret CCXT provider. It is deliberately excluded from CI: it performs live exchange requests, consumes rate limits, and can run for minutes or hours. The script and this policy are tracked; run artifacts, raw responses, logs, and temporary Xret state are not.

## What the gate claims

The gate does not prove every symbol and timestamp. It proves that the selected exchange has no unexplained violations of Xret's canonical invariants across a declared, reproducible sample design. Statistical and deterministic execution evidence is necessary but not sufficient: every promoted endpoint family also requires semantic proof that it satisfies the [canonical trade time-bar contract](../reference/time-bars.md). The design has two behavioral layers:

1. Mandatory history-derived cases run regardless of the statistical budget. They cover every CCXT-advertised timeframe that Xret's canonical grammar can express, one- and two-bar ranges, the current closed-bar boundary, the qualified provider page limit at `N-1`, `N`, `N+1`, and `2N+1`, explicit month and year boundaries, one- and three-year history, left/right incremental extension, identical no-op synchronization, concurrent synchronization of one dataset, invalid zero-length and misaligned ranges, strict/partial reads, and catalog validation/rebuild equivalence.
2. Additional symbol, timeframe, length, and historical-offset combinations are deterministically stratified and sampled independently for every exact market-family/settlement scope. The planner round-robins every available major, secondary, and fallback symbol stratum before reusing a stratum. The default is 135 statistical cases per scope, the minimum zero-failure sample for a one-sided 99.9% upper bound below a 5% violation rate under the declared sampling model. Mandatory cases never count toward those 135 observations. A scope whose distinct sample universe is smaller than the requested budget fails planning instead of borrowing observations from a sibling scope.

The confidence statement applies to the sampled invariant-violation rate under the sampling assumptions. It is not a probability that an exchange is universally reliable. Cases sharing one endpoint, symbol, or contiguous page sequence are correlated, so the mathematical bound is a declared sampling target rather than a proof of universal independence. The mandatory suite addresses known low-frequency, high-impact failure modes that random sampling would discover inefficiently.

## Mandatory semantic evidence

Sampling cannot discover an undocumented unit or distinguish two consistently different event universes. Promotion therefore also requires endpoint-family evidence for:

- official market identity, market kind, settlement, and contract interpretation;
- interval duration, origin, timezone, label, and finality;
- executed-trade OHLC rather than mark, index, settlement, midpoint, or quote-derived prices;
- exact base-asset volume, either native or losslessly normalized with verified instrument metadata; and
- official empty-interval, retention, and bounded-history behavior.

Evidence combines official venue documentation, the installed CCXT adapter's field and parameter mapping, direct official-versus-CCXT response comparison, and targeted invariant probes. A provider response that is internally consistent but uses a different unit or event universe is not a passing canonical bar. Statistical confidence applies only after the meaning and sampled population are established.

## Invariants exercised

The harness checks canonical market identity, timeframe alignment, in-range and strictly increasing timestamps, uniqueness, finite OHLCV values, OHLC relationships, non-negative volume, bounded acquisition, `fetch`/`sync` consistency, incremental and idempotent synchronization, concurrent same-dataset synchronization, strict/partial scan agreement, and catalog rebuild equivalence. Concurrent expected-unavailability evidence passes only when both synchronizations independently report the same gaps. The semantic probe aggregates complete one-minute public-trade buckets independently and requires OHLC plus base-volume agreement for every available market family, allowing only the market's declared amount precision rather than a broad relative approximation. A provider-observed empty interval passes only as `pass_expected_unavailability` after Xret's normal strict/partial coverage contract reports the same gap; it remains explicit unavailable coverage and is never filled or exempted in product logic.

Malformed-row injection, publication failure, crash recovery, and other destructive fault injection remain deterministic repository-test prerequisites. They are not repeated against public exchange endpoints because a live venue cannot be instructed to return a controlled corrupt response or crash at an exact storage phase.

## Cost and parallelism policy

Cases within one exchange run sequentially because the exchange's rate limit is the primary bottleneck and concurrent requests can distort the evidence. Different exchanges may run in parallel. The default of four workers matches the current machine's four physical cores while avoiding unnecessary pressure on a single venue. Increase it only when the run contains several independent exchanges and the operator has confirmed rate-limit headroom. Runtime now scales with the number of exact scopes because each one receives its own mandatory suite and 135 statistical observations; combining spot and perpetual in one command no longer dilutes either scope's confidence budget.

A venue stops after ten failed cases by default. This fail-fast threshold does not promote partial evidence: it only prevents an already-rejected endpoint from spending the full statistical budget repeating the same failure. Use `--max-failures` to change the diagnostic budget; a passing venue always completes every planned case.

The default plan uses six representative symbols, all expressible advertised timeframes, the mandatory risk suite, and 135 deterministic statistical cases drawn across 32-, 96-, and 257-bar windows ending in recent and older historical strata. Use `--plan-only` first to inspect mandatory/statistical counts and estimated pages; the estimate accounts for both bar-count and maximum-span pagination caps. Use a pilot run with one exchange before widening the venue set. The script records the confidence target, failure-rate threshold, seed, timeout, retry, effective provider page limit, worker count, request count, returned-row count, and elapsed time in its artifacts.

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

Qualification and promotion are endpoint-family scoped. Use `--market spot` or `--market perpetual` when one family must be proved independently; repeat the option to select both. The planner still allocates independent mandatory and statistical evidence to each selected exact scope. A discovery error in any requested family keeps the venue result at `candidate` instead of silently shrinking the requested population.

Derivative qualification is settlement-scoped. Repeat `--settle USDT` or another canonical settlement to restrict perpetual definitions and cases to the exact support claim; spot definitions are unaffected. The gate plans a complete mandatory suite, 135 statistical observations, and a separate executed-trade semantic witness for every selected settlement. Never infer that a passing USDT endpoint also approves USDC, inverse, or another settlement family.

The semantic probe runs before the bulk lifecycle campaign so hundreds of historical calls cannot exhaust an anonymous-IP budget before the independent witness is collected. It uses unified REST `fetchTrades` when it can establish a complete public-trade minute and requests the matching candle through the same qualified native or unified bounded-window translation as the product adapter. If the trade method is absent, fails because it is actually wallet-specific, or produces a candle disagreement consistent with a bounded-tail truncation, the gate can cross-check a fully observed minute through public CCXT Pro `watchTrades`. The fallback is qualification evidence only; it does not add a second historical acquisition path to `CcxtProvider`.

To run the same gate over every client exposed by the installed CCXT version, use `--all-ccxt`. This is a campaign-scale operation and should be run with an operator-selected output directory and conservative worker count:

```bash
uv run python tools/ccxt_qualification_gate.py \
  --all-ccxt \
  --output /tmp/xret-qualification/all-$(date -u +%Y%m%dT%H%M%SZ) \
  --workers 8
```

The all-client mode is an evidence campaign, not a blanket approval. After market discovery, the harness installs a conservative qualification-only pagination profile for each exact market-family/settlement scope so the production allowlist cannot predetermine the result. It removes every temporary profile when that venue run ends. A temporary perpetual profile whose volume semantics have no explicit compatibility policy remains `candidate` even when its structural cases pass; an AI or human must adjudicate and encode the volume policy before a promotion rerun. The full bounded-window, pagination, lifecycle, semantic, and edge-case suite then decides whether the remaining assumptions are safe. Production remains fail-closed until a passing endpoint is deliberately promoted.

Some CCXT adapters derive the native right bound from `since + limit`, while some documented native endpoints use a closed right boundary. During investigation, repeat `--omit-until CLIENT_ID` or `--accept-end-boundary CLIENT_ID` to test those hypotheses without changing production policy. The latter accepts only a row exactly equal to the requested right boundary and discards that witness; any other out-of-range row remains fatal. A passing qualification-only hypothesis must be encoded as an exact typed observation profile and rerun from a built wheel before promotion.

The harness must be run from the repository against the current workspace environment. It may install process-local qualification profiles for the duration of one exact-scope run, but it restores the production registry afterward, never adds native REST fallbacks, and never treats a temporary profile as approval.

## Promotion rule

Promotion requires every mandatory case to pass or return only contract-correct expected unavailability, all 135 statistical observations to complete without an invariant violation, and every available market family to produce an affirmative trade/candle semantic witness. Unknown event source, volume unit, interval origin, finality, or a failed bounded-window proof prevents promotion. A native listing, retention, or no-trade boundary remains visible unavailable coverage; it is never converted into synthetic coverage or an exchange-specific Xret exception. Spot and perpetual, settlement, historical, and live claims are promoted separately. Passing one endpoint family or one timeframe never promotes the whole exchange.
