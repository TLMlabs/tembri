# ADR: PropAMM price by place, payment by place, and calls into pools

**Date:** 2026-11-11
**Status:** Accepted

## Context

A maker writes at most one update per market per block, so every fill in a
block trades against the same quote. Whether a later fill in the block gets
the first fill's price, a worse one, or no fill at all is a property of each
venue's pricing rule. Earlier scratch work read the median gap only; the
median can be near zero while the price still moves with the notional taken.
A refused fill appears only in traces: calls into pools are internal frames
of router transactions, and a router that catches the revert still succeeds.

## Decision

1. `tlm_classify::place` measures the price gap. A priced market-block is
   one block, pool and stablecoin (WETH/USDC and WETH/USDT in one pool are
   different markets); its first fill sets the side. A later fill is a fill
   on that side in a transaction with no earlier fill in the market-block.
   Fills that share a transaction with an earlier one are one order split by
   a router; they are not compared, but their notional counts as taken. The
   gap is the later price against the first, in bp, signed so that positive
   means the later fill paid more.
2. Per venue the period summary reports the median, mean and own-notional
   weighted gap, the share within 0.01 bp, the gap by place (2, 3, 4 or
   later) and by earlier same-side notional (the `size_cuts_usd` bands), and
   a least-squares fit of the gap on own, first, earlier same-side and
   earlier opposite-side notional (per $10,000), with standard errors. A
   regressor that does not vary within a venue is left out rather than
   reported as zero. The fit separates the effect of size on a curve (own
   and first notional, near equal and opposite) from the effect of place at
   fixed size (earlier same-side notional).
3. `tlm_classify::propamm_position_tests_by_venue` runs the existing order
   test per venue and adds the median payment per gas at place 1, 2 and 3 or
   later, single-swap transactions only, in contested market-blocks whose
   first transaction is single-swap.
4. `tembri propamm-pool-calls --config ... [--date | --from --to]` writes
   `pool_calls.ndjson` per day: every `call` frame into a pool of the fill
   rules, from Xatu traces, with its caller, whether it reverted, whether its
   transaction succeeded, and the revert reason (`Error(string)`,
   `Panic(code)`, a custom error named from the openchain signature database,
   or the bare selector). Static and delegate calls are left out. The
   command reads only the columns it needs, never the call input (about
   300 MB per row group uncompressed).
5. It is a separate command, not part of `propamm-report`, so that fetching
   traces for revert counts does not change the report's other stages. With
   `traces = "auto"`, `propamm-report` uses traces for internal registry
   updates whenever every partition of a day has them; rerunning it on a
   traced day would add those updates to `events`, `pairs` and everything
   built on them. The existing period stages were written without traces, so
   `config/propamm_reports.toml` now sets `traces = "off"`. Using internal
   updates would be a separate change, with every day rerun.
6. The period summary's "Calls into pools" section reads `pool_calls` for
   the days that have it and says how many. Each reverted frame is placed
   against successful fills on the same pool in its block: no fill in the
   block, before the first fill, in a transaction that also filled there,
   or after an earlier fill.

## Consequences

- Price and payment by place need only the existing stages.
- Calls into pools need Xatu traces for the period (about 113 MB per
  1,000-block partition).
- Builders drop most transactions that would fail before inclusion, so
  failed transactions are undercounted. Reverts caught inside included
  transactions are all seen.
- FermiSwap reverts with `Error("SP")`. Its ABI is unverified, so whether
  `SP` is a price or a freshness check is open.
- Covers WETH/stablecoin fills only for price by place (fills with a CEX
  price). El Zorro's vault balance is not read.
