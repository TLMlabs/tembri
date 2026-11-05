# ADR: PropAMM period summary, order after the update, and optional relay receipts

**Date:** 2026-11-05
**Status:** Accepted

## Context

Thirty-day totals are needed, but `tembri propamm-report` writes one
report per day, and it refused a day without relay receipts. Relay receipts
exist for only two days (2026-10-05 and 2026-10-09), because the relay data
APIs keep a short history.

## Decision

1. Relay receipts are optional in `propamm-report`. Without
   `relays.ndjson` for the day, the Sealing section says "Not available"
   and every other section is unchanged.
2. `tembri propamm-period --config config/propamm_reports.toml [--from
   YYYY-MM-DD --to YYYY-MM-DD]` reads the stages that `propamm-report`
   wrote for each day (`builder_payments`, `cex_markouts`, `events`,
   `period_blocks`) and writes `propamm_period_<first>_<last>.md`. It does
   not decode anything again. A day without `builder_payments.ndjson` is an
   error, not a gap.
3. The payment tables are one function, `payment_tables`, shared by the
   daily and period reports, so the two cannot drift apart.
4. The period summary adds two sections, computed in
   `tlm_classify::position`:
   - **Order after the update.** A market-block is one block, venue and
     unordered token pair. Each transaction counts once, at its first fill,
     ordered by (transaction position, log index). In market-blocks with two
     or more transactions, each neighbouring pair is compared by payment to
     the builder per gas, three ways: priority fee only; priority fee plus
     direct transfers; the same, keeping market-blocks whose first
     transaction is single-swap. Per-gas payments are compared by
     cross-multiplying in u128, so ties are exact. Medians follow Python's
     `statistics.median` (mean of the two middle values).
   - **Value of the first place.** For each contested first transaction, its
     per-gas payment above the median of lone transactions with the same
     builder, venue and single-swap flag, floored at zero, times its gas,
     valued at the Binance ETH price at the block. Reported by builder as a
     share of all payments. It is an upper bound, since payments cover the
     whole transaction.

## Consequences

- A 30-day period needs 30 daily runs first (about 2.5 minutes each with
  traces).
- Later items (fills per update, cost by size) add sections to the
  period summary.
- Check against the scratch analysis (drafts/analysis/2026-11-04-position),
  2026-09-10 to 2026-10-09: totals to builders agree within 0.3% (Titan
  $61,384 against $61,411), contested market-blocks 8,917 against 8,965,
  and every percentage within one point. The scratch script used float
  comparisons and grouped market-blocks from raw logs; the remaining
  differences were not traced further.
