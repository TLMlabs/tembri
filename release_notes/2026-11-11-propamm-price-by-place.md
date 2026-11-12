# PropAMM: price by place, payment by place, calls into pools

**Date:** 11 November 2026

## Added

- `tembri propamm-period` writes three sections:
  - **Price by place:** per venue, how a later fill's price in the block
    compares with the first fill's: median, mean and weighted gap, share at
    the same price, by place and by notional already taken, and a fit that
    holds own size fixed.
  - **Payment by place:** the order test per venue, and median payment per
    gas at each place for single-swap transactions.
  - **Calls into pools:** calls into PropAMM pools from traces, how many
    reverted, whether the router caught the revert, where the revert sits
    against fills on the same pool, the reasons and the main callers.
- `tembri propamm-pool-calls` writes the daily `pool_calls.ndjson` stage
  from Xatu traces. It does not change any `propamm-report` stage.

See `adr/2026-11-11-propamm-price-by-place-and-pool-calls.md`.
