# PropAMM: cost by size in the period summary

**Date:** 7 November 2026

## Added

- `tembri propamm-period` writes a "Cost by size" section: WETH/stablecoin
  fills priced at horizon 0, by the `size_cuts_usd` bands of the report
  config plus an "all" row, with fills, notional, share of notional, the
  mean cost per fill and the notional-weighted cost. Costs are pooled over
  the period, not means of daily means.
- A "Taker cost" section: cost at 0, 60 and 300 s pooled per fill, as the
  mean of daily means with its standard error, and notional-weighted; and
  cost by volatility band (the configured terciles).
- In "Order after the update", the fills and transactions that trade after
  an earlier fill on the same market in their block
  (`tlm_classify::propamm_shared_updates`).
