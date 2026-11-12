# ADR: Payments of PropAMM fill transactions to builders

**Date:** 2026-11-05
**Status:** Accepted

## Context

The daily report compared priority fees with the spread takers pay makers.
Takers reach PropAMMs through bundles, and a bundle can also pay the builder
by sending ETH to the block's fee recipient inside the transaction. A
30-day scratch analysis (`drafts/analysis/2026-11-04-position/pay30.py`)
found those direct transfers larger than the priority fees at Titan. The
README listed direct builder payments as planned.

## Decision

1. A new library function, `tlm_classify::propamm_builder_payments`, gives
   one `PropammBuilderPayment` per fill transaction:
   - priority fee = effective priority fee per gas x gas used. Type 2 and
     later use min(max priority fee, max fee - base fee); legacy types use
     gas price - base fee. This is the rule of the execution report;
   - direct transfers = native ETH transfers inside the transaction,
     including internal calls, to the block's `author` (fee recipient),
     excluding transfers sent by the fee recipient, so the builder's payment
     to the proposer is not counted;
   - `other_swap_logs` = swap logs from Uniswap V2, V3 and V4, Curve and
     Balancer V2 in the same transaction. Zero marks a single-swap
     transaction.
2. `tembri propamm-report` reads Xatu `blocks`, `transactions`,
   `native_transfers` and `logs` for the day's fill transactions, writes the
   stage `builder_payments.ndjson`, and adds a section "Payments to builders"
   by builder, for all and for single-swap WETH/stablecoin fill
   transactions: priority fee, direct transfers, their total, and the spread
   to makers (taker-signed horizon-0 CEX markout x notional), in USD at the
   Binance ETH/USDT price at the block.
3. If any partition of the day lacks `native_transfers.parquet`, the stage is
   not written and the section says so, rather than reporting zero
   transfers.

## Validation

On 2026-10-05, an independent pyarrow computation from the raw Xatu files
matches every one of the 2,557 fill transactions to the wei, for both the
priority fee and the direct transfers. On the same day the scratch formula
(Xatu `gas_price` - base fee) equals the effective priority fee on every
transaction, so the 30-day scratch figures stand. Xatu native transfers were
checked against traces on chunk 26125000 earlier (4,901 of 4,901
transactions). The other sections of the 2026-10-05 report are unchanged.

## Consequences

- Payments cover the whole transaction. For routed transactions they also
  buy the other swaps, so the single-swap table is the cleaner comparison.
- Payments in tokens other than ETH are not counted.
- Builder names come from block extra-data; the fee recipient is recorded on
  each row.
- The report is daily. Period totals (30-day builder payments)
  need a period aggregate, which is the next step.
