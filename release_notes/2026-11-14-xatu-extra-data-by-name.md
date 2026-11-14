# Xatu builder signal read by column name

**Date:** 14 November 2026

## Fixed

- Xatu activity-tip reports read the builder signal from `extra_data` by
  name. They used to read column 7, which is `extra_data` in the 12-column
  block layout but `extra_data_string` in the 11-column one. When that raw
  value was not valid UTF-8 the block was labelled `unknown`. On 1 July 2025
  (7,118 blocks, all in the 11-column layout) this hit 177 blocks; the 2026
  partitions use the 12-column layout and are unaffected.
- `block_number` and `gas_used` are also found by name, and every block
  column is checked for its physical type.

## Changed

- extraData that holds a geth-style client version record (an RLP list of a
  packed version and short text fields) is labelled as text, for example
  `0xd883010f0b846765746888676f312e32342e32856c696e7578` becomes
  `geth/v1.15.11/go1.24.2/linux`. This applies everywhere `builder_name()` is
  used, including the PropAMM stages, so those stages show the new labels
  only after a rerun. Other extraData is unchanged, and an empty value is
  still `unknown`.
- On 1 July 2025 the 177 blocks now fall under eleven geth version labels;
  59 blocks remain `unknown`. No other row of that day's report changes.

## Validation

- The schema tests now use the two real Xatu block layouts; the earlier
  fixtures did not match either.
- A regression test writes block 22,820,690 in both layouts and checks that
  both give `geth/v1.15.11/go1.24.2/linux`, with the same gas used and base
  fee.
- `cargo test --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` pass.

Thanks to @akatsukiwork0320 for reporting the column-order issue, with the
block counts and the regression block.
