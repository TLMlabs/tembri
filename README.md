# TEMBRI

**T**ransaction, **E**thereum, **M**empool, **B**lock and **R**elay **I**ntelligence.

Tembri is a Rust toolbox for reproducible Ethereum execution and mempool measurements. It fetches public data, assigns evidence-based activity labels, and produces Markdown reports on effective priority fees, transaction ordering, builder signals, and transaction lifecycle.

**License:** [Apache-2.0](LICENSE).

> Status: early development. The execution-data workflow and activity/tip reports are usable today; several richer data joins described below remain planned.

## What works today

- Fetch and cache public Xatu execution datasets: blocks, transactions, logs, traces, ERC-20 transfers, and native transfers.
- Compute EIP-1559 effective priority fee correctly for type-2 and legacy transactions.
- Produce activity-versus-tip and position reports for a day, a configured multi-day period, or all locally available blocks in a calendar month.
- Classify evidence-backed activity categories, including native transfers, token transfers, Uniswap V2/V3 swaps, multi-pool arbitrage candidates, contract deployments, and other contract interactions.
- Split results by normalized transaction position and early-block regions.
- Report builder signals derived from block `extra_data`, including block share, transaction volume, zero-tip transactions, mean tip, mean position, and tip/position correlation.
- Ingest public-mempool observation and admission files and derive conservative lifecycle outcomes: first seen, admitted, and sender/nonce replacement before admission.
- Decode reviewed PropAMM fill logs and Priority Update Registry signed-batch calls into typed update/fill rows; pair each fill only with an earlier update for the same block and pool.
- Measure what PropAMM fill transactions pay their block's builder: effective priority fee plus direct ETH transfers to the fee recipient (Xatu native transfers), against the spread paid to makers, by builder, in the daily PropAMM report.

## Important boundaries

| Topic | Current behaviour |
|---|---|
| Builder identity | The current execution report uses the block `extra_data` signal. It is not a verified relay/builder identity join. |
| Zero effective tip | Reported exactly as zero. It is a useful execution-data signal, but it does **not** by itself prove a transaction was private. |
| Public/private flow | Requires a compatible mempool archive joined to execution data; this join is not yet included in the activity-tip report. |
| Mempool lifecycle | Supported when observation and admission NDJSON inputs are supplied. Coverage depends on the archive and its schema. |
| Activity labels | Evidence-based heuristics, not a complete protocol taxonomy. “Arbitrage candidate” is deliberately conservative. |
| Traces and direct payments | Direct ETH payments to the fee recipient are measured for PropAMM fill transactions (`propamm-report`, `adr/2026-11-05-propamm-builder-payments.md`). Payments in other tokens are not counted. Full trace-flow analysis for other transactions is still planned. |
| Market-volatility regimes | Planned. |
| PropAMM coverage | The decoder is rule-driven. Only reviewed address/topic rules and explicitly allow-listed registry targets are decoded; it does not discover venues or infer fills from generic transfers. |
| PropAMM pairing | A pair proves receipt order in a block, not that an update was the newest off-chain quote or that a builder received it first. |
| PropAMM venues | Fills decode for FermiSwap, El Zorro, bopAMM, Tempest, Metric, Kipseli, Goliath, Park and the legacy bopAMM (about 96% of `Swapped` logs in September–October 2026); updates for FermiSwap, El Zorro, bopAMM, Kipseli and Tempest. Tempest and Park implementations are unverified; their fill rules rest on listings or a proxy name plus a token-transfer check. Fermi updates are the pricing engine's event, accepted on BuilderNet's description because the engine is unverified. |
| PropAMM quote age | Registry venues except Tempest accept only state stamped with the current block's timestamp, so maker-time quote age is 0 by contract. `update_timestamp` is a target block time, not a signing time. |
| PropAMM reference price | Uniswap V3 USDC/WETH 0.05% post-swap price; a mid only within its 5 bp fee, arbitraged by the same searchers, and carried through the 42% of blocks without a swap (`last_swap_block`). |
| PropAMM markouts | Taker-signed, WETH/USDC and WETH/USDT fills only (USDT priced as USDC), against the Uniswap reference. Both sides show positive horizon-0 markouts, so bp-level values reflect reference error and routing selection, not adverse selection. |
| PropAMM CEX markouts | `propamm-cex-markouts` uses Binance one-second spot klines: the last trade, not a quoted mid (no archived spot quotes). USDC fills are converted through USDC/USDT. Horizon 0 is the block timestamp; a fill is priced no later than its block's sealing, which relays received 0.1–3.0 s after the block timestamp on 2026-10-05; an offset of a few seconds adds about 1 bp of noise per fill. |
| PropAMM taker classes | `propamm-takers` classes fills by exact transaction structure and the reviewed table `config/propamm_takers.ndjson` (Sourcify-named contracts only). Unverified contracts are classed by structure, never as "searchers"; the atomic-arbitrage token-flow test is not yet in the pipeline. |
| PropAMM routes | `propamm-routes` uses Mempool Dumpster first sightings against the block timestamp (the slot start). The archive sees about half of all included transactions; "not seen" means not seen by that archive, not proven private. Same-pool sandwiches only; sandwiches on a fill transaction's AMM legs are a scratch analysis (`adr/2026-10-30-propamm-routes-golden-blocks-builders.md`). |
| PropAMM target self-writes | A registry write whose caller is the reviewed target itself (bopAMM inside every fill) is `target_write`, not an update, and is never paired. |
| PropAMM same-transaction updates | Execution data cannot order an internal update against a log in one transaction. Such an update is flagged (`same_tx_update`) and never paired. |
| PropAMM update timestamps | Priority Update Registry signed batches carry a maker-selected timestamp. It is retained on decoded update rows but has not yet been used for quote-age or markout analysis. |

## Quick start

Prerequisites: a recent stable Rust toolchain and enough local disk for cached Parquet data.

```bash
git clone https://github.com/TLM-Research/tembri.git
cd tembri
cargo run -p tembri -- --help
```

The CLI currently exposes:

```text
tembri <fetch|fetch-xatu|fetch-cryo|classify|metrics|propamm-bytecode-group|propamm-project|propamm-decode|propamm-pair|propamm-report|propamm-period|mempool-lifecycle|run-period|report>
```

## Run an execution report

A reusable weekly configuration is provided in `config/weekly_execution_reports.toml`. Review its block range, dates, source URL, and output directory before running it.

```bash
cargo run -p tembri -- run-period \
  --config config/weekly_execution_reports.toml
```

To analyse all locally available canonical blocks for a calendar month:

```bash
cargo run -p tembri -- report activity-tip-xatu \
  --xatu data/raw/xatu \
  --period 2026-08 \
  --out reports/activity_tip_xatu_2026-08.md
```

For reproducible execution measurements, retain the cached input partitions and record the exact configuration alongside each report.

## PropAMM P0: reviewed evidence → pairing

The P0 workflow separates contract-specific evidence from measurement. First,
project execution data into calls and logs, then decode only reviewed evidence;
finally pair the resulting update/fill rows. The output keeps unmatched fills
instead of silently dropping them.

```text
Xatu blocks + transactions + logs
                 │
                 ▼
          propamm-project
                 │  PropammLog + PropammCall NDJSON
                 ▼
PropammLog + PropammCall + reviewed rules
                 │
                 ▼
          propamm-decode
                 │  PropammEvent NDJSON
                 ▼
           propamm-pair
                 │
                 ├── pairs.ndjson
                 ├── blocks.ndjson
                 └── builders.ndjson
```

`config/labels.toml` records the currently reviewed mainnet entrypoints,
registry targets, and event topic. Address validation does not by itself prove
pool topology or an ABI, so new rules require independent ABI and sample-log
review.

### Group exact runtime bytecode

Use runtime bytecode fetched at one stated block to identify exact implementation
matches. A group is discovery evidence only: it does not label a contract as a
PropAMM or prove that two contracts share pool configuration.

```bash
cargo run -p tembri -- propamm-bytecode-group \
  --input data/propamm/runtime_bytecode.ndjson \
  --out data/propamm/bytecode_groups.ndjson
```

### Pair decoded events

The pairing layer accepts decoded update/fill evidence as NDJSON. It emits one
row per fill, including `propamm_fill_without_update` where an earlier,
same-pool update is absent from the block. Paired rows include transaction-index
and cumulative-gas gaps; block/pool and builder-signal summaries retain paired
and unmatched counts.

```bash
cargo run -p tembri -- propamm-pair \
  --input data/propamm/events.ndjson \
  --pairs-out data/propamm/pairs.ndjson \
  --blocks-out data/propamm/blocks.ndjson \
  --builders-out data/propamm/builders.ndjson \
  --period-blocks data/propamm/period_blocks.ndjson
```

### Project Xatu partitions

`propamm-project` reads the Xatu partitions for a UTC day, month, or all
available blocks and writes the two decoder inputs. It keeps logs only from
the emitters named in the reviewed fill rules, and only successful top-level
transactions to the registry. Columns are resolved by schema name. A block that
contributes a row must have a header and a complete transaction list whose gas
sums to the block's `gas_used`; otherwise the command fails rather than emit a
wrong cumulative-gas position.

```bash
cargo run -p tembri -- propamm-project \
  --xatu data/raw/xatu \
  --period 2026-10-05 \
  --fill-rules config/propamm_fill_rules.ndjson \
  --registry 0xda7afeed021eafc1c1af9c362de477dad0396b81 \
  --with-traces \
  --logs-out data/propamm/logs.ndjson \
  --calls-out data/propamm/calls.ndjson \
  --period-blocks-out data/propamm/period_blocks.ndjson
```

Add `--with-traces` (after fetching the `traces` dataset) to include internal
registry calls: `call` frames to the registry whose frame and ancestors did
not revert. Reads (`static_call`) and `delegate_call` frames are not updates.
Without traces, an update made from inside another contract is missed and its
fill is reported as `propamm_fill_without_update`.

`--period-blocks-out BLOCKS.ndjson` writes every canonical block in the period
with its builder signal. Pass it to `propamm-pair --period-blocks` to add each
builder's total blocks, the denominator for its share of blocks.

### Decode reviewed evidence

Project execution data into `PropammLog` and `PropammCall` NDJSON, then supply
only reviewed exact-match fill rules and registry targets. This command emits
the event input consumed by `propamm-pair`; it rejects ambiguous fill rules.
For the reviewed registry, it recognizes both the signed-batch update path and
the verified direct `updateState` path, retaining only allow-listed targets.

```bash
cargo run -p tembri -- propamm-decode \
  --logs data/propamm/logs.ndjson \
  --calls data/propamm/calls.ndjson \
  --fill-rules config/propamm_fill_rules.ndjson \
  --registry 0xda7afeed021eafc1c1af9c362de477dad0396b81 \
  --registry-targets config/propamm_registry_targets.ndjson \
  --out data/propamm/events.ndjson
```

The reviewed input types are newline-delimited JSON:

```json
{"emitter":"0x…","topic0":"0x…","venue":"fermiswap","pool":"0x…","kind":"fill"}
{"target":"0x…","venue":"tempest","pool":"0x…"}
```

The first row is a fill rule and the second is a registry target. The
reviewed sets are `config/propamm_fill_rules.ndjson` and
`config/propamm_registry_targets.ndjson`; their evidence is recorded in
`config/labels.toml`. Golden blocks in `crates/classify/tests/golden/propamm/`
must pair as expected under these files, and every venue with a fill rule
must appear in at least one of them. `PropammLog`
and `PropammCall` also require block number, transaction hash/index,
`cumulative_gas_before`, and builder signal; calls additionally carry `to` and
calldata `input`. See the shared model types for the complete schema.

### Route and sandwiches

`propamm-routes` joins each PropAMM transaction (`propamm-project
--txs-out`) to a [Mempool Dumpster](https://github.com/flashbots/mempool-dumpster)
daily Parquet archive by hash. A transaction first seen before its block's
timestamp is `public_before_block`; one first seen at or after it is
`seen_after_block_timestamp`; one never seen is `not_seen_public`. Pass the
files for the day before, the day and the day after. The same command writes
same-pool sandwiches: a fill bracketed by two fills of one other signer, the
front in the victim's direction and the back reversed.

```bash
cargo run -p tembri -- propamm-routes \
  --events data/propamm/events.ndjson \
  --txs data/propamm/txs.ndjson \
  --period-blocks data/propamm/period_blocks.ndjson \
  --mempool data/raw/mempool_dumpster/2026-10-04.parquet,data/raw/mempool_dumpster/2026-10-05.parquet,data/raw/mempool_dumpster/2026-10-06.parquet \
  --out data/propamm/routes.ndjson \
  --sandwiches-out data/propamm/sandwiches.ndjson
```

### Daily PropAMM report

`propamm-fetch` fetches a day's inputs and `propamm-report` runs the reviewed
pipeline on them, for each UTC day in `config/propamm_reports.toml` (or
`--date YYYY-MM-DD`).

```bash
cargo run --release -p tembri -- propamm-fetch --config config/propamm_reports.toml --date 2026-10-05
cargo run --release -p tembri -- propamm-report --config config/propamm_reports.toml --date 2026-10-05
```

`propamm-fetch` writes:
- the Xatu partitions covering the day, found by probing block timestamps,
  with traces only when `traces = "required"`;
- Binance spot one-second klines (`ETHUSDT`, `USDCUSDT`) for the day and the
  next. Each zip must match Binance's published SHA-256 and its CRC-32;
- Mempool Dumpster files for the day before, the day and the day after. The
  archive publishes no checksums, so a file must match the served length and
  read as Parquet with `hash` and `timestamp` columns;
- relay receipts of every winning block with a fill, from the relay data APIs
  (`relay_dir/DATE/receipts.ndjson`; relays that do not answer are listed in
  `relays.ndjson`).

Binance and Dumpster files are listed in their directory's `SHA256SUMS`
(`sha256sum -c` format). `propamm-report` refuses a file that is not listed or
no longer matches.

`propamm-report` runs projection, decoding, pairing with quote ages, CEX
markouts, taker classes, routes and same-pool sandwiches. It writes every
stage as NDJSON under `output_dir/DATE/` and a Markdown report
`output_dir/propamm_DATE.md` with:
- activity by venue, builders and pairing;
- markout distributions and taker classes;
- routes;
- spreads by volatility cluster, size and builder;
- a size comparison with Uniswap V3 swaps priced the same way;
- the time relays first received each winning block, and markouts at that
  time.

It refuses a day with fewer than `min_blocks_per_day` blocks or a gap, and
uses Xatu traces when every partition of the day has them (`traces = "auto"`).

## Run a lifecycle report

Lifecycle measurement consumes newline-delimited JSON inputs for public-mempool observations and block admissions. Activity labels can be joined through an optional activity file.

```bash
cargo run -p tembri -- mempool-lifecycle \
  --observations data/mempool/observations.ndjson \
  --admissions data/mempool/admissions.ndjson \
  --activities data/mempool/activities.ndjson \
  --out reports/mempool_lifecycle.md
```

The classifier treats a transaction as dropped only when a later transaction with the same sender and nonce is seen before admission. It intentionally does not infer a drop merely because an observation disappears from an incomplete feed.

## Report contents

The activity-tip report includes:

- effective priority fee in gwei/gas, using the EIP-1559 formula;
- activity-type volume, zero-tip share, average tip, and position statistics;
- average tip in position buckets, including 0–10%, 10–20%, 20–30%, and 30–60%;
- early-block-region breakdowns;
- builder-signal block share and zero-tip behaviour.

The lifecycle report includes first-seen-to-admission delay where the inputs permit it, plus conservative replacement/drop outcomes.

## Project layout

| Path | Purpose |
|---|---|
| `bin/tembri` | Command-line application |
| `crates/ingest` | Xatu, Cryo, RPC, and local input handling |
| `crates/model` | Shared data model |
| `crates/classify` | Evidence-based transaction/activity classification |
| `crates/metrics` | Tip, position, and lifecycle metrics |
| `crates/replay` | Mempool lifecycle reconstruction |
| `crates/report` | Report selection and Markdown rendering |
| `config/` | Reusable report configurations |
| `adr/` | Architecture decision records |

## Roadmap

- Verified builder and relay joins, including bid and proposer-payment measurements.
- Public/private-flow joins using compatible mempool archives.
- Broader, evaluated activity taxonomy: liquidations, protocol-specific arbitrage, lending, bridges, NFT activity, and DeFi trading.
- Direct-payment and trace-flow measurement.
- Volatility and gas-demand regimes.
- Daily PropAMM reports, effective spreads by builder and volatility, and relay-timing measurements (RN-40 P2).
- Label-quality evaluation set and versioned label datasets.

## Data sources and acknowledgements

Tembri currently uses [Xatu](https://github.com/ethpandaops/xatu) public data for its execution workflow. The project will draw on public relay, mempool, and verified-source datasets as their joins become available. Classification ideas are informed by [Brontes](https://github.com/SorellaLabs/brontes), [mev-inspect-py](https://github.com/flashbots/mev-inspect-py), and [ZeroMEV](https://zeromev.org).

## License

Tembri is licensed under the [Apache License, Version 2.0](LICENSE).
