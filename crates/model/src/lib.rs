//! Shared domain types. Monetary values are wei and are deliberately integer-only.
use serde::{Deserialize, Serialize};

pub type Wei = u128;
pub type Gas = u64;
pub type BlockNumber = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub number: BlockNumber,
    pub slot: u64,
    pub timestamp: u64,
    pub gas_limit: Gas,
    pub gas_used: Gas,
    pub base_fee: Wei,
    pub fee_recipient: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transaction {
    pub hash: String,
    pub position: u32,
    pub gas_limit: Gas,
    pub gas_used: Gas,
    pub effective_priority_fee: Wei,
    pub cumulative_gas_before: Gas,
    pub max_fee_per_gas: Wei,
    pub direct_payment: Wei,
    pub selector: Option<String>,
    pub log_topics: Vec<String>,
}

impl Transaction {
    /// Observed above-base-fee density, denominated in wei per signed gas.
    pub fn density(&self) -> Wei {
        if self.gas_limit == 0 {
            return 0;
        }
        (self
            .effective_priority_fee
            .saturating_mul(self.gas_used as Wei)
            .saturating_add(self.direct_payment))
            / self.gas_limit as Wei
    }

    pub fn is_in_early_region(&self, block: &Block, chi_bps: u16) -> bool {
        let early_gas = (block.gas_limit as u128 * chi_bps as u128 / 10_000) as Gas;
        self.cumulative_gas_before.saturating_add(self.gas_used) <= early_gas
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MempoolSeen {
    pub hash: String,
    pub first_seen: Option<u64>,
    pub sources: u16,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxLabels {
    pub in_early_region: bool,
    pub public: bool,
    pub wait_seconds: Option<u64>,
    pub arbitrage: bool,
    pub liquidation: bool,
    pub oracle: bool,
    pub bundle: bool,
    pub time_sensitive: bool,
    pub consumer_candidate: bool,
    pub provider_candidate: bool,
    pub delay_tolerant: bool,
    pub density: Wei,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelConfig {
    pub chi_bps: u16,
    pub beta_bps: u16,
    pub slot_seconds: u64,
    pub liquidation_selectors: Vec<String>,
    pub oracle_addresses: Vec<String>,
}

impl Default for LabelConfig {
    fn default() -> Self {
        Self {
            chi_bps: 2_000,
            beta_bps: 1_000,
            slot_seconds: 12,
            liquidation_selectors: Vec::new(),
            oracle_addresses: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayParams {
    pub chi_bps: u16,
    pub beta_bps: u16,
    pub omega_bps: u16,
    pub theta_bps: u16,
    pub provider_rebate_cap: Wei,
    pub reserve_target: Wei,
    pub reserve_kappa_bps: u16,
}

impl Default for ReplayParams {
    fn default() -> Self {
        Self {
            chi_bps: 2_000,
            beta_bps: 1_000,
            omega_bps: 5_000,
            theta_bps: 10_000,
            provider_rebate_cap: 0,
            reserve_target: 0,
            reserve_kappa_bps: 0,
        }
    }
}

pub fn bps_mul(value: Wei, bps: u16) -> Wei {
    value.saturating_mul(bps as Wei) / 10_000
}

/// Daily storage row for the v0.2 `blocks` table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockRow {
    pub block_number: BlockNumber,
    pub slot: u64,
    pub timestamp: u64,
    pub gas_limit: Gas,
    pub gas_used: Gas,
    pub base_fee: Wei,
    pub fee_recipient: String,
    pub extra_data: String,
    pub tx_count: u32,
    pub builder_pubkey: Option<String>,
    pub proposer_value: Option<Wei>,
    pub relay: Option<String>,
    pub builder: Option<String>,
    pub builder_source: Option<String>,
    pub builder_conflict: bool,
}

/// Daily storage row for the v0.2 `txs` table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxRow {
    pub block_number: BlockNumber,
    pub position: u32,
    pub hash: String,
    pub from: String,
    pub to: Option<String>,
    pub tx_type: u8,
    pub gas_limit: Gas,
    pub gas_used: Gas,
    pub max_fee: Option<Wei>,
    pub max_priority_fee: Option<Wei>,
    pub effective_priority_fee: Wei,
    pub cumulative_gas_before: Gas,
    pub status: bool,
    pub selector: Option<String>,
    pub log_topics: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MempoolRow {
    pub hash: String,
    pub first_seen_ms: u64,
    pub sources: u16,
    pub included_block: Option<BlockNumber>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MempoolObservation {
    pub hash: String,
    pub sender: String,
    pub nonce: u64,
    pub seen_at_ms: u64,
    pub submitted_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MempoolAdmission {
    pub hash: String,
    pub admitted_at_ms: u64,
    pub block_number: BlockNumber,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MempoolActivity {
    pub hash: String,
    pub activity_type: String,
}

/// A decoded PropAMM update or fill.  This is deliberately downstream of ABI
/// decoding: a venue-specific decoder supplies these rows, while pairing stays
/// identical across venues and remains testable without treating a selector as
/// evidence of a fill.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammEvent {
    pub block_number: BlockNumber,
    pub pool: String,
    pub venue: String,
    pub builder: Option<String>,
    pub tx_hash: String,
    pub tx_position: u32,
    pub cumulative_gas_before: Gas,
    /// Maker-selected timestamp from a decoded registry update, if present.
    #[serde(default)]
    pub update_timestamp: Option<u64>,
    pub kind: PropammEventKind,
    /// Terms of a fill decoded from the reviewed `IPropAMM.Swapped` event.
    #[serde(default)]
    pub fill: Option<PropammFillTerms>,
}

/// `Swapped(sender, tokenIn, tokenOut, amountIn, amountOut, recipient)` as
/// declared by the verified `IPropAMM` interface (El Zorro, BopAmm).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammFillTerms {
    pub log_index: u32,
    pub sender: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: u128,
    pub amount_out: u128,
    pub recipient: String,
}

/// A log projected from execution data before venue-specific decoding.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammLog {
    pub block_number: BlockNumber,
    pub emitter: String,
    pub tx_hash: String,
    pub tx_position: u32,
    pub cumulative_gas_before: Gas,
    pub log_index: u32,
    pub topic0: String,
    pub builder: Option<String>,
    /// `topic1..topic3`, as present.
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub data: Option<String>,
}

/// A transaction projected from execution data for calldata-level decoding.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammCall {
    pub block_number: BlockNumber,
    pub to: String,
    pub tx_hash: String,
    pub tx_position: u32,
    pub cumulative_gas_before: Gas,
    pub input: String,
    pub builder: Option<String>,
    /// Trace position of an internal call (for example `0_1`); `None` for the
    /// transaction's top-level call.
    #[serde(default)]
    pub trace_address: Option<String>,
    /// Immediate caller of an internal call; `None` for a top-level call.
    #[serde(default)]
    pub from: Option<String>,
}

/// A reviewed event rule. `pool` is explicit so a registry update may later be
/// decoded to its target pool rather than incorrectly using the registry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammEventRule {
    pub emitter: String,
    pub topic0: String,
    pub venue: String,
    pub pool: String,
    pub kind: PropammEventKind,
}

/// An explicitly reviewed registry target. A registry call is not considered
/// PropAMM activity unless its decoded target matches one of these rows.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammRegistryTarget {
    pub target: String,
    pub venue: String,
    pub pool: String,
}

/// Runtime bytecode collected from a deployed contract at a stated block.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BytecodeRow {
    pub address: String,
    pub runtime_bytecode: String,
}

/// Exact runtime-bytecode group. Membership is discovery evidence only; it
/// does not assign a protocol or infer that the contracts are interchangeable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BytecodeGroup {
    pub code_hash: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PropammEventKind {
    Update,
    Fill,
    /// A registry write made by a reviewed target itself from inside another
    /// transaction (for example bopAMM writing state back during a fill). It
    /// is not a maker quote update and is never paired.
    TargetWrite,
}

/// The receipt-order relationship for one PropAMM fill.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammFillPair {
    pub block_number: BlockNumber,
    pub pool: String,
    pub venue: String,
    pub builder: Option<String>,
    pub fill_tx_hash: String,
    pub fill_tx_position: u32,
    pub result: PropammPairingResult,
    pub update_tx_hash: Option<String>,
    pub update_tx_position: Option<u32>,
    pub transaction_gap: Option<u32>,
    pub gas_gap: Option<Gas>,
    pub fills_before_next_update: u32,
    /// An update for this pool is in the fill's own transaction. Execution data
    /// does not order an internal call against a log, so it is never paired.
    #[serde(default)]
    pub same_tx_update: bool,
    /// Block of the latest update for this pool in an earlier transaction,
    /// carried across blocks within the input. `None` when no such update is
    /// in the input, which near the start of a period is censoring, not
    /// evidence that the pool had no update.
    #[serde(default)]
    pub last_update_block: Option<BlockNumber>,
    /// `block_number - last_update_block`; zero for a same-block pair.
    #[serde(default)]
    pub blocks_since_update: Option<u64>,
    /// Maker-supplied timestamp of that update (registry updates only). It is
    /// not a builder-receipt time.
    #[serde(default)]
    pub update_timestamp: Option<u64>,
    /// Fill block timestamp minus `update_timestamp`, in seconds. Signed: a
    /// maker clock may run ahead of block time.
    #[serde(default)]
    pub quote_age_seconds: Option<i64>,
}

/// A fill is retained even when no qualifying same-block update is observed.
/// The serialized spelling is part of the report/data contract.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PropammPairingResult {
    PropammFill,
    PropammFillWithoutUpdate,
}

/// Same-block, same-pool PropAMM activity derived from decoded events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammBlockSummary {
    pub block_number: BlockNumber,
    pub pool: String,
    pub venue: String,
    pub builder: Option<String>,
    pub updates: u32,
    pub fills: u32,
    pub paired_fills: u32,
    pub fills_without_update: u32,
    #[serde(default)]
    pub target_writes: u32,
}

/// One canonical block in a measurement period with its builder signal; the
/// denominator for a builder's share of blocks.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammPeriodBlock {
    pub block_number: BlockNumber,
    pub builder: Option<String>,
    /// Block timestamp in Unix seconds.
    #[serde(default)]
    pub timestamp: Option<u64>,
}

/// One post-swap price of the reviewed on-chain reference pool.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReferencePricePoint {
    pub block_number: BlockNumber,
    pub tx_position: u32,
    pub log_index: u32,
    /// Pool `sqrtPriceX96` after the swap, as emitted (`0x`-prefixed hex).
    pub sqrt_price_x96: String,
    /// Quote-token units per base-token unit (USDC per WETH for the reviewed pool).
    pub price: f64,
}

/// Taker-signed markouts of one WETH/stablecoin PropAMM fill against the
/// on-chain reference price. Positive means the taker gained (adverse
/// selection for the maker). `ref_block_*` is the block of the reference swap
/// behind each price, so stale references can be filtered.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PropammMarkout {
    pub block_number: BlockNumber,
    pub venue: String,
    pub pool: String,
    pub builder: Option<String>,
    pub fill_tx_hash: String,
    pub fill_tx_position: u32,
    pub log_index: u32,
    pub sender: String,
    pub recipient: String,
    /// `buy_weth` or `sell_weth`, from the taker's side.
    pub taker_side: String,
    /// `usdc` or `usdt`; USDT is priced as USDC.
    pub stable: String,
    pub weth_amount: f64,
    pub stable_amount: f64,
    /// Stablecoin per WETH.
    pub fill_price: f64,
    pub mid_at_fill: Option<f64>,
    pub ref_block_at_fill: Option<BlockNumber>,
    pub markout_0_bp: Option<f64>,
    pub markout_1_bp: Option<f64>,
    pub ref_block_1: Option<BlockNumber>,
    pub markout_5_bp: Option<f64>,
    pub ref_block_5: Option<BlockNumber>,
    pub markout_25_bp: Option<f64>,
    pub ref_block_25: Option<BlockNumber>,
}

/// A taker-signed markout of one WETH/stablecoin fill against a CEX trade
/// price, 12, 4 and 2 seconds before the fill block's timestamp, at it, and
/// 12, 60 and 300 seconds after it. The difference between horizon 0 and a
/// negative horizon is the taker-signed price move just before the block.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PropammCexMarkout {
    pub block_number: BlockNumber,
    /// Fill block timestamp in Unix seconds: the horizon-0 time.
    pub block_timestamp: u64,
    pub venue: String,
    pub pool: String,
    pub builder: Option<String>,
    pub fill_tx_hash: String,
    pub fill_tx_position: u32,
    pub log_index: u32,
    pub sender: String,
    pub recipient: String,
    /// `buy_weth` or `sell_weth`, from the taker's side.
    pub taker_side: String,
    /// `usdc` or `usdt`. A USDC fill is priced as ETH/USDT divided by USDC/USDT.
    pub stable: String,
    pub weth_amount: f64,
    pub stable_amount: f64,
    /// Stablecoin per WETH.
    pub fill_price: f64,
    /// CEX price in the fill's stablecoin at the block timestamp.
    pub cex_mid_at_fill: Option<f64>,
    /// Seconds from the last ETH/USDT trade to the block timestamp.
    pub cex_age_at_fill_s: Option<u64>,
    pub markout_minus_12s_bp: Option<f64>,
    pub markout_minus_4s_bp: Option<f64>,
    pub markout_minus_2s_bp: Option<f64>,
    pub markout_0s_bp: Option<f64>,
    pub markout_12s_bp: Option<f64>,
    pub markout_60s_bp: Option<f64>,
    pub markout_300s_bp: Option<f64>,
}

/// The envelope of a transaction that emitted a reviewed PropAMM log.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammTxEnvelope {
    pub block_number: BlockNumber,
    pub tx_hash: String,
    pub tx_position: u32,
    /// Signer.
    pub from: String,
    /// `None` for a contract creation.
    pub to: Option<String>,
    pub max_priority_fee_per_gas: Option<u64>,
}

/// How a PropAMM transaction reached its block, as far as a public-mempool
/// archive can tell. The archive's first sighting is compared with the block
/// timestamp (the slot start).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PropammRoute {
    /// First seen publicly before the block timestamp.
    PublicBeforeBlock,
    /// First seen publicly at or after the block timestamp: visible only
    /// once the block was being or had been proposed.
    SeenAfterBlockTimestamp,
    /// Never seen by the archive.
    NotSeenPublic,
}

/// The route of one transaction with a reviewed PropAMM log.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammTxRoute {
    pub block_number: BlockNumber,
    pub tx_hash: String,
    pub tx_position: u32,
    pub route: PropammRoute,
    /// Archive first-seen time, Unix milliseconds.
    pub first_seen_ms: Option<u64>,
    /// Block timestamp in milliseconds minus `first_seen_ms`; positive when
    /// seen before the slot start.
    pub lead_ms: Option<i64>,
}

/// A fill bracketed on the same pool and block by two fills of one other
/// signer: the front in the victim's direction, the back reversed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammSandwich {
    pub block_number: BlockNumber,
    pub pool: String,
    pub venue: String,
    pub victim_tx_hash: String,
    pub victim_log_index: u32,
    pub front_tx_hash: String,
    pub back_tx_hash: String,
    /// Signer of the front and back transactions.
    pub attacker: String,
}

/// A reviewed address that identifies a taker's order-flow channel.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammTakerLabel {
    pub address: String,
    /// `intent_settlement`, `aggregator`, `account_abstraction` or
    /// `batch_utility`.
    pub class: String,
    pub name: String,
    pub evidence: String,
}

/// The taker class of one decoded fill.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammTakerFill {
    pub block_number: BlockNumber,
    pub venue: String,
    pub fill_tx_hash: String,
    pub log_index: u32,
    /// The pool's immediate caller (`Swapped.sender`).
    pub sender: String,
    pub tx_from: String,
    pub tx_to: Option<String>,
    pub class: String,
    /// The reviewed label that set the class, if any.
    pub label_name: Option<String>,
}

/// Mean CEX markouts of one taker class. Standard errors treat fills as
/// independent, so they are lower bounds.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PropammTakerClassSummary {
    pub class: String,
    pub fills: u32,
    pub senders: u32,
    pub signers: u32,
    /// Taker-signed CEX move over the 12 s before the block (bp).
    pub pre_12s_move_bp: Option<f64>,
    /// Share of fills with a 12 s move above 0.5 bp that traded against it.
    pub against_move_share: Option<f64>,
    pub markout_0s_bp: Option<f64>,
    pub markout_0s_se: Option<f64>,
    pub markout_12s_bp: Option<f64>,
    pub markout_60s_bp: Option<f64>,
    pub markout_60s_se: Option<f64>,
    pub markout_300s_bp: Option<f64>,
    pub markout_300s_se: Option<f64>,
}

/// The reference price at the close of each period block. A block without a
/// swap carries the latest earlier close; `last_swap_block` says how stale.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReferenceBlockPrice {
    pub block_number: BlockNumber,
    pub close: Option<f64>,
    pub swaps: u32,
    pub last_swap_block: Option<BlockNumber>,
}

/// Builder-attribution aggregation. Builder is an execution-data signal until
/// a verified relay/builder join is available.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammBuilderSummary {
    pub builder: Option<String>,
    /// All canonical blocks this builder signal built in the period, whether
    /// or not they carry PropAMM activity; `None` when no period was supplied.
    #[serde(default)]
    pub period_blocks: Option<u32>,
    pub active_blocks: u32,
    pub updates: u32,
    pub fills: u32,
    pub paired_fills: u32,
    pub fills_without_update: u32,
}

/// What one PropAMM fill transaction paid the builder of its block: the
/// EIP-1559 priority fee, and ETH sent to the block's fee recipient inside
/// the transaction (direct transfers, including internal calls). Transfers
/// sent by the fee recipient itself are excluded, so a builder's payment to
/// the proposer is not counted. Amounts are in wei.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropammBuilderPayment {
    pub block_number: BlockNumber,
    pub tx_hash: String,
    pub tx_position: u32,
    pub builder: Option<String>,
    /// The block's `author` (fee recipient).
    pub fee_recipient: String,
    pub gas_used: u64,
    /// Type 2 and later: min(max priority fee, max fee - base fee); legacy:
    /// gas price - base fee.
    pub effective_priority_fee_per_gas: u64,
    pub priority_fee_wei: u128,
    pub direct_transfer_wei: u128,
    /// Number of transfers to the fee recipient counted in `direct_transfer_wei`.
    pub direct_transfers: u32,
    /// Swap logs in the transaction other than PropAMM fills (Uniswap V2,
    /// V3 and V4, Curve, Balancer V2). Zero means a single-swap transaction.
    pub other_swap_logs: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MempoolLifecycleStatus {
    Admitted,
    /// Inferred only from a later distinct hash with the same sender and nonce.
    DroppedBySender {
        replacement_hash: String,
    },
    PendingOrUnobserved,
}

/// Per-hash lifecycle measurement. Durations begin at first public observation;
/// they do not claim to observe a sender's private submission time.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MempoolLifecycle {
    pub hash: String,
    pub sender: String,
    pub nonce: u64,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub public_visibility_delay_ms: Option<u64>,
    pub admission_delay_ms: Option<u64>,
    pub observed_residence_ms: u64,
    pub admitted_block: Option<BlockNumber>,
    pub status: MempoolLifecycleStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceRow {
    pub hash: String,
    pub direct_payment_to_fee_recipient: Wei,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LabelRow {
    pub hash: String,
    pub chi_bps: u16,
    pub in_early_region: bool,
    pub public: bool,
    pub wait_seconds: Option<u64>,
    pub pos_share_bps: u16,
    pub tip_percentile_bps: u16,
    pub position_percentile_bps: u16,
    pub replaced: bool,
    pub arbitrage: bool,
    pub liquidation: bool,
    pub oracle: bool,
    pub bundle: bool,
    pub time_sensitive: bool,
    pub consumer_candidate: bool,
    pub provider_candidate: bool,
    pub delay_tolerant: bool,
    pub density: Wei,
}

/// The first time one relay received a winning block, from the relay data
/// API (`builder_blocks_received?block_hash=`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayBlockReceipt {
    pub block_number: BlockNumber,
    pub block_hash: String,
    pub relay: String,
    /// Earliest submission of this block hash, Unix milliseconds; `None`
    /// when the relay answered but never received the block.
    pub received_ms: Option<u64>,
    /// Set when every attempt to query the relay failed.
    pub error: Option<String>,
}

/// Whether a relay answered the probe before a receipt fetch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayStatus {
    pub relay: String,
    pub error: Option<String>,
}
