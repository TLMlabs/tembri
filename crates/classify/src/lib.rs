//! Deterministic transaction labels. Protocol-specific signature lists belong in config.

pub mod activity;
pub mod cex_price;
pub mod fee_fingerprint;
pub mod lifecycle;
pub mod markout;
pub mod payment;
pub mod position;
pub mod propamm;
pub mod reference_price;
pub mod route;
pub mod taker;
pub mod uniswap;
pub use activity::{
    ActivityFilter, ClassifiedActivity, CompositionCounts, MempoolVisibility, PublicMempoolIndex,
    TipBucket,
};
pub use fee_fingerprint::{fingerprint_fee_fields, FeeFingerprint, FeeFingerprintConfig};
pub use lifecycle::measure_mempool_lifecycle;
pub use cex_price::{cex_markout_at, propamm_cex_markouts, CexSeries, CEX_HORIZONS_SECONDS, CEX_MAX_AGE_SECONDS};
pub use markout::propamm_markouts;
pub use position::{
    propamm_early_place_value, propamm_position_tests, propamm_shared_updates, EarlyPlaceValue, OrderTest, PositionTests, SharedUpdates,
};
pub use payment::{effective_priority_fee_per_gas, propamm_builder_payments, NativeTransfer, PaymentBlock, PaymentTx, OTHER_SWAP_TOPICS};
pub use route::{classify_propamm_routes, find_propamm_sandwiches};
pub use uniswap::{uniswap_v3_fill, UniswapV3Pool};
pub use taker::{classify_propamm_takers, summarize_taker_markouts, TAKER_CLASS_RANK};
pub use reference_price::{
    decode_reference_swap, reference_block_closes, reference_mid_before, REFERENCE_POOL,
    UNISWAP_V3_SWAP_TOPIC,
};
pub use propamm::{
    add_period_blocks, add_quote_ages,
    decode_propamm_activity, decode_propamm_log, decode_registry_batch_updates,
    decode_registry_direct_update, decode_swapped_terms, group_runtime_bytecode, pair_propamm_events,
    summarize_propamm_blocks, summarize_propamm_builders, REGISTRY_BATCH_UPDATE_SELECTOR,
    REGISTRY_UPDATE_STATE_SELECTOR, SWAPPED_TOPIC,
};

use tlm_model::{Block, LabelConfig, MempoolSeen, Transaction, TxLabels};

pub fn classify(
    block: &Block,
    tx: &Transaction,
    seen: Option<&MempoolSeen>,
    median_tip: u128,
    config: &LabelConfig,
    known_bundle_member: bool,
) -> TxLabels {
    let public = seen.and_then(|s| s.first_seen).is_some();
    let wait_seconds = seen
        .and_then(|s| s.first_seen)
        .map(|first| block.timestamp.saturating_sub(first));
    let liquidation = tx.selector.as_ref().is_some_and(|s| {
        config
            .liquidation_selectors
            .iter()
            .any(|known| known.eq_ignore_ascii_case(s))
    });
    // A complete arbitrage detector requires decoded swap logs; this conservative v0 rule
    // lets ingestion supply a bundle flag and avoids claiming arbitrary swaps are arbitrage.
    let arbitrage = false;
    let oracle = false;
    let time_sensitive = !public || arbitrage || liquidation || oracle || known_bundle_member;
    let in_early_region = tx.is_in_early_region(block, config.chi_bps);
    let density = tx.density();
    let floor = tlm_model::bps_mul(block.base_fee, config.beta_bps);
    let consumer_candidate = in_early_region && density >= floor && time_sensitive;
    let provider_candidate =
        public && !in_early_region && tx.effective_priority_fee <= median_tip && !time_sensitive;
    let delay_tolerant = provider_candidate
        && wait_seconds.is_some_and(|wait| wait >= config.slot_seconds)
        && tx.max_fee_per_gas >= block.base_fee;
    TxLabels {
        in_early_region,
        public,
        wait_seconds,
        arbitrage,
        liquidation,
        oracle,
        bundle: known_bundle_member,
        time_sensitive,
        consumer_candidate,
        provider_candidate,
        delay_tolerant,
        density,
    }
}

/// Economic activity shown in tip and position reports. Candidate labels are conservative.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActivityType {
    Liquidation,
    MultiPoolArbitrageCandidate,
    DefiTrade,
    OracleUpdate,
    TokenTransfer,
    NativeTransfer,
    ContractDeployment,
    OtherContractInteraction,
}

impl ActivityType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Liquidation => "liquidation",
            Self::MultiPoolArbitrageCandidate => "multi-pool arbitrage candidate",
            Self::DefiTrade => "DeFi trade (single-pool swap)",
            Self::OracleUpdate => "oracle update",
            Self::TokenTransfer => "token transfer / payment",
            Self::NativeTransfer => "native transfer / empty calldata",
            Self::ContractDeployment => "contract deployment",
            Self::OtherContractInteraction => "other contract interaction",
        }
    }
}

/// Evidence extracted from logs and the transaction envelope before classification.
/// distinct_swap_pools must count log-emitter addresses, not topic occurrences.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityEvidence {
    pub to: Option<String>,
    pub has_calldata: bool,
    pub erc20_transfer_events: u32,
    pub known_swap_events: u32,
    pub distinct_swap_pools: u32,
    pub liquidation_event: bool,
    pub oracle_event: bool,
}

pub fn classify_activity(
    selector: Option<&str>,
    evidence: &ActivityEvidence,
    config: &LabelConfig,
) -> ActivityType {
    if evidence.liquidation_event
        || selector.is_some_and(|s| {
            config
                .liquidation_selectors
                .iter()
                .any(|known| known.eq_ignore_ascii_case(s))
        })
    {
        return ActivityType::Liquidation;
    }
    if evidence.oracle_event
        || evidence.to.as_ref().is_some_and(|to| {
            config
                .oracle_addresses
                .iter()
                .any(|known| known.eq_ignore_ascii_case(to))
        })
    {
        return ActivityType::OracleUpdate;
    }
    if evidence.distinct_swap_pools >= 2 {
        return ActivityType::MultiPoolArbitrageCandidate;
    }
    if evidence.known_swap_events > 0 {
        return ActivityType::DefiTrade;
    }
    if evidence.erc20_transfer_events > 0 {
        return ActivityType::TokenTransfer;
    }
    if evidence.to.is_none() {
        return ActivityType::ContractDeployment;
    }
    if !evidence.has_calldata {
        return ActivityType::NativeTransfer;
    }
    ActivityType::OtherContractInteraction
}

#[cfg(test)]
mod activity_tests {
    use super::*;
    #[test]
    fn multi_pool_requires_distinct_emitters() {
        let e = ActivityEvidence {
            known_swap_events: 2,
            distinct_swap_pools: 1,
            ..Default::default()
        };
        assert_eq!(
            classify_activity(None, &e, &LabelConfig::default()),
            ActivityType::DefiTrade
        );
        let e = ActivityEvidence {
            known_swap_events: 2,
            distinct_swap_pools: 2,
            ..Default::default()
        };
        assert_eq!(
            classify_activity(None, &e, &LabelConfig::default()),
            ActivityType::MultiPoolArbitrageCandidate
        );
    }
    #[test]
    fn transfer_beats_generic_contract_call() {
        let e = ActivityEvidence {
            to: Some("0x1".into()),
            has_calldata: true,
            erc20_transfer_events: 1,
            ..Default::default()
        };
        assert_eq!(
            classify_activity(Some("0xa9059cbb"), &e, &LabelConfig::default()),
            ActivityType::TokenTransfer
        );
    }
}
