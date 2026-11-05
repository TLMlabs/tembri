//! What PropAMM fill transactions pay the builder of their block: the
//! EIP-1559 priority fee plus direct ETH transfers to the block's fee
//! recipient.

use std::collections::{BTreeMap, HashMap};
use tlm_model::PropammBuilderPayment;

/// Topic0 of swap events from other venues. A fill transaction with none of
/// these besides its PropAMM fill is a single-swap transaction.
pub const OTHER_SWAP_TOPICS: [&str; 5] = [
    // Uniswap V2 Swap
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
    // Uniswap V3 Swap
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    // Uniswap V4 Swap
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
    // Curve TokenExchange
    "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140",
    // Balancer V2 Swap
    "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
];

/// The block fields a payment needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentBlock {
    pub fee_recipient: String,
    pub base_fee_per_gas: u64,
    pub builder: Option<String>,
}

/// The fee fields of one fill transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentTx {
    pub block_number: u64,
    pub tx_hash: String,
    pub tx_position: u32,
    pub tx_type: u8,
    pub gas_used: u64,
    /// Xatu `gas_price`. Used only for legacy (type 0 and 1) transactions.
    pub gas_price: u64,
    pub max_priority_fee_per_gas: u64,
    pub max_fee_per_gas: u64,
}

/// One native ETH transfer inside a transaction, including internal calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTransfer {
    pub block_number: u64,
    pub tx_hash: String,
    pub from: String,
    pub to: String,
    pub value_wei: u128,
}

/// Effective priority fee per gas under EIP-1559. Type 2 and later use
/// min(max priority fee, max fee - base fee); earlier types use gas price -
/// base fee. Xatu's `gas_price` can equal the max fee for type 2, so it is
/// not used for those.
pub fn effective_priority_fee_per_gas(tx: &PaymentTx, base_fee: u64) -> u64 {
    if tx.tx_type >= 2 {
        tx.max_priority_fee_per_gas.min(tx.max_fee_per_gas.saturating_sub(base_fee))
    } else {
        tx.gas_price.saturating_sub(base_fee)
    }
}

/// One payment row per fill transaction. A transaction whose block is
/// missing from `blocks` is an error. Addresses and hashes are compared in
/// lowercase. `other_swap_logs` maps a transaction hash to its count of
/// `OTHER_SWAP_TOPICS` logs.
pub fn propamm_builder_payments(
    txs: &[PaymentTx],
    blocks: &HashMap<u64, PaymentBlock>,
    transfers: &[NativeTransfer],
    other_swap_logs: &HashMap<String, u32>,
) -> Result<Vec<PropammBuilderPayment>, String> {
    let mut direct: HashMap<String, (u128, u32)> = HashMap::new();
    for transfer in transfers {
        let Some(block) = blocks.get(&transfer.block_number) else {
            continue;
        };
        let recipient = block.fee_recipient.to_ascii_lowercase();
        if transfer.value_wei == 0
            || transfer.to.to_ascii_lowercase() != recipient
            || transfer.from.to_ascii_lowercase() == recipient
        {
            continue;
        }
        let entry = direct.entry(transfer.tx_hash.to_ascii_lowercase()).or_default();
        entry.0 = entry.0.checked_add(transfer.value_wei).ok_or("direct transfers overflow u128")?;
        entry.1 += 1;
    }
    let mut rows = BTreeMap::new();
    for tx in txs {
        let block = blocks
            .get(&tx.block_number)
            .ok_or_else(|| format!("fill transaction {} has no block {}", tx.tx_hash, tx.block_number))?;
        let hash = tx.tx_hash.to_ascii_lowercase();
        let tip = effective_priority_fee_per_gas(tx, block.base_fee_per_gas);
        let (direct_transfer_wei, direct_transfers) = direct.get(&hash).copied().unwrap_or_default();
        rows.insert(
            (tx.block_number, tx.tx_position, hash.clone()),
            PropammBuilderPayment {
                block_number: tx.block_number,
                tx_hash: hash.clone(),
                tx_position: tx.tx_position,
                builder: block.builder.clone(),
                fee_recipient: block.fee_recipient.to_ascii_lowercase(),
                gas_used: tx.gas_used,
                effective_priority_fee_per_gas: tip,
                priority_fee_wei: u128::from(tip) * u128::from(tx.gas_used),
                direct_transfer_wei,
                direct_transfers,
                other_swap_logs: other_swap_logs.get(&hash).copied().unwrap_or(0),
            },
        );
    }
    Ok(rows.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECIPIENT: &str = "0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97";

    fn tx(hash: &str, tx_type: u8) -> PaymentTx {
        PaymentTx {
            block_number: 10,
            tx_hash: hash.into(),
            tx_position: 3,
            tx_type,
            gas_used: 100_000,
            gas_price: 9_000_000_000,
            max_priority_fee_per_gas: 2_000_000_000,
            max_fee_per_gas: 9_000_000_000,
        }
    }

    fn blocks() -> HashMap<u64, PaymentBlock> {
        HashMap::from([(
            10,
            PaymentBlock { fee_recipient: RECIPIENT.to_uppercase().replace("0X", "0x"), base_fee_per_gas: 8_000_000_000, builder: Some("titan".into()) },
        )])
    }

    fn transfer(hash: &str, from: &str, to: &str, value_wei: u128) -> NativeTransfer {
        NativeTransfer { block_number: 10, tx_hash: hash.into(), from: from.into(), to: to.into(), value_wei }
    }

    #[test]
    fn type_two_tip_is_capped_by_max_fee_not_read_from_gas_price() {
        // max fee 9 gwei - base 8 gwei = 1 gwei, below the 2 gwei max priority fee.
        assert_eq!(effective_priority_fee_per_gas(&tx("0xa", 2), 8_000_000_000), 1_000_000_000);
        let mut legacy = tx("0xa", 0);
        legacy.gas_price = 8_500_000_000;
        assert_eq!(effective_priority_fee_per_gas(&legacy, 8_000_000_000), 500_000_000);
    }

    #[test]
    fn counts_transfers_to_the_fee_recipient_but_not_from_it() {
        let transfers = [
            transfer("0xA", "0xsearcher", RECIPIENT, 5),
            transfer("0xa", "0xrouter", &RECIPIENT.to_uppercase().replace("0X", "0x"), 7),
            transfer("0xa", RECIPIENT, "0xproposer", 1_000),
            transfer("0xa", "0xsearcher", "0xsomeone", 11),
            transfer("0xa", "0xsearcher", RECIPIENT, 0),
        ];
        let swaps = HashMap::from([("0xa".to_string(), 2)]);
        let rows = propamm_builder_payments(&[tx("0xA", 2), tx("0xb", 2)], &blocks(), &transfers, &swaps).unwrap();
        assert_eq!(rows.len(), 2);
        let a = rows.iter().find(|row| row.tx_hash == "0xa").unwrap();
        assert_eq!((a.direct_transfer_wei, a.direct_transfers, a.other_swap_logs), (12, 2, 2));
        assert_eq!(a.priority_fee_wei, 1_000_000_000u128 * 100_000);
        assert_eq!(a.fee_recipient, RECIPIENT);
        let b = rows.iter().find(|row| row.tx_hash == "0xb").unwrap();
        assert_eq!((b.direct_transfer_wei, b.direct_transfers, b.other_swap_logs), (0, 0, 0));
    }

    #[test]
    fn a_fill_without_its_block_is_an_error() {
        let mut orphan = tx("0xc", 2);
        orphan.block_number = 11;
        assert!(propamm_builder_payments(&[orphan], &blocks(), &[], &HashMap::new()).is_err());
    }
}
