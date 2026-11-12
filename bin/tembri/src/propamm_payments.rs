//! Read what PropAMM fill transactions paid their block's builder: fee fields
//! from Xatu transactions and blocks, direct ETH transfers to the block's fee
//! recipient from Xatu native transfers, and other swap logs in the same
//! transaction. The arithmetic is `tlm_classify::propamm_builder_payments`.

use crate::propamm_project::columns;
use crate::{builder_name, parquet_fee_u64, parquet_text, parquet_u64, xatu_reader};
use parquet::file::reader::FileReader;
use parquet::record::RowAccessor;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tlm_classify::{NativeTransfer, PaymentBlock, PaymentTx, OTHER_SWAP_TOPICS};
use tlm_model::PropammBuilderPayment;

/// Payments of the fill transactions `fill_txs` (lowercase hashes) in the
/// plan's blocks. `Ok(None)` when a partition has no `native_transfers.parquet`:
/// without it the direct transfers would be silently zero.
pub(crate) fn read_builder_payments(
    plan: &tlm_report::ReportPlan,
    fill_txs: &HashSet<String>,
) -> Result<Option<Vec<PropammBuilderPayment>>, String> {
    if plan.partitions.iter().any(|partition| !partition.join("native_transfers.parquet").is_file()) {
        return Ok(None);
    }
    let selected = |block: u64| plan.block_numbers.as_ref().is_none_or(|blocks| blocks.contains(&block));
    let mut blocks = HashMap::new();
    let mut txs = Vec::new();
    let mut transfers = Vec::new();
    let mut other_swaps = HashMap::new();
    for partition in &plan.partitions {
        blocks.extend(read_blocks(&partition.join("blocks.parquet"), &selected)?);
        txs.extend(read_fee_txs(&partition.join("transactions.parquet"), fill_txs, &selected)?);
        transfers.extend(read_transfers(&partition.join("native_transfers.parquet"), fill_txs, &selected)?);
        for (hash, count) in read_other_swaps(&partition.join("logs.parquet"), fill_txs, &selected)? {
            *other_swaps.entry(hash).or_insert(0) += count;
        }
    }
    let found: HashSet<&str> = txs.iter().map(|tx| tx.tx_hash.as_str()).collect();
    if let Some(missing) = fill_txs.iter().find(|hash| !found.contains(hash.as_str())) {
        return Err(format!("fill transaction {missing} is missing from Xatu transactions"));
    }
    tlm_classify::propamm_builder_payments(&txs, &blocks, &transfers, &other_swaps).map(Some)
}

fn read_blocks(path: &PathBuf, selected: &impl Fn(u64) -> bool) -> Result<HashMap<u64, PaymentBlock>, String> {
    let reader = xatu_reader(path)?;
    let [number, author, base_fee, extra_data] =
        columns(&reader, path, &["block_number", "author", "base_fee_per_gas", "extra_data"])?[..]
    else {
        unreachable!("four columns requested")
    };
    let mut blocks = HashMap::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let block = parquet_u64(&row, number)?;
        if !selected(block) {
            continue;
        }
        let base_fee_per_gas = parquet_u64(&row, base_fee)?;
        if base_fee_per_gas == 0 {
            return Err(format!("{}: block {block} has a zero base fee", path.display()));
        }
        blocks.insert(
            block,
            PaymentBlock {
                fee_recipient: parquet_text(&row, author).ok_or_else(|| format!("{}: block {block} has no author", path.display()))?,
                base_fee_per_gas,
                builder: parquet_text(&row, extra_data).map(builder_name),
            },
        );
    }
    Ok(blocks)
}

fn read_fee_txs(path: &PathBuf, wanted: &HashSet<String>, selected: &impl Fn(u64) -> bool) -> Result<Vec<PaymentTx>, String> {
    let reader = xatu_reader(path)?;
    let [number, index, hash, tx_type, gas_used, gas_price, priority, max_fee] = columns(
        &reader,
        path,
        &[
            "block_number",
            "transaction_index",
            "transaction_hash",
            "transaction_type",
            "gas_used",
            "gas_price",
            "max_priority_fee_per_gas",
            "max_fee_per_gas",
        ],
    )?[..]
    else {
        unreachable!("eight columns requested")
    };
    let mut txs = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let Some(tx_hash) = parquet_text(&row, hash) else { continue };
        if !wanted.contains(&tx_hash) {
            continue;
        }
        let block_number = parquet_u64(&row, number)?;
        if !selected(block_number) {
            continue;
        }
        let kind = u8::try_from(parquet_u64(&row, tx_type)?).map_err(|e| e.to_string())?;
        txs.push(PaymentTx {
            block_number,
            tx_position: u32::try_from(parquet_u64(&row, index)?).map_err(|e| e.to_string())?,
            tx_type: kind,
            gas_used: parquet_u64(&row, gas_used)?,
            gas_price: parquet_fee_u64(&row, gas_price)?,
            // Absent (zero) for legacy transactions, which use gas_price.
            max_priority_fee_per_gas: parquet_u64(&row, priority).unwrap_or(0),
            max_fee_per_gas: parquet_u64(&row, max_fee).unwrap_or(0),
            tx_hash,
        });
    }
    Ok(txs)
}

/// Xatu stores native-transfer values as 32 little-endian bytes.
fn le_wei(bytes: &[u8]) -> Result<u128, String> {
    if bytes.len() > 32 || bytes.iter().skip(16).any(|byte| *byte != 0) {
        return Err("native transfer value exceeds u128".into());
    }
    Ok(bytes.iter().take(16).enumerate().fold(0u128, |value, (index, byte)| value | (u128::from(*byte) << (8 * index))))
}

fn read_transfers(path: &PathBuf, wanted: &HashSet<String>, selected: &impl Fn(u64) -> bool) -> Result<Vec<NativeTransfer>, String> {
    let reader = xatu_reader(path)?;
    let [number, hash, from, to, value] =
        columns(&reader, path, &["block_number", "transaction_hash", "from_address", "to_address", "value"])?[..]
    else {
        unreachable!("five columns requested")
    };
    let mut transfers = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let Some(tx_hash) = parquet_text(&row, hash) else { continue };
        if !wanted.contains(&tx_hash) {
            continue;
        }
        let block_number = parquet_u64(&row, number)?;
        if !selected(block_number) {
            continue;
        }
        transfers.push(NativeTransfer {
            block_number,
            tx_hash,
            from: parquet_text(&row, from).unwrap_or_default(),
            to: parquet_text(&row, to).unwrap_or_default(),
            value_wei: le_wei(row.get_bytes(value).map_err(|e| e.to_string())?.data())?,
        });
    }
    Ok(transfers)
}

fn read_other_swaps(path: &PathBuf, wanted: &HashSet<String>, selected: &impl Fn(u64) -> bool) -> Result<HashMap<String, u32>, String> {
    let reader = xatu_reader(path)?;
    let [number, hash, topic0] = columns(&reader, path, &["block_number", "transaction_hash", "topic0"])?[..] else {
        unreachable!("three columns requested")
    };
    let mut counts = HashMap::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let Some(topic) = parquet_text(&row, topic0) else { continue };
        if !OTHER_SWAP_TOPICS.contains(&topic.as_str()) {
            continue;
        }
        let Some(tx_hash) = parquet_text(&row, hash) else { continue };
        if wanted.contains(&tx_hash) && selected(parquet_u64(&row, number)?) {
            *counts.entry(tx_hash).or_insert(0) += 1;
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::le_wei;

    #[test]
    fn reads_little_endian_values_and_rejects_values_above_u128() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x10;
        bytes[1] = 0x27;
        assert_eq!(le_wei(&bytes), Ok(10_000));
        bytes[20] = 1;
        assert!(le_wei(&bytes).is_err());
    }
}
