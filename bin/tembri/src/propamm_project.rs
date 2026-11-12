//! Project Xatu execution partitions into PropAMM decoder inputs.
//!
//! This is a pre-filter, not a decoder: it keeps logs from reviewed fill-rule
//! emitters and successful top-level calls to the reviewed registry, and adds
//! each transaction's position, cumulative gas before it, and the block's
//! builder signal. Venue and kind are assigned only by `propamm-decode`.
//!
//! Without traces only top-level registry transactions are visible. With
//! traces, internal `call` frames to the registry are added when neither the
//! frame nor any ancestor reverted. `static_call` frames are reads and
//! `delegate_call` frames run registry code in another contract's storage, so
//! neither is an update.

use crate::{builder_name, parquet_text, parquet_u64, xatu_plan_files, xatu_reader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use tlm_classify::{decode_reference_swap, REFERENCE_POOL};
use tlm_model::{PropammCall, PropammLog, PropammPeriodBlock, PropammTxEnvelope, ReferencePricePoint};

#[derive(Clone, Debug)]
pub(crate) struct XatuBlockHeader {
    pub gas_used: u64,
    pub builder: Option<String>,
    /// Unix seconds.
    pub timestamp: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct XatuTxRow {
    pub block_number: u64,
    pub index: u32,
    pub hash: String,
    pub from: Option<String>,
    pub to: Option<String>,
    /// `None` for transactions without the field (legacy type).
    pub max_priority_fee_per_gas: Option<u64>,
    /// Retained only for calls to the registry; other calldata is not needed.
    pub input: Option<String>,
    pub gas_used: u64,
    pub success: bool,
}

/// A trace frame. Only frames sent to the registry and frames that reverted
/// are retained: the latter are needed to tell whether an ancestor reverted.
#[derive(Clone, Debug)]
pub(crate) struct XatuTraceRow {
    pub block_number: u64,
    pub tx_index: u32,
    pub tx_hash: String,
    /// Path from the top-level frame; empty for the top-level frame itself.
    pub trace_address: Vec<u32>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub call_type: String,
    pub input: Option<String>,
    pub reverted: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct XatuLogRow {
    pub block_number: u64,
    pub tx_index: u32,
    pub tx_hash: String,
    pub log_index: u32,
    pub address: String,
    pub topic0: String,
    /// `topic1..topic3`, as present.
    pub topics: Vec<String>,
    pub data: Option<String>,
}

/// Project one partition's rows. Every block that contributes a row must have
/// a header and a complete transaction list (contiguous indexes whose gas sums
/// to the header's `gas_used`); otherwise cumulative gas would be wrong and the
/// projection fails rather than emitting a misleading position.
pub(crate) fn project_propamm_rows(
    headers: &HashMap<u64, XatuBlockHeader>,
    txs: &[XatuTxRow],
    logs: &[XatuLogRow],
    traces: Option<&[XatuTraceRow]>,
    registry: &str,
    emitters: &BTreeSet<String>,
) -> Result<(Vec<PropammLog>, Vec<PropammCall>), String> {
    let internal = match traces {
        Some(traces) => internal_registry_calls(txs, traces, registry)?,
        None => Vec::new(),
    };
    let mut by_block: BTreeMap<u64, Vec<&XatuTxRow>> = BTreeMap::new();
    for tx in txs {
        by_block.entry(tx.block_number).or_default().push(tx);
    }

    let relevant_logs: Vec<&XatuLogRow> = logs
        .iter()
        .filter(|log| emitters.contains(&log.address.to_ascii_lowercase()))
        .collect();
    let mut relevant_blocks: BTreeSet<u64> = relevant_logs.iter().map(|log| log.block_number).collect();
    relevant_blocks.extend(
        txs.iter()
            .filter(|tx| is_registry_call(tx, registry))
            .map(|tx| tx.block_number),
    );
    relevant_blocks.extend(internal.iter().map(|frame| frame.block_number));

    // (block, tx index) -> (tx row, cumulative gas before it)
    let mut positions: HashMap<(u64, u32), (&XatuTxRow, u64)> = HashMap::new();
    for block_number in &relevant_blocks {
        let header = headers
            .get(block_number)
            .ok_or_else(|| format!("block {block_number} has PropAMM rows but no block header"))?;
        let mut block_txs = by_block.remove(block_number).unwrap_or_default();
        block_txs.sort_by_key(|tx| tx.index);
        let mut cumulative = 0u64;
        for (expected, tx) in block_txs.iter().enumerate() {
            if u32::try_from(expected).ok() != Some(tx.index) {
                return Err(format!(
                    "block {block_number} transactions are not contiguous at index {expected}"
                ));
            }
            positions.insert((*block_number, tx.index), (tx, cumulative));
            cumulative = cumulative
                .checked_add(tx.gas_used)
                .ok_or_else(|| format!("block {block_number} cumulative gas overflow"))?;
        }
        if cumulative != header.gas_used {
            return Err(format!(
                "block {block_number} transaction gas {cumulative} does not match block gas_used {}",
                header.gas_used
            ));
        }
    }

    let mut calls = Vec::new();
    for tx in txs.iter().filter(|tx| is_registry_call(tx, registry)) {
        let (_, cumulative_gas_before) = positions[&(tx.block_number, tx.index)];
        calls.push(PropammCall {
            block_number: tx.block_number,
            to: registry.to_ascii_lowercase(),
            tx_hash: tx.hash.clone(),
            tx_position: tx.index,
            cumulative_gas_before,
            input: tx.input.clone().unwrap_or_else(|| "0x".into()),
            builder: headers[&tx.block_number].builder.clone(),
            trace_address: None,
            from: None,
        });
    }
    for frame in internal {
        let (tx, cumulative_gas_before) = positions
            .get(&(frame.block_number, frame.tx_index))
            .ok_or_else(|| {
                format!(
                    "trace in block {} has no transaction at index {}",
                    frame.block_number, frame.tx_index
                )
            })?;
        if !tx.hash.eq_ignore_ascii_case(&frame.tx_hash) {
            return Err(format!(
                "trace in block {} names {} but transaction index {} is {}",
                frame.block_number, frame.tx_hash, frame.tx_index, tx.hash
            ));
        }
        calls.push(PropammCall {
            block_number: frame.block_number,
            to: registry.to_ascii_lowercase(),
            tx_hash: frame.tx_hash.clone(),
            tx_position: frame.tx_index,
            cumulative_gas_before: *cumulative_gas_before,
            input: frame.input.clone().unwrap_or_else(|| "0x".into()),
            builder: headers[&frame.block_number].builder.clone(),
            trace_address: Some(
                frame
                    .trace_address
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join("_"),
            ),
            from: frame.from.clone(),
        });
    }

    let mut projected_logs = Vec::new();
    for log in relevant_logs {
        let (tx, cumulative_gas_before) = positions
            .get(&(log.block_number, log.tx_index))
            .ok_or_else(|| {
                format!(
                    "log {} in block {} has no transaction at index {}",
                    log.log_index, log.block_number, log.tx_index
                )
            })?;
        if !tx.hash.eq_ignore_ascii_case(&log.tx_hash) {
            return Err(format!(
                "log {} in block {} names {} but transaction index {} is {}",
                log.log_index, log.block_number, log.tx_hash, log.tx_index, tx.hash
            ));
        }
        if !tx.success {
            return Err(format!(
                "log {} in block {} belongs to a failed transaction",
                log.log_index, log.block_number
            ));
        }
        projected_logs.push(PropammLog {
            block_number: log.block_number,
            emitter: log.address.to_ascii_lowercase(),
            tx_hash: log.tx_hash.clone(),
            tx_position: log.tx_index,
            cumulative_gas_before: *cumulative_gas_before,
            log_index: log.log_index,
            topic0: log.topic0.to_ascii_lowercase(),
            builder: headers[&log.block_number].builder.clone(),
            topics: log.topics.clone(),
            data: log.data.clone(),
        });
    }
    Ok((projected_logs, calls))
}

/// Internal registry `call` frames whose state change persisted.
///
/// Also checks that every top-level registry call has its top-level frame in
/// the traces, so an incomplete traces partition fails instead of silently
/// dropping internal updates.
fn internal_registry_calls<'a>(
    txs: &[XatuTxRow],
    traces: &'a [XatuTraceRow],
    registry: &str,
) -> Result<Vec<&'a XatuTraceRow>, String> {
    let mut reverted: HashMap<&str, Vec<&[u32]>> = HashMap::new();
    let mut roots: BTreeSet<String> = BTreeSet::new();
    for frame in traces {
        if frame.reverted {
            reverted
                .entry(frame.tx_hash.as_str())
                .or_default()
                .push(&frame.trace_address);
        }
        if frame.trace_address.is_empty() && to_registry(frame.to.as_deref(), registry) {
            roots.insert(frame.tx_hash.to_ascii_lowercase());
        }
    }
    if let Some(missing) = txs
        .iter()
        .filter(|tx| is_registry_call(tx, registry))
        .find(|tx| !roots.contains(&tx.hash.to_ascii_lowercase()))
    {
        return Err(format!(
            "traces lack the top-level frame of registry transaction {}",
            missing.hash
        ));
    }
    Ok(traces
        .iter()
        .filter(|frame| !frame.trace_address.is_empty())
        .filter(|frame| frame.call_type == "call" && to_registry(frame.to.as_deref(), registry))
        .filter(|frame| {
            !reverted.get(frame.tx_hash.as_str()).is_some_and(|paths| {
                paths
                    .iter()
                    .any(|path| frame.trace_address.starts_with(path))
            })
        })
        .collect())
}

fn to_registry(to: Option<&str>, registry: &str) -> bool {
    to.is_some_and(|to| to.eq_ignore_ascii_case(registry))
}

/// A reverted registry call did not change state, so it is not an update.
fn is_registry_call(tx: &XatuTxRow, registry: &str) -> bool {
    tx.success && to_registry(tx.to.as_deref(), registry)
}

/// Decoder inputs, every period block's builder signal, the reference
/// pool's price points (empty unless requested), and the envelope of every
/// transaction with a reviewed PropAMM log.
pub(crate) type ProjectedPlan = (
    Vec<PropammLog>,
    Vec<PropammCall>,
    Vec<PropammPeriodBlock>,
    Vec<ReferencePricePoint>,
    Vec<PropammTxEnvelope>,
);

/// Read and project every partition in the plan, restricted to its blocks.
pub(crate) fn project_xatu_plan(
    plan: &tlm_report::ReportPlan,
    registry: &str,
    emitters: &BTreeSet<String>,
    with_traces: bool,
    with_reference: bool,
) -> Result<ProjectedPlan, String> {
    let mut read_emitters = emitters.clone();
    if with_reference {
        read_emitters.insert(REFERENCE_POOL.to_string());
    }
    let mut reference = Vec::new();
    let mut all_logs = Vec::new();
    let mut all_calls = Vec::new();
    let mut period_blocks = Vec::new();
    let mut envelopes = Vec::new();
    for blocks_path in xatu_plan_files(plan, "blocks")? {
        let partition = blocks_path
            .parent()
            .ok_or("blocks parquet has no parent directory")?;
        let selected = |block: u64| {
            plan.block_numbers
                .as_ref()
                .is_none_or(|blocks| blocks.contains(&block))
        };
        let headers = read_headers(&blocks_path, &selected)?;
        period_blocks.extend(headers.iter().map(|(block_number, header)| PropammPeriodBlock {
            block_number: *block_number,
            builder: header.builder.clone(),
            timestamp: Some(header.timestamp),
        }));
        let txs = read_txs(&partition.join("transactions.parquet"), registry, &selected)?;
        let logs = read_logs(&partition.join("logs.parquet"), &read_emitters, &selected)?;
        if with_reference {
            for log in logs.iter().filter(|log| log.address == REFERENCE_POOL) {
                let point = decode_reference_swap(
                    log.block_number,
                    log.tx_index,
                    log.log_index,
                    &log.topic0,
                    log.data.as_deref().unwrap_or("0x"),
                )
                .map_err(|error| format!("{}: {error}", partition.display()))?;
                reference.extend(point);
            }
        }
        let log_txs: BTreeSet<&str> = logs
            .iter()
            .filter(|log| emitters.contains(&log.address.to_ascii_lowercase()))
            .map(|log| log.tx_hash.as_str())
            .collect();
        for tx in txs.iter().filter(|tx| log_txs.contains(tx.hash.as_str())) {
            envelopes.push(PropammTxEnvelope {
                block_number: tx.block_number,
                tx_hash: tx.hash.clone(),
                tx_position: tx.index,
                from: tx.from.clone().ok_or_else(|| format!("transaction {} has no sender", tx.hash))?,
                to: tx.to.clone(),
                max_priority_fee_per_gas: tx.max_priority_fee_per_gas,
            });
        }
        let traces = if with_traces {
            Some(read_traces(&partition.join("traces.parquet"), registry, &selected)?)
        } else {
            None
        };
        let (logs, calls) =
            project_propamm_rows(&headers, &txs, &logs, traces.as_deref(), registry, emitters)
            .map_err(|error| format!("{}: {error}", partition.display()))?;
        all_logs.extend(logs);
        all_calls.extend(calls);
    }
    all_logs.sort_by_key(|log| (log.block_number, log.tx_position, log.log_index));
    all_calls.sort_by(|left, right| {
        (left.block_number, left.tx_position, &left.trace_address)
            .cmp(&(right.block_number, right.tx_position, &right.trace_address))
    });
    period_blocks.sort_by_key(|block| block.block_number);
    reference.sort_by_key(|point| (point.block_number, point.tx_position, point.log_index));
    envelopes.sort_by_key(|tx| (tx.block_number, tx.tx_position));
    Ok((all_logs, all_calls, period_blocks, reference, envelopes))
}

/// Logs emitted by `addresses` in the plan's blocks, across its partitions.
pub(crate) fn read_plan_logs(
    plan: &tlm_report::ReportPlan,
    addresses: &BTreeSet<String>,
) -> Result<Vec<XatuLogRow>, String> {
    let selected = |block: u64| plan.block_numbers.as_ref().is_none_or(|blocks| blocks.contains(&block));
    let mut logs = Vec::new();
    for path in xatu_plan_files(plan, "logs")? {
        logs.extend(read_logs(&path, addresses, &selected)?);
    }
    logs.sort_by_key(|log| (log.block_number, log.tx_index, log.log_index));
    Ok(logs)
}

/// Number, Unix-second timestamp and hash of every block in a Xatu blocks
/// file.
pub(crate) fn read_block_hashes(path: &PathBuf) -> Result<Vec<(u64, u64, String)>, String> {
    let reader = xatu_reader(path)?;
    let [number, date_time, hash] = columns(&reader, path, &["block_number", "block_date_time", "block_hash"])?[..] else {
        unreachable!("three columns requested")
    };
    let mut blocks = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let block_hash = parquet_text(&row, hash).ok_or_else(|| format!("{}: block without hash", path.display()))?;
        blocks.push((parquet_u64(&row, number)?, block_timestamp(&row, date_time)?, block_hash));
    }
    blocks.sort();
    Ok(blocks)
}

/// Resolve Xatu columns by name: physical positions differ between schema
/// versions (see the base-fee fix of 2026-10-29).
pub(crate) fn columns(reader: &SerializedFileReader<File>, path: &Path, names: &[&str]) -> Result<Vec<usize>, String> {
    let schema = reader.metadata().file_metadata().schema_descr();
    names
        .iter()
        .map(|name| {
            schema
                .columns()
                .iter()
                .position(|column| column.name() == *name)
                .ok_or_else(|| format!("{} is missing column {name}", path.display()))
        })
        .collect()
}

fn read_headers(
    path: &PathBuf,
    selected: &impl Fn(u64) -> bool,
) -> Result<HashMap<u64, XatuBlockHeader>, String> {
    let reader = xatu_reader(path)?;
    let [number, gas_used, extra_data, date_time] =
        columns(&reader, path, &["block_number", "gas_used", "extra_data", "block_date_time"])?[..]
    else {
        unreachable!("four columns requested")
    };
    let mut headers = HashMap::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let block = parquet_u64(&row, number)?;
        if selected(block) {
            headers.insert(
                block,
                XatuBlockHeader {
                    gas_used: parquet_u64(&row, gas_used)?,
                    builder: parquet_text(&row, extra_data).map(builder_name),
                    timestamp: block_timestamp(&row, date_time)?,
                },
            );
        }
    }
    Ok(headers)
}

/// Xatu stores `block_date_time` as a UTC millisecond timestamp; Ethereum
/// block times are whole seconds.
fn block_timestamp(row: &parquet::record::Row, col: usize) -> Result<u64, String> {
    let millis = row.get_timestamp_millis(col).map_err(|e| format!("block_date_time: {e}"))?;
    u64::try_from(millis / 1000).map_err(|_| "block_date_time is before 1970".into())
}

fn read_txs(
    path: &PathBuf,
    registry: &str,
    selected: &impl Fn(u64) -> bool,
) -> Result<Vec<XatuTxRow>, String> {
    let reader = xatu_reader(path)?;
    let [number, index, hash, from, to, priority_fee, input, gas_used, success] = columns(
        &reader,
        path,
        &[
            "block_number",
            "transaction_index",
            "transaction_hash",
            "from_address",
            "to_address",
            "max_priority_fee_per_gas",
            "input",
            "gas_used",
            "success",
        ],
    )?[..]
    else {
        unreachable!("nine columns requested")
    };
    let mut txs = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let block_number = parquet_u64(&row, number)?;
        if !selected(block_number) {
            continue;
        }
        let to = parquet_text(&row, to);
        let is_registry = to.as_deref().is_some_and(|to| to.eq_ignore_ascii_case(registry));
        txs.push(XatuTxRow {
            block_number,
            index: u32::try_from(parquet_u64(&row, index)?).map_err(|e| e.to_string())?,
            hash: parquet_text(&row, hash).ok_or("transaction without hash")?,
            from: parquet_text(&row, from),
            max_priority_fee_per_gas: parquet_u64(&row, priority_fee).ok(),
            input: if is_registry { parquet_text(&row, input) } else { None },
            to,
            gas_used: parquet_u64(&row, gas_used)?,
            success: row.get_bool(success).map_err(|e| e.to_string())?,
        });
    }
    Ok(txs)
}

fn read_traces(
    path: &PathBuf,
    registry: &str,
    selected: &impl Fn(u64) -> bool,
) -> Result<Vec<XatuTraceRow>, String> {
    if !path.is_file() {
        return Err(format!("{} is missing; fetch the traces dataset", path.display()));
    }
    let reader = xatu_reader(path)?;
    let [number, tx_index, tx_hash, from, to, input, call_type, trace_address, error] = columns(
        &reader,
        path,
        &[
            "block_number",
            "transaction_index",
            "transaction_hash",
            "action_from",
            "action_to",
            "action_input",
            "action_call_type",
            "trace_address",
            "error",
        ],
    )?[..]
    else {
        unreachable!("nine columns requested")
    };
    let mut frames = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let target = parquet_text(&row, to);
        let is_registry = to_registry(target.as_deref(), registry);
        let reverted = parquet_text(&row, error).is_some_and(|error| !error.is_empty());
        if !is_registry && !reverted {
            continue;
        }
        let block_number = parquet_u64(&row, number)?;
        if !selected(block_number) {
            continue;
        }
        frames.push(XatuTraceRow {
            block_number,
            tx_index: u32::try_from(parquet_u64(&row, tx_index)?).map_err(|e| e.to_string())?,
            tx_hash: parquet_text(&row, tx_hash).ok_or("trace without transaction hash")?,
            trace_address: parse_trace_address(parquet_text(&row, trace_address).as_deref())?,
            from: parquet_text(&row, from).map(|caller| caller.to_ascii_lowercase()),
            to: target,
            call_type: parquet_text(&row, call_type).unwrap_or_default(),
            input: if is_registry { parquet_text(&row, input) } else { None },
            reverted,
        });
    }
    Ok(frames)
}

/// Xatu writes a frame path as `0_1_2`; the top-level frame has no path.
fn parse_trace_address(value: Option<&str>) -> Result<Vec<u32>, String> {
    match value {
        None | Some("") => Ok(Vec::new()),
        Some(path) => path
            .split('_')
            .map(|part| part.parse().map_err(|_| format!("invalid trace address {path}")))
            .collect(),
    }
}

fn read_logs(
    path: &PathBuf,
    emitters: &BTreeSet<String>,
    selected: &impl Fn(u64) -> bool,
) -> Result<Vec<XatuLogRow>, String> {
    let reader = xatu_reader(path)?;
    let [number, tx_index, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data] = columns(
        &reader,
        path,
        &[
            "block_number",
            "transaction_index",
            "transaction_hash",
            "log_index",
            "address",
            "topic0",
            "topic1",
            "topic2",
            "topic3",
            "data",
        ],
    )?[..]
    else {
        unreachable!("ten columns requested")
    };
    let mut logs = Vec::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let Some(emitter) = parquet_text(&row, address) else {
            continue;
        };
        if !emitters.contains(&emitter) {
            continue;
        }
        let block_number = parquet_u64(&row, number)?;
        if !selected(block_number) {
            continue;
        }
        logs.push(XatuLogRow {
            block_number,
            tx_index: u32::try_from(parquet_u64(&row, tx_index)?).map_err(|e| e.to_string())?,
            tx_hash: parquet_text(&row, tx_hash).ok_or("log without transaction hash")?,
            log_index: u32::try_from(parquet_u64(&row, log_index)?).map_err(|e| e.to_string())?,
            address: emitter,
            topic0: parquet_text(&row, topic0).unwrap_or_default(),
            topics: [topic1, topic2, topic3]
                .into_iter()
                .map_while(|column| parquet_text(&row, column))
                .collect(),
            data: parquet_text(&row, data),
        });
    }
    Ok(logs)
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTRY: &str = "0xda7afeed021eafc1c1af9c362de477dad0396b81";
    const FERMI: &str = "0x5979458912f80b96d30d4220af8e2e4925a33320";

    fn tx(block_number: u64, index: u32, to: &str, gas_used: u64, success: bool) -> XatuTxRow {
        XatuTxRow {
            block_number,
            index,
            hash: format!("0x{block_number}{index}"),
            from: None,
            to: Some(to.into()),
            max_priority_fee_per_gas: None,
            input: Some("0xa9114b0f".into()),
            gas_used,
            success,
        }
    }

    fn log(block_number: u64, tx_index: u32, address: &str) -> XatuLogRow {
        XatuLogRow {
            block_number,
            tx_index,
            tx_hash: format!("0x{block_number}{tx_index}"),
            log_index: 7,
            address: address.into(),
            topic0: "0xTOPIC".into(),
            topics: Vec::new(),
            data: None,
        }
    }

    fn headers(gas_used: u64) -> HashMap<u64, XatuBlockHeader> {
        HashMap::from([(
            10,
            XatuBlockHeader {
                gas_used,
                builder: Some("titan".into()),
                timestamp: 0,
            },
        )])
    }

    fn emitters() -> BTreeSet<String> {
        BTreeSet::from([FERMI.to_string()])
    }

    #[test]
    fn projects_registry_calls_and_reviewed_emitter_logs_with_cumulative_gas() {
        let txs = [
            tx(10, 0, "0xother", 100, true),
            tx(10, 1, &REGISTRY.to_ascii_uppercase(), 30, true),
            tx(10, 2, "0xrouter", 50, true),
        ];
        let logs = [log(10, 2, &FERMI.to_ascii_uppercase()), log(10, 0, "0xunreviewed")];
        let (logs, calls) =
            project_propamm_rows(&headers(180), &txs, &logs, None, REGISTRY, &emitters()).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].tx_position, 1);
        assert_eq!(calls[0].cumulative_gas_before, 100);
        assert_eq!(calls[0].builder.as_deref(), Some("titan"));
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].emitter, FERMI);
        assert_eq!(logs[0].topic0, "0xtopic");
        assert_eq!(logs[0].cumulative_gas_before, 130);
    }

    fn frame(index: u32, path: &[u32], to: &str, call_type: &str, reverted: bool) -> XatuTraceRow {
        XatuTraceRow {
            block_number: 10,
            tx_index: index,
            tx_hash: format!("0x10{index}"),
            trace_address: path.to_vec(),
            from: Some("0xcaller".into()),
            to: Some(to.into()),
            call_type: call_type.into(),
            input: Some("0xa9114b0f".into()),
            reverted,
        }
    }

    #[test]
    fn projects_persisted_internal_registry_calls_only() {
        let txs = [
            tx(10, 0, REGISTRY, 30, true),
            tx(10, 1, "0xmaker", 50, true),
            tx(10, 2, "0xrouter", 70, true),
        ];
        let traces = [
            frame(0, &[], REGISTRY, "call", false),
            frame(1, &[0], REGISTRY, "call", false),
            frame(1, &[1], REGISTRY, "static_call", false),
            frame(1, &[2], REGISTRY, "delegate_call", false),
            frame(2, &[0], "0xpool", "call", true),
            frame(2, &[0, 3], REGISTRY, "call", false),
            frame(2, &[1, 0], REGISTRY, "call", false),
        ];
        let (_, calls) =
            project_propamm_rows(&headers(150), &txs, &[], Some(&traces), REGISTRY, &emitters())
                .unwrap();
        let found: Vec<_> = calls
            .iter()
            .map(|call| (call.tx_position, call.trace_address.as_deref(), call.cumulative_gas_before))
            .collect();
        assert_eq!(
            found,
            [(0, None, 0), (1, Some("0"), 30), (2, Some("1_0"), 80)]
        );
    }

    #[test]
    fn fails_closed_when_traces_lack_a_top_level_registry_call() {
        let txs = [tx(10, 0, REGISTRY, 30, true)];
        assert!(
            project_propamm_rows(&headers(30), &txs, &[], Some(&[]), REGISTRY, &emitters())
                .unwrap_err()
                .contains("lack the top-level frame")
        );
        assert_eq!(parse_trace_address(Some("0_12_3")).unwrap(), [0, 12, 3]);
        assert!(parse_trace_address(None).unwrap().is_empty());
    }

    #[test]
    fn drops_reverted_registry_calls() {
        let txs = [tx(10, 0, REGISTRY, 30, false)];
        let (_, calls) = project_propamm_rows(&headers(30), &txs, &[], None, REGISTRY, &emitters()).unwrap();
        assert!(calls.is_empty());
    }

    #[test]
    fn fails_closed_on_incomplete_block_transactions() {
        let gap = [tx(10, 0, "0xother", 100, true), tx(10, 2, REGISTRY, 30, true)];
        assert!(project_propamm_rows(&headers(130), &gap, &[], None, REGISTRY, &emitters())
            .unwrap_err()
            .contains("not contiguous"));

        let short = [tx(10, 0, REGISTRY, 30, true)];
        assert!(project_propamm_rows(&headers(999), &short, &[], None, REGISTRY, &emitters())
            .unwrap_err()
            .contains("does not match block gas_used"));

        let orphan = [log(10, 0, FERMI)];
        assert!(project_propamm_rows(&headers(0), &[], &orphan, None, REGISTRY, &emitters())
            .unwrap_err()
            .contains("no transaction"));
    }
}
