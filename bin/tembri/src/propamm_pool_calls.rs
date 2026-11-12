//! `call` frames into reviewed PropAMM pools, from Xatu traces, one stage per
//! day (`pool_calls.ndjson` next to the `propamm-report` stages).
//!
//! Separate from `propamm-report` on purpose: that report reads traces only
//! for internal registry updates, and its other stages must not change when
//! traces are fetched for revert counts.

use crate::propamm_project::columns;
use crate::propamm_report::{day_plan, load_settings, Settings};
use crate::{parquet_text, parquet_u64, read_ndjson, write_ndjson, xatu_reader};
use parquet::file::reader::FileReader;
use parquet::record::RowAccessor;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::ExitCode;
use tlm_model::{PropammEventRule, PropammPoolCall};

pub(crate) fn propamm_pool_calls_cmd(v: Vec<String>) -> ExitCode {
    let mut settings = match load_settings(&v, "propamm-pool-calls") {
        Ok(settings) => settings,
        Err(code) => return code,
    };
    let value = |name: &str| v.windows(2).find(|window| window[0] == name).map(|window| window[1].clone());
    match (value("--from"), value("--to")) {
        (Some(from), Some(to)) => match crate::propamm_period::date_range(&from, &to) {
            Ok(dates) => settings.dates = dates,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::from(2);
            }
        },
        (None, None) => {}
        _ => {
            eprintln!("Usage: tembri propamm-pool-calls --config config/propamm_reports.toml [--date YYYY-MM-DD | --from YYYY-MM-DD --to YYYY-MM-DD]");
            return ExitCode::from(2);
        }
    }
    let mut failed = false;
    for date in &settings.dates {
        match run_day(&settings, date) {
            Ok((path, rows)) => println!("{date}: {rows} pool calls -> {}", path.display()),
            Err(error) => {
                eprintln!("PropAMM pool calls for {date} failed: {error}");
                failed = true;
            }
        }
    }
    if failed { ExitCode::from(1) } else { ExitCode::SUCCESS }
}

fn run_day(settings: &Settings, date: &str) -> Result<(PathBuf, usize), String> {
    let (plan, _) = day_plan(settings, date)?;
    let blocks = plan.block_numbers.as_ref().ok_or("plan has no block selection")?;
    let (first, last) = (*blocks.first().ok_or("no blocks")?, *blocks.last().ok_or("no blocks")?);
    let rules: Vec<PropammEventRule> = read_ndjson(&settings.fill_rules)?;
    let pools: HashMap<String, String> = rules.iter().map(|rule| (rule.pool.to_ascii_lowercase(), rule.venue.clone())).collect();
    let mut rows = Vec::new();
    for partition in &plan.partitions {
        let traces = partition.join("traces.parquet");
        if !traces.is_file() {
            return Err(format!("{} is missing; fetch the traces dataset (tembri fetch-xatu --datasets traces)", traces.display()));
        }
        let failed = failed_transactions(&partition.join("transactions.parquet"))?;
        rows.extend(pool_calls(&traces, &pools, &failed, first, last)?);
    }
    rows.sort_by_key(|row: &PropammPoolCall| (row.block_number, row.tx_position));
    let path = settings.output_dir.join(date).join("pool_calls.ndjson");
    write_ndjson(&path, &rows)?;
    Ok((path, rows.len()))
}

/// A row projection of the named columns, so wide columns (call input) are
/// never decoded. Returns the projection and each name's index in it.
fn projection(
    reader: &parquet::file::reader::SerializedFileReader<std::fs::File>,
    path: &std::path::Path,
    names: &[&str],
) -> Result<(parquet::schema::types::Type, Vec<usize>), String> {
    columns(reader, path, names)?;
    let root = reader.metadata().file_metadata().schema();
    let fields: Vec<_> = root.get_fields().iter().filter(|field| names.contains(&field.name())).cloned().collect();
    let index = names
        .iter()
        .map(|name| fields.iter().position(|field| field.name() == *name).ok_or_else(|| format!("{} is missing column {name}", path.display())))
        .collect::<Result<Vec<_>, _>>()?;
    let projection = parquet::schema::types::Type::group_type_builder(root.name()).with_fields(fields).build().map_err(|e| e.to_string())?;
    Ok((projection, index))
}

fn failed_transactions(path: &PathBuf) -> Result<HashSet<String>, String> {
    let reader = xatu_reader(path)?;
    let (projection, index) = projection(&reader, path, &["transaction_hash", "success"])?;
    let mut failed = HashSet::new();
    for row in reader.get_row_iter(Some(projection)).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        if !row.get_bool(index[1]).map_err(|e| e.to_string())? {
            failed.insert(parquet_text(&row, index[0]).ok_or("transaction without hash")?);
        }
    }
    Ok(failed)
}

fn pool_calls(
    path: &PathBuf,
    pools: &HashMap<String, String>,
    failed: &HashSet<String>,
    first: u64,
    last: u64,
) -> Result<Vec<PropammPoolCall>, String> {
    let reader = xatu_reader(path)?;
    let names = ["block_number", "transaction_index", "transaction_hash", "action_from", "action_to", "action_call_type", "error", "result_output"];
    let (projection, index) = projection(&reader, path, &names)?;
    let [number, tx_index, tx_hash, from, to, call_type, error, output] = index[..] else {
        unreachable!("eight columns requested")
    };
    let mut rows = Vec::new();
    for row in reader.get_row_iter(Some(projection)).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let Some(venue) = parquet_text(&row, to).and_then(|target| pools.get(&target).map(|venue| (target, venue.clone()))) else {
            continue;
        };
        if parquet_text(&row, call_type).as_deref() != Some("call") {
            continue;
        }
        let block_number = parquet_u64(&row, number)?;
        if block_number < first || block_number > last {
            continue;
        }
        let hash = parquet_text(&row, tx_hash).ok_or("trace without transaction hash")?;
        let reverted = parquet_text(&row, error).is_some_and(|error| !error.is_empty());
        rows.push(PropammPoolCall {
            block_number,
            tx_position: u32::try_from(parquet_u64(&row, tx_index)?).map_err(|e| e.to_string())?,
            tx_succeeded: !failed.contains(&hash),
            tx_hash: hash,
            caller: parquet_text(&row, from).unwrap_or_default(),
            pool: venue.0,
            venue: venue.1,
            reverted,
            revert_reason: if reverted { parquet_text(&row, output).and_then(|data| tlm_classify::revert_reason(&data)) } else { None },
        });
    }
    Ok(rows)
}
