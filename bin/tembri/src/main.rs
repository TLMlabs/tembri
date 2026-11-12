mod propamm_project;
mod propamm_fetch;
mod propamm_payments;
mod propamm_period;
mod propamm_pool_calls;
mod propamm_report;

use parquet::{
    basic::Type as ParquetPhysicalType,
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Command, ExitCode},
};

fn main() -> ExitCode {
    let mut a = std::env::args();
    let _ = a.next();
    match a.next().as_deref() {
        Some("fetch") => fetch(a.collect()),
        Some("fetch-cryo") => fetch_cryo(a.collect()),
        Some("fetch-xatu") => fetch_xatu(a.collect()),
        Some("classify") => classify(a.collect()),
        Some("metrics") => metrics(a.collect()),
        Some("propamm-bytecode-group") => propamm_bytecode_group(a.collect()),
        Some("propamm-project") => propamm_project_cmd(a.collect()),
        Some("propamm-decode") => propamm_decode(a.collect()),
        Some("propamm-pair") => propamm_pair(a.collect()),
        Some("propamm-markouts") => propamm_markouts(a.collect()),
        Some("propamm-cex-markouts") => propamm_cex_markouts(a.collect()),
        Some("propamm-takers") => propamm_takers(a.collect()),
        Some("propamm-routes") => propamm_routes(a.collect()),
        Some("propamm-fetch") => propamm_fetch::propamm_fetch_cmd(a.collect()),
        Some("propamm-report") => propamm_report::propamm_report_cmd(a.collect()),
        Some("propamm-period") => propamm_period::propamm_period_cmd(a.collect()),
        Some("propamm-pool-calls") => propamm_pool_calls::propamm_pool_calls_cmd(a.collect()),
        Some("mempool-lifecycle") => mempool_lifecycle(a.collect()),
        Some("run-period") => run_period(a.collect()),
        Some("report") => report(a.collect()),
        _ => {
            println!("Usage: tembri <fetch|fetch-xatu|fetch-cryo|classify|metrics|propamm-bytecode-group|propamm-project|propamm-decode|propamm-pair|propamm-markouts|propamm-cex-markouts|propamm-takers|propamm-routes|propamm-fetch|propamm-report|propamm-period|propamm-pool-calls|mempool-lifecycle|run-period|report> ...");
            ExitCode::from(2)
        }
    }
}
fn fetch(v: Vec<String>) -> ExitCode {
    let mut a = v.into_iter();
    let (mut from, mut to, mut out, mut rpc) = (
        None,
        None,
        PathBuf::from("data/raw"),
        std::env::var("TLM_RPC_URL").ok(),
    );
    while let Some(k) = a.next() {
        let Some(x) = a.next() else {
            return ExitCode::from(2);
        };
        match k.as_str() {
            "--from" => from = x.parse().ok(),
            "--to" => to = x.parse().ok(),
            "--out" => out = PathBuf::from(x),
            "--rpc" => rpc = Some(x),
            _ => return ExitCode::from(2),
        }
    }
    let (Some(from), Some(to), Some(rpc_url)) = (from, to, rpc) else {
        eprintln!("fetch needs --from --to and TLM_RPC_URL");
        return ExitCode::from(2);
    };
    match tlm_ingest::fetch_range_to_ndjson(&tlm_ingest::FetchConfig {
        rpc_url,
        range: tlm_ingest::FetchRange { from, to },
        output_dir: out.clone(),
    }) {
        Ok(s) => {
            println!(
                "fetched {} blocks and {} transactions into {}",
                s.blocks,
                s.transactions,
                out.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("fetch failed: {e}");
            ExitCode::from(1)
        }
    }
}
fn arg(v: &[String], name: &str) -> Option<PathBuf> {
    v.windows(2)
        .find(|x| x[0] == name)
        .map(|x| PathBuf::from(&x[1]))
}
fn classify(v: Vec<String>) -> ExitCode {
    let (Some(raw), Some(mempool), Some(out)) =
        (arg(&v, "--raw"), arg(&v, "--mempool"), arg(&v, "--out"))
    else {
        eprintln!("Usage: tembri classify --raw DIR --mempool DAY.csv.zip --out DIR");
        return ExitCode::from(2);
    };
    let block: Value = read_lines(&raw.join("blocks.ndjson"))
        .into_iter()
        .next()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap();
    let mut seen = HashMap::new();
    let unzip = Command::new("unzip")
        .args(["-p", mempool.to_str().unwrap()])
        .output()
        .expect("unzip");
    for line in String::from_utf8_lossy(&unzip.stdout).lines().skip(1) {
        let f: Vec<_> = line.split(',').collect();
        if f.len() > 1 {
            seen.entry(f[1].to_lowercase())
                .or_insert(f[0].parse::<u64>().unwrap_or(0));
        }
    }
    let txs = read_lines(&raw.join("txs.ndjson"));
    let mut tips: Vec<u64> = txs
        .iter()
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .filter_map(|x| x["effective_priority_fee_wei"].as_u64())
        .collect();
    tips.sort();
    let median = tips.get(tips.len() / 2).copied().unwrap_or(0);
    fs::create_dir_all(&out).unwrap();
    let mut w = File::create(out.join("labels.ndjson")).unwrap();
    let gl = block["gas_limit"].as_u64().unwrap();
    let base = block["base_fee_wei"].as_u64().unwrap();
    let early = gl / 5;
    let floor = base / 10;
    for s in txs {
        let t: Value = serde_json::from_str(&s).unwrap();
        let hash = t["hash"].as_str().unwrap().to_lowercase();
        let used = t["gas_used"].as_u64().unwrap();
        let limit = t["gas_limit"].as_u64().unwrap();
        let tip = t["effective_priority_fee_wei"].as_u64().unwrap();
        let cum = t["cumulative_gas_before"].as_u64().unwrap();
        let public = seen.contains_key(&hash);
        let in_early = cum + used <= early;
        let density = (tip as u128 * used as u128 / limit.max(1) as u128) as u64;
        let consumer = in_early && density >= floor && !public;
        let provider = public && !in_early && tip <= median;
        let row = json!({"hash":hash,"public":public,"first_seen_ms":seen.get(&hash),"in_early_region":in_early,"density_wei":density,"consumer_candidate":consumer,"provider_candidate":provider,"delay_tolerant":provider,"wait_seconds":null});
        w.write_all(format!("{row}\n").as_bytes()).unwrap();
    }
    println!("wrote {}", out.join("labels.ndjson").display());
    ExitCode::SUCCESS
}

fn mempool_lifecycle(v: Vec<String>) -> ExitCode {
    let (Some(observations_path), Some(admissions_path), Some(out)) = (
        arg(&v, "--observations"),
        arg(&v, "--admissions"),
        arg(&v, "--out"),
    ) else {
        eprintln!("Usage: tembri mempool-lifecycle --observations OBSERVATIONS.ndjson --admissions ADMISSIONS.ndjson --out REPORT.md [--rows-out LIFECYCLE.ndjson]");
        return ExitCode::from(2);
    };
    let observations: Result<Vec<tlm_model::MempoolObservation>, _> =
        read_lines(&observations_path)
            .iter()
            .map(|line| serde_json::from_str(line))
            .collect();
    let admissions: Result<Vec<tlm_model::MempoolAdmission>, _> = read_lines(&admissions_path)
        .iter()
        .map(|line| serde_json::from_str(line))
        .collect();
    let (Ok(observations), Ok(admissions)) = (observations, admissions) else {
        eprintln!("mempool-lifecycle input must be NDJSON matching MempoolObservation and MempoolAdmission");
        return ExitCode::from(2);
    };
    let activity_by_hash: HashMap<String, String> = match arg(&v, "--activities") {
        Some(path) => {
            let parsed: Result<Vec<tlm_model::MempoolActivity>, _> = read_lines(&path)
                .iter()
                .map(|line| serde_json::from_str(line))
                .collect();
            let Ok(parsed) = parsed else {
                eprintln!("--activities must be NDJSON matching MempoolActivity");
                return ExitCode::from(2);
            };
            parsed
                .into_iter()
                .map(|row| (row.hash.to_ascii_lowercase(), row.activity_type))
                .collect()
        }
        None => HashMap::new(),
    };
    let rows = tlm_classify::measure_mempool_lifecycle(observations, admissions);
    let rows_out = arg(&v, "--rows-out").unwrap_or_else(|| out.with_extension("ndjson"));
    if let Some(parent) = rows_out.parent() {
        if let Err(error) = fs::create_dir_all(parent) {
            eprintln!("cannot create output directory: {error}");
            return ExitCode::from(1);
        }
    }
    let lifecycle_ndjson = match rows
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(lines) => lines.join("\n") + "\n",
        Err(error) => {
            eprintln!("cannot serialize lifecycle rows: {error}");
            return ExitCode::from(1);
        }
    };
    if let Err(error) = fs::write(&rows_out, lifecycle_ndjson) {
        eprintln!("cannot write lifecycle rows: {error}");
        return ExitCode::from(1);
    }

    let mut admitted = Vec::new();
    let mut dropped = Vec::new();
    let mut pending = Vec::new();
    for row in &rows {
        match row.status {
            tlm_model::MempoolLifecycleStatus::Admitted => admitted.push(row),
            tlm_model::MempoolLifecycleStatus::DroppedBySender { .. } => dropped.push(row),
            tlm_model::MempoolLifecycleStatus::PendingOrUnobserved => pending.push(row),
        }
    }
    let average_ms = |values: &[&tlm_model::MempoolLifecycle],
                      field: fn(&tlm_model::MempoolLifecycle) -> u64| {
        if values.is_empty() {
            None
        } else {
            Some(values.iter().map(|row| field(row) as f64).sum::<f64>() / values.len() as f64)
        }
    };
    let average_optional_ms =
        |values: &[&tlm_model::MempoolLifecycle],
         field: fn(&tlm_model::MempoolLifecycle) -> Option<u64>| {
            let values: Vec<_> = values.iter().filter_map(|row| field(row)).collect();
            if values.is_empty() {
                None
            } else {
                Some(values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64)
            }
        };
    let total = rows.len().max(1) as f64;
    let display_seconds = |value: Option<f64>| {
        value
            .map(|milliseconds| format!("{:.3}", milliseconds / 1_000.0))
            .unwrap_or_else(|| "N/A".into())
    };
    let mut report = format!(
        "# Public mempool lifecycle\n\nTransactions: {}. All durations begin at the first public observation; they do not measure a wallet private submission time. A sender drop is inferred only when a later, distinct transaction with the same sender and nonce is observed before the original is admitted.\n\n| Outcome | Transactions | Share | Avg. observed residence (s) | Avg. admission delay (s) |\n| --- | ---: | ---: | ---: | ---: |\n",
        rows.len()
    );
    for (name, group) in [
        ("Admitted", &admitted),
        ("Dropped by sender (inferred)", &dropped),
        ("Pending / unobserved", &pending),
    ] {
        report.push_str(&format!(
            "| {name} | {} | {:.2}% | {} | {} |\n",
            group.len(),
            100.0 * group.len() as f64 / total,
            display_seconds(average_ms(group, |row| row.observed_residence_ms)),
            display_seconds(average_optional_ms(group, |row| row.admission_delay_ms)),
        ));
    }
    let all_rows: Vec<_> = rows.iter().collect();
    let mut activity_groups: std::collections::BTreeMap<String, Vec<&tlm_model::MempoolLifecycle>> =
        std::collections::BTreeMap::new();
    for row in &rows {
        activity_groups
            .entry(
                activity_by_hash
                    .get(&row.hash)
                    .cloned()
                    .unwrap_or_else(|| "unclassified".into()),
            )
            .or_default()
            .push(row);
    }
    report.push_str("\n## Lifecycle profile by activity type\n\n| Activity type | Transactions | Admitted | Sender drops | Pending | Avg. admission delay (s) | Avg. observed residence (s) |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (activity, group) in activity_groups {
        let admitted_count = group
            .iter()
            .filter(|row| matches!(row.status, tlm_model::MempoolLifecycleStatus::Admitted))
            .count();
        let dropped_count = group
            .iter()
            .filter(|row| {
                matches!(
                    row.status,
                    tlm_model::MempoolLifecycleStatus::DroppedBySender { .. }
                )
            })
            .count();
        let pending_count = group.len() - admitted_count - dropped_count;
        report.push_str(&format!(
            "| {activity} | {} | {:.2}% | {:.2}% | {:.2}% | {} | {} |\n",
            group.len(),
            100.0 * admitted_count as f64 / group.len().max(1) as f64,
            100.0 * dropped_count as f64 / group.len().max(1) as f64,
            100.0 * pending_count as f64 / group.len().max(1) as f64,
            display_seconds(average_optional_ms(&group, |row| row.admission_delay_ms)),
            display_seconds(average_ms(&group, |row| row.observed_residence_ms)),
        ));
    }
    let observed_visibility = average_optional_ms(&all_rows, |row| row.public_visibility_delay_ms);
    report.push_str(&format!(
        "\nFirst-public-seen delay is available for {} of {} transactions: average {} seconds.\n\nDetailed lifecycle rows: {}.\n",
        rows.iter().filter(|row| row.public_visibility_delay_ms.is_some()).count(),
        rows.len(),
        display_seconds(observed_visibility),
        rows_out.display()
    ));
    if let Some(parent) = out.parent() {
        if let Err(error) = fs::create_dir_all(parent) {
            eprintln!("cannot create report directory: {error}");
            return ExitCode::from(1);
        }
    }
    match fs::write(&out, report) {
        Ok(()) => {
            println!("wrote {} and {}", out.display(), rows_out.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("cannot write report: {error}");
            ExitCode::from(1)
        }
    }
}

fn metrics(v: Vec<String>) -> ExitCode {
    let (Some(raw), Some(labels), Some(out)) =
        (arg(&v, "--raw"), arg(&v, "--labels"), arg(&v, "--out"))
    else {
        eprintln!("Usage: tembri metrics --raw DIR --labels DIR --out FILE");
        return ExitCode::from(2);
    };
    let b: Value = serde_json::from_str(&read_lines(&raw.join("blocks.ndjson"))[0]).unwrap();
    let mut tx: HashMap<String, Value> = HashMap::new();
    for s in read_lines(&raw.join("txs.ndjson")) {
        let x: Value = serde_json::from_str(&s).unwrap();
        tx.insert(x["hash"].as_str().unwrap().to_lowercase(), x);
    }
    let mut d = 0u64;
    let (mut supply, mut ds, mut private) = (0u64, 0u64, 0u64);
    for s in read_lines(&labels.join("labels.ndjson")) {
        let l: Value = serde_json::from_str(&s).unwrap();
        let t = &tx[l["hash"].as_str().unwrap()];
        let used = t["gas_used"].as_u64().unwrap();
        if l["consumer_candidate"].as_bool() == Some(true) {
            d += t["gas_limit"].as_u64().unwrap()
        }
        if l["provider_candidate"].as_bool() == Some(true) {
            supply += used;
            ds += used
        }
        if l["in_early_region"].as_bool() == Some(true) && l["public"].as_bool() == Some(false) {
            private += used
        }
    }
    let row = json!({"block_number":b["block_number"],"consumer_demand_gas":d,"provider_supply_gas":supply,"delay_tolerant_supply_gas":ds,"private_early_gas":private,"early_capacity_gas":b["gas_limit"].as_u64().unwrap()/5});
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).unwrap()
    };
    File::create(&out)
        .unwrap()
        .write_all(format!("{row}\n").as_bytes())
        .unwrap();
    println!("wrote {}", out.display());
    ExitCode::SUCCESS
}

fn propamm_bytecode_group(v: Vec<String>) -> ExitCode {
    let (Some(input), Some(out)) = (arg(&v, "--input"), arg(&v, "--out")) else {
        eprintln!(
            "Usage: tembri propamm-bytecode-group --input BYTECODE.ndjson --out GROUPS.ndjson"
        );
        return ExitCode::from(2);
    };
    let rows = match read_ndjson::<tlm_model::BytecodeRow>(&input) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid bytecode input: {error}");
            return ExitCode::from(2);
        }
    };
    let groups = match tlm_classify::group_runtime_bytecode(&rows) {
        Ok(groups) => groups,
        Err(error) => {
            eprintln!("bytecode grouping failed: {error}");
            return ExitCode::from(2);
        }
    };
    match write_ndjson(&out, &groups) {
        Ok(()) => {
            println!("wrote {} exact-bytecode groups", groups.len());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("cannot write bytecode groups: {error}");
            ExitCode::from(1)
        }
    }
}

fn propamm_project_cmd(v: Vec<String>) -> ExitCode {
    let (Some(xatu), Some(fill_rules_path), Some(registry), Some(logs_out), Some(calls_out)) = (
        arg(&v, "--xatu"),
        arg(&v, "--fill-rules"),
        v.windows(2)
            .find(|window| window[0] == "--registry")
            .map(|window| window[1].to_ascii_lowercase()),
        arg(&v, "--logs-out"),
        arg(&v, "--calls-out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-project --xatu DIR [--period available|YYYY-MM|YYYY-MM-DD] --fill-rules RULES.ndjson --registry ADDRESS [--with-traces] --logs-out LOGS.ndjson --calls-out CALLS.ndjson [--period-blocks-out BLOCKS.ndjson] [--txs-out TXS.ndjson] [--reference-out PRICES.ndjson] [--reference-blocks-out CLOSES.ndjson]"
        );
        return ExitCode::from(2);
    };
    let period = match tlm_report::ReportPeriod::parse(
        v.windows(2)
            .find(|window| window[0] == "--period")
            .map(|window| window[1].as_str())
            .unwrap_or("available"),
    ) {
        Ok(period) => period,
        Err(error) => {
            eprintln!("invalid period: {error}");
            return ExitCode::from(2);
        }
    };
    let rules = match read_ndjson::<tlm_model::PropammEventRule>(&fill_rules_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM fill-rule input: {error}");
            return ExitCode::from(2);
        }
    };
    let emitters = rules
        .iter()
        .map(|rule| rule.emitter.to_ascii_lowercase())
        .collect();
    let config = tlm_report::ReportConfig::activity_tip(&xatu, ".", period);
    let plan = match tlm_report::plan(&config) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("PropAMM projection plan failed: {error}");
            return ExitCode::from(1);
        }
    };
    let with_traces = v.iter().any(|flag| flag == "--with-traces");
    let reference_out = arg(&v, "--reference-out");
    let reference_blocks_out = arg(&v, "--reference-blocks-out");
    let with_reference = reference_out.is_some() || reference_blocks_out.is_some();
    let (logs, calls, period_blocks, reference, envelopes) = match propamm_project::project_xatu_plan(
        &plan,
        &registry,
        &emitters,
        with_traces,
        with_reference,
    ) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("PropAMM projection failed: {error}");
            return ExitCode::from(1);
        }
    };
    if let Err(error) = write_ndjson(&logs_out, &logs) {
        eprintln!("cannot write PropAMM logs: {error}");
        return ExitCode::from(1);
    }
    if let Err(error) = write_ndjson(&calls_out, &calls) {
        eprintln!("cannot write PropAMM calls: {error}");
        return ExitCode::from(1);
    }
    if let Some(path) = arg(&v, "--txs-out") {
        if let Err(error) = write_ndjson(&path, &envelopes) {
            eprintln!("cannot write PropAMM transaction envelopes: {error}");
            return ExitCode::from(1);
        }
    }
    if let Some(path) = arg(&v, "--period-blocks-out") {
        if let Err(error) = write_ndjson(&path, &period_blocks) {
            eprintln!("cannot write period blocks: {error}");
            return ExitCode::from(1);
        }
    }
    if let Some(path) = reference_out {
        if let Err(error) = write_ndjson(&path, &reference) {
            eprintln!("cannot write reference prices: {error}");
            return ExitCode::from(1);
        }
    }
    if let Some(path) = reference_blocks_out {
        let closes = tlm_classify::reference_block_closes(&reference, &period_blocks);
        if let Err(error) = write_ndjson(&path, &closes) {
            eprintln!("cannot write reference block prices: {error}");
            return ExitCode::from(1);
        }
    }
    println!(
        "wrote {} PropAMM log rows and {} registry call rows for {}",
        logs.len(),
        calls.len(),
        plan.label
    );
    ExitCode::SUCCESS
}

fn propamm_decode(v: Vec<String>) -> ExitCode {
    let (Some(logs_path), Some(calls_path), Some(fill_rules_path), Some(registry), Some(targets_path), Some(out)) = (
        arg(&v, "--logs"),
        arg(&v, "--calls"),
        arg(&v, "--fill-rules"),
        v.windows(2)
            .find(|window| window[0] == "--registry")
            .map(|window| window[1].clone()),
        arg(&v, "--registry-targets"),
        arg(&v, "--out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-decode --logs LOGS.ndjson --calls CALLS.ndjson --fill-rules RULES.ndjson --registry ADDRESS --registry-targets TARGETS.ndjson --out EVENTS.ndjson"
        );
        return ExitCode::from(2);
    };
    let logs = match read_ndjson::<tlm_model::PropammLog>(&logs_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM log input: {error}");
            return ExitCode::from(2);
        }
    };
    let calls = match read_ndjson::<tlm_model::PropammCall>(&calls_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM call input: {error}");
            return ExitCode::from(2);
        }
    };
    let rules = match read_ndjson::<tlm_model::PropammEventRule>(&fill_rules_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM fill-rule input: {error}");
            return ExitCode::from(2);
        }
    };
    let targets = match read_ndjson::<tlm_model::PropammRegistryTarget>(&targets_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM registry-target input: {error}");
            return ExitCode::from(2);
        }
    };
    let events = match tlm_classify::decode_propamm_activity(
        &logs, &calls, &rules, &registry, &targets,
    ) {
        Ok(events) => events,
        Err(error) => {
            eprintln!("PropAMM decode failed: {error}");
            return ExitCode::from(2);
        }
    };
    match write_ndjson(&out, &events) {
        Ok(()) => {
            println!("wrote {} PropAMM event rows", events.len());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("cannot write PropAMM events: {error}");
            ExitCode::from(1)
        }
    }
}

fn propamm_markouts(v: Vec<String>) -> ExitCode {
    let (Some(events_path), Some(reference_path), Some(closes_path), Some(out)) = (
        arg(&v, "--events"),
        arg(&v, "--reference"),
        arg(&v, "--reference-blocks"),
        arg(&v, "--out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-markouts --events EVENTS.ndjson --reference PRICES.ndjson --reference-blocks CLOSES.ndjson --out MARKOUTS.ndjson"
        );
        return ExitCode::from(2);
    };
    let events = match read_ndjson::<tlm_model::PropammEvent>(&events_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM event input: {error}");
            return ExitCode::from(2);
        }
    };
    let mut points = match read_ndjson::<tlm_model::ReferencePricePoint>(&reference_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid reference price input: {error}");
            return ExitCode::from(2);
        }
    };
    points.sort_by_key(|point| (point.block_number, point.tx_position, point.log_index));
    let closes = match read_ndjson::<tlm_model::ReferenceBlockPrice>(&closes_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid reference block input: {error}");
            return ExitCode::from(2);
        }
    };
    let rows = match tlm_classify::propamm_markouts(&events, &points, &closes) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("PropAMM markouts failed: {error}");
            return ExitCode::from(1);
        }
    };
    if let Err(error) = write_ndjson(&out, &rows) {
        eprintln!("cannot write PropAMM markouts: {error}");
        return ExitCode::from(1);
    }
    let fills = events
        .iter()
        .filter(|event| event.kind == tlm_model::PropammEventKind::Fill)
        .count();
    println!("wrote {} WETH/stablecoin markout rows of {fills} fills", rows.len());
    ExitCode::SUCCESS
}

/// Load comma-separated Binance kline CSV files into one series.
fn read_cex_series(paths: &str) -> Result<tlm_classify::CexSeries, String> {
    let mut series = tlm_classify::CexSeries::default();
    for path in paths.split(',') {
        let csv = fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
        series.add_binance_klines(&csv).map_err(|error| format!("{path}: {error}"))?;
    }
    Ok(series)
}

fn propamm_cex_markouts(v: Vec<String>) -> ExitCode {
    let value = |name: &str| v.windows(2).find(|window| window[0] == name).map(|window| window[1].clone());
    let (Some(events_path), Some(period_path), Some(eth_usdt), Some(usdc_usdt), Some(out)) = (
        arg(&v, "--events"),
        arg(&v, "--period-blocks"),
        value("--eth-usdt"),
        value("--usdc-usdt"),
        arg(&v, "--out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-cex-markouts --events EVENTS.ndjson --period-blocks BLOCKS.ndjson --eth-usdt KLINES.csv[,KLINES.csv] --usdc-usdt KLINES.csv[,KLINES.csv] --out MARKOUTS.ndjson"
        );
        return ExitCode::from(2);
    };
    let events = match read_ndjson::<tlm_model::PropammEvent>(&events_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM event input: {error}");
            return ExitCode::from(2);
        }
    };
    let period = match read_ndjson::<tlm_model::PropammPeriodBlock>(&period_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid period block input: {error}");
            return ExitCode::from(2);
        }
    };
    let (eth_usdt, usdc_usdt) = match (read_cex_series(&eth_usdt), read_cex_series(&usdc_usdt)) {
        (Ok(eth), Ok(usdc)) => (eth, usdc),
        (Err(error), _) | (_, Err(error)) => {
            eprintln!("invalid Binance kline input: {error}");
            return ExitCode::from(2);
        }
    };
    let rows = match tlm_classify::propamm_cex_markouts(&events, &period, &eth_usdt, &usdc_usdt) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("PropAMM CEX markouts failed: {error}");
            return ExitCode::from(1);
        }
    };
    if let Err(error) = write_ndjson(&out, &rows) {
        eprintln!("cannot write PropAMM CEX markouts: {error}");
        return ExitCode::from(1);
    }
    let priced = rows.iter().filter(|row| row.markout_0s_bp.is_some()).count();
    println!(
        "wrote {} WETH/stablecoin CEX markout rows, {priced} priced at the fill, from {} ETH/USDT and {} USDC/USDT traded seconds",
        rows.len(),
        eth_usdt.len(),
        usdc_usdt.len()
    );
    ExitCode::SUCCESS
}

fn propamm_takers(v: Vec<String>) -> ExitCode {
    let (Some(events_path), Some(txs_path), Some(labels_path), Some(out)) = (
        arg(&v, "--events"),
        arg(&v, "--txs"),
        arg(&v, "--takers"),
        arg(&v, "--out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-takers --events EVENTS.ndjson --txs TXS.ndjson --takers TAKERS.ndjson --out TAKER_FILLS.ndjson [--cex-markouts MARKOUTS.ndjson --summary-out CLASSES.ndjson]"
        );
        return ExitCode::from(2);
    };
    let events = match read_ndjson::<tlm_model::PropammEvent>(&events_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM event input: {error}");
            return ExitCode::from(2);
        }
    };
    let txs = match read_ndjson::<tlm_model::PropammTxEnvelope>(&txs_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM transaction input: {error}");
            return ExitCode::from(2);
        }
    };
    let labels = match read_ndjson::<tlm_model::PropammTakerLabel>(&labels_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid taker label input: {error}");
            return ExitCode::from(2);
        }
    };
    let takers = match tlm_classify::classify_propamm_takers(&events, &txs, &labels) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("PropAMM taker classification failed: {error}");
            return ExitCode::from(1);
        }
    };
    if let Err(error) = write_ndjson(&out, &takers) {
        eprintln!("cannot write PropAMM taker fills: {error}");
        return ExitCode::from(1);
    }
    println!("wrote {} PropAMM taker fill rows", takers.len());
    match (arg(&v, "--cex-markouts"), arg(&v, "--summary-out")) {
        (Some(markouts_path), Some(summary_out)) => {
            let markouts = match read_ndjson::<tlm_model::PropammCexMarkout>(&markouts_path) {
                Ok(rows) => rows,
                Err(error) => {
                    eprintln!("invalid PropAMM CEX markout input: {error}");
                    return ExitCode::from(2);
                }
            };
            let summary = tlm_classify::summarize_taker_markouts(&takers, &markouts);
            if let Err(error) = write_ndjson(&summary_out, &summary) {
                eprintln!("cannot write PropAMM taker summary: {error}");
                return ExitCode::from(1);
            }
            println!("wrote {} taker class rows", summary.len());
        }
        (None, None) => {}
        _ => {
            eprintln!("--cex-markouts and --summary-out go together");
            return ExitCode::from(2);
        }
    }
    ExitCode::SUCCESS
}

fn propamm_routes(v: Vec<String>) -> ExitCode {
    let (Some(events_path), Some(txs_path), Some(period_path), Some(mempool), Some(out), Some(sandwiches_out)) = (
        arg(&v, "--events"),
        arg(&v, "--txs"),
        arg(&v, "--period-blocks"),
        v.windows(2).find(|x| x[0] == "--mempool").map(|x| x[1].clone()),
        arg(&v, "--out"),
        arg(&v, "--sandwiches-out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-routes --events EVENTS.ndjson --txs TXS.ndjson --period-blocks BLOCKS.ndjson --mempool DUMPSTER.parquet[,DUMPSTER.parquet] --out ROUTES.ndjson --sandwiches-out SANDWICHES.ndjson"
        );
        return ExitCode::from(2);
    };
    let events = match read_ndjson::<tlm_model::PropammEvent>(&events_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM event input: {error}");
            return ExitCode::from(2);
        }
    };
    let txs = match read_ndjson::<tlm_model::PropammTxEnvelope>(&txs_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM transaction input: {error}");
            return ExitCode::from(2);
        }
    };
    let period = match read_ndjson::<tlm_model::PropammPeriodBlock>(&period_path) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid period block input: {error}");
            return ExitCode::from(2);
        }
    };
    let wanted: HashSet<String> = txs.iter().map(|tx| tx.tx_hash.to_ascii_lowercase()).collect();
    let mut first_seen = HashMap::new();
    for path in mempool.split(',').filter(|path| !path.is_empty()) {
        if let Err(error) = read_dumpster_first_seen(&PathBuf::from(path), &wanted, &mut first_seen) {
            eprintln!("cannot read Mempool Dumpster file {path}: {error}");
            return ExitCode::from(1);
        }
    }
    let routes = match tlm_classify::classify_propamm_routes(&txs, &period, &first_seen) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("PropAMM route classification failed: {error}");
            return ExitCode::from(1);
        }
    };
    let sandwiches = match tlm_classify::find_propamm_sandwiches(&events, &txs) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("PropAMM sandwich search failed: {error}");
            return ExitCode::from(1);
        }
    };
    for (path, result) in [(&out, write_ndjson(&out, &routes)), (&sandwiches_out, write_ndjson(&sandwiches_out, &sandwiches))] {
        if let Err(error) = result {
            eprintln!("cannot write {}: {error}", path.display());
            return ExitCode::from(1);
        }
    }
    println!("wrote {} PropAMM transaction routes and {} sandwiches", routes.len(), sandwiches.len());
    ExitCode::SUCCESS
}

/// Read the earliest `timestamp` (Unix milliseconds) of each wanted `hash`
/// from a Flashbots Mempool Dumpster daily Parquet file, by column name.
fn read_dumpster_first_seen(
    path: &PathBuf,
    wanted: &HashSet<String>,
    first_seen: &mut HashMap<String, u64>,
) -> Result<(), String> {
    let reader = xatu_reader(path)?;
    let root = reader.metadata().file_metadata().schema();
    let fields: Vec<_> = root
        .get_fields()
        .iter()
        .filter(|field| matches!(field.name(), "hash" | "timestamp"))
        .cloned()
        .collect();
    let hash = fields.iter().position(|field| field.name() == "hash").ok_or("missing column hash")?;
    let timestamp = fields
        .iter()
        .position(|field| field.name() == "timestamp")
        .ok_or("missing column timestamp")?;
    let projection = parquet::schema::types::Type::group_type_builder(root.name())
        .with_fields(fields)
        .build()
        .map_err(|e| e.to_string())?;
    for row in reader.get_row_iter(Some(projection)).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let tx_hash = row.get_string(hash).map_err(|e| e.to_string())?.to_ascii_lowercase();
        if !wanted.contains(&tx_hash) {
            continue;
        }
        let millis = row.get_timestamp_millis(timestamp).map_err(|e| e.to_string())?;
        let millis = u64::try_from(millis).map_err(|_| "timestamp before 1970")?;
        first_seen
            .entry(tx_hash)
            .and_modify(|seen: &mut u64| *seen = (*seen).min(millis))
            .or_insert(millis);
    }
    Ok(())
}

fn propamm_pair(v: Vec<String>) -> ExitCode {
    let (Some(input), Some(pairs_out), Some(blocks_out), Some(builders_out)) = (
        arg(&v, "--input"),
        arg(&v, "--pairs-out"),
        arg(&v, "--blocks-out"),
        arg(&v, "--builders-out"),
    ) else {
        eprintln!(
            "Usage: tembri propamm-pair --input EVENTS.ndjson --pairs-out PAIRS.ndjson --blocks-out BLOCKS.ndjson --builders-out BUILDERS.ndjson [--period-blocks BLOCKS.ndjson]"
        );
        return ExitCode::from(2);
    };
    let events = match read_ndjson::<tlm_model::PropammEvent>(&input) {
        Ok(rows) => rows,
        Err(error) => {
            eprintln!("invalid PropAMM event input: {error}");
            return ExitCode::from(2);
        }
    };
    let mut pairs = tlm_classify::pair_propamm_events(&events);
    let blocks = tlm_classify::summarize_propamm_blocks(&events, &pairs);
    let mut builders = tlm_classify::summarize_propamm_builders(&blocks);
    if let Some(path) = arg(&v, "--period-blocks") {
        let period = match read_ndjson::<tlm_model::PropammPeriodBlock>(&path) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("invalid period-block input: {error}");
                return ExitCode::from(2);
            }
        };
        builders = match tlm_classify::add_period_blocks(&builders, &period, &blocks) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("PropAMM builder summary failed: {error}");
                return ExitCode::from(2);
            }
        };
        pairs = match tlm_classify::add_quote_ages(&pairs, &period) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("PropAMM quote age failed: {error}");
                return ExitCode::from(2);
            }
        };
    }
    if let Err(error) = write_ndjson(&pairs_out, &pairs) {
        eprintln!("cannot write PropAMM pairs: {error}");
        return ExitCode::from(1);
    }
    if let Err(error) = write_ndjson(&blocks_out, &blocks) {
        eprintln!("cannot write PropAMM blocks: {error}");
        return ExitCode::from(1);
    }
    if let Err(error) = write_ndjson(&builders_out, &builders) {
        eprintln!("cannot write PropAMM builders: {error}");
        return ExitCode::from(1);
    }
    println!(
        "wrote {} fill pair rows, {} block/pool rows, and {} builder rows",
        pairs.len(),
        blocks.len(),
        builders.len()
    );
    ExitCode::SUCCESS
}

fn read_ndjson<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Result<Vec<T>, String> {
    let input = File::open(path).map_err(|error| error.to_string())?;
    BufReader::new(input)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            Ok(line) => Some(
                serde_json::from_str(&line)
                    .map_err(|error| format!("{} line {}: {error}", path.display(), index + 1)),
            ),
            Err(error) => Some(Err(format!("{} line {}: {error}", path.display(), index + 1))),
        })
        .collect()
}

fn write_ndjson<T: serde::Serialize>(path: &PathBuf, rows: &[T]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let body = rows
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?
        .join("\n");
    fs::write(path, if body.is_empty() { body } else { body + "\n" })
        .map_err(|error| error.to_string())
}

fn read_lines(p: &PathBuf) -> Vec<String> {
    BufReader::new(File::open(p).unwrap())
        .lines()
        .map_while(Result::ok)
        .collect()
}

fn fetch_cryo(v: Vec<String>) -> ExitCode {
    let get = |k: &str| v.windows(2).find(|x| x[0] == k).map(|x| x[1].clone());
    let (Some(date), Some(from), Some(to)) = (
        get("--date"),
        get("--from").and_then(|x| x.parse().ok()),
        get("--to").and_then(|x| x.parse().ok()),
    ) else {
        eprintln!("Usage: tembri fetch-cryo --date YYYY-MM-DD --from BLOCK --to BLOCK [--out DIR] [--cryo PATH]");
        return ExitCode::from(2);
    };
    let out = get("--out")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/parquet"));
    let cryo = get("--cryo").unwrap_or_else(|| "cryo".into());
    let day = tlm_ingest::CryoDay {
        date,
        from_block: from,
        to_block: to,
        output_dir: out.clone(),
        requests_per_second: 8,
        max_concurrent_requests: 1,
    };
    match tlm_ingest::run_cryo_day(&cryo, &day) {
        Ok(()) => {
            println!("cryo partition ready: {}", out.join(&day.date).display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("cryo fetch failed: {e}");
            ExitCode::from(1)
        }
    }
}

fn run_period(v: Vec<String>) -> ExitCode {
    let Some(config_path) = arg(&v, "--config") else {
        eprintln!("Usage: tembri run-period --config CONFIG.toml");
        return ExitCode::from(2);
    };
    let config = match simple_config(&config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("invalid run-period config: {error}");
            return ExitCode::from(2);
        }
    };
    let required = |key: &str| {
        config
            .get(key)
            .cloned()
            .ok_or_else(|| format!("missing {key}"))
    };
    let (Ok(from), Ok(to), Ok(data_dir), Ok(output_dir), Ok(dates)) = (
        required("from_block")
            .and_then(|value| value.parse().map_err(|_| "invalid from_block".into())),
        required("to_block").and_then(|value| value.parse().map_err(|_| "invalid to_block".into())),
        required("data_dir").map(PathBuf::from),
        required("output_dir").map(PathBuf::from),
        required("dates"),
    ) else {
        eprintln!("run-period requires from_block, to_block, data_dir, output_dir, and dates");
        return ExitCode::from(2);
    };
    let names = config
        .get("datasets")
        .map(String::as_str)
        .unwrap_or("blocks,transactions,logs");
    let datasets = match xatu_datasets(names) {
        Ok(datasets) => datasets,
        Err(error) => {
            eprintln!("invalid datasets: {error}");
            return ExitCode::from(2);
        }
    };
    let min_blocks = config
        .get("min_blocks_per_day")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(6_500);
    match tlm_ingest::fetch_xatu_range(from, to, &datasets, &data_dir) {
        Ok((downloaded, cached)) => {
            println!("Xatu ready: {downloaded} downloaded, {cached} cached")
        }
        Err(error) => {
            eprintln!("Xatu fetch failed: {error}");
            return ExitCode::from(1);
        }
    }
    let source_dir = data_dir.join("xatu");
    for date in dates
        .split(',')
        .map(str::trim)
        .filter(|date| !date.is_empty())
    {
        let period = match tlm_report::ReportPeriod::parse(date) {
            Ok(period) => period,
            Err(error) => {
                eprintln!("invalid configured date {date}: {error}");
                return ExitCode::from(2);
            }
        };
        let plan = match tlm_report::plan(&tlm_report::ReportConfig::activity_tip(
            &source_dir,
            &output_dir,
            period,
        )) {
            Ok(plan) => plan,
            Err(error) => {
                eprintln!("cannot plan {date}: {error}");
                return ExitCode::from(1);
            }
        };
        let block_count = plan
            .block_numbers
            .as_ref()
            .map_or(0, |numbers| numbers.len());
        if block_count < min_blocks {
            eprintln!(
                "refusing partial day {date}: only {block_count} blocks (minimum {min_blocks})"
            );
            return ExitCode::from(1);
        }
        let out = output_dir.join(format!(
            "activity_tip_xatu_builder_{date}_eip1559_region_tip_v3.md"
        ));
        match activity_tip_xatu_report(&plan, &out) {
            Ok(()) => println!("wrote {}", out.display()),
            Err(error) => {
                eprintln!("report failed for {date}: {error}");
                return ExitCode::from(1);
            }
        }
    }
    ExitCode::SUCCESS
}

fn simple_config(path: &PathBuf) -> Result<HashMap<String, String>, String> {
    let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut values = HashMap::new();
    for (line_number, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("line {} must be key = value", line_number + 1))?;
        let value = value.trim().trim_matches(char::from(34)).to_owned();
        values.insert(key.trim().to_owned(), value);
    }
    Ok(values)
}

fn xatu_datasets(names: &str) -> Result<Vec<tlm_ingest::XatuDataset>, String> {
    names
        .split(',')
        .map(|name| match name.trim() {
            "blocks" => Ok(tlm_ingest::XatuDataset::Blocks),
            "transactions" => Ok(tlm_ingest::XatuDataset::Transactions),
            "logs" => Ok(tlm_ingest::XatuDataset::Logs),
            "traces" => Ok(tlm_ingest::XatuDataset::Traces),
            "erc20_transfers" => Ok(tlm_ingest::XatuDataset::Erc20Transfers),
            "native_transfers" => Ok(tlm_ingest::XatuDataset::NativeTransfers),
            unknown => Err(format!("unknown Xatu dataset {unknown}")),
        })
        .collect()
}

fn report(v: Vec<String>) -> ExitCode {
    let Some(kind) = v.first().map(String::as_str) else {
        eprintln!("Usage: tembri report activity-tip --input observations.csv --out report.md");
        return ExitCode::from(2);
    };
    if kind == "activity-tip-xatu" {
        let Some(xatu) = arg(&v, "--xatu") else {
            eprintln!("activity-tip-xatu needs --xatu DIR");
            return ExitCode::from(2);
        };
        let period = match tlm_report::ReportPeriod::parse(
            v.windows(2)
                .find(|x| x[0] == "--period")
                .map(|x| x[1].as_str())
                .unwrap_or("available"),
        ) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("invalid period: {e}");
                return ExitCode::from(2);
            }
        };
        let out = arg(&v, "--out").unwrap_or_else(|| {
            PathBuf::from(format!(
                "/share/tlm_proj/report/activity_tip_xatu_{}.md",
                period.label()
            ))
        });
        let config = tlm_report::ReportConfig::activity_tip(
            &xatu,
            out.parent().unwrap_or_else(|| std::path::Path::new(".")),
            period,
        );
        let plan = match tlm_report::plan(&config) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("report plan failed: {e}");
                return ExitCode::from(1);
            }
        };
        return match activity_tip_xatu_report(&plan, &out) {
            Ok(()) => {
                println!("wrote {}", out.display());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("report failed: {e}");
                ExitCode::from(1)
            }
        };
    }
    if kind != "activity-tip" {
        eprintln!("unknown report: {kind}");
        return ExitCode::from(2);
    }
    let Some(input) = arg(&v, "--input") else {
        eprintln!("activity-tip needs --input observations.csv");
        return ExitCode::from(2);
    };
    let Some(out) = arg(&v, "--out") else {
        eprintln!("activity-tip needs --out report.md");
        return ExitCode::from(2);
    };
    match activity_tip_report(&input, &out) {
        Ok(()) => {
            println!("wrote {}", out.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("report failed: {e}");
            ExitCode::from(1)
        }
    }
}

#[derive(Default)]
struct ActivityStats {
    tips: Vec<u128>,
    positions: Vec<f64>,
    tip_positions: Vec<(u128, f64)>,
}
fn percentile(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    };
    sorted[((sorted.len() - 1) as f64 * pct).round() as usize]
}
fn activity_tip_report(input: &PathBuf, out: &PathBuf) -> Result<(), String> {
    let csv = fs::read_to_string(input).map_err(|e| e.to_string())?;
    let mut columns = HashMap::new();
    let mut lines = csv.lines();
    for (i, name) in lines.next().ok_or("empty CSV")?.split(',').enumerate() {
        columns.insert(name, i);
    }
    let ty = *columns
        .get("economic_type")
        .ok_or("CSV missing economic_type")?;
    let tip = *columns
        .get("effective_tip_wei_per_gas")
        .ok_or("CSV missing effective_tip_wei_per_gas")?;
    let pos = *columns.get("position").ok_or("CSV missing position")?;
    let mut groups: std::collections::BTreeMap<String, ActivityStats> =
        std::collections::BTreeMap::new();
    for line in lines {
        let fields: Vec<_> = line.split(',').collect();
        if fields.len() <= pos {
            continue;
        };
        let Ok(t) = fields[tip].parse::<u128>() else {
            continue;
        };
        let Ok(p) = fields[pos].parse::<f64>() else {
            continue;
        };
        let g = groups.entry(fields[ty].to_owned()).or_default();
        g.tips.push(t);
        g.positions.push(p);
    }
    let mut markdown=String::from("# Effective tip vs. activity type\n\n| Activity type | Transactions | Tip = 0 tx | Tip = 0 share | Private flow | Public flow | Median tip (wei/gas) | P10 tip | P90 tip | Avg. position | P10 position | P90 position |\n| --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (kind, mut stat) in groups {
        stat.tips.sort_unstable();
        stat.positions.sort_by(|a, b| a.total_cmp(b));
        let n = stat.tips.len();
        let avg = stat.positions.iter().sum::<f64>() / n as f64;
        let ppos = |q: f64| stat.positions[((n - 1) as f64 * q).round() as usize];
        markdown.push_str(&format!(
            "| {kind} | {n} | {:.2} | {} | {} | {} | {:.4} | {:.4} | {:.4} |\n",
            stat.tips.iter().map(|tip| *tip as f64).sum::<f64>() / n as f64,
            percentile(&stat.tips, 0.5),
            percentile(&stat.tips, 0.1),
            percentile(&stat.tips, 0.9),
            avg,
            ppos(0.1),
            ppos(0.9)
        ));
    }
    markdown.push_str("\nActivity definitions are evidence-based: a multi-pool arbitrage candidate requires swap logs from two or more distinct pool contracts; it is not a confirmed arbitrage claim. Liquidation and oracle labels require configured, reviewed ABI evidence.\n");
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?
    };
    fs::write(out, markdown).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::{base_fee_column_index, validated_base_fee};

    #[test]
    fn finds_base_fee_by_name_in_legacy_xatu_block_schema() {
        let fields = [
            ("slot", true),
            ("proposer_index", true),
            ("block_number", true),
            ("block_hash", false),
            ("parent_hash", false),
            ("gas_used", true),
            ("gas_limit", true),
            ("extra_data", false),
            ("base_fee_per_gas", true),
            ("meta_network_id", true),
        ];
        assert_eq!(base_fee_column_index(&fields), Ok(8));
    }

    #[test]
    fn finds_base_fee_by_name_in_current_xatu_block_schema() {
        let fields = [
            ("slot", true),
            ("proposer_index", true),
            ("block_number", true),
            ("block_hash", false),
            ("parent_hash", false),
            ("gas_used", true),
            ("gas_limit", true),
            ("extra_data", false),
            ("extra_data_string", false),
            ("base_fee_per_gas", true),
        ];
        assert_eq!(base_fee_column_index(&fields), Ok(9));
    }

    #[test]
    fn rejects_missing_or_incompatible_base_fee_field() {
        assert!(base_fee_column_index(&[("meta_network_id", true)]).is_err());
        assert!(base_fee_column_index(&[("base_fee_per_gas", false)]).is_err());
    }

    #[test]
    fn rejects_zero_base_fee() {
        assert!(validated_base_fee(0).is_err());
        assert_eq!(validated_base_fee(1), Ok(1));
    }
}

fn fetch_xatu(v: Vec<String>) -> ExitCode {
    let get = |k: &str| v.windows(2).find(|x| x[0] == k).map(|x| x[1].clone());
    let (Some(from), Some(to)) = (
        get("--from").and_then(|x| x.parse().ok()),
        get("--to").and_then(|x| x.parse().ok()),
    ) else {
        eprintln!("Usage: tembri fetch-xatu --from BLOCK --to BLOCK [--out DIR] [--datasets blocks,transactions,logs]");
        return ExitCode::from(2);
    };
    let out = get("--out")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data/raw"));
    let names = get("--datasets").unwrap_or_else(|| "blocks,transactions,logs".into());
    let mut sets = Vec::new();
    for name in names.split(',') {
        sets.push(match name {
            "blocks" => tlm_ingest::XatuDataset::Blocks,
            "transactions" => tlm_ingest::XatuDataset::Transactions,
            "logs" => tlm_ingest::XatuDataset::Logs,
            "traces" => tlm_ingest::XatuDataset::Traces,
            "erc20_transfers" => tlm_ingest::XatuDataset::Erc20Transfers,
            "native_transfers" => tlm_ingest::XatuDataset::NativeTransfers,
            _ => {
                eprintln!("unknown Xatu dataset: {name}");
                return ExitCode::from(2);
            }
        });
    }
    match tlm_ingest::fetch_xatu_range(from, to, &sets, &out) {
        Ok((downloaded, cached)) => {
            println!(
                "Xatu ready: {downloaded} downloaded, {cached} cached in {}",
                out.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Xatu fetch failed: {e}");
            ExitCode::from(1)
        }
    }
}

const ERC20_TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const UNISWAP_V2_SWAP: &str = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const UNISWAP_V3_SWAP: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

#[derive(Clone)]
struct XatuBlock {
    base_fee: u64,
    gas_used: u64,
    builder_signal: String,
}
#[derive(Default)]
struct BuilderStats {
    blocks: u64,
    txs: u64,
    zero_tip_txs: u64,
    zero_tip_top_5: u64,
    zero_tip_top_10: u64,
    zero_tip_top_20: u64,
    correlation_sum: f64,
    correlations: u64,
    tip_sum: f64,
    position_sum: f64,
}
fn pearson(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let (sx, sy) = points
        .iter()
        .fold((0.0, 0.0), |(x, y), (a, b)| (x + a, y + b));
    let (mx, my) = (sx / n, sy / n);
    let (num, dx, dy) = points
        .iter()
        .fold((0.0, 0.0, 0.0), |(num, dx, dy), (x, y)| {
            let a = x - mx;
            let b = y - my;
            (num + a * b, dx + a * a, dy + b * b)
        });
    (dx > 0.0 && dy > 0.0).then(|| num / (dx * dy).sqrt())
}
struct XatuTx {
    index: u64,
    to: Option<String>,
    has_calldata: bool,
    selector: Option<String>,
    gas_used: u64,
    gas_price: u64,
    tx_type: u64,
    max_priority_fee: u64,
    max_fee: u64,
}
#[derive(Default)]
struct FeeDiagnostics {
    type2_or_newer: u64,
    gas_price_equals_max_fee: u64,
}

#[derive(Clone)]
struct FingerprintRow {
    activity: String,
    builder: String,
    priority_fee: u64,
    effective_tip: u64,
    position: f64,
    fingerprint: tlm_classify::FeeFingerprint,
}

fn append_fee_fingerprint_report(markdown: &mut String, rows: &[FingerprintRow]) -> Result<(), String> {
    if rows.is_empty() {
        markdown.push_str("\n## Fee-field fingerprints (v4)\n\nNo type-2-or-later transactions were available.\n");
        return Ok(());
    }
    let nonzero_priority = rows.iter().filter(|row| row.priority_fee > 0).count();
    if nonzero_priority == 0 {
        return Err("validation stop: no type-2 transaction has a nonzero max_priority_fee".into());
    }
    let base_fee_matches = rows.iter().filter(|row| row.fingerprint.tip_at_base_fee_lag.is_some()).count();
    if base_fee_matches * 100 > rows.len() * 90 {
        return Err("validation stop: tip_at_base_fee exceeds 90% of type-2 transactions".into());
    }
    let missing_lag = rows.iter().filter(|row| row.fingerprint.missing_prior_lag).count();
    if missing_lag * 100 > rows.len() {
        return Err("validation stop: missing prior-base-fee lags exceed 1% of type-2 transactions".into());
    }
    markdown.push_str("\n## Fee-field fingerprints (v4)\n\nFingerprints describe signed-fee patterns, not wallet identity. They use `max_priority_fee_per_gas` and `max_fee_per_gas` on type-2+ transactions. Base-fee comparisons examine the including block and up to three preceding blocks. A base-fee-like match has 5% tolerance; the max-fee formula has 1% tolerance.\n");
    markdown.push_str(&format!("\nType-2+ transactions: {}. Missing one or more parent-base-fee lags: {} ({:.3}%).\n", rows.len(), missing_lag, 100.0 * missing_lag as f64 / rows.len() as f64));

    let mut histogram: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    for row in rows { *histogram.entry(row.priority_fee).or_default() += 1; }
    let mut histogram: Vec<_> = histogram.into_iter().collect();
    histogram.sort_by(|(left_fee, left_count), (right_fee, right_count)| right_count.cmp(left_count).then_with(|| left_fee.cmp(right_fee)));
    markdown.push_str("\n### Signed priority-fee histogram\n\n| max priority fee (gwei/gas) | Transactions | Share of type-2+ |\n| ---: | ---: | ---: |\n");
    for (fee, count) in histogram.into_iter().take(20) {
        markdown.push_str(&format!("| {:.6} | {count} | {:.2}% |\n", fee as f64 / 1_000_000_000.0, 100.0 * count as f64 / rows.len() as f64));
    }

    let mut activities: std::collections::BTreeMap<&str, Vec<&FingerprintRow>> = std::collections::BTreeMap::new();
    for row in rows { activities.entry(&row.activity).or_default().push(row); }
    markdown.push_str("\n### Fingerprint shares by activity type\n\n`any default` is the union of base-fee-like priority fee, max-fee formula, and exact round-tip fingerprints. Early-position shares are calculated separately for tagged and untagged rows.\n\n| Activity type | Type-2+ tx | At base fee | At twice base fee (control) | Max-fee formula | Round tip | Any default | Tagged avg. position | Untagged avg. position | Tagged first 5% | Untagged first 5% | Tagged first 10% | Untagged first 10% |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (activity, values) in &activities {
        let n = values.len().max(1) as f64;
        let tagged: Vec<_> = values.iter().copied().filter(|row| row.fingerprint.any_default()).collect();
        let untagged: Vec<_> = values.iter().copied().filter(|row| !row.fingerprint.any_default()).collect();
        let share = |items: &[&FingerprintRow], test: fn(&FingerprintRow) -> bool| 100.0 * items.iter().filter(|row| test(row)).count() as f64 / n;
        let avg_position = |items: &[&FingerprintRow]| if items.is_empty() { f64::NAN } else { items.iter().map(|row| row.position).sum::<f64>() / items.len() as f64 };
        let early = |items: &[&FingerprintRow], ceiling: f64| if items.is_empty() { f64::NAN } else { 100.0 * items.iter().filter(|row| row.position <= ceiling).count() as f64 / items.len() as f64 };
        markdown.push_str(&format!("| {activity} | {} | {:.2}% | {:.2}% | {:.2}% | {:.2}% | {:.2}% | {:.4} | {:.4} | {:.2}% | {:.2}% | {:.2}% | {:.2}% |\n", values.len(), share(values, |row| row.fingerprint.tip_at_base_fee_lag.is_some()), share(values, |row| row.fingerprint.tip_at_twice_base_fee_lag.is_some()), share(values, |row| row.fingerprint.default_max_fee_formula_lag.is_some()), share(values, |row| row.fingerprint.round_tip), 100.0 * tagged.len() as f64 / n, avg_position(&tagged), avg_position(&untagged), early(&tagged, 0.05), early(&untagged, 0.05), early(&tagged, 0.10), early(&untagged, 0.10)));
    }

    let mut builders: std::collections::BTreeMap<&str, Vec<&FingerprintRow>> = std::collections::BTreeMap::new();
    for row in rows { builders.entry(&row.builder).or_default().push(row); }
    let mut builders: Vec<_> = builders.into_iter().collect();
    builders.sort_by(|(left_name, left), (right_name, right)| right.len().cmp(&left.len()).then_with(|| left_name.cmp(right_name)));
    markdown.push_str("\n### Fingerprint shares by builder signal\n\n| Builder signal | Type-2+ tx | Any default | Tip at base fee |\n| --- | ---: | ---: | ---: |\n");
    for (builder, values) in builders {
        let n = values.len().max(1) as f64;
        markdown.push_str(&format!("| {builder} | {} | {:.2}% | {:.2}% |\n", values.len(), 100.0 * values.iter().filter(|row| row.fingerprint.any_default()).count() as f64 / n, 100.0 * values.iter().filter(|row| row.fingerprint.tip_at_base_fee_lag.is_some()).count() as f64 / n));
    }

    markdown.push_str("\n### Position-gradient robustness by individual fingerprint\n\nThe first band is 0–10% of cumulative block gas; the late band is 30–60%. Each fingerprint column is evaluated on that tag's rows, so columns can overlap. `Untagged` has none of the three default fingerprints. Ratio is early-band mean effective tip divided by late-band mean effective tip.\n\n| Activity type | All | Round tip | Max-fee formula | Tip at base fee | Untagged |\n| --- | ---: | ---: | ---: | ---: | ---: |\n");
    for (activity, values) in activities {
        let ratio = |subset: Vec<&&FingerprintRow>| {
            let early: Vec<_> = subset.iter().filter(|row| row.position <= 0.10).collect();
            let late: Vec<_> = subset.iter().filter(|row| row.position > 0.30 && row.position <= 0.60).collect();
            if early.is_empty() || late.is_empty() { f64::NAN } else {
                let e = early.iter().map(|row| row.effective_tip as f64).sum::<f64>() / early.len() as f64;
                let l = late.iter().map(|row| row.effective_tip as f64).sum::<f64>() / late.len() as f64;
                if l == 0.0 { f64::NAN } else { e / l }
            }
        };
        let all: Vec<_> = values.iter().collect();
        let round: Vec<_> = values.iter().filter(|row| row.fingerprint.round_tip).collect();
        let formula: Vec<_> = values.iter().filter(|row| row.fingerprint.default_max_fee_formula_lag.is_some()).collect();
        let base_fee: Vec<_> = values.iter().filter(|row| row.fingerprint.tip_at_base_fee_lag.is_some()).collect();
        let untagged: Vec<_> = values.iter().filter(|row| !row.fingerprint.any_default()).collect();
        markdown.push_str(&format!("| {activity} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |\n", ratio(all), ratio(round), ratio(formula), ratio(base_fee), ratio(untagged)));
    }
    Ok(())
}
fn effective_tip(tx: &XatuTx, base_fee: u64) -> u64 {
    if tx.tx_type >= 2 && tx.max_fee > 0 {
        tx.max_priority_fee.min(tx.max_fee.saturating_sub(base_fee))
    } else {
        tx.gas_price.saturating_sub(base_fee)
    }
}
#[derive(Default)]
struct XatuEvidence {
    transfers: u32,
    swaps: std::collections::BTreeSet<String>,
}

fn parquet_text(row: &parquet::record::Row, col: usize) -> Option<String> {
    row.get_bytes(col)
        .ok()
        .and_then(|b| std::str::from_utf8(b.data()).ok())
        .map(|s| s.to_ascii_lowercase())
}
fn builder_name(raw: String) -> String {
    let hex = raw.strip_prefix("0x").filter(|value| value.len() % 2 == 0);
    let decoded = hex.and_then(|value| {
        let bytes: Option<Vec<u8>> = (0..value.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&value[i..i + 2], 16).ok())
            .collect();
        bytes.and_then(|b| String::from_utf8(b).ok())
    });
    decoded
        .unwrap_or(raw)
        .trim_matches(char::from(0))
        .to_ascii_lowercase()
}
fn parquet_u64(row: &parquet::record::Row, col: usize) -> Result<u64, String> {
    row.get_ulong(col)
        .or_else(|_| row.get_uint(col).map(u64::from))
        .or_else(|_| row.get_ubyte(col).map(u64::from))
        .or_else(|_| {
            row.get_int(col)
                .map_err(|e| e.to_string())
                .and_then(|value| u64::try_from(value).map_err(|_| "negative value where an unsigned value was expected".into()))
        })
        .map_err(|e| e.to_string())
}
fn parquet_fee_u64(row: &parquet::record::Row, col: usize) -> Result<u64, String> {
    if let Ok(value) = parquet_u64(row, col) {
        return Ok(value);
    }
    let bytes = row.get_bytes(col).map_err(|e| e.to_string())?.data();
    if bytes.len() > 16 {
        return Err("fee value wider than 128 bits".into());
    }
    let mut value = 0u128;
    for (index, byte) in bytes.iter().enumerate() {
        value |= (*byte as u128) << (8 * index);
    }
    u64::try_from(value).map_err(|_| "fee exceeds u64".into())
}
fn xatu_plan_files(plan: &tlm_report::ReportPlan, name: &str) -> Result<Vec<PathBuf>, String> {
    let files: Vec<_> = plan
        .partitions
        .iter()
        .map(|partition| partition.join(format!("{name}.parquet")))
        .filter(|path| path.is_file())
        .collect();
    if files.is_empty() {
        return Err(format!("report plan has no {name}.parquet files"));
    }
    Ok(files)
}
fn xatu_reader(path: &PathBuf) -> Result<SerializedFileReader<File>, String> {
    SerializedFileReader::new(File::open(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}

/// Resolve this field by its stable schema name. Xatu's historical block schemas
/// place it at different physical positions, so a positional lookup can silently
/// read `meta_network_id` as a base fee.
fn base_fee_column_index(columns: &[(&str, bool)]) -> Result<usize, String> {
    let Some((index, (_, is_int64))) = columns
        .iter()
        .enumerate()
        .find(|(_, (name, _))| *name == "base_fee_per_gas")
    else {
        return Err("Xatu blocks schema is missing base_fee_per_gas".into());
    };
    if !is_int64 {
        return Err("Xatu blocks base_fee_per_gas must have INT64 physical type".into());
    }
    Ok(index)
}

fn xatu_base_fee_column(reader: &SerializedFileReader<File>) -> Result<usize, String> {
    let columns: Vec<_> = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|column| {
            (
                column.name(),
                column.physical_type() == ParquetPhysicalType::INT64,
            )
        })
        .collect();
    base_fee_column_index(&columns)
}

fn validated_base_fee(value: u64) -> Result<u64, String> {
    if value == 0 {
        return Err("Xatu blocks base_fee_per_gas is zero".into());
    }
    Ok(value)
}
fn xatu_evidence(
    log_path: &PathBuf,
    plan: &tlm_report::ReportPlan,
) -> Result<HashMap<(u64, u64), XatuEvidence>, String> {
    let reader = xatu_reader(log_path)?;
    let mut evidence = HashMap::new();
    for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
        let row = row.map_err(|e| e.to_string())?;
        let key = (parquet_u64(&row, 1)?, parquet_u64(&row, 2)?);
        if plan
            .block_numbers
            .as_ref()
            .is_some_and(|selected| !selected.contains(&key.0))
        {
            continue;
        }
        let topic = parquet_text(&row, 7).unwrap_or_default();
        let e = evidence.entry(key).or_insert_with(XatuEvidence::default);
        if topic == ERC20_TRANSFER {
            e.transfers += 1;
        }
        if (topic == UNISWAP_V2_SWAP || topic == UNISWAP_V3_SWAP) && parquet_text(&row, 6).is_some()
        {
            e.swaps.insert(parquet_text(&row, 6).unwrap());
        }
    }
    Ok(evidence)
}

fn activity_tip_xatu_report(plan: &tlm_report::ReportPlan, out: &PathBuf) -> Result<(), String> {
    let mut blocks: HashMap<u64, XatuBlock> = HashMap::new();
    for path in xatu_plan_files(plan, "blocks")? {
        let reader = xatu_reader(&path)?;
        let base_fee_column = xatu_base_fee_column(&reader)?;
        for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
            let row = row.map_err(|e| e.to_string())?;
            let number = parquet_u64(&row, 2)?;
            if plan
                .block_numbers
                .as_ref()
                .is_some_and(|selected| !selected.contains(&number))
            {
                continue;
            }
            blocks.insert(
                number,
                XatuBlock {
                    gas_used: parquet_u64(&row, 5).unwrap_or(0),
                    base_fee: validated_base_fee(parquet_u64(&row, base_fee_column)?)?,
                    builder_signal: parquet_text(&row, 7)
                        .map(builder_name)
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "unknown".into()),
                },
            );
        }
    }
    let mut groups: std::collections::BTreeMap<String, ActivityStats> =
        std::collections::BTreeMap::new();
    let mut builders: std::collections::BTreeMap<String, BuilderStats> =
        std::collections::BTreeMap::new();
    let region_thresholds = [5u8, 10, 15, 20, 25, 30];
    let mut region_activity: std::collections::BTreeMap<
        u8,
        std::collections::BTreeMap<String, u64>,
    > = std::collections::BTreeMap::new();
    let mut region_totals: std::collections::BTreeMap<u8, u64> = std::collections::BTreeMap::new();
    let mut region_zero: std::collections::BTreeMap<u8, std::collections::BTreeMap<String, u64>> =
        std::collections::BTreeMap::new();
    let mut tx_count = 0u64;
    let mut fee_diagnostics = FeeDiagnostics::default();
    let mut fingerprint_rows: Vec<FingerprintRow> = Vec::new();
    let fingerprint_config = tlm_classify::FeeFingerprintConfig::default();
    for path in xatu_plan_files(plan, "transactions")? {
        let log_path = path
            .parent()
            .ok_or("transaction parquet has no parent directory")?
            .join("logs.parquet");
        let mut evidence = xatu_evidence(&log_path, plan)?;
        let reader = xatu_reader(&path)?;
        let mut per_block: HashMap<u64, Vec<XatuTx>> = HashMap::new();
        for row in reader.get_row_iter(None).map_err(|e| e.to_string())? {
            let row = row.map_err(|e| e.to_string())?;
            let block = parquet_u64(&row, 1)?;
            if plan
                .block_numbers
                .as_ref()
                .is_some_and(|selected| !selected.contains(&block))
            {
                continue;
            }
            let input = parquet_text(&row, 8);
            let selector = input
                .as_ref()
                .filter(|v| v.len() >= 10)
                .map(|v| v[..10].to_owned());
            per_block.entry(block).or_default().push(XatuTx {
                index: parquet_u64(&row, 2)?,
                to: parquet_text(&row, 6),
                has_calldata: input.is_some_and(|x| x.len() > 2),
                selector,
                gas_used: parquet_u64(&row, 10).unwrap_or(0),
                gas_price: parquet_fee_u64(&row, 11).unwrap_or(0),
                tx_type: parquet_u64(&row, 12).unwrap_or(0),
                max_priority_fee: parquet_u64(&row, 13).unwrap_or(0),
                max_fee: parquet_u64(&row, 14).unwrap_or(0),
            });
        }
        for (number, mut txs) in per_block {
            let Some(block) = blocks.get(&number).cloned() else {
                continue;
            };
            if block.gas_used == 0 {
                continue;
            }
            txs.sort_by_key(|tx| tx.index);
            let mut cumulative = 0u64;
            let mut builder_points = Vec::with_capacity(txs.len());
            let mut builder_tip_sum = 0.0;
            let mut builder_position_sum = 0.0;
            let mut builder_zero_tip_txs = 0u64;
            let mut builder_zero_tip_top_5 = 0u64;
            let mut builder_zero_tip_top_10 = 0u64;
            let mut builder_zero_tip_top_20 = 0u64;
            for tx in txs {
                cumulative = cumulative.saturating_add(tx.gas_used);
                if tx.tx_type >= 2 {
                    fee_diagnostics.type2_or_newer += 1;
                    if tx.max_fee > 0 && tx.gas_price == tx.max_fee {
                        fee_diagnostics.gas_price_equals_max_fee += 1;
                    }
                }
                let effective_tip = effective_tip(&tx, block.base_fee);
                if effective_tip == 0 {
                    builder_zero_tip_txs += 1;
                }
                let e = evidence.remove(&(number, tx.index)).unwrap_or_default();
                let a = tlm_classify::classify_activity(
                    tx.selector.as_deref(),
                    &tlm_classify::ActivityEvidence {
                        to: tx.to,
                        has_calldata: tx.has_calldata,
                        erc20_transfer_events: e.transfers,
                        known_swap_events: e.swaps.len() as u32,
                        distinct_swap_pools: e.swaps.len() as u32,
                        liquidation_event: false,
                        oracle_event: false,
                    },
                    &tlm_model::LabelConfig::default(),
                );
                let g = groups.entry(a.as_str().to_owned()).or_default();
                g.tips.push(effective_tip as u128);
                let position = (cumulative as f64 / block.gas_used as f64).min(1.0);
                if tx.tx_type >= 2 {
                    let base_fees = [0u64, 1, 2, 3].map(|lag| blocks.get(&number.saturating_sub(lag)).map(|candidate| candidate.base_fee));
                    fingerprint_rows.push(FingerprintRow {
                        activity: a.as_str().to_owned(),
                        builder: block.builder_signal.clone(),
                        priority_fee: tx.max_priority_fee,
                        effective_tip,
                        position,
                        fingerprint: tlm_classify::fingerprint_fee_fields(tx.tx_type, tx.max_priority_fee, tx.max_fee, base_fees, &fingerprint_config),
                    });
                }
                if effective_tip == 0 {
                    if position <= 0.05 {
                        builder_zero_tip_top_5 += 1;
                    }
                    if position <= 0.10 {
                        builder_zero_tip_top_10 += 1;
                    }
                    if position <= 0.20 {
                        builder_zero_tip_top_20 += 1;
                    }
                }
                g.positions.push(position);
                g.tip_positions.push((effective_tip as u128, position));
                for threshold in region_thresholds {
                    if position <= threshold as f64 / 100.0 {
                        *region_totals.entry(threshold).or_default() += 1;
                        *region_activity
                            .entry(threshold)
                            .or_default()
                            .entry(a.as_str().to_owned())
                            .or_default() += 1;
                        // Keep the early-region table on the same EIP-1559-aware
                        // definition as the activity and builder tables.
                        if effective_tip == 0 {
                            *region_zero
                                .entry(threshold)
                                .or_default()
                                .entry(a.as_str().to_owned())
                                .or_default() += 1;
                        }
                    }
                }
                let tip = effective_tip as f64 / 1_000_000_000.0;
                builder_points.push((tip, position));
                builder_tip_sum += tip;
                builder_position_sum += position;
                tx_count += 1;
            }
            let b = builders.entry(block.builder_signal).or_default();
            b.blocks += 1;
            b.txs += builder_points.len() as u64;
            b.zero_tip_txs += builder_zero_tip_txs;
            b.zero_tip_top_5 += builder_zero_tip_top_5;
            b.zero_tip_top_10 += builder_zero_tip_top_10;
            b.zero_tip_top_20 += builder_zero_tip_top_20;
            b.tip_sum += builder_tip_sum;
            b.position_sum += builder_position_sum;
            if let Some(correlation) = pearson(&builder_points) {
                b.correlation_sum += correlation;
                b.correlations += 1;
            }
        }
    }
    let mut markdown=format!("# Effective tip vs. activity type — Xatu public data ({})\n\nSource: Xatu canonical execution Parquet. Blocks analysed: {}. Transactions analysed: {tx_count}. Positions are cumulative receipt gas used divided by block gas used.\n\n| Activity type | Transactions | Tip < 1 wei share | Tip < 0.001 gwei share | Avg. effective tip (gwei/gas) | Avg. tip at P10 position (gwei/gas) | Avg. tip at P20 position (gwei/gas) | Avg. tip at P30 position (gwei/gas) | Avg. tip at P60 position (gwei/gas) | Avg. position | P10 position | P90 position |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n", plan.label, blocks.len());
    for (kind, mut stat) in groups {
        stat.tips.sort_unstable();
        stat.positions.sort_by(|a, b| a.total_cmp(b));
        let n = stat.tips.len();
        if n == 0 {
            continue;
        };
        let avg = stat.positions.iter().sum::<f64>() / n as f64;
        let pos = |q: f64| stat.positions[((n - 1) as f64 * q).round() as usize];
        let band = |predicate: fn(f64) -> bool| {
            let values: Vec<_> = stat
                .tip_positions
                .iter()
                .filter(|(_, position)| predicate(*position))
                .map(|(tip, _)| *tip as f64)
                .collect();
            if values.is_empty() {
                f64::NAN
            } else {
                values.iter().sum::<f64>() / values.len() as f64
            }
        };
        markdown.push_str(&format!(
            "| {kind} | {n} | {:.2}% | {:.2}% | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.4} | {:.4} | {:.4} |\n",
            100.0 * stat.tips.iter().filter(|tip| **tip < 1).count() as f64 / n as f64,
            100.0 * stat.tips.iter().filter(|tip| **tip < 1_000_000).count() as f64 / n as f64,
            stat.tips.iter().map(|tip| *tip as f64).sum::<f64>() / n as f64 / 1_000_000_000.0,
            band(|position|position<=0.10) / 1_000_000_000.0,band(|position|position>0.10 && position<=0.20) / 1_000_000_000.0,band(|position|position>0.20 && position<=0.30) / 1_000_000_000.0,band(|position|position>0.30 && position<=0.60) / 1_000_000_000.0,avg,pos(0.1),pos(0.9)
        ));
    }
    markdown.push_str(&format!("\nFee method: type-2+ effective tip = min(max priority fee, max fee − base fee); legacy transactions use gas price − base fee. Type-2+ transactions: {}; Xatu gas_price equal to max_fee: {} ({:.2}%).\n",fee_diagnostics.type2_or_newer,fee_diagnostics.gas_price_equals_max_fee,100.0*fee_diagnostics.gas_price_equals_max_fee as f64/fee_diagnostics.type2_or_newer.max(1) as f64));
    markdown.push_str("\nPrivate/public-flow columns require a public-mempool hash join. This Xatu execution-only run has no such join attached, so they are intentionally N/A; tip = 0 is a measured fee bucket, not a private-flow label.\n");
    append_fee_fingerprint_report(&mut markdown, &fingerprint_rows)?;
    markdown.push_str("\n## Builder signal and within-block tip-position correlation\n\nThe builder signal is the block extra-data string, not a relay-verified builder identity. Correlation is the mean per-block Pearson correlation between effective tip (gwei/gas) and cumulative-gas position. The early-position columns are cumulative shares of zero-tip transactions in the first 5%, 10%, and 20% of gas used in a builder's blocks. Builders are sorted by observed block count.\n\n| Builder signal | Blocks | Block market share | Transactions | Zero-tip txs | Zero-tip share | Zero-tip share, first 5% | Zero-tip share, first 10% | Zero-tip share, first 20% | Mean tip (gwei/gas) | Mean position | Mean correlation |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    let mut builder_rows: Vec<_> = builders.into_iter().collect();
    builder_rows.sort_by(|(left_name, left), (right_name, right)| {
        right
            .blocks
            .cmp(&left.blocks)
            .then_with(|| left_name.cmp(right_name))
    });
    for (builder, stats) in builder_rows {
        let mean_tip = stats.tip_sum / stats.txs.max(1) as f64;
        let mean_position = stats.position_sum / stats.txs.max(1) as f64;
        let correlation = if stats.correlations == 0 {
            f64::NAN
        } else {
            stats.correlation_sum / stats.correlations as f64
        };
        markdown.push_str(&format!(
            "| {builder} | {} | {:.2}% | {} | {} | {:.2}% | {:.2}% | {:.2}% | {:.2}% | {:.4} | {:.4} | {:.4} |\n",
            stats.blocks,
            100.0 * stats.blocks as f64 / blocks.len().max(1) as f64,
            stats.txs,
            stats.zero_tip_txs,
            100.0 * stats.zero_tip_txs as f64 / stats.txs.max(1) as f64,
            100.0 * stats.zero_tip_top_5 as f64 / stats.zero_tip_txs.max(1) as f64,
            100.0 * stats.zero_tip_top_10 as f64 / stats.zero_tip_txs.max(1) as f64,
            100.0 * stats.zero_tip_top_20 as f64 / stats.zero_tip_txs.max(1) as f64,
            mean_tip,
            mean_position,
            correlation
        ));
    }
    markdown.push_str("\n## Activity composition in early block regions\n\nEach region is the first stated percentage of cumulative gas used in every block. Shares below are within that region, not shares of all transactions.\n");
    for threshold in region_thresholds {
        let total = region_totals.get(&threshold).copied().unwrap_or(0);
        markdown.push_str(&format!("\n### First {threshold}% of block gas — {total} transactions ({:.2}% of all analysed transactions)\n\n| Activity type | Avg. tx / block | Share of region | Tip = 0 share | Private flow share | Public flow share |\n| --- | ---: | ---: | ---: | --- | --- |\n",100.0*total as f64/tx_count.max(1) as f64));
        let mut rows: Vec<_> = region_activity
            .remove(&threshold)
            .unwrap_or_default()
            .into_iter()
            .collect();
        rows.sort_by(|(left_name, left), (right_name, right)| {
            right.cmp(left).then_with(|| left_name.cmp(right_name))
        });
        for (activity, count) in rows {
            let zero = region_zero
                .get(&threshold)
                .and_then(|by_type| by_type.get(&activity))
                .copied()
                .unwrap_or(0);
            markdown.push_str(&format!(
                "| {activity} | {:.2} | {:.2}% | {:.2}% | N/A | N/A |\n",
                count as f64 / blocks.len().max(1) as f64,
                100.0 * count as f64 / total.max(1) as f64,
                100.0 * zero as f64 / count.max(1) as f64
            ));
        }
    }
    markdown.push_str("\nPrivate/public shares require a public-mempool hash join and are therefore N/A in this execution-only run. Tip = 0 is measured directly and is not a private-flow label.\n");
    markdown.push_str("\nTrace status: Xatu trace partitions for this window are schema-only (zero trace rows), so direct payments to the block author cannot yet be measured from this source.\n");
    markdown.push_str("\nClassification: a DeFi trade has a known Uniswap V2/V3 swap event; a multi-pool arbitrage candidate has known swap events from at least two distinct emitting pool contracts. This is a candidate label, not confirmed token-flow arbitrage. Liquidation and oracle labels remain unavailable until reviewed ABI event/selector registries are configured.\n");
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?
    };
    fs::write(out, markdown).map_err(|e| e.to_string())
}
