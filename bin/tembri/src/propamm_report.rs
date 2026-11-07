//! Daily PropAMM report (TODO item 18): the reviewed pipeline for one UTC
//! day, from Xatu partitions, Binance one-second klines, Mempool Dumpster
//! files and relay receipts, written as NDJSON stages and one Markdown
//! report. `tembri propamm-fetch` fetches the inputs.

use crate::{propamm_project, read_cex_series, read_dumpster_first_seen, read_ndjson, simple_config, write_ndjson, xatu_plan_files};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use tlm_model::{
    PropammBuilderPayment, PropammBuilderSummary, PropammCexMarkout, PropammEvent, PropammEventKind,
    PropammFillPair, PropammPairingResult, PropammPeriodBlock, PropammRoute, PropammSandwich,
    PropammTakerClassSummary, PropammTakerFill, PropammTxRoute, RelayBlockReceipt, RelayStatus,
};

#[derive(Debug, PartialEq)]
pub(crate) enum Traces {
    Auto,
    Required,
    Off,
}

#[derive(Debug)]
pub(crate) struct Settings {
    pub xatu_dir: PathBuf,
    pub binance_dir: PathBuf,
    pub mempool_dir: PathBuf,
    pub relay_dir: PathBuf,
    pub output_dir: PathBuf,
    pub fill_rules: PathBuf,
    registry_targets: PathBuf,
    takers: PathBuf,
    registry: String,
    pub traces: Traces,
    min_blocks: usize,
    pub relays: Vec<String>,
    uniswap_v3_pools: Vec<tlm_classify::UniswapV3Pool>,
    pub volatility_window_s: u64,
    pub volatility_cuts_bp: [f64; 2],
    pub size_cuts_usd: Vec<f64>,
    pub dates: Vec<String>,
}

/// Binance symbols the report prices fills with.
pub(crate) const KLINE_SYMBOLS: [&str; 2] = ["ETHUSDT", "USDCUSDT"];

fn list(value: &str) -> Vec<String> {
    value.split(',').map(|item| item.trim().to_string()).filter(|item| !item.is_empty()).collect()
}

fn settings(config: &HashMap<String, String>) -> Result<Settings, String> {
    let get = |key: &str| config.get(key).cloned().ok_or_else(|| format!("missing {key}"));
    let numbers = |key: &str| -> Result<Vec<f64>, String> {
        get(key)?
            .split(',')
            .map(|value| value.trim().parse::<f64>().map_err(|_| format!("{key} must be comma-separated numbers")))
            .collect()
    };
    let cuts = numbers("volatility_cuts_bp")?;
    let [low, high] = cuts[..] else {
        return Err("volatility_cuts_bp needs two cuts".into());
    };
    let size_cuts_usd = numbers("size_cuts_usd")?;
    if low >= high || size_cuts_usd.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("cuts must increase".into());
    }
    Ok(Settings {
        xatu_dir: get("xatu_dir")?.into(),
        binance_dir: get("binance_dir")?.into(),
        mempool_dir: get("mempool_dir")?.into(),
        relay_dir: get("relay_dir")?.into(),
        output_dir: get("output_dir")?.into(),
        fill_rules: get("fill_rules")?.into(),
        registry_targets: get("registry_targets")?.into(),
        takers: get("takers")?.into(),
        registry: get("registry")?.to_ascii_lowercase(),
        traces: match get("traces")?.as_str() {
            "auto" => Traces::Auto,
            "required" => Traces::Required,
            "off" => Traces::Off,
            other => return Err(format!("traces must be auto, required or off, not {other}")),
        },
        min_blocks: get("min_blocks_per_day")?.parse().map_err(|_| "invalid min_blocks_per_day")?,
        relays: list(&get("relays")?),
        uniswap_v3_pools: list(&get("uniswap_v3_pools")?)
            .iter()
            .map(|entry| {
                let (address, stable) = entry.split_once(':').ok_or_else(|| format!("uniswap_v3_pools entry {entry} needs ADDRESS:usdc|usdt"))?;
                tlm_classify::UniswapV3Pool::weth_stable(address, stable)
            })
            .collect::<Result<_, _>>()?,
        volatility_window_s: get("volatility_window_s")?.parse().map_err(|_| "invalid volatility_window_s")?,
        volatility_cuts_bp: [low, high],
        size_cuts_usd,
        dates: list(&get("dates")?),
    })
}

/// The settings of `--config`, with `--date` replacing `dates`.
pub(crate) fn load_settings(v: &[String], command: &str) -> Result<Settings, ExitCode> {
    let Some(config_path) = crate::arg(v, "--config") else {
        eprintln!("Usage: tembri {command} --config config/propamm_reports.toml [--date YYYY-MM-DD]");
        return Err(ExitCode::from(2));
    };
    let mut settings = simple_config(&config_path).and_then(|config| settings(&config)).map_err(|error| {
        eprintln!("invalid PropAMM report config: {error}");
        ExitCode::from(2)
    })?;
    if let Some(date) = v.windows(2).find(|window| window[0] == "--date").map(|window| window[1].clone()) {
        settings.dates = vec![date];
    }
    Ok(settings)
}

pub(crate) fn propamm_report_cmd(v: Vec<String>) -> ExitCode {
    let settings = match load_settings(&v, "propamm-report") {
        Ok(settings) => settings,
        Err(code) => return code,
    };
    for date in settings.dates.clone() {
        match run_day(&settings, &date) {
            Ok(path) => println!("wrote {}", path.display()),
            Err(error) => {
                eprintln!("PropAMM report for {date} failed: {error}");
                return ExitCode::from(1);
            }
        }
    }
    ExitCode::SUCCESS
}

/// Everything the Markdown report summarises for one day.
struct Day<'a> {
    date: &'a str,
    traces: bool,
    period: &'a [PropammPeriodBlock],
    events: &'a [PropammEvent],
    pairs: &'a [PropammFillPair],
    builders: &'a [PropammBuilderSummary],
    markouts: &'a [PropammCexMarkout],
    takers: &'a [PropammTakerFill],
    classes: &'a [PropammTakerClassSummary],
    routes: &'a [PropammTxRoute],
    sandwiches: &'a [PropammSandwich],
    /// ETH/USDT volatility (bp per second) at each fill block's timestamp.
    volatility: &'a HashMap<u64, Option<f64>>,
    /// Uniswap V3 swaps outside fill transactions, priced as fills.
    uniswap: &'a [PropammCexMarkout],
    /// Whether relay receipts were fetched for the day.
    relay_available: bool,
    relays: &'a [RelayStatus],
    receipts: &'a [RelayBlockReceipt],
    /// Payments of each fill transaction to its block's builder; `None` when
    /// Xatu native transfers are missing for the day.
    payments: Option<&'a [PropammBuilderPayment]>,
    /// ETH/USDT close at each period block's timestamp, for USD values.
    eth_usd: &'a HashMap<u64, f64>,
    /// Markout of each fill (transaction, log index) at its block's first
    /// relay receipt.
    at_receipt: &'a HashMap<(String, u32), f64>,
    pub volatility_cuts_bp: [f64; 2],
    size_cuts_usd: &'a [f64],
}

/// The plan of a complete UTC day: at least `min_blocks_per_day` blocks and
/// no gap.
pub(crate) fn day_plan(settings: &Settings, date: &str) -> Result<(tlm_report::ReportPlan, chrono::NaiveDate), String> {
    let period = tlm_report::ReportPeriod::parse(date)?;
    let tlm_report::ReportPeriod::UtcDay(day) = period else {
        return Err("a PropAMM report covers one UTC day".into());
    };
    let plan = tlm_report::plan(&tlm_report::ReportConfig::activity_tip(&settings.xatu_dir, ".", period))?;
    let blocks_selected = plan.block_numbers.as_ref().ok_or("plan has no block selection")?;
    let (first, last) = (
        *blocks_selected.first().ok_or("no blocks")?,
        *blocks_selected.last().ok_or("no blocks")?,
    );
    if blocks_selected.len() < settings.min_blocks || (last - first + 1) as usize != blocks_selected.len() {
        return Err(format!(
            "{} blocks from {first} to {last}: the day is incomplete (minimum {}, no gaps)",
            blocks_selected.len(),
            settings.min_blocks
        ));
    }
    Ok((plan, day))
}

fn run_day(settings: &Settings, date: &str) -> Result<PathBuf, String> {
    let (plan, day) = day_plan(settings, date)?;
    let traced = xatu_plan_files(&plan, "traces").map(|files| files.len()).unwrap_or(0) == plan.partitions.len();
    let with_traces = match settings.traces {
        Traces::Off => false,
        Traces::Auto => traced,
        Traces::Required if traced => true,
        Traces::Required => return Err("traces are required but missing for some partitions".into()),
    };

    let rules: Vec<tlm_model::PropammEventRule> = read_ndjson(&settings.fill_rules)?;
    let targets: Vec<tlm_model::PropammRegistryTarget> = read_ndjson(&settings.registry_targets)?;
    let labels: Vec<tlm_model::PropammTakerLabel> = read_ndjson(&settings.takers)?;
    let emitters: BTreeSet<String> = rules.iter().map(|rule| rule.emitter.to_ascii_lowercase()).collect();
    let (logs, calls, period_blocks, _, envelopes) =
        propamm_project::project_xatu_plan(&plan, &settings.registry, &emitters, with_traces, false)?;
    let events = tlm_classify::decode_propamm_activity(&logs, &calls, &rules, &settings.registry, &targets)?;
    let pairs = tlm_classify::add_quote_ages(&tlm_classify::pair_propamm_events(&events), &period_blocks)?;
    let blocks = tlm_classify::summarize_propamm_blocks(&events, &pairs);
    let builders = tlm_classify::add_period_blocks(&tlm_classify::summarize_propamm_builders(&blocks), &period_blocks, &blocks)?;

    let next = day.succ_opt().ok_or("date overflow")?;
    let previous = day.pred_opt().ok_or("date overflow")?;
    let klines = |symbol: &str| -> Result<String, String> {
        let paths: Vec<String> = [day, next]
            .iter()
            .map(|date| settings.binance_dir.join(format!("{symbol}-1s-{date}.csv")))
            .map(|path| {
                tlm_ingest::propamm_inputs::verify_listed(&path)?;
                Ok::<_, String>(path.display().to_string())
            })
            .collect::<Result<_, _>>()?;
        Ok(paths.join(","))
    };
    let [eth_symbol, usdc_symbol] = KLINE_SYMBOLS;
    let eth = read_cex_series(&klines(eth_symbol)?)?;
    let usdc = read_cex_series(&klines(usdc_symbol)?)?;
    let markouts = tlm_classify::propamm_cex_markouts(&events, &period_blocks, &eth, &usdc)?;
    let takers = tlm_classify::classify_propamm_takers(&events, &envelopes, &labels)?;
    let classes = tlm_classify::summarize_taker_markouts(&takers, &markouts);

    let wanted: HashSet<String> = envelopes.iter().map(|tx| tx.tx_hash.to_ascii_lowercase()).collect();
    let mut first_seen = HashMap::new();
    for date in [previous, day, next] {
        let path = settings.mempool_dir.join(format!("{date}.parquet"));
        tlm_ingest::propamm_inputs::verify_listed(&path)?;
        read_dumpster_first_seen(&path, &wanted, &mut first_seen)?;
    }
    let routes = tlm_classify::classify_propamm_routes(&envelopes, &period_blocks, &first_seen)?;
    let sandwiches = tlm_classify::find_propamm_sandwiches(&events, &envelopes)?;
    let times: HashMap<u64, u64> = period_blocks.iter().filter_map(|block| Some((block.block_number, block.timestamp?))).collect();
    let volatility: HashMap<u64, Option<f64>> = markouts
        .iter()
        .map(|row| (row.block_number, eth.return_volatility_bp(times[&row.block_number], settings.volatility_window_s)))
        .collect();

    // Uniswap V3 swaps outside PropAMM fill transactions, priced like fills.
    let pools: HashMap<&str, &tlm_classify::UniswapV3Pool> =
        settings.uniswap_v3_pools.iter().map(|pool| (pool.address.as_str(), pool)).collect();
    let pool_addresses: BTreeSet<String> = pools.keys().map(|address| address.to_string()).collect();
    let builders_of: HashMap<u64, Option<String>> =
        period_blocks.iter().map(|block| (block.block_number, block.builder.clone())).collect();
    let fill_txs: HashSet<&str> =
        events.iter().filter(|event| event.kind == PropammEventKind::Fill).map(|event| event.tx_hash.as_str()).collect();
    let mut swaps = Vec::new();
    for log in propamm_project::read_plan_logs(&plan, &pool_addresses)? {
        if !log.topic0.eq_ignore_ascii_case(tlm_classify::UNISWAP_V3_SWAP_TOPIC) || fill_txs.contains(log.tx_hash.as_str()) {
            continue;
        }
        let builder = builders_of.get(&log.block_number).cloned().flatten();
        swaps.extend(tlm_classify::uniswap_v3_fill(
            pools[log.address.as_str()],
            "uniswap_v3",
            log.block_number,
            builder,
            &log.tx_hash,
            log.tx_index,
            log.log_index,
            &log.topics,
            log.data.as_deref().unwrap_or("0x"),
        )?);
    }
    let uniswap = tlm_classify::propamm_cex_markouts(&swaps, &period_blocks, &eth, &usdc)?;

    // What fill transactions paid their block's builder.
    let fill_tx_set: HashSet<String> = fill_txs.iter().map(|hash| hash.to_ascii_lowercase()).collect();
    let payments = crate::propamm_payments::read_builder_payments(&plan, &fill_tx_set)?;
    let eth_usd: HashMap<u64, f64> = period_blocks
        .iter()
        .filter_map(|block| Some((block.block_number, eth.price_at(block.timestamp?)?.0)))
        .collect();

    // Relay receipts of the winning blocks with fills.
    // Optional: without them the Sealing section says so.
    let relay_day = settings.relay_dir.join(date);
    let relay_available = relay_day.join("relays.ndjson").is_file();
    let (relays, receipts): (Vec<RelayStatus>, Vec<RelayBlockReceipt>) = if relay_available {
        (read_ndjson(&relay_day.join("relays.ndjson"))?, read_ndjson(&relay_day.join("receipts.ndjson"))?)
    } else {
        (Vec::new(), Vec::new())
    };
    let mut received: HashMap<u64, u64> = HashMap::new();
    for receipt in &receipts {
        if let Some(time) = receipt.received_ms {
            let earliest = received.entry(receipt.block_number).or_insert(time);
            *earliest = (*earliest).min(time);
        }
    }
    let at_receipt: HashMap<(String, u32), f64> = markouts
        .iter()
        .filter_map(|row| {
            let time = received.get(&row.block_number)? / 1000;
            Some(((row.fill_tx_hash.clone(), row.log_index), tlm_classify::cex_markout_at(row, time, &eth, &usdc)?))
        })
        .collect();

    let dir = settings.output_dir.join(date);
    fs::create_dir_all(&dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let stage = |name: &str| dir.join(format!("{name}.ndjson"));
    write_ndjson(&stage("logs"), &logs)?;
    write_ndjson(&stage("calls"), &calls)?;
    write_ndjson(&stage("period_blocks"), &period_blocks)?;
    write_ndjson(&stage("txs"), &envelopes)?;
    write_ndjson(&stage("events"), &events)?;
    write_ndjson(&stage("pairs"), &pairs)?;
    write_ndjson(&stage("blocks"), &blocks)?;
    write_ndjson(&stage("builders"), &builders)?;
    write_ndjson(&stage("cex_markouts"), &markouts)?;
    write_ndjson(&stage("takers"), &takers)?;
    write_ndjson(&stage("taker_classes"), &classes)?;
    write_ndjson(&stage("routes"), &routes)?;
    write_ndjson(&stage("sandwiches"), &sandwiches)?;
    write_ndjson(&stage("uniswap_v3_markouts"), &uniswap)?;
    if let Some(payments) = &payments {
        write_ndjson(&stage("builder_payments"), payments)?;
    }
    let markdown = render(&Day {
        date,
        traces: with_traces,
        period: &period_blocks,
        events: &events,
        pairs: &pairs,
        builders: &builders,
        markouts: &markouts,
        takers: &takers,
        classes: &classes,
        routes: &routes,
        sandwiches: &sandwiches,
        volatility: &volatility,
        uniswap: &uniswap,
        relay_available,
        relays: &relays,
        receipts: &receipts,
        payments: payments.as_deref(),
        eth_usd: &eth_usd,
        at_receipt: &at_receipt,
        volatility_cuts_bp: settings.volatility_cuts_bp,
        size_cuts_usd: &settings.size_cuts_usd,
    });
    let path = settings.output_dir.join(format!("propamm_{date}.md"));
    fs::write(&path, markdown).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(path)
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

/// Nearest-rank quantile of an unsorted sample.
fn quantile(values: &[f64], q: f64) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted.get(index).copied()
}

fn bp(value: Option<f64>) -> String {
    value.map_or("n/a".into(), |value| format!("{value:.2}"))
}

fn share(part: usize, whole: usize) -> String {
    if whole == 0 {
        "n/a".into()
    } else {
        format!("{:.1}%", 100.0 * part as f64 / whole as f64)
    }
}

fn builder_label(builder: Option<&str>) -> String {
    match builder {
        Some(name) if !name.is_empty() => name.replace('|', "/"),
        _ => "(no extra-data)".into(),
    }
}

fn render<'a>(day: &Day<'a>) -> String {
    let mut out = String::new();
    let fills: Vec<&PropammEvent> = day.events.iter().filter(|event| event.kind == PropammEventKind::Fill).collect();
    let first = day.period.first().map_or(0, |block| block.block_number);
    let last = day.period.last().map_or(0, |block| block.block_number);
    let _ = writeln!(out, "# PropAMM daily report: {}\n", day.date);
    let _ = writeln!(
        out,
        "Blocks {first} to {last} ({} blocks). Fills decoded under `config/propamm_fill_rules.ndjson`; registry updates {}. Prices are Binance spot one-second last trades; horizon 0 is the block timestamp. Routes use Mempool Dumpster first sightings.\n",
        day.period.len(),
        if day.traces { "include internal calls (Xatu traces)" } else { "are top-level calls only (no traces), so internal updates are missed" }
    );

    // Activity by venue.
    let markouts_by_venue = group(day.markouts, |row| row.venue.clone());
    let pairs_by_venue = group(day.pairs, |pair| pair.venue.clone());
    let venues: BTreeMap<&str, usize> = fills.iter().fold(BTreeMap::new(), |mut counts, event| {
        *counts.entry(event.venue.as_str()).or_default() += 1;
        counts
    });
    let mut venue_order: Vec<_> = venues.iter().collect();
    venue_order.sort_by(|left, right| right.1.cmp(left.1).then(left.0.cmp(right.0)));
    // Distinct update transactions per block and pool: one signed batch can
    // carry several entries for one target.
    let mut update_txs: BTreeMap<(u64, (&str, &str)), BTreeSet<u32>> = BTreeMap::new();
    for event in day.events.iter().filter(|event| event.kind == PropammEventKind::Update) {
        update_txs
            .entry((event.block_number, (event.pool.as_str(), event.venue.as_str())))
            .or_default()
            .insert(event.tx_position);
    }
    let _ = writeln!(out, "## Activity by venue\n");
    let _ = writeln!(out, "| Venue | Fills | WETH/stable fills | Stable notional (USD) | Update transactions | Paired | Pool-blocks with 2+ update transactions |\n|---|---:|---:|---:|---:|---:|---:|");
    for (venue, count) in &venue_order {
        let rows = markouts_by_venue.get(**venue).map_or(&[][..], Vec::as_slice);
        let notional: f64 = rows.iter().map(|row| row.stable_amount).sum();
        let updates: usize = update_txs.iter().filter(|((_, pool_venue), _)| pool_venue.1 == **venue).map(|(_, txs)| txs.len()).sum();
        let pairs = pairs_by_venue.get(**venue).map_or(&[][..], Vec::as_slice);
        let paired = pairs.iter().filter(|pair| pair.result == PropammPairingResult::PropammFill).count();
        let repeated = update_txs.iter().filter(|((_, pool_venue), txs)| pool_venue.1 == **venue && txs.len() >= 2).count();
        let _ = writeln!(out, "| {venue} | {count} | {} | {notional:.0} | {updates} | {} | {repeated} |", rows.len(), share(paired, pairs.len()));
    }

    // Builders.
    let total_blocks: u32 = day.builders.iter().filter_map(|row| row.period_blocks).sum();
    let total_fills: u32 = day.builders.iter().map(|row| row.fills).sum();
    let mut builders: Vec<&PropammBuilderSummary> = day.builders.iter().filter(|row| row.fills > 0).collect();
    builders.sort_by_key(|row| std::cmp::Reverse(row.fills));
    let _ = writeln!(out, "\n## Builders\n\nBuilders with at least one fill. Names come from block extra-data.\n");
    let _ = writeln!(out, "| Builder | Blocks | Share of blocks | Fills | Share of fills | Paired |\n|---|---:|---:|---:|---:|---:|");
    for row in builders {
        let blocks = row.period_blocks.unwrap_or(0);
        let _ = writeln!(
            out,
            "| {} | {blocks} | {} | {} | {} | {} |",
            builder_label(row.builder.as_deref()),
            share(blocks as usize, total_blocks as usize),
            row.fills,
            share(row.fills as usize, total_fills as usize),
            share(row.paired_fills as usize, row.fills as usize)
        );
    }

    render_payments(&mut out, day);

    // Pairing.
    let _ = writeln!(out, "\n## Pairing\n\nA paired fill has an earlier update for its pool in the same block. Gaps are from that update to the fill.\n");
    let _ = writeln!(out, "| Venue | Fills | Without update | Same-transaction update | Median transaction gap | Median gas gap | Fills on one update (max) |\n|---|---:|---:|---:|---:|---:|---:|");
    for (venue, _) in &venue_order {
        let pairs = pairs_by_venue.get(**venue).map_or(&[][..], Vec::as_slice);
        let without = pairs.iter().filter(|pair| pair.result == PropammPairingResult::PropammFillWithoutUpdate).count();
        let same_tx = pairs.iter().filter(|pair| pair.same_tx_update).count();
        let gaps: Vec<f64> = pairs.iter().filter_map(|pair| pair.transaction_gap.map(f64::from)).collect();
        let gas: Vec<f64> = pairs.iter().filter_map(|pair| pair.gas_gap.map(|gas| gas as f64)).collect();
        let most = pairs.iter().map(|pair| pair.fills_before_next_update).max().unwrap_or(0);
        let _ = writeln!(
            out,
            "| {venue} | {} | {without} | {same_tx} | {} | {} | {most} |",
            pairs.len(),
            quantile(&gaps, 0.5).map_or("n/a".into(), |value| format!("{value:.0}")),
            quantile(&gas, 0.5).map_or("n/a".into(), |value| format!("{value:.0}"))
        );
    }

    // Markouts.
    type Horizon = fn(&PropammCexMarkout) -> Option<f64>;
    let horizons: [(&str, Horizon); 7] = [
        ("−12 s", |row| row.markout_minus_12s_bp),
        ("−4 s", |row| row.markout_minus_4s_bp),
        ("−2 s", |row| row.markout_minus_2s_bp),
        ("0 s", |row| row.markout_0s_bp),
        ("+12 s", |row| row.markout_12s_bp),
        ("+60 s", |row| row.markout_60s_bp),
        ("+300 s", |row| row.markout_300s_bp),
    ];
    let _ = writeln!(
        out,
        "\n## CEX markouts\n\n{} WETH/USDC and WETH/USDT fills. Taker-signed, in bp: positive means the taker gained.\n",
        day.markouts.len()
    );
    let _ = writeln!(out, "| Horizon | Priced | Mean | Median | P10 | P90 |\n|---|---:|---:|---:|---:|---:|");
    for (name, horizon) in horizons {
        let values: Vec<f64> = day.markouts.iter().filter_map(horizon).collect();
        let _ = writeln!(
            out,
            "| {name} | {} | {} | {} | {} | {} |",
            values.len(),
            bp(mean(&values)),
            bp(quantile(&values, 0.5)),
            bp(quantile(&values, 0.1)),
            bp(quantile(&values, 0.9))
        );
    }
    let horizon_means = |rows: &[&PropammCexMarkout]| -> String {
        [3usize, 4, 5, 6]
            .iter()
            .map(|index| bp(mean(&rows.iter().filter_map(|row| horizons[*index].1(row)).collect::<Vec<_>>())))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    let _ = writeln!(out, "\n| Group | Fills | 0 s | +12 s | +60 s | +300 s |\n|---|---:|---:|---:|---:|---:|");
    for side in ["buy_weth", "sell_weth"] {
        let rows: Vec<_> = day.markouts.iter().filter(|row| row.taker_side == side).collect();
        let _ = writeln!(out, "| {side} | {} | {} |", rows.len(), horizon_means(&rows));
    }
    for (venue, _) in &venue_order {
        let rows: Vec<&PropammCexMarkout> = markouts_by_venue.get(**venue).map_or(&[][..], Vec::as_slice).to_vec();
        if !rows.is_empty() {
            let _ = writeln!(out, "| {venue} | {} | {} |", rows.len(), horizon_means(&rows));
        }
    }

    // Taker classes and routes.
    let route_of: HashMap<&str, &PropammRoute> = day.routes.iter().map(|route| (route.tx_hash.as_str(), &route.route)).collect();
    let is_public = |hash: &str| route_of.get(hash) == Some(&&PropammRoute::PublicBeforeBlock);
    let _ = writeln!(
        out,
        "\n## Taker classes\n\nClasses from transaction structure and `config/propamm_takers.ndjson`. Fills and the public share count all fills; the move and markouts cover WETH/stablecoin fills, in bp. The 12 s move is the taker-signed CEX move before the block.\n"
    );
    let _ = writeln!(out, "| Class | Fills | Signers | 12 s move | Against the move | 0 s | +60 s | Public before block |\n|---|---:|---:|---:|---:|---:|---:|---:|");
    let mut classes: Vec<&PropammTakerClassSummary> = day.classes.iter().collect();
    classes.sort_by_key(|class| std::cmp::Reverse(class.fills));
    for class in classes {
        let fills: Vec<&PropammTakerFill> = day.takers.iter().filter(|fill| fill.class == class.class).collect();
        let public = fills.iter().filter(|fill| is_public(&fill.fill_tx_hash)).count();
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            class.class,
            fills.len(),
            class.signers,
            bp(class.pre_12s_move_bp),
            class.against_move_share.map_or("n/a".into(), |value| format!("{:.0}%", 100.0 * value)),
            bp(class.markout_0s_bp),
            bp(class.markout_60s_bp),
            share(public, fills.len())
        );
    }
    let _ = writeln!(out, "\n## Routes\n\nBy fill. Public means first seen by the archive before the block timestamp.\n");
    let _ = writeln!(out, "| Venue | Fills | Public before block | Seen after block timestamp | Not seen |\n|---|---:|---:|---:|---:|");
    for (venue, count) in &venue_order {
        let mut counts: BTreeMap<&PropammRoute, usize> = BTreeMap::new();
        for fill in fills.iter().filter(|event| event.venue == **venue) {
            if let Some(route) = route_of.get(fill.tx_hash.as_str()) {
                *counts.entry(route).or_default() += 1;
            }
        }
        let get = |route: PropammRoute| share(counts.get(&route).copied().unwrap_or(0), **count);
        let _ = writeln!(
            out,
            "| {venue} | {count} | {} | {} | {} |",
            get(PropammRoute::PublicBeforeBlock),
            get(PropammRoute::SeenAfterBlockTimestamp),
            get(PropammRoute::NotSeenPublic)
        );
    }
    let fill_hashes: HashSet<&str> = fills.iter().map(|event| event.tx_hash.as_str()).collect();
    let leads: Vec<f64> = day
        .routes
        .iter()
        .filter(|route| route.route == PropammRoute::PublicBeforeBlock && fill_hashes.contains(route.tx_hash.as_str()))
        .filter_map(|route| route.lead_ms.map(|lead| lead as f64))
        .collect();
    let _ = writeln!(
        out,
        "\nPublic fill transactions were first seen a median {} ms before the block timestamp. Same-pool sandwiches: {}.",
        quantile(&leads, 0.5).map_or("n/a".into(), |value| format!("{value:.0}")),
        day.sandwiches.len()
    );

    // Spreads.
    let [low, high] = day.volatility_cuts_bp;
    let cost = |rows: &[&PropammCexMarkout]| -> (usize, Option<f64>, Option<f64>) {
        let priced: Vec<_> = rows.iter().filter_map(|row| row.markout_0s_bp.map(|markout| (-markout, row.stable_amount))).collect();
        let notional: f64 = priced.iter().map(|(_, usd)| usd).sum();
        let weighted = (notional > 0.0).then(|| priced.iter().map(|(cost, usd)| cost * usd).sum::<f64>() / notional);
        (priced.len(), mean(&priced.iter().map(|(cost, _)| *cost).collect::<Vec<_>>()), weighted)
    };
    let _ = writeln!(
        out,
        "\n## Spreads\n\nTaker cost is the negated horizon-0 markout, in bp. Volatility is the standard deviation of ETH/USDT one-second log returns over the window before the block; clusters are cut at {low} and {high} bp.\n"
    );
    let _ = writeln!(out, "| Group | Fills | Mean cost | USD-weighted cost |\n|---|---:|---:|---:|");
    let mut line = |name: String, rows: Vec<&PropammCexMarkout>| {
        let (count, plain, weighted) = cost(&rows);
        if count > 0 {
            let _ = writeln!(out, "| {name} | {count} | {} | {} |", bp(plain), bp(weighted));
        }
    };
    let cluster = |row: &PropammCexMarkout| match day.volatility.get(&row.block_number).copied().flatten() {
        None => "volatility n/a",
        Some(value) if value < low => "low volatility",
        Some(value) if value < high => "mid volatility",
        Some(_) => "high volatility",
    };
    for name in ["low volatility", "mid volatility", "high volatility", "volatility n/a"] {
        line(name.into(), day.markouts.iter().filter(|row| cluster(row) == name).collect());
    }
    let mut lower = 0.0;
    for upper in day.size_cuts_usd.iter().copied().chain([f64::INFINITY]) {
        let name = if upper.is_infinite() { format!("over ${lower:.0}") } else { format!("${lower:.0} to ${upper:.0}") };
        line(name, day.markouts.iter().filter(|row| row.stable_amount >= lower && row.stable_amount < upper).collect());
        lower = upper;
    }
    let by_builder = group(day.markouts, |row| builder_label(row.builder.as_deref()));
    let mut builder_rows: Vec<_> = by_builder.into_iter().collect();
    builder_rows.sort_by_key(|(_, rows)| std::cmp::Reverse(rows.len()));
    for (builder, rows) in builder_rows {
        line(builder, rows);
    }

    // Uniswap V3 comparison.
    let _ = writeln!(
        out,
        "\n## Uniswap V3 comparison\n\nSwaps on the configured Uniswap V3 WETH/USDC and WETH/USDT pools, outside PropAMM fill transactions, priced like the fills. Costs include the pool fee. Large Uniswap swaps are mostly arbitrage trading the pool toward Binance, so their cost reflects that selection.\n"
    );
    let _ = writeln!(out, "| Size | PropAMM fills | PropAMM cost | Uniswap swaps | Uniswap cost |\n|---|---:|---:|---:|---:|");
    let mut lower = 0.0;
    for upper in day.size_cuts_usd.iter().copied().chain([f64::INFINITY]) {
        let name = if upper.is_infinite() { format!("over ${lower:.0}") } else { format!("${lower:.0} to ${upper:.0}") };
        let within = |rows: &'a [PropammCexMarkout]| -> Vec<&'a PropammCexMarkout> {
            rows.iter().filter(|row| row.stable_amount >= lower && row.stable_amount < upper).collect()
        };
        let (fills, fill_cost, _) = cost(&within(day.markouts));
        let (swaps, swap_cost, _) = cost(&within(day.uniswap));
        let _ = writeln!(out, "| {name} | {fills} | {} | {swaps} | {} |", bp(fill_cost), bp(swap_cost));
        lower = upper;
    }

    // Sealing.
    if !day.relay_available {
        let _ = writeln!(out, "\n## Sealing\n\nNot available: no relay receipts for this day (`tembri propamm-fetch`).");
        return out;
    }
    let answered: Vec<&str> = day.relays.iter().filter(|relay| relay.error.is_none()).map(|relay| relay.relay.as_str()).collect();
    let silent: Vec<&str> = day.relays.iter().filter(|relay| relay.error.is_some()).map(|relay| relay.relay.as_str()).collect();
    let failed = day.receipts.iter().filter(|receipt| receipt.error.is_some()).count();
    let mut received: HashMap<u64, u64> = HashMap::new();
    for receipt in day.receipts {
        if let Some(time) = receipt.received_ms {
            let earliest = received.entry(receipt.block_number).or_insert(time);
            *earliest = (*earliest).min(time);
        }
    }
    let fill_blocks: BTreeSet<u64> = fills.iter().map(|event| event.block_number).collect();
    let period_of: HashMap<u64, &PropammPeriodBlock> = day.period.iter().map(|block| (block.block_number, block)).collect();
    let mut offsets: BTreeMap<String, (usize, Vec<f64>)> = BTreeMap::new();
    for block in &fill_blocks {
        let Some(period) = period_of.get(block) else { continue };
        let offset = period
            .timestamp
            .zip(received.get(block))
            .map(|(timestamp, &time)| time as f64 - (timestamp * 1000) as f64);
        for key in ["all".to_string(), builder_label(period.builder.as_deref())] {
            let entry = offsets.entry(key).or_default();
            entry.0 += 1;
            entry.1.extend(offset);
        }
    }
    let _ = writeln!(
        out,
        "\n## Sealing\n\nThe first time an answering relay received each winning block with fills, from the relay data APIs, in ms after the block timestamp. Relays answering: {}. Not answering: {}. Failed queries: {failed}.\n",
        if answered.is_empty() { "none".into() } else { answered.join(", ") },
        if silent.is_empty() { "none".into() } else { silent.join(", ") }
    );
    let _ = writeln!(out, "| Builder | Blocks with fills | Received | Median | P10 | P90 |\n|---|---:|---:|---:|---:|---:|");
    let mut offset_rows: Vec<_> = offsets.iter().collect();
    offset_rows.sort_by_key(|(name, (blocks, _))| (name.as_str() != "all", std::cmp::Reverse(*blocks)));
    for (name, (blocks, values)) in offset_rows {
        let ms = |q: f64| quantile(values, q).map_or("n/a".into(), |value| format!("{value:.0}"));
        let _ = writeln!(out, "| {name} | {blocks} | {} | {} | {} | {} |", share(values.len(), *blocks), ms(0.5), ms(0.1), ms(0.9));
    }
    let sealed: Vec<&PropammCexMarkout> = day
        .markouts
        .iter()
        .filter(|row| day.at_receipt.contains_key(&(row.fill_tx_hash.clone(), row.log_index)))
        .collect();
    let at = |pick: &dyn Fn(&PropammCexMarkout) -> Option<f64>| bp(mean(&sealed.iter().filter_map(|row| pick(row)).collect::<Vec<_>>()));
    let _ = writeln!(
        out,
        "\nMean markout of the {} WETH/stablecoin fills in received blocks, in bp: {} at the block timestamp, {} at the first receipt, {} at +12 s. A fill's quote is fixed by the time its block reaches a relay.",
        sealed.len(),
        at(&|row| row.markout_0s_bp),
        at(&|row| day.at_receipt.get(&(row.fill_tx_hash.clone(), row.log_index)).copied()),
        at(&|row| row.markout_12s_bp)
    );
    out
}

/// Totals of one group of fill transactions, in USD.
#[derive(Default)]
struct PaymentTotals {
    txs: usize,
    notional: f64,
    priority_fee: f64,
    direct: f64,
    spread: f64,
}

/// What WETH/stablecoin fill transactions paid their block's builder,
/// against the spread their fills paid makers at horizon 0.
fn render_payments(out: &mut String, day: &Day) {
    let _ = writeln!(out, "\n## Payments to builders\n");
    let Some(payments) = day.payments else {
        let _ = writeln!(out, "Not available: Xatu `native_transfers` is missing for some partitions of the day (`tembri fetch-xatu --datasets native_transfers`).");
        return;
    };
    payment_tables(out, payments, day.markouts, day.eth_usd);
}

/// The two payment tables (all and single-swap fill transactions) for any
/// set of days: `eth_usd` maps each block to its ETH/USDT price.
pub(crate) fn payment_tables(
    out: &mut String,
    payments: &[PropammBuilderPayment],
    markouts: &[PropammCexMarkout],
    eth_usd: &HashMap<u64, f64>,
) {
    // Spread and notional of each transaction's WETH/stablecoin fills.
    let mut fills: HashMap<&str, (f64, f64)> = HashMap::new();
    for row in markouts {
        if let Some(markout) = row.markout_0s_bp {
            let entry = fills.entry(row.fill_tx_hash.as_str()).or_default();
            entry.0 += row.stable_amount;
            entry.1 += -markout / 1e4 * row.stable_amount;
        }
    }
    let fills: HashMap<String, (f64, f64)> = fills.into_iter().map(|(hash, value)| (hash.to_ascii_lowercase(), value)).collect();
    let mut all: BTreeMap<String, PaymentTotals> = BTreeMap::new();
    let mut single: BTreeMap<String, PaymentTotals> = BTreeMap::new();
    let mut unpriced = 0usize;
    for payment in payments {
        let Some(&(notional, spread)) = fills.get(&payment.tx_hash) else { continue };
        let Some(&eth) = eth_usd.get(&payment.block_number) else {
            unpriced += 1;
            continue;
        };
        let to_usd = |wei: u128| wei as f64 / 1e18 * eth;
        let builder = builder_label(payment.builder.as_deref());
        let groups: &mut [&mut BTreeMap<String, PaymentTotals>] =
            if payment.other_swap_logs == 0 { &mut [&mut all, &mut single] } else { &mut [&mut all] };
        for group in groups.iter_mut() {
            let totals = group.entry(builder.clone()).or_default();
            totals.txs += 1;
            totals.notional += notional;
            totals.priority_fee += to_usd(payment.priority_fee_wei);
            totals.direct += to_usd(payment.direct_transfer_wei);
            totals.spread += spread;
        }
    }
    let _ = writeln!(
        out,
        "WETH/stablecoin fill transactions. Priority fee = effective priority fee per gas x gas used. Direct transfers = ETH sent to the block's fee recipient inside the transaction (Xatu native transfers, including internal calls), excluding transfers from the fee recipient. ETH is valued at the Binance ETH/USDT price at the block. Spread = taker-signed horizon-0 CEX markout x notional. Payments cover the whole transaction, including any other swaps; the second table keeps transactions with no other swap log.{}\n",
        if unpriced > 0 { format!(" {unpriced} transactions without a Binance price are left out.") } else { String::new() }
    );
    for (title, group) in [("All fill transactions", &all), ("Single-swap fill transactions", &single)] {
        let _ = writeln!(out, "{title}:\n");
        let _ = writeln!(out, "| Builder | Transactions | Notional (USD) | Priority fee (USD) | Direct transfers (USD) | Total to builder (USD) | Spread to makers (USD) | Total, bp of notional |\n|---|---:|---:|---:|---:|---:|---:|---:|");
        let mut rows: Vec<_> = group.iter().collect();
        rows.sort_by(|left, right| right.1.notional.total_cmp(&left.1.notional));
        for (builder, totals) in rows {
            let total = totals.priority_fee + totals.direct;
            let _ = writeln!(
                out,
                "| {builder} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {} |",
                totals.txs,
                totals.notional,
                totals.priority_fee,
                totals.direct,
                total,
                totals.spread,
                bp((totals.notional > 0.0).then(|| total / totals.notional * 1e4))
            );
        }
        let _ = writeln!(out);
    }
}

fn group<T>(rows: &[T], key: impl Fn(&T) -> String) -> HashMap<String, Vec<&T>> {
    let mut groups: HashMap<String, Vec<&T>> = HashMap::new();
    for row in rows {
        groups.entry(key(row)).or_default().push(row);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> HashMap<String, String> {
        [
            ("dates", "2026-10-05"),
            ("xatu_dir", "x"),
            ("binance_dir", "b"),
            ("mempool_dir", "m"),
            ("relay_dir", "l"),
            ("relays", "a.example, b.example"),
            ("uniswap_v3_pools", "0x88E6A0C2DDD26FEEB64F039A2C41296FCB3F5640:usdc"),
            ("output_dir", "o"),
            ("fill_rules", "r"),
            ("registry_targets", "t"),
            ("takers", "k"),
            ("registry", "0xDA7A"),
            ("traces", "auto"),
            ("min_blocks_per_day", "6500"),
            ("volatility_window_s", "300"),
            ("volatility_cuts_bp", "0.44,0.72"),
            ("size_cuts_usd", "1000,10000"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
    }

    #[test]
    fn settings_require_every_key_and_increasing_cuts() {
        let parsed = settings(&config()).unwrap();
        assert_eq!((parsed.registry.as_str(), parsed.traces == Traces::Auto), ("0xda7a", true));
        assert_eq!(parsed.size_cuts_usd, [1000.0, 10000.0]);
        assert_eq!(parsed.relays, ["a.example", "b.example"]);
        assert_eq!(parsed.uniswap_v3_pools[0].address, "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640");
        let mut pool = config();
        pool.insert("uniswap_v3_pools".into(), "0x88e6:dai".into());
        assert!(settings(&pool).is_err());
        let mut missing = config();
        missing.remove("takers");
        assert!(settings(&missing).is_err());
        let mut reversed = config();
        reversed.insert("volatility_cuts_bp".into(), "0.72,0.44".into());
        assert!(settings(&reversed).is_err());
        let mut traces = config();
        traces.insert("traces".into(), "sometimes".into());
        assert!(settings(&traces).is_err());
    }

    #[test]
    fn renders_every_section_from_one_fill() {
        let fill = PropammEvent {
            block_number: 7,
            pool: "0xpool".into(),
            venue: "venue".into(),
            builder: Some("titan".into()),
            tx_hash: "0xfill".into(),
            tx_position: 2,
            cumulative_gas_before: 0,
            update_timestamp: None,
            kind: PropammEventKind::Fill,
            fill: None,
        };
        let markout = PropammCexMarkout {
            block_number: 7,
            block_timestamp: 100,
            venue: "venue".into(),
            pool: "0xpool".into(),
            builder: Some("titan".into()),
            fill_tx_hash: "0xfill".into(),
            fill_tx_position: 2,
            log_index: 0,
            sender: "0xs".into(),
            recipient: "0xr".into(),
            taker_side: "buy_weth".into(),
            stable: "usdc".into(),
            weth_amount: 1.0,
            stable_amount: 2000.0,
            fill_price: 2000.0,
            cex_mid_at_fill: Some(2000.0),
            cex_age_at_fill_s: Some(0),
            markout_minus_12s_bp: Some(1.0),
            markout_minus_4s_bp: None,
            markout_minus_2s_bp: None,
            markout_0s_bp: Some(-0.5),
            markout_12s_bp: None,
            markout_60s_bp: None,
            markout_300s_bp: None,
        };
        let route = PropammTxRoute {
            block_number: 7,
            tx_hash: "0xfill".into(),
            tx_position: 2,
            route: PropammRoute::PublicBeforeBlock,
            first_seen_ms: Some(99_000),
            lead_ms: Some(1_000),
        };
        let period = [PropammPeriodBlock { block_number: 7, builder: Some("titan".into()), timestamp: Some(100) }];
        let volatility = HashMap::from([(7, Some(0.5))]);
        let swap = PropammCexMarkout { venue: "uniswap_v3".into(), fill_tx_hash: "0xswap".into(), stable_amount: 500.0, markout_0s_bp: Some(-5.0), ..markout.clone() };
        let relays = [
            RelayStatus { relay: "a.example".into(), error: None },
            RelayStatus { relay: "b.example".into(), error: Some("timeout".into()) },
        ];
        let receipt = |received_ms| RelayBlockReceipt { block_number: 7, block_hash: "0xb".into(), relay: "a.example".into(), received_ms, error: None };
        let receipts = [receipt(Some(101_200)), receipt(Some(100_900))];
        let at_receipt = HashMap::from([(("0xfill".to_string(), 0), -0.7)]);
        let payment = PropammBuilderPayment {
            block_number: 7,
            tx_hash: "0xfill".into(),
            tx_position: 2,
            builder: Some("titan".into()),
            fee_recipient: "0xfee".into(),
            gas_used: 1_000_000,
            effective_priority_fee_per_gas: 2_000_000_000,
            priority_fee_wei: 2_000_000_000_000_000,
            direct_transfer_wei: 3_000_000_000_000_000,
            direct_transfers: 1,
            other_swap_logs: 0,
        };
        let eth_usd = HashMap::from([(7, 2000.0)]);
        let day = Day {
            date: "2026-10-05",
            traces: false,
            period: &period,
            events: &[fill],
            pairs: &[],
            builders: &[],
            markouts: &[markout],
            takers: &[],
            classes: &[],
            routes: &[route],
            sandwiches: &[],
            volatility: &volatility,
            uniswap: &[swap],
            relay_available: true,
            relays: &relays,
            receipts: &receipts,
            at_receipt: &at_receipt,
            volatility_cuts_bp: [0.44, 0.72],
            size_cuts_usd: &[1000.0],
            payments: Some(std::slice::from_ref(&payment)),
            eth_usd: &eth_usd,
        };
        let markdown = render(&day);
        let without_relays = render(&Day { relay_available: false, payments: None, ..day });
        assert!(without_relays.contains("## Sealing\n\nNot available: no relay receipts"));
        assert!(without_relays.contains("## Payments to builders\n\nNot available: Xatu `native_transfers`"));
        for heading in ["## Activity by venue", "## Builders", "## Pairing", "## CEX markouts", "## Taker classes", "## Routes", "## Spreads", "## Uniswap V3 comparison", "## Sealing", "## Payments to builders"] {
            assert!(markdown.contains(heading), "{heading}");
        }
        assert!(markdown.contains("| venue | 1 | 1 | 2000 |"));
        assert!(markdown.contains("| mid volatility | 1 | 0.50 | 0.50 |"));
        assert!(markdown.contains("| over $1000 | 1 | 0.50 | 0.50 |"));
        assert!(markdown.contains("| venue | 1 | 100.0% | 0.0% | 0.0% |"));
        assert!(markdown.contains("median 1000 ms"));
        assert!(markdown.contains("| $0 to $1000 | 0 | n/a | 1 | 5.00 |"));
        assert!(markdown.contains("| over $1000 | 1 | 0.50 | 0 | n/a |"));
        assert!(markdown.contains("Relays answering: a.example. Not answering: b.example."));
        assert!(markdown.contains("| all | 1 | 100.0% | 900 | 900 | 900 |"));
        assert!(markdown.contains("-0.50 at the block timestamp, -0.70 at the first receipt"));
        // $4 priority fee and $6 direct transfer on $2000; spread 0.5 bp of $2000 = $0.10.
        assert_eq!(markdown.matches("| titan | 1 | 2000 | 4 | 6 | 10 | 0 | 50.00 |").count(), 2);
    }
}
