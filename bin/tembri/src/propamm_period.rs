//! Period totals over daily PropAMM reports. `tembri propamm-report` must
//! have run for every day: this command reads its stages and does not
//! re-decode anything.

use crate::propamm_report::{load_settings, payment_tables, Settings, KLINE_SYMBOLS};
use crate::{read_cex_series, read_ndjson};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use tlm_model::{PropammBuilderPayment, PropammCexMarkout, PropammEvent, PropammEventKind, PropammPeriodBlock};

/// Every date from `from` to `to`, inclusive.
fn date_range(from: &str, to: &str) -> Result<Vec<String>, String> {
    let parse = |value: &str| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| format!("invalid date {value}"));
    let (mut day, last) = (parse(from)?, parse(to)?);
    if day > last {
        return Err("--from is after --to".into());
    }
    let mut dates = Vec::new();
    while day <= last {
        dates.push(day.to_string());
        day = day.succ_opt().ok_or("date overflow")?;
    }
    Ok(dates)
}

pub(crate) fn propamm_period_cmd(v: Vec<String>) -> ExitCode {
    let mut settings = match load_settings(&v, "propamm-period") {
        Ok(settings) => settings,
        Err(code) => return code,
    };
    let value = |name: &str| v.windows(2).find(|window| window[0] == name).map(|window| window[1].clone());
    match (value("--from"), value("--to")) {
        (Some(from), Some(to)) => match date_range(&from, &to) {
            Ok(dates) => settings.dates = dates,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::from(2);
            }
        },
        (None, None) => {}
        _ => {
            eprintln!("Usage: tembri propamm-period --config config/propamm_reports.toml [--from YYYY-MM-DD --to YYYY-MM-DD]");
            return ExitCode::from(2);
        }
    }
    match run_period(&settings, &settings.dates) {
        Ok(path) => {
            println!("wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("PropAMM period summary failed: {error}");
            ExitCode::from(1)
        }
    }
}

fn run_period(settings: &Settings, dates: &[String]) -> Result<PathBuf, String> {
    let (output_dir, binance_dir) = (&settings.output_dir, &settings.binance_dir);
    let (Some(first), Some(last)) = (dates.first(), dates.last()) else {
        return Err("no dates".into());
    };
    let mut payments: Vec<PropammBuilderPayment> = Vec::new();
    let mut markouts: Vec<PropammCexMarkout> = Vec::new();
    let mut fills: Vec<PropammEvent> = Vec::new();
    let mut eth_usd: HashMap<u64, f64> = HashMap::new();
    let mut daily: Vec<DayMarkouts> = Vec::new();
    let mut volatility: HashMap<u64, Option<f64>> = HashMap::new();
    let [eth_symbol, _] = KLINE_SYMBOLS;
    for date in dates {
        let dir = output_dir.join(date);
        let stage = |name: &str| dir.join(format!("{name}.ndjson"));
        if !stage("builder_payments").is_file() {
            return Err(format!("{} is missing; run tembri propamm-report for {date} with Xatu native transfers", stage("builder_payments").display()));
        }
        payments.extend(read_ndjson::<PropammBuilderPayment>(&stage("builder_payments"))?);
        let day_markouts: Vec<PropammCexMarkout> = read_ndjson(&stage("cex_markouts"))?;
        fills.extend(read_ndjson::<PropammEvent>(&stage("events"))?.into_iter().filter(|event| event.kind == PropammEventKind::Fill));
        let blocks: Vec<PropammPeriodBlock> = read_ndjson(&stage("period_blocks"))?;
        let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| format!("invalid date {date}"))?;
        let next = day.succ_opt().ok_or("date overflow")?;
        let previous = day.pred_opt().ok_or("date overflow")?;
        // The previous day's file, when present, covers the volatility window of the day's first blocks.
        let paths: Vec<String> = [previous, day, next]
            .iter()
            .map(|day| binance_dir.join(format!("{eth_symbol}-1s-{day}.csv")))
            .filter(|path| *path != binance_dir.join(format!("{eth_symbol}-1s-{previous}.csv")) || path.is_file())
            .map(|path| path.display().to_string())
            .collect();
        let eth = read_cex_series(&paths.join(","))?;
        eth_usd.extend(blocks.iter().filter_map(|block| Some((block.block_number, eth.price_at(block.timestamp?)?.0))));
        for row in &day_markouts {
            volatility
                .entry(row.block_number)
                .or_insert_with(|| eth.return_volatility_bp(row.block_timestamp, settings.volatility_window_s));
        }
        daily.push(day_cost(&day_markouts));
        markouts.extend(day_markouts);
    }
    let mut out = String::new();
    let _ = writeln!(out, "# PropAMM period summary: {first} to {last}\n");
    let _ = writeln!(
        out,
        "{} days, from the stages of `tembri propamm-report` in `{}`. {} fill transactions with payment rows.\n",
        dates.len(),
        output_dir.display(),
        payments.len()
    );
    let _ = writeln!(out, "## Payments to builders\n");
    payment_tables(&mut out, &payments, &markouts, &eth_usd);
    render_position(&mut out, &fills, &payments, &eth_usd);
    render_taker_cost(&mut out, &markouts, &daily, &volatility, settings.volatility_cuts_bp);
    render_cost_by_size(&mut out, &markouts, &settings.size_cuts_usd);
    let path = output_dir.join(format!("propamm_period_{first}_{last}.md"));
    fs::write(&path, out).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(path)
}

fn percent(part: usize, whole: usize) -> String {
    if whole == 0 { "n/a".into() } else { format!("{:.0}%", 100.0 * part as f64 / whole as f64) }
}

fn gwei(value: Option<f64>) -> String {
    value.map_or("n/a".into(), |value| format!("{value:.3}"))
}

/// Order after the update, and the value of the first place, over all pairs.
fn render_position(out: &mut String, fills: &[PropammEvent], payments: &[PropammBuilderPayment], eth_usd: &HashMap<u64, f64>) {
    let tests = tlm_classify::propamm_position_tests(fills, payments);
    let _ = writeln!(
        out,
        "## Order after the update\n\nAll pairs. A market-block is one block, venue and unordered token pair; each transaction counts once, at its first fill. Contested market-blocks have two or more transactions. Neighbouring transactions are compared by payment to the builder per gas. Builders sort bundles by payment, so this partly holds by construction.\n"
    );
    let _ = writeln!(out, "| Payment compared | Contested market-blocks | Neighbour pairs | Earlier pays more | Less | Equal | Median first (gwei/gas) | Median later | Median alone |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (name, test) in [
        ("priority fee only", &tests.priority_fee),
        ("priority fee + direct transfers", &tests.total),
        ("priority fee + direct transfers, first transaction single-swap", &tests.total_single_swap),
    ] {
        let _ = writeln!(
            out,
            "| {name} | {} | {} | {} | {} | {} | {} | {} | {} |",
            test.market_blocks,
            test.neighbour_pairs,
            percent(test.earlier_pays_more, test.neighbour_pairs),
            percent(test.earlier_pays_less, test.neighbour_pairs),
            percent(test.equal, test.neighbour_pairs),
            gwei(test.median_first_gwei),
            gwei(test.median_later_gwei),
            gwei(test.median_solo_gwei)
        );
    }
    let shared = tlm_classify::propamm_shared_updates(fills);
    let _ = writeln!(
        out,
        "\nFills after an earlier fill on the same market in their block (all {} fills, with or without payment rows): {} ({}); by transaction, {} of {} ({}). Market-blocks with two or more transactions: {}.",
        shared.fills,
        shared.later_fills,
        percent(shared.later_fills, shared.fills),
        shared.later_transactions,
        shared.transactions,
        percent(shared.later_transactions, shared.transactions),
        shared.contested_market_blocks
    );
    let values = tlm_classify::propamm_early_place_value(fills, payments, eth_usd);
    let _ = writeln!(
        out,
        "\n## Value of the first place\n\nAll pairs. Premium = the first transaction's payment per gas above the median of transactions alone in their market-block (same builder, venue and single-swap flag), times its gas, in USD. An upper bound, since payments cover the whole transaction.\n"
    );
    let _ = writeln!(out, "| Builder | Fill transactions | All payments to builder (USD) | Contested first places | Premium (USD) | Share of all payments |\n|---|---:|---:|---:|---:|---:|");
    let mut rows: Vec<_> = values.iter().collect();
    rows.sort_by(|left, right| right.1.all_payments_usd.total_cmp(&left.1.all_payments_usd));
    for (builder, value) in rows {
        let share = if value.all_payments_usd > 0.0 { format!("{:.0}%", 100.0 * value.premium_usd / value.all_payments_usd) } else { "n/a".into() };
        let _ = writeln!(
            out,
            "| {} | {} | {:.0} | {} | {:.0} | {share} |",
            builder.as_deref().map_or("(no extra-data)".to_string(), |name| name.replace('|', "/")),
            value.fill_transactions,
            value.all_payments_usd,
            value.contested_first_places,
            value.premium_usd
        );
    }
    let _ = writeln!(out);
}

/// Per-fill mean taker cost of one day, at horizons 0, 60 and 300 s.
#[derive(Debug, Default, PartialEq)]
struct DayMarkouts {
    cost_bp: [Option<f64>; 3],
}

fn plain_mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

/// Taker cost at a horizon: the negated markout.
fn horizon_costs(rows: &[&PropammCexMarkout], horizon: usize) -> Vec<(f64, f64)> {
    rows.iter()
        .filter_map(|row| {
            let markout = [row.markout_0s_bp, row.markout_60s_bp, row.markout_300s_bp][horizon]?;
            Some((-markout, row.stable_amount))
        })
        .collect()
}

fn day_cost(rows: &[PropammCexMarkout]) -> DayMarkouts {
    let rows: Vec<&PropammCexMarkout> = rows.iter().collect();
    let mut day = DayMarkouts::default();
    for (horizon, slot) in day.cost_bp.iter_mut().enumerate() {
        *slot = plain_mean(&horizon_costs(&rows, horizon).iter().map(|(cost, _)| *cost).collect::<Vec<_>>());
    }
    day
}

/// Mean and standard error of daily means (sample standard deviation over sqrt(days)).
fn mean_of_daily(values: &[f64]) -> (Option<f64>, Option<f64>) {
    let mean = plain_mean(values);
    let se = mean.filter(|_| values.len() > 1).map(|mean| {
        let variance = values.iter().map(|value| (value - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
        (variance / values.len() as f64).sqrt()
    });
    (mean, se)
}

/// Taker cost over the period: pooled per fill, mean of daily means (with the
/// standard error of daily means), and notional-weighted; then by volatility band.
fn render_taker_cost(
    out: &mut String,
    markouts: &[PropammCexMarkout],
    daily: &[DayMarkouts],
    volatility: &HashMap<u64, Option<f64>>,
    [low, high]: [f64; 2],
) {
    let bp = |value: Option<f64>| value.map_or("n/a".into(), |value| format!("{value:.2}"));
    let _ = writeln!(
        out,
        "## Taker cost\n\nWETH/stablecoin fills. Taker cost is the negated markout, in bp. Pooled: the mean over all fills. Mean of daily means: each day weighs the same; SE is the standard error of the daily means. Weighted: by stablecoin notional.\n"
    );
    let _ = writeln!(out, "| Horizon | Fills | Pooled per fill | Mean of daily means | SE | Notional-weighted |\n|---|---:|---:|---:|---:|---:|");
    let rows: Vec<&PropammCexMarkout> = markouts.iter().collect();
    for (horizon, name) in ["0 s", "60 s", "300 s"].iter().enumerate() {
        let costs = horizon_costs(&rows, horizon);
        let notional: f64 = costs.iter().map(|(_, usd)| usd).sum();
        let weighted = (notional > 0.0).then(|| costs.iter().map(|(cost, usd)| cost * usd).sum::<f64>() / notional);
        let days: Vec<f64> = daily.iter().filter_map(|day| day.cost_bp[horizon]).collect();
        let (mean, se) = mean_of_daily(&days);
        let pooled = plain_mean(&costs.iter().map(|(cost, _)| *cost).collect::<Vec<_>>());
        let _ = writeln!(out, "| {name} | {} | {} | {} | {} | {} |", costs.len(), bp(pooled), bp(mean), bp(se), bp(weighted));
    }
    let _ = writeln!(
        out,
        "\nBy volatility at the fill block: the standard deviation of ETH/USDT one-second log returns over the window before it, cut at {low} and {high} bp (the configured terciles).\n"
    );
    let _ = writeln!(out, "| Volatility | Fills | Pooled per fill | Notional-weighted |\n|---|---:|---:|---:|");
    for name in ["low", "mid", "high", "n/a"] {
        let band: Vec<&PropammCexMarkout> = markouts
            .iter()
            .filter(|row| {
                let band = match volatility.get(&row.block_number).copied().flatten() {
                    None => "n/a",
                    Some(value) if value < low => "low",
                    Some(value) if value < high => "mid",
                    Some(_) => "high",
                };
                band == name
            })
            .collect();
        let costs = horizon_costs(&band, 0);
        if costs.is_empty() {
            continue;
        }
        let notional: f64 = costs.iter().map(|(_, usd)| usd).sum();
        let weighted = (notional > 0.0).then(|| costs.iter().map(|(cost, usd)| cost * usd).sum::<f64>() / notional);
        let pooled = plain_mean(&costs.iter().map(|(cost, _)| *cost).collect::<Vec<_>>());
        let _ = writeln!(out, "| {name} | {} | {} | {} |", costs.len(), bp(pooled), bp(weighted));
    }
    let _ = writeln!(out);
}

/// One size band: fills priced at horizon 0, their notional, and the taker
/// cost (the negated horizon-0 markout) per fill and weighted by notional.
#[derive(Debug, PartialEq)]
struct SizeBand {
    name: String,
    fills: usize,
    notional_usd: f64,
    mean_cost_bp: Option<f64>,
    weighted_cost_bp: Option<f64>,
}

fn size_bands(markouts: &[PropammCexMarkout], cuts: &[f64]) -> Vec<SizeBand> {
    let band = |name: String, rows: Vec<&PropammCexMarkout>| {
        let priced: Vec<(f64, f64)> = rows.iter().filter_map(|row| row.markout_0s_bp.map(|markout| (-markout, row.stable_amount))).collect();
        let notional_usd: f64 = priced.iter().map(|(_, usd)| usd).sum();
        let fills = priced.len();
        SizeBand {
            name,
            fills,
            notional_usd,
            mean_cost_bp: (fills > 0).then(|| priced.iter().map(|(cost, _)| cost).sum::<f64>() / fills as f64),
            weighted_cost_bp: (notional_usd > 0.0).then(|| priced.iter().map(|(cost, usd)| cost * usd).sum::<f64>() / notional_usd),
        }
    };
    let mut bands = Vec::new();
    let mut lower = 0.0;
    for upper in cuts.iter().copied().chain([f64::INFINITY]) {
        let name = if upper.is_infinite() { format!("over ${lower:.0}") } else { format!("${lower:.0} to ${upper:.0}") };
        bands.push(band(name, markouts.iter().filter(|row| row.stable_amount >= lower && row.stable_amount < upper).collect()));
        lower = upper;
    }
    bands.push(band("all".into(), markouts.iter().collect()));
    bands
}

/// Taker cost by fill size, per fill and notional-weighted, over the period.
fn render_cost_by_size(out: &mut String, markouts: &[PropammCexMarkout], cuts: &[f64]) {
    let _ = writeln!(
        out,
        "## Cost by size\n\nWETH/stablecoin fills priced at horizon 0. Taker cost is the negated horizon-0 markout, in bp. The per-fill mean and the notional-weighted mean differ when large fills pay a different cost.\n"
    );
    let _ = writeln!(out, "| Size | Fills | Notional (USD) | Share of notional | Mean cost per fill | Notional-weighted cost |\n|---|---:|---:|---:|---:|---:|");
    let bands = size_bands(markouts, cuts);
    let total = bands.last().map_or(0.0, |band| band.notional_usd);
    for band in &bands {
        let share = if total > 0.0 { format!("{:.1}%", 100.0 * band.notional_usd / total) } else { "n/a".into() };
        let bp = |value: Option<f64>| value.map_or("n/a".into(), |value| format!("{value:.2}"));
        let _ = writeln!(
            out,
            "| {} | {} | {:.0} | {share} | {} | {} |",
            band.name,
            band.fills,
            band.notional_usd,
            bp(band.mean_cost_bp),
            bp(band.weighted_cost_bp)
        );
    }
    let _ = writeln!(out);
}

#[cfg(test)]
mod tests {
    use super::{date_range, mean_of_daily, size_bands};
    use tlm_model::PropammCexMarkout;

    fn markout(stable_amount: f64, markout_0s_bp: Option<f64>) -> PropammCexMarkout {
        PropammCexMarkout {
            block_number: 1,
            block_timestamp: 0,
            venue: "v".into(),
            pool: "p".into(),
            builder: None,
            fill_tx_hash: "0x1".into(),
            fill_tx_position: 0,
            log_index: 0,
            sender: "s".into(),
            recipient: "r".into(),
            taker_side: "buy_weth".into(),
            stable: "usdc".into(),
            weth_amount: 0.0,
            stable_amount,
            fill_price: 0.0,
            cex_mid_at_fill: None,
            cex_age_at_fill_s: None,
            markout_minus_12s_bp: None,
            markout_minus_4s_bp: None,
            markout_minus_2s_bp: None,
            markout_0s_bp,
            markout_12s_bp: None,
            markout_60s_bp: None,
            markout_300s_bp: None,
        }
    }

    #[test]
    fn mean_of_daily_means_and_its_standard_error() {
        let (mean, se) = mean_of_daily(&[1.0, 2.0, 3.0]);
        assert_eq!(mean, Some(2.0));
        assert!((se.unwrap() - (1.0f64 / 3.0).sqrt()).abs() < 1e-12);
        assert_eq!(mean_of_daily(&[1.0]), (Some(1.0), None));
    }

    #[test]
    fn size_bands_separate_per_fill_and_weighted_cost() {
        // Two small fills at 0.5 bp and one large fill at 2 bp: per fill the mean is 1.0,
        // weighted by notional it is close to the large fill's cost. Unpriced fills are left out.
        let rows = [markout(100.0, Some(-0.5)), markout(300.0, Some(-0.5)), markout(200_000.0, Some(-2.0)), markout(500.0, None)];
        let bands = size_bands(&rows, &[1000.0, 100_000.0]);
        assert_eq!(bands.iter().map(|band| band.name.as_str()).collect::<Vec<_>>(), ["$0 to $1000", "$1000 to $100000", "over $100000", "all"]);
        assert_eq!((bands[0].fills, bands[1].fills, bands[2].fills, bands[3].fills), (2, 0, 1, 3));
        assert_eq!(bands[0].mean_cost_bp, Some(0.5));
        assert_eq!(bands[1].mean_cost_bp, None);
        let all = &bands[3];
        assert!((all.mean_cost_bp.unwrap() - 1.0).abs() < 1e-12);
        assert!((all.weighted_cost_bp.unwrap() - (0.5 * 400.0 + 2.0 * 200_000.0) / 200_400.0).abs() < 1e-12);
    }

    #[test]
    fn date_range_is_inclusive_and_ordered() {
        assert_eq!(date_range("2026-09-30", "2026-10-02").unwrap(), ["2026-09-30", "2026-10-01", "2026-10-02"]);
        assert!(date_range("2026-10-02", "2026-09-30").is_err());
    }
}
