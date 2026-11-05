//! Period totals over daily PropAMM reports. `tembri propamm-report` must
//! have run for every day: this command reads its stages and does not
//! re-decode anything.

use crate::propamm_report::{load_settings, payment_tables, KLINE_SYMBOLS};
use crate::{read_cex_series, read_ndjson};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
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
    match run_period(&settings.output_dir, &settings.binance_dir, &settings.dates) {
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

fn run_period(output_dir: &Path, binance_dir: &Path, dates: &[String]) -> Result<PathBuf, String> {
    let (Some(first), Some(last)) = (dates.first(), dates.last()) else {
        return Err("no dates".into());
    };
    let mut payments: Vec<PropammBuilderPayment> = Vec::new();
    let mut markouts: Vec<PropammCexMarkout> = Vec::new();
    let mut fills: Vec<PropammEvent> = Vec::new();
    let mut eth_usd: HashMap<u64, f64> = HashMap::new();
    let [eth_symbol, _] = KLINE_SYMBOLS;
    for date in dates {
        let dir = output_dir.join(date);
        let stage = |name: &str| dir.join(format!("{name}.ndjson"));
        if !stage("builder_payments").is_file() {
            return Err(format!("{} is missing; run tembri propamm-report for {date} with Xatu native transfers", stage("builder_payments").display()));
        }
        payments.extend(read_ndjson::<PropammBuilderPayment>(&stage("builder_payments"))?);
        markouts.extend(read_ndjson::<PropammCexMarkout>(&stage("cex_markouts"))?);
        fills.extend(read_ndjson::<PropammEvent>(&stage("events"))?.into_iter().filter(|event| event.kind == PropammEventKind::Fill));
        let blocks: Vec<PropammPeriodBlock> = read_ndjson(&stage("period_blocks"))?;
        let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| format!("invalid date {date}"))?;
        let next = day.succ_opt().ok_or("date overflow")?;
        let paths: Vec<String> =
            [day, next].iter().map(|day| binance_dir.join(format!("{eth_symbol}-1s-{day}.csv")).display().to_string()).collect();
        let eth = read_cex_series(&paths.join(","))?;
        eth_usd.extend(blocks.iter().filter_map(|block| Some((block.block_number, eth.price_at(block.timestamp?)?.0))));
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

#[cfg(test)]
mod tests {
    use super::date_range;

    #[test]
    fn date_range_is_inclusive_and_ordered() {
        assert_eq!(date_range("2026-09-30", "2026-10-02").unwrap(), ["2026-09-30", "2026-10-01", "2026-10-02"]);
        assert!(date_range("2026-10-02", "2026-09-30").is_err());
    }
}
