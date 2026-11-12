//! How a fill's price depends on its place in the block, and what calls into
//! PropAMM pools were refused.
//!
//! A *priced market-block* is one block, one pool and one stablecoin (WETH/USDC
//! and WETH/USDT in one pool are different markets). Its first fill sets the
//! side. A *later fill* is a fill on the same side in a different transaction
//! from every earlier fill of the market-block: fills that share a transaction
//! with an earlier one are one order split by a router, not a competitor for
//! place, but their notional counts as taken. The *price gap* is the later
//! fill's price against the first fill's, in bp, signed so that a positive gap
//! means the later fill paid more.

use std::collections::{BTreeMap, HashMap, HashSet};
use tlm_model::{PropammCexMarkout, PropammEvent, PropammEventKind, PropammPoolCall};

/// Regressor scale: coefficients are per $10,000 of notional.
pub const GAP_FIT_SCALE_USD: f64 = 10_000.0;

/// A gap at or below this is "the first fill's price" (bp).
pub const SAME_PRICE_BP: f64 = 0.01;

#[derive(Clone, Debug, PartialEq)]
pub struct LaterFill {
    pub venue: String,
    /// 2 for the first competitor after the first fill, and so on.
    pub place: u32,
    /// Notional of earlier fills on the same side, including the first fill
    /// and fills split from earlier transactions (USD).
    pub prior_same_usd: f64,
    /// Notional of earlier fills on the other side (USD).
    pub prior_opposite_usd: f64,
    pub own_usd: f64,
    pub first_usd: f64,
    pub gap_bp: f64,
}

pub fn propamm_later_fills(markouts: &[PropammCexMarkout]) -> Vec<LaterFill> {
    let mut markets: BTreeMap<(u64, &str, &str), Vec<&PropammCexMarkout>> = BTreeMap::new();
    for row in markouts {
        markets.entry((row.block_number, row.pool.as_str(), row.stable.as_str())).or_default().push(row);
    }
    let mut later = Vec::new();
    for rows in markets.values_mut() {
        rows.sort_by_key(|row| (row.fill_tx_position, row.log_index));
        let first = rows[0];
        if !(first.fill_price.is_finite() && first.fill_price > 0.0) {
            continue;
        }
        let sign = if first.taker_side == "buy_weth" { 1.0 } else { -1.0 };
        let (mut prior_same, mut prior_opposite, mut place) = (first.stable_amount, 0.0, 1);
        let mut transactions: HashSet<&str> = HashSet::from([first.fill_tx_hash.as_str()]);
        for row in &rows[1..] {
            if row.taker_side != first.taker_side {
                prior_opposite += row.stable_amount;
                continue;
            }
            if !transactions.insert(row.fill_tx_hash.as_str()) {
                prior_same += row.stable_amount;
                continue;
            }
            place += 1;
            if row.fill_price.is_finite() {
                later.push(LaterFill {
                    venue: row.venue.clone(),
                    place,
                    prior_same_usd: prior_same,
                    prior_opposite_usd: prior_opposite,
                    own_usd: row.stable_amount,
                    first_usd: first.stable_amount,
                    gap_bp: sign * (row.fill_price - first.fill_price) / first.fill_price * 1e4,
                });
            }
            prior_same += row.stable_amount;
        }
    }
    later
}

/// Least squares of the gap on own, first, prior same-side and prior
/// opposite-side notional (per $10k), with an intercept. A regressor that
/// does not vary is left out (`None`).
#[derive(Clone, Debug, PartialEq)]
pub struct GapFit {
    /// (estimate, standard error) for intercept, own, first, prior same side,
    /// prior opposite side.
    pub terms: [Option<(f64, f64)>; 5],
}

pub fn fit_price_gap(rows: &[&LaterFill]) -> Option<GapFit> {
    let per = |usd: f64| usd / GAP_FIT_SCALE_USD;
    let all: Vec<[f64; 5]> = rows
        .iter()
        .map(|row| [1.0, per(row.own_usd), per(row.first_usd), per(row.prior_same_usd), per(row.prior_opposite_usd)])
        .collect();
    let used: Vec<usize> = (0..5)
        .filter(|&column| column == 0 || all.iter().any(|x| x[column] != all[0][column]))
        .collect();
    let k = used.len();
    if rows.len() <= k {
        return None;
    }
    let mut xtx = vec![vec![0.0; k]; k];
    let mut xty = vec![0.0; k];
    for (x, row) in all.iter().zip(rows) {
        for (i, &a) in used.iter().enumerate() {
            xty[i] += x[a] * row.gap_bp;
            for (j, &b) in used.iter().enumerate() {
                xtx[i][j] += x[a] * x[b];
            }
        }
    }
    let inverse = invert(xtx)?;
    let beta: Vec<f64> = (0..k).map(|i| (0..k).map(|j| inverse[i][j] * xty[j]).sum()).collect();
    let residual: f64 = all
        .iter()
        .zip(rows)
        .map(|(x, row)| {
            let fitted: f64 = used.iter().enumerate().map(|(i, &a)| beta[i] * x[a]).sum();
            (row.gap_bp - fitted).powi(2)
        })
        .sum();
    let variance = residual / (rows.len() - k) as f64;
    let mut terms = [None; 5];
    for (i, &column) in used.iter().enumerate() {
        terms[column] = Some((beta[i], (variance * inverse[i][i]).max(0.0).sqrt()));
    }
    Some(GapFit { terms })
}

/// Gauss-Jordan inverse with partial pivoting; `None` when singular.
fn invert(mut a: Vec<Vec<f64>>) -> Option<Vec<Vec<f64>>> {
    let n = a.len();
    let mut inverse: Vec<Vec<f64>> = (0..n).map(|i| (0..n).map(|j| if i == j { 1.0 } else { 0.0 }).collect()).collect();
    for column in 0..n {
        let pivot = (column..n).max_by(|&x, &y| a[x][column].abs().total_cmp(&a[y][column].abs()))?;
        if a[pivot][column].abs() < 1e-12 {
            return None;
        }
        a.swap(column, pivot);
        inverse.swap(column, pivot);
        let scale = a[column][column];
        for j in 0..n {
            a[column][j] /= scale;
            inverse[column][j] /= scale;
        }
        for row in 0..n {
            if row != column {
                let factor = a[row][column];
                for j in 0..n {
                    a[row][j] -= factor * a[column][j];
                    inverse[row][j] -= factor * inverse[column][j];
                }
            }
        }
    }
    Some(inverse)
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len() % 2 == 1 { values[middle] } else { (values[middle - 1] + values[middle]) / 2.0 })
}

/// Count, median and mean of a set of gaps.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GapStats {
    pub fills: usize,
    pub median_bp: Option<f64>,
    pub mean_bp: Option<f64>,
}

fn stats(gaps: impl Iterator<Item = f64>) -> GapStats {
    let mut values: Vec<f64> = gaps.collect();
    let mean_bp = (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64);
    GapStats { fills: values.len(), median_bp: median(&mut values), mean_bp }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GapSummary {
    pub all: GapStats,
    /// Weighted by the later fill's own notional.
    pub weighted_bp: Option<f64>,
    /// Share of later fills within `SAME_PRICE_BP` of the first fill's price.
    pub same_price_share: Option<f64>,
    /// Places 2, 3, and 4 or later.
    pub by_place: [GapStats; 3],
    /// By prior same-side notional, between the cuts (USD).
    pub by_prior: Vec<(String, GapStats)>,
    pub fit: Option<GapFit>,
}

pub fn summarize_price_gaps(later: &[LaterFill], prior_cuts_usd: &[f64]) -> BTreeMap<String, GapSummary> {
    let mut by_venue: BTreeMap<&str, Vec<&LaterFill>> = BTreeMap::new();
    for row in later {
        by_venue.entry(row.venue.as_str()).or_default().push(row);
    }
    by_venue
        .into_iter()
        .map(|(venue, rows)| {
            let notional: f64 = rows.iter().map(|row| row.own_usd).sum();
            let place = |low: u32, high: u32| stats(rows.iter().filter(|row| (low..=high).contains(&row.place)).map(|row| row.gap_bp));
            let mut by_prior = Vec::new();
            let mut lower = 0.0;
            for upper in prior_cuts_usd.iter().copied().chain([f64::INFINITY]) {
                let name = if upper.is_infinite() { format!("over ${lower:.0}") } else { format!("${lower:.0} to ${upper:.0}") };
                by_prior.push((name, stats(rows.iter().filter(|row| row.prior_same_usd >= lower && row.prior_same_usd < upper).map(|row| row.gap_bp))));
                lower = upper;
            }
            let summary = GapSummary {
                all: stats(rows.iter().map(|row| row.gap_bp)),
                weighted_bp: (notional > 0.0).then(|| rows.iter().map(|row| row.gap_bp * row.own_usd).sum::<f64>() / notional),
                same_price_share: (!rows.is_empty())
                    .then(|| rows.iter().filter(|row| row.gap_bp.abs() <= SAME_PRICE_BP).count() as f64 / rows.len() as f64),
                by_place: [place(2, 2), place(3, 3), place(4, u32::MAX)],
                by_prior,
                fit: fit_price_gap(&rows),
            };
            (venue.to_string(), summary)
        })
        .collect()
}

/// Names of custom-error selectors seen in reverted calls into PropAMM pools,
/// from the openchain signature database (looked up 2026-11-10). A selector
/// can have more than one name; these are the only ones listed.
const ERROR_NAMES: [(&str, &str); 8] = [
    ("0x666a2814", "StaleUpdate()"),
    ("0x9c3d724b", "StaleBook()"),
    ("0x4617192b", "UnsupportedPair()"),
    ("0x0deb63e7", "NoActiveVenue(address,address)"),
    ("0x3016795c", "UnsupportedPair(address,address)"),
    ("0xa343d5e0", "InsufficientVaultBalance(address,uint256,uint256)"),
    ("0xbb55fd27", "InsufficientLiquidity()"),
    ("0xb830c139", "InactivePair(address,address)"),
];

/// Revert reason from a frame's return data (hex, with or without `0x`).
/// `None` for empty return data.
pub fn revert_reason(output: &str) -> Option<String> {
    let hex = output.trim().trim_start_matches("0x").to_ascii_lowercase();
    if hex.len() < 8 {
        return None;
    }
    let selector = format!("0x{}", &hex[..8]);
    let word = |index: usize| hex.get(8 + 64 * index..8 + 64 * (index + 1));
    match selector.as_str() {
        "0x08c379a0" => {
            // ABI string: offset word, length word, then the bytes. The length fits in the low 8 bytes.
            let length = usize::from_str_radix(&word(1)?[48..], 16).ok()?;
            let text = hex.get(8 + 128..8 + 128 + 2 * length)?;
            let bytes: Option<Vec<u8>> = (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok()).collect();
            Some(format!("Error(\"{}\")", String::from_utf8_lossy(&bytes?)))
        }
        "0x4e487b71" => {
            let code = word(0)?.trim_start_matches('0');
            Some(format!("Panic(0x{})", if code.is_empty() { "0" } else { code }))
        }
        _ => Some(ERROR_NAMES.iter().find(|(known, _)| *known == selector).map_or(selector.clone(), |(_, name)| name.to_string())),
    }
}

/// Revert reasons that say the pool lacked the depth for the fill.
pub const DEPTH_REASONS: [&str; 2] = ["InsufficientLiquidity()", "InsufficientVaultBalance(address,uint256,uint256)"];

/// Reverted frames placed against successful fills on the same pool in the block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ByPlace {
    pub no_fill_in_block: usize,
    pub before_first_fill: usize,
    /// In a transaction that also filled on that pool.
    pub with_fill: usize,
    /// In a transaction after an earlier fill on that pool.
    pub after_fill: usize,
}

/// Calls into one venue's pools over a period.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PoolCallSummary {
    pub calls: usize,
    pub reverted: usize,
    /// Reverted frames inside transactions that still succeeded.
    pub caught: usize,
    /// All reverted frames by place.
    pub by_place: ByPlace,
    /// Reverted frames with a reason in `DEPTH_REASONS`, by place.
    pub depth: ByPlace,
    pub reasons: BTreeMap<String, usize>,
    /// Reasons of reverted frames after or with a fill on the same pool.
    pub late_reasons: BTreeMap<String, usize>,
}

pub fn summarize_pool_calls(calls: &[PropammPoolCall], events: &[PropammEvent]) -> BTreeMap<String, PoolCallSummary> {
    let mut fills: HashMap<(u64, &str), Vec<u32>> = HashMap::new();
    for event in events.iter().filter(|event| event.kind == PropammEventKind::Fill) {
        fills.entry((event.block_number, event.pool.as_str())).or_default().push(event.tx_position);
    }
    let mut summaries: BTreeMap<String, PoolCallSummary> = BTreeMap::new();
    for call in calls {
        let summary = summaries.entry(call.venue.clone()).or_default();
        summary.calls += 1;
        if !call.reverted {
            continue;
        }
        summary.reverted += 1;
        summary.caught += usize::from(call.tx_succeeded);
        let reason = call.revert_reason.clone().unwrap_or_else(|| "(no data)".into());
        *summary.reasons.entry(reason.clone()).or_default() += 1;
        let positions = fills.get(&(call.block_number, call.pool.as_str()));
        let depth = DEPTH_REASONS.contains(&reason.as_str());
        let mut late = false;
        for place in [&mut summary.by_place].into_iter().chain(depth.then_some(&mut summary.depth)) {
            match positions {
                None => place.no_fill_in_block += 1,
                Some(positions) if positions.contains(&call.tx_position) => {
                    place.with_fill += 1;
                    late = true;
                }
                Some(positions) if positions.iter().any(|&position| position < call.tx_position) => {
                    place.after_fill += 1;
                    late = true;
                }
                Some(_) => place.before_first_fill += 1,
            }
        }
        if late {
            *summary.late_reasons.entry(reason).or_default() += 1;
        }
    }
    summaries
}

#[cfg(test)]
mod tests {
    use super::*;

    fn priced(block: u64, tx: &str, position: u32, side: &str, stable: &str, usd: f64, price: f64) -> PropammCexMarkout {
        PropammCexMarkout {
            block_number: block,
            block_timestamp: 0,
            venue: "v".into(),
            pool: "0xpool".into(),
            builder: None,
            fill_tx_hash: tx.into(),
            fill_tx_position: position,
            log_index: position,
            sender: "s".into(),
            recipient: "r".into(),
            taker_side: side.into(),
            stable: stable.into(),
            weth_amount: usd / price,
            stable_amount: usd,
            fill_price: price,
            cex_mid_at_fill: None,
            cex_age_at_fill_s: None,
            markout_minus_12s_bp: None,
            markout_minus_4s_bp: None,
            markout_minus_2s_bp: None,
            markout_0s_bp: None,
            markout_12s_bp: None,
            markout_60s_bp: None,
            markout_300s_bp: None,
        }
    }

    #[test]
    fn later_fills_skip_split_orders_and_other_markets_and_sign_the_gap() {
        let rows = [
            // Block 1, USDC market: a buys at 2000, a again (split), b sells (other side), c buys at 2000.2.
            priced(1, "0xa", 1, "buy_weth", "usdc", 1000.0, 2000.0),
            priced(1, "0xa", 2, "buy_weth", "usdc", 500.0, 2000.1),
            priced(1, "0xb", 3, "sell_weth", "usdc", 300.0, 1999.0),
            priced(1, "0xc", 4, "buy_weth", "usdc", 2000.0, 2000.2),
            // Block 1, USDT market in the same pool: its own first fill.
            priced(1, "0xd", 5, "buy_weth", "usdt", 100.0, 1990.0),
            // Block 2: sells; a lower later price is worse for the seller.
            priced(2, "0xe", 1, "sell_weth", "usdc", 100.0, 2000.0),
            priced(2, "0xf", 2, "sell_weth", "usdc", 100.0, 1999.8),
        ];
        let later = propamm_later_fills(&rows);
        assert_eq!(later.len(), 2);
        let c = &later[0];
        assert_eq!((c.place, c.prior_same_usd, c.prior_opposite_usd, c.own_usd, c.first_usd), (2, 1500.0, 300.0, 2000.0, 1000.0));
        assert!((c.gap_bp - 1.0).abs() < 1e-9);
        assert!((later[1].gap_bp - 1.0).abs() < 1e-9);
    }

    #[test]
    fn fit_recovers_a_linear_gap_and_drops_constant_regressors() {
        // gap = 0.1 + 0.5 * own + 0.3 * prior (per $10k); first and opposite constant.
        let rows: Vec<LaterFill> = (0..20)
            .map(|i| {
                let (own, prior) = (1000.0 * (i % 5) as f64, 2500.0 * (i / 5) as f64);
                LaterFill {
                    venue: "v".into(),
                    place: 2,
                    prior_same_usd: prior,
                    prior_opposite_usd: 0.0,
                    own_usd: own,
                    first_usd: 500.0,
                    gap_bp: 0.1 + 0.5 * own / 1e4 + 0.3 * prior / 1e4,
                }
            })
            .collect();
        let fit = fit_price_gap(&rows.iter().collect::<Vec<_>>()).unwrap();
        let estimate = |term: usize| fit.terms[term].unwrap().0;
        assert!((estimate(0) - 0.1).abs() < 1e-9 && (estimate(1) - 0.5).abs() < 1e-9 && (estimate(3) - 0.3).abs() < 1e-9);
        assert_eq!((fit.terms[2], fit.terms[4]), (None, None));
        let summary = &summarize_price_gaps(&rows, &[1000.0])["v"];
        assert_eq!(summary.all.fills, 20);
        assert_eq!(summary.by_prior.iter().map(|(_, stats)| stats.fills).collect::<Vec<_>>(), [5, 15]);
    }

    #[test]
    fn decodes_revert_reasons() {
        let error_sp = "0x08c379a0\
            0000000000000000000000000000000000000000000000000000000000000020\
            0000000000000000000000000000000000000000000000000000000000000002\
            5350000000000000000000000000000000000000000000000000000000000000";
        assert_eq!(revert_reason(error_sp).as_deref(), Some("Error(\"SP\")"));
        assert_eq!(
            revert_reason("0x4e487b710000000000000000000000000000000000000000000000000000000000000011").as_deref(),
            Some("Panic(0x11)")
        );
        assert_eq!(revert_reason("0x666a2814").as_deref(), Some("StaleUpdate()"));
        assert_eq!(revert_reason("0xf4f85db4").as_deref(), Some("0xf4f85db4"));
        assert_eq!(revert_reason(""), None);
    }

    #[test]
    fn pool_calls_sort_reverts_by_their_place_against_fills() {
        let fill = |position: u32| PropammEvent {
            block_number: 1,
            pool: "0xpool".into(),
            venue: "v".into(),
            builder: None,
            tx_hash: format!("0x{position}"),
            tx_position: position,
            cumulative_gas_before: 0,
            update_timestamp: None,
            kind: PropammEventKind::Fill,
            fill: None,
        };
        let call = |block: u64, position: u32, reverted: bool, succeeded: bool, reason: Option<&str>| PropammPoolCall {
            block_number: block,
            tx_hash: format!("0xc{position}"),
            tx_position: position,
            caller: "0xrouter".into(),
            pool: "0xpool".into(),
            venue: "v".into(),
            reverted,
            tx_succeeded: succeeded,
            revert_reason: reason.map(str::to_string),
        };
        let calls = [
            call(1, 5, false, true, None),
            call(1, 3, true, true, Some("StaleUpdate()")),
            call(1, 5, true, true, Some("Error(\"SP\")")),
            call(1, 9, true, false, Some("InsufficientLiquidity()")),
            call(2, 1, true, true, Some("StaleUpdate()")),
        ];
        let summary = &summarize_pool_calls(&calls, &[fill(5)])["v"];
        assert_eq!((summary.calls, summary.reverted, summary.caught), (5, 4, 3));
        assert_eq!(summary.by_place, ByPlace { no_fill_in_block: 1, before_first_fill: 1, with_fill: 1, after_fill: 1 });
        assert_eq!(summary.depth, ByPlace { after_fill: 1, ..ByPlace::default() });
        assert_eq!(summary.reasons["StaleUpdate()"], 2);
        assert_eq!(summary.late_reasons.keys().map(String::as_str).collect::<Vec<_>>(), ["Error(\"SP\")", "InsufficientLiquidity()"]);
    }
}
