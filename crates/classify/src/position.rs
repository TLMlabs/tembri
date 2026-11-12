//! Whether payment to the builder decides which taker trades first after a
//! maker's update, and what the first place is worth.
//!
//! A *market-block* is one block, one venue and one unordered token pair. On
//! venues with one update per market per block, its fills trade against the
//! same update. Within a market-block, each transaction counts once, at its
//! first fill (by transaction position, then log index). A market-block with
//! two or more transactions is *contested*.

use std::collections::{BTreeMap, HashMap};
use tlm_model::{PropammBuilderPayment, PropammEvent, PropammEventKind};

/// One transaction's place in a market-block, with its payment to the builder.
#[derive(Clone, Debug, PartialEq)]
struct Place {
    block_number: u64,
    builder: Option<String>,
    venue: String,
    single_swap: bool,
    gas_used: u64,
    priority_fee_wei: u128,
    total_wei: u128,
}

impl Place {
    fn per_gas_gwei(&self, total: bool) -> f64 {
        let wei = if total { self.total_wei } else { self.priority_fee_wei };
        wei as f64 / self.gas_used as f64 / 1e9
    }
}

/// Neighbouring transactions in contested market-blocks: does the earlier one
/// pay more per gas?
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderTest {
    pub market_blocks: usize,
    pub neighbour_pairs: usize,
    pub earlier_pays_more: usize,
    pub earlier_pays_less: usize,
    pub equal: usize,
    /// Median payment per gas (gwei) of the first transaction, the later
    /// ones, and transactions alone in their market-block.
    pub median_first_gwei: Option<f64>,
    pub median_later_gwei: Option<f64>,
    pub median_solo_gwei: Option<f64>,
}

/// The order test under each payment measure and selection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PositionTests {
    /// Priority fee only, all contested market-blocks.
    pub priority_fee: OrderTest,
    /// Priority fee plus direct transfers, all contested market-blocks.
    pub total: OrderTest,
    /// Total payment, market-blocks whose first transaction is single-swap.
    pub total_single_swap: OrderTest,
}

/// Per builder: payments of all fill transactions, and the premium of first
/// places over a lone transaction.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EarlyPlaceValue {
    pub fill_transactions: usize,
    pub all_payments_usd: f64,
    pub contested_first_places: usize,
    /// Sum over first places of max(0, payment per gas - median payment per
    /// gas of lone transactions with the same builder, venue and single-swap
    /// flag) x gas used, in USD. An upper bound: payments cover the whole
    /// transaction.
    pub premium_usd: f64,
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len() % 2 == 1 { values[middle] } else { (values[middle - 1] + values[middle]) / 2.0 })
}

/// Block, venue and the unordered token pair.
type MarketBlock<'a> = (u64, &'a str, String, String);

/// Contested and lone market-blocks, each as its transactions in order.
fn market_blocks(events: &[PropammEvent], payments: &[PropammBuilderPayment]) -> Vec<Vec<Place>> {
    let paid: HashMap<&str, &PropammBuilderPayment> = payments.iter().map(|row| (row.tx_hash.as_str(), row)).collect();
    let mut groups: BTreeMap<MarketBlock, BTreeMap<(u32, u32), Place>> = BTreeMap::new();
    let mut seen: HashMap<MarketBlock, Vec<String>> = HashMap::new();
    for event in events.iter().filter(|event| event.kind == PropammEventKind::Fill) {
        let Some(terms) = &event.fill else { continue };
        let hash = event.tx_hash.to_ascii_lowercase();
        let Some(payment) = paid.get(hash.as_str()) else { continue };
        if payment.gas_used == 0 {
            continue;
        }
        let (a, b) = (terms.token_in.to_ascii_lowercase(), terms.token_out.to_ascii_lowercase());
        let pair = if a <= b { (a, b) } else { (b, a) };
        let key = (event.block_number, event.venue.as_str(), pair.0, pair.1);
        let txs = seen.entry(key.clone()).or_default();
        if txs.contains(&hash) {
            continue;
        }
        txs.push(hash);
        groups.entry(key).or_default().insert(
            (event.tx_position, terms.log_index),
            Place {
                block_number: event.block_number,
                builder: payment.builder.clone(),
                venue: event.venue.clone(),
                single_swap: payment.other_swap_logs == 0,
                gas_used: payment.gas_used,
                priority_fee_wei: payment.priority_fee_wei,
                total_wei: payment.priority_fee_wei + payment.direct_transfer_wei,
            },
        );
    }
    groups.into_values().map(|places| places.into_values().collect()).collect()
}

fn order_test(groups: &[Vec<Place>], total: bool, select: impl Fn(&Place) -> bool) -> OrderTest {
    let mut test = OrderTest::default();
    let (mut first, mut later, mut solo) = (Vec::new(), Vec::new(), Vec::new());
    for places in groups {
        let Some(head) = places.first() else { continue };
        if !select(head) {
            continue;
        }
        if places.len() == 1 {
            solo.push(head.per_gas_gwei(total));
            continue;
        }
        test.market_blocks += 1;
        first.push(head.per_gas_gwei(total));
        later.extend(places[1..].iter().map(|place| place.per_gas_gwei(total)));
        for pair in places.windows(2) {
            // Exact comparison of per-gas payments: a/ga against b/gb.
            let wei = |place: &Place| if total { place.total_wei } else { place.priority_fee_wei };
            let earlier = wei(&pair[0]) * u128::from(pair[1].gas_used);
            let next = wei(&pair[1]) * u128::from(pair[0].gas_used);
            test.neighbour_pairs += 1;
            match earlier.cmp(&next) {
                std::cmp::Ordering::Greater => test.earlier_pays_more += 1,
                std::cmp::Ordering::Less => test.earlier_pays_less += 1,
                std::cmp::Ordering::Equal => test.equal += 1,
            }
        }
    }
    test.median_first_gwei = median(&mut first);
    test.median_later_gwei = median(&mut later);
    test.median_solo_gwei = median(&mut solo);
    test
}

/// Fills that trade after an earlier fill on the same market in their block,
/// and so against the state that fill left. Counted over all fills, with or
/// without payment rows, by fill and by transaction (a transaction counts
/// once per market-block, at its first fill).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SharedUpdates {
    pub fills: usize,
    pub later_fills: usize,
    pub transactions: usize,
    pub later_transactions: usize,
    /// Market-blocks with two or more transactions.
    pub contested_market_blocks: usize,
}

pub fn propamm_shared_updates(events: &[PropammEvent]) -> SharedUpdates {
    let mut groups: HashMap<MarketBlock, (usize, Vec<String>)> = HashMap::new();
    for event in events.iter().filter(|event| event.kind == PropammEventKind::Fill) {
        let Some(terms) = &event.fill else { continue };
        let (a, b) = (terms.token_in.to_ascii_lowercase(), terms.token_out.to_ascii_lowercase());
        let pair = if a <= b { (a, b) } else { (b, a) };
        let entry = groups.entry((event.block_number, event.venue.as_str(), pair.0, pair.1)).or_default();
        entry.0 += 1;
        let hash = event.tx_hash.to_ascii_lowercase();
        if !entry.1.contains(&hash) {
            entry.1.push(hash);
        }
    }
    let mut shared = SharedUpdates::default();
    for (fills, txs) in groups.values() {
        shared.fills += fills;
        shared.later_fills += fills - 1;
        shared.transactions += txs.len();
        shared.later_transactions += txs.len() - 1;
        shared.contested_market_blocks += usize::from(txs.len() > 1);
    }
    shared
}

/// `propamm_shared_updates` per venue.
pub fn propamm_shared_updates_by_venue(events: &[PropammEvent]) -> BTreeMap<String, SharedUpdates> {
    let mut by_venue: BTreeMap<String, Vec<PropammEvent>> = BTreeMap::new();
    for event in events.iter().filter(|event| event.kind == PropammEventKind::Fill) {
        by_venue.entry(event.venue.clone()).or_default().push(event.clone());
    }
    by_venue.into_iter().map(|(venue, events)| (venue, propamm_shared_updates(&events))).collect()
}

/// The order tests after a PropAMM update.
pub fn propamm_position_tests(events: &[PropammEvent], payments: &[PropammBuilderPayment]) -> PositionTests {
    let groups = market_blocks(events, payments);
    PositionTests {
        priority_fee: order_test(&groups, false, |_| true),
        total: order_test(&groups, true, |_| true),
        total_single_swap: order_test(&groups, true, |place| place.single_swap),
    }
}

/// The order test of one venue, with the median payment per gas by place.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VenueOrder {
    /// Total payment, all contested market-blocks of the venue.
    pub total: OrderTest,
    /// Total payment, market-blocks whose first transaction is single-swap.
    pub total_single_swap: OrderTest,
    /// In contested market-blocks whose first transaction is single-swap:
    /// median payment per gas (gwei) of single-swap transactions at place 1,
    /// 2, and 3 or later, with their counts.
    pub single_swap_place_gwei: [Option<f64>; 3],
    pub single_swap_place_counts: [usize; 3],
}

/// `propamm_position_tests` per venue.
pub fn propamm_position_tests_by_venue(events: &[PropammEvent], payments: &[PropammBuilderPayment]) -> BTreeMap<String, VenueOrder> {
    let mut by_venue: BTreeMap<String, Vec<Vec<Place>>> = BTreeMap::new();
    for places in market_blocks(events, payments) {
        if let Some(head) = places.first() {
            by_venue.entry(head.venue.clone()).or_default().push(places);
        }
    }
    by_venue
        .into_iter()
        .map(|(venue, groups)| {
            let mut by_place: [Vec<f64>; 3] = Default::default();
            for places in groups.iter().filter(|places| places.len() >= 2 && places[0].single_swap) {
                for (index, place) in places.iter().enumerate().filter(|(_, place)| place.single_swap) {
                    by_place[index.min(2)].push(place.per_gas_gwei(true));
                }
            }
            let single_swap_place_counts = [by_place[0].len(), by_place[1].len(), by_place[2].len()];
            let [first, second, later] = &mut by_place;
            let order = VenueOrder {
                total: order_test(&groups, true, |_| true),
                total_single_swap: order_test(&groups, true, |place| place.single_swap),
                single_swap_place_gwei: [median(first), median(second), median(later)],
                single_swap_place_counts,
            };
            (venue, order)
        })
        .collect()
}

/// The value of the first place after an update, by builder.
/// `eth_usd` maps blocks to the ETH/USD price; transactions in
/// unpriced blocks are left out of every total.
pub fn propamm_early_place_value(
    events: &[PropammEvent],
    payments: &[PropammBuilderPayment],
    eth_usd: &HashMap<u64, f64>,
) -> BTreeMap<Option<String>, EarlyPlaceValue> {
    let mut values: BTreeMap<Option<String>, EarlyPlaceValue> = BTreeMap::new();
    for payment in payments {
        let Some(eth) = eth_usd.get(&payment.block_number) else { continue };
        let value = values.entry(payment.builder.clone()).or_default();
        value.fill_transactions += 1;
        value.all_payments_usd += (payment.priority_fee_wei + payment.direct_transfer_wei) as f64 / 1e18 * eth;
    }
    let groups = market_blocks(events, payments);
    let mut lone: HashMap<(Option<String>, String, bool), Vec<f64>> = HashMap::new();
    for places in groups.iter().filter(|places| places.len() == 1) {
        let place = &places[0];
        lone.entry((place.builder.clone(), place.venue.clone(), place.single_swap)).or_default().push(place.per_gas_gwei(true));
    }
    let baseline: HashMap<_, f64> = lone.into_iter().filter_map(|(key, mut values)| Some((key, median(&mut values)?))).collect();
    for places in groups.iter().filter(|places| places.len() >= 2) {
        let head = &places[0];
        let Some(eth) = eth_usd.get(&head.block_number) else { continue };
        let Some(base) = baseline.get(&(head.builder.clone(), head.venue.clone(), head.single_swap)) else { continue };
        let value = values.entry(head.builder.clone()).or_default();
        value.contested_first_places += 1;
        value.premium_usd += (head.per_gas_gwei(true) - base).max(0.0) * head.gas_used as f64 / 1e9 * eth;
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use tlm_model::PropammFillTerms;

    fn fill(block: u64, tx: &str, position: u32, venue: &str, tokens: (&str, &str)) -> PropammEvent {
        PropammEvent {
            block_number: block,
            pool: "0xpool".into(),
            venue: venue.into(),
            builder: Some("titan".into()),
            tx_hash: tx.into(),
            tx_position: position,
            cumulative_gas_before: 0,
            update_timestamp: None,
            kind: PropammEventKind::Fill,
            fill: Some(PropammFillTerms {
                log_index: position,
                sender: "0xs".into(),
                token_in: tokens.0.into(),
                token_out: tokens.1.into(),
                amount_in: 1,
                amount_out: 1,
                recipient: "0xr".into(),
            }),
        }
    }

    fn paid(block: u64, tx: &str, tip_gwei: u128, transfer_gwei: u128, other_swaps: u32) -> PropammBuilderPayment {
        PropammBuilderPayment {
            block_number: block,
            tx_hash: tx.into(),
            tx_position: 0,
            builder: Some("titan".into()),
            fee_recipient: "0xfee".into(),
            gas_used: 100_000,
            effective_priority_fee_per_gas: 0,
            priority_fee_wei: tip_gwei * 1_000_000_000 * 100_000,
            direct_transfer_wei: transfer_gwei * 1_000_000_000 * 100_000,
            direct_transfers: 0,
            other_swap_logs: other_swaps,
        }
    }

    #[test]
    fn shared_updates_count_later_fills_and_transactions_per_market_block() {
        // Block 1, one market (pair in either direction): tx a with two fills, then tx b.
        // Block 1, another venue: tx c alone. Block 2: tx d alone on the first market.
        let events = [
            fill(1, "0xa", 1, "v", ("x", "y")),
            fill(1, "0xA", 2, "v", ("y", "x")),
            fill(1, "0xb", 3, "v", ("x", "y")),
            fill(1, "0xc", 4, "w", ("x", "y")),
            fill(2, "0xd", 1, "v", ("x", "y")),
        ];
        let shared = propamm_shared_updates(&events);
        assert_eq!(
            shared,
            SharedUpdates { fills: 5, later_fills: 2, transactions: 4, later_transactions: 1, contested_market_blocks: 1 }
        );
        let venues = propamm_shared_updates_by_venue(&events);
        assert_eq!((venues["v"].transactions, venues["v"].later_transactions, venues["w"].transactions), (3, 1, 1));
    }

    #[test]
    fn compares_neighbours_within_a_market_block_and_counts_each_transaction_once() {
        // Block 1: three transactions on one market (the pair in either
        // direction); the first pays by transfer, so only the total shows it.
        // Block 2: a lone transaction. Block 1 on another venue: lone too.
        let events = [
            fill(1, "0xa", 1, "kipseli", ("0xweth", "0xusdc")),
            fill(1, "0xa", 1, "kipseli", ("0xweth", "0xusdc")),
            fill(1, "0xb", 4, "kipseli", ("0xusdc", "0xweth")),
            fill(1, "0xc", 9, "kipseli", ("0xweth", "0xusdc")),
            fill(1, "0xd", 2, "fermiswap", ("0xweth", "0xusdc")),
            fill(2, "0xe", 1, "kipseli", ("0xweth", "0xusdc")),
        ];
        let payments = [paid(1, "0xa", 1, 5, 0), paid(1, "0xb", 2, 0, 1), paid(1, "0xc", 2, 0, 0), paid(1, "0xd", 1, 0, 0), paid(2, "0xe", 3, 0, 0)];
        let tests = propamm_position_tests(&events, &payments);
        assert_eq!((tests.priority_fee.market_blocks, tests.priority_fee.neighbour_pairs), (1, 2));
        assert_eq!((tests.priority_fee.earlier_pays_more, tests.priority_fee.earlier_pays_less, tests.priority_fee.equal), (0, 1, 1));
        assert_eq!((tests.total.earlier_pays_more, tests.total.earlier_pays_less, tests.total.equal), (1, 0, 1));
        assert_eq!(tests.total.median_first_gwei, Some(6.0));
        assert_eq!(tests.total.median_later_gwei, Some(2.0));
        assert_eq!(tests.total.median_solo_gwei, Some(2.0));
        assert_eq!(tests.total_single_swap.market_blocks, 1);

        // Premium: the first place pays 6 gwei/gas against a lone median of 3
        // (kipseli, single-swap, titan), on 100,000 gas at $2,000 per ETH.
        let eth = HashMap::from([(1, 2000.0), (2, 2000.0)]);
        let value = &propamm_early_place_value(&events, &payments, &eth)[&Some("titan".to_string())];
        assert_eq!((value.fill_transactions, value.contested_first_places), (5, 1));
        assert!((value.premium_usd - 3e-9 * 100_000.0 * 2000.0).abs() < 1e-9);
    }

    #[test]
    fn splits_the_order_test_by_venue_with_single_swap_places() {
        // kipseli block 1: a (single-swap, 6 gwei/gas), b (not single-swap, 2), c (single-swap, 2).
        // fermiswap block 1: d alone. fermiswap block 3: e (1 gwei/gas) then f (4), both single-swap.
        let events = [
            fill(1, "0xa", 1, "kipseli", ("0xweth", "0xusdc")),
            fill(1, "0xb", 4, "kipseli", ("0xusdc", "0xweth")),
            fill(1, "0xc", 9, "kipseli", ("0xweth", "0xusdc")),
            fill(1, "0xd", 2, "fermiswap", ("0xweth", "0xusdc")),
            fill(3, "0xe", 1, "fermiswap", ("0xweth", "0xusdc")),
            fill(3, "0xf", 2, "fermiswap", ("0xweth", "0xusdc")),
        ];
        let payments = [
            paid(1, "0xa", 1, 5, 0),
            paid(1, "0xb", 2, 0, 1),
            paid(1, "0xc", 2, 0, 0),
            paid(1, "0xd", 1, 0, 0),
            paid(3, "0xe", 1, 0, 0),
            paid(3, "0xf", 4, 0, 0),
        ];
        let venues = propamm_position_tests_by_venue(&events, &payments);
        assert_eq!(venues.keys().map(String::as_str).collect::<Vec<_>>(), ["fermiswap", "kipseli"]);
        let kipseli = &venues["kipseli"];
        assert_eq!((kipseli.total.market_blocks, kipseli.total.earlier_pays_more, kipseli.total.equal), (1, 1, 1));
        // Place 2 is not single-swap, so it is left out of the place medians; c is place 3.
        assert_eq!(kipseli.single_swap_place_counts, [1, 0, 1]);
        assert_eq!(kipseli.single_swap_place_gwei, [Some(6.0), None, Some(2.0)]);
        let fermiswap = &venues["fermiswap"];
        assert_eq!((fermiswap.total.market_blocks, fermiswap.total.earlier_pays_less), (1, 1));
        assert_eq!(fermiswap.total.median_solo_gwei, Some(1.0));
        assert_eq!(fermiswap.single_swap_place_gwei, [Some(1.0), Some(4.0), None]);
    }
}
