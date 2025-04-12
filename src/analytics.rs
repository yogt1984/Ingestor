use std::sync::Arc;
use tokio::{task::JoinHandle, time::{interval, Duration}};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use chrono::Utc;

use crate::{orderbook::ConcurrentOrderBook, tradeslog::ConcurrentTradesLog};
use crate::persistence;

const BATCH_SIZE: usize = 1000; // Save every 1000 snapshots

#[derive(Serialize, Clone)]
pub struct FeaturesSnapshot {
    pub timestamp: String,

    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub mid_price: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub imbalance: Option<Decimal>,
    pub top_bids: Vec<(Decimal, Decimal)>,
    pub top_asks: Vec<(Decimal, Decimal)>,

    pub pwi_1: Option<Decimal>,
    pub pwi_5: Option<Decimal>,
    pub pwi_25: Option<Decimal>,
    pub pwi_50: Option<Decimal>,

    pub bid_slope: Option<Decimal>,
    pub ask_slope: Option<Decimal>,

    pub volume_imbalance_top5: Option<Decimal>,

    pub bid_depth_ratio: Option<Decimal>,
    pub ask_depth_ratio: Option<Decimal>,

    pub bid_volume_001: Option<Decimal>,
    pub ask_volume_001: Option<Decimal>,

    pub bid_avg_distance: Option<Decimal>,
    pub ask_avg_distance: Option<Decimal>,

    pub last_trade_price: Option<Decimal>,
    pub vwap_50: Option<Decimal>,
    pub aggr_ratio_50: Option<Decimal>,
    pub trade_imbalance: Option<Decimal>,
    pub vwap_total: Option<Decimal>,
    pub price_change: Option<Decimal>,
    pub avg_trade_size: Option<Decimal>,
    pub signed_count_momentum: i64,
    pub trade_rate_10s: Option<f64>,
}


pub fn spawn_analytics_task(
    order_book_clone: ConcurrentOrderBook,
    trades_log_clone: ConcurrentTradesLog,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut batch_id = 0;

        loop {
            interval.tick().await;

            let snapshot = generate_snapshot(&order_book_clone, &trades_log_clone).await;

            // --- Print to Console ---
            print!("\x1B[2J\x1B[1;1H"); // Clear terminal
            println!("======================================================");
            println!("--- Order Book Features ---");
            println!("Mid Price                      : {:?}", snapshot.mid_price);
            println!("Spread                         : {:?}", snapshot.spread);
            println!("Imbalance                      : {:?}", snapshot.imbalance);
            println!("Price-Weighted Imbalance ±1%   : {:?}", snapshot.pwi_1);
            println!("Price-Weighted Imbalance ±5%   : {:?}", snapshot.pwi_5);
            println!("Price-Weighted Imbalance ±25%  : {:?}", snapshot.pwi_25);
            println!("Price-Weighted Imbalance ±50%  : {:?}", snapshot.pwi_50);
            println!("Bid Slope                      : {:?}", snapshot.bid_slope);
            println!("Ask Slope                      : {:?}", snapshot.ask_slope);
            println!("Volume Imbalance (Top 5)       : {:?}", snapshot.volume_imbalance_top5);
            println!("Depth Ratio (Top 3/10)         : Bids={:?}, Asks={:?}", snapshot.bid_depth_ratio, snapshot.ask_depth_ratio);
            println!("Volume ±0.01% Mid              : Bids={:?}, Asks={:?}", snapshot.bid_volume_001, snapshot.ask_volume_001);
            println!("Avg Distance (Top 5)           : Bids={:?}, Asks={:?}", snapshot.bid_avg_distance, snapshot.ask_avg_distance);
            println!("--- Trade Log Features ---");
            println!("Last Trade Price               : {:?}", snapshot.last_trade_price);
            println!("VWAP (last 50 trades)          : {:?}", snapshot.vwap_50);
            println!("Aggressor Volume Ratio (50)    : {:?}", snapshot.aggr_ratio_50);
            println!("Trade Imbalance                : {:?}", snapshot.trade_imbalance);
            println!("VWAP Total (cached)            : {:?}", snapshot.vwap_total);
            println!("Price Change (cached)          : {:?}", snapshot.price_change);
            println!("Average Trade Size             : {:?}", snapshot.avg_trade_size);
            println!("Signed Count Momentum          : {}", snapshot.signed_count_momentum);
            println!("Trade Rate (per sec, 10s)      : {:?}", snapshot.trade_rate_10s);
            println!("======================================================\n");

            // --- Buffer snapshots into batch ---
            batch.push(snapshot);

            if batch.len() >= BATCH_SIZE {
                let filename = format!("data/features_batch_{:03}.parquet", batch_id);

                if let Err(e) = persistence::save_feature_as_parquet(&batch, &filename) {
                    log::error!("Failed to save parquet batch {}: {}", batch_id, e);
                } else {
                    log::info!("Saved batch {} to {}", batch_id, filename);
                }

                batch.clear();
                batch_id += 1;
            }
        }
    })
}

async fn generate_snapshot(
    order_book: &ConcurrentOrderBook,
    trades_log: &ConcurrentTradesLog,
) -> FeaturesSnapshot {
    let timestamp = Utc::now().to_rfc3339();

    let best_bid_raw = order_book.best_bid().await;
    let best_ask_raw = order_book.best_ask().await;
    let best_bid = best_bid_raw.map(|(price, _qty)| price);
    let best_ask = best_ask_raw.map(|(price, _qty)| price);

    let mid_price = order_book.mid_price().await;
    let spread = order_book.spread().await;
    let imbalance = order_book.order_book_imbalance().await;
    let top_bids = order_book.top_bids(5).await;
    let top_asks = order_book.top_asks(5).await;

    let pwi_1 = order_book.price_weighted_imbalance_percent(dec!(1)).await;
    let pwi_5 = order_book.price_weighted_imbalance_percent(dec!(5)).await;
    let pwi_25 = order_book.price_weighted_imbalance_percent(dec!(25)).await;
    let pwi_50 = order_book.price_weighted_imbalance_percent(dec!(50)).await;

    let (bid_slope, ask_slope) = match order_book.slope(5).await {
        Some((bid, ask)) => (Some(bid), Some(ask)),
        None => (None, None),
    };
    let volume_imbalance_top5 = order_book.volume_imbalance().await;

    let (bid_depth_ratio, ask_depth_ratio) = match order_book.depth_ratio().await {
        Some((bid, ask)) => (Some(bid), Some(ask)),
        None => (None, None),
    };
    let (bid_volume_001, ask_volume_001) = match order_book.volume_within_percent_range(dec!(0.01)).await {
        Some((bid, ask)) => (Some(bid), Some(ask)),
        None => (None, None),
    };
    let (bid_avg_distance, ask_avg_distance) = match order_book.avg_price_distance(5).await {
        Some((bid, ask)) => (Some(bid), Some(ask)),
        None => (None, None),
    };

    let last_trade_price = trades_log.last_price().await;
    let vwap_50 = trades_log.vwap(50).await.ok();
    let aggr_ratio_50 = trades_log.aggressor_volume_ratio(50).await.ok();
    let trade_imbalance = trades_log.trade_imbalance().await;
    let vwap_total = trades_log.vwap_total().await;
    let price_change = trades_log.price_change().await;
    let avg_trade_size = trades_log.avg_trade_size().await;
    let signed_count_momentum = trades_log.signed_count_momentum().await;
    let trade_rate_10s = trades_log.trade_rate(10_000).await.ok();

    FeaturesSnapshot {
        timestamp,
        best_bid,
        best_ask,
        mid_price,
        spread,
        imbalance,
        top_bids,
        top_asks,
        pwi_1,
        pwi_5,
        pwi_25,
        pwi_50,
        bid_slope,
        ask_slope,
        volume_imbalance_top5,
        bid_depth_ratio,
        ask_depth_ratio,
        bid_volume_001,
        ask_volume_001,
        bid_avg_distance,
        ask_avg_distance,
        last_trade_price,
        vwap_50,
        aggr_ratio_50,
        trade_imbalance,
        vwap_total,
        price_change,
        avg_trade_size,
        signed_count_momentum,
        trade_rate_10s,
    }
}
