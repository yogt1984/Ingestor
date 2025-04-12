use std::sync::Arc;
use tokio::{task::JoinHandle, time::{interval, Duration}};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use chrono::Utc;

use crate::{orderbook::ConcurrentOrderBook, tradeslog::ConcurrentTradesLog};
use crate::persistence;

#[derive(Serialize)]
struct FeaturesSnapshot {
    timestamp: String,

    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    mid_price: Option<Decimal>,
    spread: Option<Decimal>,
    imbalance: Option<Decimal>,
    top_bids: Vec<(Decimal, Decimal)>,
    top_asks: Vec<(Decimal, Decimal)>,

    pwi_1: Option<Decimal>,
    pwi_5: Option<Decimal>,
    pwi_25: Option<Decimal>,
    pwi_50: Option<Decimal>,

    bid_slope: Option<Decimal>,
    ask_slope: Option<Decimal>,

    volume_imbalance_top5: Option<Decimal>,

    bid_depth_ratio: Option<Decimal>,
    ask_depth_ratio: Option<Decimal>,

    bid_volume_001: Option<Decimal>,
    ask_volume_001: Option<Decimal>,

    bid_avg_distance: Option<Decimal>,
    ask_avg_distance: Option<Decimal>,

    last_trade_price: Option<Decimal>,
    vwap_50: Option<Decimal>,
    aggr_ratio_50: Option<Decimal>,
    trade_imbalance: Option<Decimal>,
    vwap_total: Option<Decimal>,
    price_change: Option<Decimal>,
    avg_trade_size: Option<Decimal>,
    signed_count_momentum: i64,
    trade_rate_10s: Option<f64>,
}

pub fn spawn_analytics_task(
    order_book_clone: ConcurrentOrderBook,
    trades_log_clone: ConcurrentTradesLog,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        let filepath = "data/features.json";

        loop {
            interval.tick().await;

            let timestamp = Utc::now().to_rfc3339();

            // --- Order Book Analytics ---
            let best_bid_raw = order_book_clone.best_bid().await;
            let best_ask_raw = order_book_clone.best_ask().await;
            let best_bid = best_bid_raw.map(|(price, _qty)| price);
            let best_ask = best_ask_raw.map(|(price, _qty)| price);

            let mid_price = order_book_clone.mid_price().await;
            let spread = order_book_clone.spread().await;
            let imbalance = order_book_clone.order_book_imbalance().await;
            let top_bids = order_book_clone.top_bids(5).await;
            let top_asks = order_book_clone.top_asks(5).await;

            let pwi_1 = order_book_clone.price_weighted_imbalance_percent(dec!(1)).await;
            let pwi_5 = order_book_clone.price_weighted_imbalance_percent(dec!(5)).await;
            let pwi_25 = order_book_clone.price_weighted_imbalance_percent(dec!(25)).await;
            let pwi_50 = order_book_clone.price_weighted_imbalance_percent(dec!(50)).await;

            // 🛠️ FIX: Match Option<(Decimal, Decimal)> correctly
            let (bid_slope, ask_slope) = match order_book_clone.slope(5).await {
                Some((bid, ask)) => (Some(bid), Some(ask)),
                None => (None, None),
            };
            let volume_imbalance_top5 = order_book_clone.volume_imbalance().await;

            let (bid_depth_ratio, ask_depth_ratio) = match order_book_clone.depth_ratio().await {
                Some((bid, ask)) => (Some(bid), Some(ask)),
                None => (None, None),
            };
            let (bid_volume_001, ask_volume_001) = match order_book_clone.volume_within_percent_range(dec!(0.01)).await {
                Some((bid, ask)) => (Some(bid), Some(ask)),
                None => (None, None),
            };
            let (bid_avg_distance, ask_avg_distance) = match order_book_clone.avg_price_distance(5).await {
                Some((bid, ask)) => (Some(bid), Some(ask)),
                None => (None, None),
            };

            // --- Trade Log Analytics ---
            let last_trade_price = trades_log_clone.last_price().await;
            let vwap_50 = trades_log_clone.vwap(50).await.ok();
            let aggr_ratio_50 = trades_log_clone.aggressor_volume_ratio(50).await.ok();
            let trade_imbalance = trades_log_clone.trade_imbalance().await;
            let vwap_total = trades_log_clone.vwap_total().await;
            let price_change = trades_log_clone.price_change().await;
            let avg_trade_size = trades_log_clone.avg_trade_size().await;
            let signed_count_momentum = trades_log_clone.signed_count_momentum().await;
            let trade_rate_10s = trades_log_clone.trade_rate(10_000).await.ok();

            // --- Fill Features Snapshot ---
            let snapshot = FeaturesSnapshot {
                timestamp,
                best_bid,
                best_ask,
                mid_price,
                spread,
                imbalance,
                top_bids: top_bids.clone(),  
                top_asks: top_asks.clone(),  
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
            };

            // --- Save Snapshot to JSON ---
            if let Err(e) = persistence::save_feature_as_json(&snapshot, filepath) {
                log::error!("Failed to save feature snapshot: {}", e);
            }

            // --- Print ALL Computed Features ---
            print!("\x1B[2J\x1B[1;1H");
            println!("======================================================");
            println!("--- Order Book Features ---");
            println!("Best Bid                       : {:?}", best_bid);
            println!("Best Ask                       : {:?}", best_ask);
            println!("Mid Price                      : {:?}", mid_price);
            println!("Spread                         : {:?}", spread);
            println!("Imbalance                      : {:?}", imbalance);
            println!("Top 5 Bids                     : {:?}", top_bids);
            println!("Top 5 Asks                     : {:?}", top_asks);
            println!("Price-Weighted Imbalance ±1%   : {:?}", pwi_1);
            println!("Price-Weighted Imbalance ±5%   : {:?}", pwi_5);
            println!("Price-Weighted Imbalance ±25%  : {:?}", pwi_25);
            println!("Price-Weighted Imbalance ±50%  : {:?}", pwi_50);
            println!("Bid Slope                      : {:?}", bid_slope);
            println!("Ask Slope                      : {:?}", ask_slope);
            println!("Volume Imbalance (Top 5)       : {:?}", volume_imbalance_top5);
            println!("Depth Ratio (Top 3/10)         : Bids={:?}, Asks={:?}", bid_depth_ratio, ask_depth_ratio);
            println!("Volume ±0.01% Mid              : Bids={:?}, Asks={:?}", bid_volume_001, ask_volume_001);
            println!("Avg Distance (Top 5)           : Bids={:?}, Asks={:?}", bid_avg_distance, ask_avg_distance);
            println!("--- Trade Log Features ---");
            println!("Last Trade Price               : {:?}", last_trade_price);
            println!("VWAP (last 50 trades)          : {:?}", vwap_50);
            println!("Aggressor Volume Ratio (50)    : {:?}", aggr_ratio_50);
            println!("Trade Imbalance                : {:?}", trade_imbalance);
            println!("VWAP Total (cached)            : {:?}", vwap_total);
            println!("Price Change (cached)          : {:?}", price_change);
            println!("Average Trade Size             : {:?}", avg_trade_size);
            println!("Signed Count Momentum          : {}", signed_count_momentum);
            println!("Trade Rate (per sec, 10s)      : {:?}", trade_rate_10s);
            println!("======================================================\n");
        }
    })
}
