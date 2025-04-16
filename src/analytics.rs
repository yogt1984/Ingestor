use std::sync::Arc;
use tokio::{sync::{watch, Mutex}, time::{interval, Duration}};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use chrono::Utc;

use crate::{
    orderbook::ConcurrentOrderBook,
    tradeslog::ConcurrentTradesLog,
    persistence,
};

const SNAPSHOT_INTERVAL_MS: u64 = 100;
const BATCH_SIZE: usize = 1000;

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

pub async fn run_analytics_task(
    order_book: Arc<ConcurrentOrderBook>,
    trades_log: Arc<ConcurrentTradesLog>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut interval = interval(Duration::from_millis(SNAPSHOT_INTERVAL_MS));
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut batch_id = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let (ob_snap, trade_snap) = tokio::join!(
                    order_book.get_snapshot(),
                    trades_log.get_snapshot()
                );

                let snapshot = FeaturesSnapshot {
                    timestamp: Utc::now().to_rfc3339(),
                    best_bid: ob_snap.best_bid.map(|(p, _)| p),
                    best_ask: ob_snap.best_ask.map(|(p, _)| p),
                    mid_price: ob_snap.mid_price,
                    spread: ob_snap.spread,
                    imbalance: ob_snap.imbalance,
                    top_bids: ob_snap.top_bids,
                    top_asks: ob_snap.top_asks,
                    pwi_1: ob_snap.pwi_1,
                    pwi_5: ob_snap.pwi_5,
                    pwi_25: ob_snap.pwi_25,
                    pwi_50: ob_snap.pwi_50,
                    bid_slope: ob_snap.bid_slope,
                    ask_slope: ob_snap.ask_slope,
                    volume_imbalance_top5: ob_snap.volume_imbalance_top5,
                    bid_depth_ratio: ob_snap.bid_depth_ratio,
                    ask_depth_ratio: ob_snap.ask_depth_ratio,
                    bid_volume_001: ob_snap.bid_volume_001,
                    ask_volume_001: ob_snap.ask_volume_001,
                    bid_avg_distance: ob_snap.bid_avg_distance,
                    ask_avg_distance: ob_snap.ask_avg_distance,
                    last_trade_price: trade_snap.last_price,
                    vwap_50: trade_snap.vwap_50,
                    aggr_ratio_50: trade_snap.aggr_ratio_50,
                    trade_imbalance: trade_snap.trade_imbalance,
                    vwap_total: trade_snap.vwap_total,
                    price_change: trade_snap.price_change,
                    avg_trade_size: trade_snap.avg_trade_size,
                    signed_count_momentum: trade_snap.signed_count_momentum,
                    trade_rate_10s: trade_snap.trade_rate_10s,
                };
                
                // Simple console output
                println!(
                    "[{}] BID/ASK: {:?}/{:?} | MID: {:?} | SPRD: {:?} | IMB: {:?}\n\
                     PWI: 1%={:?} 5%={:?} 25%={:?} 50%={:?}\n\
                     SLOPE: B{:?}/A{:?} | VOL_IMB: {:?}\n\
                     DEPTH: B{:?}/A{:?} | VOL(0.01%): B{:?}/A{:?}\n\
                     TRADES: LAST={:?} VWAP50={:?} AGR={:?} IMB={:?}\n\
                     VWAP_TOT={:?} ΔPRICE={:?} AVG_SIZE={:?}\n\
                     MOMENTUM: {} TRADE_RATE={:?}",
                    snapshot.timestamp,
                    snapshot.best_bid,
                    snapshot.best_ask,
                    snapshot.mid_price,
                    snapshot.spread,
                    snapshot.imbalance,
                    snapshot.pwi_1,
                    snapshot.pwi_5,
                    snapshot.pwi_25,
                    snapshot.pwi_50,
                    snapshot.bid_slope,
                    snapshot.ask_slope,
                    snapshot.volume_imbalance_top5,
                    snapshot.bid_depth_ratio,
                    snapshot.ask_depth_ratio,
                    snapshot.bid_volume_001,
                    snapshot.ask_volume_001,
                    snapshot.last_trade_price,
                    snapshot.vwap_50,
                    snapshot.aggr_ratio_50,
                    snapshot.trade_imbalance,
                    snapshot.vwap_total,
                    snapshot.price_change,
                    snapshot.avg_trade_size,
                    snapshot.signed_count_momentum,
                    snapshot.trade_rate_10s
                );

                batch.push(snapshot);
                if batch.len() >= BATCH_SIZE {
                    if let Err(e) = persistence::save_feature_as_parquet(&batch, &format!("data/features_batch_{:03}.parquet", batch_id)) {
                        eprintln!("Failed to save batch {}: {}", batch_id, e);
                    }
                    batch.clear();
                    batch_id += 1;
                }
            }
            _ = shutdown_rx.changed() => {
                println!("Analytics task shutting down...");
                break;
            }
        }
    }
}

async fn generate_snapshot(
    order_book: &ConcurrentOrderBook,
    trades_log: &ConcurrentTradesLog,
) -> FeaturesSnapshot {
    let timestamp = Utc::now().to_rfc3339();
    
    let best_bid = order_book.best_bid().await.map(|(p, _)| p);
    let best_ask = order_book.best_ask().await.map(|(p, _)| p);
    let mid_price = order_book.mid_price().await;
    let spread = order_book.spread().await;
    let imbalance = order_book.order_book_imbalance().await;
    let top_bids = order_book.top_bids(5).await;
    let top_asks = order_book.top_asks(5).await;

    FeaturesSnapshot {
        timestamp,
        best_bid,
        best_ask,
        mid_price,
        spread,
        imbalance,
        top_bids,
        top_asks,
        pwi_1: order_book.price_weighted_imbalance_percent(dec!(1)).await,
        pwi_5: order_book.price_weighted_imbalance_percent(dec!(5)).await,
        pwi_25: order_book.price_weighted_imbalance_percent(dec!(25)).await,
        pwi_50: order_book.price_weighted_imbalance_percent(dec!(50)).await,
        bid_slope: order_book.slope(5).await.map(|(b, _)| b),
        ask_slope: order_book.slope(5).await.map(|(_, a)| a),
        volume_imbalance_top5: order_book.volume_imbalance().await,
        bid_depth_ratio: order_book.depth_ratio().await.map(|(b, _)| b),
        ask_depth_ratio: order_book.depth_ratio().await.map(|(_, a)| a),
        bid_volume_001: order_book.volume_within_percent_range(dec!(0.01)).await.map(|(b, _)| b),
        ask_volume_001: order_book.volume_within_percent_range(dec!(0.01)).await.map(|(_, a)| a),
        bid_avg_distance: order_book.avg_price_distance(5).await.map(|(b, _)| b),
        ask_avg_distance: order_book.avg_price_distance(5).await.map(|(_, a)| a),
        last_trade_price: trades_log.last_price().await,
        vwap_50: trades_log.vwap(50).await.ok(),
        aggr_ratio_50: trades_log.aggressor_volume_ratio(50).await.ok(),
        trade_imbalance: trades_log.trade_imbalance().await,
        vwap_total: trades_log.vwap_total().await,
        price_change: trades_log.price_change().await,
        avg_trade_size: trades_log.avg_trade_size().await,
        signed_count_momentum: trades_log.signed_count_momentum().await,
        trade_rate_10s: trades_log.trade_rate(10_000).await.ok(),
    }
}