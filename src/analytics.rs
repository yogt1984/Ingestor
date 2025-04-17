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
    pub microprice: Option<Decimal>,
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
    pub order_flow_imbalance: Option<Decimal>,
    pub order_flow_pressure: Decimal,
    pub order_flow_significance: bool,
}

pub async fn run_analytics_task(
    order_book: Arc<ConcurrentOrderBook>,
    trades_log: Arc<ConcurrentTradesLog>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    const PRESSURE_THRESHOLD: Decimal = dec!(5.0);
    const SIGNIFICANCE_THRESHOLD: Decimal = dec!(10.0);

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

                let (flow_imbalance, flow_pressure) = order_book.get_flow_imbalance().await;

                let snapshot = FeaturesSnapshot {
                    timestamp: Utc::now().to_rfc3339(),
                    best_bid: ob_snap.best_bid.map(|(p, _)| p),
                    best_ask: ob_snap.best_ask.map(|(p, _)| p),
                    mid_price: ob_snap.mid_price,
                    microprice: ob_snap.microprice,
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
                    order_flow_imbalance: flow_imbalance,
                    order_flow_pressure: flow_pressure,
                    order_flow_significance: flow_pressure >= SIGNIFICANCE_THRESHOLD,
                };
                
                // Simple console output
                println!(
                    "[{}] MID: {:.2} | MICRO: {:.2} (Δ {:.4})\n\
                     BID/ASK: {:?}/{:?} | SPRD: {:?} | IMB: {:?}\n\
                     PWI: 1%={:?} 5%={:?} 25%={:?} 50%={:?}\n\
                     SLOPE: B{:?}/A{:?} | VOL_IMB: {:?}\n\
                     DEPTH: B{:?}/A{:?} | VOL(0.01%): B{:?}/A{:?}\n\
                     TRADES: LAST={:?} VWAP50={:?} AGR={:?} IMB={:?}\n\
                     VWAP_TOT={:?} ΔPRICE={:?} AVG_SIZE={:?}\n\
                     MOMENTUM: {} TRADE_RATE={:?}\n\
                     FLWIMB: {:.3}",
                    snapshot.timestamp,
                    snapshot.mid_price.unwrap_or(dec!(0)),
                    snapshot.microprice.unwrap_or(dec!(0)),
                    snapshot.microprice.unwrap_or(dec!(0)) - snapshot.mid_price.unwrap_or(dec!(0)),  // Price difference
                    snapshot.best_bid,
                    snapshot.best_ask,
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
                    snapshot.trade_rate_10s,
                    snapshot.order_flow_imbalance.unwrap_or(dec!(0)),
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