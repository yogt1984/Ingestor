use std::sync::Arc;
use tokio::sync::{mpsc, watch};  // Add mpsc here
use tokio::time::{interval, Duration};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use chrono::Utc;
use crate::{
    orderbook::{ConcurrentOrderBook, OrderBookSnapshot},
    tradeslog::{ConcurrentTradesLog, TradeLogSnapshot},
    illiquidity::{IlliquidityMetrics, IlliquidityEngine, IlliquidityConfig}, 
};
use crate::persistence;


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
    pub trade_imbalance: Option<Decimal>,
    pub vwap_total: Option<Decimal>,
    pub price_change: Option<Decimal>,
    pub avg_trade_size: Option<Decimal>,
    pub signed_count_momentum: i64,
    pub trade_rate_10s: Option<f64>,
    pub order_flow_imbalance: Option<Decimal>,
    pub order_flow_pressure: Decimal,
    pub order_flow_significance: bool,
    pub vwap_10: Option<Decimal>,   
    pub vwap_50: Option<Decimal>,   
    pub vwap_100: Option<Decimal>,
    pub vwap_1000: Option<Decimal>,
    pub aggr_ratio_10: Option<Decimal>, 
    pub aggr_ratio_50: Option<Decimal>, 
    pub aggr_ratio_100: Option<Decimal>,
    pub aggr_ratio_1000: Option<Decimal>,
    pub volume_vector: Vec<(Decimal, (Decimal, Decimal))>,
    pub pwi_vector: Vec<(Decimal, Decimal)>,
    pub roll_spread: Option<Decimal>,
    pub amihuds_lambda: Option<Decimal>,
    pub kyles_lambda: Option<Decimal>,
    pub hasbroucks_lambda: Option<Decimal>,
    pub vpin: Option<Decimal>,
}

pub async fn run_analytics_task(
    order_book: Arc<ConcurrentOrderBook>,
    trades_log: Arc<ConcurrentTradesLog>,
    mut shutdown_rx: watch::Receiver<bool>,
    illiquidity_tx: Option<mpsc::Sender<IlliquidityMetrics>>,
) {

    const SIGNIFICANCE_THRESHOLD: Decimal = dec!(10.0);

    let mut interval = interval(Duration::from_millis(SNAPSHOT_INTERVAL_MS));
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut batch_id = 0;

    let mut illiquidity_engine = IlliquidityEngine::new(
        order_book.clone(),
        trades_log.clone(),
        Some(IlliquidityConfig::default()) 
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let (ob_snap, trade_snap) = tokio::join!(
                    order_book.get_snapshot(),
                    trades_log.get_snapshot()
                );

                let (flow_imbalance, flow_pressure) = order_book.get_flow_imbalance().await;
                
                let (volume_vector, pwi_vector) = tokio::join!(
                    order_book.volume_vector(),
                    order_book.pwi_vector()
                );

                let illiquidity_metrics = match illiquidity_engine.compute_metrics().await {
                    Ok(metrics) => metrics,
                    Err(e) => {
                        log::error!("Failed to compute illiquidity metrics: {}", e);
                        IlliquidityMetrics {
                            timestamp: Utc::now().to_rfc3339(),
                            roll_spread: None,
                            amihuds_lambda: None,
                            kyles_lambda: None,
                            hasbroucks_lambda: None,
                            vpin: None,
                        }
                    }
                };

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
                    vwap_10: trade_snap.vwap_10,
                    vwap_50: trade_snap.vwap_50,  
                    vwap_100: trade_snap.vwap_100,
                    vwap_1000: trade_snap.vwap_1000,
                    aggr_ratio_10: trade_snap.aggr_ratio_10,  
                    aggr_ratio_50: trade_snap.aggr_ratio_50,  
                    aggr_ratio_100: trade_snap.aggr_ratio_100,
                    aggr_ratio_1000: trade_snap.aggr_ratio_1000,
                    trade_imbalance: trade_snap.trade_imbalance,
                    vwap_total: trade_snap.vwap_total,
                    price_change: trade_snap.price_change,
                    avg_trade_size: trade_snap.avg_trade_size,
                    signed_count_momentum: trade_snap.signed_count_momentum,
                    trade_rate_10s: trade_snap.trade_rate_10s,
                    order_flow_imbalance: flow_imbalance,
                    order_flow_pressure: flow_pressure,
                    order_flow_significance: flow_pressure >= SIGNIFICANCE_THRESHOLD,
                    volume_vector,
                    pwi_vector,
                    roll_spread: illiquidity_metrics.roll_spread,
                    amihuds_lambda: illiquidity_metrics.amihuds_lambda,
                    kyles_lambda: illiquidity_metrics.kyles_lambda,
                    hasbroucks_lambda: illiquidity_metrics.hasbroucks_lambda,
                    vpin: illiquidity_metrics.vpin,
                };
                
                // Simple console output
                println!(
                    r#"
                [{}] CORE METRICS: MID: {:.4} | MICRO: {:.4} (Δ {:.4}) | SPRD: {:.4} | BID/ASK: {:.4}/{:.4} | IMB: {:.2}% | 
                PWI: 1%={:.2}% 5%={:.2}% 25%={:.2}% 50%={:.2}% SLOPE: B{:.4}/A{:.4}  | VOL_IMB: {:.2}% | DEPTH: B{:.2}/A{:.2}
                LIQUIDITY METRICS: Roll: {:.6} | Amihud: {:.6e} | Kyle: {:.4} | Hasbrouck: {:.4} | VPIN: {:.2}
                TRADE METRICS: LAST: {:.4} | VWAP: TOT={:.4} 10={:.4} 50={:.4} 100={:.4} 1000={:.4}| ΔPRICE: {:.4}% | SIZE: {:.2} 
                MOMENTUM: {} | RATE: {:.1}/s | AGGR: 10={:.2}% 50={:.2}% 100={:.2}% 1000={:.2}% | FLOW: IMB={:.3} | PRES={:.1} | {}
                VOLUME STRUCTURE: VOL(0.01%): B={:.2} A={:.2}  
                VEC: {:?}
                PWI_VEC: {:?}"#,
                    snapshot.timestamp,
                    // Core metrics
                    snapshot.mid_price.unwrap_or(dec!(0)),
                    snapshot.microprice.unwrap_or(dec!(0)),
                    snapshot.microprice.unwrap_or(dec!(0)) - snapshot.mid_price.unwrap_or(dec!(0)),
                    snapshot.spread.unwrap_or(dec!(0)),
                    snapshot.best_bid.unwrap_or(dec!(0)),
                    snapshot.best_ask.unwrap_or(dec!(0)),
                    snapshot.imbalance.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.pwi_1.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.pwi_5.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.pwi_25.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.pwi_50.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.bid_slope.unwrap_or(dec!(0)),
                    snapshot.ask_slope.unwrap_or(dec!(0)),
                    snapshot.volume_imbalance_top5.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.bid_depth_ratio.unwrap_or(dec!(0)),
                    snapshot.ask_depth_ratio.unwrap_or(dec!(0)),
                    
                    // Illiquidity metrics
                    snapshot.roll_spread.unwrap_or(dec!(0)),
                    snapshot.amihuds_lambda.unwrap_or(dec!(0)),
                    snapshot.kyles_lambda.unwrap_or(dec!(0)),
                    snapshot.hasbroucks_lambda.unwrap_or(dec!(0)),
                    snapshot.vpin.unwrap_or(dec!(0)),
                    
                    // Trade metrics
                    snapshot.last_trade_price.unwrap_or(dec!(0)),
                    snapshot.vwap_total.unwrap_or(dec!(0)),
                    snapshot.vwap_10.unwrap_or(dec!(0)),
                    snapshot.vwap_50.unwrap_or(dec!(0)),
                    snapshot.vwap_100.unwrap_or(dec!(0)),
                    snapshot.vwap_1000.unwrap_or(dec!(0)),
                    snapshot.price_change.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.avg_trade_size.unwrap_or(dec!(0)),
                    snapshot.signed_count_momentum,
                    snapshot.trade_rate_10s.unwrap_or(0.0),
                    snapshot.aggr_ratio_10.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.aggr_ratio_50.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.aggr_ratio_100.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.aggr_ratio_1000.unwrap_or(dec!(0)) * dec!(100),
                    snapshot.order_flow_imbalance.unwrap_or(dec!(0)),
                    snapshot.order_flow_pressure,
                    if snapshot.order_flow_significance { "SIG" } else { "insig" },
                    
                    // Volume structure
                    snapshot.bid_volume_001.unwrap_or(dec!(0)),
                    snapshot.ask_volume_001.unwrap_or(dec!(0)),
                    snapshot.volume_vector,
                    snapshot.pwi_vector
                );
                batch.push(snapshot);
                if batch.len() >= BATCH_SIZE {
                    let filename = format!(
                        "data/features_{}_{:03}.parquet",
                        chrono::Local::now().format("%Y%m%d_%H%M%S"), 
                        batch_id
                    );
                    if let Err(e) = persistence::save_feature_as_parquet(&batch, &filename) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        orderbook::ConcurrentOrderBook,
        tradeslog::{ConcurrentTradesLog, Trade},
    };
    use rust_decimal_macros::dec;
    use tokio::sync::watch;
    use std::sync::Arc;
    use chrono::Utc;

    #[tokio::test]
    async fn test_task_shutdown() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let order_book = Arc::new(ConcurrentOrderBook::new());
        let trades_log = Arc::new(ConcurrentTradesLog::new(10));

        let task = tokio::spawn(run_analytics_task(
            order_book,
            trades_log,
            shutdown_rx,
        ));

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_trade_processing() {
        let order_book = Arc::new(ConcurrentOrderBook::new());
        let trades_log = Arc::new(ConcurrentTradesLog::new(100));

        trades_log.insert_trade(Trade {
            price: dec!(100.0),
            quantity: dec!(1.0),
            timestamp: Utc::now().timestamp_millis() as u64,
            is_buyer_maker: false,
        }).await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_analytics_task(
            order_book,
            trades_log.clone(),
            shutdown_rx,
        ));

        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();

        let snapshot = trades_log.get_snapshot().await;
        assert_eq!(snapshot.last_price, Some(dec!(100.0)));
    }
}
