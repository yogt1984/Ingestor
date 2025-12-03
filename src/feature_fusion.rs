use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use chrono::Utc;
use crate::{
    orderbook::{ConcurrentOrderBook, OrderBookFeatures},
    tradeslog::{ConcurrentTradesLog, TradesLogFeatures},
    illiquidity::IlliquidityMetrics,
    entropy::EntropyMetrics,
    volatility::VolatilityMetrics,
    toxicity::ToxicityMetrics,
};

#[derive(Debug, Serialize, Clone, Default)]
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
    pub tick_entropy_1s: Option<Decimal>,
    pub tick_entropy_5s: Option<Decimal>,
    pub tick_entropy_10s: Option<Decimal>,
    pub tick_entropy_15s: Option<Decimal>,
    pub tick_entropy_30s: Option<Decimal>,
    pub tick_entropy_1m: Option<Decimal>,
    pub tick_entropy_15m: Option<Decimal>,
    pub volume_tick_entropy_1s: Option<Decimal>,
    pub volume_tick_entropy_5s: Option<Decimal>,
    pub volume_tick_entropy_10s: Option<Decimal>,
    pub volume_tick_entropy_15s: Option<Decimal>,
    pub volume_tick_entropy_30s: Option<Decimal>,
    pub volume_tick_entropy_1m: Option<Decimal>,
    pub volume_tick_entropy_15m: Option<Decimal>,
    // Volatility metrics
    pub realized_volatility_100: Option<f64>,
    pub realized_volatility_1000: Option<f64>,
    pub bipower_variation_100: Option<f64>,
    pub jump_indicator: Option<f64>,
    pub vol_of_vol: Option<f64>,
    // Toxicity metrics
    pub toxic_flow_ratio_micro: Option<Decimal>,
    pub toxic_flow_ratio_mid: Option<Decimal>,
    pub adverse_selection_micro: Option<Decimal>,
    pub adverse_selection_mid: Option<Decimal>,
    pub arrival_asymmetry: Option<Decimal>,
    pub size_toxicity_ratio: Option<Decimal>,
    pub toxicity_index: Option<Decimal>,
}

pub struct FeatureFusionEngine {
    order_book:     Arc<ConcurrentOrderBook>,
    orderbook_rx:   mpsc::Receiver<OrderBookFeatures>,
    tradeslog_rx:   mpsc::Receiver<TradesLogFeatures>,
    illiquidity_rx: mpsc::Receiver<IlliquidityMetrics>,
    entropy_rx:     mpsc::Receiver<EntropyMetrics>,
    volatility_rx:  mpsc::Receiver<VolatilityMetrics>,
    toxicity_rx:    mpsc::Receiver<ToxicityMetrics>,
    fused_tx:       mpsc::Sender<FeaturesSnapshot>,
}

impl FeatureFusionEngine {
    pub fn new(
        order_book: Arc<ConcurrentOrderBook>,
        _trades_log: Arc<ConcurrentTradesLog>,
        orderbook_rx: mpsc::Receiver<OrderBookFeatures>,
        tradeslog_rx: mpsc::Receiver<TradesLogFeatures>,
        illiquidity_rx: mpsc::Receiver<IlliquidityMetrics>,
        entropy_rx: mpsc::Receiver<EntropyMetrics>,
        volatility_rx: mpsc::Receiver<VolatilityMetrics>,
        toxicity_rx: mpsc::Receiver<ToxicityMetrics>,
        fused_tx: mpsc::Sender<FeaturesSnapshot>,
    ) -> Self {
        Self {
            order_book,
            orderbook_rx,
            tradeslog_rx,
            illiquidity_rx,
            entropy_rx,
            volatility_rx,
            toxicity_rx,
            fused_tx,
        }
    }

    pub async fn run(
        mut self,
        mut shutdown_rx: watch::Receiver<bool>
    ) {
        const SIGNIFICANCE_THRESHOLD: Decimal = dec!(10.0);

        let mut latest_orderbook: Option<OrderBookFeatures> = None;
        let mut latest_tradeslog: Option<TradesLogFeatures> = None;
        let mut latest_illiquidity: Option<IlliquidityMetrics> = None;
        let mut latest_entropy: Option<EntropyMetrics> = None;
        let mut latest_volatility: Option<VolatilityMetrics> = None;
        let mut latest_toxicity: Option<ToxicityMetrics> = None;

        loop {
            tokio::select! {
                Some(ob_features) = self.orderbook_rx.recv() => {
                    latest_orderbook = Some(ob_features);
                },

                Some(trade_features) = self.tradeslog_rx.recv() => {
                    latest_tradeslog = Some(trade_features);
                },

                Some(metrics) = self.illiquidity_rx.recv() => {
                    latest_illiquidity = Some(metrics);
                },

                Some(metrics) = self.entropy_rx.recv() => {
                    latest_entropy = Some(metrics);
                },

                Some(metrics) = self.volatility_rx.recv() => {
                    latest_volatility = Some(metrics);
                },

                Some(metrics) = self.toxicity_rx.recv() => {
                    latest_toxicity = Some(metrics);
                },

                _ = shutdown_rx.changed() => {
                    println!("Analytics task shutting down...");
                    break;
                }
            }

            // Create snapshot whenever we have orderbook and tradeslog data
            if let (Some(ref ob_snap), Some(ref trade_snap)) = (&latest_orderbook, &latest_tradeslog) {
                let (flow_imbalance, flow_pressure) = self.order_book.get_flow_imbalance().await;

                let (volume_vector, pwi_vector) = tokio::join!(
                    self.order_book.volume_vector(),
                    self.order_book.pwi_vector()
                );

                let illiquidity_metrics = latest_illiquidity.clone().unwrap_or(IlliquidityMetrics {
                    timestamp: Utc::now().to_rfc3339(),
                    roll_spread: None,
                    amihuds_lambda: None,
                    kyles_lambda: None,
                    hasbroucks_lambda: None,
                    vpin: None,
                });

                let entropy_metrics = latest_entropy.clone().unwrap_or(EntropyMetrics {
                    timestamp: Utc::now().to_rfc3339(),
                    tick_entropy_1s: None,
                    tick_entropy_5s: None,
                    tick_entropy_10s: None,
                    tick_entropy_15s: None,
                    tick_entropy_30s: None,
                    tick_entropy_1m: None,
                    tick_entropy_15m: None,
                    volume_tick_entropy_1s: None,
                    volume_tick_entropy_5s: None,
                    volume_tick_entropy_10s: None,
                    volume_tick_entropy_15s: None,
                    volume_tick_entropy_30s: None,
                    volume_tick_entropy_1m: None,
                    volume_tick_entropy_15m: None,
                });

                let volatility_metrics = latest_volatility.clone().unwrap_or_default();
                let toxicity_metrics = latest_toxicity.clone().unwrap_or_default();

                let snapshot = FeaturesSnapshot {
                    timestamp: Utc::now().to_rfc3339(),
                    best_bid: ob_snap.best_bid.map(|(p, _)| p),
                    best_ask: ob_snap.best_ask.map(|(p, _)| p),
                    mid_price: ob_snap.mid_price,
                    microprice: ob_snap.microprice,
                    spread: ob_snap.spread,
                    imbalance: ob_snap.imbalance,
                    top_bids: ob_snap.top_bids.clone(),
                    top_asks: ob_snap.top_asks.clone(),
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
                    tick_entropy_1s: entropy_metrics.tick_entropy_1s,
                    tick_entropy_5s: entropy_metrics.tick_entropy_5s,
                    tick_entropy_10s: entropy_metrics.tick_entropy_10s,
                    tick_entropy_15s: entropy_metrics.tick_entropy_15s,
                    tick_entropy_30s: entropy_metrics.tick_entropy_30s,
                    tick_entropy_1m: entropy_metrics.tick_entropy_1m,
                    tick_entropy_15m: entropy_metrics.tick_entropy_15m,
                    volume_tick_entropy_1s: entropy_metrics.volume_tick_entropy_1s,
                    volume_tick_entropy_5s: entropy_metrics.volume_tick_entropy_5s,
                    volume_tick_entropy_10s: entropy_metrics.volume_tick_entropy_10s,
                    volume_tick_entropy_15s: entropy_metrics.volume_tick_entropy_15s,
                    volume_tick_entropy_30s: entropy_metrics.volume_tick_entropy_30s,
                    volume_tick_entropy_1m: entropy_metrics.volume_tick_entropy_1m,
                    volume_tick_entropy_15m: entropy_metrics.volume_tick_entropy_15m,
                    // Volatility metrics
                    realized_volatility_100: volatility_metrics.realized_volatility_100,
                    realized_volatility_1000: volatility_metrics.realized_volatility_1000,
                    bipower_variation_100: volatility_metrics.bipower_variation_100,
                    jump_indicator: volatility_metrics.jump_indicator,
                    vol_of_vol: volatility_metrics.vol_of_vol,
                    // Toxicity metrics
                    toxic_flow_ratio_micro: toxicity_metrics.toxic_flow_ratio_micro,
                    toxic_flow_ratio_mid: toxicity_metrics.toxic_flow_ratio_mid,
                    adverse_selection_micro: toxicity_metrics.adverse_selection_micro,
                    adverse_selection_mid: toxicity_metrics.adverse_selection_mid,
                    arrival_asymmetry: toxicity_metrics.arrival_asymmetry,
                    size_toxicity_ratio: toxicity_metrics.size_toxicity_ratio,
                    toxicity_index: toxicity_metrics.toxicity_index,
                };

                // Send snapshot whenever we have new data
                if let Err(e) = self.fused_tx.send(snapshot).await {
                    eprintln!("Failed to send fused features: {}", e);
                    break;
                }
            }
        } // end loop
    } // end run
}

#[allow(dead_code)]
fn print_snapshot(snapshot: &FeaturesSnapshot) {
    println!(
        r#"
[{timestamp}] CORE METRICS:
  MID:   {mid:.4} | MICRO: {micro:.4} (Δ {micro_delta:.4}) | SPRD: {spread:.4}
  BID/ASK: {bid:.4}/{ask:.4} | IMB: {imb:.2}%
  PWI: 1%={pwi1:.2}% 5%={pwi5:.2}% 25%={pwi25:.2}% 50%={pwi50:.2}%
  SLOPE: B{bid_slope:.4}/A{ask_slope:.4}
  VOL_IMB: {vol_imb:.2}% | DEPTH: B{bid_depth:.2}/A{ask_depth:.2}
LIQUIDITY METRICS:
  Roll: {roll:.6} | Amihud: {amihud:.6e} | Kyle: {kyle:.4} | Hasbrouck: {hasbrouck:.4} | VPIN: {vpin:.2}
TRADE METRICS:
  LAST: {last:.4} | VWAP TOT={vwap_tot:.4} 10={vwap10:.4} 50={vwap50:.4} 100={vwap100:.4} 1000={vwap1000:.4}
  ΔPRICE: {price_change:.4}% | SIZE: {avg_trade:.2}
  MOMENTUM: {momentum} | RATE: {rate:.1}/s
  AGGR: 10={aggr10:.2}% 50={aggr50:.2}% 100={aggr100:.2}% 1000={aggr1000:.2}%
  FLOW: IMB={flow_imb:.3} | PRES={flow_pres:.1} | {flow_sig}
VOLUME STRUCTURE:
  VOL(0.01%): B={vol_bid:.2} A={vol_ask:.2}
  VEC: {volume_vector:?}
  PWI_VEC: {pwi_vector:?}
ENTROPY METRICS:
  TICK_ENTROPY: 1s={te1:.4} 5s={te5:.4} 10s={te10:.4} 15s={te15:.4} 30s={te30:.4} 1m={te1m:.4} 15m={te15m:.4}
  VOL_ENTROPY : 1s={ve1:.4} 5s={ve5:.4} 10s={ve10:.4} 15s={ve15:.4} 30s={ve30:.4} 1m={ve1m:.4} 15m={ve15m:.4}
"#,
        timestamp = snapshot.timestamp,
        mid = snapshot.mid_price.unwrap_or(dec!(0)),
        micro = snapshot.microprice.unwrap_or(dec!(0)),
        micro_delta = snapshot.microprice.unwrap_or(dec!(0)) - snapshot.mid_price.unwrap_or(dec!(0)),
        spread = snapshot.spread.unwrap_or(dec!(0)),
        bid = snapshot.best_bid.unwrap_or(dec!(0)),
        ask = snapshot.best_ask.unwrap_or(dec!(0)),
        imb = snapshot.imbalance.unwrap_or(dec!(0)) * dec!(100),
        pwi1 = snapshot.pwi_1.unwrap_or(dec!(0)) * dec!(100),
        pwi5 = snapshot.pwi_5.unwrap_or(dec!(0)) * dec!(100),
        pwi25 = snapshot.pwi_25.unwrap_or(dec!(0)) * dec!(100),
        pwi50 = snapshot.pwi_50.unwrap_or(dec!(0)) * dec!(100),
        bid_slope = snapshot.bid_slope.unwrap_or(dec!(0)),
        ask_slope = snapshot.ask_slope.unwrap_or(dec!(0)),
        vol_imb = snapshot.volume_imbalance_top5.unwrap_or(dec!(0)) * dec!(100),
        bid_depth = snapshot.bid_depth_ratio.unwrap_or(dec!(0)),
        ask_depth = snapshot.ask_depth_ratio.unwrap_or(dec!(0)),
        roll = snapshot.roll_spread.unwrap_or(dec!(0)),
        amihud = snapshot.amihuds_lambda.unwrap_or(dec!(0)),
        kyle = snapshot.kyles_lambda.unwrap_or(dec!(0)),
        hasbrouck = snapshot.hasbroucks_lambda.unwrap_or(dec!(0)),
        vpin = snapshot.vpin.unwrap_or(dec!(0)),
        last = snapshot.last_trade_price.unwrap_or(dec!(0)),
        vwap_tot = snapshot.vwap_total.unwrap_or(dec!(0)),
        vwap10 = snapshot.vwap_10.unwrap_or(dec!(0)),
        vwap50 = snapshot.vwap_50.unwrap_or(dec!(0)),
        vwap100 = snapshot.vwap_100.unwrap_or(dec!(0)),
        vwap1000 = snapshot.vwap_1000.unwrap_or(dec!(0)),
        price_change = snapshot.price_change.unwrap_or(dec!(0)) * dec!(100),
        avg_trade = snapshot.avg_trade_size.unwrap_or(dec!(0)),
        momentum = snapshot.signed_count_momentum,
        rate = snapshot.trade_rate_10s.unwrap_or(0.0),
        aggr10 = snapshot.aggr_ratio_10.unwrap_or(dec!(0)) * dec!(100),
        aggr50 = snapshot.aggr_ratio_50.unwrap_or(dec!(0)) * dec!(100),
        aggr100 = snapshot.aggr_ratio_100.unwrap_or(dec!(0)) * dec!(100),
        aggr1000 = snapshot.aggr_ratio_1000.unwrap_or(dec!(0)) * dec!(100),
        flow_imb = snapshot.order_flow_imbalance.unwrap_or(dec!(0)),
        flow_pres = snapshot.order_flow_pressure,
        flow_sig = if snapshot.order_flow_significance { "SIG" } else { "insig" },
        vol_bid = snapshot.bid_volume_001.unwrap_or(dec!(0)),
        vol_ask = snapshot.ask_volume_001.unwrap_or(dec!(0)),
        volume_vector = snapshot.volume_vector,
        pwi_vector = snapshot.pwi_vector,
        te1 = snapshot.tick_entropy_1s.unwrap_or(dec!(0)),
        te5 = snapshot.tick_entropy_5s.unwrap_or(dec!(0)),
        te10 = snapshot.tick_entropy_10s.unwrap_or(dec!(0)),
        te15 = snapshot.tick_entropy_15s.unwrap_or(dec!(0)),
        te30 = snapshot.tick_entropy_30s.unwrap_or(dec!(0)),
        te1m = snapshot.tick_entropy_1m.unwrap_or(dec!(0)),
        te15m = snapshot.tick_entropy_15m.unwrap_or(dec!(0)),
        ve1 = snapshot.volume_tick_entropy_1s.unwrap_or(dec!(0)),
        ve5 = snapshot.volume_tick_entropy_5s.unwrap_or(dec!(0)),
        ve10 = snapshot.volume_tick_entropy_10s.unwrap_or(dec!(0)),
        ve15 = snapshot.volume_tick_entropy_15s.unwrap_or(dec!(0)),
        ve30 = snapshot.volume_tick_entropy_30s.unwrap_or(dec!(0)),
        ve1m = snapshot.volume_tick_entropy_1m.unwrap_or(dec!(0)),
        ve15m = snapshot.volume_tick_entropy_15m.unwrap_or(dec!(0)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        orderbook::ConcurrentOrderBook,
        tradeslog::{ConcurrentTradesLog, Trade},
    };
    use rust_decimal_macros::dec;
    use tokio::sync::{watch, mpsc};
    use tokio::time::Duration;
    use std::sync::Arc;
    use chrono::Utc;

    #[tokio::test]
    async fn test_task_shutdown() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let order_book = Arc::new(ConcurrentOrderBook::new());
        let trades_log = Arc::new(ConcurrentTradesLog::new(10));

        let (_ob_tx, ob_rx) = mpsc::channel(10);
        let (_tl_tx, tl_rx) = mpsc::channel(10);
        let (_ill_tx, ill_rx) = mpsc::channel(10);
        let (_ent_tx, ent_rx) = mpsc::channel(10);
        let (_vol_tx, vol_rx) = mpsc::channel(10);
        let (_tox_tx, tox_rx) = mpsc::channel(10);

        let (_feat_tx, _feat_rx) = mpsc::channel(10);
        let fusion = FeatureFusionEngine::new(order_book, trades_log.clone(), ob_rx, tl_rx, ill_rx, ent_rx, vol_rx, tox_rx, _feat_tx);
        let task = tokio::spawn(async move {
            fusion.run(shutdown_rx).await;
        });

        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_trade_processing() {
        let order_book = Arc::new(ConcurrentOrderBook::new());
        let trades_log = Arc::new(ConcurrentTradesLog::new(100));

        trades_log.insert_trade(Trade {
            id: 0,
            price: dec!(100.0),
            quantity: dec!(1.0),
            timestamp: Utc::now().timestamp_millis() as u64,
            is_buyer_maker: false,
        }).await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_ob_tx, ob_rx)          = mpsc::channel(10);
        let (_tl_tx, tl_rx)          = mpsc::channel(10);
        let (_ill_tx, ill_rx)          = mpsc::channel(10);
        let (_ent_tx, ent_rx)          = mpsc::channel(10);
        let (_vol_tx, vol_rx)          = mpsc::channel(10);
        let (_tox_tx, tox_rx)          = mpsc::channel(10);
        let (_feat_tx, feat_rx)        = mpsc::channel(10);
        drop(feat_rx);

        let fusion = FeatureFusionEngine::new(order_book, trades_log.clone(), ob_rx, tl_rx, ill_rx, ent_rx, vol_rx, tox_rx, _feat_tx);

        let task   = tokio::spawn(async move {
            fusion.run(shutdown_rx).await;
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();

        let snapshot = trades_log.get_snapshot().await;
        assert_eq!(snapshot.last_price, Some(dec!(100.0)));
    }
}

