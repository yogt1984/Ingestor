use ingestor::{
    feature_fusion::{FeatureFusionEngine, FeaturesSnapshot},
    orderbook::{ConcurrentOrderBook, OrderBookFeatures},
    tradeslog::{ConcurrentTradesLog, Trade, TradesLogFeatures},
    illiquidity::IlliquidityMetrics,
    entropy::EntropyMetrics,
};

use rust_decimal_macros::dec;
use tokio::{sync::{watch, mpsc}, time::{sleep, Duration}};
use std::sync::Arc;

#[tokio::test]
async fn test_full_analytics_pipeline() {
    let order_book = Arc::new(ConcurrentOrderBook::new());
    let trades_log = Arc::new(ConcurrentTradesLog::new(100));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    trades_log.insert_trade(Trade {
        id: 0, // Auto-assigned
        price: dec!(100.50),
        quantity: dec!(2.0),
        timestamp: 1000,
        is_buyer_maker: false,
    }).await;

    let (_ob_tx, ob_rx) = mpsc::channel::<OrderBookFeatures>(10);
    let (_tl_tx, tl_rx) = mpsc::channel::<TradesLogFeatures>(10);
    let (_ill_tx, ill_rx) = mpsc::channel::<IlliquidityMetrics>(10);
    let (_ent_tx, ent_rx) = mpsc::channel::<EntropyMetrics>(10);
    let (_feat_tx, feat_rx) = mpsc::channel::<FeaturesSnapshot>(10);
    drop(feat_rx);

    let fusion = FeatureFusionEngine::new(
        order_book,
        trades_log.clone(),
        ob_rx,
        tl_rx,
        ill_rx,
        ent_rx,
        _feat_tx,
    );
    let handle = tokio::spawn(async move {
        fusion.run(shutdown_rx).await;
    });

    sleep(Duration::from_millis(150)).await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();

    let snapshot = trades_log.get_snapshot().await;
    assert_eq!(snapshot.last_price, Some(dec!(100.50)));
}
