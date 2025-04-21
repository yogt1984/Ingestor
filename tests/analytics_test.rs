use ingestor::{
    analytics::run_analytics_task,
    orderbook::ConcurrentOrderBook,
    tradeslog::{ConcurrentTradesLog, Trade},
};

use rust_decimal_macros::dec;
use tokio::{sync::watch, time::{sleep, Duration}};
use std::sync::Arc;

#[tokio::test]
async fn test_full_analytics_pipeline() {
    let order_book = Arc::new(ConcurrentOrderBook::new());
    let trades_log = Arc::new(ConcurrentTradesLog::new(100));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    trades_log.insert_trade(Trade {
        price: dec!(100.50),
        quantity: dec!(2.0),
        timestamp: 1000,
        is_buyer_maker: false,
    }).await;

    let handle = tokio::spawn(run_analytics_task(
        order_book,
        trades_log.clone(),
        shutdown_rx,
    ));

    sleep(Duration::from_millis(150)).await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();

    let snapshot = trades_log.get_snapshot().await;
    assert_eq!(snapshot.last_price, Some(dec!(100.50)));
}
