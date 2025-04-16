#![allow(warnings)]

mod orderbook;
mod tradeslog;
mod lob_feed_manager;
mod log_feed_manager;
mod analytics;
mod persistence;

use std::sync::Arc;
use tokio::{spawn, sync::watch};
use crate::{
    orderbook::ConcurrentOrderBook,
    tradeslog::ConcurrentTradesLog,
    lob_feed_manager::LobFeedManager,
    log_feed_manager::LogFeedManager
};

#[tokio::main]
async fn main() {
    env_logger::init();

    // Set up shutdown channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Set up the order book feed manager
    let lob_manager = LobFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms".to_string(),
        "wss://stream.binance.com:9443/ws/btcusdt@depth".to_string(),
    );
    let order_book = lob_manager.get_order_book();
    let order_book_arc = Arc::new(order_book);

    // Set up the trade log and its feed manager
    let trades_log = ConcurrentTradesLog::new(10_000);
    let trades_log_arc = Arc::new(trades_log.clone());
    let log_manager = LogFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
        trades_log,
    );

    // Spawn components
    let lob_handle = spawn(async move {
        lob_manager.start().await;
    });

    let trades_handle = spawn(async move {
        log_manager.start().await;
    });

    let analytics_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            analytics::run_analytics_task(
                order_book_arc,
                trades_log_arc,
                shutdown_rx
            ).await;
        }
    });

    // Ctrl+C handler
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.unwrap();
        shutdown_tx.send(true).unwrap();
    };

    tokio::select! {
        _ = ctrl_c => println!("Shutting down..."),
        _ = lob_handle => eprintln!("Order book feed crashed"),
        _ = trades_handle => eprintln!("Trade feed crashed"),
        _ = analytics_handle => eprintln!("Analytics task crashed"),
    }
}