#![allow(warnings)]

mod orderbook;
mod tradeslog;
mod lob_feed_manager;
mod log_feed_manager;
mod analytics;
mod persistence;

use std::sync::Arc;
use tokio::{spawn, join};
use orderbook::ConcurrentOrderBook;
use tradeslog::ConcurrentTradesLog;
use lob_feed_manager::LobFeedManager;
use log_feed_manager::LogFeedManager;

#[tokio::main]
async fn main() {
    env_logger::init();

    // Set up the order book feed manager
    let lob_manager = LobFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms".to_string(),
        "wss://stream.binance.com:9443/ws/btcusdt@depth".to_string(),
    );
    let order_book = lob_manager.get_order_book();

    // Set up the trade log and its feed manager
    let trades_log = ConcurrentTradesLog::new(10_000);
    let log_manager = LogFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
        trades_log.clone(),
    );

    // Spawn order book feed
    let lob_handle = spawn(async move {
        lob_manager.start().await;
    });

    // Spawn trade feed
    let trades_handle = spawn(async move {
        log_manager.start().await;
    });

    // Spawn analytics
    let analytics_handle = analytics::spawn_analytics_task(order_book.clone(), trades_log.clone());

    // Wait for all tasks
    let _ = join!(lob_handle, trades_handle, analytics_handle);
}
