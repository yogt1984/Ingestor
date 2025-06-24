#![allow(warnings)]

mod orderbook;
mod tradeslog;
mod lob_feed_manager;
mod log_feed_manager;
mod analytics;
mod persistence;
mod illiquidity;
mod entropy;


use std::sync::Arc;
use tokio::{spawn, sync::{watch, mpsc}, time::Duration};
use crate::{
    orderbook::ConcurrentOrderBook,
    tradeslog::ConcurrentTradesLog,
    lob_feed_manager::LobFeedManager,
    log_feed_manager::LogFeedManager,
    illiquidity::{IlliquidityEngine, IlliquidityMetrics, IlliquidityConfig},
    entropy::{EntropyEngine, EntropyMetrics, EntropyConfig},
    analytics::run_analytics_task
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

    // Create channels
    let (illiq_tx, illiq_rx)     = mpsc::channel::<IlliquidityMetrics>(100);
    let (entropy_tx, entropy_rx) = mpsc::channel::<EntropyMetrics>(100);

    let (persistence_tx, persistence_rx) = mpsc::channel::<IlliquidityMetrics>(100);

    // Spawn components
    let lob_handle = spawn(async move {
        lob_manager.start().await;
    });

    let trades_handle = spawn(async move {
        log_manager.start().await;
    });

    // Create and spawn illiquidity engine
    let illiquidity_engine = IlliquidityEngine::new(
        order_book_arc.clone(),
        trades_log_arc.clone(),
        Some(IlliquidityConfig::default()) // Add this line
    );

    let illiquidity_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            illiquidity_engine.run(shutdown_rx, persistence_tx).await.unwrap();
        }
    });

    let entropy_engine = EntropyEngine::new(
        trades_log_arc.clone(),
        Some(EntropyConfig {
            snapshot_interval_ms: 100, 
        })
    );
    
    let entropy_tx_clone = entropy_tx.clone(); // ← Clone before move
    
    let entropy_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            entropy_engine.run(shutdown_rx, entropy_tx_clone).await.unwrap();
        }
    });


    // Spawn analytics task
    let analytics_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            run_analytics_task(
                order_book_arc,
                trades_log_arc,
                shutdown_rx,
                Some(illiq_tx),
                Some(entropy_tx)
            ).await;
        }
    });

    let persistence_handle = spawn(async move {
        persistence::persist_metrics(
            persistence_rx,
            1000,
            "data/illiquidity",
            "illiquidity",
            persistence::save_illiquidity_as_parquet
        ).await.unwrap();
    });

    let entropy_persistence_handle = spawn(async move {
        persistence::persist_metrics(
            entropy_rx,
            1000,
            "data/entropy",
            "entropy",
            persistence::save_entropy_as_parquet
        ).await.unwrap();
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
        _ = illiquidity_handle => eprintln!("Illiquidity engine crashed"),
        _ = persistence_handle => eprintln!("Persistence task crashed"),
        _ = entropy_handle => eprintln!("Entropy engine crashed"),
        _ = entropy_persistence_handle => eprintln!("Entropy persistence task crashed"),
    }
}