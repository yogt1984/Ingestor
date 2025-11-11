#![allow(warnings)]

mod orderbook;
mod tradeslog;
mod lob_feed_manager;
mod log_feed_manager;
mod feature_fusion;
mod persistence;
mod illiquidity;
mod entropy;

use crossbeam::channel;
use std::sync::Arc;
use tokio::{spawn, sync::{watch, mpsc}, time::Duration};
use crate::{
    orderbook::{ConcurrentOrderBook,  OrderBookEngine,   OrderBookFeatures,  OrderBookEngineConfig},
    tradeslog::{ConcurrentTradesLog,  TradesLogEngine,   TradesLogFeatures,   TradesLogEngineConfig},     
    lob_feed_manager::LobFeedManager,
    log_feed_manager::LogFeedManager,
    illiquidity::{IlliquidityEngine, IlliquidityMetrics, IlliquidityConfig},
    entropy::{EntropyEngine,         EntropyMetrics,     EntropyConfig},
    feature_fusion::{FeatureFusionEngine, FeaturesSnapshot},
    persistence::PersistenceEngine,
};



#[tokio::main]
async fn main() {
    env_logger::init();

    let (shutdown_tx,    shutdown_rx)     = watch::channel(false);
    let (orderbook_tx,   orderbook_rx)    = mpsc::channel::<OrderBookFeatures>(100);
    let (tradeslog_tx,   tradeslog_rx)    = mpsc::channel::<TradesLogFeatures>(100);
    let (illiq_tx,       illiq_rx)        = mpsc::channel::<IlliquidityMetrics>(100);
    let (entropy_tx,     entropy_rx)      = mpsc::channel::<EntropyMetrics>(100);
    let (fused_tx,       fused_rx)        = mpsc::channel::<FeaturesSnapshot>(100);
    let (persist_tx,     persist_rx)      = channel::bounded::<FeaturesSnapshot>(2048);

    let ctrl_c = async {
        tokio::signal::ctrl_c().await.unwrap();
        shutdown_tx.send(true).unwrap();
    };

    let lob_manager    = LobFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms".to_string(),
        "wss://stream.binance.com:9443/ws/btcusdt@depth".to_string(),
    );
    let order_book_arc = Arc::new(lob_manager.get_order_book());

    let log_manager = LogFeedManager::new(
        "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
        10_000, 
    );
    let trades_log_arc = Arc::new(log_manager.get_trades_log());

    let orderbook_engine = OrderBookEngine::new(
        order_book_arc.clone(),
        Some(OrderBookEngineConfig::default()),  
        orderbook_tx,
    );

    let tradeslog_engine = TradesLogEngine::new(
        trades_log_arc.clone(),
        Some(TradesLogEngineConfig::default()),
        tradeslog_tx,
    );
    
    let illiquidity_engine = IlliquidityEngine::new(
        order_book_arc.clone(),
        trades_log_arc.clone(),
        Some(IlliquidityConfig::default()),
        illiq_tx,
    );

    let entropy_engine = EntropyEngine::new(
        order_book_arc.clone(), 
        trades_log_arc.clone(),
        Some(EntropyConfig::default()),
        entropy_tx,
    );

    let feature_fusion_engine = FeatureFusionEngine::new(
        order_book_arc.clone(),
        trades_log_arc.clone(),
        illiq_rx,
        entropy_rx,
        fused_tx,
    );

    let persistence_engine = PersistenceEngine::new(
        persist_rx,
        1000,
        "data/features",
        "features",
        96,
        persistence::save_feature_as_parquet_path,
    );

    let lob_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            lob_manager.start(shutdown_rx).await;
        }
    });

    let trades_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            log_manager.start(shutdown_rx).await;
        }
    });

    let orderbook_features_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            orderbook_engine.run(shutdown_rx).await.unwrap();
        }
    });

    let tradeslog_features_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            tradeslog_engine.run(shutdown_rx).await.unwrap();
        }
    });

    let illiquidity_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            illiquidity_engine.run(shutdown_rx).await.unwrap();
        }
    });

    let entropy_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            entropy_engine.run(shutdown_rx).await.unwrap();
        }
    });

    let analytics_handle = spawn({
        let shutdown_rx = shutdown_rx.clone();
        async move {
            feature_fusion_engine.run(shutdown_rx).await;
        }
    });

    let persistence_handle = tokio::task::spawn_blocking(move || persistence_engine.run());

    let forward_handle = {
        let mut fused_rx = fused_rx;
        tokio::spawn(async move {
            loop {
                match fused_rx.recv().await {
                    Some(snapshot) => {
                        // Non-blocking bridge with backoff if channel is full
                        let mut snapshot_opt = Some(snapshot);
                        while let Some(s) = snapshot_opt.take() {
                            match persist_tx.try_send(s) {
                                Ok(()) => break,
                                Err(channel::TrySendError::Full(s)) => {
                                    snapshot_opt = Some(s);
                                    tokio::time::sleep(Duration::from_millis(1)).await;
                                }
                                Err(channel::TrySendError::Disconnected(_)) => return,
                            }
                        }
                    }
                    None => break,
                }
            }
        })
    };


    tokio::select! {
        _ = ctrl_c                     => println!("Shutting down..."),
        _ = lob_handle                 => eprintln!("Order book feed crashed"),
        _ = trades_handle              => eprintln!("Trade feed crashed"),
        _ = analytics_handle           => eprintln!("Analytics task crashed"),
        _ = illiquidity_handle         => eprintln!("Illiquidity engine crashed"),
        res = persistence_handle        => match res {
            Ok(Ok(())) => (),
            Ok(Err(err)) => eprintln!("Persistence task crashed: {err:?}"),
            Err(err) => eprintln!("Persistence task join error: {err}"),
        },
        res = forward_handle            => if let Err(err) = res {
            eprintln!("Persistence forwarder crashed: {err}");
        },
        _ = entropy_handle             => eprintln!("Entropy engine crashed"),
    }
}
