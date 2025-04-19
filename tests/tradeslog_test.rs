use ingestor::tradeslog::{ConcurrentTradesLog, Trade, TradesLogError};
use rust_decimal_macros::dec;
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_concurrent_inserts() {
    let log = ConcurrentTradesLog::new(100);
    
    // Spawn two writers with explicit prices
    let log1 = log.clone();
    tokio::spawn(async move {
        log1.insert_trade(Trade {
            price: dec!(100),
            quantity: dec!(1),
            timestamp: 1000,
            is_buyer_maker: false,
        }).await;
    });

    let log2 = log.clone();
    tokio::spawn(async move {
        log2.insert_trade(Trade {
            price: dec!(101),
            quantity: dec!(2),
            timestamp: 2000,
            is_buyer_maker: true,
        }).await;
    });

    // Give time for inserts to complete
    sleep(Duration::from_millis(10)).await;

    // Verify results
    let trades = log.last_n_trades(2).await;
    assert_eq!(trades.len(), 2);
    assert!(trades.iter().any(|t| t.price == dec!(100)));
    assert!(trades.iter().any(|t| t.price == dec!(101)));
}

#[tokio::test]
async fn test_vwap_concurrent() {
    let log = ConcurrentTradesLog::new(10);
    
    // Spawn tasks with explicit values
    for i in 0..5 {
        let price = match i {
            0 => dec!(100),
            1 => dec!(101),
            2 => dec!(102),
            3 => dec!(103),
            4 => dec!(104),
            _ => unreachable!(),
        };
        
        let log_clone = log.clone();
        tokio::spawn(async move {
            log_clone.insert_trade(Trade {
                price,
                quantity: dec!(1),
                timestamp: i * 1000,
                is_buyer_maker: i % 2 == 0,
            }).await;
        });
    }

    sleep(Duration::from_millis(10)).await;
    
    // Calculate VWAP (100 + 101 + 102 + 103 + 104)/5 = 102
    let vwap = log.vwap(5).await.unwrap();
    assert_eq!(vwap, dec!(102));
}

#[tokio::test]
async fn test_snapshot_concurrent() {
    // Initialize log and synchronization barrier
    let log = ConcurrentTradesLog::new(10);
    let barrier = Arc::new(Barrier::new(2)); 

    log.insert_trade(Trade {
        price: dec!(100),
        quantity: dec!(1),
        timestamp: 1000,
        is_buyer_maker: false,
    }).await;

    // Clone resources for spawned task
    let log_clone = log.clone();
    let barrier_clone = barrier.clone();

    // Spawn snapshot task
    let handle = tokio::spawn(async move {
        barrier_clone.wait().await; // Sync point: waits until main thread reaches barrier
        log_clone.get_snapshot().await
    });

    // Ensure snapshot starts ONLY after this point
    barrier.wait().await; // Sync point: releases the spawned task

    // Insert second trade (guaranteed to happen AFTER snapshot starts)
    log.insert_trade(Trade {
        price: dec!(101),
        quantity: dec!(2),
        timestamp: 2000,
        is_buyer_maker: true,
    }).await;

    // Verify snapshot reflects ONLY the first trade
    let snapshot = handle.await.unwrap();
    assert_eq!(snapshot.last_price, Some(dec!(100)), "Snapshot should include only the first trade");
    assert_eq!(snapshot.trade_imbalance, Some(dec!(1.0)));
}

#[tokio::test]
async fn test_aggressor_ratio_concurrent() {
    let log = ConcurrentTradesLog::new(10);
    
    // Spawn tasks with explicit values
    for i in 0..4 {
        let (price, qty, is_buyer) = match i {
            0 => (dec!(100), dec!(1), false),
            1 => (dec!(101), dec!(2), true),
            2 => (dec!(102), dec!(3), false),
            3 => (dec!(103), dec!(4), true),
            _ => unreachable!(),
        };
        
        let log_clone = log.clone();
        tokio::spawn(async move {
            log_clone.insert_trade(Trade {
                price,
                quantity: qty,
                timestamp: i * 1000,
                is_buyer_maker: is_buyer,
            }).await;
        });
    }

    sleep(Duration::from_millis(10)).await;
    
    // Check aggressor ratio (buy volume = 1 + 3 = 4, total = 1+2+3+4 = 10)
    let ratio = log.aggressor_volume_ratio(4).await.unwrap();
    assert_eq!(ratio, dec!(0.4));
}

#[tokio::test]
async fn test_empty_log_behavior() {
    let log = ConcurrentTradesLog::new(10);
    
    assert!(log.last_price().await.is_none());
    assert!(matches!(
        log.vwap(1).await,
        Err(TradesLogError::InsufficientTrades)
    ));
}

#[tokio::test]
async fn test_zero_volume_trades() {
    let log = ConcurrentTradesLog::new(10);
    
    log.insert_trade(Trade {
        price: dec!(100),
        quantity: dec!(0),
        timestamp: 1000,
        is_buyer_maker: false,
    }).await;

    assert!(matches!(
        log.vwap(1).await,
        Err(TradesLogError::ZeroVolume)
    ));
}
