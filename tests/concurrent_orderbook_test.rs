use ingestor::orderbook::ConcurrentOrderBook;
use rust_decimal_macros::dec;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_concurrent_updates() {
    let book = ConcurrentOrderBook::new();
    
    // Spawn a writer task
    let book_clone = book.clone();
    tokio::spawn(async move {
        book_clone.apply_snapshot(
            vec![(dec!(100.0), dec!(1.0))],
            vec![(dec!(101.0), dec!(1.0))],
        ).await;
    });

    // Spawn a reader task
    let book_clone = book.clone();
    let handle = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await; // Ensure write happens first
        book_clone.best_bid().await
    });

    assert_eq!(handle.await.unwrap(), Some((dec!(100.0), dec!(1.0))));
}

#[tokio::test]
async fn test_flow_tracker_threadsafe() {
    let book = ConcurrentOrderBook::new();
    
    // Add sufficient orders to ensure minimum pressure
    book.apply_deltas(
        vec![
            (dec!(100.0), dec!(3.0)), // Add bid (above min_pressure of 2.5)
            (dec!(101.0), dec!(0.0))  // Cancel bid (to test penalty)
        ],
        vec![
            (dec!(102.0), dec!(1.0))  // Add ask
        ],
    ).await;

    let (imbalance, pressure) = book.get_flow_imbalance().await;
    
    // Verify we have sufficient pressure
    assert!(
        pressure >= dec!(2.5),
        "Pressure {} below minimum {}",
        pressure,
        dec!(2.5)
    );
    
    // Verify we got an imbalance reading
    assert!(
        imbalance.is_some(),
        "Expected Some(imbalance) with sufficient pressure"
    );
    
    // Verify cancel penalty had effect
    let imbalance_value = imbalance.unwrap();
    assert!(
        imbalance_value < dec!(1.0),  // Should be less than pure bid pressure
        "Cancel should reduce imbalance, got {}",
        imbalance_value
    );
}