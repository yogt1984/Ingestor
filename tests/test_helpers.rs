// Test helpers for creating test data
use ingestor::tradeslog::Trade;
use ingestor::orderbook::ConcurrentOrderBook;
use ingestor::tradeslog::ConcurrentTradesLog;
use num::FromPrimitive;
use std::sync::Arc;

pub fn create_test_trade(price: f64, quantity: f64, timestamp: u64, is_buyer_maker: bool) -> Trade {
    use rust_decimal::Decimal;
    Trade {
        id: 0, // Will be auto-assigned by insert_trade
        price: Decimal::from_f64(price).unwrap(),
        quantity: Decimal::from_f64(quantity).unwrap(),
        timestamp,
        is_buyer_maker,
    }
}

pub fn create_test_orderbook() -> Arc<ConcurrentOrderBook> {
    Arc::new(ConcurrentOrderBook::new())
}

pub fn create_test_tradeslog(capacity: usize) -> Arc<ConcurrentTradesLog> {
    Arc::new(ConcurrentTradesLog::new(capacity))
}

pub async fn populate_orderbook(ob: &ConcurrentOrderBook, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
    for (price, qty) in bids {
        ob.apply_deltas(vec![(rust_decimal::Decimal::from_f64(*price).unwrap(), rust_decimal::Decimal::from_f64(*qty).unwrap())], vec![]).await;
    }
    for (price, qty) in asks {
        ob.apply_deltas(vec![], vec![(rust_decimal::Decimal::from_f64(*price).unwrap(), rust_decimal::Decimal::from_f64(*qty).unwrap())]).await;
    }
}

pub async fn populate_tradeslog(log: &ConcurrentTradesLog, trades: &[(f64, f64, u64, bool)]) {
    for (price, qty, ts, is_buyer) in trades {
        log.insert_trade(create_test_trade(*price, *qty, *ts, *is_buyer)).await;
    }
}

