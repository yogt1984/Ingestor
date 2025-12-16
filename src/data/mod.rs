//! Data layer module
//!
//! Contains data structures and managers for order book, trades log,
//! WebSocket feed management, and Parquet persistence.

pub mod orderbook;
pub mod tradeslog;
pub mod lob_feed_manager;
pub mod log_feed_manager;
pub mod persistence;

pub use orderbook::{ConcurrentOrderBook, OrderBook, OrderBookEngine, OrderBookFeatures, OrderBookEngineConfig};
pub use tradeslog::{ConcurrentTradesLog, TradesLog, TradesLogEngine, TradesLogFeatures, TradesLogEngineConfig};
pub use lob_feed_manager::LobFeedManager;
pub use log_feed_manager::LogFeedManager;
pub use persistence::{PersistenceEngine, save_feature_as_parquet_path};
