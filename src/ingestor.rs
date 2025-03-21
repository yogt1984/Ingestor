#![allow(warnings)]

use std::collections::VecDeque;
use std::sync::{Arc};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use log::{info, warn, error, debug};
use env_logger;
use linregress::{FormulaRegressionBuilder, RegressionDataBuilder};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::sleep;
use tokio::join;
use tokio::task;

const LOB_URL: &str     = "wss://stream.binance.com:9443/ws/btcusdt@depth50";
const TRADE_URL: &str   = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const QUEUE_SIZE: usize = 10_000;

pub struct MarketState {
    // 7 parallel buffers, each with fixed-size [50] order book entries
    pub bids_buffers:       [Arc<AsyncMutex<[(f64, f64); 50]>>; 7],
    pub asks_buffers:       [Arc<AsyncMutex<[(f64, f64); 50]>>; 7],
    pub trades:             Arc<AsyncMutex<VecDeque<(f64, f64, i64, bool)>>>,
    midprice:               AtomicU64,
    spread:                 AtomicU64,
    imbalance:              AtomicU64,
    slope:                  AtomicU64,
    depth5:                 AtomicU64,
    depth10:                AtomicU64,
    depth20:                AtomicU64,
    depth50:                AtomicU64,
    trade_intensity:        AtomicU64,
    intertrade_duration:    AtomicU64,
    vwap:                   AtomicU64,
    cwtd:                   AtomicU64,
    trade_volume_imbalance: AtomicU64,
    price_impact:           AtomicU64,
    amihud_lambda:          AtomicU64,
    kyle_lambda:            AtomicU64,
}

