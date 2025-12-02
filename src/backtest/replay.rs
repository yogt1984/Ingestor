//! Parquet Replay Engine
//!
//! Reads historical feature data from Parquet files and replays them
//! as a time-ordered stream for backtesting.

use std::path::{Path, PathBuf};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

use polars::prelude::*;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};

use crate::feature_fusion::FeaturesSnapshot;

/// Configuration for the replay engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Directory containing Parquet files
    pub data_dir: PathBuf,
    /// Start timestamp (inclusive), None = from beginning
    pub start_time: Option<i64>,
    /// End timestamp (inclusive), None = to end
    pub end_time: Option<i64>,
    /// Speed multiplier (1.0 = real-time, 0.0 = as fast as possible)
    pub speed: f64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/features"),
            start_time: None,
            end_time: None,
            speed: 0.0, // As fast as possible for backtesting
        }
    }
}

/// A replay event with timestamp for ordering
#[derive(Debug, Clone)]
pub struct ReplayEvent {
    pub timestamp_ms: i64,
    pub snapshot: FeaturesSnapshot,
}

impl PartialEq for ReplayEvent {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_ms == other.timestamp_ms
    }
}

impl Eq for ReplayEvent {}

impl PartialOrd for ReplayEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReplayEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (earliest first)
        other.timestamp_ms.cmp(&self.timestamp_ms)
    }
}

/// Parquet replay engine
pub struct ParquetReplay {
    config: ReplayConfig,
    events: Vec<ReplayEvent>,
    current_index: usize,
}

impl ParquetReplay {
    /// Create a new replay engine with the given configuration
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            events: Vec::new(),
            current_index: 0,
        }
    }

    /// Load all Parquet files from the data directory
    pub fn load(&mut self) -> Result<usize> {
        let data_dir = &self.config.data_dir;

        if !data_dir.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", data_dir);
        }

        let mut all_events = Vec::new();

        // Find all Parquet files
        let mut parquet_files: Vec<PathBuf> = std::fs::read_dir(data_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
            .collect();

        parquet_files.sort();

        if parquet_files.is_empty() {
            anyhow::bail!("No Parquet files found in {:?}", data_dir);
        }

        log::info!("Loading {} Parquet files from {:?}", parquet_files.len(), data_dir);

        for path in &parquet_files {
            let events = self.load_file(path)?;
            all_events.extend(events);
        }

        // Sort by timestamp
        all_events.sort_by_key(|e| e.timestamp_ms);

        // Apply time filters
        if let Some(start) = self.config.start_time {
            all_events.retain(|e| e.timestamp_ms >= start);
        }
        if let Some(end) = self.config.end_time {
            all_events.retain(|e| e.timestamp_ms <= end);
        }

        let count = all_events.len();
        self.events = all_events;
        self.current_index = 0;

        log::info!("Loaded {} events", count);
        Ok(count)
    }

    /// Load a single Parquet file
    fn load_file(&self, path: &Path) -> Result<Vec<ReplayEvent>> {
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .collect()
            .context(format!("Failed to read {:?}", path))?;

        let mut events = Vec::with_capacity(df.height());

        for i in 0..df.height() {
            let snapshot = self.row_to_snapshot(&df, i)?;
            // Parse timestamp from RFC3339 string to milliseconds
            let timestamp_ms = self.get_string(&df, "timestamp", i)
                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(&ts).ok())
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0);

            events.push(ReplayEvent {
                timestamp_ms,
                snapshot,
            });
        }

        Ok(events)
    }

    /// Convert a DataFrame row to a FeaturesSnapshot
    fn row_to_snapshot(&self, df: &DataFrame, row: usize) -> Result<FeaturesSnapshot> {
        Ok(FeaturesSnapshot {
            timestamp: self.get_string(df, "timestamp", row).unwrap_or_default(),

            // Order Book
            best_bid: self.get_decimal(df, "best_bid", row),
            best_ask: self.get_decimal(df, "best_ask", row),
            mid_price: self.get_decimal(df, "mid_price", row),
            microprice: self.get_decimal(df, "microprice", row),
            spread: self.get_decimal(df, "spread", row),
            imbalance: self.get_decimal(df, "imbalance", row),
            pwi_1: self.get_decimal(df, "pwi_1", row),
            pwi_5: self.get_decimal(df, "pwi_5", row),
            pwi_25: self.get_decimal(df, "pwi_25", row),
            pwi_50: self.get_decimal(df, "pwi_50", row),
            bid_slope: self.get_decimal(df, "bid_slope", row),
            ask_slope: self.get_decimal(df, "ask_slope", row),
            volume_imbalance_top5: self.get_decimal(df, "volume_imbalance_top5", row),
            bid_depth_ratio: self.get_decimal(df, "bid_depth_ratio", row),
            ask_depth_ratio: self.get_decimal(df, "ask_depth_ratio", row),
            bid_volume_001: self.get_decimal(df, "bid_volume_001", row),
            ask_volume_001: self.get_decimal(df, "ask_volume_001", row),
            bid_avg_distance: self.get_decimal(df, "bid_avg_distance", row),
            ask_avg_distance: self.get_decimal(df, "ask_avg_distance", row),
            top_bids: Vec::new(),
            top_asks: Vec::new(),

            // Trades
            last_trade_price: self.get_decimal(df, "last_trade_price", row),
            trade_imbalance: self.get_decimal(df, "trade_imbalance", row),
            vwap_total: self.get_decimal(df, "vwap_total", row),
            vwap_10: self.get_decimal(df, "vwap_10", row),
            vwap_50: self.get_decimal(df, "vwap_50", row),
            vwap_100: self.get_decimal(df, "vwap_100", row),
            vwap_1000: self.get_decimal(df, "vwap_1000", row),
            price_change: self.get_decimal(df, "price_change", row),
            avg_trade_size: self.get_decimal(df, "avg_trade_size", row),
            signed_count_momentum: self.get_i64(df, "signed_count_momentum", row).unwrap_or(0),
            trade_rate_10s: self.get_f64(df, "trade_rate_10s", row),
            aggr_ratio_10: self.get_decimal(df, "aggr_ratio_10", row),
            aggr_ratio_50: self.get_decimal(df, "aggr_ratio_50", row),
            aggr_ratio_100: self.get_decimal(df, "aggr_ratio_100", row),
            aggr_ratio_1000: self.get_decimal(df, "aggr_ratio_1000", row),

            // Order Flow
            order_flow_imbalance: self.get_decimal(df, "order_flow_imbalance", row),
            order_flow_pressure: self.get_decimal(df, "order_flow_pressure", row).unwrap_or_default(),
            order_flow_significance: false,

            // Vectors (empty for replay - not stored in parquet)
            volume_vector: Vec::new(),
            pwi_vector: Vec::new(),

            // Illiquidity
            roll_spread: self.get_decimal(df, "roll_spread", row),
            amihuds_lambda: self.get_decimal(df, "amihuds_lambda", row),
            kyles_lambda: self.get_decimal(df, "kyles_lambda", row),
            hasbroucks_lambda: self.get_decimal(df, "hasbroucks_lambda", row),
            vpin: self.get_decimal(df, "vpin", row),

            // Entropy
            tick_entropy_1s: self.get_decimal(df, "tick_entropy_1s", row),
            tick_entropy_5s: self.get_decimal(df, "tick_entropy_5s", row),
            tick_entropy_10s: self.get_decimal(df, "tick_entropy_10s", row),
            tick_entropy_15s: self.get_decimal(df, "tick_entropy_15s", row),
            tick_entropy_30s: self.get_decimal(df, "tick_entropy_30s", row),
            tick_entropy_1m: self.get_decimal(df, "tick_entropy_1m", row),
            tick_entropy_15m: self.get_decimal(df, "tick_entropy_15m", row),
            volume_tick_entropy_1s: self.get_decimal(df, "volume_tick_entropy_1s", row),
            volume_tick_entropy_5s: self.get_decimal(df, "volume_tick_entropy_5s", row),
            volume_tick_entropy_10s: self.get_decimal(df, "volume_tick_entropy_10s", row),
            volume_tick_entropy_15s: self.get_decimal(df, "volume_tick_entropy_15s", row),
            volume_tick_entropy_30s: self.get_decimal(df, "volume_tick_entropy_30s", row),
            volume_tick_entropy_1m: self.get_decimal(df, "volume_tick_entropy_1m", row),
            volume_tick_entropy_15m: self.get_decimal(df, "volume_tick_entropy_15m", row),

            // Volatility
            realized_volatility_100: self.get_f64(df, "realized_volatility_100", row),
            realized_volatility_1000: self.get_f64(df, "realized_volatility_1000", row),
            bipower_variation_100: self.get_f64(df, "bipower_variation_100", row),
            jump_indicator: self.get_f64(df, "jump_indicator", row),
            vol_of_vol: self.get_f64(df, "vol_of_vol", row),

            // Toxicity
            toxic_flow_ratio_micro: self.get_decimal(df, "toxic_flow_ratio_micro", row),
            toxic_flow_ratio_mid: self.get_decimal(df, "toxic_flow_ratio_mid", row),
            adverse_selection_micro: self.get_decimal(df, "adverse_selection_micro", row),
            adverse_selection_mid: self.get_decimal(df, "adverse_selection_mid", row),
            arrival_asymmetry: self.get_decimal(df, "arrival_asymmetry", row),
            size_toxicity_ratio: self.get_decimal(df, "size_toxicity_ratio", row),
            toxicity_index: self.get_decimal(df, "toxicity_index", row),
        })
    }

    /// Get a Decimal value from a DataFrame column
    fn get_decimal(&self, df: &DataFrame, col: &str, row: usize) -> Option<Decimal> {
        df.column(col).ok().and_then(|s| {
            s.f64().ok().and_then(|ca| {
                ca.get(row).and_then(|v| Decimal::from_f64(v))
            })
        })
    }

    /// Get an f64 value from a DataFrame column
    fn get_f64(&self, df: &DataFrame, col: &str, row: usize) -> Option<f64> {
        df.column(col).ok().and_then(|s| {
            s.f64().ok().and_then(|ca| ca.get(row))
        })
    }

    /// Get an i64 value from a DataFrame column
    fn get_i64(&self, df: &DataFrame, col: &str, row: usize) -> Option<i64> {
        df.column(col).ok().and_then(|s| {
            s.i64().ok().and_then(|ca| ca.get(row))
        })
    }

    /// Get a String value from a DataFrame column
    fn get_string(&self, df: &DataFrame, col: &str, row: usize) -> Option<String> {
        df.column(col).ok().and_then(|s| {
            s.utf8().ok().and_then(|ca| ca.get(row).map(|s| s.to_string()))
        })
    }

    /// Get the next event in the replay
    pub fn next(&mut self) -> Option<ReplayEvent> {
        if self.current_index < self.events.len() {
            let event = self.events[self.current_index].clone();
            self.current_index += 1;
            Some(event)
        } else {
            None
        }
    }

    /// Peek at the next event without consuming it
    pub fn peek(&self) -> Option<&ReplayEvent> {
        self.events.get(self.current_index)
    }

    /// Reset the replay to the beginning
    pub fn reset(&mut self) {
        self.current_index = 0;
    }

    /// Get the total number of events
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if there are no events
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get the current position
    pub fn position(&self) -> usize {
        self.current_index
    }

    /// Get progress as a fraction [0, 1]
    pub fn progress(&self) -> f64 {
        if self.events.is_empty() {
            0.0
        } else {
            self.current_index as f64 / self.events.len() as f64
        }
    }

    /// Get time range of loaded data
    pub fn time_range(&self) -> Option<(i64, i64)> {
        if self.events.is_empty() {
            None
        } else {
            Some((
                self.events.first().unwrap().timestamp_ms,
                self.events.last().unwrap().timestamp_ms,
            ))
        }
    }

    /// Create an iterator over all events
    pub fn iter(&self) -> impl Iterator<Item = &ReplayEvent> {
        self.events.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_replay_config_default() {
        let config = ReplayConfig::default();
        assert_eq!(config.speed, 0.0);
        assert!(config.start_time.is_none());
        assert!(config.end_time.is_none());
    }

    #[test]
    fn test_replay_empty_dir() {
        let dir = tempdir().unwrap();
        let config = ReplayConfig {
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let mut replay = ParquetReplay::new(config);
        assert!(replay.load().is_err()); // Should fail - no parquet files
    }

    #[test]
    fn test_event_ordering() {
        let e1 = ReplayEvent {
            timestamp_ms: 100,
            snapshot: FeaturesSnapshot::default(),
        };
        let e2 = ReplayEvent {
            timestamp_ms: 200,
            snapshot: FeaturesSnapshot::default(),
        };

        // For min-heap, earlier should be "greater" (reversed)
        assert!(e1 > e2);
    }
}
