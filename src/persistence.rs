use anyhow::{Context, Result};
use polars::prelude::*;
use serde_json;
use serde::Serialize;
use crate::{
    feature_fusion::FeaturesSnapshot,
};
use crate::illiquidity::IlliquidityMetrics;
use rust_decimal::prelude::ToPrimitive;
use crate::entropy::EntropyMetrics;
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::Write;
use std::collections::VecDeque;
use std::ffi::OsStr;
use crossbeam::channel::Receiver;
use log::warn;


/// Save a batch of features to Parquet with comprehensive error handling
pub fn save_feature_as_parquet(features: &[FeaturesSnapshot], filepath: impl AsRef<Path>) -> Result<()> {
    let filepath = filepath.as_ref();
    // Convert Decimal fields to f64 with proper error handling
    fn decimal_to_f64(d: Option<rust_decimal::Decimal>) -> Option<f64> {
        d.and_then(|d| d.to_f64())
    }

    // Serialize complex fields to JSON strings
    fn serialize_complex<T: serde::Serialize>(value: &T) -> String {
        serde_json::to_string(value).unwrap_or_else(|_| "[]".to_string())
    }

    let mut df = df! [
        "timestamp" => features.iter().map(|f| f.timestamp.clone()).collect::<Vec<_>>(),
        "best_bid" => features.iter().map(|f| decimal_to_f64(f.best_bid)).collect::<Vec<_>>(),
        "best_ask" => features.iter().map(|f| decimal_to_f64(f.best_ask)).collect::<Vec<_>>(),
        "mid_price" => features.iter().map(|f| decimal_to_f64(f.mid_price)).collect::<Vec<_>>(),
        "microprice" => features.iter().map(|f| decimal_to_f64(f.microprice)).collect::<Vec<_>>(),
        "spread" => features.iter().map(|f| decimal_to_f64(f.spread)).collect::<Vec<_>>(),
        "imbalance" => features.iter().map(|f| decimal_to_f64(f.imbalance)).collect::<Vec<_>>(),
        "top_bids" => features.iter().map(|f| serialize_complex(&f.top_bids)).collect::<Vec<_>>(),
        "top_asks" => features.iter().map(|f| serialize_complex(&f.top_asks)).collect::<Vec<_>>(),
        "pwi_1" => features.iter().map(|f| decimal_to_f64(f.pwi_1)).collect::<Vec<_>>(),
        "pwi_5" => features.iter().map(|f| decimal_to_f64(f.pwi_5)).collect::<Vec<_>>(),
        "pwi_25" => features.iter().map(|f| decimal_to_f64(f.pwi_25)).collect::<Vec<_>>(),
        "pwi_50" => features.iter().map(|f| decimal_to_f64(f.pwi_50)).collect::<Vec<_>>(),
        "bid_slope" => features.iter().map(|f| decimal_to_f64(f.bid_slope)).collect::<Vec<_>>(),
        "ask_slope" => features.iter().map(|f| decimal_to_f64(f.ask_slope)).collect::<Vec<_>>(),
        "volume_imbalance_top5" => features.iter().map(|f| decimal_to_f64(f.volume_imbalance_top5)).collect::<Vec<_>>(),
        "bid_depth_ratio" => features.iter().map(|f| decimal_to_f64(f.bid_depth_ratio)).collect::<Vec<_>>(),
        "ask_depth_ratio" => features.iter().map(|f| decimal_to_f64(f.ask_depth_ratio)).collect::<Vec<_>>(),
        "bid_volume_001" => features.iter().map(|f| decimal_to_f64(f.bid_volume_001)).collect::<Vec<_>>(),
        "ask_volume_001" => features.iter().map(|f| decimal_to_f64(f.ask_volume_001)).collect::<Vec<_>>(),
        "bid_avg_distance" => features.iter().map(|f| decimal_to_f64(f.bid_avg_distance)).collect::<Vec<_>>(),
        "ask_avg_distance" => features.iter().map(|f| decimal_to_f64(f.ask_avg_distance)).collect::<Vec<_>>(),
        "last_trade_price" => features.iter().map(|f| decimal_to_f64(f.last_trade_price)).collect::<Vec<_>>(),
        "trade_imbalance" => features.iter().map(|f| decimal_to_f64(f.trade_imbalance)).collect::<Vec<_>>(),
        "vwap_total" => features.iter().map(|f| decimal_to_f64(f.vwap_total)).collect::<Vec<_>>(),
        "price_change" => features.iter().map(|f| decimal_to_f64(f.price_change)).collect::<Vec<_>>(),
        "avg_trade_size" => features.iter().map(|f| decimal_to_f64(f.avg_trade_size)).collect::<Vec<_>>(),
        "signed_count_momentum" => features.iter().map(|f| f.signed_count_momentum).collect::<Vec<_>>(),
        "trade_rate_10s" => features.iter().map(|f| f.trade_rate_10s.unwrap_or(f64::NAN)).collect::<Vec<_>>(),
        "order_flow_imbalance" => features.iter().map(|f| decimal_to_f64(f.order_flow_imbalance)).collect::<Vec<_>>(),
        "order_flow_pressure" => features.iter().map(|f| decimal_to_f64(Some(f.order_flow_pressure))).collect::<Vec<_>>(),
        "order_flow_significance" => features.iter().map(|f| f.order_flow_significance).collect::<Vec<_>>(),
        "vwap_10" => features.iter().map(|f| decimal_to_f64(f.vwap_10)).collect::<Vec<_>>(),
        "vwap_50" => features.iter().map(|f| decimal_to_f64(f.vwap_50)).collect::<Vec<_>>(),
        "vwap_100" => features.iter().map(|f| decimal_to_f64(f.vwap_100)).collect::<Vec<_>>(),
        "vwap_1000" => features.iter().map(|f| decimal_to_f64(f.vwap_1000)).collect::<Vec<_>>(),
        "aggr_ratio_10" => features.iter().map(|f| decimal_to_f64(f.aggr_ratio_10)).collect::<Vec<_>>(),
        "aggr_ratio_50" => features.iter().map(|f| decimal_to_f64(f.aggr_ratio_50)).collect::<Vec<_>>(),
        "aggr_ratio_100" => features.iter().map(|f| decimal_to_f64(f.aggr_ratio_100)).collect::<Vec<_>>(),
        "aggr_ratio_1000" => features.iter().map(|f| decimal_to_f64(f.aggr_ratio_1000)).collect::<Vec<_>>(),
    ].context("Failed to create DataFrame")?;

    // Create parent directories if they don't exist
    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent).context("Failed to create output directory")?;
    }

    // Write with compression and proper error handling
    ParquetWriter::new(File::create(filepath).context("Failed to create output file")?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)
        .context("Failed to write Parquet file")?;

    Ok(())
}

/// Wrapper with a concrete `&Path` signature for function pointer usage
pub fn save_feature_as_parquet_path(features: &[FeaturesSnapshot], filepath: &Path) -> Result<()> {
    save_feature_as_parquet(features, filepath)
}

/// Write newline-delimited JSON for human readability
fn write_jsonl<T: Serialize>(items: &[T], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("Failed to create JSONL output directory")?;
    }
    let mut file = File::create(path).with_context(|| format!("Failed to create {:?}", path))?;
    for item in items {
        let line = serde_json::to_string(item).context("Failed to serialize item to JSON")?;
        file.write_all(line.as_bytes()).context("Failed to write JSONL line")?;
        file.write_all(b"\n").context("Failed to write newline")?;
    }
    Ok(())
}


fn decimal_to_f64(d: Option<rust_decimal::Decimal>) -> Option<f64> {
    d.and_then(|d| d.to_f64())
}

/// Saves illiquidity metrics with identical patterns to feature persistence
pub fn save_illiquidity_as_parquet(
    snapshots: &[IlliquidityMetrics],
    filepath: impl AsRef<Path>
) -> Result<()> {
    let filepath = filepath.as_ref();
    let mut df = df! [
        "timestamp" => snapshots.iter().map(|s| s.timestamp.clone()).collect::<Vec<_>>(),
        "roll_spread" => snapshots.iter().map(|s| decimal_to_f64(s.roll_spread)).collect::<Vec<_>>(),
        "amihuds_lambda" => snapshots.iter().map(|s| decimal_to_f64(s.amihuds_lambda)).collect::<Vec<_>>(),
        "kyles_lambda" => snapshots.iter().map(|s| decimal_to_f64(s.kyles_lambda)).collect::<Vec<_>>(),
        "hasbroucks_lambda" => snapshots.iter().map(|s| decimal_to_f64(s.hasbroucks_lambda)).collect::<Vec<_>>(),
        "vpin" => snapshots.iter().map(|s| decimal_to_f64(s.vpin)).collect::<Vec<_>>(),
    ].context("Failed to create illiquidity DataFrame")?;

    // Mirror your exact file handling logic
    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent).context("Failed to create illiquidity output directory")?;
    }

    // Same compression and writing configuration
    ParquetWriter::new(File::create(filepath).context("Failed to create illiquidity output file")?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)
        .context("Failed to write illiquidity Parquet file")?;

    Ok(())
}

pub fn save_entropy_as_parquet(
    snapshots: &[EntropyMetrics],
    filepath: impl AsRef<Path>
) -> Result<()> {
    let filepath = filepath.as_ref();
    let mut df = df! [
        "timestamp" => snapshots.iter().map(|s| s.timestamp.clone()).collect::<Vec<_>>(),
        "tick_entropy_1s" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_1s)).collect::<Vec<_>>(),
        "tick_entropy_5s" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_5s)).collect::<Vec<_>>(),
        "tick_entropy_10s" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_10s)).collect::<Vec<_>>(),
        "tick_entropy_15s" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_15s)).collect::<Vec<_>>(),
        "tick_entropy_30s" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_30s)).collect::<Vec<_>>(),
        "tick_entropy_1m" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_1m)).collect::<Vec<_>>(),
        "tick_entropy_15m" => snapshots.iter().map(|s| decimal_to_f64(s.tick_entropy_15m)).collect::<Vec<_>>(),
        "volume_tick_entropy_1s" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_1s)).collect::<Vec<_>>(),
        "volume_tick_entropy_5s" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_5s)).collect::<Vec<_>>(),
        "volume_tick_entropy_10s" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_10s)).collect::<Vec<_>>(),
        "volume_tick_entropy_15s" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_15s)).collect::<Vec<_>>(),
        "volume_tick_entropy_30s" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_30s)).collect::<Vec<_>>(),
        "volume_tick_entropy_1m" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_1m)).collect::<Vec<_>>(),
        "volume_tick_entropy_15m" => snapshots.iter().map(|s| decimal_to_f64(s.volume_tick_entropy_15m)).collect::<Vec<_>>(),
    ].context("Failed to create entropy DataFrame")?;

    if let Some(parent) = filepath.parent() {
        fs::create_dir_all(parent).context("Failed to create entropy output directory")?;
    }

    ParquetWriter::new(File::create(filepath).context("Failed to create entropy output file")?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)
        .context("Failed to write entropy Parquet file")?;

    Ok(())
}

/// Unified persistence task for metric batches with file rotation.
pub struct PersistenceEngine<T> {
    rx: Receiver<T>,
    batch_size: usize,
    base_path: PathBuf,
    file_prefix: String,
    max_files: usize,
    save_fn: fn(&[T], &Path) -> Result<()>,
    retained_files: VecDeque<PathBuf>,
    write_jsonl: bool,
}

impl<T> PersistenceEngine<T>
where
    T: Serialize + Send + 'static,
{
    pub fn new(
        rx: Receiver<T>,
        batch_size: usize,
        base_path: impl Into<PathBuf>,
        file_prefix: impl Into<String>,
        max_files: usize,
        save_fn: fn(&[T], &Path) -> Result<()>,
    ) -> Self {
        Self {
            rx,
            batch_size,
            base_path: base_path.into(),
            file_prefix: file_prefix.into(),
            max_files,
            save_fn,
            retained_files: VecDeque::new(),
            write_jsonl: std::env::var("INGESTOR_WRITE_JSONL")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        }
    }

    pub fn run(mut self) -> Result<()> {
        fs::create_dir_all(&self.base_path)
            .with_context(|| format!("Failed to create base directory {:?}", self.base_path))?;

        self.bootstrap_retained_files()?;

        let mut batch = Vec::with_capacity(self.batch_size);

        while let Ok(snapshot) = self.rx.recv() {
            batch.push(snapshot);

            if batch.len() >= self.batch_size {
                self.flush_batch(&mut batch)?;
            }
        }

        if !batch.is_empty() {
            self.flush_batch(&mut batch)?;
        }

        Ok(())
    }

    fn bootstrap_retained_files(&mut self) -> Result<()> {
        let mut files: Vec<PathBuf> = fs::read_dir(&self.base_path)
            .with_context(|| format!("Failed to read directory {:?}", self.base_path))?
            .filter_map(|entry| match entry {
                Ok(entry) => {
                    let path = entry.path();
                    if self.is_managed_file(&path) {
                        Some(path)
                    } else {
                        None
                    }
                }
                Err(err) => {
                    warn!("Failed to inspect file in persistence directory: {err}");
                    None
                }
            })
            .collect();

        files.sort();
        self.retained_files = files.into();

        if self.max_files > 0 {
            while self.retained_files.len() > self.max_files {
                if let Some(path) = self.retained_files.pop_front() {
                    if let Err(err) = fs::remove_file(&path) {
                        warn!("Failed to remove overflow file {:?}: {err}", path);
                    }
                }
            }
        }

        Ok(())
    }

    fn flush_batch(&mut self, batch: &mut Vec<T>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let filename = self.next_file_path();
        (self.save_fn)(batch, &filename).with_context(|| {
            format!(
                "Failed to persist {} batch to {:?}",
                self.file_prefix, filename
            )
        })?;

        if self.write_jsonl {
            let mut jsonl_path = filename.clone();
            jsonl_path.set_extension("jsonl");
            write_jsonl(batch, &jsonl_path).with_context(|| {
                format!(
                    "Failed to write JSONL alongside parquet at {:?}",
                    jsonl_path
                )
            })?;
        }

        self.retained_files.push_back(filename.clone());
        if self.max_files > 0 {
            while self.retained_files.len() > self.max_files {
                if let Some(path) = self.retained_files.pop_front() {
                    if let Err(err) = fs::remove_file(&path) {
                        warn!("Failed to remove overflow file {:?}: {err}", path);
                    }
                }
            }
        }

        batch.clear();
        Ok(())
    }

    fn next_file_path(&self) -> PathBuf {
        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S_%3f");
        self.base_path
            .join(format!("{}_{}.parquet", self.file_prefix, timestamp))
    }

    fn is_managed_file(&self, path: &Path) -> bool {
        path.extension() == Some(OsStr::new("parquet"))
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with(&self.file_prefix))
                .unwrap_or(false)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use crossbeam::channel;
    use std::thread;

    fn create_test_snapshot() -> FeaturesSnapshot {
        FeaturesSnapshot {
            timestamp: Utc::now().to_rfc3339(),
            best_bid: Some(dec!(100.50)),
            best_ask: Some(dec!(101.00)),
            mid_price: Some(dec!(100.75)),
            microprice: Some(dec!(100.60)),
            spread: Some(dec!(0.50)),
            imbalance: Some(dec!(0.33)),
            top_bids: vec![(dec!(100.50), dec!(10.0)), (dec!(100.25), dec!(15.0))],
            top_asks: vec![(dec!(101.00), dec!(8.0)), (dec!(101.25), dec!(12.0))],
            // ... populate all other fields with test values ...
            pwi_1: Some(dec!(100.10)),
            pwi_5: Some(dec!(100.20)),
            pwi_25: Some(dec!(100.30)),
            pwi_50: Some(dec!(100.40)),
            bid_slope: Some(dec!(-0.50)),
            ask_slope: Some(dec!(0.50)),
            volume_imbalance_top5: Some(dec!(0.40)),
            bid_depth_ratio: Some(dec!(0.60)),
            ask_depth_ratio: Some(dec!(0.40)),
            bid_volume_001: Some(dec!(8.0)),
            ask_volume_001: Some(dec!(4.0)),
            bid_avg_distance: Some(dec!(0.25)),
            ask_avg_distance: Some(dec!(0.25)),
            last_trade_price: Some(dec!(100.25)),
            trade_imbalance: Some(dec!(0.60)),
            vwap_total: Some(dec!(100.30)),
            price_change: Some(dec!(0.20)),
            avg_trade_size: Some(dec!(1.50)),
            signed_count_momentum: 5,
            trade_rate_10s: Some(2.5),
            order_flow_imbalance: Some(dec!(0.30)),
            order_flow_pressure: dec!(7.50),
            order_flow_significance: false,
            vwap_10: Some(dec!(100.35)),
            vwap_50: Some(dec!(100.32)),
            vwap_100: Some(dec!(100.31)),
            vwap_1000: Some(dec!(100.25)),
            aggr_ratio_10: Some(dec!(0.60)),
            aggr_ratio_50: Some(dec!(0.55)),
            aggr_ratio_100: Some(dec!(0.52)),
            aggr_ratio_1000: Some(dec!(0.50)),
        }
    }

    #[test]
    fn test_save_single_feature() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");
        
        let features = vec![create_test_snapshot()];
        save_feature_as_parquet(&features, &path)?;

        assert!(path.exists());
        assert!(path.metadata()?.len() > 0);
        Ok(())
    }

    #[test]
    fn test_save_multiple_features() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("multi.parquet");
        
        let features = vec![
            create_test_snapshot(),
            create_test_snapshot(),
            create_test_snapshot()
        ];
        save_feature_as_parquet(&features, &path)?;

        // Verify we can read back the parquet
        let file = fs::File::open(path)?;
        let df = ParquetReader::new(file).finish()?;
        assert_eq!(df.height(), 3);
        Ok(())
    }

    #[test]
    fn test_empty_features() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("empty.parquet");
        
        save_feature_as_parquet(&[], &path)?;
        
        // Empty parquet files are still valid
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn test_creates_parent_dirs() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("newdir/test.parquet");
        
        save_feature_as_parquet(&[create_test_snapshot()], &path)?;
        
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn test_invalid_path_handling() {
        let result =
            save_feature_as_parquet(&[create_test_snapshot()], "/invalid/path/test.parquet");
        assert!(result.is_err());
    }

    #[test]
    fn test_serialization_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("roundtrip.parquet");
        
        let original = create_test_snapshot();
        save_feature_as_parquet(&[original.clone()], &path)?;

        // Read back and verify values - UPDATED FOR POLARS COMPATIBILITY:
        let file = fs::File::open(path)?;
        let df = ParquetReader::new(file).finish()?;
        
        // Correct way to access f64 values in Polars
        let col = df.column("best_bid")?.f64()?;
        if let Some(val) = col.get(0) {
            assert!((val - 100.5).abs() < f64::EPSILON);
        } else {
            panic!("No value found in column");
        }
        
        Ok(())
    }

    #[test]
    fn test_complex_field_serialization() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("complex.parquet");
        
        let features = vec![create_test_snapshot()];
        save_feature_as_parquet(&features, &path)?;

        // Verify top_bids JSON serialization
        let file = fs::File::open(path)?;
        let df = ParquetReader::new(file).finish()?;
        let json_str = df.column("top_bids")?.utf8()?.get(0).unwrap();
        assert!(json_str.contains("100.50"));
        assert!(json_str.contains("10.0"));
        
        Ok(())
    }

    #[test]
    fn test_persistence_rotation() -> Result<()> {
        let dir = tempdir()?;
        let (tx, rx) = channel::bounded(4);

        let engine = PersistenceEngine::new(
            rx,
            1,
            dir.path().to_path_buf(),
            "features",
            2,
            save_feature_as_parquet_path,
        );

        let handle = thread::spawn(move || engine.run().unwrap());

        tx.send(create_test_snapshot())?;
        tx.send(create_test_snapshot())?;
        tx.send(create_test_snapshot())?;

        drop(tx);
        handle.join().expect("engine thread panicked");

        let parquet_files: Vec<_> = fs::read_dir(dir.path())?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension() == Some(OsStr::new("parquet")))
            .collect();

        assert_eq!(parquet_files.len(), 2);

        Ok(())
    }
}
