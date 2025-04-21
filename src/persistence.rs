use anyhow::{Context, Result};
use polars::prelude::*;
use serde_json;
use crate::analytics::FeaturesSnapshot;
use rust_decimal::prelude::ToPrimitive;

/// Save a batch of features to Parquet with comprehensive error handling
pub fn save_feature_as_parquet(features: &[FeaturesSnapshot], filepath: &str) -> Result<()> {
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
    if let Some(parent) = std::path::Path::new(filepath).parent() {
        std::fs::create_dir_all(parent).context("Failed to create output directory")?;
    }

    // Write with compression and proper error handling
    ParquetWriter::new(std::fs::File::create(filepath).context("Failed to create output file")?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)
        .context("Failed to write Parquet file")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;
    use chrono::Utc;
    use rust_decimal_macros::dec;

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
        save_feature_as_parquet(&features, path.to_str().unwrap())?;

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
        save_feature_as_parquet(&features, path.to_str().unwrap())?;

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
        
        save_feature_as_parquet(&[], path.to_str().unwrap())?;
        
        // Empty parquet files are still valid
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn test_creates_parent_dirs() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("newdir/test.parquet");
        
        save_feature_as_parquet(&[create_test_snapshot()], path.to_str().unwrap())?;
        
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn test_invalid_path_handling() {
        let result = save_feature_as_parquet(
            &[create_test_snapshot()], 
            "/invalid/path/test.parquet"
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_serialization_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("roundtrip.parquet");
        
        let original = create_test_snapshot();
        save_feature_as_parquet(&[original.clone()], path.to_str().unwrap())?;

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
        save_feature_as_parquet(&features, path.to_str().unwrap())?;

        // Verify top_bids JSON serialization
        let file = fs::File::open(path)?;
        let df = ParquetReader::new(file).finish()?;
        let json_str = df.column("top_bids")?.utf8()?.get(0).unwrap();
        assert!(json_str.contains("100.50"));
        assert!(json_str.contains("10.0"));
        
        Ok(())
    }
}
