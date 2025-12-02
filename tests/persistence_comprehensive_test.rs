// Comprehensive tests for persistence module, especially parquet file content
use ingestor::persistence::{save_feature_as_parquet, validate_parquet_schema};
use ingestor::feature_fusion::FeaturesSnapshot;
use rust_decimal_macros::dec;
use num::FromPrimitive;
use chrono::Utc;
use std::fs;
use tempfile::tempdir;
use polars::prelude::*;

fn create_complete_snapshot() -> FeaturesSnapshot {
    FeaturesSnapshot {
        timestamp: Utc::now().to_rfc3339(),
        // Orderbook features
        best_bid: Some(dec!(100.50)),
        best_ask: Some(dec!(101.00)),
        mid_price: Some(dec!(100.75)),
        microprice: Some(dec!(100.60)),
        spread: Some(dec!(0.50)),
        imbalance: Some(dec!(0.33)),
        top_bids: vec![(dec!(100.50), dec!(10.0)), (dec!(100.25), dec!(15.0))],
        top_asks: vec![(dec!(101.00), dec!(8.0)), (dec!(101.25), dec!(12.0))],
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
        // Tradeslog features
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
        // Illiquidity metrics
        roll_spread: Some(dec!(0.0001)),
        amihuds_lambda: Some(dec!(0.00005)),
        kyles_lambda: Some(dec!(0.5)),
        hasbroucks_lambda: Some(dec!(0.3)),
        vpin: Some(dec!(0.25)),
        // Entropy metrics - tick entropy (CRITICAL)
        tick_entropy_1s: Some(dec!(1.2)),
        tick_entropy_5s: Some(dec!(1.5)),
        tick_entropy_10s: Some(dec!(1.8)),
        tick_entropy_15s: Some(dec!(2.0)),
        tick_entropy_30s: Some(dec!(2.2)),
        tick_entropy_1m: Some(dec!(2.5)),
        tick_entropy_15m: Some(dec!(3.0)),
        // Entropy metrics - volume tick entropy (CRITICAL)
        volume_tick_entropy_1s: Some(dec!(1.1)),
        volume_tick_entropy_5s: Some(dec!(1.4)),
        volume_tick_entropy_10s: Some(dec!(1.7)),
        volume_tick_entropy_15s: Some(dec!(1.9)),
        volume_tick_entropy_30s: Some(dec!(2.1)),
        volume_tick_entropy_1m: Some(dec!(2.4)),
        volume_tick_entropy_15m: Some(dec!(2.9)),
        // Complex vector fields
        volume_vector: vec![(dec!(0.01), (dec!(100.0), dec!(50.0)))],
        pwi_vector: vec![(dec!(0.01), dec!(100.5))],
        // Volatility metrics
        realized_volatility_100: Some(0.001),
        realized_volatility_1000: Some(0.0008),
        bipower_variation_100: Some(0.0009),
        jump_indicator: Some(1.5),
        vol_of_vol: Some(0.0002),
        // Toxicity metrics
        toxic_flow_ratio_micro: Some(dec!(0.25)),
        toxic_flow_ratio_mid: Some(dec!(0.22)),
        adverse_selection_micro: Some(dec!(0.001)),
        adverse_selection_mid: Some(dec!(0.0008)),
        arrival_asymmetry: Some(dec!(0.15)),
        size_toxicity_ratio: Some(dec!(1.2)),
        toxicity_index: Some(dec!(0.28)),
    }
}

#[test]
fn test_all_fields_persisted_single() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("complete.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    // Validate schema
    let missing = validate_parquet_schema(&path).unwrap();
    assert!(missing.is_empty(), "Missing columns: {:?}", missing);
    
    // Read back and verify all columns exist
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    // Verify all entropy columns exist and are readable
    let entropy_cols = vec![
        "tick_entropy_1s", "tick_entropy_5s", "tick_entropy_10s", "tick_entropy_15s",
        "tick_entropy_30s", "tick_entropy_1m", "tick_entropy_15m",
        "volume_tick_entropy_1s", "volume_tick_entropy_5s", "volume_tick_entropy_10s",
        "volume_tick_entropy_15s", "volume_tick_entropy_30s", "volume_tick_entropy_1m",
        "volume_tick_entropy_15m",
    ];
    
    for col in &entropy_cols {
        assert!(df.column(col).is_ok(), "Missing entropy column: {}", col);
        let series = df.column(col).unwrap();
        assert_eq!(series.len(), 1, "Column {} should have 1 row", col);
    }
}

#[test]
fn test_entropy_values_persisted_correctly() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("entropy_values.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    // Verify entropy values match
    let te1s = df.column("tick_entropy_1s").unwrap().f64().unwrap().get(0).unwrap();
    assert!((te1s - 1.2).abs() < 0.001, "tick_entropy_1s should be 1.2");
    
    let ve1s = df.column("volume_tick_entropy_1s").unwrap().f64().unwrap().get(0).unwrap();
    assert!((ve1s - 1.1).abs() < 0.001, "volume_tick_entropy_1s should be 1.1");
    
    let te15m = df.column("tick_entropy_15m").unwrap().f64().unwrap().get(0).unwrap();
    assert!((te15m - 3.0).abs() < 0.001, "tick_entropy_15m should be 3.0");
}

#[test]
fn test_all_illiquidity_fields_persisted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("illiquidity.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    let illiquidity_cols = vec![
        "roll_spread", "amihuds_lambda", "kyles_lambda", "hasbroucks_lambda", "vpin",
    ];
    
    for col in &illiquidity_cols {
        assert!(df.column(col).is_ok(), "Missing illiquidity column: {}", col);
    }
    
    // Verify values
    let vpin = df.column("vpin").unwrap().f64().unwrap().get(0).unwrap();
    assert!((vpin - 0.25).abs() < 0.001);
}

#[test]
fn test_complex_vectors_persisted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("vectors.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    // Verify volume_vector and pwi_vector are present and contain JSON
    assert!(df.column("volume_vector").is_ok());
    assert!(df.column("pwi_vector").is_ok());
    
    let vol_vec_str = df.column("volume_vector").unwrap().utf8().unwrap().get(0).unwrap();
    assert!(vol_vec_str.contains("100.0"), "volume_vector should contain price data");
    
    let pwi_vec_str = df.column("pwi_vector").unwrap().utf8().unwrap().get(0).unwrap();
    assert!(pwi_vec_str.contains("100.5"), "pwi_vector should contain price data");
}

#[test]
fn test_multiple_snapshots_all_fields() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("multiple.parquet");
    
    let snapshots = vec![
        create_complete_snapshot(),
        create_complete_snapshot(),
        create_complete_snapshot(),
    ];
    
    save_feature_as_parquet(&snapshots, &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    assert_eq!(df.height(), 3);
    
    // Verify all entropy columns have 3 rows
    let te1s = df.column("tick_entropy_1s").unwrap().f64().unwrap();
    assert_eq!(te1s.len(), 3);
    
    // Verify no nulls in entropy columns
    assert_eq!(te1s.null_count(), 0);
}

#[test]
fn test_null_entropy_handling() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("null_entropy.parquet");
    
    let mut snapshot = create_complete_snapshot();
    // Set some entropy fields to None
    snapshot.tick_entropy_1s = None;
    snapshot.volume_tick_entropy_5s = None;
    
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    // Verify columns still exist even with None values
    assert!(df.column("tick_entropy_1s").is_ok());
    assert!(df.column("volume_tick_entropy_5s").is_ok());
    
    // Verify nulls are handled
    let te1s = df.column("tick_entropy_1s").unwrap().f64().unwrap();
    assert_eq!(te1s.null_count(), 1);
}

#[test]
fn test_all_orderbook_features_persisted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("orderbook.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    let orderbook_cols = vec![
        "best_bid", "best_ask", "mid_price", "microprice", "spread", "imbalance",
        "pwi_1", "pwi_5", "pwi_25", "pwi_50",
        "bid_slope", "ask_slope", "volume_imbalance_top5",
        "bid_depth_ratio", "ask_depth_ratio",
        "bid_volume_001", "ask_volume_001",
        "bid_avg_distance", "ask_avg_distance",
    ];
    
    for col in &orderbook_cols {
        assert!(df.column(col).is_ok(), "Missing orderbook column: {}", col);
    }
}

#[test]
fn test_all_tradeslog_features_persisted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("tradeslog.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    let tradeslog_cols = vec![
        "last_trade_price", "trade_imbalance", "vwap_total", "price_change", "avg_trade_size",
        "signed_count_momentum", "trade_rate_10s",
        "order_flow_imbalance", "order_flow_pressure", "order_flow_significance",
        "vwap_10", "vwap_50", "vwap_100", "vwap_1000",
        "aggr_ratio_10", "aggr_ratio_50", "aggr_ratio_100", "aggr_ratio_1000",
    ];
    
    for col in &tradeslog_cols {
        assert!(df.column(col).is_ok(), "Missing tradeslog column: {}", col);
    }
}

#[test]
fn test_timestamp_persisted() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("timestamp.parquet");
    
    let snapshot = create_complete_snapshot();
    let expected_ts = snapshot.timestamp.clone();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    let ts_col = df.column("timestamp").unwrap().utf8().unwrap().get(0).unwrap();
    assert_eq!(ts_col, &expected_ts);
}

#[test]
fn test_top_bids_asks_json_serialization() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("top_levels.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    let top_bids = df.column("top_bids").unwrap().utf8().unwrap().get(0).unwrap();
    assert!(top_bids.contains("100.50"), "top_bids should contain price");
    assert!(top_bids.contains("10.0"), "top_bids should contain volume");
    
    let top_asks = df.column("top_asks").unwrap().utf8().unwrap().get(0).unwrap();
    assert!(top_asks.contains("101.00"), "top_asks should contain price");
}

#[test]
fn test_large_batch_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large_batch.parquet");
    
    let mut snapshots = Vec::new();
    for i in 0..1000 {
        let mut snapshot = create_complete_snapshot();
        // Vary entropy values
        snapshot.tick_entropy_1s = Some(rust_decimal::Decimal::from_f64(1.0 + i as f64 * 0.001).unwrap_or(dec!(0)));
        snapshots.push(snapshot);
    }
    
    save_feature_as_parquet(&snapshots, &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    assert_eq!(df.height(), 1000);
    
    // Verify all entropy columns have correct row count
    let te1s = df.column("tick_entropy_1s").unwrap().f64().unwrap();
    assert_eq!(te1s.len(), 1000);
    assert_eq!(te1s.null_count(), 0);
}

#[test]
fn test_parquet_file_readable_by_polars() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("polars_readable.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    // Try to read with polars lazy API
    let df = polars::prelude::LazyFrame::scan_parquet(&path, ScanArgsParquet::default())
        .unwrap()
        .collect()
        .unwrap();
    
    assert_eq!(df.height(), 1);
    assert!(df.column("tick_entropy_1s").is_ok());
}

#[test]
fn test_parquet_compression() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("compressed.parquet");
    
    let mut snapshots = Vec::new();
    for _ in 0..100 {
        snapshots.push(create_complete_snapshot());
    }
    
    save_feature_as_parquet(&snapshots, &path).unwrap();
    
    // File should exist and be reasonably sized
    let metadata = fs::metadata(&path).unwrap();
    assert!(metadata.len() > 0);
    // Compressed parquet should be smaller than uncompressed
    assert!(metadata.len() < 10_000_000); // Sanity check
}

#[test]
fn test_all_entropy_windows_present() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("entropy_windows.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    // Verify all 7 tick entropy windows
    for window in &["1s", "5s", "10s", "15s", "30s", "1m", "15m"] {
        let col = format!("tick_entropy_{}", window);
        assert!(df.column(&col).is_ok(), "Missing tick_entropy_{}", window);
    }
    
    // Verify all 7 volume tick entropy windows
    for window in &["1s", "5s", "10s", "15s", "30s", "1m", "15m"] {
        let col = format!("volume_tick_entropy_{}", window);
        assert!(df.column(&col).is_ok(), "Missing volume_tick_entropy_{}", window);
    }
}

#[test]
fn test_entropy_data_types() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("entropy_types.parquet");
    
    let snapshot = create_complete_snapshot();
    save_feature_as_parquet(&[snapshot], &path).unwrap();
    
    let df = polars::prelude::ParquetReader::new(
        std::fs::File::open(&path).unwrap()
    ).finish().unwrap();
    
    // Verify entropy columns are f64 (nullable)
    let te1s = df.column("tick_entropy_1s").unwrap();
    assert!(matches!(te1s.dtype(), DataType::Float64));
    
    let ve1s = df.column("volume_tick_entropy_1s").unwrap();
    assert!(matches!(ve1s.dtype(), DataType::Float64));
}

