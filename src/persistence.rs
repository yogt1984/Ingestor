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