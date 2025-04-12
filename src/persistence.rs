use anyhow::Result;
use polars::prelude::*;
use crate::analytics::FeaturesSnapshot;

use rust_decimal::prelude::ToPrimitive;

/// Save a batch of features directly to Parquet using Polars
pub fn save_feature_as_parquet(features: &[FeaturesSnapshot], filepath: &str) -> Result<()> {
    let mut df = df![
        "timestamp" => features.iter().map(|f| f.timestamp.clone()).collect::<Vec<_>>(),

        "best_bid" => features.iter().map(|f| f.best_bid.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "best_ask" => features.iter().map(|f| f.best_ask.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "mid_price" => features.iter().map(|f| f.mid_price.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "spread" => features.iter().map(|f| f.spread.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "imbalance" => features.iter().map(|f| f.imbalance.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        // Top bids and asks: skip for now because Vec<(Decimal, Decimal)> not directly supported
        // You can flatten later or serialize separately if you want to store top_bids/top_asks.

        "pwi_1" => features.iter().map(|f| f.pwi_1.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "pwi_5" => features.iter().map(|f| f.pwi_5.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "pwi_25" => features.iter().map(|f| f.pwi_25.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "pwi_50" => features.iter().map(|f| f.pwi_50.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "bid_slope" => features.iter().map(|f| f.bid_slope.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "ask_slope" => features.iter().map(|f| f.ask_slope.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "volume_imbalance_top5" => features.iter().map(|f| f.volume_imbalance_top5.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "bid_depth_ratio" => features.iter().map(|f| f.bid_depth_ratio.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "ask_depth_ratio" => features.iter().map(|f| f.ask_depth_ratio.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "bid_volume_001" => features.iter().map(|f| f.bid_volume_001.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "ask_volume_001" => features.iter().map(|f| f.ask_volume_001.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "bid_avg_distance" => features.iter().map(|f| f.bid_avg_distance.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "ask_avg_distance" => features.iter().map(|f| f.ask_avg_distance.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "last_trade_price" => features.iter().map(|f| f.last_trade_price.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "vwap_50" => features.iter().map(|f| f.vwap_50.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "aggr_ratio_50" => features.iter().map(|f| f.aggr_ratio_50.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "trade_imbalance" => features.iter().map(|f| f.trade_imbalance.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "vwap_total" => features.iter().map(|f| f.vwap_total.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "price_change" => features.iter().map(|f| f.price_change.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),
        "avg_trade_size" => features.iter().map(|f| f.avg_trade_size.map(|d| d.to_f64().unwrap_or(f64::NAN))).collect::<Vec<_>>(),

        "signed_count_momentum" => features.iter().map(|f| Some(f.signed_count_momentum)).collect::<Vec<_>>(),
        "trade_rate_10s" => features.iter().map(|f| f.trade_rate_10s.unwrap_or(f64::NAN)).collect::<Vec<_>>(),
    ]?;

    ParquetWriter::new(std::fs::File::create(filepath)?)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut df)?;

    Ok(())
}
