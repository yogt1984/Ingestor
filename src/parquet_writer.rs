use polars::prelude::*;
use std::fs::create_dir_all;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn write_to_parquet(trades_log: &ConcurrentTradesLog) -> anyhow::Result<()> {
    let recent_trades = trades_log.get_last_n(500); // adjust logic to filter last 5 minutes

    let timestamps: Vec<i64> = recent_trades.iter().map(|t| t.timestamp).collect();
    let prices: Vec<f64> = recent_trades.iter().map(|t| t.price.to_f64().unwrap()).collect();
    let sizes: Vec<f64> = recent_trades.iter().map(|t| t.qty.to_f64().unwrap()).collect();

    let df = df![
        "timestamp" => timestamps,
        "price" => prices,
        "size" => sizes,
    ]?;

    create_dir_all("/data")?;

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let file_path = format!("/data/trades_{}.parquet", now);

    let file = std::fs::File::create(&file_path)?;
    ParquetWriter::new(file).finish(&df)?;

    Ok(())
}

