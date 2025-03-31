use std::sync::Arc;
use tokio::{task::JoinHandle, time::{interval, Duration}};
use rust_decimal_macros::dec;

use crate::{orderbook::ConcurrentOrderBook, tradeslog::ConcurrentTradesLog};

pub fn spawn_analytics_task(
    order_book_clone: ConcurrentOrderBook,
    trades_log_clone: ConcurrentTradesLog,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            // --- Order Book Analytics ---
            let best_bid = order_book_clone.best_bid().await;
            let best_ask = order_book_clone.best_ask().await;
            let mid_price = order_book_clone.mid_price().await;
            let spread = order_book_clone.spread().await;
            let imbalance = order_book_clone.order_book_imbalance().await;
            let top_bids = order_book_clone.top_bids(5).await;
            let top_asks = order_book_clone.top_asks(5).await;

            print!("\x1B[2J\x1B[1;1H");
            println!("======================================================");
            println!("--- Order Book Features ---");
            println!("Best Bid                       : {:?}", best_bid);
            println!("Best Ask                       : {:?}", best_ask);
            println!("Mid Price                      : {:?}", mid_price);
            println!("Spread                         : {:?}", spread);
            println!("Imbalance                      : {:?}", imbalance);
            println!("Top 5 Bids                     : {:?}", top_bids);
            println!("Top 5 Asks                     : {:?}", top_asks);

            if let Some(pwi_1) = order_book_clone.price_weighted_imbalance_percent(dec!(1)).await {
                println!("Price-Weighted Imbalance ±1%   : {:.4}", pwi_1);
            }
            if let Some(pwi_5) = order_book_clone.price_weighted_imbalance_percent(dec!(5)).await {
                println!("Price-Weighted Imbalance ±5%   : {:.4}", pwi_5);
            }
            if let Some(pwi_25) = order_book_clone.price_weighted_imbalance_percent(dec!(25)).await {
                println!("Price-Weighted Imbalance ±25%  : {:.4}", pwi_25);
            }
            if let Some(pwi_50) = order_book_clone.price_weighted_imbalance_percent(dec!(50)).await {
                println!("Price-Weighted Imbalance ±50%  : {:.4}", pwi_50);
            }

            if let Some((bid_slope, ask_slope)) = order_book_clone.slope(5).await {
                println!("Bid Slope                      : {:?}", bid_slope);
                println!("Ask Slope                      : {:?}", ask_slope);
            }

            if let Some(vol_imb) = order_book_clone.volume_imbalance().await {
                println!("Volume Imbalance (Top 5)       : {:?}", vol_imb);
            }

            if let Some((bid_ratio, ask_ratio)) = order_book_clone.depth_ratio().await {
                println!("Depth Ratio (Top 3/10)         : Bids={:.4}, Asks={:.4}", bid_ratio, ask_ratio);
            }

            if let Some((bid_vol, ask_vol)) = order_book_clone.volume_within_percent_range(dec!(0.01)).await {
                println!("Volume ±0.01% Mid              : Bids={:.4}, Asks={:.4}", bid_vol, ask_vol);
            }

            if let Some((bid_dist, ask_dist)) = order_book_clone.avg_price_distance(5).await {
                println!("Avg Distance (Top 5)           : Bids={:.4}, Asks={:.4}", bid_dist, ask_dist);
            }

            // --- Trade Log Analytics ---
            println!("--- Trade Log Features ---");

            if let Some(last_price) = trades_log_clone.last_price().await {
                println!("Last Trade Price               : {:?}", last_price);
            }

            if let Ok(vwap_50) = trades_log_clone.vwap(50).await {
                println!("VWAP (last 50 trades)          : {:.4}", vwap_50);
            }

            if let Ok(aggr_ratio) = trades_log_clone.aggressor_volume_ratio(50).await {
                println!("Aggressor Volume Ratio (50)    : {:.4}", aggr_ratio);
            }

            if let Some(trade_imbalance) = trades_log_clone.trade_imbalance().await {
                println!("Trade Imbalance                : {:.4}", trade_imbalance);
            }

            if let Some(vwap_total) = trades_log_clone.vwap_total().await {
                println!("VWAP Total (cached)            : {:.4}", vwap_total);
            }

            if let Some(price_change) = trades_log_clone.price_change().await {
                println!("Price Change (cached)          : {:.4}", price_change);
            }

            if let Some(avg_size) = trades_log_clone.avg_trade_size().await {
                println!("Average Trade Size             : {:.4}", avg_size);
            }

            println!("Signed Count Momentum          : {}", trades_log_clone.signed_count_momentum().await);

            if let Ok(rate) = trades_log_clone.trade_rate(10_000).await {
                println!("Trade Rate (per sec, 10s)      : {:.2}", rate);
            }

            println!("======================================================\n");
        }
    })
}
