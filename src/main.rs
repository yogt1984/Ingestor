use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs::File;
use std::io::Write;

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms";
const QUEUE_SIZE: usize = 1024;

#[derive(Debug, Deserialize)]
struct DepthUpdate {
    e: String, // Event type
    E: u64,    // Event time
    s: String, // Symbol
    U: u64,    // First update ID
    u: u64,    // Final update ID
    b: Vec<[String; 2]>, // Bids (price, quantity)
    a: Vec<[String; 2]>, // Asks (price, quantity)
}

struct FeatureExtractor {
    mid_price_window: VecDeque<f64>,
    volume_imbalance_window: VecDeque<f64>,
    spread_window: VecDeque<f64>,
    lookback: usize,
}

impl FeatureExtractor {
    fn new(lookback: usize) -> Self {
        Self {
            mid_price_window: VecDeque::with_capacity(lookback),
            volume_imbalance_window: VecDeque::with_capacity(lookback),
            spread_window: VecDeque::with_capacity(lookback),
            lookback,
        }
    }

    fn volume_imbalance(&self, total_bid_volume: f64, total_ask_volume: f64) -> f64 {
        if total_ask_volume > 0.0 {
            total_bid_volume / total_ask_volume
        } else {
            0.0
        }
    }

    fn price_weighted_volume_imbalance(&self, bids: &[[String; 2]], asks: &[[String; 2]]) -> f64 {
        let bid_weighted: f64 = bids.iter().map(|b| b[0].parse::<f64>().unwrap_or(0.0) * b[1].parse::<f64>().unwrap_or(0.0)).sum();
        let ask_weighted: f64 = asks.iter().map(|a| a[0].parse::<f64>().unwrap_or(0.0) * a[1].parse::<f64>().unwrap_or(0.0)).sum();
        bid_weighted - ask_weighted
    }

    fn order_book_slope(&self, bids: &[[String; 2]], asks: &[[String; 2]]) -> f64 {
        let bid_prices: Vec<f64> = bids.iter().map(|b| b[0].parse::<f64>().unwrap_or(0.0)).collect();
        let ask_prices: Vec<f64> = asks.iter().map(|a| a[0].parse::<f64>().unwrap_or(0.0)).collect();
        let bid_slope = if bid_prices.len() > 1 {
            (bid_prices[0] - bid_prices[1]) / 1.0
        } else {
            0.0
        };
        let ask_slope = if ask_prices.len() > 1 {
            (ask_prices[1] - ask_prices[0]) / 1.0
        } else {
            0.0
        };
        (bid_slope + ask_slope) / 2.0
    }

    fn weighted_mid_price(&self, best_bid: f64, best_ask: f64, best_bid_volume: f64, best_ask_volume: f64) -> f64 {
        let total_volume = best_bid_volume + best_ask_volume;
        if total_volume > 0.0 {
            (best_bid * best_ask_volume + best_ask * best_bid_volume) / total_volume
        } else {
            (best_bid + best_ask) / 2.0
        }
    }

    fn depth_at_top_n(&self, levels: &[[String; 2]], n: usize) -> f64 {
        levels.iter().take(n).map(|l| l[1].parse::<f64>().unwrap_or(0.0)).sum()
    }

    fn mid_price_change(&mut self, mid_price: f64) -> f64 {
        if let Some(prev_mid_price) = self.mid_price_window.back() {
            mid_price - prev_mid_price
        } else {
            0.0
        }
    }

    fn rolling_mean(&self, window: &VecDeque<f64>) -> f64 {
        let sum: f64 = window.iter().sum();
        sum / window.len() as f64
    }

    fn rolling_std(&self, window: &VecDeque<f64>) -> f64 {
        let mean = self.rolling_mean(window);
        let variance: f64 = window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / window.len() as f64;
        variance.sqrt()
    }

    fn autocorrelation(&self, window: &VecDeque<f64>, lag: usize) -> f64 {
        if window.len() > lag {
            let mean = self.rolling_mean(window);
            let numerator: f64 = window.iter().zip(window.iter().skip(lag)).map(|(x, y)| (x - mean) * (y - mean)).sum();
            let denominator: f64 = window.iter().map(|x| (x - mean).powi(2)).sum();
            if denominator > 0.0 {
                numerator / denominator
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
}

fn process_message(message: &str, queue: &mut VecDeque<String>, feature_extractor: &mut FeatureExtractor) {
    if let Ok(depth_update) = serde_json::from_str::<DepthUpdate>(message) {
        // Extract best bid and ask prices and volumes
        let best_bid = depth_update.b.first().and_then(|b| b[0].parse::<f64>().ok()).unwrap_or(0.0);
        let best_ask = depth_update.a.first().and_then(|a| a[0].parse::<f64>().ok()).unwrap_or(0.0);
        let best_bid_volume = depth_update.b.first().and_then(|b| b[1].parse::<f64>().ok()).unwrap_or(0.0);
        let best_ask_volume = depth_update.a.first().and_then(|a| a[1].parse::<f64>().ok()).unwrap_or(0.0);

        // Compute total bid and ask volumes
        let total_bid_volume: f64 = depth_update.b.iter().map(|b| b[1].parse::<f64>().unwrap_or(0.0)).sum();
        let total_ask_volume: f64 = depth_update.a.iter().map(|a| a[1].parse::<f64>().unwrap_or(0.0)).sum();

        // Compute features
        let volume_imbalance = feature_extractor.volume_imbalance(total_bid_volume, total_ask_volume);
        let price_weighted_volume_imbalance = feature_extractor.price_weighted_volume_imbalance(&depth_update.b, &depth_update.a);
        let order_book_slope = feature_extractor.order_book_slope(&depth_update.b, &depth_update.a);
        let weighted_mid_price = feature_extractor.weighted_mid_price(best_bid, best_ask, best_bid_volume, best_ask_volume);
        let depth_at_top_5 = feature_extractor.depth_at_top_n(&depth_update.b, 5) + feature_extractor.depth_at_top_n(&depth_update.a, 5);
        let mid_price = (best_bid + best_ask) / 2.0;
        let mid_price_change = feature_extractor.mid_price_change(mid_price);

        // Update rolling windows
        feature_extractor.mid_price_window.push_back(mid_price);
        feature_extractor.volume_imbalance_window.push_back(volume_imbalance);
        feature_extractor.spread_window.push_back(best_ask - best_bid);

        // Compute rolling statistics
        let rolling_mean_mid_price = feature_extractor.rolling_mean(&feature_extractor.mid_price_window);
        let rolling_std_mid_price = feature_extractor.rolling_std(&feature_extractor.mid_price_window);
        let autocorrelation_mid_price = feature_extractor.autocorrelation(&feature_extractor.mid_price_window, 1);

        // Augment the original message with the extracted features
        let mut augmented_message: Value = serde_json::from_str(message).unwrap();
        augmented_message["features"] = json!({
            "timestamp": depth_update.E,
            "mid_price": mid_price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": best_ask - best_bid,
            "total_bid_volume": total_bid_volume,
            "total_ask_volume": total_ask_volume,
            "volume_imbalance": volume_imbalance,
            "price_weighted_volume_imbalance": price_weighted_volume_imbalance,
            "order_book_slope": order_book_slope,
            "weighted_mid_price": weighted_mid_price,
            "depth_at_top_5": depth_at_top_5,
            "mid_price_change": mid_price_change,
            "rolling_mean_mid_price": rolling_mean_mid_price,
            "rolling_std_mid_price": rolling_std_mid_price,
            "autocorrelation_mid_price": autocorrelation_mid_price,
        });

        // Convert the augmented message back to a string
        if let Ok(augmented_message_str) = serde_json::to_string(&augmented_message) {
            // Add the augmented message to the queue
            if queue.len() >= QUEUE_SIZE {
                queue.pop_front(); // Remove the oldest message
            }
            queue.push_back(augmented_message_str);
        }
    }
}

fn file_writer(queue: Arc<Mutex<VecDeque<String>>>) {
    loop {
        let mut queue = queue.lock().unwrap();
        if queue.len() >= QUEUE_SIZE {
            let start_time = Instant::now();
            let json_array: Vec<_> = queue.iter().map(|msg| serde_json::from_str::<Value>(msg).unwrap()).collect();
            let filename = format!("ingested_{}.json", start_time.elapsed().as_millis());
            if let Ok(mut file) = File::create(&filename) {
                if let Ok(json_string) = serde_json::to_string_pretty(&json_array) {
                    if file.write_all(json_string.as_bytes()).is_ok() {
                        println!("Written {} messages to {}", queue.len(), filename);
                    } else {
                        eprintln!("Failed to write to file: {}", filename);
                    }
                } else {
                    eprintln!("Failed to serialize JSON");
                }
            } else {
                eprintln!("Failed to create file: {}", filename);
            }
            queue.clear();
            let write_time = start_time.elapsed();
            println!("Time taken to write file: {} ms", write_time.as_millis());
        }
        drop(queue);
        thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::main]
async fn main() {
    let queue = Arc::new(Mutex::new(VecDeque::<String>::with_capacity(QUEUE_SIZE)));
    let queue_ws = Arc::clone(&queue);
    let feature_extractor = Arc::new(Mutex::new(FeatureExtractor::new(100)));

    let queue_file = Arc::clone(&queue);
    thread::spawn(move || {
        file_writer(queue_file);
    });

    if let Err(e) = connect_to_websocket(queue_ws, feature_extractor).await {
        eprintln!("WebSocket error: {}", e);
    }
}

async fn connect_to_websocket(queue: Arc<Mutex<VecDeque<String>>>, feature_extractor: Arc<Mutex<FeatureExtractor>>) -> Result<(), Box<dyn std::error::Error>> {
    let (ws_stream, _) = connect_async(BINANCE_WS_URL).await?;
    println!("Connected to WebSocket");

    let (_, mut read) = ws_stream.split();

    while let Some(Ok(message)) = read.next().await {
        if let Message::Text(text) = message {
            let mut queue = queue.lock().unwrap();
            let mut feature_extractor = feature_extractor.lock().unwrap();
            process_message(&text, &mut queue, &mut feature_extractor);
            println!("Received message, queue size: {}", queue.len());
        }
    }

    Ok(())
}