# Binance WebSocket Market Data Ingestor

## Project Overview
Real-time data pipeline that connects to Binance WebSocket API (BTC/USDT) to extract and compute market microstructure features for algorithmic trading strategies.

## Features

### Order Book Features (orderbook.rs)
| Feature                 | Description |
|-------------------------|-------------|
| Best Bid/Ask           | Top-of-book prices and quantities |
| Mid Price              | (best_bid + best_ask) / 2 |
| Microprice             | Volume-weighted price between best bid/ask |
| Spread                 | Absolute difference between best bid and ask |
| Imbalance              | (bid_vol - ask_vol) / (bid_vol + ask_vol) |
| Top Bids/Asks          | Full depth snapshots (price, volume) |
| PWI (1%, 5%, 25%, 50%)| Price-weighted imbalance at different depth levels |
| Bid/Ask Slope          | Slope of order book curve (liquidity distribution) |
| Volume Imbalance Top 5 | Volume ratio of top 5 bid/ask levels |
| Bid/Ask Depth Ratio    | Ratio of top 3 vs top 10 level volumes |
| Bid/Ask Volume 0.01%   | Volume within 0.01% of mid price |
| Bid/Ask Avg Distance   | Average price distance from mid price |
| Order Flow Imbalance   | Real-time pressure from order placements/cancellations |
| Order Flow Pressure    | Cumulative order flow impact |
| Order Flow Significance| Boolean flag for significant pressure events |

### Trade Features (tradeslog.rs)
| Feature                 | Description |
|-------------------------|-------------|
| Last Trade Price        | Most recent trade price |
| Trade Imbalance         | Buy vs sell trade volume ratio |
| VWAP (Total, 10, 50, 100, 1000) | Volume-weighted average price over different windows |
| Price Change            | Difference from previous trade |
| Avg Trade Size          | Mean trade quantity |
| Signed Count Momentum   | Net aggressive buy/sell trades |
| Trade Rate (10s)        | Trades per second over 10-second window |
| Aggressor Ratio (10, 50, 100, 1000) | Ratio of aggressive (taker) trades |

## Usage
```bash
# Clone repository
git clone https://github.com/yogt1984/Ingestor.git
cd Ingestor

# Build and run (release mode recommended)
cargo run --release

# Run tests
cargo test