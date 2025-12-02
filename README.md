# Binance WebSocket Market Data Ingestor

Real-time market microstructure feature extraction pipeline for algorithmic trading research.

## Overview

This system connects to Binance WebSocket API (BTC/USDT) and computes **60+ market microstructure features** in real-time, persisting them to Apache Parquet files for backtesting and ML model training.

## Quick Start

```bash
# Build and run
cargo run --release

# Run tests
cargo test
```

## Terminal UI

The application provides an interactive menu-driven TUI:

```
┌─ MAIN MENU ─────────────────────────────────────────┐
│                                                      │
│  INGESTOR - Real-Time Market Microstructure Features │
│                                                      │
│  Symbol: BTCUSDT                                     │
│                                                      │
│  Select an option:                                   │
│                                                      │
│  [0] Run Live Dashboard                              │
│  [1] Show Feature Descriptions                       │
│                                                      │
│  [q] Quit                                            │
└──────────────────────────────────────────────────────┘
```

### Live Dashboard (1Hz updates, 1-second averages)

```
 BTCUSDT | 14:32:15 | 10 samples/sec | [q] menu
┌─ ORDER BOOK ─────────────────────────────────────────┐
│ BID 97234.50  ASK 97235.00  SPREAD 0.50              │
│ MID 97234.75  MICRO 97234.82  IMB +2.34%             │
│ PWI 1%=+0.12% 5%=+0.45% 25%=+1.23% 50%=+2.34%       │
│ SLOPE B=-0.0012 A=0.0015  VOL_IMB +5.67%             │
│ DEPTH B=0.45 A=0.38  VOL_001 B=12.50 A=8.30          │
└──────────────────────────────────────────────────────┘
┌─ TRADES & FLOW ──────────────────────────────────────┐
│ LAST 97234.80  VWAP 97234.65  SIZE 0.0234            │
│ VWAP 10=97234.70 50=97234.55 100=97234.40 1000=...   │
│ MOM +15  RATE 8.5/s  FLOW I=+0.23 P=12.5             │
└──────────────────────────────────────────────────────┘
┌─ ILLIQUIDITY ────────────────────────────────────────┐
│ ROLL 0.000123  AMIHUD 1.23e-08  KYLE 0.0045  VPIN 0.32│
└──────────────────────────────────────────────────────┘
┌─ ENTROPY ────────────────────────────────────────────┐
│ TICK 1s=0.892 5s=0.934 10s=0.956 30s=0.978 1m=0.989  │
└──────────────────────────────────────────────────────┘
┌─ VOLATILITY ─────────────────────────────────────────┐
│ RV_100 0.000234  RV_1K 0.000198  BV 0.000201  JUMP 1.23│
└──────────────────────────────────────────────────────┘
┌─ TOXICITY ───────────────────────────────────────────┐
│ TOXIC M=23.4% m=21.2%  ADV 0.0012  ASYM +0.15  IDX 0.28│
└──────────────────────────────────────────────────────┘
┌─ MICRO ────┐┌─ PWI ──────┐┌─ ENT ──────┐┌─ VOL ──────┐
│ ▁▂▃▄▅▆▇█▇▆ ││ ▁▂▃▄▅▆▇█▇▆ ││ ▁▂▃▄▅▆▇█▇▆ ││ ▁▂▃▄▅▆▇█▇▆ │
└────────────┘└────────────┘└────────────┘└────────────┘
```

---

## Feature Reference

### Order Book Features

| Feature | Formula | Description |
|---------|---------|-------------|
| **Best Bid/Ask** | Top-of-book | Highest bid and lowest ask prices (Kyle, 1985) |
| **Mid Price** | (bid + ask) / 2 | Fair value estimate (Harris, 2003) |
| **Microprice** | mid + spread × imbalance | Volume-weighted mid price (Gatheral & Oomen, 2010) |
| **Spread** | ask - bid | Transaction cost measure (Roll, 1984) |
| **Imbalance** | (Vb-Va)/(Vb+Va) | Predicts short-term price direction (Cont et al., 2014) |
| **PWI 1%/5%/25%/50%** | Cumulative imbalance | Price-weighted imbalance at depth percentiles (Cartea et al., 2015) |
| **Bid/Ask Slope** | dV/dP regression | Order book resilience (Bouchaud et al., 2002) |
| **Volume Imbalance Top 5** | Extended imbalance | Deeper book dynamics |
| **Depth Ratio** | Top3/Top10 volume | Liquidity concentration (Gould et al., 2013) |
| **Volume 0.01%** | Volume within 1bp | Immediate available liquidity |

### Trade Features

| Feature | Formula | Description |
|---------|---------|-------------|
| **Last Trade Price** | Most recent execution | Current market activity |
| **Trade Imbalance** | Buy/Sell ratio | Aggression direction (Lee & Ready, 1991) |
| **VWAP** | Σ(P×V)/ΣV | Execution quality benchmark (Berkowitz et al., 1988) |
| **Price Change** | P(t) - P(t-1) | Tick-to-tick movement |
| **Avg Trade Size** | Mean quantity | Institutional activity indicator |
| **Signed Momentum** | Net buy/sell count | Directional pressure |
| **Trade Rate** | Trades/second | Market intensity |
| **Aggressor Ratio** | Taker/Total | Directional conviction (Biais et al., 1995) |

### Order Flow Features

| Feature | Formula | Description |
|---------|---------|-------------|
| **Flow Imbalance** | Placements - Cancels | Real-time pressure indicator |
| **Flow Pressure** | Cumulative flow | Sustained pressure predictor |

### Illiquidity Metrics

| Feature | Formula | Description |
|---------|---------|-------------|
| **Roll Spread** | 2√(-cov) | Effective spread from autocovariance (Roll, 1984) |
| **Amihud Lambda** | \|r\|/V | Price impact per volume (Amihud, 2002) |
| **Kyle Lambda** | dP/dQ slope | Permanent price impact (Kyle, 1985) |
| **Hasbrouck Lambda** | Trade impact | Effective spread estimator (Hasbrouck, 2009) |
| **VPIN** | Volume-sync PIN | Informed trading probability (Easley et al., 2012) |

### Entropy Metrics

| Feature | Formula | Description |
|---------|---------|-------------|
| **Tick Entropy** | H = -Σp×log(p) | Price direction randomness (Shannon, 1948) |
| **Volume Entropy** | Volume-weighted H | Trade significance adjusted |
| **Time Windows** | 1s, 5s, 10s, 15s, 30s, 1m, 15m | Multi-scale regime detection |

### Volatility Metrics

| Feature | Formula | Description |
|---------|---------|-------------|
| **Realized Volatility** | RV = √(Σr²/n) | Sum of squared returns (Andersen et al., 2003) |
| **RV Windows** | 100 and 1000 trades | Multi-scale volatility measurement |
| **Bipower Variation** | BV = (π/2)×Σ\|r_t\|\|r_{t-1}\| | Jump-robust volatility (Barndorff-Nielsen & Shephard, 2004) |
| **Jump Indicator** | Z = (RV-BV)/√Var(BV) | Jump test statistic; Z > 3 = significant jump |
| **Vol-of-Vol** | σ(σ_t) | Second-order uncertainty; regime instability |

### Toxicity Metrics

| Feature | Formula | Description |
|---------|---------|-------------|
| **Toxic Flow Ratio** | Toxic Vol / Total Vol | Informed trading proportion (Easley et al., 2012) |
| **Adverse Selection** | E[cost to informed] | Expected loss to informed traders |
| **Arrival Asymmetry** | (buys-sells)/total | Buy/sell rate imbalance |
| **Size Toxicity** | Large/Small toxic ratio | Large trade informativeness |
| **Toxicity Index** | Composite [0,1] | Weighted toxicity score |

---

## Architecture

```
Binance WebSocket
        │
        ├─→ Order Book Stream ─→ ConcurrentOrderBook ─→ OrderBookEngine
        │                              │                       │
        │                              └─→ ToxicityEngine ─────┤
        │                                                      │
        └─→ Trade Stream ─────→ ConcurrentTradesLog ─→ TradesLogEngine
                                        │                      │
                                        ├─→ EntropyEngine ─────┤
                                        ├─→ IlliquidityEngine ─┤
                                        └─→ VolatilityEngine ──┤
                                                               │
                                                               ▼
                                                    FeatureFusionEngine
                                                               │
                                              ┌────────────────┼────────────────┐
                                              ▼                ▼                ▼
                                        TUI (1Hz)      PersistenceEngine   Analysis
                                                        (Parquet files)
```

## Data Storage

Features are persisted to Apache Parquet files in `data/features/`:
- Columnar format for efficient analytical queries
- 60+ feature columns per snapshot
- Automatic file rotation

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| Symbol | BTCUSDT | Trading pair |
| Feature Rate | 100ms | Internal computation rate |
| TUI Update | 1000ms | Display refresh rate |
| Parquet Batch | 1000 | Rows per file write |

## Dependencies

- **tokio** - Async runtime
- **ratatui** - Terminal UI
- **polars** - DataFrame/Parquet I/O
- **rust_decimal** - Precise arithmetic
- **crossbeam** - Lock-free channels

## References

- Amihud, Y. (2002). Illiquidity and stock returns
- Andersen, T., et al. (2003). Modeling and forecasting realized volatility
- Barndorff-Nielsen, O. & Shephard, N. (2004). Power and bipower variation with stochastic volatility and jumps
- Biais, B., et al. (1995). An empirical analysis of the limit order book
- Bouchaud, J.P., et al. (2002). Statistical properties of stock order books
- Cartea, A., et al. (2015). Algorithmic and High-Frequency Trading
- Cont, R., et al. (2014). The price impact of order book events
- Easley, D., et al. (2012). Flow toxicity and liquidity in a high-frequency world
- Gatheral, J. & Oomen, R. (2010). Zero-intelligence realized variance estimation
- Glosten, L. & Milgrom, P. (1985). Bid, ask and transaction prices
- Gould, M., et al. (2013). Limit order books
- Harris, L. (2003). Trading and Exchanges
- Hasbrouck, J. (2009). Trading costs and returns
- Kyle, A.S. (1985). Continuous auctions and insider trading
- Lee, C. & Ready, M. (1991). Inferring trade direction
- Roll, R. (1984). A simple implicit measure of the effective bid-ask spread
- Shannon, C. (1948). A mathematical theory of communication

## License

MIT
