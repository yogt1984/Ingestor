# MARS: Momentum Adaptive Regime Strategy

A hybrid trend-following and market-making system for cryptocurrency trading, built on real-time market microstructure analysis.

## Overview

MARS (Momentum Adaptive Regime Strategy) is a quantitative trading platform that combines:

- **Trend Detection**: Identifies trending vs mean-reverting market regimes using momentum, monotonicity, Hurst exponent, and entropy features
- **Cross-Asset Momentum**: Ranks symbols by trend strength and concentrates on highest-conviction opportunities
- **Adaptive Execution**: Uses Avellaneda-Stoikov market making with regime-based skew for optimal entry/exit
- **Bounded Risk**: OCO (One-Cancels-Other) orders enforce take-profit and stop-loss on every position

### Core Strategy

```
Multi-Symbol Scanner  -->  Regime Detector  -->  A-S with Directional Skew  -->  OCO Risk Management
       |                        |                         |                            |
  BTC, ETH, SOL...     "Trending Up"              Heavy bid skew               TP: +20bps
                       "Trending Down"            Heavy ask skew               SL: -10bps
                       "Mean Reverting"           Symmetric quotes             Max 1 position
                       "Uncertain"                Wide spread / no quotes
```

### Why MARS?

Traditional market making competes on latency - a losing game against HFT firms with microsecond infrastructure. MARS competes on **prediction accuracy** instead:

| Approach | Edge Source | Latency Sensitivity | Success Odds |
|----------|-------------|---------------------|--------------|
| Pure MM (v0.1) | Speed | Critical | 5-10% |
| **MARS (v0.2)** | Prediction | Tolerant | 30-40% |

---

## Quick Start

```bash
# Build and run
cargo run --release

# Run tests (970+ tests)
cargo test --release

# Run backtester
cargo run --release --bin backtest -- --data ./data/features
```

---

## Features

### Trend Detection (NEW in v0.2)

| Feature | Description | Interpretation |
|---------|-------------|----------------|
| **Momentum** | Linear regression slope of prices | Positive = uptrend |
| **Monotonicity** | % of ticks in dominant direction | >0.7 = strong trend |
| **Hurst Exponent** | Trend persistence measure | >0.5 = trending, <0.5 = mean-reverting |
| **MA Crossover** | EMA(short) - EMA(long) | Positive = bullish |

### Market Microstructure (60+ features)

- Order book: microprice, PWI, bid/ask slope, depth ratios
- Trade flow: VWAP, aggressor ratios, signed momentum
- Entropy: tick entropy at 1s, 5s, 10s, 30s, 1m, 15m windows
- Volatility: realized volatility, bipower variation, jump detection
- Toxicity: VPIN, adverse selection metrics

---

## Architecture

```
Binance WebSocket
        |
        +-- Order Book Stream --> ConcurrentOrderBook --> OrderBookEngine
        |                              |                       |
        |                              +-- ToxicityEngine -----+
        |                                                      |
        +-- Trade Stream -------> ConcurrentTradesLog --> TradesLogEngine
                                        |                      |
                                        +-- EntropyEngine -----+
                                        +-- IlliquidityEngine -+
                                        +-- VolatilityEngine --+
                                        +-- TrendFeatureEngine + (NEW)
                                                               |
                                                               v
                                                    FeatureFusionEngine
                                                               |
                              +--------------------------------+----------------+
                              |                                |                |
                              v                                v                v
                    RegimeDetector (NEW)               TUI (1Hz)      PersistenceEngine
                              |                                              (Parquet)
                              v
                    TradingStrategy (NEW)
                              |
                              v
                    A-S Execution Layer
                              |
                              v
                    OCOManager (NEW)
```

---

## Development Roadmap

### Phase 0: Foundation
- [x] Implement `trend_features.rs` (momentum, monotonicity, Hurst exponent)
- [x] Implement `signal_processing.rs` (Kalman filter for velocity/acceleration)
- [ ] Add multi-symbol data ingestion (ETH, SOL minimum)
- [ ] Implement `cross_asset.rs` (correlation, joint momentum)
- [x] Create `RegimeDetector` trait and basic implementation
- [ ] Add regime labels to `FeaturesSnapshot`

### Phase 1: Hybrid Strategy
- [ ] Create `TradingStrategy` trait
- [ ] Implement `TrendRegimeDetector` (monotonic + entropy)
- [ ] Implement `HybridMMStrategy` (A-S with regime skew)
- [ ] Add regime-based spread/skew adjustment logic
- [ ] Implement position tracking for directional trades

### Phase 2: Risk Management
- [ ] Implement `OCOManager` for take-profit/stop-loss
- [ ] Implement `PositionManager` (size limits, exposure)
- [ ] Add drawdown tracking and circuit breaker
- [ ] Integrate OCO with backtest harness
- [ ] Add position P&L tracking in real-time

### Phase 3: Backtesting & Validation
- [ ] Update backtest harness for `TradingStrategy` trait
- [ ] Add trend-specific metrics (win rate, avg win/loss)
- [ ] Run walk-forward validation on historical data
- [ ] Parameter sweep: regime thresholds, TP/SL ratios
- [ ] Out-of-sample test on held-out data
- [ ] Document findings and regime persistence analysis

### Phase 4: Paper Trading
- [ ] Integrate hybrid strategy with TUI paper trading
- [ ] Add OCO order simulation in paper trader
- [ ] Run paper trading for 2+ weeks
- [ ] Compare paper vs backtest results
- [ ] Analyze regime detection accuracy in live data

---

## Success Criteria

| Criterion | Target |
|-----------|--------|
| Out-of-sample Sharpe | > 0.5 |
| Maximum drawdown | < 10% |
| Regime detection accuracy | > 60% |
| Win rate | > 45% |
| Risk/reward ratio | > 1.5 |

---

## Data Collection

### Storage

- **Location:** `./data/features/*.parquet`
- **Size:** ~200KB per file (1000 rows each)
- **Rate:** ~1 file per ~15 minutes

| Duration | Approx Files | Approx Size |
|----------|--------------|-------------|
| 1 day | ~96 | ~20 MB |
| 1 week | ~672 | ~135 MB |
| 1 month | ~2,880 | ~575 MB |

### Running Overnight

```bash
# Using tmux (recommended)
tmux new -s ingestor
cargo run --release
# Press [0] for Live Dashboard
# Detach: Ctrl+B, then D
# Reattach: tmux attach -t ingestor
```

---

## References

### Momentum & Trend-Following

- Jegadeesh, N. & Titman, S. (1993). Returns to Buying Winners and Selling Losers
- Moskowitz, T., Ooi, Y.H. & Pedersen, L.H. (2012). Time Series Momentum
- Asness, C., Moskowitz, T. & Pedersen, L.H. (2013). Value and Momentum Everywhere
- Lemperiere, Y. et al. (2014). Two Centuries of Trend Following

### Market Making & Execution

- Avellaneda, M. & Stoikov, S. (2008). High-frequency trading in a limit order book
- Almgren, R. & Chriss, N. (2001). Optimal Execution of Portfolio Transactions
- Cont, R., Kukanov, A. & Stoikov, S. (2014). The price impact of order book events

### Risk Management

- Daniel, K. & Moskowitz, T. (2016). Momentum Crashes

### Cryptocurrency Research

- Liu, Y., Tsyvinski, A. & Wu, X. (2019). Common Risk Factors in Cryptocurrency
- Baur, D.G. & Hoang, L.T. (2020). Technical Trading and Cryptocurrencies

See `PAPERS.md` for the complete reading list with links.

---

## License

MIT

---

## Project Status

**Current Version:** v0.2-dev (MARS pivot)

**Previous:** v0.1 explored pure market making but faced structural disadvantages (latency, adverse selection). v0.2 pivots to hybrid trend-following to compete on prediction accuracy rather than speed.
