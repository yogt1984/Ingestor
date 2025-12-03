# Ingestor: Market Microstructure & Algorithmic Trading Platform

Real-time market microstructure feature extraction and market making simulation for algorithmic trading research.

## Overview

This system connects to Binance WebSocket API and computes **60+ market microstructure features** in real-time, with:
- **Paper trading market maker** with entropy-based regime detection
- **Persistence to Apache Parquet** for backtesting and ML training
- **Interactive TUI** for live monitoring

---

## Quick Start

```bash
# Build and run
cargo run --release

# Run tests (106 tests)
cargo test
```

---

## Terminal UI

```
┌─ MAIN MENU ─────────────────────────────────────────────┐
│                                                          │
│  INGESTOR - Real-Time Market Microstructure Features     │
│                                                          │
│  Symbol: BTCUSDT                                         │
│                                                          │
│  Select an option:                                       │
│                                                          │
│  [0] Live Dashboard                                      │
│  [1] Live Dashboard + Market Maker (paper trading)       │
│  [2] Feature Descriptions                                │
│                                                          │
│  Settings:                                               │
│  [p] Persist features to disk: ON                        │
│                                                          │
│  [q] Quit                                                │
└──────────────────────────────────────────────────────────┘
```

### Market Maker Dashboard

```
 BTCUSDT | 14:32:15 | MARKET MAKER (paper) | [r] reset [q] menu
┌─ MARKET MAKER STATE ────────────────────────────────────┐
│ REGIME HIGH ENTROPY   ENTROPY 0.923                     │
│ FAIR VALUE 97234.82  HALF SPREAD 4.50  SKEW +0.02       │
│ INVENTORY +0.005200  AVG ENTRY 97230.00  MAX 0.1000     │
│ VOLATILITY 0.000234  TOXICITY 0.28                      │
└─────────────────────────────────────────────────────────┘
┌─ P&L ───────────────────────────────────────────────────┐
│ REALIZED +0.0234  UNREALIZED +0.0012  TOTAL +0.0246     │
│ FEES PAID 0.000045  TRADES 12  VOLUME 0.0520            │
└─────────────────────────────────────────────────────────┘
┌─ QUOTES ────────────────────────────────────────────────┐
│   BID 97230.32 x 0.0010      ASK 97239.32 x 0.0010      │
│   MID 97234.75  MICRO 97234.82  SPREAD 0.50             │
└─────────────────────────────────────────────────────────┘
┌─ SIMULATOR ─────────────────────────────────────────────┐
│ TRADES SEEN 1234  BID FILLS 6  ASK FILLS 6  FILL 12.5%  │
│ BID MISSES 42  ASK MISSES 38  FILL VOL 0.0120           │
└─────────────────────────────────────────────────────────┘
```

---

## Market Making Engine

### Architecture

The market maker implements an **Avellaneda-Stoikov** style quoting strategy with entropy-based regime control:

```
FeaturesSnapshot ──→ MarketMakerEngine ──→ MMQuotes (bid/ask)
                            │
                            ├─→ Entropy Score ──→ MarketRegime
                            ├─→ Volatility ──────→ Spread Adjustment
                            ├─→ Inventory ───────→ Quote Skew
                            └─→ Flow Imbalance ──→ Directional Lean

                    MMSimulator (Paper Trading)
                            │
                            └─→ Trade Stream ──→ Simulated Fills ──→ PnL
```

### Regime Detection

| Regime | Entropy Score | Spread Mult | Behavior |
|--------|---------------|-------------|----------|
| **High Entropy** | ≥ 0.7 | 1.0x | Random flow, tight quotes, aggressive |
| **Medium Entropy** | 0.4 - 0.7 | 1.5x | Uncertain, widen slightly |
| **Low Entropy** | < 0.4 | 3.0x | One-sided flow, wide spreads, reduce size |

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_spread_bps` | 2.0 | Base half-spread in basis points |
| `inventory_skew_factor` | 0.5 | How much to skew per unit inventory |
| `max_inventory` | 0.1 BTC | Position limit |
| `quote_size` | 0.001 BTC | Base order size |
| `risk_aversion` | 0.1 | Gamma in Avellaneda-Stoikov |

---

## Development Roadmap

### 10 hrs/week × 6 months = 260 hours to MVP

#### Phase 1: Foundation (Weeks 1-4, ~40 hrs) ✅ COMPLETE
- [x] Real-time order book ingestion
- [x] Trade stream processing
- [x] 60+ microstructure features
- [x] Entropy-based regime detection
- [x] Volatility metrics (RV, BV, jumps)
- [x] Toxicity metrics (VPIN, adverse selection)
- [x] Parquet persistence
- [x] Basic MM engine with paper trading
- [x] Interactive TUI

#### Phase 2: Backtesting Infrastructure (Weeks 5-8, ~40 hrs)
- [ ] Historical data replay from Parquet
- [ ] Fill simulation with realistic queue model
- [ ] Slippage and latency modeling
- [ ] Performance metrics suite (Sharpe, drawdown, fill rate)
- [ ] Parameter sensitivity analysis
- [ ] Walk-forward validation framework

#### Phase 3: Strategy Optimization (Weeks 9-14, ~60 hrs)
- [ ] Grid search for MM parameters
- [ ] Bayesian optimization (Optuna integration)
- [ ] Multi-objective optimization (Sharpe vs drawdown)
- [ ] Regime-specific parameter sets
- [ ] Out-of-sample validation
- [ ] Statistical significance testing

#### Phase 4: RL Integration (Weeks 15-20, ~60 hrs)
- [ ] Gymnasium environment wrapper
- [ ] State space design (features → state vector)
- [ ] Reward shaping (PnL, inventory penalty, risk-adjusted)
- [ ] PPO/SAC baseline training
- [ ] Curriculum learning (easy → hard regimes)
- [ ] Policy comparison vs rule-based

#### Phase 5: Live Trading Preparation (Weeks 21-24, ~40 hrs)
- [ ] Paper trading validation (minimum 4 weeks)
- [ ] Risk management layer (kill switches, limits)
- [ ] Order management system (OMS)
- [ ] Execution quality monitoring
- [ ] Live/paper mode switching
- [ ] Alerting and logging

#### Phase 6: Production & Evolution (Ongoing, ~20 hrs)
- [ ] Multi-symbol support
- [ ] Cross-exchange arbitrage signals
- [ ] Ensemble of strategies
- [ ] Continuous retraining pipeline
- [ ] Performance attribution

---

## Methodology: Path to Consistent Profits

### The Scientific Approach

```
        ┌─────────────────────────────────────────────────────┐
        │                 HYPOTHESIS                          │
        │   "Entropy-based regime detection allows            │
        │    profitable market making by avoiding             │
        │    adverse selection in low-entropy regimes"        │
        └───────────────────────┬─────────────────────────────┘
                                │
        ┌───────────────────────▼─────────────────────────────┐
        │                 DATA COLLECTION                      │
        │   - Collect 3+ months of tick data                  │
        │   - Compute all 60+ features                        │
        │   - Label regime transitions                        │
        └───────────────────────┬─────────────────────────────┘
                                │
        ┌───────────────────────▼─────────────────────────────┐
        │                 BACKTESTING                          │
        │   - Walk-forward validation                         │
        │   - 1000+ simulated trades minimum                  │
        │   - Multiple market conditions                      │
        └───────────────────────┬─────────────────────────────┘
                                │
        ┌───────────────────────▼─────────────────────────────┐
        │                 PAPER TRADING                        │
        │   - Minimum 4 weeks live simulation                 │
        │   - Compare to backtest expectations                │
        │   - Identify execution gaps                         │
        └───────────────────────┬─────────────────────────────┘
                                │
        ┌───────────────────────▼─────────────────────────────┐
        │                 LIVE (tiny size)                     │
        │   - Start with 0.1x target position                 │
        │   - Scale up over 2-4 weeks                         │
        │   - Continuous monitoring                           │
        └─────────────────────────────────────────────────────┘
```

### What "Works" Means

A strategy "works" when it demonstrates:

1. **Statistical Edge**: Sharpe > 2.0 over 1000+ trades
2. **Robustness**: Profitable in 70%+ of monthly periods
3. **Drawdown Control**: Max drawdown < 20% of annual return
4. **Fill Rate**: > 15% of quotes get filled
5. **Inventory Mean-Reversion**: Position returns to zero within reasonable time
6. **Execution Feasibility**: Latency budget achievable with your infrastructure

### Metaheuristic Optimization Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    STRATEGY ENSEMBLE                         │
│                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│   │ Strategy A  │  │ Strategy B  │  │ Strategy C  │        │
│   │ (Tight MM)  │  │ (Wide MM)   │  │ (Adaptive)  │        │
│   │ High entropy│  │ Low entropy │  │ RL-based    │        │
│   └─────────────┘  └─────────────┘  └─────────────┘        │
│          │                │                │                │
│          └────────────────┼────────────────┘                │
│                           │                                 │
│                           ▼                                 │
│                  ┌─────────────────┐                        │
│                  │  Meta-Optimizer │                        │
│                  │  (Optuna/DEAP)  │                        │
│                  └─────────────────┘                        │
│                           │                                 │
│          ┌────────────────┼────────────────┐                │
│          ▼                ▼                ▼                │
│   ┌───────────┐    ┌───────────┐    ┌───────────┐          │
│   │  Params   │    │  Weights  │    │  Regime   │          │
│   │  Tuning   │    │ Allocation│    │ Switching │          │
│   └───────────┘    └───────────┘    └───────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### Success Criteria at Each Phase

| Phase | Hours | Success Metric |
|-------|-------|----------------|
| Foundation | 40 | Features computed in <10ms, 60+ metrics |
| Backtesting | 80 | Backtest matches paper within 20% |
| Optimization | 140 | Found parameters with Sharpe > 1.5 |
| RL | 200 | RL policy beats rule-based by 10%+ |
| Live Prep | 240 | Paper trading Sharpe > 1.0 for 4 weeks |
| Production | 260 | Ready for live with 0.1x size |

---

## Required Reading

### Essential (Read First)

1. **Avellaneda & Stoikov (2008)** - "High-frequency trading in a limit order book"
   - THE foundational paper for market making
   - Optimal quoting under inventory risk

2. **Guéant, Lehalle, Fernandez-Tapia (2013)** - "Dealing with the Inventory Risk"
   - Practical extensions to Avellaneda-Stoikov
   - Closed-form solutions

3. **Cartea, Jaimungal, Penalva (2015)** - "Algorithmic and High-Frequency Trading"
   - Textbook covering everything
   - Mathematical rigor + practical insights

### RL for Trading

4. **Spooner et al. (2018)** - "Market Making via Reinforcement Learning"
   - First serious RL application to MM
   - Reward shaping insights

5. **Sadighian (2019)** - "Deep Reinforcement Learning for Market Making"
   - Handles illiquid markets
   - Practical architecture

6. **Ganesh et al. (2019)** - "Reinforcement Learning for Market Making in Lit and Dark Pools"
   - Multi-venue considerations
   - State space design

### Microstructure

7. **Cont, Kukanov, Stoikov (2014)** - "The Price Impact of Order Book Events"
   - Understanding when you get filled
   - Queue position matters

8. **Easley et al. (2012)** - "Flow Toxicity and Liquidity in a High-Frequency World"
   - VPIN and informed trading
   - When to pull quotes

9. **Lehalle & Laruelle (2018)** - "Market Microstructure in Practice"
   - Real-world implementation issues
   - Latency, execution quality

---

## Feature Reference

### Order Book Features

| Feature | Formula | Description |
|---------|---------|-------------|
| **Microprice** | mid + spread × imbalance | Volume-weighted fair value (Gatheral & Oomen, 2010) |
| **PWI 1%/5%/25%/50%** | Cumulative imbalance | Depth-weighted order flow |
| **Bid/Ask Slope** | dV/dP regression | Order book resilience |
| **Depth Ratio** | Top3/Top10 volume | Liquidity concentration |

### Entropy Metrics

| Feature | Windows | Description |
|---------|---------|-------------|
| **Tick Entropy** | 1s, 5s, 10s, 15s, 30s, 1m, 15m | Price direction randomness |
| **Volume Entropy** | Same windows | Trade-size weighted |

High entropy (≈1.0) = random, good for MM
Low entropy (≈0.3) = directional, avoid MM

### Volatility Metrics

| Feature | Formula | Description |
|---------|---------|-------------|
| **Realized Volatility** | √(Σr²/n) | Standard volatility |
| **Bipower Variation** | (π/2)×Σ\|r_t\|\|r_{t-1}\| | Jump-robust (Barndorff-Nielsen, 2004) |
| **Jump Indicator** | (RV-BV)/√Var | Z-score for jumps |

### Toxicity Metrics

| Feature | Range | Description |
|---------|-------|-------------|
| **VPIN** | 0-1 | Informed trading probability |
| **Toxicity Index** | 0-1 | Composite adverse selection |

---

## System Architecture

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
                              ┌────────────────────────────────┼────────────────┐
                              │                                │                │
                              ▼                                ▼                ▼
                    MarketMakerEngine                    TUI (1Hz)      PersistenceEngine
                              │                                              (Parquet)
                              ▼
                       MMSimulator
                       (Paper Trading)
```

---

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| Symbol | BTCUSDT | Trading pair |
| Feature Rate | 100ms | Internal computation rate |
| TUI Update | 1000ms | Display refresh rate |
| Parquet Batch | 1000 | Rows per file write |
| MM Base Spread | 2 bps | Per-side spread |
| MM Max Inventory | 0.1 BTC | Position limit |

---

## Dependencies

- **tokio** - Async runtime
- **ratatui** - Terminal UI
- **polars** - DataFrame/Parquet I/O
- **rust_decimal** - Precise arithmetic
- **crossbeam** - Lock-free channels
- **serde** - Serialization

---

## License

MIT

---

## References

### Market Making (Core)
- Avellaneda, M. & Stoikov, S. (2008). High-frequency trading in a limit order book
- Guéant, O., Lehalle, C.A. & Fernandez-Tapia, J. (2013). Dealing with inventory risk
- Guéant, O. (2017). Optimal Market Making
- Cartea, A., Jaimungal, S. & Penalva, J. (2015). Algorithmic and High-Frequency Trading (textbook)

### Microstructure & Fill Simulation
- Cont, R., Kukanov, A. & Stoikov, S. (2014). The price impact of order book events
- Moallemi, C.C. & Yuan, K. (2017). The value of queue position in a limit order book
- Easley, D., López de Prado, M. & O'Hara, M. (2012). Flow toxicity and liquidity
- Kyle, A.S. (1985). Continuous auctions and insider trading
- Glosten, L. & Milgrom, P. (1985). Bid, ask and transaction prices
- Harris, L. (2003). Trading and Exchanges (textbook)
- Hasbrouck, J. (2009). Trading costs and returns for US equities
- Lehalle, C.A. & Laruelle, S. (2018). Market Microstructure in Practice

### Order Book Analysis
- Biais, B., Hillion, P. & Spatt, C. (1995). An empirical analysis of the limit order book
- Bouchaud, J.P., Mézard, M. & Potters, M. (2002). Statistical properties of stock order books
- Gatheral, J. & Oomen, R. (2010). Zero-intelligence realized variance estimation
- Gould, M., Porter, M., Williams, S. et al. (2013). Limit order books
- Roll, R. (1984). A simple implicit measure of the effective bid-ask spread
- Lee, C. & Ready, M. (1991). Inferring trade direction from intraday data
- Amihud, Y. (2002). Illiquidity and stock returns

### Volatility & Jumps
- Andersen, T., Bollerslev, T., Diebold, F. & Labys, P. (2003). Realized volatility
- Barndorff-Nielsen, O. & Shephard, N. (2004). Power and bipower variation with jumps

### RL for Trading
- Spooner, T., Fearnley, J., Mayraz, G. et al. (2018). Market making via reinforcement learning
- Sadighian, J. (2019). Deep reinforcement learning for market making in corporate bonds
- Ganesh, S., Vadori, N., Xu, M. et al. (2019). RL for market making in lit and dark pools
- Ning, B., Ling, F. & Jaimungal, S. (2021). Double deep Q-learning for optimal execution

### Deep Learning for Finance
- Zhang, Z., Zohren, S. & Roberts, S. (2019). DeepLOB: Deep convolutional neural networks for LOB
- Sirignano, J. & Cont, R. (2019). Universal features of price formation in financial markets
- Kolm, P. & Ritter, G. (2019). Modern perspectives on reinforcement learning in finance
