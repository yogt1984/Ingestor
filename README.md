```
╔═══════════════════════════════════════════════════════════════════════════╗
║                                                                           ║
║    ███╗   ███╗ █████╗ ██████╗ ███████╗                                     ║
║    ████╗ ████║██╔══██╗██╔══██╗██╔════╝                                     ║
║    ██╔████╔██║███████║██████╔╝███████╗                                     ║
║    ██║╚██╔╝██║██╔══██║██╔══██╗╚════██║                                     ║
║    ██║ ╚═╝ ██║██║  ██║██║  ██║███████║                                     ║
║    ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝                                     ║
║                                                                           ║
║    █████╗ ██████╗  █████╗ ██████╗ █████╗ ██████╗ ███████╗██████╗            ║
║   ██╔══██╗██╔══██╗██╔══██╗██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗           ║
║   ███████║██████╔╝███████║██████╔╝███████║██████╔╝█████╗  ██████╔╝           ║
║   ██╔══██║██╔══██╗██╔══██║██╔══██╗██╔══██║██╔══██╗██╔══╝  ██╔══██╗           ║
║   ██║  ██║██║  ██║██║  ██║██║  ██║██║  ██║██║  ██║███████╗██║  ██║           ║
║   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝           ║
║                                                                           ║
║    ███████╗████████╗██████╗  █████╗ ████████╗███████╗██████╗ ██╗   ██╗     ║
║    ██╔════╝╚══██╔══╝██╔══██╗██╔══██╗╚══██╔══╝██╔════╝██╔══██╗╚██╗ ██╔╝     ║
║    ███████╗   ██║   ██████╔╝███████║   ██║   █████╗  ██████╔╝ ╚████╔╝      ║
║    ╚════██║   ██║   ██╔══██╗██╔══██║   ██║   ██╔══╝  ██╔══██╗  ╚██╔╝       ║
║    ███████║   ██║   ██║  ██║██║  ██║   ██║   ███████╗██║  ██║   ██║        ║
║    ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝   ╚═╝        ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝

                    Momentum Adaptive Regime Strategy
         A Hybrid Quantitative Trading Platform for Cryptocurrency Markets
```

# MARS: Momentum Adaptive Regime Strategy

**MARS** is a sophisticated quantitative trading platform that combines trend-following momentum strategies with adaptive market-making execution. Built on real-time market microstructure analysis, MARS competes on **prediction accuracy** rather than latency, making it accessible to traders without ultra-low-latency infrastructure.

---

## 🎯 Core Philosophy

Traditional market making competes on speed—a losing game against HFT firms with microsecond infrastructure. **MARS competes on prediction accuracy** instead:

| Approach | Edge Source | Latency Sensitivity | Success Odds |
|----------|-------------|---------------------|--------------|
| Pure Market Making | Speed | Critical | 5-10% |
| **MARS** | Prediction | Tolerant | 30-40% |

---

## 🏗️ Architecture Overview

MARS integrates multiple sophisticated components:

```
Multi-Symbol Scanner  →  Regime Detector  →  Adaptive Execution  →  Risk Management
       |                      |                      |                      |
  BTC, ETH, SOL...    "Trending Up"          Heavy bid skew         TP: +20bps
                      "Trending Down"        Heavy ask skew         SL: -10bps
                      "Mean Reverting"       Symmetric quotes        Max 1 position
                      "Uncertain"            Wide spread / no quotes
```

### Key Components

- **Regime Detection**: Identifies trending vs mean-reverting market regimes using momentum, monotonicity, Hurst exponent, and entropy features
- **Cross-Asset Momentum**: Ranks symbols by trend strength and concentrates on highest-conviction opportunities
- **Adaptive Execution**: Uses Avellaneda-Stoikov market making with regime-based skew for optimal entry/exit
- **Bounded Risk**: OCO (One-Cancels-Other) orders enforce take-profit and stop-loss on every position

---

## 🚀 Quick Start

```bash
# Build the project
cargo build --release

# Run the main application
cargo run --release

# Run comprehensive test suite (1000+ tests)
cargo test --release

# Run backtest evaluation
cargo run --release --bin backtest -- evaluate --data ./data/features
```

---

## 📊 Market Making Capabilities

MARS implements sophisticated market-making strategies designed for cryptocurrency markets:

### Avellaneda-Stoikov Market Making
- **Optimal Spread Calculation**: Dynamic spread adjustment based on inventory risk and volatility
- **Regime-Adaptive Skewing**: Directional bias in quotes based on detected market regimes
- **Inventory Management**: Automatic position rebalancing to maintain target inventory levels

### Machine Learning Enhanced Market Making
- **Feature-Based Spread/Skew**: ML models learn optimal parameters from 60+ microstructure features
- **Regime-Specific Parameters**: Different spread/skew configurations for high/medium/low entropy regimes
- **Walk-Forward Validation**: Robust cross-validated weight optimization

### Fixed Spread Market Making
- **Simple Execution**: Fixed spread and skew parameters for baseline comparison
- **Low Computational Overhead**: Minimal processing requirements

**Key Features:**
- Real-time order book analysis (microprice, PWI, bid/ask slope, depth ratios)
- Trade flow analysis (VWAP, aggressor ratios, signed momentum)
- Entropy-based regime detection at multiple timeframes (1s, 5s, 10s, 30s, 1m, 15m)
- Volatility modeling (realized volatility, bipower variation, jump detection)
- Toxicity metrics (VPIN, adverse selection detection)

---

## 📈 Momentum Trading Capabilities

MARS implements time-series momentum strategies based on academic research:

### Time-Series Momentum (TSMOM)
- **Signal Generation**: Cumulative return and moving average crossover signals
- **Volatility Targeting**: Position sizing based on realized volatility (EWMA)
- **Multi-Timeframe Analysis**: Configurable lookback periods and bar frequencies

### Trend Detection Features
- **Momentum**: Linear regression slope of prices (positive = uptrend)
- **Monotonicity**: Percentage of ticks in dominant direction (>0.7 = strong trend)
- **Hurst Exponent**: Trend persistence measure (>0.5 = trending, <0.5 = mean-reverting)
- **MA Crossover**: EMA(short) - EMA(long) signals

### Cross-Asset Momentum
- **Symbol Ranking**: Ranks multiple symbols by trend strength
- **Concentration**: Focuses on highest-conviction opportunities
- **Regime-Based Allocation**: Adjusts position sizing based on detected regimes

---

## 🛠️ Command-Line Interface

MARS provides a comprehensive CLI for backtesting, validation, and research. All commands support both Market Making (MM) and Momentum (MOM) algorithms unless otherwise specified.

### Core Backtest Commands

#### `evaluate` (alias: `single`)
Run a single backtest evaluation on historical data.

```bash
cargo run --release --bin backtest -- evaluate \
    --data ./data/features \
    --algorithm as \
    --spread 3.0 \
    --skew 0.7 \
    --output results.json
```

**Key Options:**
- `--data`: Path to data directory containing Parquet files
- `--algorithm`: Algorithm type (`as`, `ml`, `fixed`, `mom`)
- `--spread`: Base spread in basis points
- `--skew`: Inventory skew factor
- `--output`: Output file for results (JSON)

#### `tune` (alias: `grid-search`)
Extended grid search over all key parameters for hyperparameter optimization.

```bash
cargo run --release --bin backtest -- tune \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7,1.0 \
    --high-entropies 0.6,0.7,0.8 \
    --fill-probs 0.05,0.10,0.15
```

**Optimizes:**
- Spread values
- Skew values
- High entropy thresholds
- Fill probability parameters

#### `grid` (MM algorithms only)
2D grid search over spread and skew parameters for quick parameter exploration.

```bash
cargo run --release --bin backtest -- grid \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7
```

**Restriction:** Only supports MM algorithms (`as`, `ml`, `fixed`)

#### `walk-forward` (alias: `wf`)
Time-series cross-validation to prevent overfitting.

```bash
cargo run --release --bin backtest -- walk-forward \
    --folds 5 \
    --test-hours 24 \
    --rolling
```

**Features:**
- Multiple fold configurations
- Rolling vs anchored/expanding windows
- Prevents lookahead bias

#### `oos-validate` (alias: `oos`)
Out-of-sample validation with hold-out test to detect overfitting.

```bash
cargo run --release --bin backtest -- oos-validate \
    --holdout 0.20 \
    --embargo-hours 1.0 \
    --spreads 1,2,3 \
    --skews 0.3,0.5
```

**Features:**
- Configurable hold-out fraction (10-50%)
- Embargo period between train/test
- Overfitting detection metrics

### Advanced Optimization Commands

#### `regime-search`
Regime-specific grid search to optimize parameters per regime independently.

```bash
cargo run --release --bin backtest -- regime-search \
    --high-spreads 0.5,1.0,1.5 \
    --med-spreads 2.0,2.5,3.0 \
    --low-spreads 4.0,5.0,none \
    --high-skews 0.2,0.3,0.4 \
    --med-skews 0.4,0.5,0.6 \
    --low-skews 0.8,1.0,1.2
```

**Optimizes:**
- Separate parameters for high/medium/low entropy regimes
- Allows no-quoting in low entropy regime

#### `regime-optimize`
Find best parameters per regime using comprehensive search.

```bash
cargo run --release --bin backtest -- regime-optimize \
    --spreads 0.5,1.0,1.5,2.0,2.5,3.0,4.0,5.0 \
    --skews 0.2,0.3,0.4,0.5,0.6,0.7,0.8,1.0 \
    --allow-no-quote
```

#### `multi-objective`
Multi-objective optimization to find Pareto frontier solutions.

```bash
cargo run --release --bin backtest -- multi-objective \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7,1.0 \
    --w-sharpe 0.4 \
    --w-drawdown 0.3 \
    --w-fill 0.2 \
    --w-turnover 0.1
```

**Optimizes:**
- Sharpe ratio
- Maximum drawdown
- Fill rate
- Turnover

### Machine Learning Commands

#### `train` (alias: `train-ml`)
Train ML weights using grid search optimization.

```bash
cargo run --release --bin backtest -- train \
    --train-ratio 0.7 \
    --spread-intercepts 1.0,2.0,3.0,4.0,5.0 \
    --spread-entropy-weights -3.0,-2.0,-1.0,0.0 \
    --spread-vol-weights 200.0,400.0,600.0 \
    --skew-intercepts 0.3,0.5,0.7 \
    --skew-inv-weights -1.0,-0.8,-0.6,-0.4 \
    --output weights.json
```

#### `walk-forward-ml`
Robust cross-validated ML weight optimization using walk-forward validation.

```bash
cargo run --release --bin backtest -- walk-forward-ml \
    --folds 5 \
    --min-train-hours 100 \
    --test-hours 24 \
    --rolling \
    --embargo-hours 1.0 \
    --weights-output consensus_weights.json
```

### Validation Campaign Commands

#### `simulate` (alias: `simulate-campaign`)
Simulate a 4-week validation campaign using historical data.

```bash
cargo run --release --bin backtest -- simulate \
    --weeks 4 \
    --session-hours 8.0 \
    --min-sessions-per-week 5 \
    --spread 2.0 \
    --skew 0.5 \
    --expected-fill-rate 0.10 \
    --expected-sharpe 1.0 \
    --expected-return 0.05 \
    --min-weekly-trades 50 \
    --max-drawdown-pct 5.0 \
    --min-win-rate 0.40
```

**Features:**
- Multi-week campaign simulation
- Weekly validation gates
- Performance comparison against backtest expectations

#### `campaign`
Run validation campaign (both algorithm types).

```bash
cargo run --release --bin backtest -- campaign \
    --weeks 4 \
    --session-hours 8.0 \
    --min-sessions-per-week 5 \
    --preset "GridSearch-Best" \
    --campaigns-dir ./data/campaigns \
    --output campaign_report.json
```

**Supports:** Both MM and MOM algorithms

### Data Management Commands

#### `validate-data` (alias: `vd`)
Validate data quality and integrity.

```bash
cargo run --release --bin backtest -- validate-data \
    --output validation_report.json
```

#### `info`
Show information about available data.

```bash
cargo run --release --bin backtest -- info
```

### Comparison Commands

#### `compare`
Compare ML algorithm vs Avellaneda-Stoikov.

```bash
cargo run --release --bin backtest -- compare \
    --algorithm ml \
    --weights weights.json \
    --output comparison.json
```

#### `head-to-head`
Head-to-head comparison: ML vs Avellaneda-Stoikov on same data.

```bash
cargo run --release --bin backtest -- head-to-head \
    --weights ml_weights.json \
    --as-spread 2.0 \
    --as-skew 0.5 \
    --output head_to_head.json
```

### Session Validation Commands

#### `simulate-session`
Simulate a paper trading session using historical data.

```bash
cargo run --release --bin backtest -- simulate-session \
    --duration 1.0 \
    --preset "GridSearch-Best" \
    --spread 2.0 \
    --skew 0.5 \
    --sessions-dir ./data/sessions
```

#### `validate-session`
Validate paper trading sessions against backtest expectations.

```bash
cargo run --release --bin backtest -- validate-session \
    --session session_summary.json \
    --sessions-dir ./data/sessions \
    --min-hours 0.5 \
    --min-trades 5
```

---

## 📚 Academic Foundations

MARS is built on rigorous academic research in quantitative finance:

### Momentum & Trend-Following

**Jegadeesh, N. & Titman, S. (1993).** *Returns to Buying Winners and Selling Losers*  
*Journal of Finance, 48(1), 65-91.*  
The foundational cross-sectional momentum paper establishing the momentum effect in equity markets.

**Moskowitz, T., Ooi, Y.H. & Pedersen, L.H. (2012).** *Time Series Momentum*  
*Journal of Financial Economics, 104(2), 228-250.*  
The basis for MARS's TSMOM implementation. Core formula: `m_t = Σ r_{t-i}` (cumulative return over lookback), signal: `s_t = sign(m_t)`, with volatility targeting: `w_t = σ* / σ_t`.

**Asness, C., Moskowitz, T. & Pedersen, L.H. (2013).** *Value and Momentum Everywhere*  
*Journal of Finance, 68(3), 929-985.*  
Demonstrates the universality of momentum effects across asset classes and markets.

**Lemperiere, Y., Deremble, C., Seager, P., Potters, M. & Bouchaud, J.P. (2014).** *Two Centuries of Trend Following*  
*Quantitative Finance, 14(8), 1417-1431.*  
Long-term analysis of trend-following strategies across two centuries of data.

**Baltas, N. & Kosowski, R. (2013).** *Momentum Strategies in Futures Markets and Trend-Following Funds*  
*Review of Derivatives Research, 16(1), 39-75.*  
Analysis of momentum strategies in futures markets.

### Market Making & Execution

**Avellaneda, M. & Stoikov, S. (2008).** *High-frequency trading in a limit order book*  
*Quantitative Finance, 8(3), 217-224.*  
The foundational market-making model used in MARS for optimal spread calculation and inventory management.

**Almgren, R. & Chriss, N. (2001).** *Optimal Execution of Portfolio Transactions*  
*Journal of Risk, 3(2), 5-40.*  
Optimal execution framework for minimizing market impact.

**Cont, R., Kukanov, A. & Stoikov, S. (2014).** *The price impact of order book events*  
*Journal of Financial Markets, 17, 1-25.*  
Analysis of order book dynamics and price impact.

### Risk Management

**Daniel, K. & Moskowitz, T. (2016).** *Momentum Crashes*  
*Journal of Financial Economics, 122(2), 221-247.*  
Analysis of momentum strategy crashes and risk management implications.

### Cryptocurrency Research

**Liu, Y., Tsyvinski, A. & Wu, X. (2019).** *Common Risk Factors in Cryptocurrency*  
*Journal of Finance, 74(6), 2581-2627.*  
Factor model for cryptocurrency returns and risk.

**Baur, D.G. & Hoang, L.T. (2020).** *Technical Trading and Cryptocurrencies*  
*Finance Research Letters, 35, 101-281.*  
Analysis of technical trading strategies in cryptocurrency markets.

---

## 🔬 Market Microstructure Features

MARS extracts 60+ real-time features from market microstructure:

### Order Book Features
- Microprice (weighted mid-price)
- Price-Weighted Imbalance (PWI)
- Bid/ask slope and depth ratios
- Order book imbalance metrics

### Trade Flow Features
- Volume-Weighted Average Price (VWAP)
- Aggressor ratios (buyer/seller initiated)
- Signed momentum and trade intensity

### Entropy Features
- Tick entropy at multiple timeframes: 1s, 5s, 10s, 30s, 1m, 15m
- Regime detection based on entropy levels

### Volatility Features
- Realized volatility (multiple estimators)
- Bipower variation
- Jump detection and identification

### Toxicity Features
- Volume-Synchronized Probability of Informed Trading (VPIN)
- Adverse selection metrics
- Order flow toxicity indicators

---

## 🛡️ Risk Management

### OCO Manager
- Take-profit and stop-loss orders with basis point offsets
- Absolute price targets
- Automatic order cancellation on fill

### Position Manager
- Volatility-based position sizing
- Kelly criterion for optimal sizing
- Exposure limits and position constraints

### Risk Manager
- Staged circuit breaker system (warning/reduce/halt/emergency)
- Real-time drawdown tracking
- Risk limit enforcement

### P&L Tracker
- Real-time P&L tracking
- FIFO cost basis accounting
- Attribution by source (market making vs momentum)
- Equity curve generation

---

## 📦 Data Storage

### Storage Format
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

## 🏛️ System Architecture

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
                                        +-- TrendFeatureEngine +
                                                               |
                                                               v
                                                    FeatureFusionEngine
                                                               |
                              +--------------------------------+----------------+
                              |                                |                |
                              v                                v                v
                    RegimeDetector                    TUI (1Hz)      PersistenceEngine
                              |                                              (Parquet)
                              v
                    TradingStrategy
                              |
                              v
                    A-S Execution Layer
                              |
                              v
                    OCOManager
```

---

## 📄 License

MIT License

---

## 🔗 Additional Resources

For a complete reading list with links to all referenced papers, see `PAPERS.md`.

---

*MARS: Where prediction accuracy meets adaptive execution.*
