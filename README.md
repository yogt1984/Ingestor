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

# Run grid search after collecting data
cargo run --release --bin backtest -- grid-search --test-gate
```

---

## Data Collection Guide

### Overnight/Continuous Recording

To collect data for backtesting, you need to run the ingestor continuously. Here are several methods:

#### Method 1: tmux (Recommended)
```bash
# Start a new tmux session
tmux new -s ingestor

# Run the ingestor
cargo run --release

# Press [0] for Live Dashboard
# Detach from tmux: Ctrl+B, then D

# Reattach later
tmux attach -t ingestor
```

#### Method 2: screen
```bash
# Start a new screen session
screen -S ingestor

# Run the ingestor
cargo run --release

# Detach: Ctrl+A, then D

# Reattach later
screen -r ingestor
```

#### Method 3: nohup (Headless)
```bash
# Run in background (no TUI)
nohup cargo run --release > ingestor.log 2>&1 &

# Check status
tail -f ingestor.log

# Stop
pkill -f "target/release/ingestor"
```

#### Method 4: systemd Service
```bash
# Create /etc/systemd/system/ingestor.service
[Unit]
Description=Ingestor Market Data Collector
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/Ingestor
ExecStart=/path/to/Ingestor/target/release/ingestor
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target

# Enable and start
sudo systemctl enable ingestor
sudo systemctl start ingestor
```

### Data Storage

- **Location:** `./data/features/*.parquet`
- **Size:** ~200KB per file (1000 rows each)
- **Rate:** ~1 file per ~15 minutes
- **Retention:** Default unlimited, configurable via [s] in menu

| Duration | Approx Files | Approx Size |
|----------|--------------|-------------|
| 1 day | ~96 | ~20 MB |
| 1 week | ~672 | ~135 MB |
| 1 month | ~2,880 | ~575 MB |
| 3 months | ~8,640 | ~1.7 GB |

### Recommended Data Collection

For reliable backtesting:
- **Minimum:** 2 weeks (for basic parameter validation)
- **Recommended:** 1-3 months (for walk-forward validation)
- **Ideal:** 6+ months (for regime diversity)

---

## Terminal UI

```
┌─ MAIN MENU ─────────────────────────────────────────────┐
│                                                          │
│  INGESTOR - Real-Time Market Microstructure Features     │
│                                                          │
│  Symbol: BTCUSDT                                         │
│                                                          │
│  DATA COLLECTION                                         │
│  [0] Live Dashboard - stream & save features             │
│  [1] Live + Market Maker - paper trade (default params)  │
│  [6] Paper Trade w/ Preset - validate optimized params   │
│                                                          │
│  BACKTESTING                                             │
│  [3] Run Backtest                                        │
│  [4] Walk-Forward Validation                             │
│  [5] Data Quality Check                                  │
│                                                          │
│  INFO                                                    │
│  [2] Feature Descriptions                                │
│                                                          │
│  SETTINGS                                                │
│  [p] Persist to disk: ON                                 │
│  [s] Max storage: 10 GB                                  │
│                                                          │
│  [q] Quit                                                │
└──────────────────────────────────────────────────────────┘
```

### Market Maker Dashboard

When using `[6] Paper Trade w/ Preset`, the dashboard shows which preset is active:

```
 BTCUSDT | 14:32:15 | Preset: GridSearch-Best (2025-12-03 16:00) | Quotes: 1234 | [r] reset [q] menu
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

### Paper Trading with Presets

The `[6] Paper Trade w/ Preset` option lets you validate optimized parameters with live market data:

1. **Select a Preset**: Choose from presets created via grid-search or Bayesian optimization
2. **Paper Trade**: Run the market maker with those parameters against live data
3. **Measure Fill Rate**: Compare actual fills vs backtest assumptions
4. **Session Saved**: Results automatically saved to `./data/sessions/`

Example preset selection screen:
```
┌─ PRESET SELECTION ──────────────────────────────────────┐
│                                                          │
│  SELECT PARAMETER PRESET FOR PAPER TRADING               │
│                                                          │
│  AVAILABLE PRESETS:                                      │
│                                                          │
│  >> [1] GridSearch-Best                                  │
│         Developed: 2025-12-03 16:00 via grid-search      │
│         Spread: 1.0bps | Skew: 0.30 | Entropy: 0.70      │
│         Expected: +5.1% return | 59.5% win | 452 trades  │
│                                                          │
│     [2] GridSearch-Conservative                          │
│         Developed: 2025-12-03 16:00 via grid-search      │
│         Spread: 1.0bps | Skew: 0.30 | Entropy: 0.70      │
│         Expected: +1.1% return | 55.0% win | 202 trades  │
│                                                          │
│  [up/down] Navigate  [Enter] Select  [q] Back            │
└──────────────────────────────────────────────────────────┘
```

---

## Algorithm: Avellaneda-Stoikov Market Making

### Theoretical Foundation

The core algorithm is the **Avellaneda-Stoikov (2008)** optimal market making model, which solves:

> *"Where should a market maker place bid/ask quotes to maximize profit while managing inventory risk?"*

#### Reservation Price

The key insight is the **reservation price** - the price at which the market maker is indifferent to trading:

```
reservation_price = mid_price - inventory × γ × σ² × T
```

Where:
- `γ` (gamma) = risk aversion parameter (how much we dislike inventory risk)
- `σ` = volatility estimate
- `T` = time horizon
- `inventory` = current position

**Intuition**: If we're long, our reservation price drops (we want to sell). If we're short, it rises (we want to buy).

#### Optimal Spread

```
optimal_spread = γ × σ² × T + (2/γ) × ln(1 + γ/k)
```

Where `k` is a market liquidity parameter. The spread widens with volatility and risk aversion.

### Implementation Extensions

Our implementation extends vanilla Avellaneda-Stoikov with:

| Extension | Paper | Purpose |
|-----------|-------|---------|
| **Entropy Regime Detection** | Novel | Avoid adverse selection in trending markets |
| **Flow Toxicity (VPIN)** | Easley et al. (2012) | Detect informed trading |
| **Queue Position Modeling** | Moallemi & Yuan (2017) | Realistic fill simulation |
| **Microprice Fair Value** | Gatheral & Oomen (2010) | Better fair value estimate |
| **Bipower Volatility** | Barndorff-Nielsen (2004) | Jump-robust volatility |

### Architecture

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

**Why Entropy?** Low entropy means price movements are predictable (trending). In trending markets, market makers get "picked off" by informed traders who know where price is going. High entropy means random walk behavior, ideal for capturing bid-ask spread.

---

## Degrees of Freedom (Parameters)

### Market Making Parameters (`MMConfig`)

| Parameter | Symbol | Default | Range | Description |
|-----------|--------|---------|-------|-------------|
| `base_spread_bps` | s | 2.0 | 0.5 - 10.0 | Base half-spread in basis points |
| `inventory_skew_factor` | κ | 0.5 | 0.1 - 2.0 | How much to skew per unit inventory |
| `max_inventory` | I_max | 0.1 | 0.01 - 1.0 | Position limit (BTC) |
| `quote_size` | q | 0.001 | 0.0001 - 0.01 | Base order size (BTC) |
| `risk_aversion` | γ | 0.1 | 0.01 - 1.0 | Avellaneda-Stoikov gamma |
| `high_entropy_threshold` | θ_h | 0.7 | 0.5 - 0.9 | Above = high entropy regime |
| `low_entropy_threshold` | θ_l | 0.4 | 0.2 - 0.6 | Below = low entropy regime |
| `medium_entropy_spread_mult` | m_m | 1.5 | 1.0 - 3.0 | Spread multiplier in medium |
| `low_entropy_spread_mult` | m_l | 3.0 | 1.5 - 5.0 | Spread multiplier in low |
| `pull_quotes_in_low_entropy` | - | false | bool | Stop quoting in low entropy |

### Fill Simulation Parameters (`FillSimulatorConfig`)

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `base_fill_probability` | 0.10 | 0.01 - 0.30 | Fill rate when price touches |
| `queue_position` | 0.5 | 0.0 - 1.0 | Position in queue (0=front, 1=back) |
| `adverse_selection_factor` | 0.3 | 0.0 - 1.0 | Expected adverse price move after fill |
| `quote_latency_ms` | 50 | 10 - 500 | Latency before quote is active |
| `min_fill_fraction` | 0.1 | 0.0 - 1.0 | Minimum partial fill size |
| `competitive_spread_bps` | 2.0 | 0.5 - 5.0 | Spread below which competition is high |

### Total Degrees of Freedom

**Core tunable**: 6 (spread, skew, risk_aversion, two entropy thresholds, pull_quotes)
**Fill simulation**: 4 (fill_prob, queue_pos, adverse_selection, latency)
**Total**: ~10 key parameters for optimization

---

## Entropy Gate: Selective Market Making

### The Concept

Instead of always quoting (with regime-adjusted spreads), an **entropy gate** only activates market making when conditions are favorable:

```
IF entropy > threshold THEN
    activate market making
ELSE
    no quotes (sit out)
```

### Why This Might Work

| Condition | Entropy | Market Maker Outcome |
|-----------|---------|----------------------|
| **Trending** | Low (<0.4) | Gets run over by informed flow |
| **Choppy** | High (>0.7) | Captures spread repeatedly |
| **News Event** | Very Low | Massive adverse selection |

**Hypothesis**: By only participating in high-entropy regimes, we avoid the worst adverse selection while still capturing spread in favorable conditions.

### Current Implementation

The current implementation uses **spread widening** rather than full gating:

```rust
// In MMConfig
pull_quotes_in_low_entropy: false  // Set to true for full gating
low_entropy_spread_mult: 3.0       // Alternative: widen spread 3x
```

### Proposed Experiment: Full Entropy Gate

To test the hypothesis:

1. **Baseline**: Run MM continuously with spread adjustment (current)
2. **Gated**: Only quote when `tick_entropy_10s > 0.7`
3. **Compare**: Sharpe ratio, fill rate, adverse selection costs

**Expected outcome**: Gated version has fewer trades but higher win rate and better risk-adjusted returns.

### Trade-offs

| Approach | Pros | Cons |
|----------|------|------|
| **Always Quote** | More fills, higher volume | Adverse selection in trends |
| **Spread Widening** | Stay active, adaptive | May not be wide enough |
| **Entropy Gate** | Clean avoidance | Miss opportunities, regime lag |

### Implementation Status

The `pull_quotes_in_low_entropy` flag exists but is `false` by default. A full grid search comparing gated vs. ungated strategies is the next logical step.

---

## Grid Search & Parameter Optimization

### Available Commands

```bash
# Basic parameter sweep
cargo run --release --bin backtest -- sweep \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7

# Extended grid search (360 combinations)
cargo run --release --bin backtest -- grid-search --test-gate

# Grid search with custom parameters
cargo run --release --bin backtest -- grid-search \
    --spreads 1,2,3 \
    --skews 0.3,0.5,0.7 \
    --high-entropies 0.6,0.7,0.8 \
    --fill-probs 0.05,0.10,0.15 \
    --test-gate \
    --output results.json

# Single backtest run
cargo run --release --bin backtest -- --spread 1.0 --skew 0.3 --fill-prob 0.10

# Data info
cargo run --release --bin backtest -- info
```

### Grid Search Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--spreads` | 1,2,3,4,5 | Spread values in bps |
| `--skews` | 0.3,0.5,0.7,1.0 | Inventory skew factors |
| `--high-entropies` | 0.6,0.7,0.8 | Entropy thresholds |
| `--fill-probs` | 0.05,0.10,0.15 | Fill probability estimates |
| `--test-gate` | false | Compare GATED vs UNGATED modes |
| `--output` | none | Save results to JSON file |

### Grid Search Output

The grid search tests all parameter combinations and reports:

```
═══════════════════════════════════════════════════════
TOP 10 PARAMETER SETS (by Sharpe):
═══════════════════════════════════════════════════════
 1. Spread=1.0 Skew=0.3 Entropy=0.7 WIDE FillP=0.15
    Sharpe=-1.20 Return=+5.14% DD=0.43% WinRate=59.5% Trades=452
...

═══════════════════════════════════════════════════════
ENTROPY GATE COMPARISON:
═══════════════════════════════════════════════════════
                    UNGATED (spread widen)  vs  GATED (no quotes)
  Avg Sharpe:       -34.09                      -213.76
  Avg Trades:       185.6                       11.7
```

### Latest Grid Search Results (Dec 3, 2025)

See `REPORT_03_12_25.md` for full analysis. Key findings:

| Finding | Implication |
|---------|-------------|
| **Only spread=1 bps profitable** | Wider spreads don't overcome costs |
| **UNGATED >> GATED** | Don't pull quotes, widen spreads instead |
| **Entropy threshold irrelevant** | 0.6/0.7/0.8 produce identical results |
| **Best config: spread=1, skew=0.3** | +5% return, 62% win rate, 0.3% drawdown |

### Bayesian Optimization (Optuna)

For smarter parameter search than grid search:

```bash
# Install Optuna
pip3 install optuna

# Run Bayesian optimization (50 trials, ~5 min)
python3 scripts/optimize.py --trials 50 --metric return

# Optimize for different metrics
python3 scripts/optimize.py --trials 100 --metric sharpe
python3 scripts/optimize.py --trials 100 --metric risk_adjusted

# Save results to custom file
python3 scripts/optimize.py --trials 50 --output my_results.json
```

**Why Bayesian over Grid Search?**
- Grid search tests ALL combinations (360 for our default grid)
- Bayesian learns from previous trials → focuses on promising regions
- 50 Bayesian trials often beats 360 grid search trials

**Output includes:**
- Best parameters found
- Parameter importance ranking
- All trial results (saved to JSON)

### Multi-Objective Optimization

Key objectives to balance:

1. **Sharpe Ratio**: Risk-adjusted returns
2. **Max Drawdown**: Worst peak-to-trough decline
3. **Fill Rate**: Percentage of quotes that get filled
4. **Inventory Turnover**: How quickly positions close

### Walk-Forward Validation

Critical to avoid overfitting:

```
|------ In-Sample ------|-- Out-of-Sample --|
|  Optimize parameters  |  Validate edge    |
        Fold 1

         |------ In-Sample ------|-- Out-of-Sample --|
                  Fold 2

                   |------ In-Sample ------|-- Out-of-Sample --|
                            Fold 3
```

Already implemented via `backtest walk-forward` command.

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

#### Phase 2: Backtesting Infrastructure (Weeks 5-8, ~40 hrs) ✅ COMPLETE
- [x] Historical data replay from Parquet
- [x] Fill simulation with realistic queue model (Cont et al., Moallemi & Yuan)
- [x] Slippage and latency modeling
- [x] Performance metrics suite (Sharpe, drawdown, fill rate)
- [x] Walk-forward validation framework
- [x] Data quality validation pipeline
- [x] Forward testing session logging
- [x] Backtest vs live comparison reports

#### Phase 3: Strategy Optimization (Weeks 9-14, ~60 hrs) 🔄 IN PROGRESS
- [x] Basic parameter sweep (spread × skew grid)
- [x] Extended grid search (entropy thresholds, fill params)
- [x] Entropy gate experiment (gated vs ungated) - **Result: UNGATED wins**
- [x] Bayesian optimization (Optuna integration)
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

## Development Workflow

The recommended workflow for developing and validating trading strategies:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  1. COLLECT     │────>│  2. OPTIMIZE    │────>│  3. VALIDATE    │────>│  4. COMPARE     │
│     DATA        │     │    PARAMETERS   │     │    (PAPER)      │     │    RESULTS      │
│                 │     │                 │     │                 │     │                 │
│  [0] Live       │     │  grid-search    │     │  [6] Paper      │     │  Backtest vs    │
│  Dashboard      │     │  or Optuna      │     │  Trade Preset   │     │  Paper Trade    │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Step 1: Collect Data
```bash
# Run overnight (use tmux or screen)
cargo run --release
# Press [0] for Live Dashboard
```

### Step 2: Optimize Parameters
```bash
# Grid search (comprehensive)
cargo run --release --bin backtest -- grid-search --test-gate

# Bayesian optimization (faster)
python3 scripts/optimize.py --trials 50 --metric return
```

### Step 3: Paper Trade with Preset
```bash
cargo run --release
# Press [6] to select a preset
# Select "GridSearch-Best" and run against live data
```

### Step 4: Compare Results
Compare backtest expectations with paper trading reality:
- **Fill rate**: Backtest assumption (e.g., 10%) vs actual observed fills
- **Sharpe ratio**: Expected vs realized
- **Win rate**: Backtest vs live
- Session results saved in `./data/sessions/`

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
