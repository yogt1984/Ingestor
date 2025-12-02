# Development Backlog

## Current Status: Phase 2 In Progress 🔄

**Completed**: Real-time feature extraction, paper trading MM, TUI, persistence, basic backtesting

---

## Session Notes: 2025-12-02

### What Was Done Today
1. **Wired up backtesting module** - Added `pub mod backtest` to lib.rs, added `clap` dependency
2. **Fixed FeaturesSnapshot struct mismatch** - Updated replay.rs to match current struct (timestamp as String, added missing fields)
3. **Fixed timestamp parsing** - Changed from `timestamp_ms: i64` to parsing RFC3339 strings with chrono
4. **Fixed critical data bug** - Events with `mid_price = None` were becoming 0, causing bogus fills. Added validation to skip invalid events.
5. **Fixed per-trade PnL calculation** - Was returning `None` for all trades, now properly calculates realized PnL on position closes.

### Current Backtest Results (with fixes)
```
Best Parameters (1bps spread, 0.3 skew):
- Sharpe: +0.44
- Return: +52% over 47 days
- Win Rate: 63%
- Max DD: 0.54%
- Trades: 2,113
```

### Known Issues / Next Steps
1. **Fill model too optimistic** - Current model assumes fill when mid-price crosses quote. Real MM needs queue position.
   - 52% return over 47 days is unrealistic
   - Investigate why tight spreads (1bps) show such high returns

2. **Data quality issues** - Some Parquet files have missing `mid_price` values (now filtered, but should fix at source)

3. **Debug logging to clean up** - Remove temporary `log::info!` statements in harness.rs

---

## Phase 2: Backtesting Infrastructure (In Progress)

### Priority 1: Replay Engine ✅ DONE
- [x] `src/backtest/replay.rs` - Read Parquet files and replay as time-ordered stream
- [x] `src/backtest/mod.rs` - Backtest harness
- [x] Time simulation (accelerated replay)
- [x] Event ordering (features + trades)

### Priority 2: Fill Simulation ⚠️ PARTIAL
- [ ] **Realistic queue-position model** ← NEXT PRIORITY
  - Current: Fill if mid crosses quote (too aggressive)
  - Need: Model queue position, partial fills, adverse selection
- [ ] Slippage estimation
- [ ] Latency modeling (configurable delay)

### Priority 3: Performance Metrics ✅ DONE
- [x] Sharpe ratio (annualized)
- [x] Maximum drawdown
- [x] Fill rate analysis
- [x] Inventory distribution
- [x] PnL attribution (spread capture vs directional)
- [x] Win rate, profit factor, Sortino, Calmar

### Priority 4: Validation Framework
- [ ] Walk-forward validation
- [ ] Out-of-sample testing
- [ ] Statistical significance (t-test on daily returns)

---

## Phase 3: Strategy Optimization

### Priority 1: Parameter Search
- [ ] Grid search for MM parameters
- [ ] Results persistence (JSON/Parquet)
- [ ] Visualization scripts (Python)

### Priority 2: Bayesian Optimization
- [ ] Optuna integration (Python side)
- [ ] Multi-objective (Sharpe vs drawdown)
- [ ] Early stopping for bad trials

### Priority 3: Regime-Specific Parameters
- [ ] Different configs per regime
- [ ] Automatic regime labeling from entropy
- [ ] Regime transition analysis

---

## Phase 4: Machine Learning Integration

### Stage A: Supervised Learning (Recommended First)

#### A1. Regime Classifier
```
Input:  60 features (from Parquet)
Output: P(HighEntropy), P(MediumEntropy), P(LowEntropy)
Labels: Derived from entropy thresholds (ground truth available)
Model:  MLP or XGBoost
```
- [ ] Python training script
- [ ] Model export (ONNX)
- [ ] Rust inference integration

#### A2. Fill Probability Predictor
```
Input:  Quote price, spread, volatility, queue depth, toxicity
Output: P(bid_fill_in_1s), P(ask_fill_in_1s)
Labels: From simulated/actual fills
Model:  Logistic regression → MLP
```
- [ ] Label generation from historical data
- [ ] Training pipeline
- [ ] Integration with MM decision

#### A3. Short-Term Direction Predictor
```
Input:  Sequence of last N feature snapshots
Output: P(up), P(down), P(flat) in next 1s/5s
Labels: From actual price movements
Model:  LSTM/GRU or Transformer
```
- [ ] Sequence dataset creation
- [ ] Model training
- [ ] Alpha signal integration

### Stage B: Reinforcement Learning

#### B1. Environment Setup
- [ ] Gymnasium environment wrapper
- [ ] State space: [features, inventory, unrealized_pnl, time_in_position]
- [ ] Action space: [spread_adjustment, skew_adjustment, size_adjustment]
- [ ] Reward: risk-adjusted PnL with inventory penalty

#### B2. Training Infrastructure
- [ ] PPO baseline (stable-baselines3)
- [ ] SAC for continuous actions
- [ ] Curriculum learning (easy → hard regimes)

#### B3. Evaluation
- [ ] Policy comparison vs rule-based
- [ ] Robustness testing (different periods)
- [ ] Interpretability analysis

---

## Phase 5: Live Trading Preparation

### Risk Management
- [ ] Kill switch (max loss per hour/day)
- [ ] Position limits (hard caps)
- [ ] Volatility-based sizing
- [ ] Regime-based exposure limits

### Order Management System
- [ ] Order state machine
- [ ] Retry logic with backoff
- [ ] Order tracking and reconciliation

### Monitoring
- [ ] Real-time PnL dashboard
- [ ] Alerting (Telegram/Discord)
- [ ] Performance logging

### Execution
- [ ] Paper → Live mode switching
- [ ] Gradual size scaling
- [ ] A/B testing infrastructure

---

## Phase 6: Production & Evolution

### Multi-Asset
- [ ] Symbol configuration
- [ ] Cross-asset correlation signals
- [ ] Portfolio-level risk management

### Strategy Ensemble
- [ ] Multiple strategy instances
- [ ] Meta-optimizer for allocation
- [ ] Regime-based strategy switching

### Continuous Improvement
- [ ] Automated retraining pipeline
- [ ] Performance attribution
- [ ] Drift detection

---

## Technical Debt

- [ ] Fix Dependabot vulnerabilities (9 issues)
- [ ] Add benchmarks for feature computation
- [ ] Improve error handling in WebSocket reconnection
- [ ] Add integration tests for full pipeline

---

## Code by Day / Test by Night Schedule

### Week 1: Backtesting Foundation
| Day | Task | Night Job |
|-----|------|-----------|
| Mon | Replay engine skeleton | Collect live data |
| Tue | Time-ordered event stream | Collect live data |
| Wed | Backtest harness | Run first backtest |
| Thu | Fill simulation | Sweep parameters |
| Fri | Performance metrics | Full backtest run |
| Sat | Analysis & debugging | Long backtest |
| Sun | Documentation | Collect weekend data |

### Week 2: Optimization
| Day | Task | Night Job |
|-----|------|-----------|
| Mon | Grid search implementation | Parameter sweep |
| Tue | Results persistence | Larger sweep |
| Wed | Optuna integration | Bayesian optimization |
| Thu | Multi-objective setup | Pareto frontier search |
| Fri | Regime-specific params | Regime analysis |
| Sat | Visualization scripts | Generate reports |
| Sun | Best params selection | Validation run |

### Week 3: Supervised ML
| Day | Task | Night Job |
|-----|------|-----------|
| Mon | Dataset preparation | Data export |
| Tue | Regime classifier | Training run |
| Wed | Fill predictor labels | Training run |
| Thu | Fill predictor model | Hyperparameter search |
| Fri | ONNX export | Validation |
| Sat | Rust integration | Integration test |
| Sun | A/B comparison | Performance analysis |

### Week 4: RL Foundation
| Day | Task | Night Job |
|-----|------|-----------|
| Mon | Gym environment | Environment test |
| Tue | State/action/reward | Sanity checks |
| Wed | PPO training setup | First training run |
| Thu | Training monitoring | Longer training |
| Fri | Policy evaluation | Comparison run |
| Sat | Debugging & tuning | Best hyperparams |
| Sun | Documentation | Final evaluation |

---

## Success Metrics

| Phase | Metric | Target |
|-------|--------|--------|
| Backtesting | Backtest vs paper trading gap | < 20% |
| Optimization | Best Sharpe found | > 1.5 |
| Supervised ML | Regime classification accuracy | > 70% |
| RL | RL vs rule-based improvement | > 10% |
| Paper Trading | 4-week Sharpe | > 1.0 |

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2024-12-02 | Start with backtesting, not RL | Need baseline validation first |
| 2024-12-02 | Supervised ML before RL | Clearer targets, easier debugging |
| 2024-12-02 | Rust for core, Python for ML | Performance where needed, ecosystem where helpful |
| 2025-12-02 | Skip events with missing mid_price | Data quality issue causing fills at price=0, corrupting backtest results |
| 2025-12-02 | Priority: Fix fill simulation realism | Current 52% returns are unrealistic, need queue-position model before trusting results |

---

## References for Implementation

### Backtesting
- Pardo, R. (2008). The Evaluation and Optimization of Trading Strategies
- López de Prado, M. (2018). Advances in Financial Machine Learning (walk-forward)

### Fill Simulation
- Cont, R. et al. (2014). The Price Impact of Order Book Events
- Moallemi, C. & Yuan, K. (2017). The value of queue position

### RL for Trading
- Spooner, T. et al. (2018). Market Making via Reinforcement Learning
- Kolm, P. & Ritter, G. (2019). Modern perspectives on RL in finance
