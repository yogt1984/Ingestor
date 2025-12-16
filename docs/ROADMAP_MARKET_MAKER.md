# Market Maker Development Roadmap

## Overview

This document outlines the complete development roadmap for building a functional market making algorithm, from data ingestion through backtesting, forward testing, and eventually live trading with ML/RL integration.

---

## 1. Task Ordering: Data Ingestion → MM Development → Backtesting → Forward Testing

Given current state (Phase 1 complete, Phase 2 partial), here's the optimal sequence:

### Phase 2A: Complete Backtesting Foundation (1-2 weeks)

| Priority | Task | Rationale |
|----------|------|-----------|
| 1 | **Fix fill simulation realism** | Current 52% returns are fantasy. Without realistic fills, all downstream work is useless |
| 2 | **Add trade-level data to Parquet** | Currently only storing features, need actual trades for fill simulation |
| 3 | **Walk-forward validation** | Prevent overfitting before optimizing anything |
| 4 | **Data quality pipeline** | Fix missing `mid_price` at source, add validation |

### Phase 2B: Forward Testing Infrastructure (1 week)

| Priority | Task | Rationale |
|----------|------|-----------|
| 1 | **Paper trading metrics comparison** | Compare backtest predictions vs paper trading reality |
| 2 | **Live/Paper mode switching** | Same code path, different execution |
| 3 | **Execution quality logging** | Track slippage, fill rate, latency |

### Phase 3: Strategy Optimization (2 weeks)

| Priority | Task | Rationale |
|----------|------|-----------|
| 1 | **Regime-specific parameters** | Different spreads for high/medium/low entropy |
| 2 | **Multi-objective optimization** | Sharpe vs drawdown Pareto frontier |
| 3 | **Statistical significance testing** | Ensure results aren't luck |

### Phase 4: ML/RL Integration (4-8 weeks)

See section 3 below for detailed progression.

---

## 2. Menu Redesign

Current menu is basic. Here's the proposed structure:

```
┌─ INGESTOR MAIN MENU ─────────────────────────────────────────┐
│                                                               │
│  DATA & MONITORING                                            │
│  [0] Live Dashboard (features only)                           │
│  [1] Live Dashboard + Market Maker (paper trading)            │
│                                                               │
│  BACKTESTING                                                  │
│  [2] Run Backtest (single)                                    │
│  [3] Parameter Sweep                                          │
│  [4] Walk-Forward Validation                                  │
│                                                               │
│  STRATEGY COMPARISON                                          │
│  [5] Compare Strategies (side-by-side backtest)               │
│  [6] Strategy Tournament (run all, rank by Sharpe)            │
│                                                               │
│  CONFIGURATION                                                │
│  [7] MM Parameters                                            │
│  [8] Feature Descriptions                                     │
│                                                               │
│  SETTINGS                                                     │
│  [p] Persist features: ON                                     │
│  [s] Symbol: BTCUSDT                                          │
│  [m] Mode: Paper / Live                                       │
│                                                               │
│  [q] Quit                                                     │
└───────────────────────────────────────────────────────────────┘
```

### New Additions

- Backtest submenu (single, sweep, walk-forward)
- Strategy comparison tools
- Mode switching (paper/live)
- Symbol selection (future multi-asset)

---

## 3. RL Algorithm Progression: Simple → Sophisticated

### Tier 1: Tabular / Simple (Week 1-2)

**Start here to validate environment and reward design**

| Algorithm | Complexity | Description | When to Use |
|-----------|------------|-------------|-------------|
| **Q-Learning** | Very Low | Discretize state/action, lookup table | Baseline, debugging environment |
| **SARSA** | Very Low | On-policy variant of Q-learning | More stable, conservative |

**Implementation:**
- Discretize: inventory (5 bins), spread (3 bins), regime (3 states)
- Actions: widen/narrow spread, increase/decrease skew
- Reward: PnL - inventory_penalty

### Tier 2: Function Approximation (Week 2-3)

**Scale to continuous states**

| Algorithm | Complexity | Description | When to Use |
|-----------|------------|-------------|-------------|
| **DQN** | Medium | Neural network Q-function | First deep RL attempt |
| **Double DQN** | Medium | Reduce overestimation bias | More stable than DQN |
| **Dueling DQN** | Medium | Separate value/advantage streams | Better state evaluation |

**Implementation:**
- State: 10-20 key features (entropy, volatility, imbalance, inventory, PnL)
- Action: Discrete (e.g., 9 actions = 3 spreads × 3 skews)
- Network: 2-3 hidden layers, 64-128 units

### Tier 3: Policy Gradient (Week 3-5)

**Continuous actions, better exploration**

| Algorithm | Complexity | Description | When to Use |
|-----------|------------|-------------|-------------|
| **PPO** | Medium-High | Stable policy updates, clipped objective | **Recommended starting point for serious work** |
| **A2C/A3C** | Medium | Actor-critic, parallel environments | Faster training |
| **SAC** | High | Entropy-regularized, off-policy | Best sample efficiency, continuous actions |

**Implementation:**
- State: Full feature vector (60+ features) or learned embedding
- Action: Continuous [spread_mult, skew, size_mult]
- PPO hyperparams: clip=0.2, epochs=10, minibatch=64

### Tier 4: Advanced Deep RL (Week 5-8)

**Sophisticated architectures**

| Algorithm | Complexity | Description | When to Use |
|-----------|------------|-------------|-------------|
| **TD3** | High | Twin critics, delayed policy updates | Continuous control, less overestimation |
| **DDPG** | High | Deterministic policy gradient | When exploration is costly |
| **Distributional RL (C51, QR-DQN)** | Very High | Learn return distribution | Risk-sensitive trading |

### Tier 5: Sequence Models + Transfer Learning (Week 8+)

**State-of-the-art approaches**

| Approach | Description | Implementation |
|----------|-------------|----------------|
| **LSTM/GRU + RL** | Temporal state encoding | Replace MLP with recurrent encoder |
| **Transformer + RL** | Attention over history | Decision Transformer architecture |
| **Transfer Learning** | Pre-train on simulation, fine-tune on real | Train on multiple symbols, transfer to new |
| **Meta-RL (MAML)** | Learn to adapt quickly | Fast adaptation to regime changes |
| **World Models** | Learn environment dynamics | Dreamer-style, plan in latent space |

### Transfer Learning Strategy

1. Pre-train feature encoder on price prediction (self-supervised)
2. Freeze encoder, train RL head on MM task
3. Fine-tune end-to-end with smaller learning rate
4. Transfer across symbols (BTC→ETH) or exchanges

---

## 4. Testing Multiple MM Algorithms Simultaneously

### Conceptual Architecture

```
                    ┌─────────────────────────────────────────┐
                    │         STRATEGY ARENA                  │
                    │                                         │
  Live/Replay  ───►│  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
  Data Stream      │  │ MM v1   │ │ MM v2   │ │ RL v1   │   │
                    │  │ (tight) │ │ (wide)  │ │ (PPO)   │   │
                    │  └────┬────┘ └────┬────┘ └────┬────┘   │
                    │       │           │           │         │
                    │       ▼           ▼           ▼         │
                    │  ┌─────────────────────────────────────┐   │
                    │  │      SIMULATED EXECUTION        │   │
                    │  │   (same fill model for all)     │   │
                    │  └─────────────────────────────────┘   │
                    │                   │                     │
                    │                   ▼                     │
                    │  ┌─────────────────────────────────┐   │
                    │  │       METRICS AGGREGATOR        │   │
                    │  │  Sharpe, DD, Fill Rate, PnL     │   │
                    │  └─────────────────────────────────┘   │
                    │                   │                     │
                    └───────────────────┼─────────────────────┘
                                        ▼
                              ┌──────────────────┐
                              │   LEADERBOARD    │
                              │ Rank by Sharpe   │
                              │ Statistical sig  │
                              └──────────────────┘
```

### Key Principles

#### 1. Same Data, Same Fill Model

- All strategies see identical feature stream
- Same fill simulation rules (no strategy gets unfair advantage)
- Differences are purely algorithmic

#### 2. Independent State

- Each strategy has its own inventory, PnL tracker
- No cross-contamination
- Can run in parallel (no shared mutable state)

#### 3. Strategy Registry Pattern

```rust
trait MMStrategy {
    fn compute_quotes(&mut self, features: &FeaturesSnapshot) -> MMQuotes;
    fn on_fill(&mut self, fill: Fill);
    fn name(&self) -> &str;
}
```

- Implement trait for each strategy variant
- Arena loops over all registered strategies

#### 4. Tournament Modes

- **Head-to-head**: Compare 2 strategies on same period
- **Round-robin**: All pairs, aggregate win/loss
- **Time-series CV**: Walk-forward on multiple folds, rank consistency
- **Bootstrap**: Resample returns, confidence intervals on Sharpe difference

#### 5. Statistical Rigor

- Paired t-test on daily returns
- Bootstrap confidence intervals
- Multiple comparison correction (Bonferroni/Holm)
- Minimum 1000 trades for significance

---

## 5. Recommended Roadmap for Finding a Functional Algorithm

### Month 1: Foundation

```
Week 1: Fix fill simulation (queue model, slippage)
Week 2: Walk-forward validation framework
Week 3: Baseline rule-based strategies (3-5 variants)
Week 4: Strategy arena, statistical comparison
```

### Month 2: Optimization

```
Week 1: Grid search optimal rule-based parameters
Week 2: Regime-specific parameter sets
Week 3: Ensemble of rule-based strategies
Week 4: Paper trading validation (2+ weeks running)
```

### Month 3: Simple ML

```
Week 1: Regime classifier (supervised, XGBoost)
Week 2: Fill probability predictor
Week 3: Integrate predictions into MM
Week 4: Compare ML-enhanced vs pure rule-based
```

### Month 4: RL

```
Week 1: Gym environment, PPO baseline
Week 2: Reward shaping experiments
Week 3: Compare RL vs best rule-based
Week 4: Curriculum learning (easy→hard regimes)
```

### Month 5-6: Refinement

```
- Hyperparameter tuning
- Robustness testing (different periods, volatility regimes)
- Paper trading extended validation
- Gradual live deployment (0.1x → 0.5x → 1x size)
```

---

## 6. Success Probability Maximizers

1. **Fix fill simulation FIRST** - Everything else is garbage without this
2. **Rule-based baseline before ML** - Often 80% of the edge
3. **Regime-aware always** - Different markets need different strategies
4. **Statistical significance** - Don't fool yourself with lucky runs
5. **Paper trading validation** - Backtest ≠ reality
6. **Start small live** - 0.1x size for first 4 weeks

---

## 7. Timeline Summary

| Phase | Duration | Key Deliverable |
|-------|----------|-----------------|
| 2A: Backtesting | 2 weeks | Realistic fill simulation, walk-forward CV |
| 2B: Forward Testing | 1 week | Paper trading comparison infrastructure |
| 3: Optimization | 2 weeks | Best rule-based parameters per regime |
| 4: ML/RL | 4-8 weeks | PPO agent beating rule-based by 10%+ |
| 5: Production | 4 weeks | Live trading at 0.1x size |

**Total: ~4-6 months to production-ready system**

---

## 8. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Overfitting to backtest | Walk-forward validation, out-of-sample testing |
| Unrealistic fill assumptions | Queue position model, slippage estimation |
| Regime change | Adaptive parameters, regime detection |
| Black swan events | Position limits, kill switches, max drawdown stops |
| Code bugs | Extensive testing, paper trading validation |

---

## References

### Backtesting
- Pardo, R. (2008). The Evaluation and Optimization of Trading Strategies
- López de Prado, M. (2018). Advances in Financial Machine Learning

### Fill Simulation
- Cont, R. et al. (2014). The Price Impact of Order Book Events
- Moallemi, C. & Yuan, K. (2017). The Value of Queue Position

### RL for Trading
- Spooner, T. et al. (2018). Market Making via Reinforcement Learning
- Kolm, P. & Ritter, G. (2019). Modern Perspectives on RL in Finance

### Market Making
- Avellaneda, M. & Stoikov, S. (2008). High-Frequency Trading in a Limit Order Book
- Guéant, O. (2017). Optimal Market Making
