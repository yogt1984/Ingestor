# INGESTOR - Quick Reference Card

## Start Here

```bash
# 1. Collect data (run continuously)
cargo run --release
# Press [0] → Live Dashboard
# Leave running for 3+ months

# 2. Optimize parameters (after data collection)
cargo run --release --bin backtest -- grid-search

# 3. Paper trade (validate on live data)
cargo run --release
# Press [6] → Select best preset

# 4. Monitor performance
# Check ./data/sessions/*.json for results
```

---

## TUI MENU QUICK MAP

```
MAIN MENU (cargo run --release)
│
├── DATA COLLECTION
│   ├── [0] Live Dashboard → Save features to Parquet
│   ├── [1] Live + MM → Paper trade (default params)
│   └── [6] Paper Trade w/ Preset → Use optimized params
│
├── BACKTESTING
│   ├── [3] Run Backtest → Single test
│   ├── [4] Walk-Forward → Cross-validate
│   ├── [5] Data Quality → Validate files
│   └── [7] Campaign Sim → 4-week simulation
│
├── INFO
│   └── [2] Feature Descriptions → 60+ feature list
│
├── SETTINGS
│   ├── [p] Persist: ON/OFF
│   └── [s] Max Storage: Set GB limit
│
└── [q] Quit
```

---

## KEY STATISTICS (Current Best)

**GridSearch-Best (2025-12-03):**
- Spread: 1.0 bps
- Skew: 0.3
- Return: +5.14% (47 days)
- Win Rate: 59.5%
- Trades: 452
- Sharpe: -1.20

---

## ALGORITHMS AT A GLANCE

### Avellaneda-Stoikov (Classic)
- **When to use:** Robust, validated, understood
- **Speed:** Fast (real-time capable)
- **Tuning:** 3 main parameters (spread, skew, entropy threshold)

### ML Spread/Skew (Adaptive)
- **When to use:** Market changing, want adaptivity
- **Speed:** Fast (linear model)
- **Tuning:** 8 weights + training procedure
- **Best:** +3.2% return, 14 trades

---

## BACKTEST COMMANDS

```bash
# Single test with custom params
cargo run --release --bin backtest -- \
    --spread 1.0 --skew 0.3 --fill-prob 0.10

# Sweep spreads and skews
cargo run --release --bin backtest -- sweep \
    --spreads 1,2,3,4,5 --skews 0.3,0.5,0.7

# Comprehensive grid search
cargo run --release --bin backtest -- grid-search

# Time-series validation (prevent overfitting)
cargo run --release --bin backtest -- walk-forward

# Check data quality
cargo run --release --bin backtest -- validate

# Get data info
cargo run --release --bin backtest -- info
```

---

## FEATURES COLLECTED (60+)

**Core (24):**
- Order book: bid/ask, spread, imbalance, PWI, slopes, depth
- Trades: imbalance, VWAP, momentum, rate, sizes

**Entropy (14):**
- Tick entropy (7 windows: 1s, 5s, 10s, 15s, 30s, 1m, 15m)
- Volume entropy (7 windows)

**Volatility (5):**
- Realized vol, bipower vol, jump indicator, vol-of-vol

**Toxicity (7):**
- Flow toxicity, adverse selection, VPIN, arrival asymmetry

---

## DATA LOCATIONS

| What | Where | Format |
|------|-------|--------|
| Live data | ./data/features/*.parquet | Parquet (1000 rows, ~200KB each) |
| Presets | ./data/presets.json | JSON |
| Sessions | ./data/sessions/*.json | JSON with P&L details |

---

## PARAMETERS TO TUNE

**Market Making:**
- `--spread`: Base half-spread (bps). Optimal: 1.0
- `--skew`: Inventory adjustment. Optimal: 0.3
- `--high-entropy`: Threshold (0-1). Optimal: 0.7
- `--low-entropy`: Threshold (0-1). Optimal: 0.4
- `--fill-prob`: Fill assumption (0-1). Typical: 0.10

**Execution:**
- `--max-inventory`: Position limit BTC. Typical: 0.1
- `--quote-size`: Order size BTC. Typical: 0.001
- `--fee-rate`: Trading fee. Typical: 0.0001 (1 bps)

---

## EXPECTED OUTCOMES

### With 47 days of data:
- Return: 1-5% (depending on params)
- Sharpe: -2 to 0 (realistic fill model)
- Trades: 200-500
- Win Rate: 50-60%

### Profitable Indicators:
- Return > transaction costs (0.2% for 10% fill rate)
- Win rate > 52%
- Inventory mean-reverts daily

### Red Flags:
- Return < 0.1% (might be noise)
- Win rate < 50% (adverse selection winning)
- Max drawdown > 2% (too risky)

---

## VALIDATION CHECKLIST

Before going live, verify:
- [ ] 4+ weeks of paper trading data collected
- [ ] Paper fill rate within 2x of backtest assumption
- [ ] Sharpe ratio and return within 30% of backtest
- [ ] Win rate at least 50%
- [ ] Max drawdown < 1% of annual return
- [ ] No regime shifts in recent data

---

## TROUBLESHOOTING

| Problem | Solution |
|---------|----------|
| Data not saving | Check [p] is ON; verify disk space |
| Backtest slow | Reduce parameter ranges; use --quiet |
| Fill rate too low | Increase quote size; check spreads vs market |
| High drawdown | Reduce skew; lower inventory limit |
| Overfitting | Run walk-forward validation |
| Sharpe negative | Normal with realistic fills (focus on return) |

---

## FILE STRUCTURE

```
Ingestor/
├── src/
│   ├── main.rs              # Entry point
│   ├── tui.rs               # Terminal UI
│   ├── market_maker.rs      # Avellaneda-Stoikov algo
│   ├── mm_simulator.rs      # Paper trading engine
│   ├── presets.rs           # Parameter presets
│   ├── algorithms/
│   │   ├── mod.rs           # Algorithm exports
│   │   ├── traits.rs        # Algorithm trait
│   │   ├── avellaneda_stoikov.rs
│   │   └── ml_spread_skew.rs
│   ├── backtest/            # Backtesting CLI
│   │   └── mod.rs           # Replay, harness, metrics
│   ├── entropy.rs           # Regime detection
│   ├── volatility.rs        # Volatility metrics
│   ├── persistence.rs       # Parquet I/O
│   └── feature_fusion.rs    # Feature merging
│
├── data/
│   ├── features/            # Live parquet files
│   ├── presets.json         # Saved parameter sets
│   └── sessions/            # Paper trading results
│
├── Makefile                 # Build shortcut
├── USER_MANUAL.md           # Full documentation
└── README.md                # Overview & refs
```

---

## NEXT STEPS

### To Start Trading:
1. Run [0] for 3 months to collect data
2. Run grid-search to find best params
3. Run [6] paper trade for 4 weeks
4. Compare backtest vs paper
5. Start live with 0.01 BTC max inventory

### To Improve:
1. Try ML algorithm (more adaptive)
2. Tune fill probability based on paper trading
3. Add multi-symbol support
4. Implement RL-based policy

---

## CONTACT & RESOURCES

**Academic References:**
- Avellaneda & Stoikov (2008) - Market making foundation
- Cont et al. (2014) - Price impact / fills
- Easley et al. (2012) - Flow toxicity (VPIN)

**Project Status:**
- Phase 1: Data ingestion ✓
- Phase 2: Backtesting ✓
- Phase 3: Optimization ✓
- Phase 4: ML training (in progress)
- Phase 5: RL integration (planned)
