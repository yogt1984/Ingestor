# INGESTOR Documentation Index

This project includes comprehensive documentation for users and developers.

## User-Facing Documentation

### 1. USER_MANUAL.md (Complete Reference)
**Purpose:** Comprehensive user guide covering all features
**Length:** 27 KB, 764 lines
**Contents:**
- TUI Menu Options & Key Bindings (all 13 menu items)
- Available Algorithms (Avellaneda-Stoikov, ML Spread/Skew)
- Preset System (4 built-in presets, how to create custom)
- CLI Commands for Backtesting (9 command types with examples)
- Data Flow Architecture (pipeline diagrams and stages)
- Feature Overview (60+ computed microstructure features)
- Workflow: Data → Optimize → Trade → Validate
- Troubleshooting guide

**Start here if:** You want to understand everything about the platform

### 2. QUICK_REFERENCE.md (Cheatsheet)
**Purpose:** Quick lookup guide for common tasks
**Length:** 6.8 KB, 257 lines
**Contents:**
- Quick Start (4 steps)
- TUI Menu Quick Map
- Key Statistics (best observed performance)
- Algorithm Comparison (A-S vs ML)
- Backtest Command Cheatsheet
- Feature Summary (60+ features organized by category)
- Parameter Tuning Guide
- Expected Outcomes
- Validation Checklist
- Troubleshooting Quick Fixes
- File Structure

**Start here if:** You need fast lookup while working

## Developer-Facing Documentation

### 3. README.md (Project Overview)
**Contents:**
- System architecture and data flow
- Algorithm theory (Avellaneda-Stoikov foundation)
- Implementation extensions (entropy, toxicity, queue modeling)
- Degrees of freedom and parameters
- Entropy gate concept
- Grid search results and methodology
- Development roadmap (phases 1-6)
- Required reading (academic references)
- Configuration details

**Start here if:** You want to understand the theory and architecture

### 4. CLAUDE.md (Project Context)
**Contents:**
- Project status (Phase 2 complete, Phase 4 in progress)
- Architecture overview
- Important files and their purposes
- Running the project commands
- Recent session notes
- Known issues
- Coding conventions

**Start here if:** You're contributing to the codebase

### 5. ALGORITHM_INTEGRATION_GUIDE.md
**Contents:**
- How to integrate new algorithms
- Trait-based architecture design
- Implementation steps
- Testing requirements

**Start here if:** You want to add new trading strategies

### 6. LIVE_TRADING_ARCHITECTURE.md
**Contents:**
- Transition from backtesting to live trading
- Risk management layers
- Order management system
- Execution quality monitoring
- Live trading considerations

**Start here if:** You want to go live with real trading

### 7. PAPER_TRADING_ANALYSIS.md & PAPER_TRADING_INDEX.md
**Contents:**
- Session analysis and validation
- Paper trading results
- Comparing paper vs backtest expectations

**Start here if:** You want to analyze paper trading results

### 8. RESEARCH_EXECUTIVE_SUMMARY.md & RESEARCH_INDEX.md
**Contents:**
- Research findings and conclusions
- Algorithm comparison results
- Optimization methodology

**Start here if:** You want research conclusions

### 9. REPORT_03_12_25.md
**Contents:**
- Grid search detailed results (360 combinations tested)
- Top 10 parameter sets
- Performance metrics by configuration
- Key findings and recommendations

**Start here if:** You want to see specific optimization results

### 10. ROADMAP_MARKET_MAKER.md
**Contents:**
- Development phases and timeline
- Feature implementation status
- Technical debt items
- Future extensions

**Start here if:** You want to understand future directions

## Source Code Reference

Key source files by functionality:

### Core Trading Logic
- `src/main.rs` - Application orchestration
- `src/market_maker.rs` - Avellaneda-Stoikov implementation
- `src/algorithms/` - Trait-based algorithm system
  - `traits.rs` - Algorithm interface
  - `avellaneda_stoikov.rs` - Wrapper for A-S
  - `ml_spread_skew.rs` - ML algorithm
  - `mod.rs` - Factory functions

### Data Collection
- `src/lob_feed_manager.rs` - Binance order book stream
- `src/log_feed_manager.rs` - Binance trade stream
- `src/orderbook.rs` - Order book processing (813 LOC)
- `src/tradeslog.rs` - Trade stream processing

### Feature Computation
- `src/feature_fusion.rs` - Merge all metrics (FeaturesSnapshot)
- `src/entropy.rs` - Entropy-based regime detection
- `src/volatility.rs` - Volatility metrics (RV, BV, jumps)
- `src/illiquidity.rs` - Liquidity metrics (spreads, lambdas, VPIN)
- `src/toxicity.rs` - Adverse selection metrics

### Simulation & Validation
- `src/mm_simulator.rs` - Paper trading engine
- `src/risk_manager.rs` - Risk controls and limits
- `src/persistence.rs` - Parquet file I/O
- `src/presets.rs` - Parameter preset system

### User Interface
- `src/tui.rs` - Terminal UI (2531 LOC)
- Full menu system, modes, and displays

### Backtesting
- `src/backtest/` - Complete backtesting module
  - Replay engine (chronological replay)
  - Harness (test running)
  - Metrics computation
  - Walk-forward validation
  - ML training

### CLI Tools
- `src/bin/backtest.rs` - Comprehensive backtest CLI
  - Single tests, sweeps, grid search
  - Walk-forward validation
  - Multi-objective optimization

## Quick Navigation

### I want to...

**Understand what the platform does:**
→ README.md (Overview section)

**Learn all TUI menu options:**
→ USER_MANUAL.md (TUI Menu Options)

**Compare the two algorithms:**
→ QUICK_REFERENCE.md (Algorithms at a Glance)

**Run backtests:**
→ USER_MANUAL.md (CLI Commands) or QUICK_REFERENCE.md (Backtest Commands)

**Create a custom preset:**
→ USER_MANUAL.md (Preset System - Creating Custom Presets)

**Understand the code architecture:**
→ README.md (System Architecture) or CLAUDE.md

**Add a new algorithm:**
→ ALGORITHM_INTEGRATION_GUIDE.md

**Go live with trading:**
→ LIVE_TRADING_ARCHITECTURE.md + USER_MANUAL.md (Validation Checklist)

**See optimization results:**
→ REPORT_03_12_25.md or QUICK_REFERENCE.md (Key Statistics)

**Validate paper trading results:**
→ PAPER_TRADING_ANALYSIS.md

## Data Locations

- **Live feature data:** `./data/features/*.parquet`
- **Parameter presets:** `./data/presets.json`
- **Paper trading sessions:** `./data/sessions/*.json`
- **Backtest results:** Saved to specified `--output` file

## Building & Running

```bash
# Build and run main application
cargo build --release
cargo run --release

# Run backtesting CLI
cargo run --release --bin backtest -- [COMMAND] [OPTIONS]

# Run tests
cargo test
```

## Key Statistics (Current Best Results)

**GridSearch-Best (December 3, 2025):**
- Algorithm: Avellaneda-Stoikov
- Spread: 1.0 bps
- Skew: 0.3
- Expected Return: +5.14% over 47 days
- Win Rate: 59.5% (238 wins / 452 trades)
- Sharpe Ratio: -1.20
- Max Drawdown: 0.43%
- Data: Oct 16 - Dec 2, 2025 (73k events)

**ML-Trained (December 6, 2025):**
- Algorithm: ML Spread/Skew Predictor
- Method: Walk-forward ML training
- Expected Return: +3.2%
- Trades: 14 (more selective)
- Training Sharpe: -1.49

## Academic References

Core papers (see README.md for full list):
- Avellaneda & Stoikov (2008) - High-frequency trading in a limit order book
- Cont et al. (2014) - Price impact of order book events
- Moallemi & Yuan (2017) - Queue position in LOB
- Easley et al. (2012) - Flow toxicity (VPIN)
- Barndorff-Nielsen & Shephard (2004) - Bipower variation

## Document Maintenance

- **USER_MANUAL.md** - Updated when UI/algorithm features change
- **QUICK_REFERENCE.md** - Updated when commands change
- **RESEARCH_*.md** - Updated after new optimization runs
- **LIVE_TRADING_*.md** - Updated as live infrastructure develops
- **README.md** - Main reference, updated with major changes

---

Last Updated: December 8, 2025
Total Lines of Documentation: 3000+
Documentation to Code Ratio: ~1:3 (balanced for reference)
