# Ingestor Project Context

## Project Overview
Real-time market microstructure feature extraction and market making simulation platform for algorithmic trading research on Binance.

## Current Status: Phase 2 In Progress
- **Phase 1 COMPLETE**: Real-time ingestion, 60+ features, paper trading MM, TUI, Parquet persistence
- **Phase 2 IN PROGRESS**: Backtesting infrastructure

## Key Architecture

```
Binance WebSocket → OrderBook/Trades → Feature Engines → FeaturesSnapshot
                                                              ↓
                                            MarketMakerEngine (Avellaneda-Stoikov)
                                                              ↓
                                            MMSimulator (Paper Trading)
                                                              ↓
                                            PersistenceEngine (Parquet)
```

## Important Files
- `src/main.rs` - Application orchestration
- `src/orderbook.rs` - Order book features (813 LOC)
- `src/entropy.rs` - Regime detection via tick entropy
- `src/market_maker.rs` - Avellaneda-Stoikov MM engine
- `src/mm_simulator.rs` - Paper trading simulator
- `src/persistence.rs` - Parquet I/O
- `src/backtest/` - Backtesting module (replay, harness, metrics)
- `src/bin/backtest.rs` - Backtest CLI tool

## Running the Project

```bash
# Main application (live data + TUI)
make
# or
cargo run --release

# Backtester
cargo run --release --bin backtest -- --data ./data/features
cargo run --release --bin backtest -- --data ./data/features info
cargo run --release --bin backtest -- sweep --spreads 1,2,3 --skews 0.3,0.5,0.7
```

## Recent Session: 2025-12-03

### What Was Done
1. Added entropy gate and threshold CLI params (`--entropy-gate`, `--high-entropy`)
2. Implemented extended `grid-search` command with `--test-gate` flag
3. Ran comprehensive comparison: 360 parameter combinations

### Grid Search Results (Entropy Gating Analysis)

**Test: UNGATED (widen spreads in low entropy) vs GATED (pull quotes in low entropy)**

| Metric | UNGATED (WIDE) | GATED |
|--------|----------------|-------|
| Avg Sharpe | **-34.09** | -213.76 |
| Avg Trades | **185.6** | 11.7 |
| Configs w/ trades | 180 | 84 |

**Best Parameters (Spread=1.0 bps, Skew=0.3, WIDE, FillProb=15%)**:
- Sharpe: -1.20
- Return: +5.14% over 47 days
- Win Rate: 59.5%
- Trades: 452

### Key Conclusions

1. **UNGATED (WIDE) is significantly better than GATED** - More trades, better Sharpe
2. **Entropy threshold has NO effect** - Results identical for 0.6/0.7/0.8
3. **GATE mode too restrictive** - Produces 0 trades for spreads ≥4 bps
4. **Only spread=1.0 bps profitable** - Wider spreads produce negative returns
5. **All Sharpe ratios negative** with realistic fill model (expected - see below)

### Realistic Fill Simulation Impact

With queue-position-based fill model (10% base fill probability):
- Naive fills: 894 fills, +6.14% return (overly optimistic)
- Realistic fills: 203 fills, -0.30% return (conservative)

The negative Sharpes with realistic fills indicate the current strategy parameters don't overcome transaction costs and adverse selection.

### Next Priority
- Consider removing entropy gating feature (no benefit found)
- Focus on tighter spreads (1 bps) with lower skew (0.3)
- Explore ML-based spread/skew adaptation (Phase 4)

## Known Issues
1. Entropy gating provides no measurable benefit
2. Some Parquet files have missing `mid_price` values (now filtered)
3. Dependabot: 9 vulnerabilities (1 critical) - in technical debt backlog

## Data Location
- Feature Parquet files: `./data/features/`
- ~97 files, ~47 days of data (Oct 16 - Dec 2, 2025)
- ~73k events total

## Coding Conventions
- Rust for core (performance critical)
- Python planned for ML (Phase 4)
- Use `rust_decimal` for price/quantity precision
- Async with Tokio
- Tests in same file or `tests/` directory

## References
See `README.md` for full academic references. Key papers:
- Avellaneda & Stoikov (2008) - Market making foundation
- Cont et al. (2014) - Price impact / queue position
