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

## Recent Session: 2025-12-02

### What Was Done
1. Wired up backtesting module (was written but not exposed in lib.rs)
2. Fixed FeaturesSnapshot struct mismatch in replay.rs
3. Fixed timestamp parsing (RFC3339 strings from Parquet)
4. Fixed critical bug: events with `mid_price = None` were becoming 0, causing bogus fills
5. Fixed per-trade PnL calculation

### Current Backtest Results
```
Best Parameters (1bps spread, 0.3 skew):
- Sharpe: +0.44
- Return: +52% over 47 days (unrealistic - fill model too optimistic)
- Win Rate: 63%
- Trades: 2,113
```

### Next Priority
**Fix fill simulation realism** - Current model assumes fill when mid-price crosses quote. Need:
- Queue position modeling
- Partial fills
- Adverse selection modeling

## Known Issues
1. Fill model too optimistic (52% return is unrealistic)
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
