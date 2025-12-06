# Ingestor Project Context

## Project Overview
Real-time market microstructure feature extraction and market making simulation platform for algorithmic trading research on Binance.

## Current Status: Phase 2 Complete, Phase 4 In Progress
- **Phase 1 COMPLETE**: Real-time ingestion, 60+ features, paper trading MM, TUI, Parquet persistence
- **Phase 2 COMPLETE**: Backtesting infrastructure (replay, harness, fill simulator, walk-forward validation)
- **Phase 4 IN PROGRESS**: ML-based spread/skew adaptation

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
- `src/algorithms/` - ML and traditional market making algorithms

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
cargo run --release --bin backtest -- grid-search
cargo run --release --bin backtest -- walk-forward-ml
```

## Recent Session: 2025-12-06

### What Was Done
1. Implemented MLSpreadSkewAlgorithm with linear model for dynamic spread/skew adjustment
2. Implemented walk-forward ML training with consensus weight optimization
3. Removed entropy gating feature (--entropy-gate, --test-gate flags) - provided no benefit

### ML Algorithm Features
- Linear model: spread = intercept + w_entropy*entropy + w_volatility*volatility
- Linear model: skew = intercept + w_inventory*inventory
- Walk-forward validation with Sharpe-weighted consensus weights
- Weight stability metrics across time folds

### Best Parameters (from grid search)
- Spread: 1.0 bps
- Skew: 0.3
- High Entropy Threshold: 0.7
- Fill Probability: 10%
- Expected Return: +5.14% over 47 days
- Win Rate: 59.5%
- Trades: 452

### Key Conclusions
1. Spread widening is preferred over quote pulling in low entropy regimes
2. Entropy threshold has minimal effect (0.6/0.7/0.8 produce similar results)
3. Only tight spreads (1 bps) are profitable
4. All Sharpe ratios negative with realistic fill model (expected with 10% fill assumption)

## Known Issues
1. Some Parquet files have missing `mid_price` values (now filtered)
2. Dependabot: 9 vulnerabilities (1 critical) - in technical debt backlog

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
