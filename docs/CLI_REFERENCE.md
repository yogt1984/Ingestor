# Ingestor CLI Reference

Command-line interface reference for the Ingestor platform. Three main CLI tools are available for research, backtesting, and validation workflows.

## Table of Contents

- [Quick Start](#quick-start)
- [validate](#validate-cli) - Validation Pipeline
- [backtest](#backtest-cli) - Backtesting Engine
- [research](#research-cli) - Research Analysis
- [Common Patterns](#common-patterns)
- [Configuration Files](#configuration-files)
- [Exit Codes](#exit-codes)

---

## Quick Start

```bash
# Build all CLI tools
cargo build --release

# Run research to analyze market data
cargo run --release --bin research -- run --data ./data/features

# Run a backtest to evaluate strategy
cargo run --release --bin backtest -- evaluate --data ./data/features

# Validate an algorithm through the pipeline
cargo run --release --bin validate -- --config ./configs/algo.json
```

---

## validate CLI

Run validation pipeline on algorithm configurations. Supports sequential stages from backtest through live deployment.

### Basic Usage

```bash
# Run full pipeline with config file
cargo run --release --bin validate -- --config ./configs/algo.json

# Run specific stages only
cargo run --release --bin validate -- --config ./configs/algo.json --stages backtest,forward,oos

# Start from a specific stage (partial run)
cargo run --release --bin validate -- --config ./configs/algo.json --from forward

# Generate config from research and validate
cargo run --release --bin validate -- --from-research ./research/ --stages backtest,forward
```

### Subcommands

| Command | Alias | Description |
|---------|-------|-------------|
| `run` | (default) | Run the validation pipeline |
| `presets` | - | List available pipeline presets |
| `stages` | - | List available validation stages |
| `status` | - | Show status of previous runs |
| `show <run_id>` | - | Show detailed info about a specific run |

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--config` | `-c` | - | Path to algorithm config file (JSON) |
| `--from-research` | - | - | Generate config from research state at path |
| `--stages` | `-s` | all | Comma-separated list of stages to run |
| `--from` | - | - | Start from this stage (for partial runs) |
| `--data` | `-d` | `./data/features` | Path to data directory with Parquet files |
| `--results` | `-r` | `./results` | Path to results directory for persistence |
| `--preset` | - | default | Runner preset to use |
| `--quiet` | `-q` | false | Quiet mode (minimal output) |
| `--json` | - | false | Output results as JSON |
| `--output` | `-o` | - | Save results to file |
| `--name` | - | `validate` | Run name prefix for identification |
| `--continue-on-failure` | - | false | Don't stop on first failed stage |
| `--no-persist` | - | false | Disable persistence (don't save results) |

### Validation Stages

| Stage | Description |
|-------|-------------|
| `backtest` | Historical replay validation |
| `forward` | Walk-forward validation (train/test windows) |
| `oos` | Out-of-sample holdout validation (default 20%) |
| `paper` | Paper trading validation (live data, simulated execution) |
| `live` | Live trading validation (real execution with OCO risk) |

### Pipeline Presets

| Preset | Description |
|--------|-------------|
| `default` | Standard configuration, stops on first failure |
| `production` | Conservative settings, strict thresholds, full audit |
| `research` | Relaxed settings, continues on failures, lower thresholds |
| `fast` | Quick validation, backtest only |

### Examples

```bash
# List available presets
cargo run --release --bin validate -- presets

# List available stages
cargo run --release --bin validate -- stages

# Show last 10 validation runs
cargo run --release --bin validate -- status --last 10

# Show details of a specific run
cargo run --release --bin validate -- show abc123-run-id

# Run with research preset (continues on failure)
cargo run --release --bin validate -- --config algo.json --preset research

# Run backtest and forward stages only
cargo run --release --bin validate -- --config algo.json --stages backtest,forward

# Output as JSON for CI/CD integration
cargo run --release --bin validate -- --config algo.json --json

# Save results to file
cargo run --release --bin validate -- --config algo.json -o results.json
```

### JSON Output Format

```json
{
  "success": true,
  "pipeline_id": "uuid",
  "algorithm_id": "algo-uuid",
  "status": "Passed",
  "stages_passed": 3,
  "stages_failed": 0,
  "stages_skipped": 2,
  "duration_seconds": 45.2,
  "timestamp": "2024-01-15T10:30:00Z",
  "stage_results": [
    {
      "stage": "Backtest",
      "outcome": "passed",
      "passed": true,
      "sharpe": 1.5,
      "max_drawdown": 0.08,
      "win_rate": 0.58,
      "trade_count": 1250
    }
  ],
  "recommendation": "Algorithm passed all validation stages. Ready for deployment."
}
```

---

## backtest CLI

Run backtests on historical market data with various modes including single evaluation, parameter sweeps, and walk-forward validation.

### Basic Usage

```bash
# Basic backtest with defaults
cargo run --release --bin backtest -- evaluate

# With custom parameters
cargo run --release --bin backtest -- evaluate \
    --data ./data/features \
    --spread 3.0 \
    --skew 0.7 \
    --output results.json
```

### Subcommands

| Command | Alias | Description |
|---------|-------|-------------|
| `evaluate` | `single` | Run a single backtest evaluation |
| `algorithms` | - | List available algorithms and parameters |
| `sweep` | - | Run parameter sweep |
| `tune` | `grid-search` | Hyperparameter optimization |
| `walk-forward` | `wf` | Time-series cross-validation |
| `train` | `train-ml` | ML weight training |
| `simulate` | `simulate-campaign` | 4-week validation campaign simulation |
| `validate-data` | `vd` | Data quality validation |

### Global Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--data` | `-d` | `./data/features` | Path to data directory with Parquet files |
| `--algorithm` | `-a` | `as` | Algorithm: `as` (Avellaneda-Stoikov), `ml`, `fixed` |
| `--weights-file` | - | - | Path to ML weights file (required for ML algo) |
| `--spread` | - | `2.0` | Base spread in basis points (per side) |
| `--skew` | - | `0.5` | Inventory skew factor |
| `--max-inventory` | - | `0.1` | Maximum inventory |
| `--quote-size` | - | `0.001` | Quote size |
| `--fee-rate` | - | `0.0001` | Fee rate (1 bps) |
| `--output` | `-o` | - | Output file for results (JSON) |
| `--quiet` | `-q` | false | Quiet mode (no progress output) |
| `--json` | - | false | Output results as JSON |
| `--stats` | - | false | Show statistical significance report |

### Fill Simulation Options

| Option | Default | Description |
|--------|---------|-------------|
| `--naive-fills` | false | Use naive fill simulation (for comparison) |
| `--fill-prob` | `0.10` | Fill probability (0.0-1.0) for realistic simulation |
| `--queue-pos` | `0.5` | Queue position (0.0=front, 1.0=back) |

### Regime-Based Options

| Option | Default | Description |
|--------|---------|-------------|
| `--regime-params` | false | Use regime-specific parameters |
| `--high-entropy` | `0.7` | High entropy threshold |
| `--low-entropy` | `0.4` | Low entropy threshold |
| `--high-spread` | `1.0` | High entropy spread (bps) |
| `--med-spread` | `2.5` | Medium entropy spread (bps) |
| `--low-spread` | `5.0` | Low entropy spread (bps) |
| `--high-skew` | `0.3` | High entropy skew |
| `--med-skew` | `0.5` | Medium entropy skew |
| `--low-skew` | `1.0` | Low entropy skew |
| `--quote-low-entropy` | false | Quote in low entropy regime |

### Examples

```bash
# List available algorithms
cargo run --release --bin backtest -- algorithms
cargo run --release --bin backtest -- algorithms --algo as --json

# Single evaluation with stats
cargo run --release --bin backtest -- evaluate --stats

# Parameter sweep
cargo run --release --bin backtest -- sweep --spreads 1,2,3,4,5 --skews 0.3,0.5,0.7

# Walk-forward validation with 5 folds
cargo run --release --bin backtest -- walk-forward --folds 5

# Hyperparameter tuning
cargo run --release --bin backtest -- tune

# ML training
cargo run --release --bin backtest -- train

# 4-week validation campaign
cargo run --release --bin backtest -- simulate

# Using regime-specific parameters
cargo run --release --bin backtest -- evaluate \
    --regime-params \
    --high-spread 1.0 --med-spread 2.5 --low-spread 5.0 \
    --high-skew 0.3 --med-skew 0.5 --low-skew 1.0

# Using ML algorithm with custom weights
cargo run --release --bin backtest -- evaluate \
    --algorithm ml \
    --weights-file ./models/weights.json

# JSON output for scripting/Optuna
cargo run --release --bin backtest -- evaluate --json
```

---

## research CLI

Run research analysis on historical market data to estimate market microstructure parameters and generate trading recommendations.

### Basic Usage

```bash
# Run research on historical data
cargo run --release --bin research -- run --data ./data/features --output ./research/

# Display current research status
cargo run --release --bin research -- status --store ./research/
```

### Subcommands

| Command | Alias | Description |
|---------|-------|-------------|
| `run` | `r` | Run research analysis on historical feature data |
| `status` | `s` | Display current research status |

### Run Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--data` | `-d` | `./data/features` | Path to data directory with Parquet files |
| `--output` | `-o` | `./research` | Path to output directory for research state |
| `--symbol` | `-s` | `BTCUSDT` | Trading symbol |
| `--start` | - | - | Start date for filtering (YYYY-MM-DD) |
| `--end` | - | - | End date for filtering (YYYY-MM-DD) |
| `--min-samples` | - | `100` | Minimum samples before engine is ready |
| `--checkpoint-interval` | - | `10000` | Samples between saves |
| `--resume` | - | false | Resume from previous state if available |
| `--quiet` | `-q` | false | Quiet mode (disable progress bar) |
| `--json` | - | false | Output results as JSON |

### Status Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--store` | `-s` | `./research` | Path to research store directory |
| `--symbol` | `-y` | `BTCUSDT` | Trading symbol to query |
| `--verbose` | `-v` | false | Show verbose output with all details |
| `--top-signals` | - | `5` | Number of top signals to display |
| `--json` | - | false | Output results as JSON |

### Examples

```bash
# Run research with date range filtering
cargo run --release --bin research -- run --data ./data/features \
    --start 2024-01-01 --end 2024-01-31 --output ./research/

# Run research for specific symbol
cargo run --release --bin research -- run --data ./data/features \
    --symbol ETHUSDT --output ./research/

# Resume from previous state
cargo run --release --bin research -- run --data ./data/features --resume

# Quiet mode
cargo run --release --bin research -- run --data ./data/features -q

# Display research status
cargo run --release --bin research -- status --store ./research/

# Status with JSON output
cargo run --release --bin research -- status --store ./research/ --json

# Verbose status with more signals
cargo run --release --bin research -- status --store ./research/ -v --top-signals 10
```

### Research Output

The research CLI produces:
- **MIDC estimate (kappa)** with interpretation
- **Persistence statistics** across regimes
- **Top conditional probability signals**
- **Tradeable assessment** with color-coded regime
- **Recommendation** for trading strategy

---

## Common Patterns

### Typical Workflow

```bash
# 1. Run research to analyze market characteristics
cargo run --release --bin research -- run --data ./data/features --output ./research/

# 2. Check research status
cargo run --release --bin research -- status --store ./research/

# 3. Run backtest with research-informed parameters
cargo run --release --bin backtest -- evaluate \
    --data ./data/features \
    --regime-params \
    --stats

# 4. Validate the algorithm configuration
cargo run --release --bin validate -- --from-research ./research/ --stages backtest,forward,oos
```

### CI/CD Integration

```bash
# Run validation with JSON output and non-zero exit on failure
cargo run --release --bin validate -- --config algo.json --json --quiet

# Exit codes:
# 0 = Pipeline passed
# 1 = Pipeline failed (algorithm didn't pass)
# 2 = Pipeline error (execution errors)
# 3 = Configuration/setup error
```

### Parameter Exploration

```bash
# Quick parameter sweep
cargo run --release --bin backtest -- sweep \
    --spreads 1,2,3,4,5 \
    --skews 0.3,0.5,0.7,0.9 \
    --json > sweep_results.json

# Walk-forward for robustness check
cargo run --release --bin backtest -- walk-forward --folds 10 --json
```

---

## Configuration Files

### Algorithm Config (JSON)

Used by `validate --config`:

```json
{
  "id": "unique-algo-id",
  "name": "My Algorithm",
  "strategy_type": "MarketMaking",
  "entry_params": {
    "min_confidence": 0.6,
    "min_edge": 0.001
  },
  "exit_params": {
    "take_profit_pct": 0.02,
    "stop_loss_pct": 0.01
  },
  "market_making_params": {
    "base_spread_bps": 2.0,
    "inventory_skew": 0.5,
    "max_inventory": 0.1
  },
  "position_params": {
    "max_position_size": 1.0,
    "sizing_method": "Fixed"
  },
  "regime_filters": {
    "min_entropy": 0.4,
    "max_volatility": 0.05
  }
}
```

### ML Weights File (JSON)

Used by `backtest --weights-file`:

```json
{
  "spread_weights": {
    "volatility": 0.3,
    "entropy": 0.2,
    "imbalance": 0.1,
    "inventory": 0.15
  },
  "skew_weights": {
    "inventory": 0.4,
    "imbalance": 0.3,
    "momentum": 0.2
  }
}
```

---

## Exit Codes

| Code | Command | Meaning |
|------|---------|---------|
| 0 | all | Success |
| 1 | validate | Pipeline failed (algorithm validation failed) |
| 2 | validate | Pipeline error (execution errors) |
| 3 | validate | Configuration/setup error |
| 1 | backtest | Execution error |
| 1 | research | Execution error |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level (e.g., `info`, `debug`, `warn`) |
| `INGESTOR_DATA_PATH` | Default data directory |

---

## See Also

- [REQUIREMENTS_V0.2.md](REQUIREMENTS_V0.2.md) - Full requirements specification
- [ALGORITHM_INTEGRATION_GUIDE.md](ALGORITHM_INTEGRATION_GUIDE.md) - Algorithm development guide
- [ARCHITECTURE.md](ARCHITECTURE.md) - System architecture overview
