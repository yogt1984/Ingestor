# INGESTOR CODEBASE - COMPREHENSIVE CAPABILITIES AUDIT

## EXECUTIVE SUMMARY

The Ingestor system is a complete real-time market microstructure feature extraction and algorithmic trading backtesting platform. It includes:

- **60+ real-time market microstructure features** (order book, trades, entropy, volatility, toxicity, illiquidity)
- **Live TUI dashboard** with 8 modes
- **Multiple market-making algorithms** (Avellaneda-Stoikov, ML-based spread/skew prediction)
- **Comprehensive backtesting infrastructure** with 16 specialized modules
- **Paper trading with risk management** and real-time strategy validation
- **Advanced validation and optimization** tools for ML models and trading strategies

---

# PART 1: CLI COMMANDS (src/bin/backtest.rs)

## Main Commands (Subcommands)

### 1. **Single** (Default)
- Run a single backtest with given parameters
- Usage: `cargo run --release --bin backtest -- [OPTIONS]`
- Features:
  - JSON output mode (`--json`) for scripting/Optuna integration
  - Statistical significance reporting (`--stats`) with PSR/DSR/bootstrap CI
  - Regime-specific parameters (`--regime-params`)
  - Realistic fill simulation with queue position modeling

### 2. **Sweep**
- Parameter sweep across spread and skew values
- Command: `sweep --spreads <vals> --skews <vals>`
- Example: `--spreads 1,2,3 --skews 0.3,0.5,0.7`
- Outputs best parameters by Sharpe ratio

### 3. **Walk-Forward**
- Time-series cross-validation to detect overfitting
- Command: `walk-forward --folds <n> --test-hours <h> [--rolling]`
- Parameters:
  - `--folds`: Number of time-based splits (default: 5)
  - `--test-hours`: Test period per fold (default: 24)
  - `--rolling`: Use rolling window vs. anchored/expanding window

### 4. **Validate**
- Data quality validation before backtesting
- Command: `validate [--output <path>]`
- Checks:
  - Missing values
  - Data consistency
  - Time alignment
  - Feature distributions

### 5. **Info**
- Display data directory statistics
- Command: `info`
- Shows:
  - Number of events
  - Time range (start/end timestamps)
  - Duration (hours/days)
  - Event rate (events/second)

### 6. **GridSearch**
- Extended grid search over all key parameters
- Command: `grid-search [OPTIONS]`
- Parameters:
  - `--spreads`: Spread values (default: 1,2,3,4,5 bps)
  - `--skews`: Skew values (default: 0.3,0.5,0.7,1.0)
  - `--high-entropies`: Entropy thresholds (default: 0.6,0.7,0.8)
  - `--fill-probs`: Fill probability values (default: 0.05,0.10,0.15)
- Output: JSON with all results

### 7. **RegimeSearch**
- Regime-specific grid search (optimize per entropy regime)
- Command: `regime-search [OPTIONS]`
- Parameters:
  - `--high-spreads`: High entropy spreads (default: 0.5,1.0,1.5)
  - `--med-spreads`: Medium entropy spreads (default: 2.0,2.5,3.0)
  - `--low-spreads`: Low entropy spreads (default: 4.0,5.0,none)
  - `--high-skews`: High entropy skews (default: 0.2,0.3,0.4)
  - `--med-skews`: Medium entropy skews (default: 0.4,0.5,0.6)
  - `--low-skews`: Low entropy skews (default: 0.8,1.0,1.2)
  - `--fill-probs`: Fill probabilities

### 8. **OosValidate**
- Out-of-sample validation (hold-out test set)
- Command: `oos-validate [OPTIONS]`
- Parameters:
  - `--holdout`: Fraction reserved for test (default: 0.20)
  - `--embargo-hours`: Gap between train/test (default: 1.0)
  - `--spreads`, `--skews`, `--fill-probs`: Values to test
- Detects overfitting via out-of-sample performance

### 9. **MultiObjective**
- Pareto frontier optimization (multi-objective Sharpe/drawdown/fill/turnover)
- Command: `multi-objective [OPTIONS]`
- Optimizes:
  - Sharpe ratio (weight: `--w-sharpe`, default: 0.4)
  - Max drawdown (weight: `--w-drawdown`, default: 0.3)
  - Fill rate (weight: `--w-fill`, default: 0.2)
  - Turnover (weight: `--w-turnover`, default: 0.1)
- Outputs: Pareto-efficient parameter sets

### 10. **RegimeOptimize**
- Find best parameters independently per market regime
- Command: `regime-optimize [OPTIONS]`
- Parameters:
  - `--spreads`: Spreads to test (default: 0.5-5.0 bps)
  - `--skews`: Skews to test (default: 0.2-1.0)
  - `--allow-no-quote`: Allow no-quoting in low entropy (default: true)
  - `--min-trades`: Minimum trades for valid optimization (default: 10)

### 11. **TrainMl**
- Train ML weights using grid search optimization
- Command: `train-ml [OPTIONS]`
- Training parameters:
  - `--train-ratio`: Fraction for training (default: 0.7)
  - `--spread-intercepts`: Spread model intercepts (default: 1.0,2.0,3.0,4.0,5.0)
  - `--spread-entropy-weights`: Entropy coefficient (default: -3.0,-2.0,-1.0,0.0)
  - `--spread-vol-weights`: Volatility coefficient (default: 200.0,400.0,600.0)
  - `--skew-intercepts`: Skew model intercepts (default: 0.3,0.5,0.7)
  - `--skew-inv-weights`: Inventory coefficient (default: -1.0,-0.8,-0.6,-0.4)
- Outputs: Optimal model weights as JSON

### 12. **Compare**
- Compare different algorithms (ML vs Avellaneda-Stoikov)
- Command: `compare --algorithm <algo> [--weights <path>]`
- Options:
  - `--algorithm`: "ml" or "as" (avellaneda-stoikov)
  - `--weights`: Path to ML weights JSON file
- Outputs: Performance comparison metrics

### 13. **HeadToHead**
- Head-to-head ML vs Avellaneda-Stoikov on same data
- Command: `head-to-head [--weights <path>] [OPTIONS]`
- ML-specific: `--weights` for custom weights (uses defaults if omitted)
- A-S-specific: `--as-spread`, `--as-skew`
- Outputs: Comparative analysis

### 14. **WalkForwardMl**
- Walk-forward ML training (robust cross-validated weight optimization)
- Command: `walk-forward-ml [OPTIONS]`
- Parameters:
  - `--folds`: Number of folds (default: 5)
  - `--min-train-hours`: Minimum training period (default: 100)
  - `--test-hours`: Test period per fold (default: 24)
  - `--rolling`: Use rolling vs. expanding window
  - `--embargo-hours`: Train/test gap (default: 1.0)
  - ML parameter grids (spread/skew intercepts, weights)
- Outputs:
  - `--output`: Training results
  - `--weights-output`: Consensus weights across folds

### 15. **ValidateSession**
- Validate paper trading sessions against backtest expectations
- Command: `validate-session [OPTIONS]`
- Parameters:
  - `--session`: Specific session file (optional)
  - `--sessions-dir`: Directory containing session files (default: ./data/sessions)
  - `--min-hours`: Minimum session duration (default: 0.5)
  - `--min-trades`: Minimum trades required (default: 5)
- Outputs: Validation report against backtest performance

### 16. **SimulateSession**
- Simulate a paper trading session using historical data
- Command: `simulate-session [OPTIONS]`
- Parameters:
  - `--duration`: Session length in hours (default: 1.0)
  - `--preset`: Preset name to use (optional)
  - `--spread`, `--skew`: Manual parameters if no preset
  - `--sessions-dir`: Output directory (default: ./data/sessions)
- Outputs: Session result JSON with trades and PnL

### 17. **SimulateCampaign**
- Simulate 4-week validation campaign on historical data
- Command: `simulate-campaign [OPTIONS]`
- Campaign structure:
  - `--weeks`: Number of weeks (default: 4)
  - `--session-hours`: Hours per session (default: 8.0)
  - `--min-sessions-per-week`: Valid week threshold (default: 5)
- Expected performance (for comparison):
  - `--expected-fill-rate`: (default: 0.10)
  - `--expected-sharpe`: (default: 1.0)
  - `--expected-return`: (default: 0.05)
- Validation gates:
  - `--min-weekly-trades`: (default: 50)
  - `--max-drawdown-pct`: (default: 5.0)
  - `--min-win-rate`: (default: 0.40)
- Output: Campaign report with weekly/daily metrics and verdicts

---

## Global CLI Options (Apply to all commands)

### Data Parameters
- `--data`: Path to data directory (default: ./data/features)
- `--output`, `-o`: Output file for results (JSON format)
- `--quiet`, `-q`: Suppress progress output

### Market Making Parameters (Default values, can be overridden)
- `--spread`: Base spread in basis points (default: 2.0 bps)
- `--skew`: Inventory skew factor (default: 0.5)
- `--max-inventory`: Maximum inventory (default: 0.1)
- `--quote-size`: Order size per quote (default: 0.001)
- `--fee-rate`: Exchange fee rate (default: 0.0001 = 1 bps)

### Fill Simulation Parameters
- `--naive-fills`: Use naive fill simulation (flag, off by default)
- `--fill-prob`: Fill probability 0.0-1.0 (default: 0.10 = 10%)
- `--queue-pos`: Queue position 0.0-1.0 (default: 0.5 = middle)

### Entropy/Regime Parameters
- `--high-entropy`: High entropy threshold (default: 0.7)
- `--low-entropy`: Low entropy threshold (default: 0.4)
- `--regime-params`: Enable regime-specific parameters (flag)
- `--high-spread`: Spread in high entropy (default: 1.0 bps)
- `--med-spread`: Spread in medium entropy (default: 2.5 bps)
- `--low-spread`: Spread in low entropy (default: 5.0 bps)
- `--high-skew`: Skew in high entropy (default: 0.3)
- `--med-skew`: Skew in medium entropy (default: 0.5)
- `--low-skew`: Skew in low entropy (default: 1.0)
- `--quote-low-entropy`: Quote in low entropy regimes (flag)

### Output Parameters
- `--json`: Output results as JSON (for scripting)
- `--stats`: Show statistical significance report (PSR, DSR, bootstrap CI)

---

# PART 2: TUI MENU OPTIONS (src/tui.rs)

## Main Menu (AppMode::Menu)

### Data Collection Section
- **[0] Live Dashboard**
  - Stream features from Binance WebSocket
  - Save to ./data/features/*.parquet (time-indexed)
  - Real-time visualization of 60+ features
  - Key: `q` to return to menu

- **[1] Live + Market Maker**
  - Paper trading with default Avellaneda-Stoikov parameters
  - Real-time quote generation and fill simulation
  - Key: `q` to exit, `r` to reset session
  - Features: Trade logging to ./data/sessions/

- **[6] Paper Trade w/ Preset**
  - Paper trading using optimized presets (from grid searches)
  - Risk-managed execution with halt/emergency controls
  - Key: `q` to exit, `r` to reset, `h` to manual halt
  - Displays current risk action and inventory

### Backtesting Section
- **[3] Run Backtest**
  - Single backtest on collected data with current parameters
  - Display: Sharpe, return, max drawdown, trades, win rate
  - Key: `q` to return

- **[4] Walk-Forward Validation**
  - Time-series cross-validation on collected data
  - Detects overfitting via out-of-sample performance
  - Key: `q` to return

- **[5] Data Quality Check**
  - Validate data integrity before backtesting
  - Check for: missing values, gaps, distributions
  - Key: `q` to return

- **[7] Campaign Simulation**
  - Simulate 4-week validation campaign on historical data
  - Weekly summary with pass/fail gates
  - Compare against expected backtest performance
  - Key: `q` to return

### Information Section
- **[2] Feature Descriptions**
  - Browse all 60+ microstructure features with explanations
  - Scrollable: `Up/Down` or `k/j`, `PgUp/PgDn`
  - Key: `q` to return

### Settings Section
- **[p] Persist to Disk**
  - Toggle persistence: `ON` (saves features) / `OFF` (memory only)
  - Default: ON

- **[s] Max Storage**
  - Cycle through storage limits: 1GB → 5GB → 10GB → 50GB → 100GB → UNLIMITED
  - Each Parquet file ~200KB with 1000 rows (~100 seconds @ 10Hz)
  - Calculation: 200KB/file = ~5000 files per GB

### Exit
- **[q] or [Esc] Quit**
  - Gracefully shutdown all components
  - Save any active sessions

---

## Live Dashboard (AppMode::Live)
- **Display panels** (6 feature panels + sparklines):
  1. Order Book: BID/ASK, SPREAD, MID, MICRO, IMB, PWI, SLOPE, DEPTH, VOL
  2. Trades & Flow: LAST, VWAP (10/50/100/1K), MOM, RATE, FLOW
  3. Illiquidity: ROLL, AMIHUD, KYLE, VPIN, HASBROUCK
  4. Entropy: TICK (1s/5s/10s/30s/1m/15m), VOL_TICK
  5. Volatility: RV_100, RV_1K, BV, JUMP_IND, VOL_OF_VOL
  6. Toxicity: TOXIC_RATIO (micro/mid), ADV, ASYM, SIZE, IDX
  7. Sparklines: Microprice, PWI50, Entropy 1m, Volatility RV100 (60s history)
- Navigation: `Up/Down` or `k/j`, `PgUp/PgDn` (not scrollable, fixed panels)
- Key: `q` to return to menu

---

## Feature Descriptions (AppMode::Features)
- **Browsable list** of all 60+ features with meanings
- Includes sections:
  - Order Book (12 features)
  - Trades Log (12 features)
  - Illiquidity (5 features)
  - Entropy (13 features)
  - Volatility (5 features)
  - Toxicity (5 features)
  - Custom/Derived (8+ features)
- Navigation: `Up/Down` or `k/j`, `PgUp/PgDn`
- Key: `q` to return to menu

---

## Live Market Maker (AppMode::LiveMM)
- **Real-time paper trading** with default parameters
- Displays:
  - Current best bid/ask from order book
  - Generated quotes (bid/ask price and size)
  - Inventory (current position)
  - Market regime (High/Medium/Low entropy)
  - PnL tracker (realized, unrealized, fees)
  - Trade summary (count, fill rate)
  - Forward testing session info (trades logged)
- Updates: 1Hz (1 second intervals)
- Key: `q` to exit and save session, `r` to reset
- Trade logging: Automatic to ./data/sessions/

---

## Paper Trade with Preset (AppMode::PaperTradePreset)
- **Risk-managed paper trading** with selected preset
- Displays:
  - Preset name, algorithm type, creation date
  - Current quotes and inventory
  - **Risk manager status**: 
    - Risk action (OK, Reduce, Halt, Emergency)
    - Risk metrics (inventory %, daily drawdown %)
  - PnL (same as LiveMM)
  - Trade summary
- Controls:
  - `q` to exit and save session
  - `r` to reset (keeps risk manager)
  - `h` to manual halt toggle (emergency stop)
- Risk gates:
  - Daily losses exceed threshold → Reduce position size
  - Daily losses exceed critical → Halt (no new trades)
  - Emergency stop → Manual reset required

---

## Backtest Screen (AppMode::Backtest)
- **Running backtest display**
- Progress:
  - Current parameter set being tested
  - Completion percentage
  - Time elapsed / estimated remaining
- Results updated in real-time:
  - Sharpe ratio, return %, max drawdown %
  - Number of trades, win rate %
  - Best parameters so far
- Navigation: Scroll with `Up/Down` or `k/j`
- Key: `q` to return to menu (may interrupt backtest)

---

## Walk-Forward Validation Screen (AppMode::WalkForward)
- **Cross-validation progress**
- Display per fold:
  - Training period: X to Y (dates/times)
  - Test period: X to Y
  - In-sample Sharpe (training)
  - Out-of-sample Sharpe (test) - indicates overfitting if much worse
- Summary statistics:
  - Average IS Sharpe
  - Average OOS Sharpe
  - Degradation %
- Navigation: Scroll as needed
- Key: `q` to return to menu

---

## Data Quality Check Screen (AppMode::DataQuality)
- **Data validation results**
- Checks displayed:
  - Total events loaded
  - Missing values (% per column)
  - Time gaps (identified, duration)
  - Feature range validation (outliers)
  - Timestamp monotonicity
- Pass/Fail status per check
- Recommendation: "Safe to backtest" or "Fix these issues first"
- Navigation: Scroll as needed
- Key: `q` to return to menu

---

## Campaign Simulation Screen (AppMode::CampaignSimulation)
- **4-week campaign progress**
- Weekly breakdown:
  - Week 1-4 dates
  - Sessions completed/required
  - Trades count
  - Avg Sharpe ratio
  - Avg return %
  - Max drawdown %
  - **Pass/Fail verdict** for each gate:
    - Trades gate: `min_weekly_trades`
    - Sharpe gate: expected Sharpe vs. actual
    - Drawdown gate: `max_drawdown_pct`
    - Win rate gate: `min_win_rate`
- Summary:
  - Overall pass/fail (all gates must pass)
  - Ready to deploy? Yes/No
- Navigation: Scroll as needed
- Key: `q` to return to menu

---

# PART 3: ALGORITHM IMPLEMENTATIONS (src/algorithms/)

## Algorithm Types (Enum)

```rust
pub enum AlgorithmType {
    AvellanedaStoikov,   // Classic inventory-based MM
    MLSpreadSkew,        // ML-based spread/skew predictor
}
```

---

## 1. Avellaneda-Stoikov (2008)
- **Type**: `AvellanedaStoikov`
- **Purpose**: Classic inventory-based market making with optimal spread and skew
- **Source**: Classic 2008 paper by Avellaneda & Stoikov
- **Configuration**:
  ```
  max_inventory: Decimal
  quote_size: Decimal
  regime_params: RegimeParams (spread/skew per entropy regime)
  risk_aversion: f64
  ```
- **Features**:
  - Inventory-skewed spread: `spread += 2 * risk_aversion * skew_factor * inventory`
  - Regime-dependent execution (different params per entropy regime)
  - Exponential back-off on fills
- **Configurable per regime**:
  - High entropy: aggressive (tight spread, small skew)
  - Medium entropy: balanced
  - Low entropy: defensive (wide spread, large skew, optional no-quoting)

---

## 2. ML Spread-Skew Predictor
- **Type**: `MLSpreadSkew`
- **Purpose**: ML-based spread/skew adaptation using learned linear weights
- **Model Architecture**:
  ```
  spread_bps = w_spread_intercept 
             + w_entropy * (entropy_score - 0.5)
             + w_volatility * volatility
  
  skew_factor = w_skew_intercept 
              + w_inventory * (normalized_inventory)
  ```
- **Configuration**:
  ```rust
  pub struct MLSpreadSkewConfig {
      max_inventory: Decimal,
      quote_size: Decimal,
  }
  
  pub struct MLModelWeights {
      spread: SpreadWeights {
          intercept: f64,
          entropy_weight: f64,
          volatility_weight: f64,
      },
      skew: SkewWeights {
          intercept: f64,
          inventory_weight: f64,
      },
  }
  ```
- **Features**:
  - Linear regression-based (no neural networks, interpretable)
  - Entropy-aware: narrows spread in mean-reverting markets
  - Volatility-aware: widens spread in high volatility
  - Inventory-aware: skew adapts to position
  - Learned from historical data via grid search or walk-forward validation

---

## Trait: MarketMakingAlgorithm
All algorithms implement:
```rust
pub trait MarketMakingAlgorithm: Send + Sync {
    fn algorithm_type(&self) -> AlgorithmType;
    fn type_string(&self) -> &'static str;
    fn name(&self) -> &'static str;
    fn compute_quotes(
        &self,
        state: &MMState,
        mid_price: Decimal,
        volatility: f64,
        entropy_score: f64,
        flow_imbalance: f64,
        timestamp_ms: u64,
    ) -> MMQuotes;
    fn on_fill(&mut self, fill: &Fill, fee_rate: Decimal);
    fn reset(&mut self);
    fn get_state(&self) -> MMState;
}
```

---

## Entropy Score Computation (Shared Utility)
```rust
pub fn compute_entropy_score(
    tick_entropy_1s: Option<Decimal>,
    tick_entropy_5s: Option<Decimal>,
    tick_entropy_10s: Option<Decimal>,
) -> f64
```
- Combines 1s, 5s, 10s tick entropy readings
- Normalized to 0.0-1.0 (MAX_ENTROPY = log2(3) ≈ 1.585)
- Returns 0.5 if no data available

---

## Flow Imbalance Computation (Shared Utility)
```rust
pub fn compute_flow_imbalance(
    aggr_buy_vol: Decimal,
    aggr_sell_vol: Decimal,
) -> f64
```
- Computes (buy - sell) / (buy + sell)
- Range: -1.0 (all sells) to 1.0 (all buys)
- Used for order flow pressure assessment

---

# PART 4: BACKTEST MODULES & VALIDATION MODES (src/backtest/)

## Core Backtesting Infrastructure

### 1. **Replay** (ParquetReplay)
- Reads Parquet files from `./data/features/`
- Maintains time order across files
- Provides event stream for backtest harness
- Features:
  - Efficient memory-mapped I/O
  - Filtered by time range if needed
  - Type: `ReplayEvent` with timestamp, mid_price, features

### 2. **Fill Simulator** (FillSimulator)
- Two modes:
  1. **Naive**: Always fill at limit price (unrealistic)
  2. **Realistic** (default):
     - Queue position model: front (0.0) to back (1.0)
     - Fill probability: 0.0-1.0 (user-specified, default 10%)
     - Slippage based on queue position
     - Order rejection simulation
- Configuration:
  ```
  base_fill_probability: f64
  queue_position: f64
  fee_rate: Decimal
  ```

### 3. **Metrics** (PerformanceMetrics)
- Trade-level metrics:
  - PnL per trade
  - Fill rate (%)
  - Avg trade size
  - Trade duration
- Portfolio metrics:
  - Total return (%)
  - Sharpe ratio (annualized if daily, else per period)
  - Maximum drawdown (%)
  - Sortino ratio
  - Calmar ratio
  - Win rate (% of profitable trades)
  - Profit factor (gross wins / gross losses)
- Risk metrics:
  - Daily volatility
  - Consecutive loss runs
  - Largest loss

### 4. **Harness** (BacktestEngine)
- Main orchestration for single backtests
- Combines:
  - Parquet replay
  - Algorithm quote generation
  - Fill simulator
  - Metrics calculation
  - Trade logging
- Configuration: `BacktestConfig`

### 5. **Walk-Forward Validation** (WalkForwardEngine)
- Time-series cross-validation
- Modes:
  1. **Anchored**: Train on increasing window (t0-t1, t0-t2, ...)
  2. **Rolling**: Train on fixed-size window (t0-t1, t1-t2, ...)
- Prevents lookahead bias
- Detects overfitting: `IS_Sharpe >> OOS_Sharpe` indicates overfitting
- Output: `WalkForwardResults`
  - In-sample Sharpe per fold
  - Out-of-sample Sharpe per fold
  - Sharpe degradation %

### 6. **Data Quality Validator** (DataValidator)
- Checks before backtesting:
  - Non-null checks (% missing per feature)
  - Timestamp monotonicity
  - Time gaps (identifies disconnected periods)
  - Feature ranges (outlier detection)
  - Feature correlations (data sanity)
- Output: `DataQualityReport`

### 7. **Statistics** (compute_statistics)
- Statistical significance testing:
  - **PSR** (Probabilistic Sharpe Ratio): P(true Sharpe > benchmark)
  - **DSR** (Deflated Sharpe Ratio): Multiple testing correction
  - **Bootstrap CI**: Confidence intervals via resampling
- Configuration: `StatisticalReport`

### 8. **OOS Validation** (OOSValidator)
- Hold-out test set validation
- Process:
  1. Split: First N% training, last M% testing
  2. Embargo: Gap between train/test (default 1 hour)
  3. Train on training data
  4. Evaluate on test data
- Output: `ValidationReport`
  - Overfitting verdict: None, Mild, Moderate, Severe
  - IS vs OOS metrics comparison

### 9. **Multi-Objective Optimization** (MultiObjectiveOptimizer)
- Pareto frontier: Find non-dominated parameter sets
- Objectives (weighted):
  - Sharpe ratio (w=0.4 default)
  - Max drawdown (w=0.3 default, minimize)
  - Fill rate (w=0.2 default)
  - Turnover (w=0.1 default, minimize)
- Output: `MOResults`
  - Pareto frontier (non-dominated parameter sets)
  - Composite scores per parameter set

### 10. **Regime Optimizer** (RegimeOptimizer)
- Optimize parameters **per entropy regime** independently
- Splits data by entropy thresholds:
  - High entropy (> high_threshold): aggressive params
  - Medium entropy: balanced params
  - Low entropy (< low_threshold): defensive params
- Outputs best params for each regime
- Key finding: Low entropy regime is most challenging (needs wide spreads)

### 11. **ML Trainer** (MLTrainer)
- Grid search optimization for ML weights
- Trains spread model: `spread = intercept + w_entropy * entropy + w_vol * vol`
- Trains skew model: `skew = intercept + w_inventory * inventory`
- Output: `MLTrainingResults`
  - Optimal weights per model
  - Training info (data size, folds, grid size)

### 12. **Walk-Forward ML** (WalkForwardMLTrainer)
- Robust ML weight optimization with cross-validation
- Per fold:
  1. Train on training period
  2. Grid search optimal weights
  3. Test on test period
  4. Record IS and OOS Sharpe
- Consensus weights: Sharpe-weighted average across folds
- Weight stability metrics: How much weights vary across folds
- Output: `WalkForwardMLResults`
  - Consensus weights
  - Per-fold IS/OOS Sharpe
  - Weight stability score

### 13. **Paper Validation** (SessionValidator)
- Validates paper trading sessions against backtest
- Compares:
  - Expected Sharpe (from backtest) vs Actual (from session)
  - Expected return vs Actual
  - Expected fill rate vs Actual
  - Win rate
  - Max drawdown
- Verdict:
  - All expectations met → PASS (ready for wider deployment)
  - Some expectations missed → WARN (investigate)
  - Major discrepancies → FAIL (backtest unrealistic)

### 14. **Session Runner** (SessionRunner)
- Simulates a paper trading session on historical data
- Input:
  - Duration (hours)
  - Algorithm (preset or custom)
  - Start timestamp
- Output: `SessionResult`
  - Trades executed (timestamp, price, size, side)
  - PnL (realized and unrealized)
  - Fill rate
  - Duration
  - Sharpe/return/drawdown
- File: Auto-saved to `./data/sessions/<session_id>.json`

### 15. **Validation Campaign** (ValidationCampaign)
- Simulates multi-week (typically 4-week) validation campaign
- Structure:
  - Multiple weeks (default 4)
  - Multiple sessions per week (default 5x 8-hour sessions)
  - 20 simulated sessions total
- Validation gates (all must pass):
  1. **Trade gate**: `min_weekly_trades` per week (default 50)
  2. **Sharpe gate**: OOS Sharpe ≥ expected (default 1.0)
  3. **Drawdown gate**: Max drawdown ≤ expected (default 5%)
  4. **Win rate gate**: Win rate ≥ expected (default 40%)
- Output: `CampaignReport`
  - Weekly summary (trades, Sharpe, return, drawdown, win rate)
  - Daily session results
  - Pass/fail per week per gate
  - Overall verdict: "Ready to deploy" or "Needs tuning"

---

## Validation Modes Summary

| Mode | Purpose | Key Output | Use Case |
|------|---------|-----------|----------|
| **Single Backtest** | Test one parameter set | Sharpe, return, DD%, trades | Quick testing |
| **Sweep** | Test 2D parameter grid | Table of Sharpe by (spread, skew) | Param sensitivity |
| **GridSearch** | 4D param optimization | Best params by Sharpe | Comprehensive tuning |
| **RegimeSearch** | Optimize per entropy regime | Best params per regime | Regime-aware tuning |
| **Walk-Forward** | Time-series CV | IS/OOS Sharpe degradation | Detect overfitting |
| **OOS Validation** | Hold-out test set | Overfitting verdict | Validate generalization |
| **Multi-Objective** | Pareto frontier | Non-dominated param sets | Balance multiple goals |
| **RegimeOptimize** | Independent regime tuning | Best params per regime | Regime specialization |
| **TrainML** | Grid search for ML weights | Optimal weights | Learn from data |
| **WalkForwardML** | Cross-validated ML training | Consensus weights, stability | Robust ML tuning |
| **ValidateSession** | Compare session vs backtest | Pass/fail verdict | Real vs expected |
| **SimulateSession** | Single session on history | Session result | Test one scenario |
| **SimulateCampaign** | 4-week simulated campaign | Weekly gates, verdict | Pre-deployment check |

---

# PART 5: FORWARD TESTING MODULES (src/forward_testing/)

## Forward Testing Infrastructure

### 1. **A/B Testing** (ab_testing.rs)
- Compare two algorithms head-to-head
- Features:
  - Simultaneous execution
  - Statistical comparison (t-test, bootstrap CI)
  - Win rate analysis
  - Risk-adjusted return comparison
- Use case: Compare ML vs A-S in live/paper trading

### 2. **Drift Detection** (drift_detection.rs)
- Monitor for performance degradation
- Alerts when live performance diverges from backtest
- Metrics tracked:
  - Sharpe ratio
  - Win rate
  - Fill rate
  - Drawdown
- Triggers on: threshold breach (e.g., live Sharpe < backtest - 0.5)

### 3. **Regime Monitoring** (regime_monitor.rs)
- Track performance across different entropy regimes
- Alerts when regime-specific performance degrades
- Helps diagnose: "Is the problem in high/medium/low entropy?"

### 4. **Statistical Significance** (statistical.rs)
- T-tests for comparing two strategy variants
- Bootstrap confidence intervals
- Helps determine: "Is this difference real or random?"

---

# PART 6: ADDITIONAL CAPABILITIES NOT EXPOSED VIA CLI/TUI

## Core Market-Maker Components

### 1. **Market Maker Engine** (src/market_maker.rs)
- Avellaneda-Stoikov core implementation
- Public interface:
  - `compute_quotes()`: Generate bid/ask quotes
  - `process_fill()`: Update position after fill
  - `update_mark_to_market()`: Mark position to market
  - `reset()`: Clear state
- Internal: Inventory tracking, PnL computation, fee tracking

### 2. **Paper Trading Engine** (src/mm_simulator.rs)
- PaperTradingEngine: MM-only (uses MM engine + simulator)
- GenericPaperTradingEngine: Algorithm-agnostic (uses any MarketMakingAlgorithm)
- RiskManagedPaperTradingEngine: Paper trading with risk controls
- Features:
  - Real-time quote generation
  - Order fill simulation
  - PnL tracking
  - Trade logging

### 3. **Risk Manager** (src/risk_manager.rs)
- Available but not exposed via CLI
- Risk controls:
  - Daily loss limit (e.g., -$100 max loss)
  - Position size limit (e.g., max 1 BTC)
  - Leverage limits
  - Correlation checks (reduce if correlated positions)
- Actions:
  - OK: Normal trading
  - Reduce: Cut size to reduce exposure
  - Halt: No new trades (reduce only)
  - Emergency: Flat position immediately
- Used in TUI preset mode and SessionRunner

### 4. **Feature Engines** (src/*.rs)
- **OrderBook** (orderbook.rs): 813 LOC, extracts 12+ order book features
  - Spread, imbalance, PWI, slope, depth ratios, volume at levels
- **TradesLog** (tradeslog.rs): Trade flow features
  - VWAP, momentum, aggressor ratio, trade rate
- **Illiquidity** (illiquidity.rs): Market liquidity metrics
  - Roll spread, Amihud's lambda, Kyle's lambda, VPIN
- **Entropy** (entropy.rs): Tick entropy regime detection
  - 1s, 5s, 10s, 15s, 30s, 1m, 15m tick entropy
  - Volume-weighted tick entropy
- **Volatility** (volatility.rs): Volatility measures
  - Realized volatility (100, 1000 ticks)
  - Bipower variation, jump indicator, vol-of-vol
- **Toxicity** (toxicity.rs): Adverse selection metrics
  - Toxic flow ratio, adverse selection cost, asymmetry
- **FeatureFusion** (feature_fusion.rs): Combines all 60+ features into FeaturesSnapshot

### 5. **Persistence Engine** (src/persistence.rs)
- Parquet I/O
- Saves FeaturesSnapshot to `./data/features/<timestamp>.parquet`
- Supports configurable max files for disk space management
- Used by both live and backtesting modes

### 6. **Preset Store** (src/presets.rs)
- Save/load optimized parameter presets
- Stores:
  - Algorithm type (A-S or ML)
  - Parameters (spread, skew, or ML weights)
  - Creation timestamp
  - Performance metrics (Sharpe, return, DD%)
- File: `./data/presets.json`
- Used in TUI's paper trading mode

---

# PART 7: FEATURE SET (60+ Microstructure Features)

## Order Book Features (12)
1. best_bid, best_ask
2. mid_price, microprice
3. spread
4. imbalance
5. pwi_1, pwi_5, pwi_25, pwi_50 (Price-weighted imbalance at 1%, 5%, 25%, 50% levels)
6. bid_slope, ask_slope
7. volume_imbalance_top5
8. bid_depth_ratio, ask_depth_ratio
9. bid_volume_001, ask_volume_001 (volume within 0.01% of mid)

## Trades Features (12)
1. last_trade_price
2. trade_imbalance
3. vwap_total, vwap_10, vwap_50, vwap_100, vwap_1000
4. price_change
5. avg_trade_size
6. signed_count_momentum
7. trade_rate_10s
8. aggr_ratio_10, aggr_ratio_50, aggr_ratio_100, aggr_ratio_1000

## Illiquidity Features (5)
1. roll_spread
2. amihuds_lambda
3. kyles_lambda
4. hasbroucks_lambda
5. vpin

## Entropy Features (13)
1. tick_entropy_1s through 15m (7 values)
2. volume_tick_entropy_1s through 1m (7 values)

## Volatility Features (5)
1. realized_volatility_100
2. realized_volatility_1000
3. bipower_variation_100
4. jump_indicator
5. vol_of_vol

## Toxicity Features (5)
1. toxic_flow_ratio_micro
2. toxic_flow_ratio_mid
3. adverse_selection_micro
4. arrival_asymmetry
5. size_toxicity_ratio
6. toxicity_index (composite)

## Order Flow Features (2)
1. order_flow_imbalance
2. order_flow_pressure

---

# PART 8: DATA STRUCTURES & TYPES

## Core Algorithm Types
```rust
pub struct MMQuotes {
    pub bid: Option<Quote>,
    pub ask: Option<Quote>,
    pub regime: MarketRegime,
}

pub struct Quote {
    pub price: Decimal,
    pub size: Decimal,
}

pub struct Fill {
    pub side: QuoteSide,
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
}

pub struct MMState {
    pub inventory: Decimal,
    pub pnl: PnLTracker,
    pub last_fill: Option<(Decimal, Decimal)>,
}

pub enum MarketRegime {
    HighEntropy,      // Favorable for MM
    MediumEntropy,    // Balanced
    LowEntropy,       // Unfavorable (trending)
}
```

## Market Regime Definition
```rust
pub struct RegimeThresholds {
    pub high_entropy_threshold: f64,     // Default: 0.7
    pub low_entropy_threshold: f64,      // Default: 0.4
}

pub struct RegimeConfig {
    pub spread_bps: f64,
    pub skew_factor: f64,
    pub size_mult: f64,                  // Size multiplier vs quote_size
    pub should_quote: bool,              // Allow quotes in this regime
}

pub struct RegimeParams {
    pub high_entropy: RegimeConfig,
    pub medium_entropy: RegimeConfig,
    pub low_entropy: RegimeConfig,
}
```

---

# SUMMARY TABLE

| Component | Exposure | Type | Purpose |
|-----------|----------|------|---------|
| **Avellaneda-Stoikov** | CLI + TUI + Backtest | Algorithm | Classic MM baseline |
| **ML Spread-Skew** | CLI + TUI + Backtest | Algorithm | Learned MM variant |
| **Single Backtest** | CLI + TUI | Validation | Quick parameter test |
| **Sweep** | CLI | Validation | 2D parameter grid |
| **GridSearch** | CLI + TUI | Validation | 4D comprehensive tuning |
| **RegimeSearch** | CLI | Validation | Per-regime tuning |
| **Walk-Forward** | CLI + TUI | Validation | Overfitting detection |
| **OOS Validation** | CLI | Validation | Hold-out test |
| **Multi-Objective** | CLI | Validation | Pareto frontier |
| **RegimeOptimize** | CLI | Validation | Independent regime opt |
| **TrainML** | CLI | Validation | Grid search ML weights |
| **WalkForwardML** | CLI | Validation | Cross-validated ML tuning |
| **Campaign Simulation** | CLI + TUI | Validation | Pre-deployment test |
| **Session Validation** | CLI | Validation | Real vs expected check |
| **Paper Trading** | TUI | Trading | Live strategy testing |
| **Risk Management** | Code only | Risk | Position & loss limits |
| **A/B Testing** | Code only | Analysis | Algorithm comparison |
| **Drift Detection** | Code only | Monitoring | Performance tracking |
| **Regime Monitoring** | Code only | Monitoring | Regime-specific alerts |
| **60+ Features** | Live + Backtest | Data | Market microstructure |

---

# QUICK ACCESS REFERENCE

## Most Important CLI Commands for Research
```bash
# 1. Explore data
cargo run --release --bin backtest -- info

# 2. Quick test
cargo run --release --bin backtest -- --spread 2 --skew 0.5

# 3. Optimize parameters
cargo run --release --bin backtest -- grid-search

# 4. Check for overfitting
cargo run --release --bin backtest -- walk-forward

# 5. Train ML model
cargo run --release --bin backtest -- walk-forward-ml --folds 5

# 6. Validate on new data
cargo run --release --bin backtest -- simulate-campaign

# 7. Compare algorithms
cargo run --release --bin backtest -- head-to-head --weights weights.json
```

## Most Important TUI Modes for Testing
1. **[0] Live Dashboard** - Monitor live features
2. **[1] Live + Market Maker** - Paper trade with defaults
3. **[6] Paper Trade w/ Preset** - Paper trade with optimized preset
4. **[3] Run Backtest** - Quick backtest check
5. **[7] Campaign Simulation** - Pre-deployment validation

