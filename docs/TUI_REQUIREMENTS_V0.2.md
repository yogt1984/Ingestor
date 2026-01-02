# TUI Requirements Document

## TUI-CLI Convergence Roadmap

**Version:** 1.0
**Date:** 2025-12-29
**Status:** Planning

---

## 1. Executive Summary

This document defines the requirements for achieving full feature parity between the Command Line Interface (CLI) and Terminal User Interface (TUI) of the Ingestor trading system.

### Current State
- **CLI Commands:** 22 subcommands across 4 binaries
- **TUI Coverage:** 15 commands accessible (68%)
- **Gap:** 7 CLI commands not accessible via TUI

### Goal
Achieve 100% CLI-TUI parity with enhanced interactive features, enabling users to perform all operations through either interface.

---

## 2. Current Coverage Matrix

### 2.1 Binary: `backtest`

| Subcommand | CLI | TUI | Status |
|------------|-----|-----|--------|
| `evaluate` | ✅ | ✅ | Complete |
| `sweep` | ✅ | ✅ | Complete |
| `walk-forward` | ✅ | ✅ | Complete |
| `tune` | ✅ | ❌ | **Gap** |
| `regime-search` | ✅ | ❌ | **Gap** |
| `oos-validate` | ✅ | ✅ | Complete |
| `multi-objective` | ✅ | ❌ | **Gap** |
| `regime-optimize` | ✅ | ❌ | **Gap** |
| `train` | ✅ | ❌ | **Gap** |
| `simulate` | ✅ | ❌ | **Gap** |
| `grid` | ✅ | ✅ | Complete |
| `campaign` | ✅ | ✅ | Complete |
| `paper` | ✅ | ✅ | Complete |

### 2.2 Binary: `research`

| Subcommand | CLI | TUI | Status |
|------------|-----|-----|--------|
| `run` | ✅ | ✅ | Complete |
| `status` | ✅ | ✅ | Complete |

### 2.3 Binary: `validate`

| Subcommand | CLI | TUI | Status |
|------------|-----|-----|--------|
| `run` | ✅ | ✅ | Complete |
| `presets` | ✅ | ✅ | Complete |
| `stages` | ✅ | ❌ | **Gap** |
| `status` | ✅ | ❌ | **Gap** (info only) |
| `show` | ✅ | ❌ | **Gap** (info only) |

### 2.4 Binary: `algorithm`

| Subcommand | CLI | TUI | Status |
|------------|-----|-----|--------|
| `create` | ✅ | ✅ | Complete |
| `list` | ✅ | ✅ | Complete |
| `show` | ✅ | ✅ | Complete |

---

## 3. Requirements by Phase

### Phase 1: Wire Existing Placeholders (TUI-9.x)

**Priority:** High
**Effort:** Low
**Sprint:** 1

These menu items exist but are not yet connected to functionality.

#### TUI-9.1: Data Live Stream
- **Menu Path:** Data → Live Stream
- **Description:** Display real-time market data stream
- **Acceptance Criteria:**
  - [ ] Shows live price updates for configured symbols
  - [ ] Displays bid/ask spread
  - [ ] Shows volume information
  - [ ] Auto-refreshes at configurable interval
- **CLI Equivalent:** N/A (TUI-only feature)

#### TUI-9.2: Data Features
- **Menu Path:** Data → Features
- **Description:** Display available features/indicators
- **Acceptance Criteria:**
  - [ ] Lists all computed features
  - [ ] Shows feature descriptions
  - [ ] Displays feature dependencies
  - [ ] Indicates feature computation status
- **CLI Equivalent:** N/A (TUI-only feature)

#### TUI-9.3: Data Info
- **Menu Path:** Data → Info
- **Description:** Show data source information
- **Acceptance Criteria:**
  - [ ] Displays configured data sources
  - [ ] Shows date range of available data
  - [ ] Indicates data freshness
  - [ ] Shows symbol coverage
- **CLI Equivalent:** N/A (TUI-only feature)

#### TUI-9.4: Data Quality
- **Menu Path:** Data → Quality
- **Description:** Display data quality metrics
- **Acceptance Criteria:**
  - [ ] Shows missing data percentage
  - [ ] Displays gap analysis
  - [ ] Indicates data anomalies
  - [ ] Provides quality score per symbol
- **CLI Equivalent:** N/A (TUI-only feature)

#### TUI-9.5: Trade Live
- **Menu Path:** Trade → Live
- **Description:** Execute live trading
- **Acceptance Criteria:**
  - [ ] Requires explicit confirmation before enabling
  - [ ] Shows current algorithm selection
  - [ ] Displays risk parameters
  - [ ] Provides emergency stop button
  - [ ] Shows real-time P&L
- **CLI Equivalent:** Future `trade live` command
- **Safety:** Must require double confirmation

#### TUI-9.6: Trade Sessions
- **Menu Path:** Trade → Sessions
- **Description:** List and manage trading sessions
- **Acceptance Criteria:**
  - [ ] Lists all trading sessions (paper/live)
  - [ ] Shows session status (active/completed)
  - [ ] Displays session P&L summary
  - [ ] Allows session inspection
- **CLI Equivalent:** Future `trade sessions` command

#### TUI-9.7: Trade Validate Session
- **Menu Path:** Trade → Validate Session
- **Description:** Validate session configuration
- **Acceptance Criteria:**
  - [ ] Validates algorithm configuration
  - [ ] Checks risk parameters
  - [ ] Verifies data availability
  - [ ] Reports validation results
- **CLI Equivalent:** Future `trade validate` command

#### TUI-9.8: Research Create Config
- **Menu Path:** Research → Create Config
- **Description:** Interactive configuration wizard
- **Acceptance Criteria:**
  - [ ] Step-by-step configuration builder
  - [ ] Validates inputs at each step
  - [ ] Saves configuration to file
  - [ ] Supports templates
- **CLI Equivalent:** N/A (TUI-only interactive wizard)

#### TUI-9.9: Validate History
- **Menu Path:** Validate → History
- **Description:** Show validation run history
- **Acceptance Criteria:**
  - [ ] Lists past validation runs
  - [ ] Shows run parameters
  - [ ] Displays results summary
  - [ ] Allows result comparison
- **CLI Equivalent:** Future `validate history` command

---

### Phase 2: Core Backtest Commands (TUI-10.x)

**Priority:** High
**Effort:** Medium
**Sprint:** 2

Add missing high-value backtest commands to TUI.

#### TUI-10.1: Backtest Tune
- **Menu Path:** Validate → Tune
- **Description:** Hyperparameter tuning interface
- **Acceptance Criteria:**
  - [ ] Select algorithm to tune
  - [ ] Configure parameter ranges
  - [ ] Set optimization objective
  - [ ] Display tuning progress
  - [ ] Show best parameters found
  - [ ] Save tuned configuration
- **CLI Equivalent:** `backtest tune`
- **Required Widgets:** Number input, progress bar

#### TUI-10.2: Backtest Train
- **Menu Path:** Validate → Train
- **Description:** Model training interface
- **Acceptance Criteria:**
  - [ ] Select model to train
  - [ ] Configure training parameters
  - [ ] Set train/validation split
  - [ ] Display training progress
  - [ ] Show training metrics
  - [ ] Save trained model
- **CLI Equivalent:** `backtest train`
- **Required Widgets:** Progress bar, metrics display

#### TUI-10.3: Backtest Simulate
- **Menu Path:** Validate → Simulate
- **Description:** Run trading simulations
- **Acceptance Criteria:**
  - [ ] Select algorithm
  - [ ] Configure simulation parameters
  - [ ] Set market conditions
  - [ ] Display simulation progress
  - [ ] Show simulation results
  - [ ] Compare multiple simulations
- **CLI Equivalent:** `backtest simulate`
- **Required Widgets:** Progress bar, results table

---

### Phase 3: Advanced Optimization (TUI-11.x)

**Priority:** Medium
**Effort:** Medium
**Sprint:** 3

Add regime-based and multi-objective optimization.

#### TUI-11.1: Regime Search
- **Menu Path:** Validate → Regime → Search
- **Description:** Find market regimes in data
- **Acceptance Criteria:**
  - [ ] Configure regime detection parameters
  - [ ] Run regime analysis
  - [ ] Display detected regimes
  - [ ] Show regime statistics
  - [ ] Export regime definitions
- **CLI Equivalent:** `backtest regime-search`
- **Required Widgets:** Chart widget (optional)

#### TUI-11.2: Regime Optimize
- **Menu Path:** Validate → Regime → Optimize
- **Description:** Optimize per detected regime
- **Acceptance Criteria:**
  - [ ] Select regimes to optimize
  - [ ] Configure per-regime parameters
  - [ ] Run optimization
  - [ ] Show per-regime results
  - [ ] Create regime-aware strategy
- **CLI Equivalent:** `backtest regime-optimize`
- **Dependencies:** TUI-11.1

#### TUI-11.3: Multi-Objective Optimization
- **Menu Path:** Validate → Multi-Objective
- **Description:** Pareto-optimal parameter search
- **Acceptance Criteria:**
  - [ ] Select multiple objectives (Sharpe, Max DD, etc.)
  - [ ] Configure objective weights
  - [ ] Run multi-objective optimization
  - [ ] Display Pareto frontier
  - [ ] Select preferred solution
- **CLI Equivalent:** `backtest multi-objective`
- **Required Widgets:** Results table with sorting

---

### Phase 4: Validation Info Commands (TUI-12.x)

**Priority:** Low
**Effort:** Low
**Sprint:** 4

Add validation information display.

#### TUI-12.1: Validate Stages
- **Menu Path:** Validate → Info → Stages
- **Description:** Show validation pipeline stages
- **Acceptance Criteria:**
  - [ ] List all pipeline stages
  - [ ] Show stage descriptions
  - [ ] Display stage dependencies
  - [ ] Indicate current stage status
- **CLI Equivalent:** `validate stages`

#### TUI-12.2: Validate Status
- **Menu Path:** Validate → Info → Status
- **Description:** Show current validation status
- **Acceptance Criteria:**
  - [ ] Display active validation runs
  - [ ] Show progress per stage
  - [ ] Indicate estimated completion
  - [ ] Show resource usage
- **CLI Equivalent:** `validate status`

#### TUI-12.3: Validate Show
- **Menu Path:** Validate → Info → Details
- **Description:** Show detailed validation info
- **Acceptance Criteria:**
  - [ ] Display validation configuration
  - [ ] Show intermediate results
  - [ ] Display detailed metrics
  - [ ] Export results option
- **CLI Equivalent:** `validate show`

---

### Phase 5: Interactive Input Widgets (TUI-13.x)

**Priority:** Medium
**Effort:** High
**Sprint:** 5-6

Build reusable input widgets for parameter configuration.

#### TUI-13.1: Text Input Widget
- **Location:** `src/ui/widgets/text_input.rs`
- **Description:** Single-line text input
- **Acceptance Criteria:**
  - [ ] Cursor movement (left/right, home/end)
  - [ ] Character insert/delete
  - [ ] Selection support (optional)
  - [ ] Validation callback
  - [ ] Placeholder text
  - [ ] Max length constraint
- **Use Cases:** Symbol entry, file paths, names

#### TUI-13.2: Number Input Widget
- **Location:** `src/ui/widgets/number_input.rs`
- **Description:** Numeric value input
- **Acceptance Criteria:**
  - [ ] Integer and float support
  - [ ] Min/max constraints
  - [ ] Step increment (up/down arrows)
  - [ ] Validation on input
  - [ ] Format display (decimals, percentage)
- **Use Cases:** Iterations, window sizes, thresholds

#### TUI-13.3: Date Picker Widget
- **Location:** `src/ui/widgets/date_picker.rs`
- **Description:** Date selection interface
- **Acceptance Criteria:**
  - [ ] Calendar view
  - [ ] Date range selection
  - [ ] Quick presets (1M, 3M, 1Y, YTD)
  - [ ] Manual date entry
  - [ ] Validation against data availability
- **Use Cases:** Backtest date ranges, data queries

#### TUI-13.4: Dropdown Widget
- **Location:** `src/ui/widgets/dropdown.rs`
- **Description:** Single-select from options
- **Acceptance Criteria:**
  - [ ] Expandable option list
  - [ ] Keyboard navigation
  - [ ] Type-to-search filter
  - [ ] Custom option rendering
  - [ ] Disabled options support
- **Use Cases:** Strategy type, regime selection

#### TUI-13.5: Checkbox List Widget
- **Location:** `src/ui/widgets/checkbox_list.rs`
- **Description:** Multi-select from options
- **Acceptance Criteria:**
  - [ ] Toggle individual items
  - [ ] Select all / deselect all
  - [ ] Keyboard navigation
  - [ ] Item grouping (optional)
  - [ ] Minimum/maximum selection constraints
- **Use Cases:** Feature selection, stage selection

#### TUI-13.6: Slider Widget
- **Location:** `src/ui/widgets/slider.rs`
- **Description:** Continuous value selection
- **Acceptance Criteria:**
  - [ ] Horizontal slider bar
  - [ ] Value display
  - [ ] Min/max labels
  - [ ] Step snapping
  - [ ] Keyboard adjustment
- **Use Cases:** Risk parameters, thresholds

---

### Phase 6: Real-time Output Display (TUI-14.x)

**Priority:** High
**Effort:** High
**Sprint:** 7-8

Build output widgets for progress and results.

#### TUI-14.1: Progress Widget
- **Location:** `src/ui/widgets/progress.rs`
- **Description:** Progress bar with status
- **Acceptance Criteria:**
  - [ ] Percentage bar visualization
  - [ ] Current/total count display
  - [ ] Elapsed time
  - [ ] Estimated time remaining
  - [ ] Status message
  - [ ] Indeterminate mode
- **Use Cases:** Backtest progress, training progress

#### TUI-14.2: Log Panel Widget
- **Location:** `src/ui/widgets/log_panel.rs`
- **Description:** Scrollable log output
- **Acceptance Criteria:**
  - [ ] Auto-scroll to bottom
  - [ ] Scroll lock toggle
  - [ ] Log level filtering
  - [ ] Search within logs
  - [ ] Copy to clipboard
  - [ ] Timestamp display
- **Use Cases:** Command output, debug logs

#### TUI-14.3: Metrics Dashboard Widget
- **Location:** `src/ui/widgets/metrics_dashboard.rs`
- **Description:** Real-time metrics display
- **Acceptance Criteria:**
  - [ ] Key metrics cards (P&L, Sharpe, DD)
  - [ ] Color-coded values (green/red)
  - [ ] Trend indicators
  - [ ] Configurable metrics selection
  - [ ] Auto-refresh
- **Use Cases:** Live trading, paper trading

#### TUI-14.4: Chart Widget
- **Location:** `src/ui/widgets/chart.rs`
- **Description:** Basic ASCII/Unicode charts
- **Acceptance Criteria:**
  - [ ] Line chart support
  - [ ] Bar chart support
  - [ ] Axis labels
  - [ ] Legend
  - [ ] Multiple series
  - [ ] Auto-scaling
- **Use Cases:** Equity curve, drawdown visualization
- **Library:** Consider `ratatui` built-in charts

---

## 4. Architecture Requirements

### 4.1 Unified Command Executor

**Requirement:** Create shared execution layer for CLI and TUI.

```
src/commands/
├── mod.rs
├── executor.rs         # CommandExecutor trait
├── backtest.rs         # Backtest command implementations
├── research.rs         # Research command implementations
├── validate.rs         # Validate command implementations
├── algorithm.rs        # Algorithm command implementations
└── params/
    ├── mod.rs
    ├── backtest_params.rs
    ├── tune_params.rs
    └── ...
```

**Acceptance Criteria:**
- [ ] Single implementation used by both CLI and TUI
- [ ] Async execution support
- [ ] Progress callback mechanism
- [ ] Cancellation support
- [ ] Result streaming

### 4.2 Parameter Builder Pattern

**Requirement:** Standardized parameter construction.

```rust
pub struct BacktestParamsBuilder {
    algorithm_id: Option<String>,
    symbol: Option<String>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    // ...
}

impl BacktestParamsBuilder {
    pub fn new() -> Self;
    pub fn algorithm_id(self, id: impl Into<String>) -> Self;
    pub fn symbol(self, symbol: impl Into<String>) -> Self;
    pub fn build(self) -> Result<BacktestParams, ValidationError>;
}
```

**Acceptance Criteria:**
- [ ] Builder for each command type
- [ ] Validation on build
- [ ] Sensible defaults
- [ ] Serialization support (for persistence)

### 4.3 Event-Driven Progress Updates

**Requirement:** Real-time progress communication.

```rust
pub enum ProgressEvent {
    Started { total: usize },
    Progress { current: usize, message: String },
    Metric { name: String, value: f64 },
    Log { level: LogLevel, message: String },
    Completed { result: Box<dyn Any> },
    Error { error: Error },
}

pub trait ProgressSubscriber: Send + Sync {
    fn on_event(&self, event: ProgressEvent);
}
```

**Acceptance Criteria:**
- [ ] Non-blocking event emission
- [ ] Multiple subscriber support
- [ ] Event buffering for slow subscribers
- [ ] Thread-safe implementation

---

## 5. Testing Requirements

### 5.1 Unit Tests

Each new component requires:
- [ ] Happy path tests
- [ ] Error handling tests
- [ ] Edge case tests
- [ ] Mock-based isolation tests

**Coverage Target:** 80% line coverage minimum

### 5.2 Integration Tests

- [ ] CLI-TUI parity tests (same inputs → same outputs)
- [ ] Widget interaction tests
- [ ] State management tests
- [ ] Event flow tests

### 5.3 Manual Testing

- [ ] Visual inspection of all widgets
- [ ] Keyboard navigation verification
- [ ] Screen resize handling
- [ ] Color/theme consistency

---

## 6. Implementation Schedule

| Sprint | Phase | Tasks | Duration |
|--------|-------|-------|----------|
| 1 | Phase 1 | TUI-9.1 to TUI-9.9 | 1 week |
| 2 | Phase 2 | TUI-10.1 to TUI-10.3 | 1 week |
| 3 | Phase 5 (partial) | TUI-13.1, TUI-13.2 | 1 week |
| 4 | Phase 6 (partial) | TUI-14.1, TUI-14.2 | 1 week |
| 5 | Phase 3 | TUI-11.1 to TUI-11.3 | 1 week |
| 6 | Phase 4 | TUI-12.1 to TUI-12.3 | 0.5 week |
| 7 | Phase 5 (complete) | TUI-13.3 to TUI-13.6 | 1 week |
| 8 | Phase 6 (complete) | TUI-14.3, TUI-14.4 | 1 week |

**Total Estimated Duration:** 7.5 weeks

---

## 7. Success Metrics

| Metric | Target |
|--------|--------|
| CLI-TUI Parity | 100% |
| Test Coverage | ≥80% |
| Widget Reusability | ≥90% shared between features |
| User Task Completion | All CLI tasks achievable via TUI |
| Response Time | <100ms for UI interactions |

---

## 8. Dependencies

### External Crates
- `ratatui` - TUI framework (existing)
- `crossterm` - Terminal handling (existing)
- `tokio` - Async runtime (existing)

### Internal Dependencies
- `ConfigStore` - Configuration persistence
- `AlgorithmConfig` - Algorithm definitions
- `ValidationPipeline` - Validation execution

---

## 9. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Complex widget interactions | High | Start with simple widgets, iterate |
| Performance with large datasets | Medium | Implement pagination, lazy loading |
| Terminal compatibility | Medium | Test on multiple terminal emulators |
| Async complexity | High | Use established patterns, thorough testing |

---

## 10. Appendix

### A. CLI Command Reference

```
backtest evaluate   - Run backtest evaluation
backtest sweep      - Parameter sweep
backtest walk-forward - Walk-forward analysis
backtest tune       - Hyperparameter tuning
backtest regime-search - Find market regimes
backtest oos-validate - Out-of-sample validation
backtest multi-objective - Multi-objective optimization
backtest regime-optimize - Per-regime optimization
backtest train      - Train models
backtest simulate   - Run simulations
backtest grid       - Grid search
backtest campaign   - Run trading campaign
backtest paper      - Paper trading

research run        - Run research
research status     - Research status

validate run        - Run validation
validate presets    - Show presets
validate stages     - Show stages
validate status     - Validation status
validate show       - Show details

algorithm create    - Create algorithm
algorithm list      - List algorithms
algorithm show      - Show algorithm
```

### B. TUI Menu Structure (Target)

```
Main Menu
├── Research
│   ├── Run
│   ├── Status
│   └── Create Config
├── Algorithms
│   ├── List
│   ├── Select
│   ├── View
│   ├── New
│   └── Filters...
├── Validate
│   ├── Backtest
│   ├── Walk-Forward
│   ├── OOS
│   ├── All
│   ├── Grid Search
│   ├── Sweep
│   ├── Tune          ← NEW
│   ├── Train         ← NEW
│   ├── Simulate      ← NEW
│   ├── Regime        ← NEW submenu
│   │   ├── Search
│   │   └── Optimize
│   ├── Multi-Objective ← NEW
│   ├── History
│   ├── Presets
│   └── Info          ← NEW submenu
│       ├── Stages
│       ├── Status
│       └── Details
├── Trade
│   ├── Paper
│   ├── Campaign
│   ├── Live
│   ├── Sessions
│   └── Validate Session
└── Data
    ├── Live Stream
    ├── Features
    ├── Info
    └── Quality
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-12-29 | Claude | Initial draft |
