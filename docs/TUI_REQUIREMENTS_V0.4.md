# TUI Requirements Document v0.4 - Complete CLI Parity with Visual Tools

## Executive Summary

**Version:** 0.4  
**Date:** 2025-01-XX  
**Status:** Implementation Ready  
**Goal:** 100% CLI-TUI parity with ALL options accessible + Rich visual tools

This document provides a **comprehensive roadmap** to achieve complete CLI-TUI parity with:
- **Every CLI option** accessible via TUI
- **Visual parameter configuration** tools
- **Rich results visualization** (charts, tables, metrics)
- **Interactive widgets** for all parameter types
- **Unified command execution layer** (no code duplication)
- **Algorithm type indicators** - Commands specific to Market Making (MM) or Momentum (MOM) are clearly marked

### Important: Algorithm Type Restrictions

Some commands are **specific to certain algorithm types**:
- **Market Making (MM) only:** `tune`, `regime-search`, `multi-objective`, `regime-optimize`, `train`, `walk-forward-ml`, `grid`
- **Both types:** `evaluate`, `sweep`, `walk-forward`, `oos-validate`, `simulate`, `campaign`, `paper`

In the TUI, these restrictions will be shown in parentheses: e.g., "Tune (MM)" or "Regime Search (MM)".

---

## Core Architecture: Unified Command Execution Layer

```
┌─────────────────────────────────────────────────────────────┐
│              Command Execution Layer                          │
│              (src/commands/)                                  │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Backtest    │  │  Research    │  │  Validate    │      │
│  │  Commands    │  │  Commands    │  │  Commands    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐                         │
│  │  Algorithm   │  │  Common      │                         │
│  │  Commands    │  │  (Progress)  │                         │
│  └──────────────┘  └──────────────┘                         │
└─────────────────────────────────────────────────────────────┘
           │                              │
           ▼                              ▼
    ┌──────────────┐              ┌──────────────┐
    │   CLI (bin/)  │              │   TUI (ui/)  │
    │  Uses commands│              │ Uses commands│
    └──────────────┘              └──────────────┘
```

**Key Principle:** CLI and TUI share the same command execution logic. Only the UI layer differs.

---

## Phase 1: Command Execution Layer (Week 1-2)

### Task 1.1: Module Structure

```
src/commands/
├── mod.rs                    # Module exports
├── backtest.rs               # All 14 backtest commands
├── research.rs               # All 2 research commands
├── validate.rs               # All 5 validate commands
├── algorithm.rs              # All 3 algorithm commands
├── params/                   # Parameter builders
│   ├── mod.rs
│   ├── backtest_params.rs   # All backtest parameter types
│   ├── research_params.rs
│   ├── validate_params.rs
│   └── algorithm_params.rs
└── common.rs                 # Progress callbacks, shared types
```

### Task 1.2: Backtest Commands Extraction

**File:** `src/commands/backtest.rs`

Extract ALL backtest commands with ALL their parameters:

```rust
pub struct BacktestCommands;

impl BacktestCommands {
    // Single evaluation
    pub async fn evaluate(
        params: EvaluateParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<EvaluateResult>;
    
    // Parameter sweep
    pub async fn sweep(
        params: SweepParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<SweepResult>;
    
    // Walk-forward validation
    pub async fn walk_forward(
        params: WalkForwardParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<WalkForwardResult>;
    
    // Hyperparameter tuning (grid search) - MM only
    pub async fn tune(
        params: TuneParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<TuneResult>;
    
    // Regime search - MM only
    pub async fn regime_search(
        params: RegimeSearchParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<RegimeSearchResult>;
    
    // Out-of-sample validation - Both (but params shown are MM-specific)
    pub async fn oos_validate(
        params: OOSValidateParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<OOSValidateResult>;
    
    // Multi-objective optimization - MM only
    pub async fn multi_objective(
        params: MultiObjectiveParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<MultiObjectiveResult>;
    
    // Regime-specific optimization - MM only
    pub async fn regime_optimize(
        params: RegimeOptimizeParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<RegimeOptimizeResult>;
    
    // ML training - MM only (ML Spread/Skew algorithm)
    pub async fn train(
        params: TrainParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<TrainResult>;
    
    // Walk-forward ML training - MM only
    pub async fn walk_forward_ml(
        params: WalkForwardMLParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<WalkForwardMLResult>;
    
    // Campaign simulation
    pub async fn simulate(
        params: SimulateParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<SimulateResult>;
    
    // Grid search - MM only
    pub async fn grid(
        params: GridParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<GridResult>;
    
    // Validation campaign
    pub async fn campaign(
        params: CampaignParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<CampaignResult>;
    
    // Paper trading
    pub async fn paper(
        params: PaperParams,
        progress: Option<Box<dyn ProgressCallback>>,
    ) -> Result<PaperResult>;
    
    // List algorithms
    pub async fn list_algorithms(
        params: ListAlgorithmsParams,
    ) -> Result<ListAlgorithmsResult>;
}
```

### Task 1.3: Complete Parameter Definitions

**File:** `src/commands/params/backtest_params.rs`

Define parameter structs for EVERY command with ALL options:

```rust
// Evaluate command - ALL options from CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluateParams {
    // Data options
    pub data_path: PathBuf,
    pub algorithm: String,
    pub weights_file: Option<PathBuf>,
    
    // Trading parameters
    pub spread: f64,
    pub skew: f64,
    pub max_inventory: f64,
    pub quote_size: f64,
    pub fee_rate: f64,
    
    // Fill simulation
    pub naive_fills: bool,
    pub fill_prob: f64,
    pub queue_pos: f64,
    
    // Regime parameters
    pub high_entropy: f64,
    pub low_entropy: f64,
    pub regime_params: bool,
    pub high_spread: f64,
    pub med_spread: f64,
    pub low_spread: f64,
    pub high_skew: f64,
    pub med_skew: f64,
    pub low_skew: f64,
    pub quote_low_entropy: bool,
    
    // Output options
    pub output: Option<PathBuf>,
    pub json: bool,
    pub quiet: bool,
    pub stats: bool,
}

// Tune command - ALL options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuneParams {
    pub data_path: PathBuf,
    pub algorithm: String,
    pub weights_file: Option<PathBuf>,
    
    // Grid search ranges (comma-separated strings)
    pub spreads: String,              // "1,2,3,4,5"
    pub skews: String,                 // "0.3,0.5,0.7,1.0"
    pub high_entropies: String,        // "0.6,0.7,0.8"
    pub fill_probs: String,            // "0.05,0.10,0.15"
    
    // Common options
    pub output: Option<PathBuf>,
    pub json: bool,
    pub quiet: bool,
}

// RegimeSearch command - ALL options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeSearchParams {
    pub data_path: PathBuf,
    pub algorithm: String,
    
    // Regime-specific spreads
    pub high_spreads: String,          // "0.5,1.0,1.5"
    pub med_spreads: String,           // "2.0,2.5,3.0"
    pub low_spreads: String,           // "4.0,5.0,none"
    
    // Regime-specific skews
    pub high_skews: String,            // "0.2,0.3,0.4"
    pub med_skews: String,             // "0.4,0.5,0.6"
    pub low_skews: String,             // "0.8,1.0,1.2"
    
    pub fill_probs: String,
    pub output: Option<PathBuf>,
    pub json: bool,
}

// MultiObjective command - ALL options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiObjectiveParams {
    pub data_path: PathBuf,
    pub algorithm: String,
    
    // Grid search ranges
    pub spreads: String,
    pub skews: String,
    pub fill_probs: String,
    pub high_entropies: String,
    
    // Objective weights (must sum to 1.0)
    pub w_sharpe: f64,                 // Weight for Sharpe ratio
    pub w_drawdown: f64,               // Weight for drawdown
    pub w_fill: f64,                   // Weight for fill rate
    pub w_turnover: f64,               // Weight for turnover
    
    // Constraints
    pub min_trades: usize,
    
    pub output: Option<PathBuf>,
    pub json: bool,
}

// Train command - ALL options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainParams {
    pub data_path: PathBuf,
    
    // Training split
    pub train_ratio: f64,              // 0.7 = 70% train, 30% test
    
    // Spread model parameters (comma-separated)
    pub spread_intercepts: String,     // "1.0,2.0,3.0,4.0,5.0"
    pub spread_entropy_weights: String, // "-3.0,-2.0,-1.0,0.0"
    pub spread_vol_weights: String,    // "200.0,400.0,600.0"
    
    // Skew model parameters
    pub skew_intercepts: String,       // "0.3,0.5,0.7"
    pub skew_inv_weights: String,      // "-1.0,-0.8,-0.6,-0.4"
    
    pub output: Option<PathBuf>,
    pub json: bool,
}

// WalkForwardML command - ALL options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardMLParams {
    pub data_path: PathBuf,
    
    // Walk-forward configuration
    pub folds: usize,
    pub min_train_hours: f64,
    pub test_hours: f64,
    pub rolling: bool,
    pub embargo_hours: f64,
    
    // ML parameter grids (same as Train)
    pub spread_intercepts: String,
    pub spread_entropy_weights: String,
    pub spread_vol_weights: String,
    pub skew_intercepts: String,
    pub skew_inv_weights: String,
    
    pub output: Option<PathBuf>,
    pub weights_output: Option<PathBuf>,
    pub json: bool,
}

// Simulate command - ALL options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulateParams {
    pub data_path: PathBuf,
    pub algorithm: String,
    
    // Campaign configuration
    pub weeks: u8,
    pub session_hours: f64,
    pub min_sessions_per_week: u8,
    
    // Trading parameters
    pub preset: Option<String>,
    pub spread: f64,
    pub skew: f64,
    
    // Expected metrics (for comparison)
    pub expected_fill_rate: f64,
    pub expected_sharpe: f64,
    pub expected_return: f64,
    
    // Validation gates
    pub min_weekly_trades: usize,
    pub max_drawdown_pct: f64,
    pub min_win_rate: f64,
    
    pub campaigns_dir: PathBuf,
    pub output: Option<PathBuf>,
    pub json: bool,
}

// ... (Continue for ALL 14 commands)
```

**Acceptance Criteria:**
- [ ] Every CLI option mapped to parameter struct
- [ ] Parameter builders with validation
- [ ] Default values match CLI defaults
- [ ] Serialization support (for presets)

---

## Phase 2: Visual Parameter Configuration (Week 3-4)

### Task 2.1: Parameter Input Widgets

**File:** `src/ui/widgets/params/`

Create specialized widgets for each parameter type:

#### Text Input Widget
```rust
// src/ui/widgets/params/text_input.rs
pub struct TextInputWidget {
    value: String,
    placeholder: String,
    max_length: Option<usize>,
    validator: Option<Box<dyn Fn(&str) -> Result<(), String>>>,
}

// Features:
// - Cursor movement (left/right, home/end)
// - Character insert/delete
// - Validation with error display
// - Placeholder text
// - Max length constraint
```

#### Number Input Widget
```rust
// src/ui/widgets/params/number_input.rs
pub struct NumberInputWidget {
    value: f64,
    min: Option<f64>,
    max: Option<f64>,
    step: f64,
    decimals: usize,
    format: NumberFormat,  // Decimal, Percentage, BasisPoints
}

// Features:
// - Increment/decrement (up/down arrows, +/- keys)
// - Min/max validation
// - Step snapping
// - Format display (decimals, %, bps)
// - Slider mode (optional)
```

#### Comma-Separated List Widget
```rust
// src/ui/widgets/params/comma_list.rs
pub struct CommaListWidget {
    values: Vec<f64>,  // Parsed from comma-separated string
    raw_string: String,
    item_type: ListItemType,  // Float, Int, String, Enum
}

// Features:
// - Add/remove items
// - Edit individual items
// - Validation (no duplicates, sorted, etc.)
// - Visual list display
// - Quick presets (e.g., "1,2,3,4,5")
```

#### Boolean Toggle Widget
```rust
// src/ui/widgets/params/toggle.rs
pub struct ToggleWidget {
    value: bool,
    label_on: String,
    label_off: String,
}

// Features:
// - Space/Enter to toggle
// - Visual indicator (checkbox, switch)
```

#### Path Input Widget
```rust
// src/ui/widgets/params/path_input.rs
pub struct PathInputWidget {
    value: PathBuf,
    path_type: PathType,  // File, Directory, Optional
    validator: Option<Box<dyn Fn(&Path) -> Result<(), String>>>,
}

// Features:
// - Path completion (tab)
// - File browser integration
// - Existence validation
// - Relative/absolute path support
```

#### Dropdown Widget
```rust
// src/ui/widgets/params/dropdown.rs
pub struct DropdownWidget<T> {
    options: Vec<T>,
    selected: usize,
    display_fn: Box<dyn Fn(&T) -> String>,
}

// Features:
// - Expandable list
// - Keyboard navigation
// - Type-to-search
// - Custom rendering
```

#### Slider Widget (for ranges)
```rust
// src/ui/widgets/params/slider.rs
pub struct SliderWidget {
    value: f64,
    min: f64,
    max: f64,
    step: f64,
    label: String,
    show_value: bool,
}

// Features:
// - Horizontal slider bar
// - Mouse/keyboard adjustment
// - Value display
// - Min/max labels
```

### Task 2.2: Parameter Configuration Screens

**File:** `src/ui/screens/params/`

Create configuration screens for each command type:

#### Backtest Evaluate Config Screen
```rust
// src/ui/screens/params/backtest_evaluate.rs
pub struct BacktestEvaluateConfigScreen {
    params: EvaluateParamsBuilder,
    current_field: EvaluateField,
    field_order: Vec<EvaluateField>,
}

pub enum EvaluateField {
    DataPath,
    Algorithm,
    WeightsFile,
    Spread,
    Skew,
    MaxInventory,
    QuoteSize,
    FeeRate,
    FillMode,  // Naive vs Realistic
    FillProb,
    QueuePos,
    HighEntropy,
    LowEntropy,
    RegimeParams,  // Boolean
    HighSpread,
    MedSpread,
    LowSpread,
    HighSkew,
    MedSkew,
    LowSkew,
    QuoteLowEntropy,
    Output,
    Stats,
}

// Layout:
// ┌─────────────────────────────────────────┐
// │  Backtest Evaluate Configuration        │
// ├─────────────────────────────────────────┤
// │                                         │
// │  Data Path: [./data/features      ]    │
// │  Algorithm: [as ▼]                     │
// │  Spread (bps): [2.0] [━━━━━━━━━━]      │
// │  Skew: [0.5] [━━━━━━━━━━━━━━━━━━]      │
// │                                         │
// │  Fill Mode: ( ) Naive  (•) Realistic   │
// │  Fill Prob: [0.10] [━━━━━━━━━━━━━━]    │
// │                                         │
// │  [Advanced Options ▼]                  │
// │    Regime Params: [ ] Enable           │
// │    High Spread: [1.0]                  │
// │    ...                                 │
// │                                         │
// │  [Cancel]  [Save Preset]  [Run]       │
// └─────────────────────────────────────────┘
```

#### Tune Config Screen (Grid Search)
```rust
// src/ui/screens/params/backtest_tune.rs
// NOTE: This command is specific to Market Making algorithms
// (Avellaneda-Stoikov, ML Spread/Skew, Fixed Spread)
pub struct BacktestTuneConfigScreen {
    params: TuneParamsBuilder,
    current_field: TuneField,
    algorithm_type: AlgorithmType,  // Must be MM algorithm
}

pub enum TuneField {
    DataPath,
    Algorithm,
    Spreads,        // CommaListWidget
    Skews,          // CommaListWidget
    HighEntropies,  // CommaListWidget
    FillProbs,      // CommaListWidget
    Output,
}

// Layout with visual grid preview:
// ┌─────────────────────────────────────────┐
// │  Hyperparameter Tuning Configuration    │
// ├─────────────────────────────────────────┤
// │                                         │
// │  Spreads: [1,2,3,4,5] [+ Add]          │
// │    ┌─┬─┬─┬─┬─┐                         │
// │    │1│2│3│4│5│                         │
// │    └─┴─┴─┴─┴─┘                         │
// │                                         │
// │  Skews: [0.3,0.5,0.7,1.0] [+ Add]      │
// │    ┌───┬───┬───┬───┐                   │
// │    │0.3│0.5│0.7│1.0│                   │
// │    └───┴───┴───┴───┘                   │
// │                                         │
// │  Total Combinations: 100              │
// │  Estimated Time: ~5 minutes            │
// │                                         │
// │  [Cancel]  [Save Preset]  [Run]       │
// └─────────────────────────────────────────┘
```

#### Multi-Objective Config Screen
```rust
// src/ui/screens/params/backtest_multi_objective.rs
pub struct MultiObjectiveConfigScreen {
    params: MultiObjectiveParamsBuilder,
    current_field: MultiObjectiveField,
}

// Special widget for objective weights:
// ┌─────────────────────────────────────────┐
// │  Objective Weights (must sum to 1.0)    │
// ├─────────────────────────────────────────┤
// │                                         │
// │  Sharpe Ratio:    [0.4] [━━━━━━━━━━]   │
// │  Max Drawdown:    [0.3] [━━━━━━━━]     │
// │  Fill Rate:       [0.2] [━━━━━━]       │
// │  Turnover:        [0.1] [━━━]          │
// │                                         │
// │  Total: 1.0 ✓                          │
// │                                         │
// └─────────────────────────────────────────┘
```

### Task 2.3: Advanced Parameter Features

#### Parameter Groups/Tabs
For commands with many parameters, organize into groups:

```rust
pub enum ParameterGroup {
    Basic,      // Essential parameters
    Advanced,   // Advanced options
    Output,     // Output configuration
    Validation, // Validation settings
}
```

#### Parameter Presets
```rust
// src/ui/presets.rs
pub struct PresetManager;

impl PresetManager {
    // Save current parameter configuration
    pub fn save_preset(name: &str, params: &dyn Serialize) -> Result<()>;
    
    // Load preset
    pub fn load_preset(name: &str) -> Result<Box<dyn Deserialize>>;
    
    // List available presets
    pub fn list_presets(command_type: &str) -> Result<Vec<String>>;
    
    // Quick presets (built-in)
    pub fn quick_presets(command_type: &str) -> Vec<(&str, Box<dyn Fn() -> Box<dyn Serialize>>)>;
}
```

#### Parameter Validation
```rust
// Real-time validation as user types
pub trait ParameterValidator {
    fn validate(&self, value: &str) -> Result<(), ValidationError>;
    fn hint(&self) -> Option<String>;
}

// Examples:
// - Spread must be > 0
// - Weights must sum to 1.0
// - Path must exist
// - Comma list must have at least 2 items
```

---

## Phase 3: Visual Results Display (Week 5-6)

### Task 3.1: Results Display Widgets

**File:** `src/ui/widgets/results/`

#### Metrics Dashboard Widget
```rust
// src/ui/widgets/results/metrics_dashboard.rs
pub struct MetricsDashboardWidget {
    metrics: Vec<Metric>,
    layout: MetricsLayout,  // Grid, List, Cards
}

pub struct Metric {
    name: String,
    value: MetricValue,
    format: MetricFormat,
    trend: Option<Trend>,  // Up, Down, Neutral
    color: Option<Color>,
}

pub enum MetricValue {
    Number(f64),
    Percentage(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

// Display:
// ┌─────────────────────────────────────────┐
// │  Performance Metrics                    │
// ├─────────────────────────────────────────┤
// │  Sharpe Ratio:    1.45 ↑ (green)       │
// │  Max Drawdown:    2.3% ↓ (red)         │
// │  Win Rate:        58.2% ↑ (green)      │
// │  Total Return:    12.5% ↑ (green)      │
// │  Profit Factor:   1.85 ↑ (green)       │
// │  Trade Count:     1,234                 │
// └─────────────────────────────────────────┘
```

#### Table Widget
```rust
// src/ui/widgets/results/table.rs
pub struct TableWidget {
    headers: Vec<TableHeader>,
    rows: Vec<TableRow>,
    selected_row: usize,
    sort_column: Option<usize>,
    sort_ascending: bool,
}

pub struct TableHeader {
    name: String,
    width: Option<usize>,
    align: Alignment,
    sortable: bool,
}

// Features:
// - Sortable columns (click header)
// - Scrollable rows
// - Row selection
// - Column resizing
// - Export to CSV
```

#### Chart Widget (ASCII/Unicode)
```rust
// src/ui/widgets/results/chart.rs
pub struct ChartWidget {
    data: Vec<DataPoint>,
    chart_type: ChartType,
    title: String,
    x_label: String,
    y_label: String,
}

pub enum ChartType {
    Line,
    Bar,
    Scatter,
    Heatmap,
}

// Example Line Chart:
// ┌─────────────────────────────────────────┐
// │  Equity Curve                          │
// ├─────────────────────────────────────────┤
// │  1.2 │     ╱╲                          │
// │  1.1 │   ╱╱  ╲╲                        │
// │  1.0 │ ╱╱      ╲╲                      │
// │  0.9 │╱          ╲                     │
// │     └────────────────────────────      │
// │      0    50   100   150   200         │
// └─────────────────────────────────────────┘
```

#### Pareto Frontier Widget (for Multi-Objective)
```rust
// src/ui/widgets/results/pareto.rs
pub struct ParetoFrontierWidget {
    solutions: Vec<ParetoSolution>,
    selected: Option<usize>,
    x_axis: Objective,
    y_axis: Objective,
}

pub struct ParetoSolution {
    sharpe: f64,
    drawdown: f64,
    fill_rate: f64,
    turnover: f64,
    params: HashMap<String, f64>,
}

// Display:
// ┌─────────────────────────────────────────┐
// │  Pareto Frontier (Sharpe vs Drawdown)   │
// ├─────────────────────────────────────────┤
// │                                         │
// │  Sharpe │                              │
// │    2.0  │         ●                    │
// │    1.5  │     ●       ●                │
// │    1.0  │  ●               ●            │
// │    0.5  │●                           ● │
// │    0.0  └────────────────────────────   │
// │         0%   5%   10%  15%  20%        │
// │              Max Drawdown              │
// │                                         │
// │  Selected: Sharpe=1.5, DD=5.2%        │
// └─────────────────────────────────────────┘
```

#### Progress Widget (Enhanced)
```rust
// src/ui/widgets/results/progress.rs
pub struct ProgressWidget {
    current: usize,
    total: usize,
    message: String,
    metrics: HashMap<String, f64>,
    logs: VecDeque<LogEntry>,
    eta: Option<Duration>,
    elapsed: Duration,
}

// Display:
// ┌─────────────────────────────────────────┐
// │  Running Grid Search...                 │
// ├─────────────────────────────────────────┤
// │                                         │
// │  Progress: [████████░░] 45/100         │
// │                                         │
// │  Current: spread=2.0, skew=0.5         │
// │  Best So Far: Sharpe=1.45              │
// │                                         │
// │  Elapsed: 2m 15s                        │
// │  ETA: 2m 45s                            │
// │                                         │
// │  Logs:                                  │
// │  [INFO] Testing spread=1.0...          │
// │  [INFO] Sharpe=1.23, DD=3.2%           │
// │  [INFO] Testing spread=2.0...          │
// │                                         │
// └─────────────────────────────────────────┘
```

### Task 3.2: Command-Specific Result Screens

#### Backtest Evaluate Results
```rust
// src/ui/screens/results/backtest_evaluate.rs
pub struct BacktestEvaluateResultsScreen {
    result: EvaluateResult,
    view_mode: ViewMode,
}

pub enum ViewMode {
    Summary,      // Key metrics
    Detailed,     // All metrics + statistics
    EquityCurve,  // Chart
    TradeLog,     // Table of trades
    Inventory,    // Inventory over time
}

// Summary View:
// ┌─────────────────────────────────────────┐
// │  Backtest Results                        │
// ├─────────────────────────────────────────┤
// │                                         │
// │  ╔═══════════════════════════════════╗ │
// │  ║  Performance Summary              ║ │
// │  ╠═══════════════════════════════════╣ │
// │  ║  Sharpe Ratio:    1.45 ↑         ║ │
// │  ║  Max Drawdown:    2.3% ↓         ║ │
// │  ║  Win Rate:        58.2% ↑         ║ │
// │  ║  Total Return:    12.5% ↑         ║ │
// │  ╚═══════════════════════════════════╝ │
// │                                         │
// │  [Summary] [Chart] [Trades] [Export]  │
// └─────────────────────────────────────────┘
```

#### Grid Search Results
```rust
// src/ui/screens/results/backtest_tune.rs
pub struct BacktestTuneResultsScreen {
    result: TuneResult,
    view_mode: TuneViewMode,
    sort_by: SortMetric,
}

pub enum TuneViewMode {
    TopResults,    // Top 10 by Sharpe
    FullTable,     // All results
    Heatmap,       // 2D heatmap (spread vs skew)
    Pareto,        // Pareto frontier (if multi-objective)
}

// Heatmap View:
// ┌─────────────────────────────────────────┐
// │  Grid Search Results - Heatmap          │
// ├─────────────────────────────────────────┤
// │                                         │
// │  Skew →                                 │
// │  1.0 │ ████ ████ ████ ████ ████        │
// │  0.7 │ ████ ████ ████ ████ ████        │
// │  0.5 │ ████ ████ ████ ████ ████        │
// │  0.3 │ ████ ████ ████ ████ ████        │
// │      └────────────────────────────     │
// │         1   2   3   4   5  Spread      │
// │                                         │
// │  Color: Green=High Sharpe, Red=Low      │
// │  Best: Spread=2.0, Skew=0.5 (1.45)     │
// │                                         │
// └─────────────────────────────────────────┘
```

#### Multi-Objective Results
```rust
// src/ui/screens/results/backtest_multi_objective.rs
pub struct MultiObjectiveResultsScreen {
    result: MultiObjectiveResult,
    view_mode: MultiObjectiveViewMode,
}

pub enum MultiObjectiveViewMode {
    ParetoFrontier,  // 2D scatter plot
    TopSolutions,    // Top 10 by composite score
    Comparison,      // Compare solutions side-by-side
}

// Pareto Frontier View with interactive selection
```

---

## Phase 4: TUI Integration (Week 7-8)

### Task 4.1: TUI Command Executor

**File:** `src/ui/command_executor.rs`

```rust
pub struct TUICommandExecutor {
    progress_tx: mpsc::Sender<ProgressEvent>,
    result_tx: mpsc::Sender<CommandResult>,
}

impl TUICommandExecutor {
    // Execute command with progress updates
    pub async fn execute_backtest_evaluate(
        &self,
        params: EvaluateParams,
    ) -> Result<EvaluateResult>;
    
    // ... (all other commands)
}

// Progress events sent to TUI
pub enum ProgressEvent {
    Started { total: usize },
    Progress { current: usize, message: String },
    Metric { name: String, value: f64 },
    Log { level: LogLevel, message: String },
    Completed { result: Box<dyn Any> },
    Error { error: String },
}
```

### Task 4.2: Menu Integration

Wire all commands to TUI menus with parameter configuration:

```rust
// When user selects "Validate → Tune (MM)":
// 1. Check if selected algorithm is MM type
// 2. If not, show warning: "Tune is only available for Market Making algorithms"
// 3. Show TuneConfigScreen
// 4. User configures parameters
// 5. Execute command with progress
// 6. Show TuneResultsScreen
```

**Algorithm Type Indicators in TUI:**

Menu items should display algorithm type restrictions:
- `Tune (MM)` - Only for Market Making algorithms
- `Regime Search (MM)` - Only for Market Making algorithms
- `Multi-Objective (MM)` - Only for Market Making algorithms
- `Regime Optimize (MM)` - Only for Market Making algorithms
- `Train (MM)` - Only for ML Spread/Skew (Market Making)
- `Walk-Forward ML (MM)` - Only for Market Making algorithms
- `Grid (MM)` - Only for Market Making algorithms

Commands without markers work with both algorithm types, but may show MM-specific parameters in the UI.

### Task 4.3: Navigation Flow

```
Main Menu
  └─> Validate Menu
       └─> Tune
            └─> Config Screen (all parameters)
                 └─> [Run] → Progress Screen
                      └─> Results Screen
                           └─> [Back] → Validate Menu
```

---

## Complete Parameter Coverage Matrix

### Backtest Commands

**Note:** Commands marked with **(MM)** are specific to Market Making algorithms (Avellaneda-Stoikov, ML Spread/Skew, Fixed Spread).  
Commands marked with **(MOM)** are specific to Momentum algorithms.  
Commands without markers work with both algorithm types.

| Command | Algorithm Type | Parameters | TUI Widget Type |
|---------|---------------|-----------|-----------------|
| `evaluate` | Both | 20+ params (MM params shown, but works with MOM) | Mixed (text, number, toggle, dropdown) |
| `tune` | **(MM)** | Spreads, Skews, HighEntropies, FillProbs | CommaListWidget |
| `regime-search` | **(MM)** | High/Med/Low spreads & skews | CommaListWidget (6 lists) |
| `multi-objective` | **(MM)** | Grid params + 4 weights | CommaListWidget + SliderWidget |
| `regime-optimize` | **(MM)** | Spreads, Skews, FillProb, MinTrades | CommaListWidget + NumberInput |
| `train` | **(MM)** | TrainRatio + 5 ML param grids (ML Spread/Skew only) | NumberInput + CommaListWidget |
| `walk-forward-ml` | **(MM)** | Folds, Hours, Rolling + ML params | NumberInput + Toggle + CommaListWidget |
| `simulate` | Both | Weeks, Hours, Preset, Metrics, Gates | NumberInput + Dropdown + NumberInput |
| `walk-forward` | Both | Folds, TestHours, Rolling | NumberInput + Toggle |
| `oos-validate` | Both | Holdout, Embargo, Grid params (MM params shown) | NumberInput + CommaListWidget |
| `grid` | **(MM)** | Spreads, Skews, FillProb | CommaListWidget |
| `campaign` | Both | Weeks, Sessions, Preset, Gates | NumberInput + Dropdown |
| `paper` | Both | Duration, Preset, Spread, Skew (MM params shown) | NumberInput + Dropdown |
| `sweep` | Both | Spreads, Skews (MM params shown, but works with MOM) | CommaListWidget |

### Research Commands

| Command | Parameters | TUI Widget Type |
|---------|-----------|-----------------|
| `run` | DataPath, Output, Symbol, Dates, MinSamples, CheckpointInterval, Resume | PathInput + TextInput + DatePicker + NumberInput + Toggle |
| `status` | Store, Symbol, Verbose, TopSignals | PathInput + TextInput + Toggle + NumberInput |

### Validate Commands

| Command | Parameters | TUI Widget Type |
|---------|-----------|-----------------|
| `run` | Config, FromResearch, Stages, Data, Results, Preset, ContinueOnFailure, NoPersist | PathInput + Dropdown + Toggle |
| `presets` | (Info only) | Display only |
| `stages` | (Info only) | Display only |
| `status` | Results, Last | PathInput + NumberInput |
| `show` | Results, RunId, Json, Verbose | PathInput + TextInput + Toggle |

### Algorithm Commands

| Command | Parameters | TUI Widget Type |
|---------|-----------|-----------------|
| `create` | Research, Output, Symbol, Name, Strategy, Validate, Data, Stages | PathInput + TextInput + Dropdown + Toggle |
| `list` | Store, Symbol, Strategy, Name, ActiveOnly, Limit | PathInput + TextInput + Dropdown + Toggle + NumberInput |
| `show` | Store, Id, Json, Verbose | PathInput + TextInput + Toggle |

---

## Algorithm Type Restrictions

### Market Making (MM) Specific Commands

These commands are **only available** for Market Making algorithms:
- `tune` - Grid search over spread/skew/entropy parameters
- `regime-search` - Regime-specific parameter optimization
- `multi-objective` - Multi-objective optimization (spread/skew based)
- `regime-optimize` - Per-regime parameter optimization
- `train` - ML weight training (ML Spread/Skew only)
- `walk-forward-ml` - Walk-forward ML training
- `grid` - Grid search over spread/skew

**Market Making Algorithms:**
- Avellaneda-Stoikov (`as`, `avellaneda_stoikov`)
- ML Spread/Skew (`ml`, `ml_spread_skew`)
- Fixed Spread (`fixed`, `fixed_spread`)

### Momentum (MOM) Specific Commands

Currently, no commands are exclusive to Momentum algorithms. However, Momentum algorithms use different parameters:
- Entry thresholds: `min_momentum_signal`, `min_monotonicity`, `min_hurst`
- Exit parameters: `take_profit_bps`, `stop_loss_bps`
- Position sizing: `max_position_size`, `base_position_fraction`
- Regime filters: `min_tau_half`, `max_entropy`, `min_persistence`

**Momentum Algorithms:**
- Momentum (`momentum`)

### Universal Commands

These commands work with both algorithm types:
- `evaluate` - Single backtest evaluation
- `sweep` - Parameter sweep (but shows MM params in current implementation)
- `walk-forward` - Walk-forward validation
- `oos-validate` - Out-of-sample validation
- `simulate` - Campaign simulation
- `campaign` - Validation campaign
- `paper` - Paper trading

**Note:** Some universal commands may currently show MM-specific parameters in the UI. Future enhancements should support algorithm-specific parameter sets.

## Implementation Checklist

### Week 1-2: Command Execution Layer
- [ ] Create `src/commands/` module structure
- [ ] Extract ALL backtest commands (14 commands)
- [ ] Extract ALL research commands (2 commands)
- [ ] Extract ALL validate commands (5 commands)
- [ ] Extract ALL algorithm commands (3 commands)
- [ ] Define parameter structs for ALL commands
- [ ] Implement parameter builders with validation
- [ ] **Add algorithm type validation** (MM-only commands check algorithm type)
- [ ] Implement progress callback system
- [ ] Refactor CLI binaries to use commands
- [ ] All existing tests pass

### Week 3-4: Visual Parameter Configuration
- [ ] Create all parameter input widgets
- [ ] Create configuration screens for each command
- [ ] **Add algorithm type indicators** (MM) or (MOM) to menu items
- [ ] **Add algorithm type validation** in config screens
- [ ] **Show appropriate parameters** based on selected algorithm type
- [ ] Implement parameter groups/tabs
- [ ] Implement preset management
- [ ] Implement parameter validation
- [ ] Test all parameter inputs

### Week 5-6: Visual Results Display
- [ ] Create all results display widgets
- [ ] Create result screens for each command type
- [ ] Implement charts (line, bar, scatter, heatmap)
- [ ] Implement tables with sorting
- [ ] Implement metrics dashboard
- [ ] Implement Pareto frontier visualization
- [ ] Test all result displays

### Week 7-8: TUI Integration
- [ ] Create TUI command executor
- [ ] Wire all commands to menus
- [ ] **Add algorithm type indicators** to menu items (MM) or (MOM)
- [ ] **Implement algorithm type checks** before showing MM-only commands
- [ ] **Show warning messages** when MM-only command selected with non-MM algorithm
- [ ] Implement navigation flow
- [ ] Test end-to-end workflows
- [ ] Performance optimization

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| CLI Option Coverage | 100% | Every CLI option accessible via TUI |
| Parameter Widgets | 100% | All parameter types have widgets |
| Visual Tools | Complete | Charts, tables, metrics for all results |
| Algorithm Type Indicators | 100% | All MM/MOM-specific commands clearly marked |
| Algorithm Type Validation | 100% | MM-only commands validate algorithm type |
| User Task Completion | 100% | All CLI tasks achievable via TUI |
| Code Reuse | ≥90% | Shared between CLI and TUI |
| Response Time | <100ms | UI interactions |
| Test Coverage | ≥80% | Line coverage for new code |

---

## File Structure (Complete)

```
src/
├── commands/                    # Unified command execution
│   ├── mod.rs
│   ├── backtest.rs             # 14 backtest commands
│   ├── research.rs              # 2 research commands
│   ├── validate.rs              # 5 validate commands
│   ├── algorithm.rs             # 3 algorithm commands
│   ├── params/
│   │   ├── mod.rs
│   │   ├── backtest_params.rs  # ALL backtest parameter types
│   │   ├── research_params.rs
│   │   ├── validate_params.rs
│   │   └── algorithm_params.rs
│   └── common.rs                # Progress, shared types
│
├── bin/                         # CLI binaries (refactored)
│   ├── backtest.rs
│   ├── research.rs
│   ├── validate.rs
│   └── algorithm.rs
│
└── ui/                          # TUI (enhanced)
    ├── command_executor.rs      # TUI command execution
    ├── presets.rs               # Preset management
    ├── widgets/
    │   ├── params/              # Parameter input widgets
    │   │   ├── mod.rs
    │   │   ├── text_input.rs
    │   │   ├── number_input.rs
    │   │   ├── comma_list.rs
    │   │   ├── toggle.rs
    │   │   ├── path_input.rs
    │   │   ├── dropdown.rs
    │   │   └── slider.rs
    │   ├── results/              # Results display widgets
    │   │   ├── mod.rs
    │   │   ├── metrics_dashboard.rs
    │   │   ├── table.rs
    │   │   ├── chart.rs
    │   │   ├── pareto.rs
    │   │   └── progress.rs
    │   └── ... (existing widgets)
    ├── screens/
    │   ├── params/               # Parameter configuration screens
    │   │   ├── mod.rs
    │   │   ├── backtest_evaluate.rs
    │   │   ├── backtest_tune.rs
    │   │   ├── backtest_regime_search.rs
    │   │   ├── backtest_multi_objective.rs
    │   │   ├── backtest_train.rs
    │   │   ├── backtest_simulate.rs
    │   │   ├── research_run.rs
    │   │   ├── validate_run.rs
    │   │   └── algorithm_create.rs
    │   ├── results/              # Results display screens
    │   │   ├── mod.rs
    │   │   ├── backtest_evaluate.rs
    │   │   ├── backtest_tune.rs
    │   │   ├── backtest_multi_objective.rs
    │   │   └── ...
    │   └── ... (existing screens)
    └── ... (existing UI code)
```

---

## Next Steps

1. **Review and Approve** this comprehensive requirements document
2. **Week 1-2:** Start command execution layer extraction
3. **Week 3-4:** Build visual parameter configuration tools
4. **Week 5-6:** Build visual results display tools
5. **Week 7-8:** Integrate everything into TUI

---

*Document Version: 0.4*  
*Last Updated: 2025-01-XX*  
*Status: Ready for Implementation*

