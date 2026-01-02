# TUI Requirements Document v0.3

## Executive Summary

**Version:** 0.3  
**Date:** 2025-01-XX  
**Status:** Implementation Ready  
**Goal:** Achieve 100% CLI-TUI parity with a simple, functional implementation

This document provides a **practical, actionable roadmap** to close the gap between CLI commands and TUI functionality. The focus is on **simplicity and completeness** - every CLI command should be accessible via TUI with minimal complexity.

---

## Current State Analysis

### CLI Commands Inventory

#### `backtest` Binary (12 commands)
1. ✅ `evaluate` - Single backtest evaluation
2. ✅ `sweep` - Parameter sweep
3. ✅ `walk-forward` - Walk-forward validation
4. ❌ `tune` - Hyperparameter tuning (grid search)
5. ❌ `regime-search` - Find market regimes
6. ✅ `oos-validate` - Out-of-sample validation
7. ❌ `multi-objective` - Multi-objective optimization
8. ❌ `regime-optimize` - Per-regime optimization
9. ❌ `train` - ML weight training
10. ❌ `simulate` - Campaign simulation
11. ✅ `grid` - Grid search
12. ✅ `campaign` - Validation campaign
13. ✅ `paper` - Paper trading
14. ✅ `algorithms` - List algorithms

#### `research` Binary (2 commands)
1. ✅ `run` - Run research analysis
2. ✅ `status` - Show research status

#### `validate` Binary (5 commands)
1. ✅ `run` - Run validation pipeline
2. ✅ `presets` - List presets
3. ❌ `stages` - List validation stages
4. ❌ `status` - Show validation status
5. ❌ `show` - Show validation details

#### `algorithm` Binary (3 commands)
1. ✅ `create` - Create algorithm config
2. ✅ `list` - List algorithms
3. ✅ `show` - Show algorithm details

**Total:** 22 CLI commands  
**TUI Coverage:** 15 commands (68%)  
**Gap:** 7 commands missing

---

## Implementation Strategy

### Core Principle: **Command Execution Layer**

Instead of duplicating logic, create a **unified command execution layer** that both CLI and TUI use:

```
┌─────────────────────────────────────────┐
│         Command Execution Layer          │
│  (src/commands/mod.rs)                  │
│                                         │
│  - BacktestCommands                     │
│  - ResearchCommands                     │
│  - ValidateCommands                     │
│  - AlgorithmCommands                    │
└─────────────────────────────────────────┘
           │                    │
           ▼                    ▼
    ┌──────────┐         ┌──────────┐
    │   CLI    │         │   TUI    │
    │  (bin/*) │         │ (ui/*)   │
    └──────────┘         └──────────┘
```

### Architecture Pattern

```rust
// src/commands/backtest.rs
pub struct BacktestCommands;

impl BacktestCommands {
    pub async fn evaluate(params: EvaluateParams) -> Result<EvaluateResult>;
    pub async fn tune(params: TuneParams) -> Result<TuneResult>;
    pub async fn train(params: TrainParams) -> Result<TrainResult>;
    // ... etc
}

// CLI uses it:
let result = BacktestCommands::evaluate(params).await?;

// TUI uses it:
let result = BacktestCommands::evaluate(params).await?;
```

---

## Phase 1: Command Execution Layer (Week 1)

**Priority:** 🔴 CRITICAL  
**Goal:** Create unified command execution layer

### Task 1.1: Create Commands Module Structure

```
src/commands/
├── mod.rs
├── backtest.rs      # All backtest commands
├── research.rs      # All research commands
├── validate.rs      # All validate commands
├── algorithm.rs     # All algorithm commands
└── common.rs        # Shared types, progress callbacks
```

**Acceptance Criteria:**
- [ ] Module structure created
- [ ] Each command binary has corresponding command module
- [ ] Commands are async and return Results
- [ ] Progress callback mechanism for long-running operations

### Task 1.2: Extract Backtest Commands

**File:** `src/commands/backtest.rs`

Extract command logic from `src/bin/backtest.rs`:

```rust
pub struct BacktestCommands;

impl BacktestCommands {
    pub async fn evaluate(params: EvaluateParams) -> Result<EvaluateResult>;
    pub async fn sweep(params: SweepParams) -> Result<SweepResult>;
    pub async fn walk_forward(params: WalkForwardParams) -> Result<WalkForwardResult>;
    pub async fn tune(params: TuneParams) -> Result<TuneResult>;
    pub async fn regime_search(params: RegimeSearchParams) -> Result<RegimeSearchResult>;
    pub async fn oos_validate(params: OOSParams) -> Result<OOSResult>;
    pub async fn multi_objective(params: MultiObjectiveParams) -> Result<MultiObjectiveResult>;
    pub async fn regime_optimize(params: RegimeOptimizeParams) -> Result<RegimeOptimizeResult>;
    pub async fn train(params: TrainParams) -> Result<TrainResult>;
    pub async fn simulate(params: SimulateParams) -> Result<SimulateResult>;
    pub async fn grid(params: GridParams) -> Result<GridResult>;
    pub async fn campaign(params: CampaignParams) -> Result<CampaignResult>;
    pub async fn paper(params: PaperParams) -> Result<PaperResult>;
    pub async fn list_algorithms(params: ListAlgorithmsParams) -> Result<ListAlgorithmsResult>;
}
```

**Acceptance Criteria:**
- [ ] All 14 backtest commands extracted
- [ ] CLI binary refactored to use commands module
- [ ] All existing tests pass
- [ ] Progress callbacks implemented

### Task 1.3: Extract Research Commands

**File:** `src/commands/research.rs`

```rust
pub struct ResearchCommands;

impl ResearchCommands {
    pub async fn run(params: RunParams) -> Result<RunResult>;
    pub async fn status(params: StatusParams) -> Result<StatusResult>;
}
```

**Acceptance Criteria:**
- [ ] Both research commands extracted
- [ ] CLI binary refactored
- [ ] Tests pass

### Task 1.4: Extract Validate Commands

**File:** `src/commands/validate.rs`

```rust
pub struct ValidateCommands;

impl ValidateCommands {
    pub async fn run(params: RunParams) -> Result<RunResult>;
    pub async fn presets(params: PresetsParams) -> Result<PresetsResult>;
    pub async fn stages(params: StagesParams) -> Result<StagesResult>;
    pub async fn status(params: StatusParams) -> Result<StatusResult>;
    pub async fn show(params: ShowParams) -> Result<ShowResult>;
}
```

**Acceptance Criteria:**
- [ ] All 5 validate commands extracted
- [ ] CLI binary refactored
- [ ] Tests pass

### Task 1.5: Extract Algorithm Commands

**File:** `src/commands/algorithm.rs`

```rust
pub struct AlgorithmCommands;

impl AlgorithmCommands {
    pub async fn create(params: CreateParams) -> Result<CreateResult>;
    pub async fn list(params: ListParams) -> Result<ListResult>;
    pub async fn show(params: ShowParams) -> Result<ShowResult>;
}
```

**Acceptance Criteria:**
- [ ] All 3 algorithm commands extracted
- [ ] CLI binary refactored
- [ ] Tests pass

### Task 1.6: Progress Callback System

**File:** `src/commands/common.rs`

```rust
pub trait ProgressCallback: Send + Sync {
    fn on_progress(&self, current: usize, total: usize, message: &str);
    fn on_log(&self, level: LogLevel, message: &str);
    fn on_metric(&self, name: &str, value: f64);
}

pub struct NoOpCallback;
impl ProgressCallback for NoOpCallback { /* ... */ }
```

**Acceptance Criteria:**
- [ ] Progress callback trait defined
- [ ] All long-running commands support callbacks
- [ ] Default no-op callback for CLI
- [ ] TUI can provide custom callback

---

## Phase 2: TUI Integration (Week 2)

**Priority:** 🔴 CRITICAL  
**Goal:** Wire TUI menus to command execution layer

### Task 2.1: TUI Command Executor

**File:** `src/ui/command_executor.rs`

```rust
pub struct TUICommandExecutor {
    progress_tx: mpsc::Sender<ProgressEvent>,
}

impl TUICommandExecutor {
    pub async fn execute_backtest_evaluate(&self, params: EvaluateParams) -> Result<EvaluateResult>;
    pub async fn execute_backtest_tune(&self, params: TuneParams) -> Result<TuneResult>;
    // ... etc for all commands
}
```

**Acceptance Criteria:**
- [ ] Command executor created
- [ ] Progress events sent to TUI
- [ ] Results returned to TUI
- [ ] Error handling with user-friendly messages

### Task 2.2: Progress Display Widget

**File:** `src/ui/widgets/progress.rs`

```rust
pub struct ProgressWidget {
    current: usize,
    total: usize,
    message: String,
    metrics: HashMap<String, f64>,
    logs: VecDeque<LogEntry>,
}

pub fn draw_progress(f: &mut Frame, area: Rect, widget: &ProgressWidget);
```

**Acceptance Criteria:**
- [ ] Progress bar visualization
- [ ] Current/total display
- [ ] Status message display
- [ ] Metrics display (optional)
- [ ] Log output (scrollable)

### Task 2.3: Parameter Input Widgets

**File:** `src/ui/widgets/input.rs`

Create simple input widgets:

```rust
// Text input
pub struct TextInput {
    value: String,
    placeholder: String,
}

// Number input
pub struct NumberInput {
    value: f64,
    min: Option<f64>,
    max: Option<f64>,
    step: f64,
}

// Dropdown
pub struct Dropdown<T> {
    options: Vec<T>,
    selected: usize,
}
```

**Acceptance Criteria:**
- [ ] Text input widget
- [ ] Number input widget
- [ ] Dropdown widget
- [ ] Keyboard navigation
- [ ] Validation

### Task 2.4: Wire Missing Commands to TUI

For each missing command, add menu item and wire to executor:

#### Backtest Commands
- [ ] `tune` → Validate → Tune
- [ ] `regime-search` → Validate → Regime → Search
- [ ] `multi-objective` → Validate → Multi-Objective
- [ ] `regime-optimize` → Validate → Regime → Optimize
- [ ] `train` → Validate → Train
- [ ] `simulate` → Validate → Simulate

#### Validate Commands
- [ ] `stages` → Validate → Info → Stages
- [ ] `status` → Validate → Info → Status
- [ ] `show` → Validate → Info → Details

**Acceptance Criteria:**
- [ ] All 9 missing commands accessible via TUI
- [ ] Menu items added to appropriate submenus
- [ ] Commands execute successfully
- [ ] Results displayed in TUI

---

## Phase 3: Parameter Configuration (Week 3)

**Priority:** 🟡 HIGH  
**Goal:** Interactive parameter configuration for all commands

### Task 3.1: Parameter Builder Pattern

**File:** `src/commands/params.rs`

```rust
pub struct EvaluateParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    spread: Option<f64>,
    skew: Option<f64>,
    // ... etc
}

impl EvaluateParamsBuilder {
    pub fn new() -> Self;
    pub fn data_path(self, path: PathBuf) -> Self;
    pub fn algorithm(self, algo: String) -> Self;
    // ... etc
    pub fn build(self) -> Result<EvaluateParams, ValidationError>;
}
```

**Acceptance Criteria:**
- [ ] Builder for each command type
- [ ] Validation on build
- [ ] Sensible defaults
- [ ] Can load from config file

### Task 3.2: Parameter Configuration Screens

For each command, create a configuration screen:

**File:** `src/ui/screens/backtest_config.rs`

```rust
pub struct BacktestConfigScreen {
    params_builder: EvaluateParamsBuilder,
    current_field: ConfigField,
}

pub enum ConfigField {
    DataPath,
    Algorithm,
    Spread,
    Skew,
    // ... etc
}
```

**Acceptance Criteria:**
- [ ] Config screen for each command type
- [ ] Field-by-field navigation
- [ ] Input validation
- [ ] Save/load presets

### Task 3.3: Preset Management

**File:** `src/ui/presets.rs`

```rust
pub struct PresetManager;

impl PresetManager {
    pub fn save_preset(name: &str, params: &dyn Serialize) -> Result<()>;
    pub fn load_preset(name: &str) -> Result<Box<dyn Deserialize>>;
    pub fn list_presets() -> Result<Vec<String>>;
}
```

**Acceptance Criteria:**
- [ ] Save presets from TUI
- [ ] Load presets in TUI
- [ ] List available presets
- [ ] Preset validation

---

## Phase 4: Results Display (Week 4)

**Priority:** 🟡 HIGH  
**Goal:** Display command results in TUI

### Task 4.1: Results Display Widgets

**File:** `src/ui/widgets/results.rs`

```rust
// Table widget for tabular results
pub struct TableWidget {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    selected_row: usize,
}

// Metrics widget for key-value pairs
pub struct MetricsWidget {
    metrics: Vec<(String, String)>,
}

// Chart widget (simple ASCII)
pub struct ChartWidget {
    data: Vec<(f64, f64)>,
    title: String,
}
```

**Acceptance Criteria:**
- [ ] Table widget
- [ ] Metrics widget
- [ ] Simple chart widget
- [ ] Scrollable content
- [ ] Export to file option

### Task 4.2: Command-Specific Result Screens

For each command type, create a result display screen:

**File:** `src/ui/screens/backtest_results.rs`

```rust
pub struct BacktestResultsScreen {
    result: EvaluateResult,
    view_mode: ViewMode, // Summary, Detailed, Chart
}
```

**Acceptance Criteria:**
- [ ] Result screen for each command type
- [ ] Multiple view modes
- [ ] Export functionality
- [ ] Navigation back to menu

---

## Implementation Checklist

### Week 1: Command Execution Layer
- [ ] Create `src/commands/` module structure
- [ ] Extract all backtest commands
- [ ] Extract all research commands
- [ ] Extract all validate commands
- [ ] Extract all algorithm commands
- [ ] Implement progress callback system
- [ ] Refactor CLI binaries to use commands
- [ ] All tests pass

### Week 2: TUI Integration
- [ ] Create TUI command executor
- [ ] Implement progress display widget
- [ ] Implement basic input widgets
- [ ] Wire all missing commands to TUI menus
- [ ] Test all commands from TUI

### Week 3: Parameter Configuration
- [ ] Create parameter builder pattern
- [ ] Implement parameter configuration screens
- [ ] Implement preset management
- [ ] Test parameter input and validation

### Week 4: Results Display
- [ ] Implement results display widgets
- [ ] Create command-specific result screens
- [ ] Implement export functionality
- [ ] Test end-to-end workflows

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| CLI-TUI Parity | 100% | All 22 CLI commands accessible via TUI |
| Test Coverage | ≥80% | Line coverage for new code |
| User Task Completion | 100% | All CLI tasks achievable via TUI |
| Response Time | <100ms | UI interactions |
| Code Reuse | ≥90% | Shared between CLI and TUI |

---

## Simplified Implementation Approach

### Option A: Minimal Viable TUI (Recommended for First Phase)

**Focus:** Get all commands working, simple UI

1. **Command Execution Layer** (Week 1)
   - Extract commands to shared module
   - Keep CLI working

2. **Simple TUI Integration** (Week 2)
   - Add menu items for missing commands
   - Use default parameters (no config UI yet)
   - Show results in simple text format
   - Progress shown as spinner + message

3. **Basic Parameter Input** (Week 3)
   - Add simple parameter input for critical commands
   - Use text input for paths, numbers
   - Dropdown for algorithm selection

4. **Results Display** (Week 4)
   - Simple table for results
   - Key metrics highlighted
   - Export to file option

**Result:** Fully functional TUI with all commands accessible, minimal UI complexity

### Option B: Full-Featured TUI (Future Enhancement)

After Option A is complete, enhance with:
- Rich parameter configuration screens
- Advanced result visualization
- Preset management UI
- Interactive charts
- Multi-step wizards

---

## File Structure

```
src/
├── commands/              # NEW: Unified command execution
│   ├── mod.rs
│   ├── backtest.rs
│   ├── research.rs
│   ├── validate.rs
│   ├── algorithm.rs
│   ├── params.rs         # Parameter builders
│   └── common.rs         # Progress callbacks, shared types
│
├── bin/                  # CLI binaries (refactored to use commands)
│   ├── backtest.rs
│   ├── research.rs
│   ├── validate.rs
│   └── algorithm.rs
│
└── ui/                   # TUI (enhanced to use commands)
    ├── command_executor.rs  # NEW: TUI command execution
    ├── widgets/
    │   ├── progress.rs      # NEW: Progress display
    │   ├── input.rs          # NEW: Input widgets
    │   └── results.rs        # NEW: Results display
    ├── screens/
    │   ├── backtest_config.rs  # NEW: Parameter config
    │   ├── backtest_results.rs  # NEW: Results display
    │   └── ... (existing screens)
    └── presets.rs            # NEW: Preset management
```

---

## Testing Strategy

### Unit Tests
- [ ] Each command function has unit tests
- [ ] Parameter builders validate correctly
- [ ] Progress callbacks work

### Integration Tests
- [ ] CLI and TUI produce same results for same inputs
- [ ] Commands execute successfully from both interfaces
- [ ] Progress updates work in TUI

### Manual Testing
- [ ] All 22 commands accessible via TUI
- [ ] All commands execute successfully
- [ ] Results display correctly
- [ ] Error messages are user-friendly

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Refactoring breaks CLI | High | Comprehensive tests, incremental refactoring |
| TUI complexity | Medium | Start with Option A (minimal), enhance later |
| Parameter input UX | Medium | Use simple inputs first, enhance iteratively |
| Performance | Low | Commands are async, TUI updates are non-blocking |

---

## Next Steps

1. **Immediate (This Week)**
   - Review and approve this document
   - Set up `src/commands/` module structure
   - Start extracting backtest commands

2. **Week 1**
   - Complete command execution layer
   - Refactor CLI binaries
   - All tests passing

3. **Week 2**
   - TUI integration
   - Wire all missing commands
   - Basic progress display

4. **Week 3-4**
   - Parameter configuration
   - Results display
   - Testing and refinement

---

## Appendix: Command Mapping

### Complete CLI → TUI Mapping

| CLI Command | TUI Menu Path | Status |
|-------------|---------------|--------|
| `backtest evaluate` | Validate → Backtest | ✅ |
| `backtest sweep` | Validate → Sweep | ✅ |
| `backtest walk-forward` | Validate → Walk-Forward | ✅ |
| `backtest tune` | Validate → Tune | ❌ → ✅ |
| `backtest regime-search` | Validate → Regime → Search | ❌ → ✅ |
| `backtest oos-validate` | Validate → OOS | ✅ |
| `backtest multi-objective` | Validate → Multi-Objective | ❌ → ✅ |
| `backtest regime-optimize` | Validate → Regime → Optimize | ❌ → ✅ |
| `backtest train` | Validate → Train | ❌ → ✅ |
| `backtest simulate` | Validate → Simulate | ❌ → ✅ |
| `backtest grid` | Validate → Grid | ✅ |
| `backtest campaign` | Validate → Campaign | ✅ |
| `backtest paper` | Trade → Paper | ✅ |
| `backtest algorithms` | Algorithms → List | ✅ |
| `research run` | Research → Run | ✅ |
| `research status` | Research → Status | ✅ |
| `validate run` | Validate → Run | ✅ |
| `validate presets` | Validate → Presets | ✅ |
| `validate stages` | Validate → Info → Stages | ❌ → ✅ |
| `validate status` | Validate → Info → Status | ❌ → ✅ |
| `validate show` | Validate → Info → Details | ❌ → ✅ |
| `algorithm create` | Algorithms → Create | ✅ |
| `algorithm list` | Algorithms → List | ✅ |
| `algorithm show` | Algorithms → Show | ✅ |

**Legend:**
- ✅ = Already implemented
- ❌ → ✅ = Needs implementation (target)

---

*Document Version: 0.3*  
*Last Updated: 2025-01-XX*  
*Next Review: After Phase 1 completion*


