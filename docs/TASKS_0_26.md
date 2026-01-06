# TUI Implementation Tasks - Complete CLI Parity

**Version:** 0.26  
**Date:** 2025-01-XX  
**Status:** Planning  
**Goal:** Achieve 100% CLI-TUI parity with all options accessible via visual tools

This document defines all tasks required to implement complete TUI functionality as specified in `TUI_REQUIREMENTS_V0.4.md`.

---

## Task Organization

Tasks are organized by:
- **Phase** (1-4): Implementation phases
- **Task ID** (T-XX.XX): Unique task identifier
- **Priority**: 🔴 Critical, 🟡 High, 🟢 Medium
- **Dependencies**: Tasks that must be completed first

---

## Phase 1: Command Execution Layer (Week 1-2)

### T-1.1: Create Commands Module Structure
**Priority:** 🔴 Critical  
**Dependencies:** None  
**Estimated Time:** 2 hours

**Description:** Create the foundational module structure for unified command execution.

**Tasks:**
- [ ] Create `src/commands/` directory
- [ ] Create `src/commands/mod.rs` with module exports
- [ ] Create `src/commands/backtest.rs` (empty initially)
- [ ] Create `src/commands/research.rs` (empty initially)
- [ ] Create `src/commands/validate.rs` (empty initially)
- [ ] Create `src/commands/algorithm.rs` (empty initially)
- [ ] Create `src/commands/common.rs` for shared types
- [ ] Create `src/commands/params/` directory
- [ ] Create `src/commands/params/mod.rs`
- [ ] Create `src/commands/params/backtest_params.rs`
- [ ] Create `src/commands/params/research_params.rs`
- [ ] Create `src/commands/params/validate_params.rs`
- [ ] Create `src/commands/params/algorithm_params.rs`

**Acceptance Criteria:**
- [ ] All files compile without errors
- [ ] Module structure matches requirements document
- [ ] All modules properly exported

---

### T-1.2: Implement Progress Callback System
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1  
**Estimated Time:** 4 hours

**Description:** Create the progress callback mechanism for long-running commands.

**Tasks:**
- [ ] Define `ProgressCallback` trait in `common.rs`
- [ ] Define `ProgressEvent` enum (Started, Progress, Metric, Log, Completed, Error)
- [ ] Implement `NoOpCallback` for CLI (default)
- [ ] Implement `TUICallback` for TUI (sends events via channel)
- [ ] Add `LogLevel` enum (Info, Warn, Error, Debug)
- [ ] Add thread-safety markers (`Send + Sync`)
- [ ] Write unit tests for callback system

**Acceptance Criteria:**
- [ ] Callback trait compiles and works
- [ ] Events can be sent from async contexts
- [ ] TUI callback sends events to channel
- [ ] Unit tests pass

---

### T-1.3: Extract Backtest Evaluate Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `evaluate` command logic from CLI to shared module.

**Tasks:**
- [ ] Define `EvaluateParams` struct with ALL 20+ parameters
- [ ] Create `EvaluateParamsBuilder` with validation
- [ ] Extract evaluation logic from `src/bin/backtest.rs`
- [ ] Create `evaluate()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `EvaluateResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `EvaluateParams`
- [ ] Command executes successfully from CLI
- [ ] Progress callbacks work
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.4: Extract Backtest Tune Command (MM Only)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `tune` (grid search) command - MM algorithms only.

**Tasks:**
- [ ] Define `TuneParams` struct (spreads, skews, high_entropies, fill_probs)
- [ ] Create `TuneParamsBuilder` with validation
- [ ] Add algorithm type validation (must be MM algorithm)
- [ ] Extract grid search logic from CLI
- [ ] Create `tune()` function in `BacktestCommands`
- [ ] Add progress callback support (iteration updates)
- [ ] Define `TuneResult` struct (all results + best)
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests including algorithm type validation
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `TuneParams`
- [ ] Algorithm type validation works (rejects non-MM)
- [ ] Grid search executes correctly
- [ ] Progress updates sent during execution
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.5: Extract Backtest Regime Search Command (MM Only)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `regime-search` command - MM algorithms only.

**Tasks:**
- [ ] Define `RegimeSearchParams` struct (6 comma-separated lists)
- [ ] Create `RegimeSearchParamsBuilder` with validation
- [ ] Add algorithm type validation (must be MM algorithm)
- [ ] Extract regime search logic from CLI
- [ ] Create `regime_search()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `RegimeSearchResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `RegimeSearchParams`
- [ ] Algorithm type validation works
- [ ] Regime search executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.6: Extract Backtest Multi-Objective Command (MM Only)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `multi-objective` command - MM algorithms only.

**Tasks:**
- [ ] Define `MultiObjectiveParams` struct (grid params + 4 weights)
- [ ] Create `MultiObjectiveParamsBuilder` with validation
- [ ] Add weight sum validation (must sum to 1.0)
- [ ] Add algorithm type validation (must be MM algorithm)
- [ ] Extract multi-objective optimization logic from CLI
- [ ] Create `multi_objective()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `MultiObjectiveResult` struct (Pareto frontier)
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `MultiObjectiveParams`
- [ ] Weight validation works (sums to 1.0)
- [ ] Algorithm type validation works
- [ ] Pareto frontier computed correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.7: Extract Backtest Regime Optimize Command (MM Only)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `regime-optimize` command - MM algorithms only.

**Tasks:**
- [ ] Define `RegimeOptimizeParams` struct
- [ ] Create `RegimeOptimizeParamsBuilder` with validation
- [ ] Add algorithm type validation (must be MM algorithm)
- [ ] Extract regime optimization logic from CLI
- [ ] Create `regime_optimize()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `RegimeOptimizeResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `RegimeOptimizeParams`
- [ ] Algorithm type validation works
- [ ] Regime optimization executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.8: Extract Backtest Train Command (MM Only - ML Spread/Skew)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `train` command - ML Spread/Skew algorithm only.

**Tasks:**
- [ ] Define `TrainParams` struct (train_ratio + 5 ML param grids)
- [ ] Create `TrainParamsBuilder` with validation
- [ ] Add algorithm type validation (must be ML Spread/Skew)
- [ ] Extract ML training logic from CLI
- [ ] Create `train()` function in `BacktestCommands`
- [ ] Add progress callback support (training iterations)
- [ ] Define `TrainResult` struct (trained weights)
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `TrainParams`
- [ ] Algorithm type validation works (ML only)
- [ ] ML training executes correctly
- [ ] Trained weights saved correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.9: Extract Backtest Walk-Forward ML Command (MM Only)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 6 hours

**Description:** Extract `walk-forward-ml` command - MM algorithms only.

**Tasks:**
- [ ] Define `WalkForwardMLParams` struct
- [ ] Create `WalkForwardMLParamsBuilder` with validation
- [ ] Add algorithm type validation (must be MM algorithm)
- [ ] Extract walk-forward ML logic from CLI
- [ ] Create `walk_forward_ml()` function in `BacktestCommands`
- [ ] Add progress callback support (fold updates)
- [ ] Define `WalkForwardMLResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `WalkForwardMLParams`
- [ ] Algorithm type validation works
- [ ] Walk-forward ML executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.10: Extract Backtest Sweep Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `sweep` command - parameter sweep (both algorithm types).

**Tasks:**
- [ ] Define `SweepParams` struct with ALL options
- [ ] Create `SweepParamsBuilder` with validation
- [ ] Extract sweep logic from CLI
- [ ] Create `sweep()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `SweepResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `SweepParams`
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.11: Extract Backtest Walk-Forward Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `walk-forward` command - walk-forward validation (both algorithm types).

**Tasks:**
- [ ] Define `WalkForwardParams` struct with ALL options
- [ ] Create `WalkForwardParamsBuilder` with validation
- [ ] Extract walk-forward logic from CLI
- [ ] Create `walk_forward()` function in `BacktestCommands`
- [ ] Add progress callback support (fold updates)
- [ ] Define `WalkForwardResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `WalkForwardParams`
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.12: Extract Backtest OOS-Validate Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `oos-validate` command - out-of-sample validation (both algorithm types).

**Tasks:**
- [ ] Define `OOSValidateParams` struct with ALL options
- [ ] Create `OOSValidateParamsBuilder` with validation
- [ ] Extract OOS validation logic from CLI
- [ ] Create `oos_validate()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `OOSValidateResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `OOSValidateParams`
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.13: Extract Backtest Simulate Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `simulate` command - campaign simulation (both algorithm types).

**Tasks:**
- [ ] Define `SimulateParams` struct with ALL options
- [ ] Create `SimulateParamsBuilder` with validation
- [ ] Extract simulation logic from CLI
- [ ] Create `simulate()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `SimulateResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `SimulateParams`
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.14: Extract Backtest Grid Command (MM Only)
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `grid` command - grid search (MM algorithms only).

**Tasks:**
- [ ] Define `GridParams` struct with ALL options
- [ ] Create `GridParamsBuilder` with validation
- [ ] Add algorithm type validation (must be MM algorithm)
- [ ] Extract grid search logic from CLI
- [ ] Create `grid()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `GridResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `GridParams`
- [ ] Algorithm type validation works (rejects non-MM)
- [ ] Grid search executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.15: Extract Backtest Campaign Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `campaign` command - validation campaign (both algorithm types).

**Tasks:**
- [ ] Define `CampaignParams` struct with ALL options
- [ ] Create `CampaignParamsBuilder` with validation
- [ ] Extract campaign logic from CLI
- [ ] Create `campaign()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `CampaignResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `CampaignParams`
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.16: Extract Backtest Paper Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 3 hours

**Description:** Extract `paper` command - paper trading (both algorithm types).

**Tasks:**
- [ ] Define `PaperParams` struct with ALL options
- [ ] Create `PaperParamsBuilder` with validation
- [ ] Extract paper trading logic from CLI
- [ ] Create `paper()` function in `BacktestCommands`
- [ ] Add progress callback support
- [ ] Define `PaperResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] All CLI options mapped to `PaperParams`
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.17: Extract Backtest List Algorithms Command
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 1 hour

**Description:** Extract `list_algorithms` command - list algorithms (info only).

**Tasks:**
- [ ] Define `ListAlgorithmsParams` struct (if needed, may be empty)
- [ ] Create `ListAlgorithmsParamsBuilder` (if needed)
- [ ] Extract list algorithms logic from CLI
- [ ] Create `list_algorithms()` function in `BacktestCommands`
- [ ] Define `ListAlgorithmsResult` struct
- [ ] Update CLI binary to use extracted command
- [ ] Write unit tests
- [ ] Ensure existing tests still pass

**Acceptance Criteria:**
- [ ] Command executes correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.18: Extract Research Commands
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 8 hours

**Description:** Extract all research commands.

**Tasks:**
- [ ] Define `RunParams` struct (data, output, symbol, dates, min_samples, etc.)
- [ ] Define `StatusParams` struct (store, symbol, verbose, top_signals)
- [ ] Create parameter builders with validation
- [ ] Extract `run()` command logic from CLI
- [ ] Extract `status()` command logic from CLI
- [ ] Create `ResearchCommands` struct
- [ ] Implement `run()` and `status()` functions
- [ ] Add progress callback support
- [ ] Define result structs
- [ ] Update CLI binary to use extracted commands
- [ ] Write unit tests
- [ ] Ensure existing tests pass

**Acceptance Criteria:**
- [ ] Both research commands extracted
- [ ] All CLI options mapped to parameters
- [ ] Commands execute correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.19: Extract Validate Commands
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 10 hours

**Description:** Extract all validate commands.

**Tasks:**
- [ ] Define `RunParams` struct (config, stages, data, results, preset, etc.)
- [ ] Define `PresetsParams` struct (info only)
- [ ] Define `StagesParams` struct (info only)
- [ ] Define `StatusParams` struct (results, last)
- [ ] Define `ShowParams` struct (results, run_id, json, verbose)
- [ ] Create parameter builders with validation
- [ ] Extract all 5 command logics from CLI
- [ ] Create `ValidateCommands` struct
- [ ] Implement all 5 functions
- [ ] Add progress callback support for `run()`
- [ ] Define result structs
- [ ] Update CLI binary to use extracted commands
- [ ] Write unit tests
- [ ] Ensure existing tests pass

**Acceptance Criteria:**
- [ ] All 5 validate commands extracted
- [ ] All CLI options mapped to parameters
- [ ] Commands execute correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.20: Extract Algorithm Commands
**Priority:** 🔴 Critical  
**Dependencies:** T-1.1, T-1.2  
**Estimated Time:** 8 hours

**Description:** Extract all algorithm commands.

**Tasks:**
- [ ] Define `CreateParams` struct (research, output, symbol, name, strategy, etc.)
- [ ] Define `ListParams` struct (store, symbol, strategy, name, active_only, limit)
- [ ] Define `ShowParams` struct (store, id, json, verbose)
- [ ] Create parameter builders with validation
- [ ] Extract all 3 command logics from CLI
- [ ] Create `AlgorithmCommands` struct
- [ ] Implement `create()`, `list()`, and `show()` functions
- [ ] Add progress callback support for `create()` (if validation enabled)
- [ ] Define result structs
- [ ] Update CLI binary to use extracted commands
- [ ] Write unit tests
- [ ] Ensure existing tests pass

**Acceptance Criteria:**
- [ ] All 3 algorithm commands extracted
- [ ] All CLI options mapped to parameters
- [ ] Commands execute correctly
- [ ] Results match original CLI output
- [ ] All tests pass

---

### T-1.21: Refactor All CLI Binaries
**Priority:** 🔴 Critical  
**Dependencies:** T-1.3 through T-1.13  
**Estimated Time:** 12 hours

**Description:** Update all CLI binaries to use extracted command modules.

**Tasks:**
- [ ] Update `src/bin/backtest.rs` to use `BacktestCommands`
- [ ] Update `src/bin/research.rs` to use `ResearchCommands`
- [ ] Update `src/bin/validate.rs` to use `ValidateCommands`
- [ ] Update `src/bin/algorithm.rs` to use `AlgorithmCommands`
- [ ] Remove duplicate command logic from binaries
- [ ] Keep CLI argument parsing in binaries
- [ ] Convert CLI args to parameter structs
- [ ] Call command functions with parameters
- [ ] Handle results and output formatting
- [ ] Run full test suite
- [ ] Fix any regressions

**Acceptance Criteria:**
- [ ] All CLI binaries compile
- [ ] All CLI commands work identically to before
- [ ] No duplicate code between CLI and commands module
- [ ] All existing tests pass
- [ ] CLI output matches original

---

## Phase 2: Visual Parameter Configuration (Week 3-4)

### T-2.1: Create Text Input Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 4 hours

**Description:** Create reusable text input widget for TUI.

**Tasks:**
- [ ] Create `src/ui/widgets/params/text_input.rs`
- [ ] Implement `TextInputWidget` struct
- [ ] Implement cursor movement (left/right, home/end)
- [ ] Implement character insert/delete
- [ ] Implement placeholder text display
- [ ] Implement max length constraint
- [ ] Implement validation callback
- [ ] Implement rendering function
- [ ] Write unit tests
- [ ] Write integration tests with TUI

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] All keyboard navigation works
- [ ] Validation displays errors
- [ ] Placeholder text shows when empty
- [ ] Max length enforced
- [ ] Tests pass

---

### T-2.2: Create Number Input Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 6 hours

**Description:** Create reusable number input widget with slider support.

**Tasks:**
- [ ] Create `src/ui/widgets/params/number_input.rs`
- [ ] Implement `NumberInputWidget` struct
- [ ] Implement increment/decrement (up/down arrows, +/- keys)
- [ ] Implement min/max validation
- [ ] Implement step snapping
- [ ] Implement format display (decimals, percentage, basis points)
- [ ] Implement slider mode (optional)
- [ ] Implement rendering function
- [ ] Write unit tests
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Number input works (keyboard and increment/decrement)
- [ ] Min/max validation works
- [ ] Formatting displays correctly
- [ ] Slider mode works (if implemented)
- [ ] Tests pass

---

### T-2.3: Create Comma-Separated List Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 8 hours

**Description:** Create widget for editing comma-separated value lists (e.g., "1,2,3,4,5").

**Tasks:**
- [ ] Create `src/ui/widgets/params/comma_list.rs`
- [ ] Implement `CommaListWidget` struct
- [ ] Implement parsing comma-separated string to Vec<f64>
- [ ] Implement adding items
- [ ] Implement removing items
- [ ] Implement editing individual items
- [ ] Implement validation (no duplicates, sorted, etc.)
- [ ] Implement visual list display
- [ ] Implement quick presets
- [ ] Implement rendering function
- [ ] Write unit tests
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Can parse comma-separated strings
- [ ] Can add/remove/edit items
- [ ] Validation works
- [ ] Visual list displays correctly
- [ ] Quick presets work
- [ ] Tests pass

---

### T-2.4: Create Toggle Widget
**Priority:** 🟢 Medium  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 2 hours

**Description:** Create boolean toggle widget.

**Tasks:**
- [ ] Create `src/ui/widgets/params/toggle.rs`
- [ ] Implement `ToggleWidget` struct
- [ ] Implement toggle action (space/enter)
- [ ] Implement visual indicator (checkbox or switch)
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Toggle works (space/enter)
- [ ] Visual indicator updates
- [ ] Tests pass

---

### T-2.5: Create Path Input Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 6 hours

**Description:** Create path input widget with file browser support.

**Tasks:**
- [ ] Create `src/ui/widgets/params/path_input.rs`
- [ ] Implement `PathInputWidget` struct
- [ ] Implement path completion (tab)
- [ ] Implement file browser integration (optional)
- [ ] Implement existence validation
- [ ] Implement relative/absolute path support
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Path input works
- [ ] Path completion works (basic)
- [ ] Validation works (existence check)
- [ ] Tests pass

---

### T-2.6: Create Dropdown Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 6 hours

**Description:** Create dropdown widget for algorithm selection, etc.

**Tasks:**
- [ ] Create `src/ui/widgets/params/dropdown.rs`
- [ ] Implement `DropdownWidget<T>` struct
- [ ] Implement expandable list
- [ ] Implement keyboard navigation
- [ ] Implement type-to-search filter
- [ ] Implement custom rendering
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Dropdown expands/collapses
- [ ] Keyboard navigation works
- [ ] Type-to-search works
- [ ] Tests pass

---

### T-2.7: Create Slider Widget
**Priority:** 🟢 Medium  
**Dependencies:** Phase 1 complete  
**Estimated Time:** 4 hours

**Description:** Create slider widget for ranges (e.g., objective weights).

**Tasks:**
- [ ] Create `src/ui/widgets/params/slider.rs`
- [ ] Implement `SliderWidget` struct
- [ ] Implement horizontal slider bar
- [ ] Implement mouse/keyboard adjustment
- [ ] Implement value display
- [ ] Implement min/max labels
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Slider adjusts value
- [ ] Value displays correctly
- [ ] Min/max labels show
- [ ] Tests pass

---

### T-2.8: Create Backtest Evaluate Config Screen
**Priority:** 🟡 High  
**Dependencies:** T-2.1 through T-2.7  
**Estimated Time:** 12 hours

**Description:** Create parameter configuration screen for backtest evaluate command.

**Tasks:**
- [ ] Create `src/ui/screens/params/backtest_evaluate.rs`
- [ ] Define `BacktestEvaluateConfigScreen` struct
- [ ] Define `EvaluateField` enum (all 20+ fields)
- [ ] Implement field navigation (up/down arrows)
- [ ] Implement field editing with appropriate widgets
- [ ] Implement parameter groups/tabs (Basic, Advanced, Output)
- [ ] Implement validation
- [ ] Implement save/load presets
- [ ] Implement rendering function
- [ ] Write unit tests
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] Screen compiles and renders correctly
- [ ] All 20+ parameters editable
- [ ] Field navigation works
- [ ] Validation works
- [ ] Presets work
- [ ] Tests pass

---

### T-2.9: Create Backtest Tune Config Screen (MM Only)
**Priority:** 🟡 High  
**Dependencies:** T-2.1 through T-2.7  
**Estimated Time:** 10 hours

**Description:** Create parameter configuration screen for backtest tune command (MM only).

**Tasks:**
- [ ] Create `src/ui/screens/params/backtest_tune.rs`
- [ ] Define `BacktestTuneConfigScreen` struct
- [ ] Add algorithm type validation (must be MM)
- [ ] Implement comma-list widgets for all 4 parameter grids
- [ ] Implement grid combination preview
- [ ] Implement total combinations calculation
- [ ] Implement estimated time calculation
- [ ] Implement rendering function
- [ ] Show "(MM)" indicator in title
- [ ] Write unit tests
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] Screen compiles and renders correctly
- [ ] Algorithm type validation works
- [ ] All parameter grids editable
- [ ] Grid preview shows correctly
- [ ] Combination count accurate
- [ ] Tests pass

---

### T-2.10: Create Backtest Multi-Objective Config Screen (MM Only)
**Priority:** 🟡 High  
**Dependencies:** T-2.1 through T-2.7  
**Estimated Time:** 10 hours

**Description:** Create parameter configuration screen for multi-objective command (MM only).

**Tasks:**
- [ ] Create `src/ui/screens/params/backtest_multi_objective.rs`
- [ ] Define `MultiObjectiveConfigScreen` struct
- [ ] Add algorithm type validation (must be MM)
- [ ] Implement slider widgets for 4 objective weights
- [ ] Implement weight sum validation (must sum to 1.0)
- [ ] Implement visual weight distribution
- [ ] Implement real-time validation
- [ ] Implement rendering function
- [ ] Show "(MM)" indicator in title
- [ ] Write unit tests
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] Screen compiles and renders correctly
- [ ] Algorithm type validation works
- [ ] Weight sliders work
- [ ] Weight sum validation works
- [ ] Visual distribution shows correctly
- [ ] Tests pass

---

### T-2.11: Create Remaining Config Screens
**Priority:** 🟡 High  
**Dependencies:** T-2.1 through T-2.7  
**Estimated Time:** 30 hours

**Description:** Create configuration screens for all remaining commands.

**Screens to create:**
- [ ] `backtest_regime_search.rs` (MM only)
- [ ] `backtest_regime_optimize.rs` (MM only)
- [ ] `backtest_train.rs` (MM only - ML Spread/Skew)
- [ ] `backtest_walk_forward_ml.rs` (MM only)
- [ ] `backtest_sweep.rs` (both)
- [ ] `backtest_walk_forward.rs` (both)
- [ ] `backtest_oos_validate.rs` (both)
- [ ] `backtest_simulate.rs` (both)
- [ ] `backtest_grid.rs` (MM only)
- [ ] `backtest_campaign.rs` (both)
- [ ] `backtest_paper.rs` (both)
- [ ] `research_run.rs`
- [ ] `research_status.rs` (info only)
- [ ] `validate_run.rs`
- [ ] `validate_status.rs` (info only)
- [ ] `validate_show.rs` (info only)
- [ ] `algorithm_create.rs`
- [ ] `algorithm_list.rs` (info only)
- [ ] `algorithm_show.rs` (info only)

**For each screen:**
- [ ] Create file with struct definition
- [ ] Implement field navigation
- [ ] Implement field editing with appropriate widgets
- [ ] Add algorithm type validation if MM-only
- [ ] Show "(MM)" indicator if MM-only
- [ ] Implement validation
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] All 19 screens created
- [ ] All screens compile and render
- [ ] Algorithm type validation where needed
- [ ] All parameters editable
- [ ] Tests pass

---

### T-2.12: Implement Preset Management
**Priority:** 🟢 Medium  
**Dependencies:** T-2.8 through T-2.11  
**Estimated Time:** 8 hours

**Description:** Implement preset save/load functionality.

**Tasks:**
- [ ] Create `src/ui/presets.rs`
- [ ] Implement `PresetManager` struct
- [ ] Implement `save_preset()` function
- [ ] Implement `load_preset()` function
- [ ] Implement `list_presets()` function
- [ ] Implement `quick_presets()` function (built-in presets)
- [ ] Implement preset file storage (JSON)
- [ ] Implement preset validation
- [ ] Add preset menu to config screens
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Presets can be saved
- [ ] Presets can be loaded
- [ ] Preset list works
- [ ] Quick presets work
- [ ] Preset validation works
- [ ] Tests pass

---

## Phase 3: Visual Results Display (Week 5-6)

### T-3.1: Create Metrics Dashboard Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 2 complete  
**Estimated Time:** 6 hours

**Description:** Create widget for displaying key metrics.

**Tasks:**
- [ ] Create `src/ui/widgets/results/metrics_dashboard.rs`
- [ ] Implement `MetricsDashboardWidget` struct
- [ ] Implement `Metric` struct (name, value, format, trend, color)
- [ ] Implement metric value types (Number, Percentage, Integer, String, Boolean)
- [ ] Implement layout options (Grid, List, Cards)
- [ ] Implement color coding (green/red for trends)
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Metrics display correctly
- [ ] Color coding works
- [ ] Trend indicators work
- [ ] Tests pass

---

### T-3.2: Create Table Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 2 complete  
**Estimated Time:** 8 hours

**Description:** Create sortable, scrollable table widget.

**Tasks:**
- [ ] Create `src/ui/widgets/results/table.rs`
- [ ] Implement `TableWidget` struct
- [ ] Implement `TableHeader` struct (name, width, align, sortable)
- [ ] Implement `TableRow` struct
- [ ] Implement sortable columns (click header)
- [ ] Implement scrollable rows
- [ ] Implement row selection
- [ ] Implement column resizing (optional)
- [ ] Implement export to CSV (optional)
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Table displays correctly
- [ ] Sorting works
- [ ] Scrolling works
- [ ] Row selection works
- [ ] Tests pass

---

### T-3.3: Create Chart Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 2 complete  
**Estimated Time:** 10 hours

**Description:** Create ASCII/Unicode chart widget.

**Tasks:**
- [ ] Create `src/ui/widgets/results/chart.rs`
- [ ] Implement `ChartWidget` struct
- [ ] Implement `DataPoint` struct
- [ ] Implement chart types (Line, Bar, Scatter, Heatmap)
- [ ] Implement axis labels
- [ ] Implement legend
- [ ] Implement multiple series support
- [ ] Implement auto-scaling
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Line charts render correctly
- [ ] Bar charts render correctly
- [ ] Scatter plots render correctly
- [ ] Heatmaps render correctly
- [ ] Auto-scaling works
- [ ] Tests pass

---

### T-3.4: Create Pareto Frontier Widget
**Priority:** 🟢 Medium  
**Dependencies:** Phase 2 complete  
**Estimated Time:** 8 hours

**Description:** Create widget for displaying Pareto frontier (multi-objective results).

**Tasks:**
- [ ] Create `src/ui/widgets/results/pareto.rs`
- [ ] Implement `ParetoFrontierWidget` struct
- [ ] Implement `ParetoSolution` struct
- [ ] Implement 2D scatter plot rendering
- [ ] Implement solution selection
- [ ] Implement axis selection (which objectives to plot)
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Pareto frontier displays correctly
- [ ] Solution selection works
- [ ] Axis selection works
- [ ] Tests pass

---

### T-3.5: Create Enhanced Progress Widget
**Priority:** 🟡 High  
**Dependencies:** Phase 2 complete  
**Estimated Time:** 6 hours

**Description:** Create enhanced progress widget with metrics and logs.

**Tasks:**
- [ ] Create `src/ui/widgets/results/progress.rs`
- [ ] Implement `ProgressWidget` struct
- [ ] Implement progress bar visualization
- [ ] Implement current/total display
- [ ] Implement status message display
- [ ] Implement metrics display
- [ ] Implement log output (scrollable)
- [ ] Implement ETA calculation
- [ ] Implement elapsed time display
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Widget compiles and renders correctly
- [ ] Progress bar displays correctly
- [ ] Metrics display correctly
- [ ] Logs display correctly
- [ ] ETA calculation works
- [ ] Tests pass

---

### T-3.6: Create Backtest Evaluate Results Screen
**Priority:** 🟡 High  
**Dependencies:** T-3.1 through T-3.5  
**Estimated Time:** 8 hours

**Description:** Create results display screen for backtest evaluate command.

**Tasks:**
- [ ] Create `src/ui/screens/results/backtest_evaluate.rs`
- [ ] Define `BacktestEvaluateResultsScreen` struct
- [ ] Define `ViewMode` enum (Summary, Detailed, EquityCurve, TradeLog, Inventory)
- [ ] Implement summary view (metrics dashboard)
- [ ] Implement detailed view (all metrics + statistics)
- [ ] Implement equity curve view (chart)
- [ ] Implement trade log view (table)
- [ ] Implement inventory view (chart)
- [ ] Implement view mode switching
- [ ] Implement export functionality
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Screen compiles and renders correctly
- [ ] All view modes work
- [ ] Metrics display correctly
- [ ] Charts render correctly
- [ ] Tables render correctly
- [ ] Export works
- [ ] Tests pass

---

### T-3.7: Create Backtest Tune Results Screen
**Priority:** 🟡 High  
**Dependencies:** T-3.1 through T-3.5  
**Estimated Time:** 10 hours

**Description:** Create results display screen for backtest tune command.

**Tasks:**
- [ ] Create `src/ui/screens/results/backtest_tune.rs`
- [ ] Define `BacktestTuneResultsScreen` struct
- [ ] Define `TuneViewMode` enum (TopResults, FullTable, Heatmap, Pareto)
- [ ] Implement top results view (table of top 10)
- [ ] Implement full table view (all results, sortable)
- [ ] Implement heatmap view (2D visualization of spread vs skew)
- [ ] Implement Pareto view (if multi-objective)
- [ ] Implement view mode switching
- [ ] Implement result selection
- [ ] Implement export functionality
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Screen compiles and renders correctly
- [ ] All view modes work
- [ ] Top results display correctly
- [ ] Full table displays correctly
- [ ] Heatmap renders correctly
- [ ] Export works
- [ ] Tests pass

---

### T-3.8: Create Remaining Results Screens
**Priority:** 🟡 High  
**Dependencies:** T-3.1 through T-3.5  
**Estimated Time:** 25 hours

**Description:** Create results display screens for all remaining commands.

**Screens to create:**
- [ ] `backtest_regime_search.rs`
- [ ] `backtest_multi_objective.rs` (with Pareto frontier)
- [ ] `backtest_regime_optimize.rs`
- [ ] `backtest_train.rs`
- [ ] `backtest_walk_forward_ml.rs`
- [ ] `backtest_sweep.rs`
- [ ] `backtest_walk_forward.rs`
- [ ] `backtest_oos_validate.rs`
- [ ] `backtest_simulate.rs`
- [ ] `backtest_grid.rs`
- [ ] `backtest_campaign.rs`
- [ ] `backtest_paper.rs`
- [ ] `research_run.rs`
- [ ] `validate_run.rs`
- [ ] `algorithm_create.rs`

**For each screen:**
- [ ] Create file with struct definition
- [ ] Implement appropriate view modes
- [ ] Implement metrics display
- [ ] Implement charts/tables as needed
- [ ] Implement export functionality
- [ ] Implement rendering function
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] All 15 screens created
- [ ] All screens compile and render
- [ ] Results display correctly
- [ ] Export works
- [ ] Tests pass

---

## Phase 4: TUI Integration (Week 7-8)

### T-4.1: Create TUI Command Executor
**Priority:** 🔴 Critical  
**Dependencies:** Phase 1, Phase 2, Phase 3 complete  
**Estimated Time:** 8 hours

**Description:** Create command executor that bridges TUI and command execution layer.

**Tasks:**
- [ ] Create `src/ui/command_executor.rs`
- [ ] Implement `TUICommandExecutor` struct
- [ ] Implement progress event channel
- [ ] Implement result channel
- [ ] Implement `execute_backtest_evaluate()` function
- [ ] Implement all other command execution functions (14 backtest, 2 research, 5 validate, 3 algorithm)
- [ ] Implement error handling with user-friendly messages
- [ ] Implement cancellation support
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Executor compiles
- [ ] All commands can be executed
- [ ] Progress events sent correctly
- [ ] Results returned correctly
- [ ] Error handling works
- [ ] Tests pass

---

### T-4.2: Add Algorithm Type Indicators to Menus
**Priority:** 🔴 Critical  
**Dependencies:** T-4.1  
**Estimated Time:** 4 hours

**Description:** Add (MM) or (MOM) indicators to menu items.

**Tasks:**
- [ ] Update `src/ui/screens/validate_menu.rs`
- [ ] Add "(MM)" to `tune` menu item
- [ ] Add "(MM)" to `regime-search` menu item
- [ ] Add "(MM)" to `multi-objective` menu item
- [ ] Add "(MM)" to `regime-optimize` menu item
- [ ] Add "(MM)" to `train` menu item
- [ ] Add "(MM)" to `walk-forward-ml` menu item
- [ ] Add "(MM)" to `grid` menu item
- [ ] Update menu rendering to show indicators
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] All MM-only commands show "(MM)" indicator
- [ ] Indicators display correctly in menus
- [ ] Menu navigation still works
- [ ] Tests pass

---

### T-4.3: Implement Algorithm Type Validation
**Priority:** 🔴 Critical  
**Dependencies:** T-4.1, T-4.2  
**Estimated Time:** 6 hours

**Description:** Add algorithm type checks before showing MM-only commands.

**Tasks:**
- [ ] Add algorithm type check in menu action handlers
- [ ] Show warning message when MM-only command selected with non-MM algorithm
- [ ] Disable MM-only menu items when non-MM algorithm selected (optional)
- [ ] Add algorithm type validation in config screens
- [ ] Show error message in config screens if wrong algorithm type
- [ ] Update command executor to validate algorithm type
- [ ] Write unit tests

**Acceptance Criteria:**
- [ ] Algorithm type checks work
- [ ] Warning messages display correctly
- [ ] MM-only commands blocked for non-MM algorithms
- [ ] Config screens validate algorithm type
- [ ] Tests pass

---

### T-4.4: Wire All Commands to TUI Menus
**Priority:** 🔴 Critical  
**Dependencies:** T-4.1, T-4.2, T-4.3, Phase 2, Phase 3  
**Estimated Time:** 20 hours

**Description:** Wire all commands to TUI menus with full workflow.

**Commands to wire:**
- [ ] `backtest evaluate` → Validate → Backtest
- [ ] `backtest sweep` → Validate → Sweep
- [ ] `backtest walk-forward` → Validate → Walk-Forward
- [ ] `backtest tune` → Validate → Tune (MM)
- [ ] `backtest regime-search` → Validate → Regime → Search (MM)
- [ ] `backtest oos-validate` → Validate → OOS
- [ ] `backtest multi-objective` → Validate → Multi-Objective (MM)
- [ ] `backtest regime-optimize` → Validate → Regime → Optimize (MM)
- [ ] `backtest train` → Validate → Train (MM)
- [ ] `backtest walk-forward-ml` → Validate → Walk-Forward ML (MM)
- [ ] `backtest simulate` → Validate → Simulate
- [ ] `backtest grid` → Validate → Grid (MM)
- [ ] `backtest campaign` → Validate → Campaign
- [ ] `backtest paper` → Trade → Paper
- [ ] `research run` → Research → Run
- [ ] `research status` → Research → Status
- [ ] `validate run` → Validate → Run
- [ ] `validate presets` → Validate → Presets
- [ ] `validate stages` → Validate → Info → Stages
- [ ] `validate status` → Validate → Info → Status
- [ ] `validate show` → Validate → Info → Details
- [ ] `algorithm create` → Algorithms → Create
- [ ] `algorithm list` → Algorithms → List
- [ ] `algorithm show` → Algorithms → Show

**For each command:**
- [ ] Add menu item (if not already present)
- [ ] Wire menu action to config screen
- [ ] Wire config screen to command executor
- [ ] Wire command executor to results screen
- [ ] Implement navigation flow
- [ ] Test end-to-end workflow

**Acceptance Criteria:**
- [ ] All 24 commands accessible via TUI
- [ ] All commands execute successfully
- [ ] Navigation flow works
- [ ] Results display correctly
- [ ] Error handling works
- [ ] Tests pass

---

### T-4.5: Implement Navigation Flow
**Priority:** 🟡 High  
**Dependencies:** T-4.4  
**Estimated Time:** 6 hours

**Description:** Implement complete navigation flow between menus, config screens, and results screens.

**Tasks:**
- [ ] Implement navigation from menu to config screen
- [ ] Implement navigation from config screen to progress screen
- [ ] Implement navigation from progress screen to results screen
- [ ] Implement back navigation (results → menu)
- [ ] Implement cancel navigation (config → menu)
- [ ] Implement state management (remember selected algorithm, etc.)
- [ ] Implement keyboard shortcuts
- [ ] Write integration tests

**Acceptance Criteria:**
- [ ] Navigation flow works correctly
- [ ] Back navigation works
- [ ] Cancel navigation works
- [ ] State persists correctly
- [ ] Keyboard shortcuts work
- [ ] Tests pass

---

### T-4.6: End-to-End Testing
**Priority:** 🔴 Critical  
**Dependencies:** T-4.4, T-4.5  
**Estimated Time:** 12 hours

**Description:** Comprehensive end-to-end testing of all workflows.

**Test Scenarios:**
- [ ] Test all 24 commands from TUI
- [ ] Test algorithm type validation (MM-only commands)
- [ ] Test parameter configuration for all commands
- [ ] Test results display for all commands
- [ ] Test preset save/load
- [ ] Test error handling
- [ ] Test cancellation
- [ ] Test navigation flow
- [ ] Compare TUI results with CLI results (same inputs)
- [ ] Performance testing

**Acceptance Criteria:**
- [ ] All test scenarios pass
- [ ] TUI results match CLI results
- [ ] No regressions
- [ ] Performance acceptable (<100ms UI response)
- [ ] All edge cases handled

---

## Summary

### Total Tasks: 53
- **Phase 1:** 21 tasks (Command Execution Layer)
- **Phase 2:** 12 tasks (Visual Parameter Configuration)
- **Phase 3:** 8 tasks (Visual Results Display)
- **Phase 4:** 6 tasks (TUI Integration)
- **Testing:** 6 tasks (integrated throughout)

### Estimated Time: ~300 hours (~7.5 weeks @ 40 hours/week)

### Critical Path:
1. Phase 1 (Command Execution Layer) - Must complete first
2. Phase 2 (Parameter Configuration) - Can start after Phase 1
3. Phase 3 (Results Display) - Can start after Phase 2
4. Phase 4 (TUI Integration) - Must complete last

### Dependencies:
- All Phase 2 tasks depend on Phase 1 completion
- All Phase 3 tasks depend on Phase 2 completion
- All Phase 4 tasks depend on Phase 1, 2, 3 completion

---

## Task Status Tracking

Use this section to track task completion:

### Phase 1: Command Execution Layer
- [ ] T-1.1: Create Commands Module Structure
- [ ] T-1.2: Implement Progress Callback System
- [ ] T-1.3: Extract Backtest Evaluate Command
- [ ] T-1.4: Extract Backtest Tune Command (MM Only)
- [ ] T-1.5: Extract Backtest Regime Search Command (MM Only)
- [ ] T-1.6: Extract Backtest Multi-Objective Command (MM Only)
- [ ] T-1.7: Extract Backtest Regime Optimize Command (MM Only)
- [ ] T-1.8: Extract Backtest Train Command (MM Only - ML Spread/Skew)
- [ ] T-1.9: Extract Backtest Walk-Forward ML Command (MM Only)
- [ ] T-1.10: Extract Remaining Backtest Commands
- [ ] T-1.11: Extract Research Commands
- [ ] T-1.12: Extract Validate Commands
- [ ] T-1.13: Extract Algorithm Commands
- [ ] T-1.14: Refactor All CLI Binaries

### Phase 2: Visual Parameter Configuration
- [ ] T-2.1: Create Text Input Widget
- [ ] T-2.2: Create Number Input Widget
- [ ] T-2.3: Create Comma-Separated List Widget
- [ ] T-2.4: Create Toggle Widget
- [ ] T-2.5: Create Path Input Widget
- [ ] T-2.6: Create Dropdown Widget
- [ ] T-2.7: Create Slider Widget
- [ ] T-2.8: Create Backtest Evaluate Config Screen
- [ ] T-2.9: Create Backtest Tune Config Screen (MM Only)
- [ ] T-2.10: Create Backtest Multi-Objective Config Screen (MM Only)
- [ ] T-2.11: Create Remaining Config Screens
- [ ] T-2.12: Implement Preset Management

### Phase 3: Visual Results Display
- [ ] T-3.1: Create Metrics Dashboard Widget
- [ ] T-3.2: Create Table Widget
- [ ] T-3.3: Create Chart Widget
- [ ] T-3.4: Create Pareto Frontier Widget
- [ ] T-3.5: Create Enhanced Progress Widget
- [ ] T-3.6: Create Backtest Evaluate Results Screen
- [ ] T-3.7: Create Backtest Tune Results Screen
- [ ] T-3.8: Create Remaining Results Screens

### Phase 4: TUI Integration
- [ ] T-4.1: Create TUI Command Executor
- [ ] T-4.2: Add Algorithm Type Indicators to Menus
- [ ] T-4.3: Implement Algorithm Type Validation
- [ ] T-4.4: Wire All Commands to TUI Menus
- [ ] T-4.5: Implement Navigation Flow
- [ ] T-4.6: End-to-End Testing

---

*Document Version: 0.26*  
*Last Updated: 2025-01-XX*  
*Status: Ready for Implementation*


