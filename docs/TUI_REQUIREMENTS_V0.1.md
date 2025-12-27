# TUI Requirements v0.1

**Document Version:** 1.0
**Created:** December 26, 2025
**Parent Document:** REQUIREMENTS_V0.2.md (Phase 4: Integration & TUI)
**Philosophy:** Workflow-driven design where menus follow the natural progression: Research -> Configure -> Validate -> Trade

---

## Executive Summary

This document specifies the Terminal User Interface (TUI) implementation for Ingestor v0.2. The TUI provides ergonomic access to 100% of CLI functionality through a hierarchical menu system that reflects the algorithm development workflow.

**Design Principles:**
1. **Workflow-driven**: Menus follow Research -> Configure -> Validate -> Trade
2. **Algorithm-agnostic**: Same UX for Momentum, Market Making, and Hybrid strategies
3. **3 clicks max**: Any function reachable in ≤3 keystrokes from main menu
4. **Status always visible**: Current state displayed in persistent status bar
5. **Minimalist**: Less is more - only essential options at each level

**CLI Parity Goal:** Every CLI subcommand accessible via TUI

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TUI ARCHITECTURE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         STATUS BAR (Always Visible)                  │   │
│  │  Symbol: BTCUSDT | Algorithm: momentum_v3 | Validation: 4/5 | Idle  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                           MAIN MENU                                  │   │
│  │  [1] RESEARCH  [2] ALGORITHMS  [3] VALIDATE  [4] TRADE  [5] DATA    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │              │               │             │           │            │
│       ▼              ▼               ▼             ▼           ▼            │
│   ┌───────┐    ┌──────────┐    ┌──────────┐  ┌────────┐  ┌────────┐        │
│   │Research│   │Algorithms│    │ Validate │  │ Trade  │  │  Data  │        │
│   │Submenu │   │ Submenu  │    │ Submenu  │  │Submenu │  │Submenu │        │
│   └───┬───┘    └────┬─────┘    └────┬─────┘  └───┬────┘  └───┬────┘        │
│       │             │               │            │           │              │
│       ▼             ▼               ▼            ▼           ▼              │
│   ┌───────────────────────────────────────────────────────────────┐        │
│   │                    EXISTING TUI MODES                          │        │
│   │  (Live, Features, Backtest, WalkForward, GridSearch, etc.)    │        │
│   └───────────────────────────────────────────────────────────────┘        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Global State Model

### Task TUI-0.0: GlobalState Struct

**File:** `src/ui/state.rs`

```rust
/// Global application state shared across all screens
pub struct GlobalState {
    /// Trading symbol (e.g., "BTCUSDT")
    pub symbol: String,

    /// Currently active algorithm configuration
    pub active_algorithm: Option<AlgorithmConfigSummary>,

    /// Validation pipeline status for active algorithm
    pub validation_status: ValidationStatus,

    /// Current trading mode
    pub trading_mode: TradingMode,

    /// Research engine status
    pub research_status: ResearchStatus,

    /// Data statistics
    pub data_stats: DataStats,
}

/// Summary of active algorithm (lightweight for display)
pub struct AlgorithmConfigSummary {
    pub id: String,
    pub name: String,
    pub strategy_type: StrategyType,
    pub created_at: DateTime<Utc>,
}

/// Validation status for each stage
pub struct ValidationStatus {
    pub backtest: StageStatus,
    pub forward: StageStatus,
    pub oos: StageStatus,
    pub paper: StageStatus,
    pub live: StageStatus,
}

pub enum StageStatus {
    NotRun,
    Passed { sharpe: f64, timestamp: DateTime<Utc> },
    Failed { reason: String, timestamp: DateTime<Utc> },
    Running { progress: f64 },
}

pub enum TradingMode {
    Idle,
    Paper { started: DateTime<Utc>, pnl: f64 },
    Live { started: DateTime<Utc>, pnl: f64 },
}

pub enum ResearchStatus {
    Idle,
    Running { samples_processed: usize },
    Complete { tradeable: bool },
}

pub struct DataStats {
    pub file_count: usize,
    pub total_events: usize,
    pub date_range: Option<(NaiveDate, NaiveDate)>,
    pub size_mb: f64,
}
```

**Acceptance Criteria:**
- [ ] All structs defined with appropriate derives (Debug, Clone)
- [ ] Default implementations for all types
- [ ] `GlobalState::new(symbol: &str)` constructor
- [ ] `GlobalState::load_from_stores()` to populate from persistence
- [ ] 15+ unit tests

---

## Main Menu

### Task TUI-0.1: Main Menu Update

**File:** `src/ui/screens/main_menu.rs`

Update the existing `MainMenuItem` enum to reflect the new 5+1 structure:

```
╔══════════════════════════════════════════════════════════════╗
║  INGESTOR - Algorithmic Trading Platform                     ║
║  Symbol: BTCUSDT    Algorithm: [None/momentum_v3]            ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   [1] RESEARCH      Discover edges in market data            ║
║   [2] ALGORITHMS    Configure and manage strategies          ║
║   [3] VALIDATE      Test before risking capital              ║
║   [4] TRADE         Paper and live execution                 ║
║   [5] DATA          Monitor streams and quality              ║
║                                                              ║
║   [Q] Quit                                                   ║
║                                                              ║
╠══════════════════════════════════════════════════════════════╣
║  Status: Research complete | Validation: 3/5 passed | Idle   ║
╚══════════════════════════════════════════════════════════════╝
```

**Changes from current:**
- Remove LIVE DATA, BACKTEST, SETTINGS as top-level items
- Add ALGORITHMS as item [2]
- Renumber items to 1-5 + Q
- Integrate status bar footer

**Acceptance Criteria:**
- [ ] `MainMenuItem` enum updated: Research, Algorithms, Validate, Trade, Data, Quit
- [ ] Keys: 1, 2, 3, 4, 5, Q
- [ ] Status bar integrated into draw function
- [ ] Tests updated

---

## Submenu Framework

### Task TUI-0.2: SubMenu Trait and Navigation

**File:** `src/ui/submenu.rs`

```rust
/// Result of handling a key press in a submenu
pub enum SubMenuAction {
    /// Stay in current submenu
    None,
    /// Go back to parent menu
    Back,
    /// Navigate to a TUI mode/screen
    Navigate(AppMode),
    /// Execute a CLI command (blocking)
    ExecuteCommand(CliCommand),
    /// Show a message/dialog
    ShowMessage(String),
}

/// CLI command to execute
pub struct CliCommand {
    pub binary: &'static str,  // "research", "validate", "algorithm", "backtest"
    pub args: Vec<String>,
}

/// Trait for all submenus
pub trait SubMenu {
    /// Get the menu title
    fn title(&self) -> &str;

    /// Get menu items for display
    fn items(&self) -> Vec<SubMenuItem>;

    /// Handle key press, return action
    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction;

    /// Draw the submenu
    fn draw(&self, f: &mut Frame, area: Rect, state: &GlobalState);
}

pub struct SubMenuItem {
    pub key: char,
    pub label: &'static str,
    pub description: &'static str,
    pub enabled: bool,  // Grayed out if false
}
```

**Acceptance Criteria:**
- [ ] `SubMenu` trait defined
- [ ] `SubMenuAction` enum with all variants
- [ ] `SubMenuItem` struct for rendering
- [ ] Helper function `draw_submenu_frame()` for consistent styling
- [ ] 10+ tests

---

## Research Submenu

### Task TUI-1.0: Research Menu Implementation

**File:** `src/ui/screens/research_menu.rs`

```
╔══════════════════════════════════════════════════════════════╗
║  RESEARCH - Edge Detection                                   ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   [R] Run Research      Analyze historical data for edges    ║
║   [S] Status            View current research state          ║
║   [C] Create Config     Generate algorithm from findings     ║
║                                                              ║
║   [ESC] Back                                                 ║
╚══════════════════════════════════════════════════════════════╝
```

**CLI Mapping:**
| Key | Action | CLI Equivalent |
|-----|--------|----------------|
| R | Run research analysis | `cargo run --bin research -- run` |
| S | Show research status | `cargo run --bin research -- status` |
| C | Create algorithm config | `cargo run --bin algorithm -- create --from-research` |

**Acceptance Criteria:**
- [ ] `ResearchMenu` struct implementing `SubMenu`
- [ ] All three actions mapped correctly
- [ ] Status display shows: MIDC κ, τ_half, top signals, tradeable assessment
- [ ] "Create Config" disabled if research not complete
- [ ] 15+ tests

---

## Algorithms Submenu

### Task TUI-2.0: Algorithms Menu Implementation

**File:** `src/ui/screens/algorithms_menu.rs`

```
╔══════════════════════════════════════════════════════════════╗
║  ALGORITHMS - Strategy Configuration                         ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   [L] List             Browse saved configurations           ║
║   [S] Select           Activate an algorithm config          ║
║   [V] View             Show active config details            ║
║   [N] New              Create config manually                ║
║                                                              ║
║   ── Strategy Types ──                                       ║
║   [1] Momentum         Trend-following (MIDC, persistence)   ║
║   [2] Market Making    Spread capture (A-S, ML-spreads)      ║
║   [3] Hybrid           Adaptive switching                    ║
║                                                              ║
║   [ESC] Back                                                 ║
╚══════════════════════════════════════════════════════════════╝

Active: momentum_btc_v3 (Momentum) | Created: 2025-12-20
```

**CLI Mapping:**
| Key | Action | CLI Equivalent |
|-----|--------|----------------|
| L | List all configs | `cargo run --bin algorithm -- list` |
| S | Select/activate config | Interactive selection from list |
| V | View active config | `cargo run --bin algorithm -- show <active>` |
| N | Create new config | Interactive wizard |
| 1 | Filter by Momentum | `cargo run --bin algorithm -- list --strategy momentum` |
| 2 | Filter by Market Making | `cargo run --bin algorithm -- list --strategy market-making` |
| 3 | Filter by Hybrid | `cargo run --bin algorithm -- list --strategy hybrid` |

**Acceptance Criteria:**
- [ ] `AlgorithmsMenu` struct implementing `SubMenu`
- [ ] List view with scrollable selection
- [ ] Strategy type filtering
- [ ] Active algorithm highlighted
- [ ] Selection persists to GlobalState
- [ ] 20+ tests

---

## Validate Submenu

### Task TUI-3.0: Validate Menu Implementation

**File:** `src/ui/screens/validate_menu.rs`

```
╔══════════════════════════════════════════════════════════════╗
║  VALIDATE - Test Before Trading                              ║
║  Algorithm: momentum_btc_v3                                  ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   ── Run Stages ──                                           ║
║   [1] Backtest         Historical replay              ✓ 1.2  ║
║   [2] Walk-Forward     Time-series cross-validation   ✓ 0.9  ║
║   [3] Out-of-Sample    Holdout test (20%)             ✗      ║
║   [A] All Stages       Run full pipeline                     ║
║                                                              ║
║   ── Optimization ──                                         ║
║   [G] Grid Search      Parameter optimization                ║
║   [W] Sweep            Sensitivity analysis                  ║
║                                                              ║
║   ── Results ──                                              ║
║   [H] History          Past validation runs                  ║
║   [P] Presets          View/select pipeline presets          ║
║                                                              ║
║   [ESC] Back                                                 ║
╚══════════════════════════════════════════════════════════════╝

Last Run: Backtest ✓ | WalkFwd ✓ | OOS ✗ (Sharpe 0.3 < 0.5)
```

**CLI Mapping:**
| Key | Action | CLI Equivalent |
|-----|--------|----------------|
| 1 | Run backtest | `cargo run --bin validate -- run --stages backtest` |
| 2 | Run walk-forward | `cargo run --bin validate -- run --stages forward` |
| 3 | Run OOS | `cargo run --bin validate -- run --stages oos` |
| A | Run all stages | `cargo run --bin validate -- run` |
| G | Grid search | `cargo run --bin backtest -- tune` |
| W | Parameter sweep | `cargo run --bin backtest -- sweep` |
| H | View history | `cargo run --bin validate -- status` |
| P | View presets | `cargo run --bin validate -- presets` |

**Stage Status Display:**
- ✓ = Passed (show Sharpe in green)
- ✗ = Failed (show in red)
- ○ = Not run (gray)
- ◐ = Running (with progress %)

**Acceptance Criteria:**
- [ ] `ValidateMenu` struct implementing `SubMenu`
- [ ] Stage status indicators with metrics
- [ ] Disabled if no algorithm selected
- [ ] Links to existing TUI modes: Backtest, WalkForward, GridSearch, Sweep, OOSValidation
- [ ] 20+ tests

---

## Trade Submenu

### Task TUI-4.0: Trade Menu Implementation

**File:** `src/ui/screens/trade_menu.rs`

```
╔══════════════════════════════════════════════════════════════╗
║  TRADE - Execution                                           ║
║  Algorithm: momentum_btc_v3 | Validated: ✓                   ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   ── Paper Trading ──                                        ║
║   [P] Paper Trade      Simulated execution on live data      ║
║   [C] Campaign         4-week validation campaign            ║
║                                                              ║
║   ── Live Trading ──                                         ║
║   [L] Live             Real execution (requires validation)  ║
║                                                              ║
║   ── Sessions ──                                             ║
║   [S] Sessions         View past trading sessions            ║
║   [V] Validate Session Compare session vs backtest           ║
║                                                              ║
║   [ESC] Back                                                 ║
╚══════════════════════════════════════════════════════════════╝

⚠ Live trading requires: Backtest ✓ WalkFwd ✓ OOS ✓ Paper ✓
```

**CLI Mapping:**
| Key | Action | CLI Equivalent |
|-----|--------|----------------|
| P | Paper trading | `cargo run --bin validate -- run --stages paper` |
| C | Validation campaign | `cargo run --bin backtest -- simulate-campaign` |
| L | Live trading | `cargo run --bin validate -- run --stages live` |
| S | View sessions | List session files |
| V | Validate session | `cargo run --bin backtest -- validate-session` |

**Live Trading Gate:**
Live trading [L] is DISABLED unless ALL prior stages passed:
- Backtest: ✓
- Walk-Forward: ✓
- OOS: ✓
- Paper: ✓

**Acceptance Criteria:**
- [ ] `TradeMenu` struct implementing `SubMenu`
- [ ] Live trading gate enforcement
- [ ] Warning display for incomplete validation
- [ ] Links to existing TUI modes: PaperTradePreset, CampaignSimulation, Live, LiveMM
- [ ] Session listing functionality
- [ ] 20+ tests

---

## Data Submenu

### Task TUI-5.0: Data Menu Implementation

**File:** `src/ui/screens/data_menu.rs`

```
╔══════════════════════════════════════════════════════════════╗
║  DATA - Market Data & Quality                                ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║   [L] Live Stream      Real-time feature visualization       ║
║   [F] Features         Detailed feature inspection           ║
║   [I] Info             Dataset statistics                    ║
║   [Q] Quality          Data validation checks                ║
║                                                              ║
║   [ESC] Back                                                 ║
╚══════════════════════════════════════════════════════════════╝

Data: 97 files | 47 days | Oct 16 - Dec 02, 2025 | 73k events
```

**CLI Mapping:**
| Key | Action | CLI Equivalent |
|-----|--------|----------------|
| L | Live data stream | TUI Live mode |
| F | Feature details | TUI Features mode |
| I | Data info | `cargo run --bin backtest -- info` |
| Q | Quality check | `cargo run --bin backtest -- validate-data` |

**Acceptance Criteria:**
- [ ] `DataMenu` struct implementing `SubMenu`
- [ ] Links to existing TUI modes: Live, Features, DataInfo, DataQuality
- [ ] Data statistics in footer
- [ ] 10+ tests

---

## Status Bar Widget

### Task TUI-6.0: Status Bar Implementation

**File:** `src/ui/widgets/status_bar.rs`

```rust
/// Persistent status bar displayed at bottom of all screens
pub struct StatusBar;

impl StatusBar {
    pub fn draw(f: &mut Frame, area: Rect, state: &GlobalState) {
        // Format: Symbol | Algorithm | Validation | Trading Mode
        // Example: BTCUSDT | momentum_v3 | 4/5 ✓ | Paper (+$123.45)
    }
}
```

**Display Format:**
```
┌────────────────────────────────────────────────────────────────┐
│ BTCUSDT | momentum_v3 (MOM) | Val: 4/5 ✓ | Paper (+$123.45)   │
└────────────────────────────────────────────────────────────────┘
```

**Color Coding:**
- Symbol: White
- Algorithm: Cyan (or Yellow if none)
- Validation: Green if all pass, Yellow if partial, Red if any fail
- Trading Mode: Green for Idle, Blue for Paper, Red for Live

**Acceptance Criteria:**
- [ ] `StatusBar::draw()` function
- [ ] Color coding for all states
- [ ] Truncation for small terminals
- [ ] 10+ tests

---

## Navigation Integration

### Development Bridge: `[n] New Menu` Option

**Status:** IMPLEMENTED

During TUI v0.1 development, the old menu remains operational while the new menu is being built. This allows testing of the new menu without disrupting existing functionality.

**Bridge Implementation:**
- Old menu displays `[n] New Menu` option (added to `draw_menu()` in `tui.rs`)
- Pressing `n` from old menu enters `AppMode::NewMenu`
- From new menu, pressing `ESC` returns to old menu
- New menu keys: `1`-`5` for submenu items, `q` to quit

**Files Modified:**
- `src/ui/tui.rs`:
  - Added `NewMenu` variant to `AppMode` enum
  - Added `main_menu_state` variable initialization
  - Added key handler for `'n'` in `AppMode::Menu`
  - Added key handling for `AppMode::NewMenu`
  - Added rendering for `AppMode::NewMenu`
  - Added `[n] New Menu` option to `draw_menu()`

---

### Task TUI-7.0: Remove Bridge and Replace Old Menu

**File:** `src/ui/tui.rs`

**Objective:** Once all submenus (TUI-1.0 through TUI-5.0) are complete and tested, remove the old menu and make the new menu the default.

**Changes Required:**
1. Remove old `draw_menu()` function (the inline menu renderer)
2. Change `AppMode::Menu` to render `draw_main_menu()` instead
3. Remove `AppMode::NewMenu` variant (merge into `AppMode::Menu`)
4. Update key bindings in `AppMode::Menu` to use new menu items
5. Remove the `[n] New Menu` bridge option

**Updated AppMode Enum (after TUI-7.0):**

```rust
pub enum AppMode {
    // Main menu (new v0.1 menu)
    Menu,

    // Submenus
    ResearchMenu,
    AlgorithmsMenu,
    ValidateMenu,
    TradeMenu,
    DataMenu,

    // Existing operational modes (unchanged)
    Live,
    LiveMM,
    PresetSelect,
    PaperTradePreset,
    Features,
    Backtest,
    WalkForward,
    DataQuality,
    CampaignSimulation,
    DataInfo,
    GridSearch,
    Sweep,
    OOSValidation,
    Research,
}
```

**Key Bindings from Main Menu:**
| Key | Action |
|-----|--------|
| 1 | Go to ResearchMenu |
| 2 | Go to AlgorithmsMenu |
| 3 | Go to ValidateMenu |
| 4 | Go to TradeMenu |
| 5 | Go to DataMenu |
| Q | Quit |

**ESC Behavior:**
- From submenu → MainMenu
- From operational mode → Parent submenu (or MainMenu)

**Acceptance Criteria:**
- [ ] Old `draw_menu()` function removed
- [ ] New menu renders by default in `AppMode::Menu`
- [ ] All submenus navigable from main menu
- [ ] ESC works consistently
- [ ] Existing modes still accessible via submenus
- [ ] No functionality lost
- [ ] `[n] New Menu` option removed
- [ ] Integration tests for navigation flow

---

## Algorithm Selection Persistence

### Task TUI-8.0: Persist Active Algorithm

**File:** `src/ui/state.rs` (addition)

```rust
impl GlobalState {
    /// Save active algorithm selection to disk
    pub fn save_active_algorithm(&self) -> Result<()>;

    /// Load active algorithm selection on startup
    pub fn load_active_algorithm(&mut self) -> Result<()>;
}
```

**Persistence Location:** `~/.config/ingestor/active_algorithm.json`

**Contents:**
```json
{
  "config_id": "momentum_btc_v3_20251220",
  "selected_at": "2025-12-26T10:30:00Z"
}
```

**Acceptance Criteria:**
- [ ] Selection persists across TUI restarts
- [ ] Handles missing/corrupted file gracefully
- [ ] Validates config still exists in ConfigStore
- [ ] 10+ tests

---

## Implementation Sequence

### Week 1: Foundation

| Day | Tasks | Deliverables |
|-----|-------|--------------|
| 1 | TUI-0.0, TUI-0.1 | GlobalState, Main menu update |
| 2 | TUI-0.2 | SubMenu trait, navigation framework |
| 3 | TUI-6.0 | StatusBar widget |
| 4 | TUI-1.0 | Research submenu |
| 5 | TUI-2.0 | Algorithms submenu |

### Week 2: Completion

| Day | Tasks | Deliverables |
|-----|-------|--------------|
| 1 | TUI-3.0 | Validate submenu |
| 2 | TUI-4.0 | Trade submenu |
| 3 | TUI-5.0 | Data submenu |
| 4 | TUI-7.0 | Navigation integration |
| 5 | TUI-8.0 | Algorithm persistence |

### Week 3: Testing & Polish

| Day | Tasks | Deliverables |
|-----|-------|--------------|
| 1-2 | Testing | Unit tests, integration tests |
| 3-4 | Bug fixes | Address issues from testing |
| 5 | Documentation | Update CLAUDE.md, README |

---

## Task Reference

| ID | Task | Description | Est. Hours | Dependencies |
|----|------|-------------|------------|--------------|
| TUI-0.0 | GlobalState | State management struct | 3h | - |
| TUI-0.1 | Main Menu Update | Update to 5-item menu | 2h | TUI-0.0 |
| TUI-0.2 | SubMenu Framework | Trait and navigation | 3h | - |
| TUI-1.0 | Research Menu | Research submenu | 3h | TUI-0.2 |
| TUI-2.0 | Algorithms Menu | Algorithm submenu | 4h | TUI-0.2 |
| TUI-3.0 | Validate Menu | Validation submenu | 4h | TUI-0.2 |
| TUI-4.0 | Trade Menu | Trading submenu | 4h | TUI-0.2 |
| TUI-5.0 | Data Menu | Data submenu | 2h | TUI-0.2 |
| TUI-6.0 | Status Bar | Status bar widget | 2h | TUI-0.0 |
| TUI-7.0 | Navigation | Wire submenus to TUI | 4h | TUI-1.0 - TUI-5.0 |
| TUI-8.0 | Persistence | Algorithm selection save | 2h | TUI-0.0 |
| **Total** | | | **33h** | |

---

## CLI Parity Matrix

Complete mapping of CLI commands to TUI access:

### Research Binary (`cargo run --bin research`)
| Subcommand | TUI Access |
|------------|------------|
| `run` | Research Menu → [R] Run |
| `status` | Research Menu → [S] Status |

### Algorithm Binary (`cargo run --bin algorithm`)
| Subcommand | TUI Access |
|------------|------------|
| `create` | Research Menu → [C] Create Config |
| `list` | Algorithms Menu → [L] List |
| `show` | Algorithms Menu → [V] View |

### Validate Binary (`cargo run --bin validate`)
| Subcommand | TUI Access |
|------------|------------|
| `run` | Validate Menu → [1]/[2]/[3]/[A] |
| `presets` | Validate Menu → [P] Presets |
| `status` | Validate Menu → [H] History |

### Backtest Binary (`cargo run --bin backtest`)
| Subcommand | TUI Access |
|------------|------------|
| `info` | Data Menu → [I] Info |
| `validate-data` | Data Menu → [Q] Quality |
| `sweep` | Validate Menu → [W] Sweep |
| `tune` / `grid-search` | Validate Menu → [G] Grid Search |
| `walk-forward` | Validate Menu → [2] Walk-Forward |
| `oos-validate` | Validate Menu → [3] Out-of-Sample |
| `simulate-campaign` | Trade Menu → [C] Campaign |
| `validate-session` | Trade Menu → [V] Validate Session |
| `simulate-session` | Trade Menu → [S] Sessions |

---

## File Structure

```
src/ui/
├── mod.rs                    # Module exports (update)
├── tui.rs                    # Main TUI loop (refactor)
├── state.rs                  # NEW: GlobalState
├── submenu.rs                # NEW: SubMenu trait
├── widgets/
│   ├── mod.rs                # NEW: Widget module
│   └── status_bar.rs         # NEW: StatusBar
└── screens/
    ├── mod.rs                # Module exports (update)
    ├── main_menu.rs          # UPDATE: 5-item menu
    ├── research_menu.rs      # NEW: Research submenu
    ├── algorithms_menu.rs    # NEW: Algorithms submenu
    ├── validate_menu.rs      # NEW: Validate submenu
    ├── trade_menu.rs         # NEW: Trade submenu
    ├── data_menu.rs          # NEW: Data submenu
    └── research.rs           # EXISTING: Research dashboard
```

---

## Success Criteria

### Usability
- [ ] Any CLI function accessible in ≤3 keystrokes
- [ ] ESC always goes back (predictable)
- [ ] Status always visible (no hidden state)
- [ ] Clear feedback for disabled options

### Functionality
- [ ] 100% CLI parity via TUI
- [ ] All existing TUI modes still accessible
- [ ] Algorithm selection persists
- [ ] Live trading gate enforced

### Quality
- [ ] 150+ unit tests across all new modules
- [ ] Integration tests for navigation flows
- [ ] No regressions in existing functionality

---

## Appendix: Key Binding Reference

### Main Menu
| Key | Action |
|-----|--------|
| 1 | Research |
| 2 | Algorithms |
| 3 | Validate |
| 4 | Trade |
| 5 | Data |
| Q | Quit |

### All Submenus
| Key | Action |
|-----|--------|
| ESC | Back to parent |

### Research Menu
| Key | Action |
|-----|--------|
| R | Run research |
| S | Status |
| C | Create config |

### Algorithms Menu
| Key | Action |
|-----|--------|
| L | List |
| S | Select |
| V | View |
| N | New |
| 1 | Momentum |
| 2 | Market Making |
| 3 | Hybrid |

### Validate Menu
| Key | Action |
|-----|--------|
| 1 | Backtest |
| 2 | Walk-Forward |
| 3 | OOS |
| A | All stages |
| G | Grid Search |
| W | Sweep |
| H | History |
| P | Presets |

### Trade Menu
| Key | Action |
|-----|--------|
| P | Paper trade |
| C | Campaign |
| L | Live |
| S | Sessions |
| V | Validate session |

### Data Menu
| Key | Action |
|-----|--------|
| L | Live stream |
| F | Features |
| I | Info |
| Q | Quality |

---

*Document maintained by: Development Team*
*Last updated: December 27, 2025*
