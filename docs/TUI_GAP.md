# TUI Gap Analysis & Migration Plan

This document outlines the remaining work to fully transition from the old CLI-based menu system to the new TUI menu system.

## Current Status

**Coverage: ~95%** - Most CLI functionality is already available in the TUI.

The new TUI menu system covers the complete core workflow:
- Research Menu (feature analysis, correlations, regime detection)
- Algorithms Menu (create, list, view, delete algorithms)
- Validate Menu (backtest commands: evaluate, tune, sweep, grid, walk-forward, etc.)
- Trade Menu (paper trading, live trading configuration)

## Missing CLI Features

The following 5 CLI commands need TUI screens:

### Priority 1: Data Management (Essential)

| Command | Description | TUI Screen Name |
|---------|-------------|-----------------|
| `backtest info` | Display data statistics (file count, date range, event count) | `BacktestInfoScreen` |
| `backtest validate-data` | Validate data quality (missing values, integrity checks) | `BacktestValidateDataScreen` |

**Rationale**: Users need to inspect and validate their data before running backtests. Currently they must exit TUI and use CLI.

### Priority 2: Comparison Tools (Nice to Have)

| Command | Description | TUI Screen Name |
|---------|-------------|-----------------|
| `backtest compare` | Compare ML algorithm vs Avellaneda-Stoikov baseline | `BacktestCompareScreen` |
| `backtest head-to-head` | Side-by-side comparison of two configurations | `BacktestHeadToHeadScreen` |

**Rationale**: Useful for algorithm development but not blocking core workflow.

### Priority 3: Session Simulation (Low Priority)

| Command | Description | TUI Screen Name |
|---------|-------------|-----------------|
| `backtest simulate-session` | Run single session simulation with detailed output | `BacktestSimulateSessionScreen` |

**Rationale**: Debugging tool, rarely used in normal workflow.

## Implementation Plan

### Phase 1: Data Management Screens

1. **BacktestInfoScreen** (T-3.1)
   - Display: Total files, date range, total events, average events/file
   - Show data directory path
   - List files with timestamps
   - Add to Validate Menu under key 'i' (info)

2. **BacktestValidateDataScreen** (T-3.2)
   - Run data quality checks (missing mid_price, gaps, etc.)
   - Display pass/fail status for each check
   - Show detailed error list if validation fails
   - Add to Validate Menu under key 'd' (validate-data)

### Phase 2: Comparison Screens

3. **BacktestCompareScreen** (T-3.3)
   - Run ML vs AS comparison
   - Display side-by-side metrics (PnL, Sharpe, trades, etc.)
   - Show relative performance
   - Add to Validate Menu under key 'c' (compare)

4. **BacktestHeadToHeadScreen** (T-3.4)
   - Configuration selection for two algorithms
   - Run both backtests
   - Display comparison table
   - Add to Validate Menu under key 'h' (head-to-head)

### Phase 3: Session Simulation

5. **BacktestSimulateSessionScreen** (T-3.5)
   - File/session selection
   - Detailed tick-by-tick output
   - Trade log display
   - Add to Validate Menu under key 's' (simulate-session)

## Ergonomic Considerations

### Navigation
- Keep single-key shortcuts for common actions
- Use consistent key bindings across menus (Esc=back, Enter=select)
- Provide breadcrumb navigation showing current location

### Data Display
- Use tables with column alignment for numeric data
- Implement scrollable views for large datasets
- Add loading indicators for long-running operations

### Feedback
- Show progress bars for multi-step operations
- Display clear success/error messages
- Preserve output history within session

### Keyboard Shortcuts Reference
Current TUI shortcuts to maintain:
- `q` - Quit
- `Esc` - Back to previous menu
- `Enter` - Select/Confirm
- `Tab` - Switch focus between panels
- `1-9` - Quick menu item selection

## Disabling Old CLI System

Once all gaps are implemented, disable the old CLI by:

### Step 1: Remove CLI Entry Points
```rust
// In src/main.rs, remove or comment out:
// - process_menu_input() calls
// - main_menu display
// - CLI argument parsing for menu mode
```

### Step 2: Update main.rs
```rust
// Change startup to directly launch TUI:
fn main() -> Result<()> {
    // Skip old menu, go directly to TUI
    let mut app = App::new()?;
    app.run()?;
    Ok(())
}
```

### Step 3: Deprecate Old Menu Code
Files that can be removed after TUI is complete:
- `src/ui/menu.rs` (if exists, old text menu)
- Any `print!`/`println!` based menu code in main.rs

### Step 4: Update Documentation
- Update README.md to reflect TUI-only interface
- Update CLAUDE.md with new TUI commands
- Remove CLI command examples from docs

## Testing Checklist

Before disabling old CLI:
- [ ] All 5 missing screens implemented
- [ ] All screens have unit tests
- [ ] Integration tests pass
- [ ] Manual testing of complete workflow
- [ ] Error handling for edge cases
- [ ] Loading states for async operations

## Timeline Estimate

This is a task breakdown, not a time estimate:
1. BacktestInfoScreen - Straightforward data display
2. BacktestValidateDataScreen - Reuse existing validation logic
3. BacktestCompareScreen - Medium complexity, needs two backtest runs
4. BacktestHeadToHeadScreen - Similar to CompareScreen
5. BacktestSimulateSessionScreen - Complex, detailed output handling

## Conclusion

The TUI system is already mature with 95% coverage. The remaining 5% consists of auxiliary tools that don't block the main trading workflow. Users can complete the full research-to-trading cycle entirely within the TUI today.

Priority should be given to data management screens (info, validate-data) as these are most frequently needed before running backtests.
