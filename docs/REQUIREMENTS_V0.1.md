# INGESTOR v0.1 Requirements & Roadmap

**Document Version:** 2.0
**Created:** December 11, 2025
**Philosophy:** Validate before complexity. Ship working software.

---

## Executive Summary

This document defines a **focused, achievable** v0.1 release that prioritizes:
1. Validating the existing A-S algorithm in real paper trading
2. Achieving CLI/TUI feature parity
3. Building a proper algorithm framework for future expansion

**Timeline:** 6-8 weeks
**Algorithms in v0.1:** 3 (A-S, Fixed Spread, Linear Model)
**Success Metric:** A-S paper trading Sharpe > 0 for 2+ weeks

---

## The 5 Core Principles

### Principle 1: CLI/TUI Parity
Every backtest CLI command MUST be callable from TUI with identical parameters and defaults.

**Rationale:** Users shouldn't need to remember two interfaces. Consistency reduces cognitive load and bugs.

### Principle 2: Algorithm Agnosticism
The system MUST support pluggable algorithms via unified traits, even if only 3 are initially implemented.

**Rationale:** Building the framework now enables rapid algorithm development later without architectural rewrites.

### Principle 3: Validation Before Complexity
No new algorithm implementation until A-S achieves measurable success in paper trading.

**Rationale:** Adding algorithms to a broken system wastes effort. A working simple algorithm beats a broken complex one.

### Principle 4: Data-Driven Decisions
Every feature decision MUST be backed by backtest or paper trading data, not intuition.

**Rationale:** Gut feelings lose money. The market doesn't care about our preferences.

### Principle 5: Minimal Viable Scope
Each feature MUST answer: "Does this help validate A-S profitability?" If no, defer to v0.2.

**Rationale:** Scope creep kills projects. Ship something that works.

---

## Success Criteria for v0.1

### MUST HAVE (v0.1 is complete when ALL are true)

| Criterion | Metric | Status |
|-----------|--------|--------|
| CLI/TUI Parity | All CLI commands accessible from TUI | Pending |
| Algorithm Selection | User can choose A-S, Fixed, or Linear from both CLI and TUI | Pending |
| Paper Trading Data | A-S paper trading for 2+ weeks collected | Not Started |
| Validation Comparison | Paper vs backtest Sharpe documented | Not Started |
| Parameter Tuning | Grid search accessible from TUI | Pending |

### NICE TO HAVE (defer if time runs out)

- Workflow wizard for new users
- Status bar with persistent state
- Multi-level order support
- Hierarchical menu restructure

### EXPLICITLY OUT OF SCOPE (v0.2 or later)

- Gradient Boosting algorithm
- Neural Network algorithm
- PPO/SAC/DQN (Reinforcement Learning)
- TensorBoard integration
- Multi-exchange support

---

## Current State Assessment

### What Works Today

| Component | Status | Notes |
|-----------|--------|-------|
| Data Collection | Working | 47+ days of Parquet data |
| A-S Algorithm | Working | Backtest shows +5.14% return |
| ML Linear Model | Working | Walk-forward training exists |
| Backtesting | Working | Single, sweep, grid-search, walk-forward |
| Paper Trading | Working | Via TUI only |
| TUI | Working | But missing CLI parity |

### CLI/TUI Gap Analysis

**CLI commands NOT in TUI:**

| Command | Priority | Effort |
|---------|----------|--------|
| `grid-search` | HIGH | Medium |
| `sweep` | HIGH | Medium |
| `info` | MEDIUM | Low |
| `oos-validate` | MEDIUM | Medium |
| `regime-search` | LOW | Medium |
| `multi-objective` | LOW | High |

**TUI features NOT in CLI:**

| Feature | Priority | Effort |
|---------|----------|--------|
| Paper trading | LOW | High (requires live connection) |
| Real-time viz | LOW | N/A (inherently TUI) |

---

## Algorithm Framework Design

### Trait Hierarchy

```rust
/// Base trait - all algorithms implement this
trait MarketMakingAlgorithm {
    fn compute_quotes(&self, state: &MarketState) -> QuoteDecision;
    fn name(&self) -> &str;
    fn version(&self) -> &str;
}

/// Algorithms that describe their parameters
trait Configurable: MarketMakingAlgorithm {
    fn parameters(&self) -> Vec<ParameterDefinition>;
    fn from_config(config: &AlgorithmConfig) -> Result<Self>;
}

/// Algorithms with learnable weights
trait Trainable: Configurable {
    fn train(&mut self, data: &TrainingData) -> TrainingResult;
    fn save_weights(&self, path: &Path) -> Result<()>;
    fn load_weights(&mut self, path: &Path) -> Result<()>;
}
```

### Parameter Definition System

```rust
struct ParameterDefinition {
    name: String,
    description: String,
    param_type: ParameterType,
    default: f64,
    range: Option<(f64, f64)>,
    tunable: bool,
}

enum ParameterType {
    Continuous,  // spread_bps: 0.5 - 10.0
    Discrete,    // order_levels: 1, 2, 3
    Boolean,     // entropy_gating: true/false
}
```

### Algorithms for v0.1

| Algorithm | Category | Trainable | Parameters | Status |
|-----------|----------|-----------|------------|--------|
| Fixed Spread | Rule-Based | No | 2 | To Implement |
| Avellaneda-Stoikov | Rule-Based | No | 6 | Exists |
| Linear Model | Statistical | Yes | 7 | Exists |

---

## Roadmap: 6-8 Weeks

### Phase 0: Foundation (Week 1)

**Goal:** Clean foundation for algorithm work

| Task | Description | Effort | Depends On |
|------|-------------|--------|------------|
| 0.1 | Add `info` command to TUI | 2h | - |
| 0.2 | Add `grid-search` to TUI | 4h | - |
| 0.3 | Add `sweep` to TUI | 3h | 0.2 |
| 0.4 | Add `oos-validate` to TUI | 3h | - |
| 0.5 | Rename CLI commands for clarity | 2h | - |
| 0.6 | Update CLI `--algorithm` flag to work with all commands | 2h | - |

**Deliverable:** All major CLI commands accessible from TUI

### Phase 1: Algorithm Framework (Week 2-3)

**Goal:** Proper trait system for pluggable algorithms

| Task | Description | Effort | Depends On |
|------|-------------|--------|------------|
| 1.1 | Design `Configurable` trait with `ParameterDefinition` | 4h | - |
| 1.2 | Refactor A-S to implement `Configurable` | 4h | 1.1 |
| 1.3 | Refactor ML Linear to implement `Trainable` | 4h | 1.1 |
| 1.4 | Implement `AlgorithmRegistry` | 4h | 1.2, 1.3 |
| 1.5 | Implement `FixedSpread` algorithm (baseline) | 3h | 1.1 |
| 1.6 | Add `algorithms` command to show registry | 2h | 1.4 |
| 1.7 | Update backtest harness to use registry | 4h | 1.4 |

**Deliverable:** Three algorithms selectable via unified interface

### Phase 2: TUI Enhancement (Week 4-5)

**Goal:** Algorithm selection and parameter configuration in TUI

| Task | Description | Effort | Depends On |
|------|-------------|--------|------------|
| 2.1 | Add algorithm selection menu | 4h | 1.4 |
| 2.2 | Add parameter display for selected algorithm | 4h | 2.1 |
| 2.3 | Add parameter editing UI | 6h | 2.2 |
| 2.4 | Integrate algorithm selection with paper trading | 4h | 2.1 |
| 2.5 | Integrate algorithm selection with backtesting | 4h | 2.1 |
| 2.6 | Add optimization method selection (grid/random/bayesian) | 4h | 2.3 |

**Deliverable:** Full algorithm workflow accessible from TUI

### Phase 3: Validation (Week 6-7)

**Goal:** Real paper trading data for A-S

| Task | Description | Effort | Depends On |
|------|-------------|--------|------------|
| 3.1 | Start A-S paper trading with optimal params | 1h | 2.4 |
| 3.2 | Run for minimum 2 weeks | 2 weeks | 3.1 |
| 3.3 | Analyze paper trading results | 4h | 3.2 |
| 3.4 | Compare paper vs backtest metrics | 4h | 3.3 |
| 3.5 | Document findings and next steps | 2h | 3.4 |

**Deliverable:** Validated understanding of A-S real-world performance

### Phase 4: Polish (Week 8)

**Goal:** Documentation, testing, release prep

| Task | Description | Effort | Depends On |
|------|-------------|--------|------------|
| 4.1 | Update USER_MANUAL.md with new features | 4h | All |
| 4.2 | Update QUICK_REFERENCE.md | 2h | All |
| 4.3 | Add integration tests for algorithm framework | 4h | 1.x |
| 4.4 | Add integration tests for TUI flows | 4h | 2.x |
| 4.5 | Bug fixes and edge cases | 8h | All |
| 4.6 | Tag v0.1 release | 1h | All |

**Deliverable:** Stable, documented v0.1 release

---

## Task Breakdown: Immediate Actions

### Today's Tasks (CLI/TUI Parity)

```
[ ] 0.1 Add `info` command to TUI
    - Show data statistics (file count, date range, event count)
    - Mirror output of `cargo run --bin backtest -- info`

[ ] 0.2 Add `grid-search` to TUI
    - Add menu option under backtesting
    - Allow setting: spreads, skews, entropy thresholds
    - Show progress during search
    - Display results table when complete

[ ] 0.3 Add `sweep` to TUI
    - Similar to grid-search but simpler
    - Allow custom parameter ranges
```

### This Week's Tasks

```
[ ] 0.4 Add `oos-validate` to TUI
[ ] 0.5 Rename CLI commands:
    - grid-search → tune
    - walk-forward → validate
    - walk-forward-ml → train
    - backtest → evaluate
[ ] 0.6 Ensure --algorithm flag works with all commands
```

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| A-S doesn't work in paper trading | Medium | High | Fail fast, learn why |
| Scope creep to add more algorithms | High | Medium | Stick to 5 principles |
| TUI complexity grows unmanageable | Medium | Medium | Keep menu structure flat initially |
| Data quality issues discovered | Low | High | Run validate command regularly |
| Fill model assumptions wrong | High | High | Measure real fills in paper trading |

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-12-11 | Limit v0.1 to 3 algorithms | PPO/SAC would take months, A-S not validated yet |
| 2025-12-11 | Defer hierarchical menu | Flat menu sufficient, complexity not justified |
| 2025-12-11 | Paper trading validation required | No point adding algorithms if base doesn't work |
| 2025-12-11 | 6-8 week timeline | Realistic given scope and validation requirements |

---

## Appendix A: Command Naming Convention

### Current → v0.1

| Current | v0.1 | Rationale |
|---------|------|-----------|
| `grid-search` | `tune` | Clearer intent |
| `walk-forward` | `validate` | Industry standard term |
| `walk-forward-ml` | `train` | Clearer for ML context |
| `single` (backtest) | `evaluate` | More descriptive |
| `simulate-campaign` | `simulate` | Shorter |

### Full CLI Structure (v0.1)

```
backtest
├── evaluate      # Single backtest run
├── sweep         # Parameter sensitivity
├── tune          # Hyperparameter optimization (grid/random/bayesian)
├── validate      # Walk-forward cross-validation
├── train         # ML weight training
├── simulate      # Campaign simulation
├── info          # Data statistics
├── algorithms    # List available algorithms
└── compare       # Head-to-head algorithm comparison
```

---

## Appendix B: What v0.2 Might Include

Based on v0.1 learnings, v0.2 candidates:

| Feature | Condition for Inclusion |
|---------|------------------------|
| Gradient Boosting | If Linear Model shows promise |
| Multi-level orders | If fill rate is limiting factor |
| Neural Network | If sufficient data collected |
| Hierarchical TUI menu | If current menu becomes unwieldy |
| Status bar | User feedback indicates need |

---

## Appendix C: File Structure After v0.1

```
src/
├── algorithms/
│   ├── core/
│   │   ├── mod.rs
│   │   ├── traits.rs         # MarketMakingAlgorithm, Configurable, Trainable
│   │   ├── parameter.rs      # ParameterDefinition, ParameterType
│   │   ├── registry.rs       # AlgorithmRegistry
│   │   └── factory.rs        # create_algorithm(name, config)
│   │
│   ├── rule_based/
│   │   ├── mod.rs
│   │   ├── fixed_spread.rs   # NEW: Simplest baseline
│   │   └── avellaneda_stoikov.rs
│   │
│   └── statistical/
│       ├── mod.rs
│       └── linear_model.rs   # Renamed from ml_spread_skew
│
├── bin/
│   └── backtest.rs           # Updated with new command names
│
└── tui.rs                    # Updated with CLI parity
```

---

## Summary

**v0.1 is about proving the foundation works, not building a cathedral.**

The single most important outcome: Understanding whether A-S generates real profit in paper trading.

Everything else—more algorithms, fancy UIs, ML pipelines—is premature optimization until we answer that question.

---

*Document maintained by: Development Team*
*Last updated: December 11, 2025*
