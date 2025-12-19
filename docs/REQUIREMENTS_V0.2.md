# INGESTOR v0.2 Requirements & Roadmap

**Document Version:** 3.0
**Created:** December 13, 2025
**Last Updated:** December 18, 2025
**Philosophy:** Framework-centric design where the framework persists and algorithms are ephemeral instances born from research.

---

## Executive Summary

v0.2 implements a **persistent research framework** that continuously validates trading hypotheses and spawns parameterized algorithms when edge is detected.

**Key Insight:** Algorithms are ephemeral; the framework is permanent. Research runs continuously, validation pipelines are reusable, and algorithms are created/destroyed based on research findings.

**Core Architecture:**
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PERSISTENT FRAMEWORK                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  RESEARCH ENGINE (Always Running, Persists State)                           │
│  └── Answers: "Is I(Past; Future) > 0 right now?"                          │
│                         │                                                   │
│                         ▼                                                   │
│  VALIDATION PIPELINE (Unified, Reusable)                                    │
│  └── Backtest → Forward → OOS → Paper → Live+OCO                           │
│                         │                                                   │
│                         ▼                                                   │
│  ALGORITHM FACTORY (Parameterized)                                          │
│  └── Creates algorithm instances from research config                       │
│                         │                                                   │
│                         ▼                                                   │
│  RESULTS PERSISTENCE (Feedback Loop)                                        │
│  └── All results feed back to research engine                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Timeline:** 3 weeks
**Success Metric:** Framework that can continuously detect edge, validate it, and deploy bounded-risk algorithms

---

## Task Numbering System

```
Phase.Task format:
  0.x = Framework Foundation
  1.x = Research Engine
  2.x = Validation Pipeline
  3.x = Algorithm Framework
  4.x = Integration & TUI
```

---

## Phase 0: Framework Foundation

**Goal:** Build the persistent infrastructure that all other components depend on.

### Task 0.0: ResearchState Struct

**Aim:** Define the unified data structure that captures all research findings at a point in time.

**Details:**
- Contains MIDC estimate (κ, τ_half, confidence)
- Contains conditional probability tables
- Contains regime classification
- Contains persistence statistics
- Timestamped for historical tracking
- Serializable to Parquet

**File:** `src/framework/research_state.rs`

**Acceptance Criteria:**
- [ ] `ResearchState` struct with all fields
- [ ] `Default` implementation
- [ ] Serialization/deserialization working
- [ ] Unit tests for serialization roundtrip

---

### Task 0.1: ResearchStore Persistence

**Aim:** Implement persistence layer for research state so findings survive restarts.

**Details:**
- Save `ResearchState` to Parquet files
- Load previous state on startup
- Support checkpointing (periodic saves)
- Support historical queries (load state at time T)
- Append-only log for audit trail

**File:** `src/framework/research_store.rs`

**Acceptance Criteria:**
- [ ] `save(&ResearchState)` writes to disk
- [ ] `load() -> Option<ResearchState>` reads latest
- [ ] `load_at(timestamp) -> Option<ResearchState>` for historical
- [ ] `checkpoint()` for periodic saves
- [ ] Unit tests with temp directories

---

### Task 0.2: ValidationResult Struct

**Aim:** Define unified result structure that all validation stages produce.

**Details:**
- Stage name and type (Backtest/Forward/OOS/Paper/Live)
- Time period covered
- Key metrics: Sharpe, drawdown, win rate, trade count
- Per-trade results with research state at entry
- Pass/fail threshold evaluation
- Serializable for persistence

**File:** `src/framework/validation_result.rs`

**Acceptance Criteria:**
- [ ] `ValidationResult` struct with all metrics
- [ ] `ValidationStageType` enum
- [ ] `TradeResult` struct for per-trade data
- [ ] `passed_threshold(&self, config) -> bool` method
- [ ] Unit tests

---

### Task 0.3: ResultsStore Persistence

**Aim:** Persist all validation results for analysis and feedback loop.

**Details:**
- Save `ValidationResult` to Parquet
- Query results by stage, time period, algorithm config
- Support aggregation (average Sharpe across runs)
- Link results to research state that generated them

**File:** `src/framework/results_store.rs`

**Acceptance Criteria:**
- [ ] `save(&ValidationResult)` writes to disk
- [ ] `load_by_stage(stage) -> Vec<ValidationResult>`
- [ ] `load_by_config(config_id) -> Vec<ValidationResult>`
- [ ] Query methods for analysis
- [ ] Unit tests

---

### Task 0.4: AlgorithmConfig Struct

**Aim:** Define the configuration structure that parameterizes algorithms from research.

**Details:**
- Strategy type (Momentum/MarketMaking/Hybrid)
- Entry thresholds derived from research
- Exit parameters (TP/SL in bps)
- Position sizing parameters
- Regime filters (min τ_half, max entropy)
- Unique config ID for tracking

**File:** `src/framework/algorithm_config.rs`

**Acceptance Criteria:**
- [ ] `AlgorithmConfig` struct with all parameters
- [ ] `StrategyType` enum
- [ ] `generate_id(&self) -> String` for unique identification
- [ ] `from_research(&ResearchState) -> Self` constructor
- [ ] Unit tests

---

### Task 0.5: ConfigStore Persistence

**Aim:** Persist algorithm configurations for reproducibility and audit.

**Details:**
- Save `AlgorithmConfig` to JSON/Parquet
- Load configs by ID
- Track config lineage (which research state generated it)
- Support config comparison

**File:** `src/framework/config_store.rs`

**Acceptance Criteria:**
- [ ] `save(&AlgorithmConfig)` writes to disk
- [ ] `load(config_id) -> Option<AlgorithmConfig>`
- [ ] `list_all() -> Vec<AlgorithmConfig>`
- [ ] Unit tests

---

### Task 0.6: Framework Module Integration

**Aim:** Create the framework module and wire all components together.

**Details:**
- Create `src/framework/mod.rs`
- Export all public types
- Add framework to main `lib.rs`
- Integration tests for store interactions

**File:** `src/framework/mod.rs`

**Acceptance Criteria:**
- [ ] Module compiles and exports correctly
- [ ] Integration test: save research → save config → save result → load all
- [ ] Documentation for public API

---

## Phase 1: Research Engine

**Goal:** Build the continuous research process that detects mutual information between past features and future price.

### Task 1.0: Research Module Structure

**Aim:** Create the research module with the core `ResearchEngine` trait.

**Details:**
- Define `ResearchEngine` trait for pluggable research implementations
- Define `TradeableAssessment` output struct
- Create module structure for components
- Integration point with `ResearchStore`

**File:** `src/research/mod.rs`, `src/research/traits.rs`

**Acceptance Criteria:**
- [ ] `ResearchEngine` trait defined
- [ ] `TradeableAssessment` struct with viability flags
- [ ] Module structure created
- [ ] Trait documentation

---

### Task 1.1: MIDCEstimator Implementation

**Aim:** Implement the Market Information Diffusion Coefficient estimator with persistence.

**Details:**
- Compute returns at multiple time scales
- Calculate autocorrelation function
- Fit exponential decay: ρ(Δ) = ρ₀ · e^(-κΔ)
- Extract κ (MIDC) and τ_half (predictability horizon)
- Rolling updates for continuous estimation
- Persist estimates to `ResearchStore`

**File:** `src/research/midc_estimator.rs`

**Acceptance Criteria:**
- [ ] `MIDCEstimator` struct with configuration
- [ ] `estimate(&[PricePoint]) -> MIDCEstimate` for batch
- [ ] `update(&PricePoint)` for streaming
- [ ] `current() -> MIDCEstimate` getter
- [ ] Rolling window support
- [ ] 20+ unit tests including edge cases

---

### Task 1.2: PersistenceAnalyzer Implementation

**Aim:** Analyze how long trends persist after detection.

**Details:**
- Detect trend starts (significant directional move)
- Track trend duration until reversal
- Build distribution of trend durations
- Segment by regime (MIDC quartiles)
- Rolling updates for continuous analysis

**File:** `src/research/persistence_analyzer.rs`

**Acceptance Criteria:**
- [ ] `PersistenceAnalyzer` struct
- [ ] `on_price(&PricePoint)` for streaming updates
- [ ] `get_stats() -> PersistenceStats` with mean, median, percentiles
- [ ] `get_stats_by_regime(regime) -> PersistenceStats`
- [ ] 15+ unit tests

---

### Task 1.3: PriceSignature Implementation

**Aim:** Discretize price movements into signatures for conditional probability modeling.

**Details:**
- Magnitude buckets: Tiny/Small/Medium/Large/VeryLarge
- Speed buckets: Slow/Normal/Fast
- Direction: Up/Down
- Consistency: Choppy/Smooth (based on monotonicity)
- Configurable bucket boundaries

**File:** `src/research/price_signature.rs`

**Acceptance Criteria:**
- [ ] `PriceSignature` struct with all dimensions
- [ ] `SignatureConfig` for bucket boundaries
- [ ] `from_price_window(&[PricePoint], config) -> PriceSignature`
- [ ] `to_key(&self) -> String` for hash map keys
- [ ] 15+ unit tests

---

### Task 1.4: ConditionalModel Implementation

**Aim:** Build and update conditional probability tables P(continuation | signature).

**Details:**
- Track outcomes for each signature
- Compute P(continuation), P(reversal), expected magnitude
- Incremental updates (don't recompute from scratch)
- Confidence intervals based on sample size
- Persist tables to `ResearchStore`

**File:** `src/research/conditional_model.rs`

**Acceptance Criteria:**
- [ ] `ConditionalModel` struct
- [ ] `record_outcome(&PriceSignature, &Outcome)` for updates
- [ ] `get_probability(&PriceSignature) -> ConditionalProbability`
- [ ] `get_all_significant(min_samples, min_edge) -> Vec<SignificantSignal>`
- [ ] Serialization for persistence
- [ ] 20+ unit tests

---

### Task 1.5: ResearchEngine Orchestrator

**Aim:** Combine all research components into a single orchestrating engine.

**Details:**
- Owns MIDCEstimator, PersistenceAnalyzer, ConditionalModel
- Processes incoming features/prices
- Updates all components
- Produces `TradeableAssessment`
- Generates `AlgorithmConfig` when edge detected
- Checkpoints to `ResearchStore`

**File:** `src/research/engine.rs`

**Acceptance Criteria:**
- [ ] `ResearchEngine` struct with all components
- [ ] `new(config, store) -> Self` constructor
- [ ] `load_or_init(store_path) -> Self` for restart recovery
- [ ] `on_features(&FeaturesSnapshot)` for streaming
- [ ] `assess() -> TradeableAssessment`
- [ ] `generate_config() -> Option<AlgorithmConfig>`
- [ ] `checkpoint() -> Result<()>`
- [ ] Integration tests

---

### Task 1.6: Research Run CLI Command

**Aim:** CLI command to run research on historical data.

**Details:**
- Load Parquet feature files
- Run research engine on historical data
- Save final state to `ResearchStore`
- Print summary statistics
- Support date range filtering

**Command:** `cargo run --bin research -- run --data ./data/features --output ./research/`

**File:** `src/bin/research.rs` (partial)

**Acceptance Criteria:**
- [ ] `run` subcommand implemented
- [ ] Progress bar for long runs
- [ ] Summary output with MIDC, top signals
- [ ] State persisted to disk

---

### Task 1.7: Research Status CLI Command

**Aim:** CLI command to display current research state.

**Details:**
- Load latest `ResearchState` from store
- Display MIDC estimate and interpretation
- Display top conditional signals
- Display regime assessment
- Display recommendation

**Command:** `cargo run --bin research -- status`

**File:** `src/bin/research.rs` (partial)

**Acceptance Criteria:**
- [ ] `status` subcommand implemented
- [ ] Formatted output with all key metrics
- [ ] Color-coded regime assessment

---

### Task 1.8: Live Feature Integration

**Aim:** Connect research engine to live feature stream for continuous updates.

**Details:**
- Subscribe to `FeaturesSnapshot` channel
- Update research engine on each snapshot
- Periodic checkpointing (every N minutes)
- Emit `TradeableAssessment` changes

**File:** `src/research/live_integration.rs`

**Acceptance Criteria:**
- [ ] `LiveResearchRunner` struct
- [ ] Async task that processes feature stream
- [ ] Configurable checkpoint interval
- [ ] Integration with main application

---

## Phase 2: Validation Pipeline

**Goal:** Build a unified validation pipeline that takes any algorithm through Backtest → Forward → OOS → Paper → Live.

### Task 2.0: ValidationPipeline Trait

**Aim:** Define the trait and interfaces for validation stages.

**Details:**
- `ValidationStage` trait for individual stages
- `ValidationPipeline` for orchestrating stages
- Stage configuration (thresholds, data splits)
- Stop conditions (fail threshold, proceed to next)

**File:** `src/validation/mod.rs`, `src/validation/traits.rs`

**Acceptance Criteria:**
- [ ] `ValidationStage` trait defined
- [ ] `ValidationPipeline` struct
- [ ] `StageConfig` for thresholds
- [ ] Documentation

---

### Task 2.1: BacktestStage Implementation

**Aim:** Historical replay validation stage.

**Details:**
- Load historical features from Parquet
- Replay through algorithm with research state
- Track all trades and outcomes
- Compute metrics (Sharpe, drawdown, win rate)
- Produce `ValidationResult`

**File:** `src/validation/backtest_stage.rs`

**Acceptance Criteria:**
- [ ] `BacktestStage` struct
- [ ] `run(&AlgorithmConfig, &ResearchState, data_path) -> ValidationResult`
- [ ] Configurable date range
- [ ] Realistic fill assumptions
- [ ] 15+ tests

---

### Task 2.2: ForwardStage Implementation

**Aim:** Walk-forward validation stage.

**Details:**
- Split data into train/test windows
- Train on window N, test on window N+1
- Roll forward through all windows
- Aggregate results across windows
- Detect parameter stability

**File:** `src/validation/forward_stage.rs`

**Acceptance Criteria:**
- [ ] `ForwardStage` struct
- [ ] Configurable window sizes
- [ ] `run(&AlgorithmConfig, data_path) -> ValidationResult`
- [ ] Per-window and aggregate metrics
- [ ] 10+ tests

---

### Task 2.3: OOSStage Implementation

**Aim:** Out-of-sample validation on held-out data.

**Details:**
- Use final 20% of data (never seen during research)
- Single pass evaluation
- Strict no-lookahead guarantee
- Final go/no-go decision

**File:** `src/validation/oos_stage.rs`

**Acceptance Criteria:**
- [ ] `OOSStage` struct
- [ ] `run(&AlgorithmConfig, data_path) -> ValidationResult`
- [ ] Configurable holdout percentage
- [ ] 10+ tests

---

### Task 2.4: PaperStage Implementation

**Aim:** Paper trading with live data, simulated execution.

**Details:**
- Connect to live feature stream
- Run algorithm in real-time
- Simulate fills at market price + spread
- Track P&L in real-time
- Run for configurable duration

**File:** `src/validation/paper_stage.rs`

**Acceptance Criteria:**
- [ ] `PaperStage` struct
- [ ] Async execution with live data
- [ ] `run(&AlgorithmConfig, duration) -> ValidationResult`
- [ ] Real-time P&L tracking
- [ ] Graceful shutdown

---

### Task 2.5: LiveStage Implementation

**Aim:** Live trading with real execution and OCO risk management.

**Details:**
- Connect to exchange API (simulated for now)
- Execute trades with OCO brackets
- Track real fills and slippage
- Circuit breaker integration
- Full audit trail

**File:** `src/validation/live_stage.rs`

**Acceptance Criteria:**
- [ ] `LiveStage` struct
- [ ] OCO integration for every trade
- [ ] `run(&AlgorithmConfig, duration) -> ValidationResult`
- [ ] Slippage tracking
- [ ] Circuit breaker triggers

---

### Task 2.6: PipelineRunner Implementation

**Aim:** Orchestrate all validation stages with stop conditions.

**Details:**
- Run stages in sequence
- Stop if any stage fails threshold
- Persist results at each stage
- Produce final `PipelineResult`
- Support partial runs (start from stage N)

**File:** `src/validation/pipeline_runner.rs`

**Acceptance Criteria:**
- [ ] `PipelineRunner` struct
- [ ] `run_all(&AlgorithmConfig) -> PipelineResult`
- [ ] `run_from(stage, &AlgorithmConfig) -> PipelineResult`
- [ ] Stage-by-stage result persistence
- [ ] Integration tests

---

### Task 2.7: Validate CLI Command

**Aim:** CLI command to run validation pipeline.

**Details:**
- Load algorithm config (or generate from research)
- Run specified stages or full pipeline
- Display results and recommendations
- Save results to `ResultsStore`

**Command:** `cargo run --bin validate -- --config ./configs/algo.json --stages backtest,forward,oos`

**File:** `src/bin/validate.rs`

**Acceptance Criteria:**
- [ ] CLI with stage selection
- [ ] Progress display
- [ ] Summary output
- [ ] Results persisted

---

## Phase 3: Algorithm Framework

**Goal:** Algorithms are parameterized instances created from research, not hardcoded classes.

### Task 3.0: TradingAlgorithm Trait

**Aim:** Define unified trait for all trading algorithms (MM and MOM).

**Details:**
- Common interface for all algorithm types
- Receives features + research assessment
- Produces trading decisions
- Tracks internal state (position, P&L)
- Serializable state for checkpointing

**File:** `src/algorithms/traits.rs` (updated)

**Acceptance Criteria:**
- [ ] `TradingAlgorithm` trait defined
- [ ] `TradingDecision` enum
- [ ] `AlgorithmState` for checkpointing
- [ ] Documentation

---

### Task 3.1: MomentumAlgorithm Implementation

**Aim:** Parameterized momentum algorithm created from config.

**Details:**
- Entry based on conditional probability thresholds
- Exit via OCO (TP/SL from config)
- Position sizing from config
- Regime filters (skip if τ_half below threshold)
- No hardcoded parameters

**File:** `src/algorithms/momentum.rs`

**Acceptance Criteria:**
- [ ] `MomentumAlgorithm` struct
- [ ] `from_config(&AlgorithmConfig) -> Self`
- [ ] Implements `TradingAlgorithm` trait
- [ ] All parameters from config
- [ ] 20+ tests

---

### Task 3.2: MarketMakingAlgorithm Implementation

**Aim:** Parameterized market making algorithm for mean-reverting regimes.

**Details:**
- A-S based quoting with configurable parameters
- Spread from config
- Skew from inventory and regime
- Used when momentum not viable

**File:** `src/algorithms/market_making.rs`

**Acceptance Criteria:**
- [ ] `MarketMakingAlgorithm` struct
- [ ] `from_config(&AlgorithmConfig) -> Self`
- [ ] Implements `TradingAlgorithm` trait
- [ ] 15+ tests

---

### Task 3.3: AlgorithmFactory Implementation

**Aim:** Factory that creates algorithm instances from research state.

**Details:**
- Takes `ResearchState` as input
- Generates `AlgorithmConfig`
- Instantiates appropriate algorithm type
- Handles edge case (no edge detected → return None)

**File:** `src/algorithms/factory.rs`

**Acceptance Criteria:**
- [ ] `AlgorithmFactory` struct
- [ ] `create(&ResearchState) -> Option<Box<dyn TradingAlgorithm>>`
- [ ] Strategy type selection logic
- [ ] 10+ tests

---

### Task 3.4: Algorithm Registry

**Aim:** Registry for tracking active algorithm instances.

**Details:**
- Register/unregister algorithms
- Track algorithm lifecycle (created, running, stopped)
- Link algorithms to their source config
- Support multiple concurrent algorithms

**File:** `src/algorithms/registry.rs`

**Acceptance Criteria:**
- [ ] `AlgorithmRegistry` struct
- [ ] `register(&AlgorithmConfig, Box<dyn TradingAlgorithm>)`
- [ ] `get(config_id) -> Option<&dyn TradingAlgorithm>`
- [ ] `stop(config_id)`
- [ ] Tests

---

### Task 3.5: Remove Hardcoded Algorithms

**Aim:** Clean up legacy hardcoded algorithm implementations.

**Details:**
- Remove MOM_Simple, MOM_EntropyGated, MOM_MIDCFiltered files
- Migrate any useful logic to parameterized versions
- Update all references
- Update tests

**Files:** Various deletions

**Acceptance Criteria:**
- [ ] No hardcoded algorithm files remain
- [ ] All tests pass
- [ ] Documentation updated

---

### Task 3.6: Algorithm Create CLI Command

**Aim:** CLI command to create algorithm from current research.

**Details:**
- Load current research state
- Generate config
- Optionally run validation
- Save config to store

**Command:** `cargo run --bin algorithm -- create --validate`

**File:** `src/bin/algorithm.rs`

**Acceptance Criteria:**
- [ ] `create` subcommand
- [ ] Config generation from research
- [ ] Optional validation trigger
- [ ] Config persisted

---

## Phase 4: Integration & TUI

**Goal:** Wire everything together with a usable interface and feedback loop.

### Task 4.0: TUI Main Menu Restructure

**Aim:** Implement new 6-item main menu structure.

**Details:**
- [1] LIVE DATA - Real-time market data
- [2] RESEARCH - Research engine status and controls
- [3] VALIDATION - Validation pipeline controls
- [4] ALGORITHMS - Active algorithms dashboard
- [5] BACKTEST - Quick backtest access
- [6] SETTINGS - Configuration
- [Q] Quit

**File:** `src/ui/tui.rs` (refactored)

**Acceptance Criteria:**
- [ ] New menu structure implemented
- [ ] Clean key bindings (1-6, Q)
- [ ] Submenu navigation

---

### Task 4.1: Research Dashboard Screen

**Aim:** TUI screen displaying current research state.

**Details:**
- Current MIDC estimate with interpretation
- τ_half and regime classification
- Top conditional signals with probabilities
- Tradeable assessment
- Research engine status (running/paused)

**File:** `src/ui/screens/research.rs`

**Acceptance Criteria:**
- [ ] All research metrics displayed
- [ ] Color-coded regime indicator
- [ ] Refresh on new data
- [ ] Controls for run/pause/checkpoint

---

### Task 4.2: Validation Dashboard Screen

**Aim:** TUI screen displaying validation pipeline results.

**Details:**
- Pipeline stage status (passed/failed/pending)
- Metrics for each completed stage
- Current running stage progress
- Historical results summary

**File:** `src/ui/screens/validation.rs`

**Acceptance Criteria:**
- [ ] Stage status indicators
- [ ] Metrics table
- [ ] Progress for running stage
- [ ] Controls for start/stop

---

### Task 4.3: Algorithm Dashboard Screen

**Aim:** TUI screen displaying active algorithms.

**Details:**
- List of active algorithm instances
- Config summary for each
- Real-time P&L
- Controls for stop/restart

**File:** `src/ui/screens/algorithms.rs`

**Acceptance Criteria:**
- [ ] Algorithm list with status
- [ ] P&L display
- [ ] Config details on selection
- [ ] Stop/restart controls

---

### Task 4.4: Feedback Loop Implementation

**Aim:** Connect live trading results back to research engine.

**Details:**
- Capture all trade outcomes
- Update conditional model with real results
- Track prediction accuracy
- Detect edge decay (research vs actual)
- Alert when edge deteriorates

**File:** `src/framework/feedback_loop.rs`

**Acceptance Criteria:**
- [ ] `FeedbackLoop` struct
- [ ] `on_trade_result(&TradeResult)`
- [ ] Research engine updates
- [ ] Edge decay detection
- [ ] Integration tests

---

### Task 4.5: System Integration Tests

**Aim:** End-to-end tests for the complete framework.

**Details:**
- Test: Research → Config → Validation → Algorithm → Feedback
- Test: Restart recovery (load persisted state)
- Test: Edge decay triggers algorithm stop
- Test: Multiple concurrent algorithms

**File:** `tests/integration/framework_tests.rs`

**Acceptance Criteria:**
- [ ] Full cycle integration test
- [ ] Persistence/recovery test
- [ ] Edge decay test
- [ ] Concurrency test

---

## Implementation Sequence

### Week 1: Foundation + Research Core

```
Day 1: 0.0, 0.1 (ResearchState, ResearchStore)
Day 2: 0.2, 0.3 (ValidationResult, ResultsStore)
Day 3: 0.4, 0.5, 0.6 (AlgorithmConfig, ConfigStore, Module Integration)
Day 4: 1.0, 1.1 (Research Module, MIDCEstimator)
Day 5: 1.2, 1.3 (PersistenceAnalyzer, PriceSignature)
```

### Week 2: Research Complete + Validation Pipeline

```
Day 1: 1.4, 1.5 (ConditionalModel, ResearchEngine)
Day 2: 1.6, 1.7 (Research CLI commands)
Day 3: 2.0, 2.1 (ValidationPipeline trait, BacktestStage)
Day 4: 2.2, 2.3 (ForwardStage, OOSStage)
Day 5: 2.4, 2.6 (PaperStage, PipelineRunner)
```

### Week 3: Algorithms + Integration

```
Day 1: 3.0, 3.1 (TradingAlgorithm trait, MomentumAlgorithm)
Day 2: 3.2, 3.3 (MarketMakingAlgorithm, AlgorithmFactory)
Day 3: 2.5, 2.7 (LiveStage, Validate CLI)
Day 4: 1.8, 4.4 (Live Integration, Feedback Loop)
Day 5: 4.0, 4.1, 4.2 (TUI restructure, dashboards)
```

### Week 4: Polish + Testing (Buffer)

```
Day 1: 3.4, 3.5, 3.6 (Registry, Cleanup, Algorithm CLI)
Day 2: 4.3, 4.5 (Algorithm Dashboard, Integration Tests)
Day 3-5: Bug fixes, documentation, additional tests
```

---

## Complete Task Reference

| ID | Task | Description | Effort | Dependencies |
|----|------|-------------|--------|--------------|
| **0.0** | ResearchState | Unified research state struct | 2h | - |
| **0.1** | ResearchStore | Persistence for research state | 3h | 0.0 |
| **0.2** | ValidationResult | Unified validation result struct | 2h | - |
| **0.3** | ResultsStore | Persistence for validation results | 3h | 0.2 |
| **0.4** | AlgorithmConfig | Parameterized algorithm config | 2h | 0.0 |
| **0.5** | ConfigStore | Persistence for configs | 2h | 0.4 |
| **0.6** | Framework Module | Wire framework components | 2h | 0.0-0.5 |
| **1.0** | Research Module | Research module structure | 2h | 0.6 |
| **1.1** | MIDCEstimator | MIDC estimation with persistence | 4h | 1.0 |
| **1.2** | PersistenceAnalyzer | Trend duration analysis | 3h | 1.0 |
| **1.3** | PriceSignature | Price movement discretization | 2h | 1.0 |
| **1.4** | ConditionalModel | P(continuation\|signature) tables | 4h | 1.3 |
| **1.5** | ResearchEngine | Research orchestrator | 3h | 1.1, 1.2, 1.4 |
| **1.6** | Research Run CLI | Batch research command | 2h | 1.5 |
| **1.7** | Research Status CLI | Display research state | 1h | 1.5 |
| **1.8** | Live Integration | Connect to live features | 3h | 1.5 |
| **2.0** | ValidationPipeline | Pipeline trait and interfaces | 2h | 0.6 |
| **2.1** | BacktestStage | Historical replay validation | 3h | 2.0 |
| **2.2** | ForwardStage | Walk-forward validation | 3h | 2.0 |
| **2.3** | OOSStage | Out-of-sample validation | 2h | 2.0 |
| **2.4** | PaperStage | Paper trading validation | 4h | 2.0 |
| **2.5** | LiveStage | Live trading with OCO | 4h | 2.0 |
| **2.6** | PipelineRunner | Orchestrate all stages | 3h | 2.1-2.4 |
| **2.7** | Validate CLI | Validation command | 2h | 2.6 |
| **3.0** | TradingAlgorithm | Unified algorithm trait | 2h | 0.4 |
| **3.1** | MomentumAlgorithm | Parameterized momentum | 4h | 3.0 |
| **3.2** | MarketMakingAlgorithm | Parameterized MM | 3h | 3.0 |
| **3.3** | AlgorithmFactory | Create algorithms from research | 2h | 3.1, 3.2 |
| **3.4** | AlgorithmRegistry | Track active algorithms | 2h | 3.3 |
| **3.5** | Remove Hardcoded | Clean up legacy code | 2h | 3.3 |
| **3.6** | Algorithm CLI | Algorithm management commands | 2h | 3.3 |
| **4.0** | TUI Menu | Restructure main menu | 2h | - |
| **4.1** | Research Dashboard | Research TUI screen | 3h | 1.5 |
| **4.2** | Validation Dashboard | Validation TUI screen | 3h | 2.6 |
| **4.3** | Algorithm Dashboard | Algorithm TUI screen | 2h | 3.4 |
| **4.4** | Feedback Loop | Results → Research | 3h | 1.5, 2.5 |
| **4.5** | Integration Tests | End-to-end tests | 4h | All |

---

## Success Criteria

### Framework (Phase 0)
- [ ] All stores persist and load correctly
- [ ] State survives application restart
- [ ] Audit trail for all changes

### Research (Phase 1)
- [ ] MIDC estimates within expected range
- [ ] Conditional tables populated with >1000 samples
- [ ] Research runs continuously without memory leaks

### Validation (Phase 2)
- [ ] Pipeline runs all stages in sequence
- [ ] Stops correctly on threshold failure
- [ ] Results match manual calculations

### Algorithms (Phase 3)
- [ ] Algorithms created purely from config
- [ ] No hardcoded parameters
- [ ] Multiple algorithms can run concurrently

### Integration (Phase 4)
- [ ] Feedback loop updates research
- [ ] Edge decay detected and acted upon
- [ ] Full cycle works end-to-end

---

## Summary

**This framework implements your key insight:**

> Algorithms are ephemeral; the framework persists. Research runs continuously, validation is reusable, and algorithms are born from research findings.

The framework:
1. **Persists everything** - Research state, validation results, configs
2. **Runs research continuously** - Always knows current I(Past; Future)
3. **Validates uniformly** - Same pipeline for all algorithms
4. **Creates algorithms dynamically** - From research, not hardcoded
5. **Learns from results** - Feedback loop improves research

**Total: 37 tasks, ~95 hours, 3-4 weeks**

---

*Document maintained by: Development Team*
*Last updated: December 18, 2025*
