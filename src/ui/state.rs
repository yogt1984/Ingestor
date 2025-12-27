//! Global UI State Management (Task TUI-0.0)
//!
//! Provides centralized state management for the TUI, tracking:
//! - Active trading symbol
//! - Selected algorithm configuration
//! - Validation pipeline status
//! - Current trading mode (idle/paper/live)
//! - Research engine status
//! - Data statistics
//!
//! Per TUI_REQUIREMENTS_V0.1.md, this state is shared across all screens
//! and provides the foundation for the status bar display.

use chrono::{DateTime, NaiveDate, Utc};
use crate::core::algorithm_config::StrategyType;

// ============================================================================
// Core State Structures
// ============================================================================

/// Global application state shared across all TUI screens
///
/// This struct holds all application-wide state that needs to be
/// accessible from any screen in the TUI. It's designed to be
/// lightweight for frequent updates and display.
#[derive(Debug, Clone)]
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
///
/// This is a denormalized view of the full AlgorithmConfig,
/// containing only the fields needed for status display.
#[derive(Debug, Clone, PartialEq)]
pub struct AlgorithmConfigSummary {
    /// Unique identifier for the algorithm config
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Strategy type (Momentum, MarketMaking, Hybrid)
    pub strategy_type: StrategyType,
    /// When this config was created
    pub created_at: DateTime<Utc>,
}

/// Validation status for each pipeline stage
///
/// Tracks the status of all 5 validation stages:
/// Backtest -> Forward -> OOS -> Paper -> Live
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationStatus {
    /// Historical backtest stage
    pub backtest: StageStatus,
    /// Walk-forward validation stage
    pub forward: StageStatus,
    /// Out-of-sample validation stage
    pub oos: StageStatus,
    /// Paper trading stage
    pub paper: StageStatus,
    /// Live trading stage
    pub live: StageStatus,
}

/// Status of a single validation stage
#[derive(Debug, Clone, PartialEq)]
pub enum StageStatus {
    /// Stage has not been run yet
    NotRun,
    /// Stage passed with given Sharpe ratio
    Passed {
        sharpe: f64,
        timestamp: DateTime<Utc>,
    },
    /// Stage failed with given reason
    Failed {
        reason: String,
        timestamp: DateTime<Utc>,
    },
    /// Stage is currently running
    Running {
        /// Progress from 0.0 to 1.0
        progress: f64,
    },
}

/// Current trading mode
#[derive(Debug, Clone, PartialEq)]
pub enum TradingMode {
    /// Not actively trading
    Idle,
    /// Paper trading (simulated)
    Paper {
        started: DateTime<Utc>,
        pnl: f64,
    },
    /// Live trading (real money)
    Live {
        started: DateTime<Utc>,
        pnl: f64,
    },
}

/// Research engine status
#[derive(Debug, Clone, PartialEq)]
pub enum ResearchStatus {
    /// Research engine is idle
    Idle,
    /// Research is actively running
    Running {
        samples_processed: usize,
    },
    /// Research is complete
    Complete {
        /// Whether the research found a tradeable edge
        tradeable: bool,
    },
}

/// Statistics about available data
#[derive(Debug, Clone, PartialEq)]
pub struct DataStats {
    /// Number of Parquet files available
    pub file_count: usize,
    /// Total number of events/rows
    pub total_events: usize,
    /// Date range of data (start, end)
    pub date_range: Option<(NaiveDate, NaiveDate)>,
    /// Total data size in megabytes
    pub size_mb: f64,
}

// ============================================================================
// Default Implementations
// ============================================================================

impl Default for GlobalState {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            active_algorithm: None,
            validation_status: ValidationStatus::default(),
            trading_mode: TradingMode::Idle,
            research_status: ResearchStatus::Idle,
            data_stats: DataStats::default(),
        }
    }
}

impl Default for ValidationStatus {
    fn default() -> Self {
        Self {
            backtest: StageStatus::NotRun,
            forward: StageStatus::NotRun,
            oos: StageStatus::NotRun,
            paper: StageStatus::NotRun,
            live: StageStatus::NotRun,
        }
    }
}

impl Default for StageStatus {
    fn default() -> Self {
        StageStatus::NotRun
    }
}

impl Default for TradingMode {
    fn default() -> Self {
        TradingMode::Idle
    }
}

impl Default for ResearchStatus {
    fn default() -> Self {
        ResearchStatus::Idle
    }
}

impl Default for DataStats {
    fn default() -> Self {
        Self {
            file_count: 0,
            total_events: 0,
            date_range: None,
            size_mb: 0.0,
        }
    }
}

// ============================================================================
// GlobalState Implementation
// ============================================================================

impl GlobalState {
    /// Create a new GlobalState with the given symbol
    ///
    /// All other fields are initialized to their defaults.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            ..Default::default()
        }
    }

    /// Load state from persistence stores
    ///
    /// This method populates the GlobalState from:
    /// - ConfigStore for active algorithm
    /// - ResultsStore for validation status
    /// - Research store for research status
    /// - Data directory for data stats
    ///
    /// Note: This is a placeholder that returns default state.
    /// Full implementation requires integration with stores.
    pub fn load_from_stores(_symbol: &str) -> Self {
        // TODO: Implement full store integration in TUI-8.0
        Self::default()
    }

    /// Update the active algorithm
    pub fn set_active_algorithm(&mut self, summary: Option<AlgorithmConfigSummary>) {
        self.active_algorithm = summary;
        // Reset validation status when algorithm changes
        if self.active_algorithm.is_none() {
            self.validation_status = ValidationStatus::default();
        }
    }

    /// Update a specific validation stage status
    pub fn set_stage_status(&mut self, stage: ValidationStage, status: StageStatus) {
        match stage {
            ValidationStage::Backtest => self.validation_status.backtest = status,
            ValidationStage::Forward => self.validation_status.forward = status,
            ValidationStage::Oos => self.validation_status.oos = status,
            ValidationStage::Paper => self.validation_status.paper = status,
            ValidationStage::Live => self.validation_status.live = status,
        }
    }

    /// Get the number of passed validation stages
    pub fn passed_stages(&self) -> usize {
        [
            &self.validation_status.backtest,
            &self.validation_status.forward,
            &self.validation_status.oos,
            &self.validation_status.paper,
            &self.validation_status.live,
        ]
        .iter()
        .filter(|s| matches!(s, StageStatus::Passed { .. }))
        .count()
    }

    /// Get the total number of validation stages
    pub fn total_stages() -> usize {
        5
    }

    /// Check if all required stages have passed for live trading
    ///
    /// Per TUI_REQUIREMENTS_V0.1.md, live trading requires:
    /// Backtest ✓, Forward ✓, OOS ✓, Paper ✓
    pub fn can_trade_live(&self) -> bool {
        matches!(self.validation_status.backtest, StageStatus::Passed { .. })
            && matches!(self.validation_status.forward, StageStatus::Passed { .. })
            && matches!(self.validation_status.oos, StageStatus::Passed { .. })
            && matches!(self.validation_status.paper, StageStatus::Passed { .. })
    }

    /// Check if any validation stage is currently running
    pub fn is_validating(&self) -> bool {
        matches!(self.validation_status.backtest, StageStatus::Running { .. })
            || matches!(self.validation_status.forward, StageStatus::Running { .. })
            || matches!(self.validation_status.oos, StageStatus::Running { .. })
            || matches!(self.validation_status.paper, StageStatus::Running { .. })
            || matches!(self.validation_status.live, StageStatus::Running { .. })
    }

    /// Check if research is currently running
    pub fn is_researching(&self) -> bool {
        matches!(self.research_status, ResearchStatus::Running { .. })
    }

    /// Check if currently trading (paper or live)
    pub fn is_trading(&self) -> bool {
        !matches!(self.trading_mode, TradingMode::Idle)
    }

    /// Get the current P&L if trading
    pub fn current_pnl(&self) -> Option<f64> {
        match &self.trading_mode {
            TradingMode::Idle => None,
            TradingMode::Paper { pnl, .. } => Some(*pnl),
            TradingMode::Live { pnl, .. } => Some(*pnl),
        }
    }

    /// Update data statistics
    pub fn update_data_stats(&mut self, stats: DataStats) {
        self.data_stats = stats;
    }

    /// Update trading P&L
    pub fn update_pnl(&mut self, pnl: f64) {
        match &mut self.trading_mode {
            TradingMode::Paper { pnl: current, .. } => *current = pnl,
            TradingMode::Live { pnl: current, .. } => *current = pnl,
            TradingMode::Idle => {}
        }
    }

    /// Start paper trading
    pub fn start_paper_trading(&mut self) {
        self.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: 0.0,
        };
    }

    /// Start live trading (requires validation)
    ///
    /// Returns false if validation requirements not met.
    pub fn start_live_trading(&mut self) -> bool {
        if !self.can_trade_live() {
            return false;
        }
        self.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: 0.0,
        };
        true
    }

    /// Stop trading (return to idle)
    pub fn stop_trading(&mut self) {
        self.trading_mode = TradingMode::Idle;
    }
}

// ============================================================================
// Helper Types
// ============================================================================

/// Enum for selecting validation stages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidationStage {
    Backtest,
    Forward,
    Oos,
    Paper,
    Live,
}

impl ValidationStage {
    /// Get all validation stages in order
    pub fn all() -> &'static [ValidationStage] {
        &[
            ValidationStage::Backtest,
            ValidationStage::Forward,
            ValidationStage::Oos,
            ValidationStage::Paper,
            ValidationStage::Live,
        ]
    }

    /// Get the display name for this stage
    pub fn display_name(&self) -> &'static str {
        match self {
            ValidationStage::Backtest => "Backtest",
            ValidationStage::Forward => "Walk-Forward",
            ValidationStage::Oos => "Out-of-Sample",
            ValidationStage::Paper => "Paper Trading",
            ValidationStage::Live => "Live Trading",
        }
    }

    /// Get the short name for this stage (for status bar)
    pub fn short_name(&self) -> &'static str {
        match self {
            ValidationStage::Backtest => "BT",
            ValidationStage::Forward => "WF",
            ValidationStage::Oos => "OOS",
            ValidationStage::Paper => "Paper",
            ValidationStage::Live => "Live",
        }
    }
}

// ============================================================================
// StageStatus Helper Methods
// ============================================================================

impl StageStatus {
    /// Check if this stage has passed
    pub fn is_passed(&self) -> bool {
        matches!(self, StageStatus::Passed { .. })
    }

    /// Check if this stage has failed
    pub fn is_failed(&self) -> bool {
        matches!(self, StageStatus::Failed { .. })
    }

    /// Check if this stage is running
    pub fn is_running(&self) -> bool {
        matches!(self, StageStatus::Running { .. })
    }

    /// Check if this stage has not been run
    pub fn is_not_run(&self) -> bool {
        matches!(self, StageStatus::NotRun)
    }

    /// Get the Sharpe ratio if passed
    pub fn sharpe(&self) -> Option<f64> {
        match self {
            StageStatus::Passed { sharpe, .. } => Some(*sharpe),
            _ => None,
        }
    }

    /// Get the progress if running
    pub fn progress(&self) -> Option<f64> {
        match self {
            StageStatus::Running { progress } => Some(*progress),
            _ => None,
        }
    }

    /// Get the failure reason if failed
    pub fn failure_reason(&self) -> Option<&str> {
        match self {
            StageStatus::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }

    /// Create a passed status with current timestamp
    pub fn passed(sharpe: f64) -> Self {
        StageStatus::Passed {
            sharpe,
            timestamp: Utc::now(),
        }
    }

    /// Create a failed status with current timestamp
    pub fn failed(reason: impl Into<String>) -> Self {
        StageStatus::Failed {
            reason: reason.into(),
            timestamp: Utc::now(),
        }
    }

    /// Create a running status
    pub fn running(progress: f64) -> Self {
        StageStatus::Running {
            progress: progress.clamp(0.0, 1.0),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // GlobalState Tests
    // ========================================================================

    #[test]
    fn test_global_state_default() {
        let state = GlobalState::default();
        assert_eq!(state.symbol, "BTCUSDT");
        assert!(state.active_algorithm.is_none());
        assert_eq!(state.trading_mode, TradingMode::Idle);
        assert_eq!(state.research_status, ResearchStatus::Idle);
        assert_eq!(state.data_stats.file_count, 0);
    }

    #[test]
    fn test_global_state_new_with_symbol() {
        let state = GlobalState::new("ETHUSDT");
        assert_eq!(state.symbol, "ETHUSDT");
        assert!(state.active_algorithm.is_none());
    }

    #[test]
    fn test_global_state_new_with_string() {
        let state = GlobalState::new(String::from("SOLUSDT"));
        assert_eq!(state.symbol, "SOLUSDT");
    }

    #[test]
    fn test_global_state_new_with_empty_symbol() {
        let state = GlobalState::new("");
        assert_eq!(state.symbol, "");
    }

    #[test]
    fn test_passed_stages_initially_zero() {
        let state = GlobalState::default();
        assert_eq!(state.passed_stages(), 0);
    }

    #[test]
    fn test_passed_stages_count() {
        let mut state = GlobalState::default();
        state.validation_status.backtest = StageStatus::passed(1.5);
        assert_eq!(state.passed_stages(), 1);

        state.validation_status.forward = StageStatus::passed(1.2);
        assert_eq!(state.passed_stages(), 2);

        state.validation_status.oos = StageStatus::failed("Low Sharpe");
        assert_eq!(state.passed_stages(), 2); // Failed doesn't count
    }

    #[test]
    fn test_total_stages() {
        assert_eq!(GlobalState::total_stages(), 5);
    }

    #[test]
    fn test_can_trade_live_requires_all_stages() {
        let mut state = GlobalState::default();
        assert!(!state.can_trade_live());

        state.validation_status.backtest = StageStatus::passed(1.5);
        assert!(!state.can_trade_live());

        state.validation_status.forward = StageStatus::passed(1.2);
        assert!(!state.can_trade_live());

        state.validation_status.oos = StageStatus::passed(0.8);
        assert!(!state.can_trade_live());

        state.validation_status.paper = StageStatus::passed(0.9);
        assert!(state.can_trade_live());
    }

    #[test]
    fn test_can_trade_live_fails_with_failed_stage() {
        let mut state = GlobalState::default();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::failed("Test failure");
        state.validation_status.oos = StageStatus::passed(0.8);
        state.validation_status.paper = StageStatus::passed(0.9);
        assert!(!state.can_trade_live());
    }

    #[test]
    fn test_is_validating() {
        let mut state = GlobalState::default();
        assert!(!state.is_validating());

        state.validation_status.backtest = StageStatus::running(0.5);
        assert!(state.is_validating());

        state.validation_status.backtest = StageStatus::passed(1.0);
        assert!(!state.is_validating());
    }

    #[test]
    fn test_is_researching() {
        let mut state = GlobalState::default();
        assert!(!state.is_researching());

        state.research_status = ResearchStatus::Running { samples_processed: 100 };
        assert!(state.is_researching());

        state.research_status = ResearchStatus::Complete { tradeable: true };
        assert!(!state.is_researching());
    }

    #[test]
    fn test_is_trading() {
        let mut state = GlobalState::default();
        assert!(!state.is_trading());

        state.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: 100.0,
        };
        assert!(state.is_trading());

        state.trading_mode = TradingMode::Idle;
        assert!(!state.is_trading());
    }

    #[test]
    fn test_current_pnl() {
        let mut state = GlobalState::default();
        assert_eq!(state.current_pnl(), None);

        state.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: 150.50,
        };
        assert_eq!(state.current_pnl(), Some(150.50));

        state.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: -50.25,
        };
        assert_eq!(state.current_pnl(), Some(-50.25));
    }

    #[test]
    fn test_update_pnl() {
        let mut state = GlobalState::default();
        state.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: 0.0,
        };
        state.update_pnl(200.0);
        assert_eq!(state.current_pnl(), Some(200.0));
    }

    #[test]
    fn test_update_pnl_when_idle_does_nothing() {
        let mut state = GlobalState::default();
        state.update_pnl(200.0);
        assert_eq!(state.current_pnl(), None);
    }

    #[test]
    fn test_start_paper_trading() {
        let mut state = GlobalState::default();
        state.start_paper_trading();
        assert!(matches!(state.trading_mode, TradingMode::Paper { .. }));
        assert_eq!(state.current_pnl(), Some(0.0));
    }

    #[test]
    fn test_start_live_trading_fails_without_validation() {
        let mut state = GlobalState::default();
        assert!(!state.start_live_trading());
        assert_eq!(state.trading_mode, TradingMode::Idle);
    }

    #[test]
    fn test_start_live_trading_succeeds_with_validation() {
        let mut state = GlobalState::default();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::passed(1.2);
        state.validation_status.oos = StageStatus::passed(0.8);
        state.validation_status.paper = StageStatus::passed(0.9);

        assert!(state.start_live_trading());
        assert!(matches!(state.trading_mode, TradingMode::Live { .. }));
    }

    #[test]
    fn test_stop_trading() {
        let mut state = GlobalState::default();
        state.start_paper_trading();
        state.stop_trading();
        assert_eq!(state.trading_mode, TradingMode::Idle);
    }

    #[test]
    fn test_set_stage_status() {
        let mut state = GlobalState::default();
        state.set_stage_status(ValidationStage::Backtest, StageStatus::passed(1.5));
        assert!(state.validation_status.backtest.is_passed());

        state.set_stage_status(ValidationStage::Forward, StageStatus::failed("Test"));
        assert!(state.validation_status.forward.is_failed());
    }

    #[test]
    fn test_set_active_algorithm_resets_validation_on_none() {
        let mut state = GlobalState::default();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.set_active_algorithm(None);
        assert!(state.validation_status.backtest.is_not_run());
    }

    // ========================================================================
    // StageStatus Tests
    // ========================================================================

    #[test]
    fn test_stage_status_default() {
        let status = StageStatus::default();
        assert!(status.is_not_run());
    }

    #[test]
    fn test_stage_status_passed() {
        let status = StageStatus::passed(1.5);
        assert!(status.is_passed());
        assert_eq!(status.sharpe(), Some(1.5));
    }

    #[test]
    fn test_stage_status_failed() {
        let status = StageStatus::failed("Low Sharpe ratio");
        assert!(status.is_failed());
        assert_eq!(status.failure_reason(), Some("Low Sharpe ratio"));
    }

    #[test]
    fn test_stage_status_running() {
        let status = StageStatus::running(0.5);
        assert!(status.is_running());
        assert_eq!(status.progress(), Some(0.5));
    }

    #[test]
    fn test_stage_status_running_clamps_progress() {
        let status = StageStatus::running(1.5);
        assert_eq!(status.progress(), Some(1.0));

        let status = StageStatus::running(-0.5);
        assert_eq!(status.progress(), Some(0.0));
    }

    #[test]
    fn test_stage_status_sharpe_returns_none_for_non_passed() {
        assert_eq!(StageStatus::NotRun.sharpe(), None);
        assert_eq!(StageStatus::failed("Test").sharpe(), None);
        assert_eq!(StageStatus::running(0.5).sharpe(), None);
    }

    #[test]
    fn test_stage_status_progress_returns_none_for_non_running() {
        assert_eq!(StageStatus::NotRun.progress(), None);
        assert_eq!(StageStatus::passed(1.0).progress(), None);
        assert_eq!(StageStatus::failed("Test").progress(), None);
    }

    #[test]
    fn test_stage_status_failure_reason_returns_none_for_non_failed() {
        assert_eq!(StageStatus::NotRun.failure_reason(), None);
        assert_eq!(StageStatus::passed(1.0).failure_reason(), None);
        assert_eq!(StageStatus::running(0.5).failure_reason(), None);
    }

    // ========================================================================
    // ValidationStatus Tests
    // ========================================================================

    #[test]
    fn test_validation_status_default() {
        let status = ValidationStatus::default();
        assert!(status.backtest.is_not_run());
        assert!(status.forward.is_not_run());
        assert!(status.oos.is_not_run());
        assert!(status.paper.is_not_run());
        assert!(status.live.is_not_run());
    }

    // ========================================================================
    // ValidationStage Tests
    // ========================================================================

    #[test]
    fn test_validation_stage_all() {
        let stages = ValidationStage::all();
        assert_eq!(stages.len(), 5);
        assert_eq!(stages[0], ValidationStage::Backtest);
        assert_eq!(stages[4], ValidationStage::Live);
    }

    #[test]
    fn test_validation_stage_display_names() {
        assert_eq!(ValidationStage::Backtest.display_name(), "Backtest");
        assert_eq!(ValidationStage::Forward.display_name(), "Walk-Forward");
        assert_eq!(ValidationStage::Oos.display_name(), "Out-of-Sample");
        assert_eq!(ValidationStage::Paper.display_name(), "Paper Trading");
        assert_eq!(ValidationStage::Live.display_name(), "Live Trading");
    }

    #[test]
    fn test_validation_stage_short_names() {
        assert_eq!(ValidationStage::Backtest.short_name(), "BT");
        assert_eq!(ValidationStage::Forward.short_name(), "WF");
        assert_eq!(ValidationStage::Oos.short_name(), "OOS");
        assert_eq!(ValidationStage::Paper.short_name(), "Paper");
        assert_eq!(ValidationStage::Live.short_name(), "Live");
    }

    // ========================================================================
    // TradingMode Tests
    // ========================================================================

    #[test]
    fn test_trading_mode_default() {
        let mode = TradingMode::default();
        assert_eq!(mode, TradingMode::Idle);
    }

    #[test]
    fn test_trading_mode_equality() {
        let mode1 = TradingMode::Idle;
        let mode2 = TradingMode::Idle;
        assert_eq!(mode1, mode2);
    }

    // ========================================================================
    // ResearchStatus Tests
    // ========================================================================

    #[test]
    fn test_research_status_default() {
        let status = ResearchStatus::default();
        assert_eq!(status, ResearchStatus::Idle);
    }

    #[test]
    fn test_research_status_running() {
        let status = ResearchStatus::Running { samples_processed: 1000 };
        if let ResearchStatus::Running { samples_processed } = status {
            assert_eq!(samples_processed, 1000);
        } else {
            panic!("Expected Running status");
        }
    }

    #[test]
    fn test_research_status_complete() {
        let status = ResearchStatus::Complete { tradeable: true };
        if let ResearchStatus::Complete { tradeable } = status {
            assert!(tradeable);
        } else {
            panic!("Expected Complete status");
        }
    }

    // ========================================================================
    // DataStats Tests
    // ========================================================================

    #[test]
    fn test_data_stats_default() {
        let stats = DataStats::default();
        assert_eq!(stats.file_count, 0);
        assert_eq!(stats.total_events, 0);
        assert!(stats.date_range.is_none());
        assert_eq!(stats.size_mb, 0.0);
    }

    #[test]
    fn test_data_stats_with_values() {
        let stats = DataStats {
            file_count: 97,
            total_events: 73000,
            date_range: Some((
                NaiveDate::from_ymd_opt(2025, 10, 16).unwrap(),
                NaiveDate::from_ymd_opt(2025, 12, 2).unwrap(),
            )),
            size_mb: 150.5,
        };
        assert_eq!(stats.file_count, 97);
        assert_eq!(stats.total_events, 73000);
        assert!(stats.date_range.is_some());
        assert_eq!(stats.size_mb, 150.5);
    }

    #[test]
    fn test_update_data_stats() {
        let mut state = GlobalState::default();
        let stats = DataStats {
            file_count: 50,
            total_events: 10000,
            date_range: None,
            size_mb: 75.0,
        };
        state.update_data_stats(stats.clone());
        assert_eq!(state.data_stats.file_count, 50);
        assert_eq!(state.data_stats.total_events, 10000);
    }

    // ========================================================================
    // AlgorithmConfigSummary Tests
    // ========================================================================

    #[test]
    fn test_algorithm_config_summary() {
        let summary = AlgorithmConfigSummary {
            id: "mom_v1_20251227".to_string(),
            name: "Momentum v1".to_string(),
            strategy_type: StrategyType::Momentum,
            created_at: Utc::now(),
        };
        assert_eq!(summary.id, "mom_v1_20251227");
        assert_eq!(summary.name, "Momentum v1");
        assert_eq!(summary.strategy_type, StrategyType::Momentum);
    }

    #[test]
    fn test_algorithm_config_summary_clone() {
        let summary = AlgorithmConfigSummary {
            id: "test".to_string(),
            name: "Test".to_string(),
            strategy_type: StrategyType::MarketMaking,
            created_at: Utc::now(),
        };
        let cloned = summary.clone();
        assert_eq!(summary, cloned);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_negative_pnl() {
        let mut state = GlobalState::default();
        state.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: -1000.0,
        };
        assert_eq!(state.current_pnl(), Some(-1000.0));
    }

    #[test]
    fn test_zero_sharpe() {
        let status = StageStatus::passed(0.0);
        assert_eq!(status.sharpe(), Some(0.0));
        assert!(status.is_passed());
    }

    #[test]
    fn test_negative_sharpe() {
        let status = StageStatus::passed(-0.5);
        assert_eq!(status.sharpe(), Some(-0.5));
        assert!(status.is_passed());
    }

    #[test]
    fn test_large_sample_count() {
        let status = ResearchStatus::Running {
            samples_processed: usize::MAX,
        };
        if let ResearchStatus::Running { samples_processed } = status {
            assert_eq!(samples_processed, usize::MAX);
        }
    }

    #[test]
    fn test_clone_global_state() {
        let state = GlobalState::new("BTCUSDT");
        let cloned = state.clone();
        assert_eq!(state.symbol, cloned.symbol);
    }

    #[test]
    fn test_debug_global_state() {
        let state = GlobalState::default();
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("GlobalState"));
        assert!(debug_str.contains("BTCUSDT"));
    }
}
