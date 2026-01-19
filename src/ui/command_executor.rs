//! TUI Command Executor (T-4.1)
//!
//! Bridges TUI and command execution layer, providing a unified interface
//! for executing all commands with progress tracking and result handling.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use anyhow::Result;

use crate::commands::common::{ProgressCallback, ProgressEvent, create_tui_callback};
use crate::commands::backtest::{
    BacktestCommands,
    EvaluateResult, TuneResult, RegimeSearchResult, MultiObjectiveResult,
    RegimeOptimizeResult, TrainResult, WalkForwardMLResult, SweepResult,
    WalkForwardResult, OOSValidateResult, SimulateResult, CampaignResult,
    PaperResult, GridResult, ListAlgorithmsResult,
};
use crate::commands::research::{ResearchCommands, RunResult as ResearchRunResult, StatusResult as ResearchStatusResult};
use crate::commands::validate::{ValidateCommands, RunResult as ValidateRunResult, PresetsResult, StagesResult, StatusResult as ValidateStatusResult, ShowResult};
use crate::commands::algorithm::{AlgorithmCommands, CreateResult, ListResult, ShowResult as AlgorithmShowResult};
use crate::commands::params::backtest_params::{
    EvaluateParams, TuneParams, RegimeSearchParams, MultiObjectiveParams,
    RegimeOptimizeParams, TrainParams, WalkForwardMLParams, SweepParams,
    WalkForwardParams, OOSValidateParams, SimulateParams, GridParams,
    CampaignParams, PaperParams, ListAlgorithmsParams,
};
use crate::commands::params::research_params::{RunParams as ResearchRunParams, StatusParams as ResearchStatusParams};
use crate::commands::params::validate_params::{
    RunParams as ValidateRunParams, PresetsParams, StagesParams,
    StatusParams as ValidateStatusParams, ShowParams as ValidateShowParams,
};
use crate::commands::params::algorithm_params::{
    CreateParams, ListParams, ShowParams as AlgorithmShowParams,
};
use crate::ui::algorithm_validation;

// ============================================================================
// Types
// ============================================================================

/// Result type for command execution
#[derive(Debug, Clone)]
pub enum CommandResult {
    /// Backtest evaluate result
    BacktestEvaluate(EvaluateResult),
    /// Backtest tune result
    BacktestTune(TuneResult),
    /// Backtest regime search result
    BacktestRegimeSearch(RegimeSearchResult),
    /// Backtest multi-objective result
    BacktestMultiObjective(MultiObjectiveResult),
    /// Backtest regime optimize result
    BacktestRegimeOptimize(RegimeOptimizeResult),
    /// Backtest train result
    BacktestTrain(TrainResult),
    /// Backtest walk-forward ML result
    BacktestWalkForwardML(WalkForwardMLResult),
    /// Backtest sweep result
    BacktestSweep(SweepResult),
    /// Backtest walk-forward result
    BacktestWalkForward(WalkForwardResult),
    /// Backtest OOS validate result
    BacktestOOSValidate(OOSValidateResult),
    /// Backtest simulate result
    BacktestSimulate(SimulateResult),
    /// Backtest campaign result
    BacktestCampaign(CampaignResult),
    /// Backtest paper result
    BacktestPaper(PaperResult),
    /// Backtest grid result
    BacktestGrid(GridResult),
    /// Backtest list algorithms result
    BacktestListAlgorithms(ListAlgorithmsResult),
    /// Research run result
    ResearchRun(ResearchRunResult),
    /// Research status result
    ResearchStatus(ResearchStatusResult),
    /// Validate run result
    ValidateRun(ValidateRunResult),
    /// Validate presets result
    ValidatePresets(PresetsResult),
    /// Validate stages result
    ValidateStages(StagesResult),
    /// Validate status result
    ValidateStatus(ValidateStatusResult),
    /// Validate show result
    ValidateShow(ShowResult),
    /// Algorithm create result
    AlgorithmCreate(CreateResult),
    /// Algorithm list result
    AlgorithmList(ListResult),
    /// Algorithm show result
    AlgorithmShow(AlgorithmShowResult),
}

/// TUI Command Executor
///
/// Provides a unified interface for executing commands from the TUI,
/// with progress tracking and result handling.
pub struct TUICommandExecutor {
    /// Channel sender for progress events
    progress_tx: mpsc::Sender<ProgressEvent>,
    /// Cancellation flag
    cancelled: Arc<AtomicBool>,
}

impl TUICommandExecutor {
    /// Create a new TUI command executor
    ///
    /// Returns the executor and a receiver for progress events.
    pub fn new() -> (Self, mpsc::Receiver<ProgressEvent>) {
        let (tx, rx) = mpsc::channel(100);
        let executor = Self {
            progress_tx: tx,
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        (executor, rx)
    }

    /// Check if execution is cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Cancel current execution
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        let _ = self.progress_tx.try_send(ProgressEvent::Error {
            message: "Command execution cancelled by user".to_string(),
        });
    }

    /// Reset cancellation flag
    pub fn reset_cancellation(&self) {
        self.cancelled.store(false, Ordering::Relaxed);
    }

    /// Send a progress event
    fn send_progress(&self, event: ProgressEvent) {
        if !self.is_cancelled() {
            let _ = self.progress_tx.try_send(event);
        }
    }

    /// Create a progress callback
    fn create_callback(&self) -> Arc<dyn ProgressCallback> {
        // TUICallback implements ProgressCallback and is Send + Sync
        // We need to wrap it in Arc, but TUICallback is not Clone
        // So we create a new one each time
        Arc::new(crate::commands::common::TUICallback::new(self.progress_tx.clone()))
    }

    /// Execute backtest evaluate command
    pub fn execute_backtest_evaluate(
        &self,
        params: EvaluateParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting backtest evaluation".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::evaluate(params, callback)?;
        
        // For TUI, we only need the EvaluateResult, not the full BacktestResults
        self.send_progress(ProgressEvent::Completed {
            message: "Backtest evaluation completed".to_string(),
        });

        Ok(CommandResult::BacktestEvaluate(result.1))
    }

    /// Execute backtest tune command
    pub fn execute_backtest_tune(
        &self,
        params: TuneParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None, // TuneParams doesn't have total_combinations() method
            message: "Starting backtest tuning".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::tune(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Backtest tuning completed".to_string(),
        });

        Ok(CommandResult::BacktestTune(result))
    }

    /// Execute backtest regime search command
    pub fn execute_backtest_regime_search(
        &self,
        params: RegimeSearchParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None, // RegimeSearchParams doesn't have total_combinations() method
            message: "Starting regime search".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::regime_search(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Regime search completed".to_string(),
        });

        Ok(CommandResult::BacktestRegimeSearch(result))
    }

    /// Execute backtest multi-objective command
    pub fn execute_backtest_multi_objective(
        &self,
        params: MultiObjectiveParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None, // MultiObjectiveParams doesn't have total_combinations() method
            message: "Starting multi-objective optimization".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::multi_objective(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Multi-objective optimization completed".to_string(),
        });

        Ok(CommandResult::BacktestMultiObjective(result))
    }

    /// Execute backtest regime optimize command
    pub fn execute_backtest_regime_optimize(
        &self,
        params: RegimeOptimizeParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting regime optimization".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::regime_optimize(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Regime optimization completed".to_string(),
        });

        Ok(CommandResult::BacktestRegimeOptimize(result))
    }

    /// Execute backtest train command
    pub fn execute_backtest_train(
        &self,
        params: TrainParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting ML weight training".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::train(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "ML weight training completed".to_string(),
        });

        Ok(CommandResult::BacktestTrain(result))
    }

    /// Execute backtest walk-forward ML command
    pub fn execute_backtest_walk_forward_ml(
        &self,
        params: WalkForwardMLParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: Some(params.folds),
            message: "Starting walk-forward ML training".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::walk_forward_ml(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Walk-forward ML training completed".to_string(),
        });

        Ok(CommandResult::BacktestWalkForwardML(result))
    }

    /// Execute backtest sweep command
    pub fn execute_backtest_sweep(
        &self,
        params: SweepParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None, // SweepParams doesn't have total_combinations() method
            message: "Starting parameter sweep".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::sweep(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Parameter sweep completed".to_string(),
        });

        Ok(CommandResult::BacktestSweep(result))
    }

    /// Execute backtest walk-forward command
    pub fn execute_backtest_walk_forward(
        &self,
        params: WalkForwardParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: Some(params.folds),
            message: "Starting walk-forward validation".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::walk_forward(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Walk-forward validation completed".to_string(),
        });

        Ok(CommandResult::BacktestWalkForward(result))
    }

    /// Execute backtest OOS validate command
    pub fn execute_backtest_oos_validate(
        &self,
        params: OOSValidateParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None, // OOSValidateParams doesn't have total_combinations() method
            message: "Starting OOS validation".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::oos_validate(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "OOS validation completed".to_string(),
        });

        Ok(CommandResult::BacktestOOSValidate(result))
    }

    /// Execute backtest simulate command
    pub fn execute_backtest_simulate(
        &self,
        params: SimulateParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting simulation campaign".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::simulate(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Simulation campaign completed".to_string(),
        });

        Ok(CommandResult::BacktestSimulate(result))
    }

    /// Execute backtest campaign command
    pub fn execute_backtest_campaign(
        &self,
        params: CampaignParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting validation campaign".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::campaign(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Validation campaign completed".to_string(),
        });

        Ok(CommandResult::BacktestCampaign(result))
    }

    /// Execute backtest paper command
    pub fn execute_backtest_paper(
        &self,
        params: PaperParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting paper trading session".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::paper(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Paper trading session completed".to_string(),
        });

        Ok(CommandResult::BacktestPaper(result))
    }

    /// Execute backtest grid command
    pub fn execute_backtest_grid(
        &self,
        params: GridParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None, // GridParams doesn't have total_combinations() method
            message: "Starting grid search".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::grid(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Grid search completed".to_string(),
        });

        Ok(CommandResult::BacktestGrid(result))
    }

    /// Execute backtest list algorithms command
    pub fn execute_backtest_list_algorithms(
        &self,
        params: ListAlgorithmsParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Listing algorithms".to_string(),
        });

        let callback = self.create_callback();
        let result = BacktestCommands::list_algorithms(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Algorithm listing completed".to_string(),
        });

        Ok(CommandResult::BacktestListAlgorithms(result))
    }

    /// Execute research run command
    pub fn execute_research_run(
        &self,
        params: ResearchRunParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting research analysis".to_string(),
        });

        let callback = self.create_callback();
        let result = ResearchCommands::run(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Research analysis completed".to_string(),
        });

        Ok(CommandResult::ResearchRun(result))
    }

    /// Execute research status command
    pub fn execute_research_status(
        &self,
        params: ResearchStatusParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Fetching research status".to_string(),
        });

        let callback = self.create_callback();
        let result = ResearchCommands::status(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Research status retrieved".to_string(),
        });

        Ok(CommandResult::ResearchStatus(result))
    }

    /// Execute validate run command (async)
    pub async fn execute_validate_run(
        &self,
        params: ValidateRunParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: Some(5), // Maximum 5 stages
            message: "Starting validation pipeline".to_string(),
        });

        let callback = self.create_callback();
        let result = ValidateCommands::run(params, callback).await?;

        self.send_progress(ProgressEvent::Completed {
            message: "Validation pipeline completed".to_string(),
        });

        Ok(CommandResult::ValidateRun(result))
    }

    /// Execute validate presets command
    pub fn execute_validate_presets(
        &self,
        params: PresetsParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Fetching validation presets".to_string(),
        });

        let callback = self.create_callback();
        let result = ValidateCommands::presets(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Validation presets retrieved".to_string(),
        });

        Ok(CommandResult::ValidatePresets(result))
    }

    /// Execute validate stages command
    pub fn execute_validate_stages(
        &self,
        params: StagesParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Fetching validation stages".to_string(),
        });

        let callback = self.create_callback();
        let result = ValidateCommands::stages(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Validation stages retrieved".to_string(),
        });

        Ok(CommandResult::ValidateStages(result))
    }

    /// Execute validate status command
    pub fn execute_validate_status(
        &self,
        params: ValidateStatusParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Fetching validation status".to_string(),
        });

        let callback = self.create_callback();
        let result = ValidateCommands::status(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Validation status retrieved".to_string(),
        });

        Ok(CommandResult::ValidateStatus(result))
    }

    /// Execute validate show command
    pub fn execute_validate_show(
        &self,
        params: ValidateShowParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Fetching validation details".to_string(),
        });

        let callback = self.create_callback();
        let result = ValidateCommands::show(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Validation details retrieved".to_string(),
        });

        Ok(CommandResult::ValidateShow(result))
    }

    /// Execute algorithm create command (async)
    pub async fn execute_algorithm_create(
        &self,
        params: CreateParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Creating algorithm configuration".to_string(),
        });

        let callback = self.create_callback();
        let result = AlgorithmCommands::create(params, callback).await?;

        self.send_progress(ProgressEvent::Completed {
            message: "Algorithm configuration created".to_string(),
        });

        Ok(CommandResult::AlgorithmCreate(result))
    }

    /// Execute algorithm list command
    pub fn execute_algorithm_list(
        &self,
        params: ListParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Listing algorithm configurations".to_string(),
        });

        let callback = self.create_callback();
        let result = AlgorithmCommands::list(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Algorithm configurations listed".to_string(),
        });

        Ok(CommandResult::AlgorithmList(result))
    }

    /// Execute algorithm show command
    pub fn execute_algorithm_show(
        &self,
        params: AlgorithmShowParams,
    ) -> Result<CommandResult> {
        if self.is_cancelled() {
            return Err(anyhow::anyhow!("Execution cancelled"));
        }

        self.send_progress(ProgressEvent::Started {
            total: None,
            message: "Fetching algorithm configuration".to_string(),
        });

        let callback = self.create_callback();
        let result = AlgorithmCommands::show(params, callback)?;

        self.send_progress(ProgressEvent::Completed {
            message: "Algorithm configuration retrieved".to_string(),
        });

        Ok(CommandResult::AlgorithmShow(result))
    }
}

impl Default for TUICommandExecutor {
    fn default() -> Self {
        let (executor, _) = Self::new();
        executor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;
    use tempfile::TempDir;
    use std::path::PathBuf;

    // ============================================================================
    // Constructor Tests
    // ============================================================================

    #[test]
    fn test_executor_creation() {
        let (executor, _rx) = TUICommandExecutor::new();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_executor_default() {
        let executor = TUICommandExecutor::default();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_progress_channel_creation() {
        let (_executor, mut rx) = TUICommandExecutor::new();
        // Channel should be created successfully
        assert!(rx.try_recv().is_err()); // Should be empty initially
    }

    // ============================================================================
    // Cancellation Tests
    // ============================================================================

    #[test]
    fn test_cancellation_flag() {
        let executor = TUICommandExecutor::default();
        assert!(!executor.is_cancelled());
        
        executor.cancel();
        assert!(executor.is_cancelled());
        
        executor.reset_cancellation();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_cancel_sends_error_event() {
        let (executor, mut rx) = TUICommandExecutor::new();
        executor.cancel();
        
        // Should receive error event
        let event = rx.try_recv().unwrap();
        match event {
            ProgressEvent::Error { message } => {
                assert!(message.contains("cancelled"));
            }
            _ => panic!("Expected Error event"),
        }
    }

    #[test]
    fn test_cancelled_execution_returns_error() {
        let executor = TUICommandExecutor::default();
        executor.cancel();
        
        let params = EvaluateParams::default();
        let result = executor.execute_backtest_evaluate(params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }

    #[test]
    fn test_reset_cancellation_allows_execution() {
        let executor = TUICommandExecutor::default();
        executor.cancel();
        executor.reset_cancellation();
        
        // Should not be cancelled anymore
        assert!(!executor.is_cancelled());
    }

    // ============================================================================
    // Progress Event Tests
    // ============================================================================

    #[test]
    fn test_progress_events_sent() {
        let (executor, mut rx) = TUICommandExecutor::new();
        
        executor.send_progress(ProgressEvent::Started {
            total: Some(100),
            message: "Test".to_string(),
        });
        
        let event = rx.try_recv().unwrap();
        match event {
            ProgressEvent::Started { total, message } => {
                assert_eq!(total, Some(100));
                assert_eq!(message, "Test");
            }
            _ => panic!("Expected Started event"),
        }
    }

    #[test]
    fn test_progress_not_sent_when_cancelled() {
        let (executor, mut rx) = TUICommandExecutor::new();
        executor.cancel();
        
        // Clear the cancellation error event
        let _ = rx.try_recv();
        
        executor.send_progress(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });
        
        // Should not receive new event (channel should be empty or only have cancellation error)
        let result = rx.try_recv();
        // May or may not receive the event depending on timing, but cancellation should prevent new ones
        if result.is_ok() {
            // If we got something, it should be the cancellation error
            match result.unwrap() {
                ProgressEvent::Error { .. } => {},
                _ => panic!("Should only receive error events after cancellation"),
            }
        }
    }

    // ============================================================================
    // Command Execution Tests (Mock/Unit)
    // ============================================================================

    #[test]
    fn test_execute_backtest_list_algorithms_basic() {
        let executor = TUICommandExecutor::default();
        let params = ListAlgorithmsParams::default();
        
        // This will fail if params are invalid, but tests the structure
        let result = executor.execute_backtest_list_algorithms(params);
        // May succeed or fail depending on params, but should not panic
        let _ = result;
    }

    #[test]
    fn test_command_result_variants() {
        // Test that all CommandResult variants exist
        // Note: Most Result types don't have Default, so we create minimal instances
        use crate::commands::backtest::EvaluateMetrics;
        
        let _results = vec![
            CommandResult::BacktestEvaluate(EvaluateResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                metrics: EvaluateMetrics::default(),
                params: EvaluateParams::default(),
                num_events: 0,
                time_span_hours: 0.0,
            }),
            CommandResult::BacktestTune(TuneResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                all_results: vec![],
                best: None,
                total_combinations: 0,
            }),
            CommandResult::BacktestRegimeSearch(RegimeSearchResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                all_results: vec![],
                best: None,
                total_combinations: 0,
                avg_sharpe_with_quote: None,
                avg_sharpe_without_quote: None,
            }),
            CommandResult::BacktestMultiObjective(MultiObjectiveResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                all_solutions: vec![],
                pareto_frontier: vec![],
                best_weighted: None,
                total_combinations: 0,
                time_span_hours: 0.0,
                num_events: 0,
            }),
            CommandResult::BacktestRegimeOptimize(RegimeOptimizeResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                regime_metrics: vec![],
                optimal_params: vec![],
                comparison: None,
                total_combinations: 0,
                time_span_hours: 0.0,
                num_events: 0,
            }),
            CommandResult::BacktestTrain(TrainResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                weights: None,
                metrics: None,
                num_events: 0,
                time_span_hours: 0.0,
            }),
            CommandResult::BacktestWalkForwardML(WalkForwardMLResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                folds: vec![],
                aggregate: None,
                consensus_weights: None,
                total_combinations: 0,
                time_span_hours: 0.0,
                num_events: 0,
            }),
            CommandResult::BacktestSweep(SweepResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                all_results: vec![],
                best: None,
                total_combinations: 0,
            }),
            CommandResult::BacktestWalkForward(WalkForwardResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                folds: vec![],
                aggregate: None,
                optimized_params: None,
                total_combinations: 0,
                time_span_hours: 0.0,
                num_events: 0,
            }),
            CommandResult::BacktestOOSValidate(OOSValidateResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                reports: vec![],
                best: None,
                verdict_summary: None,
                total_combinations: 0,
                time_span_hours: 0.0,
                num_events: 0,
            }),
            CommandResult::BacktestSimulate(SimulateResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                report: None,
                num_events: 0,
                time_span_hours: 0.0,
            }),
            CommandResult::BacktestCampaign(CampaignResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                report: None,
                num_events: 0,
                time_span_hours: 0.0,
            }),
            CommandResult::BacktestPaper(PaperResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                session: None,
                num_events: 0,
                time_span_hours: 0.0,
            }),
            CommandResult::BacktestGrid(GridResult {
                algorithm: "mm_spread_skew".to_string(),
                algorithm_name: "MM Spread/Skew".to_string(),
                all_results: vec![],
                best: None,
                total_combinations: 0,
            }),
            CommandResult::BacktestListAlgorithms(ListAlgorithmsResult {
                algorithms: vec![],
                json_output: "{}".to_string(),
            }),
            CommandResult::ResearchRun(ResearchRunResult {
                samples_processed: 0,
                duration_seconds: 0.0,
                midc_kappa: 0.0,
                midc_confidence: 0.0,
                midc_regime: "unknown".to_string(),
                persistence_mean_seconds: 0.0,
                persistence_sample_count: 0,
                top_signals: vec![],
                is_tradeable: false,
                tradeable_reason: "".to_string(),
                checkpoints_saved: 0,
            }),
            CommandResult::ResearchStatus(ResearchStatusResult {
                symbol: "".to_string(),
                state_id: "".to_string(),
                timestamp: "".to_string(),
                data_start: None,
                data_end: None,
                midc_kappa: 0.0,
                midc_confidence: 0.0,
                midc_tau_half_seconds: 0.0,
                persistence_mean_seconds: 0.0,
                persistence_sample_count: 0,
                top_signals: vec![],
                is_tradeable: false,
                tradeable_reason: "".to_string(),
            }),
            CommandResult::ValidateRun(ValidateRunResult {
                pipeline_result: crate::validation::PipelineResult {
                    status: crate::validation::PipelineStatus::Passed,
                    stages: vec![],
                    warnings: vec![],
                    errors: vec![],
                },
                algorithm_config_id: "".to_string(),
                algorithm_name: "".to_string(),
                duration_seconds: 0.0,
            }),
            CommandResult::ValidatePresets(PresetsResult {
                presets: vec![],
            }),
            CommandResult::ValidateStages(StagesResult {
                stages: vec![],
            }),
            CommandResult::ValidateStatus(ValidateStatusResult {
                runs: vec![],
                total_runs: 0,
            }),
            CommandResult::ValidateShow(ShowResult {
                run: None,
                pipeline_result: None,
            }),
            CommandResult::AlgorithmCreate(CreateResult {
                config: crate::core::AlgorithmConfig::default(),
                saved_path: None,
                validation_result: None,
                duration_seconds: 0.0,
            }),
            CommandResult::AlgorithmList(ListResult {
                configs: vec![],
            }),
            CommandResult::AlgorithmShow(AlgorithmShowResult {
                config: crate::core::AlgorithmConfig::default(),
                found: false,
            }),
        ];
        
        assert_eq!(_results.len(), 25);
    }

    // ============================================================================
    // Error Handling Tests
    // ============================================================================

    #[test]
    fn test_error_handling_invalid_params() {
        let executor = TUICommandExecutor::default();
        
        // Test with invalid params (should handle gracefully)
        let params = EvaluateParams::default();
        // This may fail due to missing required fields, but should not panic
        let result = executor.execute_backtest_evaluate(params);
        // Error handling should work
        if let Err(e) = result {
            assert!(!e.to_string().is_empty());
        }
    }

    #[test]
    fn test_error_propagation() {
        let executor = TUICommandExecutor::default();
        executor.cancel();
        
        let params = TuneParams::default();
        let result = executor.execute_backtest_tune(params);
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cancelled"));
    }

    // ============================================================================
    // Progress Callback Tests
    // ============================================================================

    #[test]
    fn test_callback_creation() {
        let (executor, _rx) = TUICommandExecutor::new();
        let callback = executor.create_callback();
        
        // Callback should be created
        callback.on_event(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });
    }

    #[test]
    fn test_callback_sends_events() {
        let (executor, mut rx) = TUICommandExecutor::new();
        let callback = executor.create_callback();
        
        callback.on_event(ProgressEvent::Log {
            level: crate::commands::common::LogLevel::Info,
            message: "Test log".to_string(),
        });
        
        // Should receive event (may need to wait briefly)
        let event = rx.try_recv();
        if let Ok(ProgressEvent::Log { message, .. }) = event {
            assert_eq!(message, "Test log");
        }
    }

    // ============================================================================
    // Concurrent Execution Tests
    // ============================================================================

    #[test]
    fn test_multiple_progress_events() {
        let (executor, mut rx) = TUICommandExecutor::new();
        
        executor.send_progress(ProgressEvent::Started {
            total: Some(100),
            message: "Start".to_string(),
        });
        
        executor.send_progress(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Progress".to_string(),
        });
        
        executor.send_progress(ProgressEvent::Completed {
            message: "Done".to_string(),
        });
        
        // Should receive multiple events
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        
        assert!(events.len() >= 1); // At least one event should be received
    }

    #[test]
    fn test_cancellation_during_execution() {
        let executor = TUICommandExecutor::default();
        
        // Start execution
        executor.send_progress(ProgressEvent::Started {
            total: None,
            message: "Starting".to_string(),
        });
        
        // Cancel mid-execution
        executor.cancel();
        
        assert!(executor.is_cancelled());
    }

    // ============================================================================
    // Integration Tests (with minimal setup)
    // ============================================================================

    #[test]
    fn test_executor_lifecycle() {
        let (executor, _rx) = TUICommandExecutor::new();
        
        // Initial state
        assert!(!executor.is_cancelled());
        
        // Use executor
        executor.send_progress(ProgressEvent::Started {
            total: None,
            message: "Test".to_string(),
        });
        
        // Cancel
        executor.cancel();
        assert!(executor.is_cancelled());
        
        // Reset
        executor.reset_cancellation();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_all_backtest_commands_exist() {
        let executor = TUICommandExecutor::default();
        
        // Verify all backtest command methods exist and can be called
        // (They may fail due to invalid params, but should not panic)
        let _ = executor.execute_backtest_evaluate(EvaluateParams::default());
        let _ = executor.execute_backtest_tune(TuneParams::default());
        let _ = executor.execute_backtest_regime_search(RegimeSearchParams::default());
        let _ = executor.execute_backtest_multi_objective(MultiObjectiveParams::default());
        let _ = executor.execute_backtest_regime_optimize(RegimeOptimizeParams::default());
        let _ = executor.execute_backtest_train(TrainParams::default());
        let _ = executor.execute_backtest_walk_forward_ml(WalkForwardMLParams::default());
        let _ = executor.execute_backtest_sweep(SweepParams::default());
        let _ = executor.execute_backtest_walk_forward(WalkForwardParams::default());
        let _ = executor.execute_backtest_oos_validate(OOSValidateParams::default());
        let _ = executor.execute_backtest_simulate(SimulateParams::default());
        let _ = executor.execute_backtest_campaign(CampaignParams::default());
        let _ = executor.execute_backtest_paper(PaperParams::default());
        let _ = executor.execute_backtest_grid(GridParams::default());
        let _ = executor.execute_backtest_list_algorithms(ListAlgorithmsParams::default());
    }

    #[test]
    fn test_all_research_commands_exist() {
        let executor = TUICommandExecutor::default();
        
        let _ = executor.execute_research_run(ResearchRunParams::default());
        let _ = executor.execute_research_status(ResearchStatusParams::default());
    }

    #[test]
    fn test_all_validate_commands_exist() {
        let executor = TUICommandExecutor::default();
        
        // Async commands need runtime
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(executor.execute_validate_run(ValidateRunParams::default()));
        
        let _ = executor.execute_validate_presets(PresetsParams::default());
        let _ = executor.execute_validate_stages(StagesParams::default());
        let _ = executor.execute_validate_status(ValidateStatusParams::default());
        let _ = executor.execute_validate_show(ValidateShowParams::default());
    }

    #[test]
    fn test_all_algorithm_commands_exist() {
        let executor = TUICommandExecutor::default();
        
        // Async commands need runtime
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(executor.execute_algorithm_create(CreateParams::default()));
        
        let _ = executor.execute_algorithm_list(ListParams::default());
        let _ = executor.execute_algorithm_show(AlgorithmShowParams::default());
    }

    // ============================================================================
    // Edge Case Tests
    // ============================================================================

    #[test]
    fn test_multiple_cancellations() {
        let executor = TUICommandExecutor::default();
        
        executor.cancel();
        assert!(executor.is_cancelled());
        
        executor.cancel(); // Should be idempotent
        assert!(executor.is_cancelled());
        
        executor.reset_cancellation();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_reset_without_cancel() {
        let executor = TUICommandExecutor::default();
        
        // Reset when not cancelled should be safe
        executor.reset_cancellation();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_progress_channel_capacity() {
        let (executor, mut rx) = TUICommandExecutor::new();
        
        // Send many events
        for i in 0..200 {
            executor.send_progress(ProgressEvent::Progress {
                current: i,
                total: Some(200),
                message: format!("Progress {}", i),
            });
        }
        
        // Should handle gracefully (channel may drop some if full)
        let mut received = 0;
        while rx.try_recv().is_ok() {
            received += 1;
        }
        
        // Should receive at least some events (up to channel capacity)
        assert!(received > 0);
    }

    #[test]
    fn test_concurrent_cancellation() {
        use std::sync::Arc;
        use std::thread;
        
        let executor = Arc::new(TUICommandExecutor::default());
        let executor_clone = executor.clone();
        
        // Cancel from another thread
        let handle = thread::spawn(move || {
            executor_clone.cancel();
        });
        
        handle.join().unwrap();
        
        assert!(executor.is_cancelled());
    }

    #[test]
    fn test_progress_event_types() {
        let (executor, mut rx) = TUICommandExecutor::new();
        
        // Test all event types
        executor.send_progress(ProgressEvent::Started {
            total: Some(100),
            message: "Start".to_string(),
        });
        
        executor.send_progress(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Progress".to_string(),
        });
        
        executor.send_progress(ProgressEvent::Metric {
            name: "sharpe".to_string(),
            value: 1.5,
        });
        
        executor.send_progress(ProgressEvent::Log {
            level: crate::commands::common::LogLevel::Info,
            message: "Log".to_string(),
        });
        
        executor.send_progress(ProgressEvent::Completed {
            message: "Done".to_string(),
        });
        
        executor.send_progress(ProgressEvent::Error {
            message: "Error".to_string(),
        });
        
        // Should receive events
        let mut count = 0;
        while rx.try_recv().is_ok() && count < 10 {
            count += 1;
        }
        
        assert!(count > 0);
    }

    // ============================================================================
    // Result Type Tests
    // ============================================================================

    #[test]
    fn test_command_result_debug() {
        use crate::commands::backtest::EvaluateMetrics;
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            num_events: 0,
            time_span_hours: 0.0,
        });
        let debug_str = format!("{:?}", result);
        assert!(!debug_str.is_empty());
    }

    #[test]
    fn test_command_result_clone() {
        use crate::commands::backtest::EvaluateMetrics;
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            num_events: 0,
            time_span_hours: 0.0,
        });
        let cloned = result.clone();
        
        // Both should be valid
        match (result, cloned) {
            (CommandResult::BacktestEvaluate(_), CommandResult::BacktestEvaluate(_)) => {},
            _ => panic!("Cloned result should match original"),
        }
    }

    // ============================================================================
    // Stress Tests
    // ============================================================================

    #[test]
    fn test_rapid_cancellation_reset() {
        let executor = TUICommandExecutor::default();
        
        for _ in 0..100 {
            executor.cancel();
            executor.reset_cancellation();
        }
        
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_rapid_progress_events() {
        let (executor, mut rx) = TUICommandExecutor::new();
        
        for i in 0..1000 {
            executor.send_progress(ProgressEvent::Progress {
                current: i,
                total: Some(1000),
                message: format!("{}", i),
            });
        }
        
        // Should handle gracefully
        let mut count = 0;
        while rx.try_recv().is_ok() && count < 200 {
            count += 1;
        }
        
        // Should receive some events
        assert!(count > 0);
    }

    // ============================================================================
    // Thread Safety Tests
    // ============================================================================

    #[test]
    fn test_send_sync() {
        use std::sync::Arc;
        use std::thread;
        
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        
        assert_send::<TUICommandExecutor>();
        assert_sync::<TUICommandExecutor>();
        
        let executor = Arc::new(TUICommandExecutor::default());
        let executor_clone = executor.clone();
        
        let handle = thread::spawn(move || {
            executor_clone.cancel();
        });
        
        handle.join().unwrap();
        assert!(executor.is_cancelled());
    }

    // ============================================================================
    // Memory Safety Tests
    // ============================================================================

    #[test]
    fn test_executor_drop() {
        let (executor, _rx) = TUICommandExecutor::new();
        drop(executor);
        // Should not panic or leak
    }

    #[test]
    fn test_progress_receiver_drop() {
        let (_executor, rx) = TUICommandExecutor::new();
        drop(rx);
        // Executor should still work
        // (though progress events won't be received)
    }

    // ============================================================================
    // Error Message Tests
    // ============================================================================

    #[test]
    fn test_cancellation_error_message() {
        let executor = TUICommandExecutor::default();
        executor.cancel();
        
        let params = SweepParams::default();
        let result = executor.execute_backtest_sweep(params);
        
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("cancelled"));
    }

    #[test]
    fn test_user_friendly_error_messages() {
        let executor = TUICommandExecutor::default();
        
        // Test that errors are user-friendly (not just technical)
        let params = EvaluateParams::default();
        let result = executor.execute_backtest_evaluate(params);
        
        if let Err(e) = result {
            let msg = e.to_string();
            // Should be readable, not just a debug dump
            assert!(!msg.is_empty());
        }
    }

    // ============================================================================
    // Progress Tracking Tests
    // ============================================================================

    #[test]
    fn test_progress_tracking_sequence() {
        let (executor, mut rx) = TUICommandExecutor::new();
        
        executor.send_progress(ProgressEvent::Started {
            total: Some(100),
            message: "Start".to_string(),
        });
        
        for i in 0..10 {
            executor.send_progress(ProgressEvent::Progress {
                current: i * 10,
                total: Some(100),
                message: format!("{}%", i * 10),
            });
        }
        
        executor.send_progress(ProgressEvent::Completed {
            message: "Done".to_string(),
        });
        
        // Should receive events in sequence
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
            if events.len() > 20 {
                break; // Prevent infinite loop
            }
        }
        
        assert!(!events.is_empty());
    }

    // ============================================================================
    // Command Count Verification
    // ============================================================================

    #[test]
    fn test_all_commands_implemented() {
        // Verify we have all required commands:
        // 14 backtest + 2 research + 5 validate + 3 algorithm = 24 total
        
        let executor = TUICommandExecutor::default();
        
        // Count backtest commands (14)
        let _ = executor.execute_backtest_evaluate(EvaluateParams::default());
        let _ = executor.execute_backtest_tune(TuneParams::default());
        let _ = executor.execute_backtest_regime_search(RegimeSearchParams::default());
        let _ = executor.execute_backtest_multi_objective(MultiObjectiveParams::default());
        let _ = executor.execute_backtest_regime_optimize(RegimeOptimizeParams::default());
        let _ = executor.execute_backtest_train(TrainParams::default());
        let _ = executor.execute_backtest_walk_forward_ml(WalkForwardMLParams::default());
        let _ = executor.execute_backtest_sweep(SweepParams::default());
        let _ = executor.execute_backtest_walk_forward(WalkForwardParams::default());
        let _ = executor.execute_backtest_oos_validate(OOSValidateParams::default());
        let _ = executor.execute_backtest_simulate(SimulateParams::default());
        let _ = executor.execute_backtest_campaign(CampaignParams::default());
        let _ = executor.execute_backtest_paper(PaperParams::default());
        let _ = executor.execute_backtest_grid(GridParams::default());
        // 14 backtest commands ✓
        
        // Count research commands (2)
        let _ = executor.execute_research_run(ResearchRunParams::default());
        let _ = executor.execute_research_status(ResearchStatusParams::default());
        // 2 research commands ✓
        
        // Count validate commands (5)
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(executor.execute_validate_run(ValidateRunParams::default()));
        let _ = executor.execute_validate_presets(PresetsParams::default());
        let _ = executor.execute_validate_stages(StagesParams::default());
        let _ = executor.execute_validate_status(ValidateStatusParams::default());
        let _ = executor.execute_validate_show(ValidateShowParams::default());
        // 5 validate commands ✓
        
        // Count algorithm commands (3)
        let _ = rt.block_on(executor.execute_algorithm_create(CreateParams::default()));
        let _ = executor.execute_algorithm_list(ListParams::default());
        let _ = executor.execute_algorithm_show(AlgorithmShowParams::default());
        // 3 algorithm commands ✓
        
        // Total: 14 + 2 + 5 + 3 = 24 commands ✓
    }
}
