//! Command Workflow Manager (T-4.4)
//!
//! Manages the workflow for executing commands:
//! Menu → Config Screen → Command Executor → Results Screen

use std::sync::Arc;
use tokio::sync::mpsc;
use anyhow::Result;

use crate::ui::command_executor::{TUICommandExecutor, CommandResult};
use crate::commands::common::ProgressEvent;
use crate::ui::submenu::NavigationTarget;
use crate::ui::state::GlobalState;

// ============================================================================
// Command Workflow State
// ============================================================================

/// Current state in the command workflow
#[derive(Debug, Clone)]
pub enum WorkflowState {
    /// In menu (no active workflow)
    Menu,
    /// Showing config screen for a command
    ConfigScreen {
        /// Which config screen is active
        target: NavigationTarget,
    },
    /// Executing command (showing progress)
    Executing {
        /// Which command is executing
        command_name: String,
    },
    /// Showing results screen
    ResultsScreen {
        /// Which results screen is active
        target: NavigationTarget,
        /// The command result (boxed to avoid PartialEq requirement)
        result: Box<CommandResult>,
    },
}

impl PartialEq for WorkflowState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Menu, Self::Menu) => true,
            (Self::ConfigScreen { target: t1 }, Self::ConfigScreen { target: t2 }) => t1 == t2,
            (Self::Executing { command_name: n1 }, Self::Executing { command_name: n2 }) => n1 == n2,
            (Self::ResultsScreen { target: t1, .. }, Self::ResultsScreen { target: t2, .. }) => t1 == t2,
            _ => false,
        }
    }
}

impl Default for WorkflowState {
    fn default() -> Self {
        Self::Menu
    }
}

// ============================================================================
// Command Workflow Manager
// ============================================================================

/// Manages command execution workflow
pub struct CommandWorkflowManager {
    /// Current workflow state
    state: WorkflowState,
    /// Command executor
    executor: Arc<TUICommandExecutor>,
}

impl CommandWorkflowManager {
    /// Create a new command workflow manager
    pub fn new() -> (Self, mpsc::Receiver<ProgressEvent>) {
        let (executor, progress_rx) = TUICommandExecutor::new();
        let manager = Self {
            state: WorkflowState::Menu,
            executor: Arc::new(executor),
        };
        (manager, progress_rx)
    }

    /// Get current workflow state
    pub fn state(&self) -> &WorkflowState {
        &self.state
    }

    /// Navigate to a config screen
    pub fn navigate_to_config(&mut self, target: NavigationTarget) {
        self.state = WorkflowState::ConfigScreen { target };
    }

    /// Start executing a command
    pub fn start_execution(&mut self, command_name: String) {
        self.state = WorkflowState::Executing {
            command_name,
        };
        self.executor.reset_cancellation();
    }

    /// Complete command execution and show results
    pub fn show_results(&mut self, target: NavigationTarget, result: CommandResult) {
        self.state = WorkflowState::ResultsScreen {
            target,
            result: Box::new(result),
        };
    }

    /// Cancel current execution
    pub fn cancel_execution(&self) {
        self.executor.cancel();
    }

    /// Go back to menu
    pub fn back_to_menu(&mut self) {
        self.state = WorkflowState::Menu;
    }

    /// Get the command executor
    pub fn executor(&self) -> &Arc<TUICommandExecutor> {
        &self.executor
    }

}

impl Default for CommandWorkflowManager {
    fn default() -> Self {
        let executor = TUICommandExecutor::default();
        Self {
            state: WorkflowState::Menu,
            executor: Arc::new(executor),
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Map a config screen NavigationTarget to its corresponding results screen
pub fn config_to_results_target(config_target: &NavigationTarget) -> Option<NavigationTarget> {
    match config_target {
        NavigationTarget::BacktestEvaluateConfig => Some(NavigationTarget::BacktestEvaluateResults),
        NavigationTarget::BacktestTuneConfig => Some(NavigationTarget::BacktestTuneResults),
        NavigationTarget::BacktestRegimeSearchConfig => Some(NavigationTarget::BacktestRegimeSearchResults),
        NavigationTarget::BacktestMultiObjectiveConfig => Some(NavigationTarget::BacktestMultiObjectiveResults),
        NavigationTarget::BacktestRegimeOptimizeConfig => Some(NavigationTarget::BacktestRegimeOptimizeResults),
        NavigationTarget::BacktestTrainConfig => Some(NavigationTarget::BacktestTrainResults),
        NavigationTarget::BacktestWalkForwardMLConfig => Some(NavigationTarget::BacktestWalkForwardMLResults),
        NavigationTarget::BacktestSweepConfig => Some(NavigationTarget::BacktestSweepResults),
        NavigationTarget::BacktestWalkForwardConfig => Some(NavigationTarget::BacktestWalkForwardResults),
        NavigationTarget::BacktestOOSValidateConfig => Some(NavigationTarget::BacktestOOSValidateResults),
        NavigationTarget::BacktestSimulateConfig => Some(NavigationTarget::BacktestSimulateResults),
        NavigationTarget::BacktestGridConfig => Some(NavigationTarget::BacktestGridResults),
        NavigationTarget::BacktestCampaignConfig => Some(NavigationTarget::BacktestCampaignResults),
        NavigationTarget::BacktestPaperConfig => Some(NavigationTarget::BacktestPaperResults),
        NavigationTarget::ResearchRunConfig => Some(NavigationTarget::ResearchRunResults),
        NavigationTarget::ValidateRunConfig => Some(NavigationTarget::ValidateRunResults),
        NavigationTarget::AlgorithmCreate => Some(NavigationTarget::AlgorithmCreateResults),
        _ => None,
    }
}

/// Get command name from config screen target
pub fn get_command_name_from_config(target: &NavigationTarget) -> Option<&'static str> {
    match target {
        NavigationTarget::BacktestEvaluateConfig => Some("backtest evaluate"),
        NavigationTarget::BacktestTuneConfig => Some("backtest tune"),
        NavigationTarget::BacktestRegimeSearchConfig => Some("backtest regime-search"),
        NavigationTarget::BacktestMultiObjectiveConfig => Some("backtest multi-objective"),
        NavigationTarget::BacktestRegimeOptimizeConfig => Some("backtest regime-optimize"),
        NavigationTarget::BacktestTrainConfig => Some("backtest train"),
        NavigationTarget::BacktestWalkForwardMLConfig => Some("backtest walk-forward-ml"),
        NavigationTarget::BacktestSweepConfig => Some("backtest sweep"),
        NavigationTarget::BacktestWalkForwardConfig => Some("backtest walk-forward"),
        NavigationTarget::BacktestOOSValidateConfig => Some("backtest oos-validate"),
        NavigationTarget::BacktestSimulateConfig => Some("backtest simulate"),
        NavigationTarget::BacktestGridConfig => Some("backtest grid"),
        NavigationTarget::BacktestCampaignConfig => Some("backtest campaign"),
        NavigationTarget::BacktestPaperConfig => Some("backtest paper"),
        NavigationTarget::ResearchRunConfig => Some("research run"),
        NavigationTarget::ResearchStatusConfig => Some("research status"),
        NavigationTarget::ValidateRunConfig => Some("validate run"),
        NavigationTarget::ValidateShowConfig => Some("validate show"),
        NavigationTarget::ValidateStatusConfig => Some("validate status"),
        NavigationTarget::AlgorithmListConfig => Some("algorithm list"),
        NavigationTarget::AlgorithmShowConfig => Some("algorithm show"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // WorkflowState tests
    // ============================================================================

    #[test]
    fn test_workflow_state_default() {
        let state = WorkflowState::default();
        assert_eq!(state, WorkflowState::Menu);
    }

    #[test]
    fn test_workflow_state_config_screen() {
        let state = WorkflowState::ConfigScreen {
            target: NavigationTarget::BacktestEvaluateConfig,
        };
        match state {
            WorkflowState::ConfigScreen { target } => {
                assert_eq!(target, NavigationTarget::BacktestEvaluateConfig);
            }
            _ => panic!("Expected ConfigScreen state"),
        }
    }

    #[test]
    fn test_workflow_state_executing() {
        let state = WorkflowState::Executing {
            command_name: "test".to_string(),
        };
        match state {
            WorkflowState::Executing { command_name } => {
                assert_eq!(command_name, "test");
            }
            _ => panic!("Expected Executing state"),
        }
    }

    // ============================================================================
    // CommandWorkflowManager tests
    // ============================================================================

    #[test]
    fn test_workflow_manager_new() {
        let (manager, _rx) = CommandWorkflowManager::new();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_workflow_manager_default() {
        let manager = CommandWorkflowManager::default();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_navigate_to_config() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
        
        match manager.state() {
            WorkflowState::ConfigScreen { target } => {
                assert_eq!(*target, NavigationTarget::BacktestEvaluateConfig);
            }
            _ => panic!("Expected ConfigScreen state"),
        }
    }

    #[test]
    fn test_start_execution() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        manager.start_execution("test command".to_string());
        
        match manager.state() {
            WorkflowState::Executing { command_name } => {
                assert_eq!(command_name, "test command");
            }
            _ => panic!("Expected Executing state"),
        }
    }

    #[test]
    fn test_show_results() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        use crate::ui::command_executor::CommandResult;
        use crate::commands::backtest::EvaluateResult;
        use crate::commands::backtest::EvaluateMetrics;
        use crate::commands::params::backtest_params::EvaluateParams;
        
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            num_events: 0,
            time_span_hours: 0.0,
        });
        
        manager.show_results(NavigationTarget::BacktestEvaluateResults, result);
        
        match manager.state() {
            WorkflowState::ResultsScreen { target, result: _ } => {
                assert_eq!(*target, NavigationTarget::BacktestEvaluateResults);
            }
            _ => panic!("Expected ResultsScreen state"),
        }
    }

    #[test]
    fn test_back_to_menu() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
        manager.back_to_menu();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_cancel_execution() {
        let (manager, _rx) = CommandWorkflowManager::new();
        manager.cancel_execution();
        assert!(manager.executor().is_cancelled());
    }

    // ============================================================================
    // Helper function tests
    // ============================================================================

    #[test]
    fn test_config_to_results_target() {
        assert_eq!(
            config_to_results_target(&NavigationTarget::BacktestEvaluateConfig),
            Some(NavigationTarget::BacktestEvaluateResults)
        );
        assert_eq!(
            config_to_results_target(&NavigationTarget::BacktestTuneConfig),
            Some(NavigationTarget::BacktestTuneResults)
        );
        assert_eq!(
            config_to_results_target(&NavigationTarget::MainMenu),
            None
        );
    }

    #[test]
    fn test_get_command_name_from_config() {
        assert_eq!(
            get_command_name_from_config(&NavigationTarget::BacktestEvaluateConfig),
            Some("backtest evaluate")
        );
        assert_eq!(
            get_command_name_from_config(&NavigationTarget::BacktestTuneConfig),
            Some("backtest tune")
        );
        assert_eq!(
            get_command_name_from_config(&NavigationTarget::MainMenu),
            None
        );
    }

    #[test]
    fn test_all_config_targets_have_results() {
        let config_targets = vec![
            NavigationTarget::BacktestEvaluateConfig,
            NavigationTarget::BacktestTuneConfig,
            NavigationTarget::BacktestRegimeSearchConfig,
            NavigationTarget::BacktestMultiObjectiveConfig,
            NavigationTarget::BacktestRegimeOptimizeConfig,
            NavigationTarget::BacktestTrainConfig,
            NavigationTarget::BacktestWalkForwardMLConfig,
            NavigationTarget::BacktestSweepConfig,
            NavigationTarget::BacktestWalkForwardConfig,
            NavigationTarget::BacktestOOSValidateConfig,
            NavigationTarget::BacktestSimulateConfig,
            NavigationTarget::BacktestGridConfig,
            NavigationTarget::BacktestCampaignConfig,
            NavigationTarget::BacktestPaperConfig,
            NavigationTarget::ResearchRunConfig,
            NavigationTarget::ValidateRunConfig,
            NavigationTarget::AlgorithmCreate,
        ];

        for target in config_targets {
            let result_target = config_to_results_target(&target);
            assert!(
                result_target.is_some(),
                "Config target {:?} should have a results target",
                target
            );
        }
    }

    #[test]
    fn test_all_config_targets_have_command_names() {
        let config_targets = vec![
            NavigationTarget::BacktestEvaluateConfig,
            NavigationTarget::BacktestTuneConfig,
            NavigationTarget::BacktestRegimeSearchConfig,
            NavigationTarget::BacktestMultiObjectiveConfig,
            NavigationTarget::BacktestRegimeOptimizeConfig,
            NavigationTarget::BacktestTrainConfig,
            NavigationTarget::BacktestWalkForwardMLConfig,
            NavigationTarget::BacktestSweepConfig,
            NavigationTarget::BacktestWalkForwardConfig,
            NavigationTarget::BacktestOOSValidateConfig,
            NavigationTarget::BacktestSimulateConfig,
            NavigationTarget::BacktestGridConfig,
            NavigationTarget::BacktestCampaignConfig,
            NavigationTarget::BacktestPaperConfig,
            NavigationTarget::ResearchRunConfig,
            NavigationTarget::ResearchStatusConfig,
            NavigationTarget::ValidateRunConfig,
            NavigationTarget::ValidateShowConfig,
            NavigationTarget::ValidateStatusConfig,
            NavigationTarget::AlgorithmListConfig,
            NavigationTarget::AlgorithmShowConfig,
        ];

        for target in config_targets {
            let cmd_name = get_command_name_from_config(&target);
            assert!(
                cmd_name.is_some(),
                "Config target {:?} should have a command name",
                target
            );
        }
    }
}
