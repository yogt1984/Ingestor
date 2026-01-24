//! End-to-End Tests for TUI (T-4.6)
//!
//! Comprehensive integration tests for all TUI workflows:
//! - Command execution through TUI
//! - Navigation flow
//! - Algorithm type validation
//! - Parameter configuration
//! - Results display
//! - Preset management
//! - Error handling
//! - Cancellation

use crate::ui::{
    command_executor::{TUICommandExecutor, CommandResult},
    command_workflow::{CommandWorkflowManager, WorkflowState, config_to_results_target, get_command_name_from_config},
    algorithm_validation::{is_mm_algorithm, is_mm_algorithm_opt, validate_mm_algorithm_for_command},
    submenu::{NavigationTarget, SubMenuAction},
    tui_integration::{process_action, ActionResult, is_config_screen_target, is_results_screen_target},
    state::{GlobalState, AlgorithmConfigSummary},
};
use crate::core::algorithm_config::StrategyType;

// ============================================================================
// Command Execution Tests
// ============================================================================

#[cfg(test)]
mod command_execution_tests {
    use super::*;

    #[test]
    fn test_command_executor_creation() {
        let (executor, _rx) = TUICommandExecutor::new();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_command_executor_cancellation() {
        let (executor, _rx) = TUICommandExecutor::new();
        executor.cancel();
        assert!(executor.is_cancelled());
    }

    #[test]
    fn test_command_executor_reset_cancellation() {
        let (executor, _rx) = TUICommandExecutor::new();
        executor.cancel();
        assert!(executor.is_cancelled());
        executor.reset_cancellation();
        assert!(!executor.is_cancelled());
    }

    #[test]
    fn test_command_executor_default() {
        let executor = TUICommandExecutor::default();
        assert!(!executor.is_cancelled());
    }
}

// ============================================================================
// Navigation Flow Tests
// ============================================================================

#[cfg(test)]
mod navigation_flow_tests {
    use super::*;

    #[test]
    fn test_navigation_menu_to_config_screen() {
        let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateConfig);
        let result = process_action(action);
        assert_eq!(
            result,
            ActionResult::NavigateToConfigScreen(NavigationTarget::BacktestEvaluateConfig)
        );
    }

    #[test]
    fn test_navigation_config_to_results() {
        let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateResults);
        let result = process_action(action);
        assert_eq!(
            result,
            ActionResult::NavigateToResultsScreen(NavigationTarget::BacktestEvaluateResults)
        );
    }

    #[test]
    fn test_navigation_back_from_config() {
        let action = SubMenuAction::Back;
        let result = process_action(action);
        assert_eq!(result, ActionResult::NavigateToSubMenu(crate::ui::tui_integration::CurrentSubMenu::None));
    }

    #[test]
    fn test_workflow_manager_navigation() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        
        // Navigate to config screen
        manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
        match manager.state() {
            WorkflowState::ConfigScreen { target } => {
                assert_eq!(*target, NavigationTarget::BacktestEvaluateConfig);
            }
            _ => panic!("Expected ConfigScreen state"),
        }

        // Start execution
        manager.start_execution("backtest evaluate".to_string());
        match manager.state() {
            WorkflowState::Executing { command_name } => {
                assert_eq!(command_name, "backtest evaluate");
            }
            _ => panic!("Expected Executing state"),
        }

        // Show results
        use crate::commands::backtest::{EvaluateResult, EvaluateMetrics};
        use crate::commands::params::backtest_params::EvaluateParams;
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            events_processed: 0,
            fills_generated: 0,
        });
        manager.show_results(NavigationTarget::BacktestEvaluateResults, result);
        match manager.state() {
            WorkflowState::ResultsScreen { target, .. } => {
                assert_eq!(*target, NavigationTarget::BacktestEvaluateResults);
            }
            _ => panic!("Expected ResultsScreen state"),
        }

        // Back to menu
        manager.back_to_menu();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_complete_navigation_cycle() {
        // Test complete cycle: menu -> config -> execute -> results -> menu
        let (mut manager, _rx) = CommandWorkflowManager::new();
        
        // 1. Menu to config
        manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
        assert!(matches!(manager.state(), WorkflowState::ConfigScreen { .. }));

        // 2. Config to executing
        manager.start_execution("backtest evaluate".to_string());
        assert!(matches!(manager.state(), WorkflowState::Executing { .. }));

        // 3. Executing to results
        use crate::commands::backtest::{EvaluateResult, EvaluateMetrics};
        use crate::commands::params::backtest_params::EvaluateParams;
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            events_processed: 0,
            fills_generated: 0,
        });
        manager.show_results(NavigationTarget::BacktestEvaluateResults, result);
        assert!(matches!(manager.state(), WorkflowState::ResultsScreen { .. }));

        // 4. Results to menu
        manager.back_to_menu();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }
}

// ============================================================================
// Algorithm Type Validation Tests
// ============================================================================

#[cfg(test)]
mod algorithm_validation_tests {
    use super::*;

    fn create_mm_algorithm() -> AlgorithmConfigSummary {
        AlgorithmConfigSummary {
            id: "test-mm".to_string(),
            name: "Test MM".to_string(),
            strategy_type: StrategyType::MarketMaking,
            created_at: chrono::Utc::now(),
        }
    }

    fn create_mom_algorithm() -> AlgorithmConfigSummary {
        AlgorithmConfigSummary {
            id: "test-mom".to_string(),
            name: "Test MOM".to_string(),
            strategy_type: StrategyType::Momentum,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_is_mm_algorithm_market_making() {
        let algo = create_mm_algorithm();
        assert!(is_mm_algorithm(&algo));
    }

    #[test]
    fn test_is_mm_algorithm_momentum() {
        let algo = create_mom_algorithm();
        assert!(!is_mm_algorithm(&algo));
    }

    #[test]
    fn test_is_mm_algorithm_opt_some() {
        let algo = Some(create_mm_algorithm());
        assert!(is_mm_algorithm_opt(algo.as_ref()));
    }

    #[test]
    fn test_is_mm_algorithm_opt_none() {
        let algo: Option<AlgorithmConfigSummary> = None;
        assert!(!is_mm_algorithm_opt(algo.as_ref()));
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_with_mm() {
        let algo = Some(create_mm_algorithm());
        let result = validate_mm_algorithm_for_command(algo.as_ref(), "tune");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_with_mom() {
        let algo = Some(create_mom_algorithm());
        let result = validate_mm_algorithm_for_command(algo.as_ref(), "tune");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_with_none() {
        let algo: Option<AlgorithmConfigSummary> = None;
        let result = validate_mm_algorithm_for_command(algo.as_ref(), "tune");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_mm_algorithm_for_non_mm_command() {
        let algo = Some(create_mom_algorithm());
        let result = validate_mm_algorithm_for_command(algo.as_ref(), "evaluate");
        // Should pass for non-MM commands
        assert!(result.is_ok());
    }
}

// ============================================================================
// Config Screen Target Tests
// ============================================================================

#[cfg(test)]
mod config_screen_target_tests {
    use super::*;

    #[test]
    fn test_all_backtest_config_screens() {
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
        ];

        for target in config_targets {
            assert!(
                is_config_screen_target(&target),
                "Config screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_all_research_config_screens() {
        let config_targets = vec![
            NavigationTarget::ResearchRunConfig,
            NavigationTarget::ResearchStatusConfig,
        ];

        for target in config_targets {
            assert!(
                is_config_screen_target(&target),
                "Config screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_all_validate_config_screens() {
        let config_targets = vec![
            NavigationTarget::ValidateRunConfig,
            NavigationTarget::ValidateShowConfig,
            NavigationTarget::ValidateStatusConfig,
        ];

        for target in config_targets {
            assert!(
                is_config_screen_target(&target),
                "Config screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_all_algorithm_config_screens() {
        let config_targets = vec![
            NavigationTarget::AlgorithmListConfig,
            NavigationTarget::AlgorithmShowConfig,
        ];

        for target in config_targets {
            assert!(
                is_config_screen_target(&target),
                "Config screen target {:?} should be detected",
                target
            );
        }
    }
}

// ============================================================================
// Results Screen Target Tests
// ============================================================================

#[cfg(test)]
mod results_screen_target_tests {
    use super::*;

    #[test]
    fn test_all_backtest_results_screens() {
        let results_targets = vec![
            NavigationTarget::BacktestEvaluateResults,
            NavigationTarget::BacktestTuneResults,
            NavigationTarget::BacktestRegimeSearchResults,
            NavigationTarget::BacktestMultiObjectiveResults,
            NavigationTarget::BacktestRegimeOptimizeResults,
            NavigationTarget::BacktestTrainResults,
            NavigationTarget::BacktestWalkForwardMLResults,
            NavigationTarget::BacktestSweepResults,
            NavigationTarget::BacktestWalkForwardResults,
            NavigationTarget::BacktestOOSValidateResults,
            NavigationTarget::BacktestSimulateResults,
            NavigationTarget::BacktestGridResults,
            NavigationTarget::BacktestCampaignResults,
            NavigationTarget::BacktestPaperResults,
        ];

        for target in results_targets {
            assert!(
                is_results_screen_target(&target),
                "Results screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_all_research_results_screens() {
        let results_targets = vec![
            NavigationTarget::ResearchRunResults,
        ];

        for target in results_targets {
            assert!(
                is_results_screen_target(&target),
                "Results screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_all_validate_results_screens() {
        let results_targets = vec![
            NavigationTarget::ValidateRunResults,
        ];

        for target in results_targets {
            assert!(
                is_results_screen_target(&target),
                "Results screen target {:?} should be detected",
                target
            );
        }
    }
}

// ============================================================================
// Config to Results Mapping Tests
// ============================================================================

#[cfg(test)]
mod config_to_results_mapping_tests {
    use super::*;

    #[test]
    fn test_config_to_results_mapping_backtest_evaluate() {
        let config_target = NavigationTarget::BacktestEvaluateConfig;
        let results_target = config_to_results_target(&config_target);
        assert_eq!(
            results_target,
            Some(NavigationTarget::BacktestEvaluateResults)
        );
    }

    #[test]
    fn test_config_to_results_mapping_backtest_tune() {
        let config_target = NavigationTarget::BacktestTuneConfig;
        let results_target = config_to_results_target(&config_target);
        assert_eq!(
            results_target,
            Some(NavigationTarget::BacktestTuneResults)
        );
    }

    #[test]
    fn test_config_to_results_mapping_all_backtest_commands() {
        let mappings = vec![
            (NavigationTarget::BacktestEvaluateConfig, NavigationTarget::BacktestEvaluateResults),
            (NavigationTarget::BacktestTuneConfig, NavigationTarget::BacktestTuneResults),
            (NavigationTarget::BacktestRegimeSearchConfig, NavigationTarget::BacktestRegimeSearchResults),
            (NavigationTarget::BacktestMultiObjectiveConfig, NavigationTarget::BacktestMultiObjectiveResults),
            (NavigationTarget::BacktestRegimeOptimizeConfig, NavigationTarget::BacktestRegimeOptimizeResults),
            (NavigationTarget::BacktestTrainConfig, NavigationTarget::BacktestTrainResults),
            (NavigationTarget::BacktestWalkForwardMLConfig, NavigationTarget::BacktestWalkForwardMLResults),
            (NavigationTarget::BacktestSweepConfig, NavigationTarget::BacktestSweepResults),
            (NavigationTarget::BacktestWalkForwardConfig, NavigationTarget::BacktestWalkForwardResults),
            (NavigationTarget::BacktestOOSValidateConfig, NavigationTarget::BacktestOOSValidateResults),
            (NavigationTarget::BacktestSimulateConfig, NavigationTarget::BacktestSimulateResults),
            (NavigationTarget::BacktestGridConfig, NavigationTarget::BacktestGridResults),
            (NavigationTarget::BacktestCampaignConfig, NavigationTarget::BacktestCampaignResults),
            (NavigationTarget::BacktestPaperConfig, NavigationTarget::BacktestPaperResults),
        ];

        for (config, expected_results) in mappings {
            let results = config_to_results_target(&config);
            assert_eq!(
                results,
                Some(expected_results.clone()),
                "Config {:?} should map to {:?}",
                config,
                expected_results
            );
        }
    }

    #[test]
    fn test_config_to_results_mapping_research() {
        let config_target = NavigationTarget::ResearchRunConfig;
        let results_target = config_to_results_target(&config_target);
        assert_eq!(
            results_target,
            Some(NavigationTarget::ResearchRunResults)
        );
    }

    #[test]
    fn test_config_to_results_mapping_validate() {
        let config_target = NavigationTarget::ValidateRunConfig;
        let results_target = config_to_results_target(&config_target);
        assert_eq!(
            results_target,
            Some(NavigationTarget::ValidateRunResults)
        );
    }

    #[test]
    fn test_config_to_results_mapping_algorithm_create() {
        let config_target = NavigationTarget::AlgorithmCreate;
        let results_target = config_to_results_target(&config_target);
        assert_eq!(
            results_target,
            Some(NavigationTarget::AlgorithmCreateResults)
        );
    }
}

// ============================================================================
// Command Name Mapping Tests
// ============================================================================

#[cfg(test)]
mod command_name_mapping_tests {
    use super::*;

    #[test]
    fn test_get_command_name_backtest_evaluate() {
        let target = NavigationTarget::BacktestEvaluateConfig;
        let cmd_name = get_command_name_from_config(&target);
        assert_eq!(cmd_name, Some("backtest evaluate"));
    }

    #[test]
    fn test_get_command_name_backtest_tune() {
        let target = NavigationTarget::BacktestTuneConfig;
        let cmd_name = get_command_name_from_config(&target);
        assert_eq!(cmd_name, Some("backtest tune"));
    }

    #[test]
    fn test_get_command_name_all_backtest_commands() {
        let mappings = vec![
            (NavigationTarget::BacktestEvaluateConfig, "backtest evaluate"),
            (NavigationTarget::BacktestTuneConfig, "backtest tune"),
            (NavigationTarget::BacktestRegimeSearchConfig, "backtest regime-search"),
            (NavigationTarget::BacktestMultiObjectiveConfig, "backtest multi-objective"),
            (NavigationTarget::BacktestRegimeOptimizeConfig, "backtest regime-optimize"),
            (NavigationTarget::BacktestTrainConfig, "backtest train"),
            (NavigationTarget::BacktestWalkForwardMLConfig, "backtest walk-forward-ml"),
            (NavigationTarget::BacktestSweepConfig, "backtest sweep"),
            (NavigationTarget::BacktestWalkForwardConfig, "backtest walk-forward"),
            (NavigationTarget::BacktestOOSValidateConfig, "backtest oos-validate"),
            (NavigationTarget::BacktestSimulateConfig, "backtest simulate"),
            (NavigationTarget::BacktestGridConfig, "backtest grid"),
            (NavigationTarget::BacktestCampaignConfig, "backtest campaign"),
            (NavigationTarget::BacktestPaperConfig, "backtest paper"),
        ];

        for (target, expected_cmd) in mappings {
            let cmd_name = get_command_name_from_config(&target);
            assert_eq!(
                cmd_name,
                Some(expected_cmd),
                "Config {:?} should map to command '{}'",
                target,
                expected_cmd
            );
        }
    }

    #[test]
    fn test_get_command_name_research_commands() {
        let mappings = vec![
            (NavigationTarget::ResearchRunConfig, "research run"),
            (NavigationTarget::ResearchStatusConfig, "research status"),
        ];

        for (target, expected_cmd) in mappings {
            let cmd_name = get_command_name_from_config(&target);
            assert_eq!(
                cmd_name,
                Some(expected_cmd),
                "Config {:?} should map to command '{}'",
                target,
                expected_cmd
            );
        }
    }

    #[test]
    fn test_get_command_name_validate_commands() {
        let mappings = vec![
            (NavigationTarget::ValidateRunConfig, "validate run"),
            (NavigationTarget::ValidateShowConfig, "validate show"),
            (NavigationTarget::ValidateStatusConfig, "validate status"),
        ];

        for (target, expected_cmd) in mappings {
            let cmd_name = get_command_name_from_config(&target);
            assert_eq!(
                cmd_name,
                Some(expected_cmd),
                "Config {:?} should map to command '{}'",
                target,
                expected_cmd
            );
        }
    }

    #[test]
    fn test_get_command_name_algorithm_commands() {
        let mappings = vec![
            (NavigationTarget::AlgorithmListConfig, "algorithm list"),
            (NavigationTarget::AlgorithmShowConfig, "algorithm show"),
        ];

        for (target, expected_cmd) in mappings {
            let cmd_name = get_command_name_from_config(&target);
            assert_eq!(
                cmd_name,
                Some(expected_cmd),
                "Config {:?} should map to command '{}'",
                target,
                expected_cmd
            );
        }
    }
}

// ============================================================================
// Workflow State Management Tests
// ============================================================================

#[cfg(test)]
mod workflow_state_tests {
    use super::*;

    #[test]
    fn test_workflow_state_default() {
        let state = WorkflowState::default();
        assert_eq!(state, WorkflowState::Menu);
    }

    #[test]
    fn test_workflow_state_transitions() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        
        // Menu -> Config
        manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
        assert!(matches!(manager.state(), WorkflowState::ConfigScreen { .. }));

        // Config -> Executing
        manager.start_execution("test".to_string());
        assert!(matches!(manager.state(), WorkflowState::Executing { .. }));

        // Executing -> Results
        use crate::commands::backtest::{EvaluateResult, EvaluateMetrics};
        use crate::commands::params::backtest_params::EvaluateParams;
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            events_processed: 0,
            fills_generated: 0,
        });
        manager.show_results(NavigationTarget::BacktestEvaluateResults, result);
        assert!(matches!(manager.state(), WorkflowState::ResultsScreen { .. }));

        // Results -> Menu
        manager.back_to_menu();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_workflow_state_cancellation() {
        let (manager, _rx) = CommandWorkflowManager::new();
        manager.cancel_execution();
        assert!(manager.executor().is_cancelled());
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[test]
    fn test_validate_mm_algorithm_error_message() {
        let algo = Some(AlgorithmConfigSummary {
            id: "test-mom".to_string(),
            name: "Test MOM".to_string(),
            strategy_type: StrategyType::Momentum,
            created_at: chrono::Utc::now(),
        });
        let result = validate_mm_algorithm_for_command(algo.as_ref(), "tune");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Market Making"));
        assert!(error_msg.contains("tune"));
    }

    #[test]
    fn test_config_to_results_invalid_target() {
        let target = NavigationTarget::MainMenu;
        let results = config_to_results_target(&target);
        assert_eq!(results, None);
    }

    #[test]
    fn test_get_command_name_invalid_target() {
        let target = NavigationTarget::MainMenu;
        let cmd_name = get_command_name_from_config(&target);
        assert_eq!(cmd_name, None);
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_full_workflow_backtest_evaluate() {
        // Test complete workflow for backtest evaluate
        let (mut manager, _rx) = CommandWorkflowManager::new();
        let state = GlobalState::default();

        // 1. Navigate to config screen
        manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
        assert!(matches!(manager.state(), WorkflowState::ConfigScreen { .. }));

        // 2. Get command name
        let cmd_name = get_command_name_from_config(&NavigationTarget::BacktestEvaluateConfig);
        assert_eq!(cmd_name, Some("backtest evaluate"));

        // 3. Get results target
        let results_target = config_to_results_target(&NavigationTarget::BacktestEvaluateConfig);
        assert_eq!(results_target, Some(NavigationTarget::BacktestEvaluateResults));

        // 4. Start execution
        manager.start_execution(cmd_name.unwrap().to_string());
        assert!(matches!(manager.state(), WorkflowState::Executing { .. }));

        // 5. Show results
        use crate::commands::backtest::{EvaluateResult, EvaluateMetrics};
        use crate::commands::params::backtest_params::EvaluateParams;
        let result = CommandResult::BacktestEvaluate(EvaluateResult {
            algorithm: "test".to_string(),
            algorithm_name: "Test".to_string(),
            metrics: EvaluateMetrics::default(),
            params: EvaluateParams::default(),
            events_processed: 0,
            fills_generated: 0,
        });
        manager.show_results(results_target.unwrap(), result);
        assert!(matches!(manager.state(), WorkflowState::ResultsScreen { .. }));

        // 6. Back to menu
        manager.back_to_menu();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_algorithm_validation_in_workflow() {
        // Test that MM-only commands are validated
        let mm_algo = Some(AlgorithmConfigSummary {
            id: "test-mm".to_string(),
            name: "Test MM".to_string(),
            strategy_type: StrategyType::MarketMaking,
            created_at: chrono::Utc::now(),
        });

        let mom_algo = Some(AlgorithmConfigSummary {
            id: "test-mom".to_string(),
            name: "Test MOM".to_string(),
            strategy_type: StrategyType::Momentum,
            created_at: chrono::Utc::now(),
        });

        // MM algorithm should pass validation for MM-only commands
        assert!(validate_mm_algorithm_for_command(mm_algo.as_ref(), "tune").is_ok());
        assert!(validate_mm_algorithm_for_command(mm_algo.as_ref(), "grid").is_ok());
        assert!(validate_mm_algorithm_for_command(mm_algo.as_ref(), "regime-search").is_ok());

        // MOM algorithm should fail validation for MM-only commands
        assert!(validate_mm_algorithm_for_command(mom_algo.as_ref(), "tune").is_err());
        assert!(validate_mm_algorithm_for_command(mom_algo.as_ref(), "grid").is_err());
        assert!(validate_mm_algorithm_for_command(mom_algo.as_ref(), "regime-search").is_err());

        // Both should pass for non-MM commands
        assert!(validate_mm_algorithm_for_command(mm_algo.as_ref(), "evaluate").is_ok());
        assert!(validate_mm_algorithm_for_command(mom_algo.as_ref(), "evaluate").is_ok());
    }
}

// ============================================================================
// Cancellation Tests
// ============================================================================

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn test_cancel_during_execution() {
        let (mut manager, _rx) = CommandWorkflowManager::new();
        
        // Start execution
        manager.start_execution("test command".to_string());
        assert!(matches!(manager.state(), WorkflowState::Executing { .. }));

        // Cancel execution
        manager.cancel_execution();
        assert!(manager.executor().is_cancelled());

        // Back to menu
        manager.back_to_menu();
        assert_eq!(manager.state(), &WorkflowState::Menu);
    }

    #[test]
    fn test_cancel_before_execution() {
        let (manager, _rx) = CommandWorkflowManager::new();
        
        // Cancel before starting (should work)
        manager.cancel_execution();
        assert!(manager.executor().is_cancelled());
    }

    #[test]
    fn test_reset_after_cancel() {
        let (manager, _rx) = CommandWorkflowManager::new();
        
        manager.cancel_execution();
        assert!(manager.executor().is_cancelled());
        
        manager.executor().reset_cancellation();
        assert!(!manager.executor().is_cancelled());
    }
}

// ============================================================================
// All 24 Commands Coverage Tests
// ============================================================================

#[cfg(test)]
mod all_commands_coverage_tests {
    use super::*;

    #[test]
    fn test_all_backtest_commands_have_config_screens() {
        let commands = vec![
            ("evaluate", NavigationTarget::BacktestEvaluateConfig),
            ("tune", NavigationTarget::BacktestTuneConfig),
            ("regime-search", NavigationTarget::BacktestRegimeSearchConfig),
            ("multi-objective", NavigationTarget::BacktestMultiObjectiveConfig),
            ("regime-optimize", NavigationTarget::BacktestRegimeOptimizeConfig),
            ("train", NavigationTarget::BacktestTrainConfig),
            ("walk-forward-ml", NavigationTarget::BacktestWalkForwardMLConfig),
            ("sweep", NavigationTarget::BacktestSweepConfig),
            ("walk-forward", NavigationTarget::BacktestWalkForwardConfig),
            ("oos-validate", NavigationTarget::BacktestOOSValidateConfig),
            ("simulate", NavigationTarget::BacktestSimulateConfig),
            ("grid", NavigationTarget::BacktestGridConfig),
            ("campaign", NavigationTarget::BacktestCampaignConfig),
            ("paper", NavigationTarget::BacktestPaperConfig),
        ];

        for (_cmd_name, config_target) in commands {
            assert!(
                is_config_screen_target(&config_target),
                "Command should have config screen: {:?}",
                config_target
            );
            let cmd_name = get_command_name_from_config(&config_target);
            assert!(
                cmd_name.is_some(),
                "Config screen should have command name: {:?}",
                config_target
            );
        }
    }

    #[test]
    fn test_all_research_commands_have_config_screens() {
        let commands = vec![
            ("run", NavigationTarget::ResearchRunConfig),
            ("status", NavigationTarget::ResearchStatusConfig),
        ];

        for (_cmd_name, config_target) in commands {
            assert!(
                is_config_screen_target(&config_target),
                "Command should have config screen: {:?}",
                config_target
            );
            let cmd_name = get_command_name_from_config(&config_target);
            assert!(
                cmd_name.is_some(),
                "Config screen should have command name: {:?}",
                config_target
            );
        }
    }

    #[test]
    fn test_all_validate_commands_have_config_screens() {
        let commands = vec![
            ("run", NavigationTarget::ValidateRunConfig),
            ("show", NavigationTarget::ValidateShowConfig),
            ("status", NavigationTarget::ValidateStatusConfig),
        ];

        for (_cmd_name, config_target) in commands {
            assert!(
                is_config_screen_target(&config_target),
                "Command should have config screen: {:?}",
                config_target
            );
            let cmd_name = get_command_name_from_config(&config_target);
            assert!(
                cmd_name.is_some(),
                "Config screen should have command name: {:?}",
                config_target
            );
        }
    }

    #[test]
    fn test_all_algorithm_commands_have_config_screens() {
        let commands = vec![
            ("list", NavigationTarget::AlgorithmListConfig),
            ("show", NavigationTarget::AlgorithmShowConfig),
        ];

        for (_cmd_name, config_target) in commands {
            assert!(
                is_config_screen_target(&config_target),
                "Command should have config screen: {:?}",
                config_target
            );
            let cmd_name = get_command_name_from_config(&config_target);
            assert!(
                cmd_name.is_some(),
                "Config screen should have command name: {:?}",
                config_target
            );
        }
    }

    #[test]
    fn test_all_commands_have_results_screens() {
        // Test that all config screens map to results screens
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

        for config_target in config_targets {
            let results_target = config_to_results_target(&config_target);
            assert!(
                results_target.is_some(),
                "Config screen {:?} should map to results screen",
                config_target
            );
            if let Some(results) = results_target {
                assert!(
                    is_results_screen_target(&results),
                    "Results target {:?} should be detected as results screen",
                    results
                );
            }
        }
    }
}

// ============================================================================
// Performance Tests
// ============================================================================

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_navigation_action_processing_performance() {
        // Test that navigation action processing is fast (<1ms)
        let start = Instant::now();
        for _ in 0..1000 {
            let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateConfig);
            let _result = process_action(action);
        }
        let duration = start.elapsed();
        let avg_time = duration.as_micros() / 1000;
        assert!(
            avg_time < 1000,
            "Navigation action processing should be <1ms, got {}μs",
            avg_time
        );
    }

    #[test]
    fn test_workflow_state_transition_performance() {
        // Test that workflow state transitions are fast
        let (mut manager, _rx) = CommandWorkflowManager::new();
        let start = Instant::now();
        
        for _ in 0..100 {
            manager.navigate_to_config(NavigationTarget::BacktestEvaluateConfig);
            manager.start_execution("test".to_string());
            manager.back_to_menu();
        }
        
        let duration = start.elapsed();
        let avg_time = duration.as_micros() / 100;
        assert!(
            avg_time < 1000,
            "Workflow state transition should be <1ms, got {}μs",
            avg_time
        );
    }

    #[test]
    fn test_algorithm_validation_performance() {
        // Test that algorithm validation is fast
        let algo = Some(AlgorithmConfigSummary {
            id: "test".to_string(),
            name: "Test".to_string(),
            strategy_type: StrategyType::MarketMaking,
            created_at: chrono::Utc::now(),
        });

        let start = Instant::now();
        for _ in 0..1000 {
            let _ = validate_mm_algorithm_for_command(algo.as_ref(), "tune");
        }
        let duration = start.elapsed();
        let avg_time = duration.as_micros() / 1000;
        assert!(
            avg_time < 1000,
            "Algorithm validation should be <1ms, got {}μs",
            avg_time
        );
    }
}
