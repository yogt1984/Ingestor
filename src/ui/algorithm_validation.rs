//! Algorithm Type Validation (T-4.3)
//!
//! Provides validation functions to check if algorithms are compatible
//! with MM-only commands.

use crate::core::algorithm_config::StrategyType;
use crate::ui::state::AlgorithmConfigSummary;

// ============================================================================
// MM-Only Commands
// ============================================================================

/// List of MM-only command names for error messages
pub const MM_ONLY_COMMANDS: &[&str] = &[
    "tune",
    "regime-search",
    "multi-objective",
    "regime-optimize",
    "train",
    "walk-forward-ml",
    "grid",
];

// ============================================================================
// Validation Functions
// ============================================================================

/// Check if a strategy type is Market Making
pub fn is_market_making(strategy_type: StrategyType) -> bool {
    matches!(strategy_type, StrategyType::MarketMaking)
}

/// Check if an algorithm config is Market Making
pub fn is_mm_algorithm(algorithm: &AlgorithmConfigSummary) -> bool {
    is_market_making(algorithm.strategy_type)
}

/// Check if an algorithm config is Market Making (from Option)
pub fn is_mm_algorithm_opt(algorithm: Option<&AlgorithmConfigSummary>) -> bool {
    algorithm.map_or(false, is_mm_algorithm)
}

/// Validate that an algorithm is Market Making for MM-only commands
/// Returns Ok(()) if the command is not MM-only, or if the algorithm is MM
pub fn validate_mm_algorithm_for_command(
    algorithm: Option<&AlgorithmConfigSummary>,
    command_name: &str,
) -> Result<(), String> {
    // If this is not an MM-only command, validation passes
    if !MM_ONLY_COMMANDS.contains(&command_name) {
        return Ok(());
    }

    let algo = algorithm.ok_or_else(|| {
        format!(
            "No algorithm selected. '{}' requires a Market Making algorithm. \
             Please select an algorithm in the Algorithms menu.",
            command_name
        )
    })?;

    if !is_mm_algorithm(algo) {
        return Err(format!(
            "Command '{}' is only available for Market Making algorithms. \
             Current algorithm '{}' is a {} strategy. \
             Please select a Market Making algorithm in the Algorithms menu.",
            command_name,
            algo.name,
            algo.strategy_type
        ));
    }

    Ok(())
}

/// Get a user-friendly error message for MM-only command with non-MM algorithm
pub fn mm_only_error_message(
    command_name: &str,
    algorithm: &AlgorithmConfigSummary,
) -> String {
    format!(
        "⚠️  '{}' is only available for Market Making algorithms.\n\n\
         Current algorithm: {} ({})\n\n\
         Please select a Market Making algorithm in the Algorithms menu to use this command.",
        command_name,
        algorithm.name,
        algorithm.strategy_type
    )
}

/// Get a user-friendly warning message for MM-only command
pub fn mm_only_warning_message(command_name: &str) -> String {
    format!(
        "⚠️  '{}' requires a Market Making algorithm.\n\n\
         This command is only available for Market Making strategies. \
         Please ensure you have selected a Market Making algorithm.",
        command_name
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::algorithm_config::StrategyType;
    use chrono::Utc;

    fn create_algorithm_summary(name: &str, strategy: StrategyType) -> AlgorithmConfigSummary {
        AlgorithmConfigSummary {
            id: format!("{}_{}", name.to_lowercase(), "20251228"),
            name: name.to_string(),
            strategy_type: strategy,
            created_at: Utc::now(),
        }
    }

    // ============================================================================
    // is_market_making tests
    // ============================================================================

    #[test]
    fn test_is_market_making_true() {
        assert!(is_market_making(StrategyType::MarketMaking));
    }

    #[test]
    fn test_is_market_making_false_momentum() {
        assert!(!is_market_making(StrategyType::Momentum));
    }

    #[test]
    fn test_is_market_making_false_hybrid() {
        assert!(!is_market_making(StrategyType::Hybrid));
    }

    // ============================================================================
    // is_mm_algorithm tests
    // ============================================================================

    #[test]
    fn test_is_mm_algorithm_true() {
        let algo = create_algorithm_summary("mm_algo", StrategyType::MarketMaking);
        assert!(is_mm_algorithm(&algo));
    }

    #[test]
    fn test_is_mm_algorithm_false_momentum() {
        let algo = create_algorithm_summary("momentum_algo", StrategyType::Momentum);
        assert!(!is_mm_algorithm(&algo));
    }

    #[test]
    fn test_is_mm_algorithm_false_hybrid() {
        let algo = create_algorithm_summary("hybrid_algo", StrategyType::Hybrid);
        assert!(!is_mm_algorithm(&algo));
    }

    // ============================================================================
    // is_mm_algorithm_opt tests
    // ============================================================================

    #[test]
    fn test_is_mm_algorithm_opt_none() {
        assert!(!is_mm_algorithm_opt(None));
    }

    #[test]
    fn test_is_mm_algorithm_opt_some_mm() {
        let algo = create_algorithm_summary("mm_algo", StrategyType::MarketMaking);
        assert!(is_mm_algorithm_opt(Some(&algo)));
    }

    #[test]
    fn test_is_mm_algorithm_opt_some_momentum() {
        let algo = create_algorithm_summary("momentum_algo", StrategyType::Momentum);
        assert!(!is_mm_algorithm_opt(Some(&algo)));
    }

    // ============================================================================
    // validate_mm_algorithm_for_command tests
    // ============================================================================

    #[test]
    fn test_validate_mm_algorithm_for_command_no_algorithm() {
        let result = validate_mm_algorithm_for_command(None, "tune");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("No algorithm selected"));
        assert!(error.contains("tune"));
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_mm_algorithm() {
        let algo = create_algorithm_summary("mm_algo", StrategyType::MarketMaking);
        let result = validate_mm_algorithm_for_command(Some(&algo), "tune");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_momentum_algorithm() {
        let algo = create_algorithm_summary("momentum_algo", StrategyType::Momentum);
        let result = validate_mm_algorithm_for_command(Some(&algo), "tune");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("only available for Market Making"));
        assert!(error.contains("tune"));
        assert!(error.contains("momentum_algo"));
        assert!(error.contains("Momentum"));
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_hybrid_algorithm() {
        let algo = create_algorithm_summary("hybrid_algo", StrategyType::Hybrid);
        let result = validate_mm_algorithm_for_command(Some(&algo), "regime-search");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("only available for Market Making"));
        assert!(error.contains("regime-search"));
        assert!(error.contains("hybrid_algo"));
        assert!(error.contains("Hybrid"));
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_all_commands() {
        let algo = create_algorithm_summary("mm_algo", StrategyType::MarketMaking);
        for command in MM_ONLY_COMMANDS {
            let result = validate_mm_algorithm_for_command(Some(&algo), command);
            assert!(result.is_ok(), "Command '{}' should validate for MM algorithm", command);
        }
    }

    #[test]
    fn test_validate_mm_algorithm_for_command_all_commands_non_mm() {
        let algo = create_algorithm_summary("momentum_algo", StrategyType::Momentum);
        for command in MM_ONLY_COMMANDS {
            let result = validate_mm_algorithm_for_command(Some(&algo), command);
            assert!(result.is_err(), "Command '{}' should fail for non-MM algorithm", command);
        }
    }

    // ============================================================================
    // Error message tests
    // ============================================================================

    #[test]
    fn test_mm_only_error_message() {
        let algo = create_algorithm_summary("test_algo", StrategyType::Momentum);
        let msg = mm_only_error_message("tune", &algo);
        assert!(msg.contains("tune"));
        assert!(msg.contains("test_algo"));
        assert!(msg.contains("Momentum"));
        assert!(msg.contains("Market Making"));
    }

    #[test]
    fn test_mm_only_warning_message() {
        let msg = mm_only_warning_message("grid");
        assert!(msg.contains("grid"));
        assert!(msg.contains("Market Making"));
    }

    // ============================================================================
    // Edge case tests
    // ============================================================================

    #[test]
    fn test_all_strategy_types() {
        for strategy in StrategyType::all() {
            let algo = create_algorithm_summary("test", strategy);
            let is_mm = is_mm_algorithm(&algo);
            match strategy {
                StrategyType::MarketMaking => assert!(is_mm, "MarketMaking should be MM"),
                _ => assert!(!is_mm, "{:?} should not be MM", strategy),
            }
        }
    }

    #[test]
    fn test_validate_with_empty_command_name() {
        let algo = create_algorithm_summary("mm_algo", StrategyType::MarketMaking);
        let result = validate_mm_algorithm_for_command(Some(&algo), "");
        assert!(result.is_ok()); // Should still validate algorithm type
    }

    #[test]
    fn test_validate_with_long_command_name() {
        let algo = create_algorithm_summary("mm_algo", StrategyType::MarketMaking);
        let result = validate_mm_algorithm_for_command(
            Some(&algo),
            "very-long-command-name-that-should-still-work"
        );
        assert!(result.is_ok());
    }

    // ============================================================================
    // Integration tests
    // ============================================================================

    #[test]
    fn test_validation_workflow_mm_algorithm() {
        let algo = create_algorithm_summary("mm_spread_skew", StrategyType::MarketMaking);
        
        // Should pass validation
        assert!(is_mm_algorithm(&algo));
        assert!(is_mm_algorithm_opt(Some(&algo)));
        assert!(validate_mm_algorithm_for_command(Some(&algo), "tune").is_ok());
    }

    #[test]
    fn test_validation_workflow_non_mm_algorithm() {
        let algo = create_algorithm_summary("momentum_strategy", StrategyType::Momentum);
        
        // Should fail validation
        assert!(!is_mm_algorithm(&algo));
        assert!(!is_mm_algorithm_opt(Some(&algo)));
        assert!(validate_mm_algorithm_for_command(Some(&algo), "tune").is_err());
    }

    #[test]
    fn test_validation_workflow_no_algorithm() {
        // Should fail validation
        assert!(!is_mm_algorithm_opt(None));
        assert!(validate_mm_algorithm_for_command(None, "tune").is_err());
    }

    // ============================================================================
    // MM_ONLY_COMMANDS constant tests
    // ============================================================================

    #[test]
    fn test_mm_only_commands_constant() {
        assert!(!MM_ONLY_COMMANDS.is_empty());
        assert!(MM_ONLY_COMMANDS.contains(&"tune"));
        assert!(MM_ONLY_COMMANDS.contains(&"regime-search"));
        assert!(MM_ONLY_COMMANDS.contains(&"multi-objective"));
        assert!(MM_ONLY_COMMANDS.contains(&"regime-optimize"));
        assert!(MM_ONLY_COMMANDS.contains(&"train"));
        assert!(MM_ONLY_COMMANDS.contains(&"walk-forward-ml"));
        assert!(MM_ONLY_COMMANDS.contains(&"grid"));
    }

    #[test]
    fn test_all_mm_commands_validate_correctly() {
        let mm_algo = create_algorithm_summary("mm", StrategyType::MarketMaking);
        let non_mm_algo = create_algorithm_summary("non_mm", StrategyType::Momentum);

        for command in MM_ONLY_COMMANDS {
            // MM algorithm should pass
            assert!(
                validate_mm_algorithm_for_command(Some(&mm_algo), command).is_ok(),
                "MM algorithm should pass validation for '{}'",
                command
            );

            // Non-MM algorithm should fail
            assert!(
                validate_mm_algorithm_for_command(Some(&non_mm_algo), command).is_err(),
                "Non-MM algorithm should fail validation for '{}'",
                command
            );
        }
    }
}
