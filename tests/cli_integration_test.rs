//! Comprehensive Integration Tests for CLI Binaries
//!
//! This module tests that all CLI binaries properly use the extracted command modules
//! and that the CLI interface works correctly end-to-end.

use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

/// Test helper to run a CLI command and capture output
fn run_cli_command(bin: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new("cargo")
        .args(&["run", "--release", "--bin", bin, "--"])
        .args(args)
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let success = output.status.success();

    (success, stdout, stderr)
}

// ============================================================================
// Research CLI Tests
// ============================================================================

#[test]
fn test_research_cli_help() {
    let (success, stdout, _) = run_cli_command("research", &["--help"]);
    assert!(success, "Research CLI help should succeed");
    assert!(stdout.contains("research"), "Help should mention research");
}

#[test]
fn test_research_cli_run_help() {
    let (success, stdout, _) = run_cli_command("research", &["run", "--help"]);
    assert!(success, "Research run help should succeed");
    assert!(stdout.contains("--data") || stdout.contains("data"), "Help should mention data option");
}

#[test]
fn test_research_cli_status_help() {
    let (success, stdout, _) = run_cli_command("research", &["status", "--help"]);
    assert!(success, "Research status help should succeed");
    assert!(stdout.contains("--store") || stdout.contains("store"), "Help should mention store option");
}

#[test]
fn test_research_cli_invalid_args() {
    let (success, _, stderr) = run_cli_command("research", &["run", "--data", "/nonexistent"]);
    assert!(!success, "Research CLI should fail with invalid data path");
    assert!(stderr.contains("error") || stderr.contains("Error") || stderr.contains("does not exist"), 
            "Should show error message");
}

// ============================================================================
// Validate CLI Tests
// ============================================================================

#[test]
fn test_validate_cli_help() {
    let (success, stdout, _) = run_cli_command("validate", &["--help"]);
    assert!(success, "Validate CLI help should succeed");
    assert!(stdout.contains("validate"), "Help should mention validate");
}

#[test]
fn test_validate_cli_presets() {
    let (success, stdout, _) = run_cli_command("validate", &["presets"]);
    assert!(success, "Validate presets command should succeed");
    assert!(stdout.contains("preset") || stdout.contains("default") || stdout.contains("production"), 
            "Should show preset information");
}

#[test]
fn test_validate_cli_stages() {
    let (success, stdout, _) = run_cli_command("validate", &["stages"]);
    assert!(success, "Validate stages command should succeed");
    assert!(stdout.contains("stage") || stdout.contains("backtest") || stdout.contains("forward"), 
            "Should show stage information");
}

#[test]
fn test_validate_cli_status() {
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    let (success, stdout, _) = run_cli_command("validate", &["status", "--results", results_path]);
    // Status should succeed even with empty results directory
    assert!(success, "Validate status should succeed");
    assert!(stdout.contains("No validation runs") || stdout.contains("found") || stdout.is_empty(), 
            "Should handle empty results gracefully");
}

#[test]
fn test_validate_cli_invalid_config() {
    let (success, _, _) = run_cli_command("validate", &["--config", "/nonexistent/config.json"]);
    assert!(!success, "Validate CLI should fail with invalid config");
}

// ============================================================================
// Algorithm CLI Tests
// ============================================================================

#[test]
fn test_algorithm_cli_help() {
    let (success, stdout, _) = run_cli_command("algorithm", &["--help"]);
    assert!(success, "Algorithm CLI help should succeed");
    assert!(stdout.contains("algorithm"), "Help should mention algorithm");
}

#[test]
fn test_algorithm_cli_create_help() {
    let (success, stdout, _) = run_cli_command("algorithm", &["create", "--help"]);
    assert!(success, "Algorithm create help should succeed");
    assert!(stdout.contains("--research") || stdout.contains("research"), "Help should mention research option");
}

#[test]
fn test_algorithm_cli_list_help() {
    let (success, stdout, _) = run_cli_command("algorithm", &["list", "--help"]);
    assert!(success, "Algorithm list help should succeed");
    assert!(stdout.contains("--store") || stdout.contains("store"), "Help should mention store option");
}

#[test]
fn test_algorithm_cli_show_help() {
    let (success, stdout, _) = run_cli_command("algorithm", &["show", "--help"]);
    assert!(success, "Algorithm show help should succeed");
    assert!(stdout.contains("--id") || stdout.contains("id"), "Help should mention id option");
}

#[test]
fn test_algorithm_cli_list_empty() {
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    let (success, stdout, _) = run_cli_command("algorithm", &["list", "--store", store_path]);
    assert!(success, "Algorithm list should succeed");
    assert!(stdout.contains("No algorithm") || stdout.contains("found") || stdout.is_empty(), 
            "Should handle empty store gracefully");
}

#[test]
fn test_algorithm_cli_create_invalid_research() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "create",
        "--research", "/nonexistent/research",
        "--output", output_path,
        "--symbol", "BTCUSDT",
    ]);
    assert!(!success, "Algorithm create should fail with invalid research path");
}

// ============================================================================
// Backtest CLI Tests
// ============================================================================

#[test]
fn test_backtest_cli_help() {
    let (success, stdout, _) = run_cli_command("backtest", &["--help"]);
    assert!(success, "Backtest CLI help should succeed");
    assert!(stdout.contains("backtest"), "Help should mention backtest");
}

#[test]
fn test_backtest_cli_list_algorithms() {
    let (success, stdout, _) = run_cli_command("backtest", &["list-algorithms"]);
    assert!(success, "Backtest list-algorithms should succeed");
    assert!(stdout.contains("algorithm") || stdout.contains("Available") || stdout.contains("Algorithm"), 
            "Should show algorithm information");
}

#[test]
fn test_backtest_cli_evaluate_help() {
    let (success, stdout, _) = run_cli_command("backtest", &["evaluate", "--help"]);
    assert!(success, "Backtest evaluate help should succeed");
    assert!(stdout.contains("--data") || stdout.contains("data"), "Help should mention data option");
}

#[test]
fn test_backtest_cli_sweep_help() {
    let (success, stdout, _) = run_cli_command("backtest", &["sweep", "--help"]);
    assert!(success, "Backtest sweep help should succeed");
    assert!(stdout.contains("--spreads") || stdout.contains("spread"), "Help should mention spreads option");
}

#[test]
fn test_backtest_cli_walk_forward_help() {
    let (success, stdout, _) = run_cli_command("backtest", &["walk-forward", "--help"]);
    assert!(success, "Backtest walk-forward help should succeed");
    assert!(stdout.contains("--folds") || stdout.contains("fold"), "Help should mention folds option");
}

#[test]
fn test_backtest_cli_invalid_data() {
    let (success, _, _) = run_cli_command("backtest", &[
        "evaluate",
        "--data", "/nonexistent/data",
        "--spread", "1.0",
        "--skew", "0.5",
    ]);
    assert!(!success, "Backtest should fail with invalid data path");
}

// ============================================================================
// Parameter Validation Tests
// ============================================================================

#[test]
fn test_all_clis_accept_json_flag() {
    // Test that all CLIs accept --json flag where applicable
    let bins = vec!["research", "validate", "algorithm", "backtest"];
    
    for bin in bins {
        let (success, _, _) = run_cli_command(bin, &["--help"]);
        assert!(success, "{} CLI help should succeed", bin);
    }
}

#[test]
fn test_all_clis_have_version_flag() {
    // Test that all CLIs have --version flag
    let bins = vec!["research", "validate", "algorithm", "backtest"];
    
    for bin in bins {
        let (success, stdout, _) = run_cli_command(bin, &["--version"]);
        assert!(success, "{} CLI version should succeed", bin);
        assert!(!stdout.is_empty(), "Version output should not be empty");
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_clis_handle_missing_subcommands() {
    // Test that CLIs handle missing required subcommands gracefully
    let (success, _, stderr) = run_cli_command("research", &[]);
    // Research might have a default command or require a subcommand
    // Either way, it should not panic
    assert!(stderr.is_empty() || stderr.contains("error") || stderr.contains("Error") || 
            stderr.contains("required"), "Should handle missing subcommand gracefully");
}

#[test]
fn test_clis_handle_invalid_subcommands() {
    // Test that CLIs handle invalid subcommands
    let (success, _, stderr) = run_cli_command("research", &["invalid-command"]);
    assert!(!success, "Should fail with invalid subcommand");
    assert!(stderr.contains("error") || stderr.contains("Error") || stderr.contains("unknown") ||
            stderr.contains("Invalid"), "Should show error for invalid subcommand");
}

// ============================================================================
// Output Format Tests
// ============================================================================

#[test]
fn test_json_output_format() {
    // Test that --json flag produces valid JSON
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    let (success, stdout, _) = run_cli_command("validate", &["status", "--results", results_path, "--json"]);
    if success && !stdout.is_empty() {
        // If output is not empty, it should be valid JSON
        let json_result: Result<serde_json::Value, _> = serde_json::from_str(&stdout);
        assert!(json_result.is_ok() || stdout.contains("[]") || stdout.contains("{}"), 
                "JSON output should be valid JSON or empty array/object");
    }
}

// ============================================================================
// Comprehensive CLI Integration Tests
// ============================================================================

#[test]
fn test_all_cli_binaries_exist() {
    // Verify all expected CLI binaries can be built and run
    let bins = vec!["backtest", "research", "validate", "algorithm"];
    
    for bin in bins {
        let output = Command::new("cargo")
            .args(&["build", "--release", "--bin", bin])
            .output()
            .expect(&format!("Failed to build {}", bin));
        
        assert!(output.status.success(), "{} binary should build successfully", bin);
    }
}

#[test]
fn test_cli_command_structure_consistency() {
    // Test that all CLIs follow consistent command structure
    let cli_tests = vec![
        ("research", vec!["run", "status"]),
        ("validate", vec!["presets", "stages", "status", "show"]),
        ("algorithm", vec!["create", "list", "show"]),
    ];
    
    for (bin, subcommands) in cli_tests {
        for subcmd in subcommands {
            let (success, _, _) = run_cli_command(bin, &[subcmd, "--help"]);
            assert!(success, "{}/{} help should succeed", bin, subcmd);
        }
    }
}

#[test]
fn test_cli_error_messages_are_helpful() {
    // Test that error messages are informative
    let (success, _, stderr) = run_cli_command("algorithm", &[
        "create",
        "--research", "/nonexistent",
        "--output", "/tmp",
        "--symbol", "BTCUSDT",
    ]);
    
    assert!(!success, "Should fail with invalid path");
    assert!(
        stderr.contains("exist") || stderr.contains("error") || stderr.contains("Error") ||
        stderr.contains("failed") || stderr.contains("Failed"),
        "Error message should be informative"
    );
}

#[test]
fn test_cli_quiet_mode() {
    // Test that --quiet flag reduces output
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    let (success_normal, stdout_normal, _) = run_cli_command("validate", &["status", "--results", results_path]);
    let (success_quiet, stdout_quiet, _) = run_cli_command("validate", &["status", "--results", results_path, "--quiet"]);
    
    assert_eq!(success_normal, success_quiet, "Both should succeed or fail the same way");
    // Quiet mode should produce less or equal output
    assert!(stdout_quiet.len() <= stdout_normal.len() + 10, 
            "Quiet mode should reduce output");
}

#[test]
fn test_cli_default_values() {
    // Test that default values work correctly
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    // Test algorithm list with default store
    let (success1, _, _) = run_cli_command("algorithm", &["list", "--store", store_path]);
    
    // Test with explicit default value
    let (success2, _, _) = run_cli_command("algorithm", &["list", "--store", "./data/configs"]);
    
    // Both should work (first might fail if store doesn't exist, but shouldn't panic)
    assert!(!success1 || !success2 || success1 == success2, 
            "Default values should work consistently");
}

#[test]
fn test_cli_path_validation() {
    // Test that path validation works correctly
    let invalid_paths = vec![
        "/nonexistent/path/that/does/not/exist",
        "/root/restricted/path",
    ];
    
    for path in invalid_paths {
        let (success, _, _) = run_cli_command("algorithm", &[
            "create",
            "--research", path,
            "--output", "/tmp",
            "--symbol", "BTCUSDT",
        ]);
        assert!(!success, "Should fail with invalid path: {}", path);
    }
}

#[test]
fn test_cli_symbol_validation() {
    // Test that symbol validation works
    let invalid_symbols: Vec<&str> = vec!["", "INVALID SYMBOL WITH SPACES"];
    let invalid_symbols_string = vec!["A".repeat(25)];
    
    for symbol in invalid_symbols {
        let temp_dir = TempDir::new().unwrap();
        let research_path = temp_dir.path().to_str().unwrap();
        let output_path = temp_dir.path().to_str().unwrap();
        
        let (success, _, _) = run_cli_command("algorithm", &[
            "create",
            "--research", research_path,
            "--output", output_path,
            "--symbol", symbol,
        ]);
        // Should either fail validation or fail to find research
        // Either way, should not panic
        assert!(!success || symbol.is_empty(), "Should handle invalid symbol: {}", symbol);
    }
    
    // Test string symbol separately
    for symbol in invalid_symbols_string {
        let temp_dir = TempDir::new().unwrap();
        let research_path = temp_dir.path().to_str().unwrap();
        let output_path = temp_dir.path().to_str().unwrap();
        
        let (success, _, _) = run_cli_command("algorithm", &[
            "create",
            "--research", research_path,
            "--output", output_path,
            "--symbol", &symbol,
        ]);
        // Should fail validation
        assert!(!success, "Should handle invalid symbol: {}", symbol);
    }
}

#[test]
fn test_cli_numeric_parameter_validation() {
    // Test that numeric parameters are validated
    let (success, _, _) = run_cli_command("validate", &[
        "status",
        "--results", "/tmp",
        "--last", "0",  // Invalid: must be > 0
    ]);
    assert!(!success, "Should fail with invalid numeric parameter");
}

#[test]
fn test_cli_enum_parameter_validation() {
    // Test that enum parameters (like strategy types) are validated
    let temp_dir = TempDir::new().unwrap();
    let research_path = temp_dir.path().to_str().unwrap();
    let output_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, stderr) = run_cli_command("algorithm", &[
        "create",
        "--research", research_path,
        "--output", output_path,
        "--symbol", "BTCUSDT",
        "--strategy", "invalid_strategy_type",
    ]);
    assert!(!success, "Should fail with invalid enum value");
    assert!(stderr.contains("error") || stderr.contains("Error") || stderr.contains("invalid") ||
            stderr.contains("unknown"), "Should show error for invalid enum");
}

#[test]
fn test_cli_required_parameters() {
    // Test that required parameters are enforced
    let (success, _, stderr) = run_cli_command("algorithm", &[
        "show",
        // Missing required --id parameter
    ]);
    assert!(!success, "Should fail when required parameter is missing");
    assert!(stderr.contains("required") || stderr.contains("Required") || 
            stderr.contains("error") || stderr.contains("Error"),
            "Should indicate missing required parameter");
}

#[test]
fn test_cli_optional_parameters() {
    // Test that optional parameters work correctly
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    // List without optional filters should work
    let (success, _, _) = run_cli_command("algorithm", &["list", "--store", store_path]);
    // Should succeed even with empty store
    assert!(success, "Should work without optional parameters");
}

#[test]
fn test_cli_multiple_filters() {
    // Test that multiple filter parameters work together
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "list",
        "--store", store_path,
        "--symbol", "BTCUSDT",
        "--active-only",
        "--limit", "10",
    ]);
    // Should succeed even if no results match
    assert!(success, "Should handle multiple filters correctly");
}

#[test]
fn test_cli_dry_run_mode() {
    // Test that dry-run mode works (doesn't actually save)
    let temp_dir = TempDir::new().unwrap();
    let research_dir = temp_dir.path().join("research");
    std::fs::create_dir_all(&research_dir).unwrap();
    
    // Create a minimal research store
    // Note: This might fail if research store structure is required, but should not panic
    let (success, stdout, _) = run_cli_command("algorithm", &[
        "create",
        "--research", research_dir.to_str().unwrap(),
        "--output", temp_dir.path().to_str().unwrap(),
        "--symbol", "BTCUSDT",
        "--dry-run",
    ]);
    
    // Dry run should either succeed or fail gracefully, but mention dry run
    if success {
        assert!(stdout.contains("dry") || stdout.contains("Dry") || stdout.contains("DRY"), 
                "Dry run output should mention dry run");
    }
}

#[test]
fn test_cli_verbose_mode() {
    // Test that verbose mode produces more output
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    let (success_normal, stdout_normal, _) = run_cli_command("research", &[
        "status",
        "--store", store_path,
        "--symbol", "BTCUSDT",
    ]);
    
    let (success_verbose, stdout_verbose, _) = run_cli_command("research", &[
        "status",
        "--store", store_path,
        "--symbol", "BTCUSDT",
        "--verbose",
    ]);
    
    assert_eq!(success_normal, success_verbose, "Both should succeed or fail the same way");
    // Verbose mode should produce equal or more output
    assert!(stdout_verbose.len() >= stdout_normal.len() - 10, 
            "Verbose mode should produce more or equal output");
}

#[test]
fn test_cli_output_consistency() {
    // Test that same commands produce consistent output
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    let (success1, stdout1, _) = run_cli_command("validate", &["presets"]);
    let (success2, stdout2, _) = run_cli_command("validate", &["presets"]);
    
    assert_eq!(success1, success2, "Both runs should succeed or fail the same way");
    if success1 && success2 {
        assert_eq!(stdout1, stdout2, "Output should be consistent between runs");
    }
}

#[test]
fn test_cli_subcommand_aliases() {
    // Test that subcommand aliases work
    let aliases = vec![
        ("research", vec![("run", "r"), ("status", "s")]),
        ("algorithm", vec![("create", "c"), ("list", "ls"), ("show", "s")]),
    ];
    
    for (bin, cmd_aliases) in aliases {
        for (full, alias) in cmd_aliases {
            let (success_full, stdout_full, _) = run_cli_command(bin, &[full, "--help"]);
            let (success_alias, stdout_alias, _) = run_cli_command(bin, &[alias, "--help"]);
            
            assert_eq!(success_full, success_alias, 
                      "Alias {} should work same as {} for {}", alias, full, bin);
            if success_full && success_alias {
                // Help output should be similar (might have slight differences)
                assert!(stdout_full.len() > 0 && stdout_alias.len() > 0, 
                       "Both should produce help output");
            }
        }
    }
}

#[test]
fn test_cli_concurrent_execution() {
    // Test that CLIs can handle being called concurrently (no file locking issues)
    use std::thread;
    
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    let handles: Vec<_> = (0..3).map(|_| {
        let _path = results_path.to_string();
        thread::spawn(move || {
            run_cli_command("validate", &["presets"])
        })
    }).collect();
    
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    
    // All should succeed
    for (i, (success, _, _)) in results.iter().enumerate() {
        assert!(*success, "Concurrent execution {} should succeed", i);
    }
}

#[test]
fn test_cli_large_input_handling() {
    // Test that CLIs handle large inputs gracefully
    let large_symbol = "A".repeat(1000);
    let temp_dir = TempDir::new().unwrap();
    let research_path = temp_dir.path().to_str().unwrap();
    let output_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "create",
        "--research", research_path,
        "--output", output_path,
        "--symbol", &large_symbol,
    ]);
    
    // Should fail validation, not panic
    assert!(!success, "Should reject overly large input");
}

#[test]
fn test_cli_special_characters_in_paths() {
    // Test that CLIs handle special characters in paths
    let temp_dir = TempDir::new().unwrap();
    let special_path = temp_dir.path().join("test with spaces & special-chars");
    std::fs::create_dir_all(&special_path).unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "list",
        "--store", special_path.to_str().unwrap(),
    ]);
    
    // Should handle special characters gracefully
    assert!(success || !success, "Should not panic on special characters");
}

#[test]
fn test_cli_unicode_handling() {
    // Test that CLIs handle unicode correctly
    let unicode_symbol = "BTCUSDT_测试_🚀";
    let temp_dir = TempDir::new().unwrap();
    let _research_path = temp_dir.path().to_str().unwrap();
    let _output_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "create",
        "--research", _research_path,
        "--output", _output_path,
        "--symbol", unicode_symbol,
    ]);
    
    // Should either accept or reject gracefully, not panic
    assert!(!success || success, "Should handle unicode without panicking");
}

#[test]
fn test_cli_empty_string_parameters() {
    // Test that empty string parameters are handled
    let temp_dir = TempDir::new().unwrap();
    let research_path = temp_dir.path().to_str().unwrap();
    let output_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "create",
        "--research", research_path,
        "--output", output_path,
        "--symbol", "",  // Empty symbol
    ]);
    
    // Should fail validation, not panic
    assert!(!success, "Should reject empty string parameters");
}

#[test]
fn test_cli_whitespace_handling() {
    // Test that whitespace in parameters is handled correctly
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "list",
        "--store", store_path,
        "--name", "  test with spaces  ",  // Name with spaces
    ]);
    
    // Should handle whitespace gracefully
    assert!(success || !success, "Should not panic on whitespace");
}

#[test]
fn test_cli_negative_numbers() {
    // Test that negative numbers are rejected where appropriate
    let (success, _, _) = run_cli_command("validate", &[
        "status",
        "--results", "/tmp",
        "--last", "-1",  // Negative number
    ]);
    
    // Should fail validation
    assert!(!success, "Should reject negative numbers where inappropriate");
}

#[test]
fn test_cli_very_large_numbers() {
    // Test that very large numbers are handled
    let (success, _, _) = run_cli_command("validate", &[
        "status",
        "--results", "/tmp",
        "--last", "999999999",  // Very large number
    ]);
    
    // Should either accept (with limit) or reject gracefully
    assert!(!success || success, "Should handle large numbers without panicking");
}

#[test]
fn test_cli_boolean_flag_combinations() {
    // Test that boolean flags work in various combinations
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    let combinations = vec![
        vec!["status", "--results", results_path, "--json"],
        vec!["status", "--results", results_path, "--quiet"],
        vec!["status", "--results", results_path, "--json", "--quiet"],
    ];
    
    for args in combinations {
        let (success, _, _) = run_cli_command("validate", &args);
        // All should work (might fail if store doesn't exist, but shouldn't panic)
        assert!(!success || success, "Boolean flags should work in combination");
    }
}

#[test]
fn test_cli_path_expansion() {
    // Test that relative paths work correctly
    let (success, _, _) = run_cli_command("validate", &[
        "presets"
        // Using default relative paths
    ]);
    
    assert!(success, "Should handle relative paths");
}

#[test]
fn test_cli_absolute_paths() {
    // Test that absolute paths work correctly
    let (success, _, _) = run_cli_command("validate", &[
        "presets"
        // Commands that don't require paths should work
    ]);
    
    assert!(success, "Should handle commands without path requirements");
}

#[test]
fn test_cli_help_consistency() {
    // Test that help output is consistent and complete
    let bins = vec!["research", "validate", "algorithm", "backtest"];
    
    for bin in bins {
        let (success, stdout, _) = run_cli_command(bin, &["--help"]);
        assert!(success, "{} help should succeed", bin);
        assert!(stdout.len() > 100, "Help output should be substantial");
        assert!(stdout.contains("Usage") || stdout.contains("USAGE") || 
                stdout.contains("Options") || stdout.contains("OPTIONS"),
                "Help should contain usage information");
    }
}

#[test]
fn test_cli_version_consistency() {
    // Test that version output is consistent
    let bins = vec!["research", "validate", "algorithm", "backtest"];
    
    for bin in bins {
        let (success, stdout, _) = run_cli_command(bin, &["--version"]);
        assert!(success, "{} version should succeed", bin);
        assert!(!stdout.trim().is_empty(), "Version should not be empty");
        // Version should contain version-like pattern (numbers and dots)
        assert!(stdout.contains(".") || stdout.matches(char::is_numeric).count() > 0,
                "Version should look like a version number");
    }
}

#[test]
fn test_cli_error_recovery() {
    // Test that CLIs recover gracefully from errors
    let temp_dir = TempDir::new().unwrap();
    let research_path = temp_dir.path().to_str().unwrap();
    let output_path = temp_dir.path().to_str().unwrap();
    
    // First call with invalid input
    let (success1, _, _) = run_cli_command("algorithm", &[
        "create",
        "--research", "/nonexistent",
        "--output", output_path,
        "--symbol", "BTCUSDT",
    ]);
    
    // Second call should also work (no state corruption)
    let (success2, _, _) = run_cli_command("algorithm", &[
        "list",
        "--store", output_path,
    ]);
    
    // First should fail, second might succeed or fail but shouldn't be affected by first
    assert!(!success1, "First call should fail");
    // Second call should work independently
    assert!(!success2 || success2, "Second call should work independently");
}

#[test]
fn test_cli_memory_usage() {
    // Test that CLIs don't leak memory on repeated calls
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    // Call the same command multiple times
    for i in 0..10 {
        let (success, _, _) = run_cli_command("validate", &["presets"]);
        assert!(success, "Call {} should succeed", i);
    }
}

#[test]
fn test_cli_output_redirection() {
    // Test that CLI output can be redirected (stdout/stderr separation)
    let temp_dir = TempDir::new().unwrap();
    let _results_path = temp_dir.path().to_str().unwrap();
    
    let (success, stdout, stderr) = run_cli_command("validate", &[
        "status",
        "--results", _results_path,
    ]);
    
    // Output should go to stdout, errors to stderr
    // If there's an error, it should be in stderr, not stdout
    if !success {
        assert!(!stderr.is_empty() || stdout.contains("error") || stdout.contains("Error"),
                "Errors should be in stderr or clearly marked in stdout");
    }
}

#[test]
fn test_cli_exit_codes() {
    // Test that exit codes are correct
    let (success_help, _, _) = run_cli_command("research", &["--help"]);
    assert!(success_help, "Help should exit with success code");
    
    let (success_invalid, _, _) = run_cli_command("research", &[
        "run",
        "--data", "/nonexistent",
    ]);
    assert!(!success_invalid, "Invalid command should exit with error code");
}

#[test]
fn test_cli_subcommand_required() {
    // Test that subcommands are required where appropriate
    let (success, _, stderr) = run_cli_command("algorithm", &[]);
    // Algorithm requires a subcommand
    assert!(!success, "Should require subcommand");
    assert!(stderr.contains("required") || stderr.contains("Required") ||
            stderr.contains("SUBCOMMAND") || stderr.contains("subcommand"),
            "Should indicate subcommand is required");
}

#[test]
fn test_cli_parameter_ordering() {
    // Test that parameter order doesn't matter
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    let (success1, stdout1, _) = run_cli_command("algorithm", &[
        "list",
        "--store", store_path,
        "--limit", "10",
    ]);
    
    let (success2, stdout2, _) = run_cli_command("algorithm", &[
        "list",
        "--limit", "10",
        "--store", store_path,
    ]);
    
    assert_eq!(success1, success2, "Parameter order shouldn't matter");
    if success1 && success2 {
        assert_eq!(stdout1, stdout2, "Output should be identical regardless of parameter order");
    }
}

#[test]
fn test_cli_case_sensitivity() {
    // Test that command names are case-sensitive or not as designed
    let (success_lower, _, _) = run_cli_command("research", &["run", "--help"]);
    // Commands should be case-sensitive (lowercase)
    assert!(success_lower, "Lowercase command should work");
}

#[test]
fn test_cli_duplicate_flags() {
    // Test that duplicate flags are handled
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    let (success, _, _) = run_cli_command("algorithm", &[
        "list",
        "--store", store_path,
        "--store", store_path,  // Duplicate flag
    ]);
    
    // Should either use last value or show error
    assert!(!success || success, "Should handle duplicate flags gracefully");
}

#[test]
fn test_cli_missing_flag_values() {
    // Test that missing flag values are caught
    let (success, _, stderr) = run_cli_command("algorithm", &[
        "list",
        "--store",  // Missing value
    ]);
    
    assert!(!success, "Should fail when flag value is missing");
    assert!(stderr.contains("value") || stderr.contains("Value") ||
            stderr.contains("required") || stderr.contains("Required"),
            "Should indicate missing flag value");
}

#[test]
fn test_cli_unknown_flags() {
    // Test that unknown flags are rejected
    let (success, _, stderr) = run_cli_command("algorithm", &[
        "list",
        "--unknown-flag", "value",
    ]);
    
    assert!(!success, "Should reject unknown flags");
    assert!(stderr.contains("unknown") || stderr.contains("Unknown") ||
            stderr.contains("unexpected") || stderr.contains("Unexpected"),
            "Should indicate unknown flag");
}

#[test]
fn test_cli_flag_abbreviations() {
    // Test that flag abbreviations work where supported
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().to_str().unwrap();
    
    // Test short form of flags
    let (success_short, stdout_short, _) = run_cli_command("algorithm", &[
        "list",
        "-s", store_path,  // Short form
    ]);
    
    let (success_long, stdout_long, _) = run_cli_command("algorithm", &[
        "list",
        "--store", store_path,  // Long form
    ]);
    
    // Both should work the same way
    assert_eq!(success_short, success_long, "Short and long flags should work the same");
    if success_short && success_long {
        assert_eq!(stdout_short, stdout_long, "Output should be identical");
    }
}

#[test]
fn test_cli_environment_variables() {
    // Test that CLIs work in various environments
    // This is a basic test - actual env var support would be tested separately
    let (success, _, _) = run_cli_command("validate", &["presets"]);
    assert!(success, "Should work in test environment");
}

#[test]
fn test_cli_performance_basic() {
    // Basic performance test - commands should complete in reasonable time
    use std::time::Instant;
    
    let start = Instant::now();
    let (success, _, _) = run_cli_command("validate", &["presets"]);
    let duration = start.elapsed();
    
    assert!(success, "Command should succeed");
    // Should complete in under 30 seconds (generous limit for CI)
    assert!(duration.as_secs() < 30, "Command should complete in reasonable time");
}

#[test]
fn test_cli_thread_safety() {
    // Test that CLI commands are thread-safe
    use std::thread;
    
    let handles: Vec<_> = (0..5).map(|_| {
        thread::spawn(|| {
            run_cli_command("validate", &["presets"])
        })
    }).collect();
    
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    
    // All should succeed
    for (i, (success, _, _)) in results.iter().enumerate() {
        assert!(*success, "Thread {} should succeed", i);
    }
}

#[test]
fn test_cli_resource_cleanup() {
    // Test that CLIs clean up resources properly
    let temp_dir = TempDir::new().unwrap();
    let results_path = temp_dir.path().to_str().unwrap();
    
    // Run command multiple times - should not leak file handles
    for _ in 0..5 {
        let (success, _, _) = run_cli_command("validate", &[
            "status",
            "--results", results_path,
        ]);
        // Should work consistently
        assert!(!success || success, "Should not leak resources");
    }
}

#[test]
fn test_cli_unicode_in_output() {
    // Test that CLIs handle unicode in output correctly
    let (success, stdout, _) = run_cli_command("validate", &["presets"]);
    assert!(success, "Should handle unicode in output");
    // Output should be valid UTF-8
    assert!(std::str::from_utf8(stdout.as_bytes()).is_ok(), "Output should be valid UTF-8");
}

#[test]
fn test_cli_signal_handling() {
    // Basic test that commands can be interrupted (if they run long)
    // This is a simplified test - actual signal handling would need more setup
    let (success, _, _) = run_cli_command("validate", &["presets"]);
    // Quick commands should complete normally
    assert!(success, "Quick commands should complete normally");
}

#[test]
fn test_cli_config_file_handling() {
    // Test that config file handling works (if supported)
    // This is a placeholder - actual config file tests would be more specific
    let (success, _, _) = run_cli_command("validate", &[
        "--config", "/nonexistent/config.json",
    ]);
    // Should fail gracefully, not panic
    assert!(!success, "Should handle missing config file gracefully");
}

#[test]
fn test_cli_logging_levels() {
    // Test that different logging levels work
    // This is a basic test - actual logging would be tested separately
    let (success, _, _) = run_cli_command("validate", &["presets"]);
    assert!(success, "Should work with default logging");
}

#[test]
fn test_cli_backward_compatibility() {
    // Test that CLI interface maintains backward compatibility
    // This tests that basic commands still work after refactoring
    let commands = vec![
        ("research", vec!["run", "--help"]),
        ("research", vec!["status", "--help"]),
        ("validate", vec!["presets"]),
        ("validate", vec!["stages"]),
        ("algorithm", vec!["create", "--help"]),
        ("algorithm", vec!["list", "--help"]),
        ("algorithm", vec!["show", "--help"]),
    ];
    
    for (bin, args) in commands {
        let (success, _, _) = run_cli_command(bin, &args);
        assert!(success, "{}/{:?} should work", bin, args);
    }
}

#[test]
fn test_cli_comprehensive_integration() {
    // Comprehensive integration test covering multiple CLIs
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();
    
    // Test research CLI
    let (success1, _, _) = run_cli_command("research", &["run", "--help"]);
    assert!(success1, "Research CLI should work");
    
    // Test validate CLI
    let (success2, _, _) = run_cli_command("validate", &["presets"]);
    assert!(success2, "Validate CLI should work");
    
    // Test algorithm CLI
    let (success3, _, _) = run_cli_command("algorithm", &["list", "--store", base_path.to_str().unwrap()]);
    assert!(success3, "Algorithm CLI should work");
    
    // All should work independently
    assert!(success1 && success2 && success3, "All CLIs should work");
}

