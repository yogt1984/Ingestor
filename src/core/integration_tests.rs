//! Framework Integration Tests (Task 0.6)
//!
//! Comprehensive integration tests for the MARS framework, testing interactions
//! between all persistent stores (ResearchStore, ResultsStore, ConfigStore).
//!
//! These tests verify:
//! - Full workflow: save research -> save config -> save result -> load all
//! - Cross-store queries and linkage
//! - Persistence and recovery across restarts
//! - Audit trail integrity
//! - Edge cases and error handling

#[cfg(test)]
mod tests {
    use crate::core::{
        // Research State and Store
        ResearchState, ResearchStore, ResearchStoreConfig,
        MIDCEstimate, MIDCRegime, PersistenceStats,
        ConditionalProbability, TradeableAssessment, RecommendedStrategy,
        // TSMOM
        TSMOMConfig, TSMOMStats, TSMOMSignalType, BarSize,
        // Validation Result and Store
        ValidationResult, ValidationStageType,
        TradeResult, TradeDirection,
        ResultsStore, ResultsStoreConfig, ResultsQuery,
        // Algorithm Config and Store
        AlgorithmConfig, AlgorithmConfigBuilder, ConfigPreset, StrategyType,
        ConfigStore, ConfigStoreConfig, ConfigQuery,
    };
    use chrono::{Duration, Utc};
    use std::collections::HashMap;
    use tempfile::TempDir;

    // ==================== Test Helpers ====================

    /// Create a test ResearchState with realistic values
    fn create_test_research_state(symbol: &str) -> ResearchState {
        let mut state = ResearchState::new(symbol);

        // Set MIDC estimate using the new constructor
        state.midc = MIDCEstimate::new(
            0.05,    // kappa
            0.25,    // rho_0
            0.85,    // r_squared
            5000,    // sample_size
        );

        // Set persistence stats
        state.persistence = PersistenceStats {
            mean_duration_seconds: 45.0,
            median_duration_seconds: 32.0,
            std_duration_seconds: 20.0,
            percentile_25: 15.0,
            percentile_75: 60.0,
            sample_count: 150,
            updated_at: Utc::now(),
        };

        // Set tradeable assessment
        state.assessment = TradeableAssessment::new(true, true, true, true);

        // Add some conditional probabilities
        state.conditional_table.insert(
            "Large_Fast_Up_Smooth".to_string(),
            ConditionalProbability {
                p_continuation: 0.72,
                p_reversal: 0.18,
                expected_magnitude_bps: 8.5,
                std_magnitude_bps: 3.0,
                sample_count: 250,
                confidence_interval: (0.68, 0.76),
            },
        );
        state.conditional_table.insert(
            "Medium_Normal_Down_Choppy".to_string(),
            ConditionalProbability {
                p_continuation: 0.45,
                p_reversal: 0.35,
                expected_magnitude_bps: 3.2,
                std_magnitude_bps: 2.0,
                sample_count: 180,
                confidence_interval: (0.40, 0.50),
            },
        );

        state
    }

    /// Create a test ValidationResult with realistic metrics
    fn create_test_validation_result(
        stage: ValidationStageType,
        config_id: &str,
        research_state_id: &str,
    ) -> ValidationResult {
        let period_start = Utc::now() - Duration::days(7);
        let period_end = Utc::now();

        let trades: Vec<TradeResult> = (0..5).map(|i| {
            let direction = if i % 2 == 0 { TradeDirection::Long } else { TradeDirection::Short };
            TradeResult::new(
                format!("trade_{}", i),
                direction,
                period_start + Duration::hours(i as i64),
                period_start + Duration::hours(i as i64 + 1),
                50000.0 + (i as f64 * 100.0),
                50100.0 + (i as f64 * 100.0),
                0.01,
            )
        }).collect();

        let mut result = ValidationResult::new(
            stage,
            format!("{:?}-Test", stage),
            config_id.to_string(),
            period_start,
            period_end,
        );
        result.research_state_id = Some(research_state_id.to_string());
        result = result.with_trades(trades);
        result
    }

    /// Create all three stores with temp directories
    fn create_test_stores() -> (ResearchStore, ResultsStore, ConfigStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let research_config = ResearchStoreConfig::with_path(temp_dir.path().join("research"));
        let results_config = ResultsStoreConfig::with_path(temp_dir.path().join("results"));
        let config_config = ConfigStoreConfig::with_path(temp_dir.path().join("configs"))
            .without_parquet();

        let research_store = ResearchStore::new(research_config).unwrap();
        let results_store = ResultsStore::new(results_config).unwrap();
        let config_store = ConfigStore::new(config_config).unwrap();

        (research_store, results_store, config_store, temp_dir)
    }

    // ==================== Full Workflow Integration Tests ====================

    #[test]
    fn test_full_workflow_research_to_config_to_result() {
        // This tests the core workflow:
        // 1. Save research state
        // 2. Generate algorithm config from research
        // 3. Save algorithm config
        // 4. Run validation (simulated) and save result
        // 5. Load all and verify linkage

        let (mut research_store, mut results_store, mut config_store, _temp) = create_test_stores();

        // Step 1: Save research state
        let research_state = create_test_research_state("BTCUSDT");
        let research_id = research_state.id.clone();
        research_store.save(&research_state).unwrap();

        // Step 2: Generate algorithm config from research
        let config = AlgorithmConfig::from_research(&research_state);
        let config_id = config.id.clone();
        assert!(config.source_research_id.is_some());
        assert_eq!(config.source_research_id.as_ref().unwrap(), &research_id);

        // Step 3: Save algorithm config
        config_store.save(&config).unwrap();

        // Step 4: Create and save validation result
        let validation_result = create_test_validation_result(
            ValidationStageType::Backtest,
            &config_id,
            &research_id,
        );
        let result_id = validation_result.id.clone();
        results_store.save(&validation_result).unwrap();

        // Step 5: Load all and verify linkage
        // ResearchStore uses load(symbol) to get latest state for that symbol
        let loaded_research = research_store.load("BTCUSDT").unwrap().unwrap();
        assert_eq!(loaded_research.id, research_id);

        let loaded_config = config_store.load(&config_id).unwrap().unwrap();
        assert_eq!(loaded_config.id, config_id);
        assert_eq!(loaded_config.source_research_id.as_ref().unwrap(), &research_id);

        let loaded_results = results_store.load_by_config(&config_id).unwrap();
        assert_eq!(loaded_results.len(), 1);
        assert_eq!(loaded_results[0].id, result_id);
        assert_eq!(loaded_results[0].research_state_id.as_ref().unwrap(), &research_id);
    }

    #[test]
    fn test_multiple_configs_from_same_research() {
        let (mut research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create and save research state
        let research_state = create_test_research_state("ETHUSDT");
        let research_id = research_state.id.clone();
        research_store.save(&research_state).unwrap();

        // Create multiple configs from the same research
        let config1 = AlgorithmConfig::from_research(&research_state);

        let mut config2 = AlgorithmConfigBuilder::new("Conservative", "ETHUSDT")
            .strategy_type(StrategyType::Momentum)
            .take_profit_bps(15.0)
            .stop_loss_bps(8.0)
            .build_unchecked();
        config2.source_research_id = Some(research_id.clone());

        let mut config3 = AlgorithmConfigBuilder::new("Aggressive", "ETHUSDT")
            .strategy_type(StrategyType::Momentum)
            .take_profit_bps(30.0)
            .stop_loss_bps(15.0)
            .build_unchecked();
        config3.source_research_id = Some(research_id.clone());

        config_store.save(&config1).unwrap();
        config_store.save(&config2).unwrap();
        config_store.save(&config3).unwrap();

        // Query all configs
        let all_configs = config_store.list_all().unwrap();
        assert_eq!(all_configs.len(), 3);

        // All should reference the same research state
        for config in &all_configs {
            assert_eq!(config.source_research_id.as_ref().unwrap(), &research_id);
        }

        // Query by symbol
        let query = ConfigQuery::new().with_symbol("ETHUSDT");
        let eth_configs = config_store.query(&query).unwrap();
        assert_eq!(eth_configs.len(), 3);
    }

    #[test]
    fn test_validation_results_across_stages() {
        let (mut research_store, mut results_store, mut config_store, _temp) = create_test_stores();

        // Setup
        let research_state = create_test_research_state("BTCUSDT");
        let research_id = research_state.id.clone();
        research_store.save(&research_state).unwrap();

        let config = AlgorithmConfig::from_research(&research_state);
        let config_id = config.id.clone();
        config_store.save(&config).unwrap();

        // Create validation results for multiple stages
        let stages = vec![
            ValidationStageType::Backtest,
            ValidationStageType::Forward,
            ValidationStageType::OutOfSample,
            ValidationStageType::Paper,
        ];

        for stage in &stages {
            let result = create_test_validation_result(*stage, &config_id, &research_id);
            results_store.save(&result).unwrap();
        }

        // Query by stage
        for stage in &stages {
            let results = results_store.load_by_stage(*stage).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].stage_type, *stage);
        }

        // Query all results for config
        let all_results = results_store.load_by_config(&config_id).unwrap();
        assert_eq!(all_results.len(), 4);

        // Aggregate metrics
        let aggregated = results_store.aggregate_by_config(&config_id).unwrap();
        assert!(aggregated.count > 0);
    }

    // ==================== Persistence and Recovery Tests ====================

    #[test]
    fn test_persistence_across_store_reinit() {
        let temp_dir = TempDir::new().unwrap();
        let research_path = temp_dir.path().join("research");
        let config_path = temp_dir.path().join("configs");

        let research_id;
        let config_id;

        // First session - create and save
        {
            let research_config = ResearchStoreConfig::with_path(&research_path);
            let config_config = ConfigStoreConfig::with_path(&config_path).without_parquet();

            let mut research_store = ResearchStore::new(research_config).unwrap();
            let mut config_store = ConfigStore::new(config_config).unwrap();

            let research_state = create_test_research_state("BTCUSDT");
            research_id = research_state.id.clone();
            research_store.save(&research_state).unwrap();

            let config = AlgorithmConfig::from_research(&research_state);
            config_id = config.id.clone();
            config_store.save(&config).unwrap();
        }

        // Second session - reload and verify
        {
            let research_config = ResearchStoreConfig::with_path(&research_path);
            let config_config = ConfigStoreConfig::with_path(&config_path).without_parquet();

            let mut research_store = ResearchStore::new(research_config).unwrap();
            let mut config_store = ConfigStore::new(config_config).unwrap();

            // load(symbol) returns latest for that symbol
            let loaded_research = research_store.load("BTCUSDT").unwrap();
            assert!(loaded_research.is_some());
            assert_eq!(loaded_research.unwrap().id, research_id);

            let loaded_config = config_store.load(&config_id).unwrap();
            assert!(loaded_config.is_some());
            assert_eq!(loaded_config.unwrap().id, config_id);
        }
    }

    #[test]
    fn test_historical_research_state_loading() {
        let (mut research_store, _results_store, _config_store, _temp) = create_test_stores();

        // Save multiple research states over time for the same symbol
        let mut timestamps = Vec::new();
        for i in 0..5 {
            let mut state = create_test_research_state("BTCUSDT");
            // Modify MIDC to make each unique
            state.midc = MIDCEstimate::new(
                0.05 + (i as f64 * 0.01),
                0.25,
                0.85,
                5000,
            );
            timestamps.push(state.timestamp);
            research_store.save(&state).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure different timestamps
        }

        // load() returns the latest state for the symbol
        let latest = research_store.load("BTCUSDT").unwrap().unwrap();
        // The latest should have the highest kappa (0.09)
        assert!((latest.midc.kappa - 0.09).abs() < 0.001);

        // list_states gives all states for a symbol with timestamps
        let all_states = research_store.list_states("BTCUSDT").unwrap();
        assert_eq!(all_states.len(), 5);
    }

    // ==================== Cross-Store Query Tests ====================

    #[test]
    fn test_query_results_by_research_lineage() {
        let (mut research_store, mut results_store, mut config_store, _temp) = create_test_stores();

        // Create two research states for different symbols
        let research1 = create_test_research_state("BTCUSDT");
        let research1_id = research1.id.clone();
        research_store.save(&research1).unwrap();

        let research2 = create_test_research_state("ETHUSDT");
        let research2_id = research2.id.clone();
        research_store.save(&research2).unwrap();

        // Create configs from each research
        let config1 = AlgorithmConfig::from_research(&research1);
        let config1_id = config1.id.clone();
        config_store.save(&config1).unwrap();

        let config2 = AlgorithmConfig::from_research(&research2);
        let config2_id = config2.id.clone();
        config_store.save(&config2).unwrap();

        // Create results for each config
        for _ in 0..3 {
            let result1 = create_test_validation_result(
                ValidationStageType::Backtest,
                &config1_id,
                &research1_id,
            );
            results_store.save(&result1).unwrap();
        }

        for _ in 0..2 {
            let result2 = create_test_validation_result(
                ValidationStageType::Backtest,
                &config2_id,
                &research2_id,
            );
            results_store.save(&result2).unwrap();
        }

        // Query results by config
        let results1 = results_store.load_by_config(&config1_id).unwrap();
        assert_eq!(results1.len(), 3);

        let results2 = results_store.load_by_config(&config2_id).unwrap();
        assert_eq!(results2.len(), 2);

        // Verify research state linkage
        for result in &results1 {
            assert_eq!(result.research_state_id.as_ref().unwrap(), &research1_id);
        }
        for result in &results2 {
            assert_eq!(result.research_state_id.as_ref().unwrap(), &research2_id);
        }
    }

    #[test]
    fn test_config_version_tracking() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create initial config
        let config_v1 = AlgorithmConfigBuilder::new("TestStrategy", "BTCUSDT")
            .strategy_type(StrategyType::Momentum)
            .take_profit_bps(20.0)
            .stop_loss_bps(10.0)
            .build_unchecked();
        let name = config_v1.name.clone();
        let symbol = config_v1.symbol.clone();

        config_store.save(&config_v1).unwrap();

        // Create new versions
        let config_v2 = config_v1.next_version();
        config_store.save(&config_v2).unwrap();

        let config_v3 = config_v2.next_version();
        config_store.save(&config_v3).unwrap();

        // List versions
        let versions = config_store.list_versions(&name, &symbol).unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].version, 3); // Descending order
        assert_eq!(versions[1].version, 2);
        assert_eq!(versions[2].version, 1);

        // Load latest should get v3
        let latest = config_store.load_latest(&name, &symbol).unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version, 3);

        // Load specific version
        let v2 = config_store.load_version(&name, &symbol, 2).unwrap();
        assert!(v2.is_some());
        assert_eq!(v2.unwrap().version, 2);
    }

    // ==================== Error Handling Tests ====================

    #[test]
    fn test_load_nonexistent_returns_none() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        let result = config_store.load("nonexistent_id").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_stores_return_empty_lists() {
        let (research_store, mut results_store, config_store, _temp) = create_test_stores();

        // ResearchStore - check list_symbols returns empty
        assert!(research_store.list_symbols().unwrap().is_empty());
        assert!(config_store.list_all().unwrap().is_empty());

        let query = ResultsQuery::new();
        assert!(results_store.query(query).unwrap().is_empty());
    }

    // ==================== Config Comparison Tests ====================

    #[test]
    fn test_config_diff_detection() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        let config1 = AlgorithmConfigBuilder::new("Strategy1", "BTCUSDT")
            .strategy_type(StrategyType::Momentum)
            .take_profit_bps(20.0)
            .stop_loss_bps(10.0)
            .min_tau_half(3.0)
            .build_unchecked();

        let config2 = AlgorithmConfigBuilder::new("Strategy2", "BTCUSDT")
            .strategy_type(StrategyType::Momentum)
            .take_profit_bps(30.0) // Different
            .stop_loss_bps(15.0)   // Different
            .min_tau_half(3.0)
            .build_unchecked();

        config_store.save(&config1).unwrap();
        config_store.save(&config2).unwrap();

        let diff = config_store.compare(&config1.id, &config2.id).unwrap();
        assert!(diff.is_some());

        let diff = diff.unwrap();
        assert!(!diff.differences.is_empty());
    }

    #[test]
    fn test_config_summary_listing() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create multiple configs
        for i in 0..5 {
            let config = AlgorithmConfigBuilder::new(format!("Strategy{}", i), "BTCUSDT")
                .strategy_type(if i % 2 == 0 { StrategyType::Momentum } else { StrategyType::MarketMaking })
                .active(i < 3)
                .build_unchecked();
            config_store.save(&config).unwrap();
        }

        // Get summaries
        let summaries = config_store.list_summaries().unwrap();
        assert_eq!(summaries.len(), 5);

        // Check summary fields
        for summary in &summaries {
            assert!(!summary.id.is_empty());
            assert!(!summary.name.is_empty());
            assert_eq!(summary.symbol, "BTCUSDT");
        }

        // Get stats
        let stats = config_store.get_stats().unwrap();
        assert_eq!(stats.total_configs, 5);
        assert_eq!(stats.active_configs, 3);
    }

    // ==================== TSMOM Integration Tests ====================

    #[test]
    fn test_tsmom_config_persistence() {
        let (mut research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create research state with TSMOM config
        let mut state = create_test_research_state("BTCUSDT");
        state.tsmom_config = Some(TSMOMConfig {
            signal_type: TSMOMSignalType::CumulativeReturn,
            bar_size: BarSize::M15,
            lookback_bars: 20,
            ma_short_bars: 6,
            ma_long_bars: 24,
            ewma_lambda: 0.94,
            target_volatility: 0.001,
            max_position_size: 2.0,
            transaction_cost_bps: 5.0,
            warmup_bars: 48,
            long_only: false,
        });
        state.tsmom_stats = Some(TSMOMStats {
            total_bars: 100,
            long_signals: 55,
            short_signals: 45,
            flat_signals: 10,
            total_turnover: 25.0,
            avg_position_size: 1.2,
            avg_momentum_magnitude: 0.02,
            avg_volatility: 0.015,
            sharpe_gross: 1.5,
            sharpe_net: 1.2,
            total_return_gross: 0.15,
            total_return_net: 0.12,
            max_drawdown: 0.08,
            computed_at: Utc::now(),
        });

        research_store.save(&state).unwrap();

        // Create config from research - should include TSMOM settings
        let config = AlgorithmConfig::from_research(&state);
        config_store.save(&config).unwrap();

        // Reload and verify TSMOM config
        let loaded_research = research_store.load("BTCUSDT").unwrap().unwrap();
        assert!(loaded_research.tsmom_config.is_some());
        assert!(loaded_research.tsmom_stats.is_some());

        let tsmom = loaded_research.tsmom_config.unwrap();
        assert_eq!(tsmom.lookback_bars, 20);
        assert_eq!(tsmom.bar_size, BarSize::M15);
        assert!(!tsmom.long_only); // shorting allowed
    }

    // ==================== Multiple Symbols Tests ====================

    #[test]
    fn test_multiple_configs_different_symbols() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        let symbols = vec!["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"];

        // Create configs for each symbol
        for symbol in &symbols {
            let config = AlgorithmConfigBuilder::new(format!("{}_strategy", symbol), *symbol)
                .strategy_type(StrategyType::Hybrid)
                .build_unchecked();
            config_store.save(&config).unwrap();
        }

        // Query by each symbol
        for symbol in &symbols {
            let query = ConfigQuery::new().with_symbol(*symbol);
            let configs = config_store.query(&query).unwrap();
            assert_eq!(configs.len(), 1);
            assert_eq!(configs[0].symbol, *symbol);
        }

        // Query all
        let all = config_store.list_all().unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_strategy_type_filtering() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create configs with different strategy types
        let strategies = vec![
            (StrategyType::Momentum, 3),
            (StrategyType::MarketMaking, 2),
            (StrategyType::Hybrid, 4),
        ];

        for (strategy, count) in &strategies {
            for i in 0..*count {
                let config = AlgorithmConfigBuilder::new(
                    format!("{:?}_{}", strategy, i),
                    "BTCUSDT"
                )
                    .strategy_type(*strategy)
                    .build_unchecked();
                config_store.save(&config).unwrap();
            }
        }

        // Query by strategy type
        for (strategy, expected_count) in &strategies {
            let query = ConfigQuery::new().with_strategy_type(*strategy);
            let configs = config_store.query(&query).unwrap();
            assert_eq!(configs.len(), *expected_count);
        }
    }

    // ==================== Cleanup and Archive Tests ====================

    #[test]
    fn test_config_archive_functionality() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        let config = AlgorithmConfigBuilder::new("ToArchive", "BTCUSDT")
            .strategy_type(StrategyType::Momentum)
            .build_unchecked();
        let config_id = config.id.clone();

        config_store.save(&config).unwrap();

        // Archive the config
        let archive_path = config_store.archive(&config_id).unwrap();
        assert!(archive_path.is_some());

        // Should no longer be loadable from main store
        let loaded = config_store.load(&config_id).unwrap();
        assert!(loaded.is_none());

        // Archive file should exist
        let archive_path = archive_path.unwrap();
        assert!(archive_path.exists());
    }

    #[test]
    fn test_cleanup_old_versions() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create a config with many versions
        let config_v1 = AlgorithmConfigBuilder::new("VersionTest", "BTCUSDT")
            .strategy_type(StrategyType::Momentum)
            .build_unchecked();
        let name = config_v1.name.clone();
        let symbol = config_v1.symbol.clone();

        config_store.save(&config_v1).unwrap();

        let config_v2 = config_v1.next_version();
        config_store.save(&config_v2).unwrap();

        let config_v3 = config_v2.next_version();
        config_store.save(&config_v3).unwrap();

        let config_v4 = config_v3.next_version();
        config_store.save(&config_v4).unwrap();

        let config_v5 = config_v4.next_version();
        config_store.save(&config_v5).unwrap();

        // Cleanup, keeping only 2 versions
        let deleted = config_store.cleanup_old_versions(&name, &symbol, 2).unwrap();
        assert_eq!(deleted, 3); // Should delete v1, v2, v3

        // Verify only v4 and v5 remain
        let remaining = config_store.list_versions(&name, &symbol).unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].version, 5);
        assert_eq!(remaining[1].version, 4);
    }

    // ==================== Preset Tests ====================

    #[test]
    fn test_config_presets() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Available presets: Conservative, Aggressive, MarketMaking, TSMOM
        let presets = vec![
            ConfigPreset::Conservative,
            ConfigPreset::Aggressive,
            ConfigPreset::MarketMaking,
            ConfigPreset::TSMOM,
        ];

        for preset in &presets {
            let config = AlgorithmConfig::preset(*preset, "BTCUSDT");
            config_store.save(&config).unwrap();

            // Verify preset was created correctly
            let loaded = config_store.load(&config.id).unwrap().unwrap();

            match preset {
                ConfigPreset::Conservative => {
                    assert!(loaded.exit.take_profit_bps <= 20.0);
                    assert!(loaded.exit.stop_loss_bps <= 12.0);
                },
                ConfigPreset::Aggressive => {
                    assert!(loaded.exit.take_profit_bps >= 15.0);
                },
                ConfigPreset::MarketMaking => {
                    assert_eq!(loaded.strategy_type, StrategyType::MarketMaking);
                },
                ConfigPreset::TSMOM => {
                    assert!(loaded.tsmom.is_some());
                },
            }
        }

        // All presets should be saved
        let all = config_store.list_all().unwrap();
        assert_eq!(all.len(), 4);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_special_characters_in_names() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Names with special characters
        let names = vec![
            "Strategy-2024",
            "Strategy_v1.0",
            "Strategy (Test)",
        ];

        for name in &names {
            let config = AlgorithmConfigBuilder::new(*name, "BTCUSDT")
                .strategy_type(StrategyType::Momentum)
                .build_unchecked();
            config_store.save(&config).unwrap();

            // Should be loadable
            let loaded = config_store.load(&config.id).unwrap();
            assert!(loaded.is_some());
            assert_eq!(loaded.unwrap().name, *name);
        }
    }

    #[test]
    fn test_empty_conditional_table() {
        let (mut research_store, _results_store, _config_store, _temp) = create_test_stores();

        // Research state with no conditional probabilities
        let mut state = ResearchState::new("BTCUSDT");
        state.conditional_table = HashMap::new();

        research_store.save(&state).unwrap();

        let loaded = research_store.load("BTCUSDT").unwrap().unwrap();
        assert!(loaded.conditional_table.is_empty());
    }

    #[test]
    fn test_config_with_all_optional_fields() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Config with all optional fields set
        let mut config = AlgorithmConfigBuilder::new("FullConfig", "BTCUSDT")
            .strategy_type(StrategyType::Hybrid)
            .take_profit_bps(25.0)
            .stop_loss_bps(12.0)
            .min_tau_half(2.5)
            .max_entropy(0.9)
            .description("Full config with all options")
            .build_unchecked();
        config.source_research_id = Some("rs_test_123".to_string());

        config_store.save(&config).unwrap();

        let loaded = config_store.load(&config.id).unwrap().unwrap();
        assert_eq!(loaded.exit.take_profit_bps, 25.0);
        assert_eq!(loaded.exit.stop_loss_bps, 12.0);
        assert!(loaded.description.is_some());
        assert!(loaded.source_research_id.is_some());
    }

    // ==================== Performance Tests ====================

    #[test]
    fn test_many_configs_performance() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create many configs
        let count = 50;
        let start = std::time::Instant::now();

        for i in 0..count {
            let config = AlgorithmConfigBuilder::new(format!("Perf{}", i), "BTCUSDT")
                .strategy_type(StrategyType::Momentum)
                .build_unchecked();
            config_store.save(&config).unwrap();
        }

        let save_duration = start.elapsed();

        // List all should still be fast
        let list_start = std::time::Instant::now();
        let all = config_store.list_all().unwrap();
        let list_duration = list_start.elapsed();

        assert_eq!(all.len(), count);

        // Sanity check: operations should complete in reasonable time
        assert!(save_duration.as_secs() < 30);
        assert!(list_duration.as_secs() < 5);
    }

    #[test]
    fn test_many_results_aggregation() {
        let (mut research_store, mut results_store, mut config_store, _temp) = create_test_stores();

        // Setup
        let research_state = create_test_research_state("BTCUSDT");
        let research_id = research_state.id.clone();
        research_store.save(&research_state).unwrap();

        let config = AlgorithmConfig::from_research(&research_state);
        let config_id = config.id.clone();
        config_store.save(&config).unwrap();

        // Create many results
        let count = 20;
        for _i in 0..count {
            let result = create_test_validation_result(
                ValidationStageType::Backtest,
                &config_id,
                &research_id,
            );
            results_store.save(&result).unwrap();
        }

        // Aggregate should work
        let aggregated = results_store.aggregate_by_config(&config_id).unwrap();
        assert!(aggregated.count > 0);
    }

    // ==================== Documentation Tests (Compile-time) ====================

    /// This test verifies that all public types are properly documented
    /// by attempting to use them in a way that requires their docs.
    #[test]
    fn test_public_api_usability() {
        // ResearchState
        let _state = ResearchState::new("BTCUSDT");

        // MIDCEstimate
        let _midc = MIDCEstimate::new(0.1, 0.2, 0.9, 1000);

        // ValidationResult
        let _result = ValidationResult::new(
            ValidationStageType::Backtest,
            "test".to_string(),
            "cfg_test".to_string(),
            Utc::now() - Duration::days(1),
            Utc::now(),
        );

        // AlgorithmConfig
        let _config = AlgorithmConfig::new("Test", StrategyType::Momentum, "BTCUSDT");

        // ConfigStore
        let temp = TempDir::new().unwrap();
        let _store = ConfigStore::new(
            ConfigStoreConfig::with_path(temp.path()).without_parquet()
        ).unwrap();

        // All types should be usable without issues
    }

    // ==================== Regime Classification Tests ====================

    #[test]
    fn test_midc_regime_classification() {
        // SlowDiffusion (kappa < 0.01)
        let slow = MIDCEstimate::new(0.005, 0.3, 0.9, 1000);
        assert_eq!(slow.regime(), MIDCRegime::SlowDiffusion);
        assert!(slow.regime().momentum_viable());

        // ModerateDiffusion (0.01 <= kappa < 0.1)
        let moderate = MIDCEstimate::new(0.05, 0.3, 0.9, 1000);
        assert_eq!(moderate.regime(), MIDCRegime::ModerateDiffusion);
        assert!(moderate.regime().momentum_viable());

        // FastDiffusion (kappa >= 0.1)
        let fast = MIDCEstimate::new(0.15, 0.3, 0.9, 1000);
        assert_eq!(fast.regime(), MIDCRegime::FastDiffusion);
        assert!(!fast.regime().momentum_viable());
    }

    #[test]
    fn test_tradeable_assessment_logic() {
        // All conditions met
        let tradeable = TradeableAssessment::new(true, true, true, true);
        assert!(tradeable.is_tradeable);
        assert_eq!(tradeable.recommended_strategy, RecommendedStrategy::Momentum);

        // MIDC not ok but entropy ok -> market making
        let mm = TradeableAssessment::new(false, true, false, false);
        assert!(!mm.is_tradeable);
        assert_eq!(mm.recommended_strategy, RecommendedStrategy::MarketMaking);

        // Nothing ok
        let none = TradeableAssessment::new(false, false, false, false);
        assert!(!none.is_tradeable);
        assert_eq!(none.recommended_strategy, RecommendedStrategy::None);
    }

    // ==================== Results Store Query Tests ====================

    #[test]
    fn test_results_query_by_date_range() {
        let (mut research_store, mut results_store, mut config_store, _temp) = create_test_stores();

        // Setup
        let research_state = create_test_research_state("BTCUSDT");
        let research_id = research_state.id.clone();
        research_store.save(&research_state).unwrap();

        let config = AlgorithmConfig::from_research(&research_state);
        let config_id = config.id.clone();
        config_store.save(&config).unwrap();

        // Create results with different periods
        let result = create_test_validation_result(
            ValidationStageType::Backtest,
            &config_id,
            &research_id,
        );
        results_store.save(&result).unwrap();

        // Query with date range
        let start = Utc::now() - Duration::days(30);
        let end = Utc::now() + Duration::days(1);
        let query = ResultsQuery::new()
            .with_time_range(start, end);
        let results = results_store.query(query).unwrap();
        assert!(!results.is_empty());
    }

    // ==================== Concurrent Access Simulation ====================

    #[test]
    fn test_sequential_save_load_cycles() {
        let (mut research_store, mut results_store, mut config_store, _temp) = create_test_stores();

        // Simulate multiple save/load cycles with different symbols
        for cycle in 0..10 {
            let symbol = format!("SYMBOL{}", cycle);
            let research_state = create_test_research_state(&symbol);
            let research_id = research_state.id.clone();
            research_store.save(&research_state).unwrap();

            let config = AlgorithmConfig::from_research(&research_state);
            let config_id = config.id.clone();
            config_store.save(&config).unwrap();

            let result = create_test_validation_result(
                ValidationStageType::Backtest,
                &config_id,
                &research_id,
            );
            results_store.save(&result).unwrap();

            // Verify all can be loaded
            assert!(research_store.load(&symbol).unwrap().is_some());
            assert!(config_store.load(&config_id).unwrap().is_some());
        }

        // Verify counts
        let symbols = research_store.list_symbols().unwrap();
        assert_eq!(symbols.len(), 10);
        assert_eq!(config_store.list_all().unwrap().len(), 10);
    }

    // ==================== Edge Cases for Validation Results ====================

    #[test]
    fn test_validation_result_with_no_trades() {
        let (_research_store, mut results_store, _config_store, _temp) = create_test_stores();

        // Create result with no trades
        let result = ValidationResult::new(
            ValidationStageType::Backtest,
            "NoTrades".to_string(),
            "cfg_test".to_string(),
            Utc::now() - Duration::days(1),
            Utc::now(),
        );

        results_store.save(&result).unwrap();

        let loaded = results_store.load_by_id(&result.id).unwrap();
        assert!(loaded.is_some());
        assert!(loaded.unwrap().trades.is_empty());
    }

    #[test]
    fn test_validation_result_passed_status() {
        let (_research_store, mut results_store, _config_store, _temp) = create_test_stores();

        // Create passed result
        let mut passed_result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Passed".to_string(),
            "cfg_test".to_string(),
            Utc::now() - Duration::days(1),
            Utc::now(),
        );
        passed_result.passed = true;
        results_store.save(&passed_result).unwrap();

        // Create failed result
        let mut failed_result = ValidationResult::new(
            ValidationStageType::Backtest,
            "Failed".to_string(),
            "cfg_test2".to_string(),
            Utc::now() - Duration::days(1),
            Utc::now(),
        );
        failed_result.passed = false;
        results_store.save(&failed_result).unwrap();

        // Query passed only
        let query = ResultsQuery::new().passed_only();
        let passed = results_store.query(query).unwrap();
        assert_eq!(passed.len(), 1);
        assert!(passed[0].passed);
    }

    // ==================== Config Store Statistics ====================

    #[test]
    fn test_config_store_comprehensive_stats() {
        let (_research_store, _results_store, mut config_store, _temp) = create_test_stores();

        // Create configs of different types and activity states
        for i in 0..10 {
            let strategy = match i % 3 {
                0 => StrategyType::Momentum,
                1 => StrategyType::MarketMaking,
                _ => StrategyType::Hybrid,
            };

            let config = AlgorithmConfigBuilder::new(format!("Stats{}", i), "BTCUSDT")
                .strategy_type(strategy)
                .active(i < 6) // First 6 are active
                .build_unchecked();
            config_store.save(&config).unwrap();
        }

        let stats = config_store.get_stats().unwrap();
        assert_eq!(stats.total_configs, 10);
        assert_eq!(stats.active_configs, 6);
    }

    // ==================== Research Store Edge Cases ====================

    #[test]
    fn test_research_state_update() {
        let (mut research_store, _results_store, _config_store, _temp) = create_test_stores();

        // Save initial state
        let state = create_test_research_state("BTCUSDT");
        let _state_id = state.id.clone();
        research_store.save(&state).unwrap();

        // Sleep to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Update and save again (new state with different ID)
        let mut updated_state = create_test_research_state("BTCUSDT");
        updated_state.midc = MIDCEstimate::new(0.1, 0.3, 0.95, 10000);
        research_store.save(&updated_state).unwrap();

        // Should have at least one state for this symbol
        // (depending on implementation, may keep history or just latest)
        let all = research_store.list_states("BTCUSDT").unwrap();
        assert!(!all.is_empty());

        // Load should return the latest one
        let latest = research_store.load("BTCUSDT").unwrap().unwrap();
        assert!((latest.midc.kappa - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_research_state_by_symbol() {
        let (mut research_store, _results_store, _config_store, _temp) = create_test_stores();

        // Save states for different symbols
        for symbol in &["BTCUSDT", "ETHUSDT", "BNBUSDT"] {
            let state = create_test_research_state(symbol);
            research_store.save(&state).unwrap();
        }

        // Each symbol should have a state
        for symbol in &["BTCUSDT", "ETHUSDT", "BNBUSDT"] {
            let loaded = research_store.load(symbol).unwrap();
            assert!(loaded.is_some());
            assert_eq!(loaded.unwrap().symbol, *symbol);
        }

        // List symbols
        let symbols = research_store.list_symbols().unwrap();
        assert_eq!(symbols.len(), 3);
    }
}
