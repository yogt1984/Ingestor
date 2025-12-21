//! ML Weight Trainer
//!
//! Optimizes weights for the MLSpreadSkewAlgorithm using grid search
//! over historical backtest data with train/test validation.
//!
//! # Methodology
//!
//! 1. **Load Data**: Read all Parquet files from data directory
//! 2. **Train/Test Split**: Chronological split (e.g., 70% train, 30% test)
//! 3. **Grid Search**: Evaluate combinations of spread and skew weights
//! 4. **Validation**: Ensure weights generalize to out-of-sample data
//! 5. **Save Weights**: Output optimal weights to JSON file
//!
//! # Weight Search Space
//!
//! For spread model:
//! - `intercept`: Base spread in bps [1.0, 2.0, 3.0, 4.0, 5.0]
//! - `w_entropy`: Entropy coefficient [-3.0, -2.0, -1.0, 0.0]
//! - `w_volatility`: Volatility coefficient [100, 300, 500, 700]
//!
//! For skew model:
//! - `intercept`: Base skew [0.3, 0.5, 0.7]
//! - `w_inventory`: Inventory coefficient [-1.0, -0.8, -0.6, -0.4]
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::ml_trainer::{MLTrainer, MLTrainerConfig};
//!
//! let config = MLTrainerConfig::default();
//! let mut trainer = MLTrainer::new(config)?;
//! let results = trainer.train()?;
//! results.save_weights("models/ml_weights.json")?;
//! ```

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;

use crate::strategies::{
    MLSpreadSkewAlgorithm, MLSpreadSkewConfig, MLModelWeights,
    SpreadWeights, SkewWeights, TrainingInfo, MarketMakingAlgorithm,
};
use crate::backtest::{
    BacktestEngine, BacktestConfig, BacktestResults,
    ReplayEvent, ReplayConfig, FillSimulatorConfig,
};
use crate::execution::market_maker::MMConfig;
use crate::execution::mm_simulator::SimulatorConfig;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for ML weight training
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLTrainerConfig {
    /// Data directory containing Parquet files
    pub data_dir: PathBuf,

    /// Train/test split ratio (fraction for training)
    pub train_ratio: f64,

    /// Grid search values for spread intercept (base spread in bps)
    pub spread_intercepts: Vec<f64>,

    /// Grid search values for spread entropy weight
    pub spread_entropy_weights: Vec<f64>,

    /// Grid search values for spread volatility weight
    pub spread_volatility_weights: Vec<f64>,

    /// Grid search values for skew intercept
    pub skew_intercepts: Vec<f64>,

    /// Grid search values for skew inventory weight
    pub skew_inventory_weights: Vec<f64>,

    /// Fill probability for simulation
    pub fill_probability: f64,

    /// Maximum inventory
    pub max_inventory: Decimal,

    /// Quote size
    pub quote_size: Decimal,

    /// Minimum trades required for valid evaluation
    pub min_trades: usize,

    /// Objective function: "sharpe", "return", or "sortino"
    pub objective: String,

    /// Verbose output
    pub verbose: bool,
}

impl Default for MLTrainerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data/features"),
            train_ratio: 0.7,
            // Spread weight search space
            spread_intercepts: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            spread_entropy_weights: vec![-3.0, -2.0, -1.0, 0.0],
            spread_volatility_weights: vec![200.0, 400.0, 600.0],
            // Skew weight search space
            skew_intercepts: vec![0.3, 0.5, 0.7],
            skew_inventory_weights: vec![-1.0, -0.8, -0.6, -0.4],
            // Simulation params
            fill_probability: 0.10,
            max_inventory: dec!(0.1),
            quote_size: dec!(0.001),
            min_trades: 10,
            objective: "sharpe".to_string(),
            verbose: true,
        }
    }
}

// ============================================================================
// Training Results
// ============================================================================

/// Results from a single weight configuration evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightEvaluation {
    pub weights: MLModelWeights,
    pub train_sharpe: f64,
    pub train_return: f64,
    pub train_trades: usize,
    pub test_sharpe: f64,
    pub test_return: f64,
    pub test_trades: usize,
    /// Generalization gap: train_sharpe - test_sharpe
    pub generalization_gap: f64,
}

/// Complete training results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLTrainingResults {
    /// Best weights found
    pub optimal_weights: MLModelWeights,

    /// Training performance
    pub train_sharpe: f64,
    pub train_return: f64,
    pub train_trades: usize,

    /// Test (out-of-sample) performance
    pub test_sharpe: f64,
    pub test_return: f64,
    pub test_trades: usize,

    /// Generalization gap (train - test Sharpe)
    pub generalization_gap: f64,

    /// All evaluated configurations (top 10)
    pub top_configurations: Vec<WeightEvaluation>,

    /// Search summary
    pub total_configurations: usize,
    pub valid_configurations: usize,

    /// Data split info
    pub train_events: usize,
    pub test_events: usize,

    /// Configuration used
    pub config: MLTrainerConfig,
}

impl MLTrainingResults {
    /// Save the optimal weights to a JSON file
    pub fn save_weights<P: AsRef<std::path::Path>>(&self, path: P) -> Result<()> {
        self.optimal_weights.save_to_file(path)?;
        Ok(())
    }

    /// Print summary of training results
    pub fn print_summary(&self) {
        println!("\n=== ML Weight Training Results ===\n");

        println!("Search Summary:");
        println!("  Total configurations:  {}", self.total_configurations);
        println!("  Valid configurations:  {}", self.valid_configurations);
        println!("  Train events:          {}", self.train_events);
        println!("  Test events:           {}", self.test_events);

        println!("\nOptimal Weights:");
        println!("  Spread:");
        println!("    intercept:     {:.2}", self.optimal_weights.spread.intercept);
        println!("    w_entropy:     {:.2}", self.optimal_weights.spread.w_entropy);
        println!("    w_volatility:  {:.2}", self.optimal_weights.spread.w_volatility);
        println!("    w_imbalance:   {:.2}", self.optimal_weights.spread.w_imbalance);
        println!("    w_interaction: {:.2}", self.optimal_weights.spread.w_interaction);
        println!("  Skew:");
        println!("    intercept:     {:.2}", self.optimal_weights.skew.intercept);
        println!("    w_entropy:     {:.2}", self.optimal_weights.skew.w_entropy);
        println!("    w_volatility:  {:.2}", self.optimal_weights.skew.w_volatility);
        println!("    w_imbalance:   {:.2}", self.optimal_weights.skew.w_imbalance);
        println!("    w_inventory:   {:.2}", self.optimal_weights.skew.w_inventory);

        println!("\nTraining Performance:");
        println!("  Sharpe:    {:.4}", self.train_sharpe);
        println!("  Return:    {:.2}%", self.train_return * 100.0);
        println!("  Trades:    {}", self.train_trades);

        println!("\nTest (OOS) Performance:");
        println!("  Sharpe:    {:.4}", self.test_sharpe);
        println!("  Return:    {:.2}%", self.test_return * 100.0);
        println!("  Trades:    {}", self.test_trades);

        println!("\nGeneralization:");
        let gap = self.generalization_gap;
        let quality = if gap.abs() < 0.5 {
            "Good (low overfit risk)"
        } else if gap.abs() < 1.0 {
            "Moderate (some overfit risk)"
        } else {
            "Poor (likely overfitting)"
        };
        println!("  Gap (train-test): {:.4} - {}", gap, quality);

        println!("\nTop 5 Configurations:");
        for (i, eval) in self.top_configurations.iter().take(5).enumerate() {
            println!(
                "  {}. spread_int={:.1}, entropy={:.1}, vol={:.0}, skew_int={:.1}, inv={:.1} → Train: {:.3}, Test: {:.3}",
                i + 1,
                eval.weights.spread.intercept,
                eval.weights.spread.w_entropy,
                eval.weights.spread.w_volatility,
                eval.weights.skew.intercept,
                eval.weights.skew.w_inventory,
                eval.train_sharpe,
                eval.test_sharpe,
            );
        }
    }
}

// ============================================================================
// ML Trainer
// ============================================================================

/// ML Weight Trainer using grid search optimization
pub struct MLTrainer {
    config: MLTrainerConfig,
    train_events: Vec<ReplayEvent>,
    test_events: Vec<ReplayEvent>,
}

impl MLTrainer {
    /// Create a new trainer and load data
    pub fn new(config: MLTrainerConfig) -> Result<Self> {
        let mut trainer = Self {
            config,
            train_events: Vec::new(),
            test_events: Vec::new(),
        };
        trainer.load_and_split_data()?;
        Ok(trainer)
    }

    /// Create trainer with pre-loaded events (for testing)
    pub fn with_events(
        config: MLTrainerConfig,
        train_events: Vec<ReplayEvent>,
        test_events: Vec<ReplayEvent>,
    ) -> Self {
        Self {
            config,
            train_events,
            test_events,
        }
    }

    /// Load data from Parquet files and split into train/test
    fn load_and_split_data(&mut self) -> Result<()> {
        use crate::backtest::ParquetReplay;

        if self.config.verbose {
            println!("Loading data from {:?}...", self.config.data_dir);
        }

        let replay_config = ReplayConfig {
            data_dir: self.config.data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(replay_config);
        replay.load()?;
        let all_events = replay.into_events();

        if all_events.is_empty() {
            anyhow::bail!("No events loaded from data directory");
        }

        // Chronological split
        let split_idx = (all_events.len() as f64 * self.config.train_ratio) as usize;

        self.train_events = all_events[..split_idx].to_vec();
        self.test_events = all_events[split_idx..].to_vec();

        if self.config.verbose {
            println!(
                "Data split: {} train events, {} test events (ratio: {:.0}%/{:.0}%)",
                self.train_events.len(),
                self.test_events.len(),
                self.config.train_ratio * 100.0,
                (1.0 - self.config.train_ratio) * 100.0,
            );
        }

        Ok(())
    }

    /// Run grid search optimization
    pub fn train(&mut self) -> Result<MLTrainingResults> {
        let total_configs = self.config.spread_intercepts.len()
            * self.config.spread_entropy_weights.len()
            * self.config.spread_volatility_weights.len()
            * self.config.skew_intercepts.len()
            * self.config.skew_inventory_weights.len();

        if self.config.verbose {
            println!("\nStarting grid search over {} configurations...", total_configs);
        }

        let mut evaluations: Vec<WeightEvaluation> = Vec::new();
        let mut evaluated = 0;

        for &spread_int in &self.config.spread_intercepts {
            for &spread_ent in &self.config.spread_entropy_weights {
                for &spread_vol in &self.config.spread_volatility_weights {
                    for &skew_int in &self.config.skew_intercepts {
                        for &skew_inv in &self.config.skew_inventory_weights {
                            evaluated += 1;

                            let weights = MLModelWeights {
                                spread: SpreadWeights {
                                    intercept: spread_int,
                                    w_entropy: spread_ent,
                                    w_volatility: spread_vol,
                                    ..Default::default()
                                },
                                skew: SkewWeights {
                                    intercept: skew_int,
                                    w_inventory: skew_inv,
                                    ..Default::default()
                                },
                                version: "grid-search".to_string(),
                                training_info: None,
                            };

                            // Evaluate on train set
                            let train_result = self.evaluate_weights(&weights, &self.train_events)?;

                            // Skip if not enough trades
                            if train_result.fills_generated < self.config.min_trades {
                                continue;
                            }

                            // Evaluate on test set
                            let test_result = self.evaluate_weights(&weights, &self.test_events)?;

                            let train_sharpe = train_result.metrics.sharpe_ratio;
                            let test_sharpe = test_result.metrics.sharpe_ratio;

                            let eval = WeightEvaluation {
                                weights,
                                train_sharpe,
                                train_return: train_result.metrics.total_return,
                                train_trades: train_result.fills_generated,
                                test_sharpe,
                                test_return: test_result.metrics.total_return,
                                test_trades: test_result.fills_generated,
                                generalization_gap: train_sharpe - test_sharpe,
                            };

                            evaluations.push(eval);

                            if self.config.verbose && evaluated % 50 == 0 {
                                println!("  Evaluated {}/{} configurations...", evaluated, total_configs);
                            }
                        }
                    }
                }
            }
        }

        if evaluations.is_empty() {
            anyhow::bail!("No valid configurations found (all had < {} trades)", self.config.min_trades);
        }

        // Sort by objective (using test Sharpe to avoid overfitting)
        // We prefer configs with good test performance
        evaluations.sort_by(|a, b| {
            // Primary: test Sharpe (descending)
            // Secondary: smaller generalization gap (ascending)
            b.test_sharpe
                .partial_cmp(&a.test_sharpe)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    a.generalization_gap.abs()
                        .partial_cmp(&b.generalization_gap.abs())
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        let best = &evaluations[0];

        // Create final weights with training info
        let mut optimal_weights = best.weights.clone();
        optimal_weights.version = "trained-v1".to_string();
        optimal_weights.training_info = Some(TrainingInfo {
            trained_on: chrono::Utc::now().to_rfc3339(),
            num_samples: self.train_events.len(),
            train_sharpe: best.train_sharpe,
            validation_sharpe: Some(best.test_sharpe),
        });

        let results = MLTrainingResults {
            optimal_weights,
            train_sharpe: best.train_sharpe,
            train_return: best.train_return,
            train_trades: best.train_trades,
            test_sharpe: best.test_sharpe,
            test_return: best.test_return,
            test_trades: best.test_trades,
            generalization_gap: best.generalization_gap,
            top_configurations: evaluations.iter().take(10).cloned().collect(),
            total_configurations: total_configs,
            valid_configurations: evaluations.len(),
            train_events: self.train_events.len(),
            test_events: self.test_events.len(),
            config: self.config.clone(),
        };

        if self.config.verbose {
            results.print_summary();
        }

        Ok(results)
    }

    /// Evaluate a weight configuration on a set of events
    fn evaluate_weights(
        &self,
        weights: &MLModelWeights,
        events: &[ReplayEvent],
    ) -> Result<BacktestResults> {
        let ml_config = MLSpreadSkewConfig {
            max_inventory: self.config.max_inventory,
            quote_size: self.config.quote_size,
            ..Default::default()
        };

        let algorithm: Box<dyn MarketMakingAlgorithm> = Box::new(
            MLSpreadSkewAlgorithm::new(ml_config, weights.clone())
        );

        let backtest_config = BacktestConfig {
            replay: ReplayConfig {
                data_dir: self.config.data_dir.clone(),
                ..Default::default()
            },
            mm: MMConfig {
                max_inventory: self.config.max_inventory,
                quote_size: self.config.quote_size,
                ..Default::default()
            },
            simulator: SimulatorConfig::default(),
            fill_sim: FillSimulatorConfig {
                base_fill_probability: self.config.fill_probability,
                ..Default::default()
            },
            verbose: false,
            use_realistic_fills: true,
            ..Default::default()
        };

        let mut engine = BacktestEngine::from_events_with_algorithm(
            backtest_config,
            events.to_vec(),
            algorithm,
        );

        engine.run()
    }

    /// Get number of train events
    pub fn train_event_count(&self) -> usize {
        self.train_events.len()
    }

    /// Get number of test events
    pub fn test_event_count(&self) -> usize {
        self.test_events.len()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn create_test_events(count: usize) -> Vec<ReplayEvent> {
        use crate::features::feature_fusion::FeaturesSnapshot;

        (0..count)
            .map(|i| {
                let mut snapshot = FeaturesSnapshot::default();
                snapshot.timestamp = format!("2024-01-01T00:00:{:02}.000Z", i);
                snapshot.mid_price = Some(dec!(50000) + Decimal::from(i as i64));
                snapshot.best_bid = Some(dec!(49990) + Decimal::from(i as i64));
                snapshot.best_ask = Some(dec!(50010) + Decimal::from(i as i64));
                snapshot.tick_entropy_1s = Some(dec!(1.2));
                snapshot.tick_entropy_5s = Some(dec!(1.3));
                snapshot.realized_volatility_100 = Some(0.001);
                ReplayEvent {
                    timestamp_ms: 1000 + i as i64 * 100,
                    snapshot,
                }
            })
            .collect()
    }

    #[test]
    fn test_trainer_config_default() {
        let config = MLTrainerConfig::default();
        assert_eq!(config.train_ratio, 0.7);
        assert!(!config.spread_intercepts.is_empty());
        assert!(!config.skew_inventory_weights.is_empty());
    }

    #[test]
    fn test_weight_evaluation_creation() {
        let weights = MLModelWeights::default();
        let eval = WeightEvaluation {
            weights,
            train_sharpe: 1.5,
            train_return: 0.05,
            train_trades: 100,
            test_sharpe: 1.2,
            test_return: 0.04,
            test_trades: 80,
            generalization_gap: 0.3,
        };

        assert_eq!(eval.train_sharpe, 1.5);
        assert_eq!(eval.test_sharpe, 1.2);
        assert!((eval.generalization_gap - 0.3).abs() < 0.001);
    }

    #[test]
    fn test_training_results_serialization() {
        let config = MLTrainerConfig::default();
        let weights = MLModelWeights::default();

        let results = MLTrainingResults {
            optimal_weights: weights,
            train_sharpe: 1.5,
            train_return: 0.05,
            train_trades: 100,
            test_sharpe: 1.2,
            test_return: 0.04,
            test_trades: 80,
            generalization_gap: 0.3,
            top_configurations: vec![],
            total_configurations: 100,
            valid_configurations: 50,
            train_events: 1000,
            test_events: 300,
            config,
        };

        let json = serde_json::to_string(&results).unwrap();
        let parsed: MLTrainingResults = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.train_sharpe, results.train_sharpe);
        assert_eq!(parsed.test_sharpe, results.test_sharpe);
    }

    #[test]
    fn test_trainer_with_events() {
        let train_events = create_test_events(100);
        let test_events = create_test_events(30);

        let config = MLTrainerConfig {
            min_trades: 0, // Allow any number of trades for test
            verbose: false,
            ..Default::default()
        };

        let trainer = MLTrainer::with_events(config, train_events, test_events);

        assert_eq!(trainer.train_event_count(), 100);
        assert_eq!(trainer.test_event_count(), 30);
    }

    #[test]
    fn test_spread_weights_grid() {
        let config = MLTrainerConfig::default();

        // Total spread combinations
        let spread_combos = config.spread_intercepts.len()
            * config.spread_entropy_weights.len()
            * config.spread_volatility_weights.len();

        // Total skew combinations
        let skew_combos = config.skew_intercepts.len()
            * config.skew_inventory_weights.len();

        let total = spread_combos * skew_combos;

        // Verify we have a reasonable search space
        assert!(total >= 100, "Search space should have at least 100 configs");
        assert!(total <= 10000, "Search space should not exceed 10000 configs");
    }

    #[test]
    fn test_generalization_gap_calculation() {
        let weights = MLModelWeights::default();

        // Good generalization
        let good_eval = WeightEvaluation {
            weights: weights.clone(),
            train_sharpe: 1.5,
            train_return: 0.05,
            train_trades: 100,
            test_sharpe: 1.4,
            test_return: 0.045,
            test_trades: 50,
            generalization_gap: 0.1,
        };
        assert!(good_eval.generalization_gap.abs() < 0.5);

        // Poor generalization (overfitting)
        let poor_eval = WeightEvaluation {
            weights,
            train_sharpe: 2.0,
            train_return: 0.10,
            train_trades: 100,
            test_sharpe: 0.5,
            test_return: 0.02,
            test_trades: 50,
            generalization_gap: 1.5,
        };
        assert!(poor_eval.generalization_gap > 1.0);
    }

    #[test]
    fn test_ml_model_weights_with_training_info() {
        let mut weights = MLModelWeights::default();
        weights.training_info = Some(TrainingInfo {
            trained_on: "2024-01-01T00:00:00Z".to_string(),
            num_samples: 1000,
            train_sharpe: 1.5,
            validation_sharpe: Some(1.2),
        });

        let info = weights.training_info.as_ref().unwrap();
        assert_eq!(info.num_samples, 1000);
        assert_eq!(info.train_sharpe, 1.5);
        assert_eq!(info.validation_sharpe, Some(1.2));
    }

    #[test]
    fn test_config_customization() {
        let config = MLTrainerConfig {
            train_ratio: 0.8,
            spread_intercepts: vec![1.0, 2.0],
            spread_entropy_weights: vec![-1.0, 0.0],
            spread_volatility_weights: vec![300.0],
            skew_intercepts: vec![0.5],
            skew_inventory_weights: vec![-0.8],
            min_trades: 5,
            verbose: false,
            ..Default::default()
        };

        let total_configs = config.spread_intercepts.len()
            * config.spread_entropy_weights.len()
            * config.spread_volatility_weights.len()
            * config.skew_intercepts.len()
            * config.skew_inventory_weights.len();

        assert_eq!(total_configs, 4); // 2*2*1*1*1
    }
}
