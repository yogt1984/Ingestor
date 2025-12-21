//! Parameter Sweep Module
//!
//! Parameter sensitivity analysis via spread/skew sweeps.
//! Shared implementation for both CLI and TUI interfaces.
//!
//! # Features
//!
//! - Configurable spread and skew ranges
//! - Progress callbacks for UI integration
//! - Sorted results by Sharpe ratio
//! - JSON serialization for persistence
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::sweep::{SweepEngine, SweepConfig, SweepResult};
//!
//! let config = SweepConfig {
//!     spreads: vec![1.0, 2.0, 3.0],
//!     skews: vec![0.3, 0.5, 0.7],
//!     ..Default::default()
//! };
//!
//! let engine = SweepEngine::new(config, replay_config);
//! let results = engine.run()?;
//!
//! // Best result by Sharpe
//! if let Some(best) = results.best() {
//!     println!("Best: spread={}, skew={}, sharpe={}", best.spread, best.skew, best.sharpe);
//! }
//! ```

use std::path::PathBuf;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use crate::backtest::{BacktestEngine, BacktestConfig, FillSimulatorConfig};
use crate::backtest::replay::ReplayConfig;
use crate::execution::market_maker::{MMConfig, RegimeParams};
use crate::execution::mm_simulator::SimulatorConfig;

/// Configuration for parameter sweep
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepConfig {
    /// Spread values to test (in basis points)
    pub spreads: Vec<f64>,
    /// Skew values to test
    pub skews: Vec<f64>,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fee rate
    pub fee_rate: f64,
    /// Base fill probability
    pub fill_prob: f64,
    /// Queue position for fill simulation
    pub queue_position: f64,
    /// Use realistic fill simulation
    pub use_realistic_fills: bool,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            spreads: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            skews: vec![0.3, 0.5, 0.7, 1.0],
            max_inventory: 0.1,
            quote_size: 0.001,
            fee_rate: 0.0001,
            fill_prob: 0.10,
            queue_position: 0.5,
            use_realistic_fills: true,
        }
    }
}

impl SweepConfig {
    /// Create config from comma-separated strings (CLI compatibility)
    pub fn from_strings(spreads_str: &str, skews_str: &str) -> Self {
        let spreads: Vec<f64> = spreads_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skews: Vec<f64> = skews_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        Self {
            spreads: if spreads.is_empty() { Self::default().spreads } else { spreads },
            skews: if skews.is_empty() { Self::default().skews } else { skews },
            ..Default::default()
        }
    }

    /// Total number of parameter combinations
    pub fn total_combinations(&self) -> usize {
        self.spreads.len() * self.skews.len()
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.spreads.is_empty() {
            return Err("spreads cannot be empty".to_string());
        }
        if self.skews.is_empty() {
            return Err("skews cannot be empty".to_string());
        }

        for &spread in &self.spreads {
            if spread < 0.1 {
                return Err(format!("spread {} is too low (min 0.1 bps)", spread));
            }
            if spread < 0.0 {
                return Err(format!("spread {} cannot be negative", spread));
            }
            if spread > 100.0 {
                return Err(format!("spread {} is too high (max 100 bps)", spread));
            }
        }

        for &skew in &self.skews {
            if skew < 0.0 {
                return Err(format!("skew {} cannot be negative", skew));
            }
            if skew > 10.0 {
                return Err(format!("skew {} is too high (max 10.0)", skew));
            }
        }

        if self.max_inventory <= 0.0 {
            return Err("max_inventory must be positive".to_string());
        }
        if self.quote_size <= 0.0 {
            return Err("quote_size must be positive".to_string());
        }
        if self.fee_rate < 0.0 {
            return Err("fee_rate cannot be negative".to_string());
        }
        if self.fill_prob <= 0.0 || self.fill_prob > 1.0 {
            return Err(format!("fill_prob {} must be in (0, 1]", self.fill_prob));
        }
        if self.queue_position < 0.0 || self.queue_position > 1.0 {
            return Err(format!("queue_position {} must be in [0, 1]", self.queue_position));
        }

        Ok(())
    }
}

/// Single sweep result
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SweepResult {
    pub spread: f64,
    pub skew: f64,
    pub sharpe: f64,
    pub total_return: f64,
    pub max_drawdown: f64,
    pub num_trades: usize,
    pub win_rate: f64,
}

impl SweepResult {
    /// Check if result is valid (has trades and non-NaN values)
    pub fn is_valid(&self) -> bool {
        self.num_trades > 0
            && !self.sharpe.is_nan()
            && !self.total_return.is_nan()
            && !self.total_return.is_infinite()
            && !self.max_drawdown.is_nan()
    }

    /// Format as a single line for display
    pub fn to_line(&self) -> String {
        format!(
            "Spread={:.1}, Skew={:.1} => Sharpe={:+.2}, Return={:+.2}%, DD={:.2}%, WR={:.1}%, Trades={}",
            self.spread,
            self.skew,
            self.sharpe,
            self.total_return * 100.0,
            self.max_drawdown * 100.0,
            self.win_rate * 100.0,
            self.num_trades,
        )
    }
}

/// Progress callback for sweep updates
#[derive(Debug, Clone)]
pub struct SweepProgress {
    pub current: usize,
    pub total: usize,
    pub current_spread: f64,
    pub current_skew: f64,
    pub latest_result: Option<SweepResult>,
}

impl SweepProgress {
    /// Progress as fraction (0.0 to 1.0)
    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.current as f64 / self.total as f64
        }
    }

    /// Progress as percentage (0 to 100)
    pub fn percentage(&self) -> f64 {
        self.fraction() * 100.0
    }
}

/// Collection of sweep results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepResults {
    pub config: SweepConfig,
    pub results: Vec<SweepResult>,
}

impl SweepResults {
    /// Create new results container
    pub fn new(config: SweepConfig) -> Self {
        Self {
            config,
            results: Vec::new(),
        }
    }

    /// Add a result
    pub fn push(&mut self, result: SweepResult) {
        self.results.push(result);
    }

    /// Get the best result by Sharpe ratio
    pub fn best(&self) -> Option<&SweepResult> {
        self.results
            .iter()
            .filter(|r| r.is_valid())
            .max_by(|a, b| a.sharpe.partial_cmp(&b.sharpe).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Get top N results by Sharpe ratio
    pub fn top_n(&self, n: usize) -> Vec<&SweepResult> {
        let mut valid: Vec<_> = self.results.iter().filter(|r| r.is_valid()).collect();
        valid.sort_by(|a, b| b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal));
        valid.into_iter().take(n).collect()
    }

    /// Filter to only valid results
    pub fn filter_valid(&self) -> Vec<&SweepResult> {
        self.results.iter().filter(|r| r.is_valid()).collect()
    }

    /// Number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Save results to JSON file
    pub fn save_json(&self, path: &PathBuf) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load results from JSON file
    pub fn load_json(path: &PathBuf) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        let results: Self = serde_json::from_str(&json)?;
        Ok(results)
    }
}

/// Sweep engine that runs parameter sensitivity analysis
pub struct SweepEngine {
    config: SweepConfig,
    replay_config: ReplayConfig,
}

impl SweepEngine {
    /// Create new sweep engine
    pub fn new(config: SweepConfig, replay_config: ReplayConfig) -> Self {
        Self { config, replay_config }
    }

    /// Create with default data directory
    pub fn with_data_dir(config: SweepConfig, data_dir: PathBuf) -> Self {
        let replay_config = ReplayConfig {
            data_dir,
            ..Default::default()
        };
        Self::new(config, replay_config)
    }

    /// Run the sweep
    pub fn run(&self) -> Result<SweepResults> {
        self.run_with_progress(|_| {})
    }

    /// Run the sweep with progress callback
    pub fn run_with_progress<F>(&self, mut progress_callback: F) -> Result<SweepResults>
    where
        F: FnMut(SweepProgress),
    {
        self.config.validate().map_err(|e| anyhow::anyhow!(e))?;

        let mut results = SweepResults::new(self.config.clone());
        let total = self.config.total_combinations();
        let mut current = 0;

        for &spread in &self.config.spreads {
            for &skew in &self.config.skews {
                current += 1;

                // Report progress before running
                progress_callback(SweepProgress {
                    current,
                    total,
                    current_spread: spread,
                    current_skew: skew,
                    latest_result: None,
                });

                let mm_config = MMConfig {
                    max_inventory: Decimal::from_f64_retain(self.config.max_inventory).unwrap_or(dec!(0.1)),
                    quote_size: Decimal::from_f64_retain(self.config.quote_size).unwrap_or(dec!(0.001)),
                    regime_params: RegimeParams::uniform(spread, skew),
                    ..Default::default()
                };

                let backtest_config = BacktestConfig {
                    replay: self.replay_config.clone(),
                    mm: mm_config,
                    simulator: SimulatorConfig {
                        fee_rate: Decimal::from_f64_retain(self.config.fee_rate).unwrap_or(dec!(0.0001)),
                        ..Default::default()
                    },
                    fill_sim: FillSimulatorConfig {
                        base_fill_probability: self.config.fill_prob,
                        queue_position: self.config.queue_position,
                        fee_rate: Decimal::from_f64_retain(self.config.fee_rate).unwrap_or(dec!(0.0001)),
                        ..Default::default()
                    },
                    verbose: false,
                    use_realistic_fills: self.config.use_realistic_fills,
                    ..Default::default()
                };

                let mut engine = BacktestEngine::new(backtest_config);
                engine.load_data()?;
                let backtest_results = engine.run()?;

                let sweep_result = SweepResult {
                    spread,
                    skew,
                    sharpe: backtest_results.metrics.sharpe_ratio,
                    total_return: backtest_results.metrics.total_return,
                    max_drawdown: backtest_results.metrics.max_drawdown,
                    num_trades: backtest_results.metrics.num_trades,
                    win_rate: backtest_results.metrics.win_rate,
                };

                results.push(sweep_result.clone());

                // Report progress after running with result
                progress_callback(SweepProgress {
                    current,
                    total,
                    current_spread: spread,
                    current_skew: skew,
                    latest_result: Some(sweep_result),
                });
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== SweepConfig Tests ==========

    #[test]
    fn test_config_default() {
        let config = SweepConfig::default();
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(config.skews, vec![0.3, 0.5, 0.7, 1.0]);
        assert_eq!(config.max_inventory, 0.1);
        assert_eq!(config.quote_size, 0.001);
        assert_eq!(config.fee_rate, 0.0001);
        assert_eq!(config.fill_prob, 0.10);
        assert_eq!(config.queue_position, 0.5);
        assert!(config.use_realistic_fills);
    }

    #[test]
    fn test_config_from_strings() {
        let config = SweepConfig::from_strings("1,2,3", "0.3,0.5");
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0]);
        assert_eq!(config.skews, vec![0.3, 0.5]);
    }

    #[test]
    fn test_config_from_strings_with_spaces() {
        let config = SweepConfig::from_strings("1, 2, 3", "0.3, 0.5");
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0]);
        assert_eq!(config.skews, vec![0.3, 0.5]);
    }

    #[test]
    fn test_config_from_strings_empty_defaults() {
        let config = SweepConfig::from_strings("", "");
        assert_eq!(config.spreads, SweepConfig::default().spreads);
        assert_eq!(config.skews, SweepConfig::default().skews);
    }

    #[test]
    fn test_config_from_strings_invalid_values_filtered() {
        let config = SweepConfig::from_strings("1,abc,3", "0.3,xyz,0.7");
        assert_eq!(config.spreads, vec![1.0, 3.0]);
        assert_eq!(config.skews, vec![0.3, 0.7]);
    }

    #[test]
    fn test_config_total_combinations() {
        let config = SweepConfig {
            spreads: vec![1.0, 2.0, 3.0],
            skews: vec![0.3, 0.5],
            ..Default::default()
        };
        assert_eq!(config.total_combinations(), 6);
    }

    #[test]
    fn test_config_total_combinations_default() {
        let config = SweepConfig::default();
        assert_eq!(config.total_combinations(), 20); // 5 spreads * 4 skews
    }

    #[test]
    fn test_config_validate_success() {
        let config = SweepConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_empty_spreads() {
        let config = SweepConfig {
            spreads: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("spreads cannot be empty"));
    }

    #[test]
    fn test_config_validate_empty_skews() {
        let config = SweepConfig {
            skews: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("skews cannot be empty"));
    }

    #[test]
    fn test_config_validate_spread_too_low() {
        let config = SweepConfig {
            spreads: vec![0.05],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("too low"));
    }

    #[test]
    fn test_config_validate_spread_negative() {
        let config = SweepConfig {
            spreads: vec![-1.0],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_spread_too_high() {
        let config = SweepConfig {
            spreads: vec![150.0],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("too high"));
    }

    #[test]
    fn test_config_validate_skew_negative() {
        let config = SweepConfig {
            skews: vec![-0.5],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("negative"));
    }

    #[test]
    fn test_config_validate_skew_too_high() {
        let config = SweepConfig {
            skews: vec![15.0],
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("too high"));
    }

    #[test]
    fn test_config_validate_max_inventory_zero() {
        let config = SweepConfig {
            max_inventory: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("max_inventory"));
    }

    #[test]
    fn test_config_validate_quote_size_zero() {
        let config = SweepConfig {
            quote_size: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("quote_size"));
    }

    #[test]
    fn test_config_validate_fee_rate_negative() {
        let config = SweepConfig {
            fee_rate: -0.001,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("fee_rate"));
    }

    #[test]
    fn test_config_validate_fill_prob_zero() {
        let config = SweepConfig {
            fill_prob: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("fill_prob"));
    }

    #[test]
    fn test_config_validate_fill_prob_negative() {
        let config = SweepConfig {
            fill_prob: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_fill_prob_too_high() {
        let config = SweepConfig {
            fill_prob: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_queue_position_negative() {
        let config = SweepConfig {
            queue_position: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("queue_position"));
    }

    #[test]
    fn test_config_validate_queue_position_too_high() {
        let config = SweepConfig {
            queue_position: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = SweepConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SweepConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.spreads, deserialized.spreads);
        assert_eq!(config.skews, deserialized.skews);
    }

    // ========== SweepResult Tests ==========

    #[test]
    fn test_result_is_valid_true() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        assert!(result.is_valid());
    }

    #[test]
    fn test_result_is_valid_no_trades() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
        };
        assert!(!result.is_valid());
    }

    #[test]
    fn test_result_is_valid_nan_sharpe() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: f64::NAN,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        assert!(!result.is_valid());
    }

    #[test]
    fn test_result_is_valid_inf_return() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: f64::INFINITY,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        assert!(!result.is_valid());
    }

    #[test]
    fn test_result_to_line() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        let line = result.to_line();
        assert!(line.contains("Spread=1.0"));
        assert!(line.contains("Skew=0.5"));
        assert!(line.contains("Sharpe=+0.50"));
        assert!(line.contains("Return=+5.00%"));
        assert!(line.contains("DD=2.00%"));
        assert!(line.contains("WR=55.0%"));
        assert!(line.contains("Trades=100"));
    }

    #[test]
    fn test_result_to_line_negative_values() {
        let result = SweepResult {
            spread: 2.0,
            skew: 0.7,
            sharpe: -0.5,
            total_return: -0.03,
            max_drawdown: 0.05,
            num_trades: 50,
            win_rate: 0.40,
        };
        let line = result.to_line();
        assert!(line.contains("Sharpe=-0.50"));
        assert!(line.contains("Return=-3.00%"));
    }

    #[test]
    fn test_result_serialization() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SweepResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, deserialized);
    }

    #[test]
    fn test_result_equality() {
        let r1 = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        let r2 = r1.clone();
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_result_inequality() {
        let r1 = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        let r2 = SweepResult {
            spread: 2.0,
            ..r1.clone()
        };
        assert_ne!(r1, r2);
    }

    // ========== SweepProgress Tests ==========

    #[test]
    fn test_progress_fraction() {
        let progress = SweepProgress {
            current: 5,
            total: 10,
            current_spread: 1.0,
            current_skew: 0.5,
            latest_result: None,
        };
        assert!((progress.fraction() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_fraction_zero_total() {
        let progress = SweepProgress {
            current: 0,
            total: 0,
            current_spread: 1.0,
            current_skew: 0.5,
            latest_result: None,
        };
        assert!((progress.fraction() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_percentage() {
        let progress = SweepProgress {
            current: 3,
            total: 4,
            current_spread: 1.0,
            current_skew: 0.5,
            latest_result: None,
        };
        assert!((progress.percentage() - 75.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_with_result() {
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        let progress = SweepProgress {
            current: 5,
            total: 10,
            current_spread: 1.0,
            current_skew: 0.5,
            latest_result: Some(result.clone()),
        };
        assert!(progress.latest_result.is_some());
        assert_eq!(progress.latest_result.unwrap().sharpe, 0.5);
    }

    // ========== SweepResults Tests ==========

    #[test]
    fn test_results_new() {
        let config = SweepConfig::default();
        let results = SweepResults::new(config.clone());
        assert!(results.is_empty());
        assert_eq!(results.len(), 0);
        assert_eq!(results.config.spreads, config.spreads);
    }

    #[test]
    fn test_results_push() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        results.push(result);
        assert_eq!(results.len(), 1);
        assert!(!results.is_empty());
    }

    #[test]
    fn test_results_best_empty() {
        let config = SweepConfig::default();
        let results = SweepResults::new(config);
        assert!(results.best().is_none());
    }

    #[test]
    fn test_results_best_single() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);
        let result = SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        };
        results.push(result.clone());
        assert_eq!(results.best().unwrap().sharpe, 0.5);
    }

    #[test]
    fn test_results_best_multiple() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);

        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.3,
            total_return: 0.03,
            max_drawdown: 0.02,
            num_trades: 50,
            win_rate: 0.50,
        });
        results.push(SweepResult {
            spread: 2.0,
            skew: 0.7,
            sharpe: 0.8,
            total_return: 0.08,
            max_drawdown: 0.03,
            num_trades: 80,
            win_rate: 0.60,
        });
        results.push(SweepResult {
            spread: 3.0,
            skew: 0.3,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.01,
            num_trades: 60,
            win_rate: 0.55,
        });

        let best = results.best().unwrap();
        assert_eq!(best.spread, 2.0);
        assert_eq!(best.sharpe, 0.8);
    }

    #[test]
    fn test_results_best_filters_invalid() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);

        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 1.0,
            total_return: 0.10,
            max_drawdown: 0.01,
            num_trades: 0,
            win_rate: 0.0,
        });
        results.push(SweepResult {
            spread: 2.0,
            skew: 0.7,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        });

        let best = results.best().unwrap();
        assert_eq!(best.spread, 2.0);
        assert_eq!(best.sharpe, 0.5);
    }

    #[test]
    fn test_results_top_n() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);

        for i in 1..=5 {
            results.push(SweepResult {
                spread: i as f64,
                skew: 0.5,
                sharpe: i as f64 * 0.1,
                total_return: 0.01 * i as f64,
                max_drawdown: 0.01,
                num_trades: 100,
                win_rate: 0.55,
            });
        }

        let top3 = results.top_n(3);
        assert_eq!(top3.len(), 3);
        assert_eq!(top3[0].spread, 5.0);
        assert_eq!(top3[1].spread, 4.0);
        assert_eq!(top3[2].spread, 3.0);
    }

    #[test]
    fn test_results_top_n_more_than_available() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);

        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        });

        let top10 = results.top_n(10);
        assert_eq!(top10.len(), 1);
    }

    #[test]
    fn test_results_filter_valid() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);

        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        });
        results.push(SweepResult {
            spread: 2.0,
            skew: 0.7,
            sharpe: f64::NAN,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
        });
        results.push(SweepResult {
            spread: 3.0,
            skew: 0.3,
            sharpe: 0.3,
            total_return: 0.03,
            max_drawdown: 0.01,
            num_trades: 50,
            win_rate: 0.50,
        });

        let valid = results.filter_valid();
        assert_eq!(valid.len(), 2);
    }

    #[test]
    fn test_results_serialization() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);
        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        });

        let json = serde_json::to_string(&results).unwrap();
        let deserialized: SweepResults = serde_json::from_str(&json).unwrap();
        assert_eq!(results.len(), deserialized.len());
    }

    // ========== SweepEngine Tests ==========

    #[test]
    fn test_engine_creation() {
        let config = SweepConfig::default();
        let replay_config = ReplayConfig::default();
        let _engine = SweepEngine::new(config, replay_config);
    }

    #[test]
    fn test_engine_with_data_dir() {
        let config = SweepConfig::default();
        let _engine = SweepEngine::with_data_dir(config, PathBuf::from("./data/features"));
    }

    #[test]
    fn test_engine_validates_config() {
        let config = SweepConfig {
            spreads: vec![],
            ..Default::default()
        };
        let replay_config = ReplayConfig::default();
        let engine = SweepEngine::new(config, replay_config);
        let result = engine.run();
        assert!(result.is_err());
    }

    // ========== JSON Persistence Tests ==========

    #[test]
    fn test_results_save_and_load_json() {
        use std::fs;

        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);
        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        });
        results.push(SweepResult {
            spread: 2.0,
            skew: 0.7,
            sharpe: 0.3,
            total_return: 0.03,
            max_drawdown: 0.01,
            num_trades: 50,
            win_rate: 0.50,
        });

        let temp_path = PathBuf::from("/tmp/test_sweep_results.json");

        results.save_json(&temp_path).unwrap();
        assert!(temp_path.exists());

        let loaded = SweepResults::load_json(&temp_path).unwrap();
        assert_eq!(loaded.len(), results.len());
        assert_eq!(loaded.results[0].spread, results.results[0].spread);

        fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_results_load_json_not_found() {
        let path = PathBuf::from("/tmp/nonexistent_sweep.json");
        let result = SweepResults::load_json(&path);
        assert!(result.is_err());
    }

    // ========== Edge Cases ==========

    #[test]
    fn test_config_boundary_values() {
        let config = SweepConfig {
            spreads: vec![0.1, 100.0],
            skews: vec![0.0, 10.0],
            fill_prob: 1.0,
            queue_position: 0.0,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_single_value() {
        let config = SweepConfig {
            spreads: vec![1.0],
            skews: vec![0.5],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.total_combinations(), 1);
    }

    #[test]
    fn test_result_edge_values() {
        let result = SweepResult {
            spread: 0.1,
            skew: 0.0,
            sharpe: -10.0,
            total_return: -0.99,
            max_drawdown: 0.99,
            num_trades: 1,
            win_rate: 0.0,
        };
        assert!(result.is_valid());
    }

    #[test]
    fn test_progress_edge_values() {
        let progress = SweepProgress {
            current: 0,
            total: 1,
            current_spread: 0.1,
            current_skew: 0.0,
            latest_result: None,
        };
        assert!((progress.fraction() - 0.0).abs() < f64::EPSILON);

        let progress2 = SweepProgress {
            current: 1,
            total: 1,
            current_spread: 0.1,
            current_skew: 0.0,
            latest_result: None,
        };
        assert!((progress2.fraction() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_results_best_all_invalid() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);

        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: f64::NAN,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
        });
        results.push(SweepResult {
            spread: 2.0,
            skew: 0.7,
            sharpe: 0.5,
            total_return: f64::INFINITY,
            max_drawdown: 0.0,
            num_trades: 100,
            win_rate: 0.0,
        });

        assert!(results.best().is_none());
    }

    #[test]
    fn test_results_top_n_empty() {
        let config = SweepConfig::default();
        let results = SweepResults::new(config);
        let top5 = results.top_n(5);
        assert!(top5.is_empty());
    }

    #[test]
    fn test_results_top_n_zero() {
        let config = SweepConfig::default();
        let mut results = SweepResults::new(config);
        results.push(SweepResult {
            spread: 1.0,
            skew: 0.5,
            sharpe: 0.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
        });
        let top0 = results.top_n(0);
        assert!(top0.is_empty());
    }
}
