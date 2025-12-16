//! Grid Search Module
//!
//! Hyperparameter optimization via exhaustive grid search.
//! Shared implementation for both CLI and TUI interfaces.
//!
//! # Features
//!
//! - Configurable parameter grids (spreads, skews, entropy thresholds, fill probs)
//! - Progress callbacks for UI integration
//! - Sorted results by Sharpe ratio
//! - JSON serialization for persistence
//!
//! # Usage
//!
//! ```ignore
//! use crate::backtest::grid_search::{GridSearchEngine, GridSearchConfig, GridSearchResult};
//!
//! let config = GridSearchConfig {
//!     spreads: vec![1.0, 2.0, 3.0],
//!     skews: vec![0.3, 0.5, 0.7],
//!     ..Default::default()
//! };
//!
//! let mut engine = GridSearchEngine::new(config, replay_config);
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
use crate::trading::market_maker::{MMConfig, RegimeParams, RegimeThresholds};
use crate::trading::mm_simulator::SimulatorConfig;

/// Configuration for grid search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridSearchConfig {
    /// Spread values to test (in basis points)
    pub spreads: Vec<f64>,
    /// Skew values to test
    pub skews: Vec<f64>,
    /// High entropy threshold values to test
    pub high_entropies: Vec<f64>,
    /// Fill probability values to test
    pub fill_probs: Vec<f64>,
    /// Low entropy threshold (fixed)
    pub low_entropy_threshold: f64,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fee rate
    pub fee_rate: f64,
    /// Queue position for fill simulation
    pub queue_position: f64,
    /// Use realistic fill simulation
    pub use_realistic_fills: bool,
}

impl Default for GridSearchConfig {
    fn default() -> Self {
        Self {
            spreads: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            skews: vec![0.3, 0.5, 0.7, 1.0],
            high_entropies: vec![0.6, 0.7, 0.8],
            fill_probs: vec![0.05, 0.10, 0.15],
            low_entropy_threshold: 0.4,
            max_inventory: 0.1,
            quote_size: 0.001,
            fee_rate: 0.0001,
            queue_position: 0.5,
            use_realistic_fills: true,
        }
    }
}

impl GridSearchConfig {
    /// Create config from comma-separated strings (CLI compatibility)
    pub fn from_strings(
        spreads_str: &str,
        skews_str: &str,
        high_entropies_str: &str,
        fill_probs_str: &str,
    ) -> Self {
        let spreads: Vec<f64> = spreads_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let skews: Vec<f64> = skews_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let high_entropies: Vec<f64> = high_entropies_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let fill_probs: Vec<f64> = fill_probs_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        Self {
            spreads: if spreads.is_empty() { vec![1.0, 2.0, 3.0, 4.0, 5.0] } else { spreads },
            skews: if skews.is_empty() { vec![0.3, 0.5, 0.7, 1.0] } else { skews },
            high_entropies: if high_entropies.is_empty() { vec![0.6, 0.7, 0.8] } else { high_entropies },
            fill_probs: if fill_probs.is_empty() { vec![0.05, 0.10, 0.15] } else { fill_probs },
            ..Default::default()
        }
    }

    /// Total number of parameter combinations
    pub fn total_combinations(&self) -> usize {
        self.spreads.len() * self.skews.len() * self.high_entropies.len() * self.fill_probs.len()
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.spreads.is_empty() {
            anyhow::bail!("At least one spread value required");
        }
        if self.skews.is_empty() {
            anyhow::bail!("At least one skew value required");
        }
        if self.high_entropies.is_empty() {
            anyhow::bail!("At least one high entropy threshold required");
        }
        if self.fill_probs.is_empty() {
            anyhow::bail!("At least one fill probability required");
        }

        // Validate ranges
        for &spread in &self.spreads {
            if spread <= 0.0 || spread > 100.0 {
                anyhow::bail!("Spread must be between 0 and 100 bps, got {}", spread);
            }
        }
        for &skew in &self.skews {
            if skew < 0.0 || skew > 10.0 {
                anyhow::bail!("Skew must be between 0 and 10, got {}", skew);
            }
        }
        for &entropy in &self.high_entropies {
            if entropy < 0.0 || entropy > 1.0 {
                anyhow::bail!("High entropy threshold must be between 0 and 1, got {}", entropy);
            }
        }
        for &fill_prob in &self.fill_probs {
            if fill_prob <= 0.0 || fill_prob > 1.0 {
                anyhow::bail!("Fill probability must be between 0 and 1, got {}", fill_prob);
            }
        }
        if self.low_entropy_threshold < 0.0 || self.low_entropy_threshold > 1.0 {
            anyhow::bail!("Low entropy threshold must be between 0 and 1");
        }
        if self.max_inventory <= 0.0 {
            anyhow::bail!("Max inventory must be positive");
        }
        if self.quote_size <= 0.0 {
            anyhow::bail!("Quote size must be positive");
        }
        if self.fee_rate < 0.0 {
            anyhow::bail!("Fee rate cannot be negative");
        }
        if self.queue_position < 0.0 || self.queue_position > 1.0 {
            anyhow::bail!("Queue position must be between 0 and 1");
        }

        Ok(())
    }
}

/// Single grid search result
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GridSearchResult {
    /// Spread in basis points
    pub spread: f64,
    /// Inventory skew factor
    pub skew: f64,
    /// High entropy threshold
    pub high_entropy_threshold: f64,
    /// Fill probability assumption
    pub fill_prob: f64,
    /// Sharpe ratio (annualized)
    pub sharpe: f64,
    /// Total return
    pub total_return: f64,
    /// Maximum drawdown
    pub max_drawdown: f64,
    /// Number of trades
    pub num_trades: usize,
    /// Win rate
    pub win_rate: f64,
    /// Average PnL per trade
    pub avg_trade_pnl: f64,
}

impl GridSearchResult {
    /// Check if this result is valid (has trades and meaningful metrics)
    pub fn is_valid(&self) -> bool {
        self.num_trades > 0 && self.sharpe.is_finite() && self.total_return.is_finite()
    }

    /// Format result as a single line string
    pub fn to_line(&self, index: usize, total: usize) -> String {
        format!(
            "[{:>4}/{}] s={:.1} k={:.1} ent={:.1} fp={:.2} => Sharpe={:+.2} Ret={:+.2}% Tr={}",
            index, total,
            self.spread, self.skew, self.high_entropy_threshold, self.fill_prob,
            self.sharpe, self.total_return * 100.0, self.num_trades,
        )
    }
}

/// Progress information for callbacks
#[derive(Debug, Clone)]
pub struct GridSearchProgress {
    /// Current iteration (1-indexed)
    pub current: usize,
    /// Total iterations
    pub total: usize,
    /// Current parameters being tested
    pub spread: f64,
    pub skew: f64,
    pub high_entropy: f64,
    pub fill_prob: f64,
    /// Latest result (if available)
    pub latest_result: Option<GridSearchResult>,
}

impl GridSearchProgress {
    /// Progress as a fraction (0.0 to 1.0)
    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            1.0
        } else {
            self.current as f64 / self.total as f64
        }
    }

    /// Progress as a percentage
    pub fn percentage(&self) -> f64 {
        self.fraction() * 100.0
    }
}

/// Complete grid search results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridSearchResults {
    /// All results sorted by Sharpe ratio (descending)
    pub results: Vec<GridSearchResult>,
    /// Configuration used
    pub config: GridSearchConfig,
}

impl GridSearchResults {
    /// Get the best result (highest Sharpe)
    pub fn best(&self) -> Option<&GridSearchResult> {
        self.results.first()
    }

    /// Get top N results
    pub fn top_n(&self, n: usize) -> &[GridSearchResult] {
        let len = self.results.len().min(n);
        &self.results[..len]
    }

    /// Filter results by minimum criteria
    pub fn filter_valid(&self) -> Vec<&GridSearchResult> {
        self.results.iter().filter(|r| r.is_valid()).collect()
    }

    /// Save results to JSON file
    pub fn save_json(&self, path: &PathBuf) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.results)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load results from JSON file
    pub fn load_json(path: &PathBuf) -> Result<Vec<GridSearchResult>> {
        let content = std::fs::read_to_string(path)?;
        let results: Vec<GridSearchResult> = serde_json::from_str(&content)?;
        Ok(results)
    }
}

/// Grid search engine
pub struct GridSearchEngine {
    config: GridSearchConfig,
    replay_config: ReplayConfig,
}

impl GridSearchEngine {
    /// Create new grid search engine
    pub fn new(config: GridSearchConfig, replay_config: ReplayConfig) -> Self {
        Self {
            config,
            replay_config,
        }
    }

    /// Create with default data directory
    pub fn with_data_dir(config: GridSearchConfig, data_dir: PathBuf) -> Self {
        Self {
            config,
            replay_config: ReplayConfig {
                data_dir,
                ..Default::default()
            },
        }
    }

    /// Run grid search without progress callback
    pub fn run(&self) -> Result<GridSearchResults> {
        self.run_with_progress(|_| {})
    }

    /// Run grid search with progress callback
    pub fn run_with_progress<F>(&self, mut progress_callback: F) -> Result<GridSearchResults>
    where
        F: FnMut(GridSearchProgress),
    {
        self.config.validate()?;

        let total = self.config.total_combinations();
        let mut all_results: Vec<GridSearchResult> = Vec::with_capacity(total);
        let mut count = 0;

        for &spread in &self.config.spreads {
            for &skew in &self.config.skews {
                for &high_entropy in &self.config.high_entropies {
                    for &fill_prob in &self.config.fill_probs {
                        count += 1;

                        // Report progress before running
                        progress_callback(GridSearchProgress {
                            current: count,
                            total,
                            spread,
                            skew,
                            high_entropy,
                            fill_prob,
                            latest_result: all_results.last().cloned(),
                        });

                        // Run single backtest
                        let result = self.run_single(spread, skew, high_entropy, fill_prob)?;
                        all_results.push(result);
                    }
                }
            }
        }

        // Sort by Sharpe ratio (descending)
        all_results.sort_by(|a, b| {
            b.sharpe.partial_cmp(&a.sharpe).unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(GridSearchResults {
            results: all_results,
            config: self.config.clone(),
        })
    }

    /// Run a single backtest with given parameters
    fn run_single(
        &self,
        spread: f64,
        skew: f64,
        high_entropy: f64,
        fill_prob: f64,
    ) -> Result<GridSearchResult> {
        let regime_params = RegimeParams::uniform(spread, skew);

        let mm_config = MMConfig {
            max_inventory: Decimal::from_f64_retain(self.config.max_inventory).unwrap_or(dec!(0.1)),
            quote_size: Decimal::from_f64_retain(self.config.quote_size).unwrap_or(dec!(0.001)),
            regime_thresholds: RegimeThresholds {
                high_entropy_threshold: high_entropy,
                low_entropy_threshold: self.config.low_entropy_threshold,
            },
            regime_params,
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
                base_fill_probability: fill_prob,
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
        let results = engine.run()?;

        let avg_trade_pnl = if results.metrics.num_trades > 0 {
            results.metrics.total_return / results.metrics.num_trades as f64
        } else {
            0.0
        };

        Ok(GridSearchResult {
            spread,
            skew,
            high_entropy_threshold: high_entropy,
            fill_prob,
            sharpe: results.metrics.sharpe_ratio,
            total_return: results.metrics.total_return,
            max_drawdown: results.metrics.max_drawdown,
            num_trades: results.metrics.num_trades,
            win_rate: results.metrics.win_rate,
            avg_trade_pnl,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== GridSearchConfig Tests ====================

    #[test]
    fn test_config_default() {
        let config = GridSearchConfig::default();
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(config.skews, vec![0.3, 0.5, 0.7, 1.0]);
        assert_eq!(config.high_entropies, vec![0.6, 0.7, 0.8]);
        assert_eq!(config.fill_probs, vec![0.05, 0.10, 0.15]);
        assert_eq!(config.low_entropy_threshold, 0.4);
        assert_eq!(config.max_inventory, 0.1);
        assert_eq!(config.quote_size, 0.001);
        assert_eq!(config.fee_rate, 0.0001);
        assert_eq!(config.queue_position, 0.5);
        assert!(config.use_realistic_fills);
    }

    #[test]
    fn test_config_from_strings() {
        let config = GridSearchConfig::from_strings(
            "1,2,3",
            "0.5,1.0",
            "0.7",
            "0.10,0.15",
        );
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0]);
        assert_eq!(config.skews, vec![0.5, 1.0]);
        assert_eq!(config.high_entropies, vec![0.7]);
        assert_eq!(config.fill_probs, vec![0.10, 0.15]);
    }

    #[test]
    fn test_config_from_strings_with_spaces() {
        let config = GridSearchConfig::from_strings(
            "1, 2, 3",
            " 0.5 , 1.0 ",
            "0.7",
            "0.10",
        );
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0]);
        assert_eq!(config.skews, vec![0.5, 1.0]);
    }

    #[test]
    fn test_config_from_strings_empty_defaults() {
        let config = GridSearchConfig::from_strings("", "", "", "");
        // Should use defaults when empty
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(config.skews, vec![0.3, 0.5, 0.7, 1.0]);
        assert_eq!(config.high_entropies, vec![0.6, 0.7, 0.8]);
        assert_eq!(config.fill_probs, vec![0.05, 0.10, 0.15]);
    }

    #[test]
    fn test_config_from_strings_invalid_values_filtered() {
        let config = GridSearchConfig::from_strings(
            "1,abc,2,xyz,3",
            "0.5,invalid,1.0",
            "0.7,not_a_number",
            "0.10",
        );
        assert_eq!(config.spreads, vec![1.0, 2.0, 3.0]);
        assert_eq!(config.skews, vec![0.5, 1.0]);
        assert_eq!(config.high_entropies, vec![0.7]);
    }

    #[test]
    fn test_config_total_combinations() {
        let config = GridSearchConfig {
            spreads: vec![1.0, 2.0],
            skews: vec![0.3, 0.5, 0.7],
            high_entropies: vec![0.6, 0.7],
            fill_probs: vec![0.10],
            ..Default::default()
        };
        assert_eq!(config.total_combinations(), 2 * 3 * 2 * 1);
    }

    #[test]
    fn test_config_total_combinations_default() {
        let config = GridSearchConfig::default();
        // 5 * 4 * 3 * 3 = 180
        assert_eq!(config.total_combinations(), 180);
    }

    #[test]
    fn test_config_validate_success() {
        let config = GridSearchConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_empty_spreads() {
        let config = GridSearchConfig {
            spreads: vec![],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("spread"));
    }

    #[test]
    fn test_config_validate_empty_skews() {
        let config = GridSearchConfig {
            skews: vec![],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("skew"));
    }

    #[test]
    fn test_config_validate_empty_high_entropies() {
        let config = GridSearchConfig {
            high_entropies: vec![],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("entropy"));
    }

    #[test]
    fn test_config_validate_empty_fill_probs() {
        let config = GridSearchConfig {
            fill_probs: vec![],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("fill probability"));
    }

    #[test]
    fn test_config_validate_spread_too_low() {
        let config = GridSearchConfig {
            spreads: vec![0.0],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Spread"));
    }

    #[test]
    fn test_config_validate_spread_negative() {
        let config = GridSearchConfig {
            spreads: vec![-1.0],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Spread"));
    }

    #[test]
    fn test_config_validate_spread_too_high() {
        let config = GridSearchConfig {
            spreads: vec![101.0],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Spread"));
    }

    #[test]
    fn test_config_validate_skew_negative() {
        let config = GridSearchConfig {
            skews: vec![-0.1],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Skew"));
    }

    #[test]
    fn test_config_validate_skew_too_high() {
        let config = GridSearchConfig {
            skews: vec![11.0],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Skew"));
    }

    #[test]
    fn test_config_validate_entropy_negative() {
        let config = GridSearchConfig {
            high_entropies: vec![-0.1],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("entropy"));
    }

    #[test]
    fn test_config_validate_entropy_too_high() {
        let config = GridSearchConfig {
            high_entropies: vec![1.1],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("entropy"));
    }

    #[test]
    fn test_config_validate_fill_prob_zero() {
        let config = GridSearchConfig {
            fill_probs: vec![0.0],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Fill probability"));
    }

    #[test]
    fn test_config_validate_fill_prob_negative() {
        let config = GridSearchConfig {
            fill_probs: vec![-0.1],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Fill probability"));
    }

    #[test]
    fn test_config_validate_fill_prob_too_high() {
        let config = GridSearchConfig {
            fill_probs: vec![1.1],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Fill probability"));
    }

    #[test]
    fn test_config_validate_low_entropy_invalid() {
        let config = GridSearchConfig {
            low_entropy_threshold: -0.1,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Low entropy"));
    }

    #[test]
    fn test_config_validate_max_inventory_zero() {
        let config = GridSearchConfig {
            max_inventory: 0.0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Max inventory"));
    }

    #[test]
    fn test_config_validate_quote_size_zero() {
        let config = GridSearchConfig {
            quote_size: 0.0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Quote size"));
    }

    #[test]
    fn test_config_validate_fee_rate_negative() {
        let config = GridSearchConfig {
            fee_rate: -0.001,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Fee rate"));
    }

    #[test]
    fn test_config_validate_queue_position_negative() {
        let config = GridSearchConfig {
            queue_position: -0.1,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Queue position"));
    }

    #[test]
    fn test_config_validate_queue_position_too_high() {
        let config = GridSearchConfig {
            queue_position: 1.1,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("Queue position"));
    }

    #[test]
    fn test_config_serialization() {
        let config = GridSearchConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: GridSearchConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.spreads, deserialized.spreads);
        assert_eq!(config.skews, deserialized.skews);
        assert_eq!(config.high_entropies, deserialized.high_entropies);
        assert_eq!(config.fill_probs, deserialized.fill_probs);
    }

    // ==================== GridSearchResult Tests ====================

    #[test]
    fn test_result_is_valid_true() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        assert!(result.is_valid());
    }

    #[test]
    fn test_result_is_valid_no_trades() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        };
        assert!(!result.is_valid());
    }

    #[test]
    fn test_result_is_valid_nan_sharpe() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: f64::NAN,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        assert!(!result.is_valid());
    }

    #[test]
    fn test_result_is_valid_inf_return() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: f64::INFINITY,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        assert!(!result.is_valid());
    }

    #[test]
    fn test_result_to_line() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let line = result.to_line(1, 10);
        assert!(line.contains("s=1.0"));
        assert!(line.contains("k=0.5"));
        assert!(line.contains("ent=0.7"));
        assert!(line.contains("fp=0.10"));
        assert!(line.contains("Sharpe=+1.50"));
        assert!(line.contains("Ret=+5.00%"));
        assert!(line.contains("Tr=100"));
    }

    #[test]
    fn test_result_to_line_negative_values() {
        let result = GridSearchResult {
            spread: 2.0,
            skew: 0.3,
            high_entropy_threshold: 0.6,
            fill_prob: 0.15,
            sharpe: -0.5,
            total_return: -0.02,
            max_drawdown: 0.05,
            num_trades: 50,
            win_rate: 0.45,
            avg_trade_pnl: -0.0004,
        };
        let line = result.to_line(5, 100);
        assert!(line.contains("Sharpe=-0.50"));
        assert!(line.contains("Ret=-2.00%"));
    }

    #[test]
    fn test_result_serialization() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: GridSearchResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, deserialized);
    }

    #[test]
    fn test_result_equality() {
        let result1 = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let result2 = result1.clone();
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_result_inequality() {
        let result1 = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let result2 = GridSearchResult {
            spread: 2.0,
            ..result1.clone()
        };
        assert_ne!(result1, result2);
    }

    // ==================== GridSearchProgress Tests ====================

    #[test]
    fn test_progress_fraction() {
        let progress = GridSearchProgress {
            current: 50,
            total: 100,
            spread: 1.0,
            skew: 0.5,
            high_entropy: 0.7,
            fill_prob: 0.1,
            latest_result: None,
        };
        assert!((progress.fraction() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_fraction_zero_total() {
        let progress = GridSearchProgress {
            current: 0,
            total: 0,
            spread: 1.0,
            skew: 0.5,
            high_entropy: 0.7,
            fill_prob: 0.1,
            latest_result: None,
        };
        assert!((progress.fraction() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_percentage() {
        let progress = GridSearchProgress {
            current: 25,
            total: 100,
            spread: 1.0,
            skew: 0.5,
            high_entropy: 0.7,
            fill_prob: 0.1,
            latest_result: None,
        };
        assert!((progress.percentage() - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progress_with_result() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let progress = GridSearchProgress {
            current: 1,
            total: 10,
            spread: 2.0,
            skew: 0.3,
            high_entropy: 0.6,
            fill_prob: 0.15,
            latest_result: Some(result.clone()),
        };
        assert!(progress.latest_result.is_some());
        assert_eq!(progress.latest_result.unwrap().sharpe, 1.5);
    }

    // ==================== GridSearchResults Tests ====================

    #[test]
    fn test_results_best_empty() {
        let results = GridSearchResults {
            results: vec![],
            config: GridSearchConfig::default(),
        };
        assert!(results.best().is_none());
    }

    #[test]
    fn test_results_best_single() {
        let result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let results = GridSearchResults {
            results: vec![result.clone()],
            config: GridSearchConfig::default(),
        };
        assert_eq!(results.best().unwrap(), &result);
    }

    #[test]
    fn test_results_best_multiple() {
        let result1 = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 2.0,
            total_return: 0.08,
            max_drawdown: 0.01,
            num_trades: 150,
            win_rate: 0.60,
            avg_trade_pnl: 0.0006,
        };
        let result2 = GridSearchResult {
            spread: 2.0,
            skew: 0.3,
            high_entropy_threshold: 0.6,
            fill_prob: 0.15,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let results = GridSearchResults {
            results: vec![result1.clone(), result2],
            config: GridSearchConfig::default(),
        };
        assert_eq!(results.best().unwrap().sharpe, 2.0);
    }

    #[test]
    fn test_results_top_n() {
        let results = GridSearchResults {
            results: (0..10).map(|i| GridSearchResult {
                spread: i as f64,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.1,
                sharpe: 10.0 - i as f64,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            }).collect(),
            config: GridSearchConfig::default(),
        };
        let top5 = results.top_n(5);
        assert_eq!(top5.len(), 5);
        assert_eq!(top5[0].sharpe, 10.0);
        assert_eq!(top5[4].sharpe, 6.0);
    }

    #[test]
    fn test_results_top_n_more_than_available() {
        let results = GridSearchResults {
            results: vec![GridSearchResult {
                spread: 1.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.1,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            }],
            config: GridSearchConfig::default(),
        };
        let top10 = results.top_n(10);
        assert_eq!(top10.len(), 1);
    }

    #[test]
    fn test_results_filter_valid() {
        let valid_result = GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        };
        let invalid_result = GridSearchResult {
            spread: 2.0,
            skew: 0.3,
            high_entropy_threshold: 0.6,
            fill_prob: 0.15,
            sharpe: f64::NAN,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        };
        let results = GridSearchResults {
            results: vec![valid_result.clone(), invalid_result],
            config: GridSearchConfig::default(),
        };
        let valid = results.filter_valid();
        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0], &valid_result);
    }

    #[test]
    fn test_results_serialization() {
        let results = GridSearchResults {
            results: vec![GridSearchResult {
                spread: 1.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.1,
                sharpe: 1.5,
                total_return: 0.05,
                max_drawdown: 0.02,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.0005,
            }],
            config: GridSearchConfig::default(),
        };
        let json = serde_json::to_string(&results).unwrap();
        let deserialized: GridSearchResults = serde_json::from_str(&json).unwrap();
        assert_eq!(results.results.len(), deserialized.results.len());
    }

    // ==================== GridSearchEngine Tests ====================

    #[test]
    fn test_engine_creation() {
        let config = GridSearchConfig::default();
        let replay_config = ReplayConfig {
            data_dir: PathBuf::from("./data/features"),
            ..Default::default()
        };
        let _engine = GridSearchEngine::new(config, replay_config);
    }

    #[test]
    fn test_engine_with_data_dir() {
        let config = GridSearchConfig::default();
        let _engine = GridSearchEngine::with_data_dir(config, PathBuf::from("./data/features"));
    }

    #[test]
    fn test_engine_validates_config() {
        let config = GridSearchConfig {
            spreads: vec![],
            ..Default::default()
        };
        let engine = GridSearchEngine::with_data_dir(config, PathBuf::from("./data/features"));
        let err = engine.run().unwrap_err();
        assert!(err.to_string().contains("spread"));
    }

    // ==================== File I/O Tests ====================

    #[test]
    fn test_results_save_and_load_json() {
        let results = vec![GridSearchResult {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.1,
            sharpe: 1.5,
            total_return: 0.05,
            max_drawdown: 0.02,
            num_trades: 100,
            win_rate: 0.55,
            avg_trade_pnl: 0.0005,
        }];

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_grid_search_results.json");

        let grid_results = GridSearchResults {
            results: results.clone(),
            config: GridSearchConfig::default(),
        };

        // Save
        grid_results.save_json(&path).unwrap();

        // Load
        let loaded = GridSearchResults::load_json(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].sharpe, 1.5);

        // Cleanup
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_results_load_json_not_found() {
        let path = PathBuf::from("/nonexistent/path/file.json");
        let err = GridSearchResults::load_json(&path).unwrap_err();
        assert!(err.to_string().contains("No such file") || err.to_string().contains("cannot find"));
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_config_boundary_values() {
        let config = GridSearchConfig {
            spreads: vec![0.01, 99.99],
            skews: vec![0.0, 9.99],
            high_entropies: vec![0.0, 1.0],
            fill_probs: vec![0.01, 1.0],
            low_entropy_threshold: 0.0,
            max_inventory: 0.001,
            quote_size: 0.0001,
            fee_rate: 0.0,
            queue_position: 0.0,
            use_realistic_fills: false,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_single_value_grid() {
        let config = GridSearchConfig {
            spreads: vec![1.0],
            skews: vec![0.5],
            high_entropies: vec![0.7],
            fill_probs: vec![0.1],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.total_combinations(), 1);
    }

    #[test]
    fn test_result_edge_values() {
        let result = GridSearchResult {
            spread: f64::MIN_POSITIVE,
            skew: 0.0,
            high_entropy_threshold: 0.0,
            fill_prob: f64::MIN_POSITIVE,
            sharpe: f64::NEG_INFINITY,
            total_return: f64::NEG_INFINITY,
            max_drawdown: 1.0,
            num_trades: 1,
            win_rate: 0.0,
            avg_trade_pnl: f64::NEG_INFINITY,
        };
        assert!(!result.is_valid()); // NEG_INFINITY is not finite
    }

    #[test]
    fn test_progress_edge_values() {
        let progress = GridSearchProgress {
            current: usize::MAX,
            total: usize::MAX,
            spread: f64::MAX,
            skew: f64::MAX,
            high_entropy: f64::MAX,
            fill_prob: f64::MAX,
            latest_result: None,
        };
        assert!((progress.fraction() - 1.0).abs() < f64::EPSILON);
    }
}
