//! Backtest Command Parameters
//!
//! This module defines parameter structs and builders for all backtest commands.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};

/// Parameters for the `evaluate` command (single backtest evaluation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluateParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Base spread in basis points (per side)
    pub spread: f64,
    /// Inventory skew factor
    pub skew: f64,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: f64,
    /// Use naive fill simulation (for comparison)
    pub naive_fills: bool,
    /// Fill probability (0.0-1.0) for realistic simulation
    pub fill_prob: f64,
    /// Queue position (0.0=front, 1.0=back)
    pub queue_pos: f64,
    /// High entropy threshold (above = aggressive quoting)
    pub high_entropy: f64,
    /// Low entropy threshold (below = defensive/no quoting)
    pub low_entropy: f64,
    /// Use regime-specific parameters (different params per regime)
    pub regime_params: bool,
    /// High entropy spread (bps) - used with regime_params
    pub high_spread: f64,
    /// Medium entropy spread (bps) - used with regime_params
    pub med_spread: f64,
    /// Low entropy spread (bps) - used with regime_params
    pub low_spread: f64,
    /// High entropy skew - used with regime_params
    pub high_skew: f64,
    /// Medium entropy skew - used with regime_params
    pub med_skew: f64,
    /// Low entropy skew - used with regime_params
    pub low_skew: f64,
    /// Quote in low entropy (false = no quotes in low entropy)
    pub quote_low_entropy: bool,
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
    /// Output results as JSON (for scripting/Optuna)
    pub json: bool,
    /// Quiet mode (no progress output)
    pub quiet: bool,
    /// Show statistical significance report (PSR, DSR, bootstrap CI)
    pub stats: bool,
}

/// Builder for `EvaluateParams` with validation
pub struct EvaluateParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spread: Option<f64>,
    skew: Option<f64>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    high_entropy: Option<f64>,
    low_entropy: Option<f64>,
    regime_params: Option<bool>,
    high_spread: Option<f64>,
    med_spread: Option<f64>,
    low_spread: Option<f64>,
    high_skew: Option<f64>,
    med_skew: Option<f64>,
    low_skew: Option<f64>,
    quote_low_entropy: Option<bool>,
    output: Option<PathBuf>,
    json: Option<bool>,
    quiet: Option<bool>,
    stats: Option<bool>,
}

impl EvaluateParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            spread: None,
            skew: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            fill_prob: None,
            queue_pos: None,
            high_entropy: None,
            low_entropy: None,
            regime_params: None,
            high_spread: None,
            med_spread: None,
            low_spread: None,
            high_skew: None,
            med_skew: None,
            low_skew: None,
            quote_low_entropy: None,
            output: None,
            json: None,
            quiet: None,
            stats: None,
        }
    }

    /// Set data path
    pub fn data_path(mut self, path: PathBuf) -> Self {
        self.data_path = Some(path);
        self
    }

    /// Set algorithm
    pub fn algorithm(mut self, algo: String) -> Self {
        self.algorithm = Some(algo);
        self
    }

    /// Set weights file
    pub fn weights_file(mut self, path: Option<PathBuf>) -> Self {
        self.weights_file = path;
        self
    }

    /// Set spread
    pub fn spread(mut self, spread: f64) -> Self {
        self.spread = Some(spread);
        self
    }

    /// Set skew
    pub fn skew(mut self, skew: f64) -> Self {
        self.skew = Some(skew);
        self
    }

    /// Set max inventory
    pub fn max_inventory(mut self, max_inv: f64) -> Self {
        self.max_inventory = Some(max_inv);
        self
    }

    /// Set quote size
    pub fn quote_size(mut self, size: f64) -> Self {
        self.quote_size = Some(size);
        self
    }

    /// Set fee rate
    pub fn fee_rate(mut self, rate: f64) -> Self {
        self.fee_rate = Some(rate);
        self
    }

    /// Set naive fills flag
    pub fn naive_fills(mut self, naive: bool) -> Self {
        self.naive_fills = Some(naive);
        self
    }

    /// Set fill probability
    pub fn fill_prob(mut self, prob: f64) -> Self {
        self.fill_prob = Some(prob);
        self
    }

    /// Set queue position
    pub fn queue_pos(mut self, pos: f64) -> Self {
        self.queue_pos = Some(pos);
        self
    }

    /// Set high entropy threshold
    pub fn high_entropy(mut self, threshold: f64) -> Self {
        self.high_entropy = Some(threshold);
        self
    }

    /// Set low entropy threshold
    pub fn low_entropy(mut self, threshold: f64) -> Self {
        self.low_entropy = Some(threshold);
        self
    }

    /// Set regime params flag
    pub fn regime_params(mut self, enabled: bool) -> Self {
        self.regime_params = Some(enabled);
        self
    }

    /// Set high spread
    pub fn high_spread(mut self, spread: f64) -> Self {
        self.high_spread = Some(spread);
        self
    }

    /// Set medium spread
    pub fn med_spread(mut self, spread: f64) -> Self {
        self.med_spread = Some(spread);
        self
    }

    /// Set low spread
    pub fn low_spread(mut self, spread: f64) -> Self {
        self.low_spread = Some(spread);
        self
    }

    /// Set high skew
    pub fn high_skew(mut self, skew: f64) -> Self {
        self.high_skew = Some(skew);
        self
    }

    /// Set medium skew
    pub fn med_skew(mut self, skew: f64) -> Self {
        self.med_skew = Some(skew);
        self
    }

    /// Set low skew
    pub fn low_skew(mut self, skew: f64) -> Self {
        self.low_skew = Some(skew);
        self
    }

    /// Set quote low entropy flag
    pub fn quote_low_entropy(mut self, enabled: bool) -> Self {
        self.quote_low_entropy = Some(enabled);
        self
    }

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set JSON output flag
    pub fn json(mut self, enabled: bool) -> Self {
        self.json = Some(enabled);
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Set stats flag
    pub fn stats(mut self, enabled: bool) -> Self {
        self.stats = Some(enabled);
        self
    }

    /// Build `EvaluateParams` with validation
    pub fn build(self) -> Result<EvaluateParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;

        // Validate ranges
        if let Some(spread) = self.spread {
            if spread < 0.0 {
                anyhow::bail!("spread must be >= 0.0");
            }
        }
        if let Some(fill_prob) = self.fill_prob {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob must be in range [0.0, 1.0]");
            }
        }
        if let Some(queue_pos) = self.queue_pos {
            if !(0.0..=1.0).contains(&queue_pos) {
                anyhow::bail!("queue_pos must be in range [0.0, 1.0]");
            }
        }
        if let Some(fee_rate) = self.fee_rate {
            if fee_rate < 0.0 {
                anyhow::bail!("fee_rate must be >= 0.0");
            }
        }

        Ok(EvaluateParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            spread: self.spread.unwrap_or(2.0),
            skew: self.skew.unwrap_or(0.5),
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            fill_prob: self.fill_prob.unwrap_or(0.10),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            high_entropy: self.high_entropy.unwrap_or(0.7),
            low_entropy: self.low_entropy.unwrap_or(0.4),
            regime_params: self.regime_params.unwrap_or(false),
            high_spread: self.high_spread.unwrap_or(1.0),
            med_spread: self.med_spread.unwrap_or(2.5),
            low_spread: self.low_spread.unwrap_or(5.0),
            high_skew: self.high_skew.unwrap_or(0.3),
            med_skew: self.med_skew.unwrap_or(0.5),
            low_skew: self.low_skew.unwrap_or(1.0),
            quote_low_entropy: self.quote_low_entropy.unwrap_or(false),
            output: self.output,
            json: self.json.unwrap_or(false),
            quiet: self.quiet.unwrap_or(false),
            stats: self.stats.unwrap_or(false),
        })
    }
}

impl Default for EvaluateParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_params_builder_defaults() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        assert_eq!(params.spread, 2.0);
        assert_eq!(params.skew, 0.5);
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.high_entropy, 0.7);
        assert_eq!(params.low_entropy, 0.4);
        assert!(!params.regime_params);
        assert!(!params.json);
        assert!(!params.quiet);
        assert!(!params.stats);
    }

    #[test]
    fn test_evaluate_params_builder_validation() {
        // Missing required field
        let result = EvaluateParamsBuilder::new().build();
        assert!(result.is_err());

        // Invalid fill_prob
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.5)
            .build();
        assert!(result.is_err());

        // Invalid queue_pos
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(2.0)
            .build();
        assert!(result.is_err());

        // Invalid fee_rate
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_params_builder_custom_values() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./custom_data"))
            .algorithm("ml".to_string())
            .spread(3.5)
            .skew(0.8)
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .fill_prob(0.15)
            .queue_pos(0.3)
            .high_entropy(0.8)
            .low_entropy(0.3)
            .regime_params(true)
            .high_spread(1.5)
            .med_spread(3.0)
            .low_spread(6.0)
            .high_skew(0.4)
            .med_skew(0.6)
            .low_skew(1.2)
            .quote_low_entropy(true)
            .json(true)
            .quiet(true)
            .stats(true)
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./custom_data"));
        assert_eq!(params.algorithm, "ml");
        assert_eq!(params.spread, 3.5);
        assert_eq!(params.skew, 0.8);
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.high_entropy, 0.8);
        assert_eq!(params.low_entropy, 0.3);
        assert!(params.regime_params);
        assert_eq!(params.high_spread, 1.5);
        assert_eq!(params.med_spread, 3.0);
        assert_eq!(params.low_spread, 6.0);
        assert_eq!(params.high_skew, 0.4);
        assert_eq!(params.med_skew, 0.6);
        assert_eq!(params.low_skew, 1.2);
        assert!(params.quote_low_entropy);
        assert!(params.json);
        assert!(params.quiet);
        assert!(params.stats);
    }
}


