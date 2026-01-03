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

/// Parameters for the `tune` command (grid search - MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuneParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be MM algorithm: as, ml, or fixed)
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Spread values to test (comma-separated string, will be parsed to Vec<f64>)
    pub spreads: String,
    /// Skew values to test (comma-separated string)
    pub skews: String,
    /// High entropy threshold values to test (comma-separated string)
    pub high_entropies: String,
    /// Fill probability values to test (comma-separated string)
    pub fill_probs: String,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: f64,
    /// Use naive fill simulation (for comparison)
    pub naive_fills: bool,
    /// Queue position (0.0=front, 1.0=back)
    pub queue_pos: f64,
    /// Low entropy threshold (below = defensive/no quoting)
    pub low_entropy: f64,
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
}

/// Builder for `TuneParams` with validation
pub struct TuneParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spreads: Option<String>,
    skews: Option<String>,
    high_entropies: Option<String>,
    fill_probs: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    low_entropy: Option<f64>,
    output: Option<PathBuf>,
}

impl TuneParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            spreads: None,
            skews: None,
            high_entropies: None,
            fill_probs: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
            low_entropy: None,
            output: None,
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

    /// Set spreads (comma-separated string)
    pub fn spreads(mut self, spreads: String) -> Self {
        self.spreads = Some(spreads);
        self
    }

    /// Set skews (comma-separated string)
    pub fn skews(mut self, skews: String) -> Self {
        self.skews = Some(skews);
        self
    }

    /// Set high entropies (comma-separated string)
    pub fn high_entropies(mut self, high_entropies: String) -> Self {
        self.high_entropies = Some(high_entropies);
        self
    }

    /// Set fill probabilities (comma-separated string)
    pub fn fill_probs(mut self, fill_probs: String) -> Self {
        self.fill_probs = Some(fill_probs);
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

    /// Set queue position
    pub fn queue_pos(mut self, pos: f64) -> Self {
        self.queue_pos = Some(pos);
        self
    }

    /// Set low entropy threshold
    pub fn low_entropy(mut self, threshold: f64) -> Self {
        self.low_entropy = Some(threshold);
        self
    }

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Parse comma-separated string to Vec<f64>
    fn parse_f64_list(s: &str) -> Result<Vec<f64>> {
        let values: Vec<f64> = s
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if values.is_empty() {
            anyhow::bail!("Empty parameter list: '{}'", s);
        }
        Ok(values)
    }

    /// Build `TuneParams` with validation
    pub fn build(self) -> Result<TuneParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;
        let high_entropies = self.high_entropies
            .ok_or_else(|| anyhow::anyhow!("high_entropies is required"))?;
        let fill_probs = self.fill_probs
            .ok_or_else(|| anyhow::anyhow!("fill_probs is required"))?;

        // Parse and validate parameter lists
        let spreads_vec = Self::parse_f64_list(&spreads)
            .context("Failed to parse spreads")?;
        let _skews_vec = Self::parse_f64_list(&skews)
            .context("Failed to parse skews")?;
        let _high_entropies_vec = Self::parse_f64_list(&high_entropies)
            .context("Failed to parse high_entropies")?;
        let fill_probs_vec = Self::parse_f64_list(&fill_probs)
            .context("Failed to parse fill_probs")?;

        // Validate ranges
        for &spread in &spreads_vec {
            if spread < 0.0 {
                anyhow::bail!("spread values must be >= 0.0, found {}", spread);
            }
        }
        for &fill_prob in &fill_probs_vec {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob values must be in range [0.0, 1.0], found {}", fill_prob);
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

        Ok(TuneParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            spreads,
            skews,
            high_entropies,
            fill_probs,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            low_entropy: self.low_entropy.unwrap_or(0.4),
            output: self.output,
        })
    }
}

impl Default for TuneParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tune_params_tests {
    use super::*;

    // ============================================================================
    // Builder Defaults Tests
    // ============================================================================

    #[test]
    fn test_tune_params_builder_defaults() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .high_entropies("0.6,0.7".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build()
            .unwrap();

        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.low_entropy, 0.4);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_tune_params_missing_data_path() {
        let result = TuneParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_missing_algorithm() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_missing_spreads() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_missing_skews() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_missing_high_entropies() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_missing_fill_probs() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Parameter List Parsing Tests
    // ============================================================================

    #[test]
    fn test_tune_params_empty_spreads() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_invalid_spreads() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("invalid,not,numbers".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_mixed_valid_invalid() {
        // Should parse valid numbers and ignore invalid ones
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,invalid,2,not".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        // This will fail because after filtering invalid values, spreads might be empty
        // But if there's at least one valid value, it should work
        let result = params;
        // If it succeeds, spreads should only contain valid numbers
        if let Ok(p) = result {
            let spreads: Vec<f64> = p.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
            assert!(!spreads.is_empty());
        }
    }

    #[test]
    fn test_tune_params_whitespace_handling() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads(" 1 , 2 , 3 ".to_string())
            .skews(" 0.3 , 0.5 ".to_string())
            .high_entropies(" 0.6 , 0.7 ".to_string())
            .fill_probs(" 0.05 , 0.10 ".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(spreads, vec![1.0, 2.0, 3.0]);
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_tune_params_negative_spread() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("-1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_invalid_fill_prob() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("1.5".to_string()) // > 1.0
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_negative_fill_prob() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("-0.1".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_boundary_fill_prob() {
        // Valid boundary values
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.0,1.0".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_tune_params_invalid_queue_pos() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .queue_pos(2.0) // > 1.0
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_tune_params_invalid_fee_rate() {
        let result = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Custom Values Tests
    // ============================================================================

    #[test]
    fn test_tune_params_custom_values() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./custom_data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spreads("2,3,4".to_string())
            .skews("0.5,0.7".to_string())
            .high_entropies("0.7,0.8".to_string())
            .fill_probs("0.10,0.15".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.3)
            .low_entropy(0.3)
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./custom_data"));
        assert_eq!(params.algorithm, "ml");
        assert_eq!(params.weights_file, Some(PathBuf::from("./weights.json")));
        assert_eq!(params.spreads, "2,3,4");
        assert_eq!(params.skews, "0.5,0.7");
        assert_eq!(params.high_entropies, "0.7,0.8");
        assert_eq!(params.fill_probs, "0.10,0.15");
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.low_entropy, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./output.json")));
    }

    // ============================================================================
    // Serialization Tests
    // ============================================================================

    #[test]
    fn test_tune_params_serialization() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .high_entropies("0.6,0.7".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: TuneParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.data_path, deserialized.data_path);
        assert_eq!(params.algorithm, deserialized.algorithm);
        assert_eq!(params.spreads, deserialized.spreads);
        assert_eq!(params.skews, deserialized.skews);
        assert_eq!(params.high_entropies, deserialized.high_entropies);
        assert_eq!(params.fill_probs, deserialized.fill_probs);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_tune_params_single_values() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        // Should work with single values (1 combination)
        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(spreads.len(), 1);
    }

    #[test]
    fn test_tune_params_many_values() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads((1..=10).map(|i| i.to_string()).collect::<Vec<_>>().join(","))
            .skews((1..=5).map(|i| format!("0.{}", i)).collect::<Vec<_>>().join(","))
            .high_entropies("0.6,0.7,0.8".to_string())
            .fill_probs("0.05,0.10,0.15".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(spreads.len(), 10);
    }

    #[test]
    fn test_tune_params_very_small_values() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.0001".to_string())
            .skews("0.0001".to_string())
            .high_entropies("0.0001".to_string())
            .fill_probs("0.0001".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(spreads[0], 0.0001);
    }

    #[test]
    fn test_tune_params_very_large_values() {
        let params = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1000.0".to_string())
            .skews("100.0".to_string())
            .high_entropies("1.0".to_string())
            .fill_probs("1.0".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(spreads[0], 1000.0);
    }

    #[test]
    fn test_tune_params_clone() {
        let params1 = TuneParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .high_entropies("0.6".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .unwrap();

        let params2 = params1.clone();
        assert_eq!(params1.spreads, params2.spreads);
        assert_eq!(params1.algorithm, params2.algorithm);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Builder Defaults Tests
    // ============================================================================

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
    fn test_evaluate_params_builder_default_impl() {
        let builder1 = EvaluateParamsBuilder::new();
        let builder2 = EvaluateParamsBuilder::default();
        // Both should create equivalent builders
        assert!(builder1.data_path.is_none());
        assert!(builder2.data_path.is_none());
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_missing_data_path() {
        let result = EvaluateParamsBuilder::new()
            .algorithm("as".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_evaluate_params_missing_algorithm() {
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_evaluate_params_missing_both_required() {
        let result = EvaluateParamsBuilder::new().build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_fill_prob_validation() {
        // Valid boundary values
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(0.0)
            .build();
        assert!(params.is_ok());

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.0)
            .build();
        assert!(params.is_ok());

        // Invalid: above 1.0
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.5)
            .build();
        assert!(result.is_err());

        // Invalid: below 0.0
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(-0.1)
            .build();
        assert!(result.is_err());

        // Valid: middle value
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(0.5)
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_evaluate_params_queue_pos_validation() {
        // Valid boundary values
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(0.0)
            .build();
        assert!(params.is_ok());

        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(1.0)
            .build();
        assert!(params.is_ok());

        // Invalid: above 1.0
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(2.0)
            .build();
        assert!(result.is_err());

        // Invalid: below 0.0
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_params_fee_rate_validation() {
        // Valid: zero
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(0.0)
            .build();
        assert!(params.is_ok());

        // Valid: positive
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(0.001)
            .build();
        assert!(params.is_ok());

        // Invalid: negative
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_params_spread_validation() {
        // Valid: zero
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(0.0)
            .build();
        assert!(params.is_ok());

        // Valid: positive
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(10.0)
            .build();
        assert!(params.is_ok());

        // Invalid: negative (spread must be >= 0.0)
        let result = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(-1.0)
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Parameter Setting Tests
    // ============================================================================

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

    #[test]
    fn test_evaluate_params_builder_method_chaining() {
        // Test that all builder methods can be chained
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spread(1.0)
            .skew(0.5)
            .max_inventory(0.1)
            .quote_size(0.001)
            .fee_rate(0.0001)
            .naive_fills(false)
            .fill_prob(0.1)
            .queue_pos(0.5)
            .high_entropy(0.7)
            .low_entropy(0.4)
            .regime_params(false)
            .high_spread(1.0)
            .med_spread(2.5)
            .low_spread(5.0)
            .high_skew(0.3)
            .med_skew(0.5)
            .low_skew(1.0)
            .quote_low_entropy(false)
            .output(Some(PathBuf::from("./output.json")))
            .json(false)
            .quiet(false)
            .stats(false)
            .build()
            .unwrap();

        assert_eq!(params.weights_file, Some(PathBuf::from("./weights.json")));
        assert_eq!(params.output, Some(PathBuf::from("./output.json")));
    }

    #[test]
    fn test_evaluate_params_builder_partial_setting() {
        // Test setting only some parameters, others should use defaults
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(5.0)
            .skew(0.7)
            .build()
            .unwrap();

        assert_eq!(params.spread, 5.0);
        assert_eq!(params.skew, 0.7);
        // Other values should be defaults
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
    }

    #[test]
    fn test_evaluate_params_builder_optional_fields() {
        // Test that optional fields can be None
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(None)
            .output(None)
            .build()
            .unwrap();

        assert_eq!(params.weights_file, None);
        assert_eq!(params.output, None);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_extreme_spread_values() {
        // Very small spread
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(0.01)
            .build();
        assert!(params.is_ok());

        // Very large spread
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(1000.0)
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_evaluate_params_extreme_skew_values() {
        // Very small skew
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skew(0.01)
            .build();
        assert!(params.is_ok());

        // Very large skew
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skew(10.0)
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_evaluate_params_extreme_inventory_values() {
        // Very small inventory
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(0.0001)
            .build();
        assert!(params.is_ok());

        // Very large inventory
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(10.0)
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_evaluate_params_boundary_fill_prob() {
        // Exactly 0.0
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.as_ref().unwrap().fill_prob, 0.0);

        // Exactly 1.0
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fill_prob, 1.0);
    }

    #[test]
    fn test_evaluate_params_boundary_queue_pos() {
        // Exactly 0.0 (front of queue)
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.as_ref().unwrap().queue_pos, 0.0);

        // Exactly 1.0 (back of queue)
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().queue_pos, 1.0);
    }

    // ============================================================================
    // Boolean Flags Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_boolean_flags() {
        // All false
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(false)
            .regime_params(false)
            .quote_low_entropy(false)
            .json(false)
            .quiet(false)
            .stats(false)
            .build()
            .unwrap();

        assert!(!params.naive_fills);
        assert!(!params.regime_params);
        assert!(!params.quote_low_entropy);
        assert!(!params.json);
        assert!(!params.quiet);
        assert!(!params.stats);

        // All true
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(true)
            .regime_params(true)
            .quote_low_entropy(true)
            .json(true)
            .quiet(true)
            .stats(true)
            .build()
            .unwrap();

        assert!(params.naive_fills);
        assert!(params.regime_params);
        assert!(params.quote_low_entropy);
        assert!(params.json);
        assert!(params.quiet);
        assert!(params.stats);
    }

    // ============================================================================
    // Regime Parameters Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_regime_spreads() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .regime_params(true)
            .high_spread(1.0)
            .med_spread(2.0)
            .low_spread(3.0)
            .build()
            .unwrap();

        assert_eq!(params.high_spread, 1.0);
        assert_eq!(params.med_spread, 2.0);
        assert_eq!(params.low_spread, 3.0);
    }

    #[test]
    fn test_evaluate_params_regime_skews() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .regime_params(true)
            .high_skew(0.2)
            .med_skew(0.5)
            .low_skew(0.8)
            .build()
            .unwrap();

        assert_eq!(params.high_skew, 0.2);
        assert_eq!(params.med_skew, 0.5);
        assert_eq!(params.low_skew, 0.8);
    }

    // ============================================================================
    // Serialization Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_serialization() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(2.5)
            .skew(0.6)
            .build()
            .unwrap();

        // Test JSON serialization
        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("\"spread\":2.5"));
        assert!(json.contains("\"skew\":0.6"));
        assert!(json.contains("\"algorithm\":\"as\""));

        // Test deserialization
        let deserialized: EvaluateParams = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.spread, params.spread);
        assert_eq!(deserialized.skew, params.skew);
        assert_eq!(deserialized.algorithm, params.algorithm);
    }

    #[test]
    fn test_evaluate_params_serialization_with_optionals() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: EvaluateParams = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.weights_file, params.weights_file);
        assert_eq!(deserialized.output, params.output);
    }

    // ============================================================================
    // Path Handling Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_path_handling() {
        // Absolute path
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("/absolute/path/to/data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();
        assert!(params.data_path.is_absolute());

        // Relative path
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./relative/path"))
            .algorithm("as".to_string())
            .build()
            .unwrap();
        assert!(!params.data_path.is_absolute() || params.data_path.starts_with("."));
    }

    #[test]
    fn test_evaluate_params_weights_file_path() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .build()
            .unwrap();

        assert_eq!(params.weights_file, Some(PathBuf::from("./weights.json")));
    }

    #[test]
    fn test_evaluate_params_output_file_path() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .output(Some(PathBuf::from("./results.json")))
            .build()
            .unwrap();

        assert_eq!(params.output, Some(PathBuf::from("./results.json")));
    }

    // ============================================================================
    // Algorithm String Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_different_algorithms() {
        let algorithms = vec!["as", "ml", "fixed", "momentum"];
        for algo in algorithms {
            let params = EvaluateParamsBuilder::new()
                .data_path(PathBuf::from("./data"))
                .algorithm(algo.to_string())
                .build()
                .unwrap();
            assert_eq!(params.algorithm, algo);
        }
    }

    #[test]
    fn test_evaluate_params_empty_algorithm_string() {
        // Empty string is technically valid (validation happens at command level)
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("".to_string())
            .build()
            .unwrap();
        assert_eq!(params.algorithm, "");
    }

    // ============================================================================
    // Multiple Build Tests
    // ============================================================================


    // ============================================================================
    // Numeric Precision and Edge Cases
    // ============================================================================

    #[test]
    fn test_evaluate_params_very_small_values() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(0.0001)
            .skew(0.0001)
            .max_inventory(0.0001)
            .quote_size(0.0001)
            .fee_rate(0.000001)
            .fill_prob(0.0001)
            .queue_pos(0.0001)
            .build()
            .unwrap();

        assert_eq!(params.spread, 0.0001);
        assert_eq!(params.skew, 0.0001);
        assert_eq!(params.max_inventory, 0.0001);
    }

    #[test]
    fn test_evaluate_params_very_large_values() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(1000.0)
            .skew(100.0)
            .max_inventory(10.0)
            .quote_size(1.0)
            .fee_rate(0.1)
            .build()
            .unwrap();

        assert_eq!(params.spread, 1000.0);
        assert_eq!(params.skew, 100.0);
        assert_eq!(params.max_inventory, 10.0);
    }

    #[test]
    fn test_evaluate_params_entropy_thresholds_order() {
        // High entropy should be >= low entropy for logical consistency
        // (Note: builder doesn't enforce this, but we test the values are stored)
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_entropy(0.8)
            .low_entropy(0.3)
            .build()
            .unwrap();

        assert!(params.high_entropy > params.low_entropy);
    }

    #[test]
    fn test_evaluate_params_regime_spread_order() {
        // Typically: high_spread < med_spread < low_spread
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .regime_params(true)
            .high_spread(1.0)
            .med_spread(2.5)
            .low_spread(5.0)
            .build()
            .unwrap();

        assert!(params.high_spread < params.med_spread);
        assert!(params.med_spread < params.low_spread);
    }

    #[test]
    fn test_evaluate_params_regime_skew_order() {
        // Typically: high_skew < med_skew < low_skew
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .regime_params(true)
            .high_skew(0.3)
            .med_skew(0.5)
            .low_skew(1.0)
            .build()
            .unwrap();

        assert!(params.high_skew < params.med_skew);
        assert!(params.med_skew < params.low_skew);
    }

    // ============================================================================
    // Flag Combinations Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_all_flags_enabled() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(true)
            .regime_params(true)
            .quote_low_entropy(true)
            .json(true)
            .quiet(true)
            .stats(true)
            .build()
            .unwrap();

        assert!(params.naive_fills);
        assert!(params.regime_params);
        assert!(params.quote_low_entropy);
        assert!(params.json);
        assert!(params.quiet);
        assert!(params.stats);
    }

    #[test]
    fn test_evaluate_params_all_flags_disabled() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(false)
            .regime_params(false)
            .quote_low_entropy(false)
            .json(false)
            .quiet(false)
            .stats(false)
            .build()
            .unwrap();

        assert!(!params.naive_fills);
        assert!(!params.regime_params);
        assert!(!params.quote_low_entropy);
        assert!(!params.json);
        assert!(!params.quiet);
        assert!(!params.stats);
    }

    #[test]
    fn test_evaluate_params_naive_fills_with_fill_prob() {
        // When naive_fills is true, fill_prob and queue_pos are still stored
        // but may not be used by the backtest engine
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(true)
            .fill_prob(0.5)
            .queue_pos(0.3)
            .build()
            .unwrap();

        assert!(params.naive_fills);
        assert_eq!(params.fill_prob, 0.5);
        assert_eq!(params.queue_pos, 0.3);
    }

    // ============================================================================
    // Path Handling Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_relative_paths() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./results.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./data/features"));
        assert_eq!(params.weights_file, Some(PathBuf::from("./weights.json")));
        assert_eq!(params.output, Some(PathBuf::from("./results.json")));
    }

    #[test]
    fn test_evaluate_params_absolute_paths() {
        #[cfg(unix)]
        let abs_path = PathBuf::from("/tmp/test_data");
        #[cfg(windows)]
        let abs_path = PathBuf::from("C:\\tmp\\test_data");

        let params = EvaluateParamsBuilder::new()
            .data_path(abs_path.clone())
            .algorithm("as".to_string())
            .build()
            .unwrap();

        assert_eq!(params.data_path, abs_path);
    }

    #[test]
    fn test_evaluate_params_path_with_spaces() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data with spaces"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        assert!(params.data_path.to_string_lossy().contains("spaces"));
    }

    #[test]
    fn test_evaluate_params_empty_paths() {
        // Empty string paths are valid (though may fail at runtime)
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from(""))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from(""));
    }

    // ============================================================================
    // Algorithm String Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_algorithm_case_sensitivity() {
        // Algorithm strings are case-sensitive (validation happens later)
        let params1 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("AS".to_string())
            .build()
            .unwrap();

        let params2 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        assert_ne!(params1.algorithm, params2.algorithm);
    }

    #[test]
    fn test_evaluate_params_algorithm_whitespace() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("  as  ".to_string())
            .build()
            .unwrap();

        assert_eq!(params.algorithm, "  as  "); // Preserves whitespace
    }

    #[test]
    fn test_evaluate_params_algorithm_special_chars() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("algo-v1.2".to_string())
            .build()
            .unwrap();

        assert_eq!(params.algorithm, "algo-v1.2");
    }

    // ============================================================================
    // Serialization/Deserialization Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_roundtrip_serialization() {
        let original = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(3.0)
            .skew(0.7)
            .max_inventory(0.15)
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

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: EvaluateParams = serde_json::from_str(&json).unwrap();

        assert_eq!(original.data_path, deserialized.data_path);
        assert_eq!(original.algorithm, deserialized.algorithm);
        assert_eq!(original.spread, deserialized.spread);
        assert_eq!(original.skew, deserialized.skew);
        assert_eq!(original.max_inventory, deserialized.max_inventory);
        assert_eq!(original.quote_size, deserialized.quote_size);
        assert_eq!(original.fee_rate, deserialized.fee_rate);
        assert_eq!(original.naive_fills, deserialized.naive_fills);
        assert_eq!(original.fill_prob, deserialized.fill_prob);
        assert_eq!(original.queue_pos, deserialized.queue_pos);
        assert_eq!(original.high_entropy, deserialized.high_entropy);
        assert_eq!(original.low_entropy, deserialized.low_entropy);
        assert_eq!(original.regime_params, deserialized.regime_params);
        assert_eq!(original.high_spread, deserialized.high_spread);
        assert_eq!(original.med_spread, deserialized.med_spread);
        assert_eq!(original.low_spread, deserialized.low_spread);
        assert_eq!(original.high_skew, deserialized.high_skew);
        assert_eq!(original.med_skew, deserialized.med_skew);
        assert_eq!(original.low_skew, deserialized.low_skew);
        assert_eq!(original.quote_low_entropy, deserialized.quote_low_entropy);
        assert_eq!(original.json, deserialized.json);
        assert_eq!(original.quiet, deserialized.quiet);
        assert_eq!(original.stats, deserialized.stats);
    }

    #[test]
    fn test_evaluate_params_serialization_with_none_optionals() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(None)
            .output(None)
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: EvaluateParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.weights_file, deserialized.weights_file);
        assert_eq!(params.output, deserialized.output);
    }

    #[test]
    fn test_evaluate_params_serialization_with_some_optionals() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: EvaluateParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.weights_file, deserialized.weights_file);
        assert_eq!(params.output, deserialized.output);
    }

    // ============================================================================
    // Builder Method Chaining Edge Cases
    // ============================================================================

    #[test]
    fn test_evaluate_params_builder_set_twice() {
        // Setting the same parameter twice should use the last value
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(2.0)
            .spread(5.0) // Override
            .build()
            .unwrap();

        assert_eq!(params.spread, 5.0);
    }

    #[test]
    fn test_evaluate_params_builder_all_methods() {
        // Test that all builder methods can be called
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spread(2.0)
            .skew(0.5)
            .max_inventory(0.1)
            .quote_size(0.001)
            .fee_rate(0.0001)
            .naive_fills(false)
            .fill_prob(0.10)
            .queue_pos(0.5)
            .high_entropy(0.7)
            .low_entropy(0.4)
            .regime_params(false)
            .high_spread(1.0)
            .med_spread(2.5)
            .low_spread(5.0)
            .high_skew(0.3)
            .med_skew(0.5)
            .low_skew(1.0)
            .quote_low_entropy(false)
            .output(Some(PathBuf::from("./output.json")))
            .json(false)
            .quiet(false)
            .stats(false)
            .build()
            .unwrap();

        assert_eq!(params.spread, 2.0);
        assert_eq!(params.skew, 0.5);
    }

    // ============================================================================
    // Clone and Debug Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_clone() {
        let params1 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(3.0)
            .build()
            .unwrap();

        let params2 = params1.clone();
        assert_eq!(params1.spread, params2.spread);
        assert_eq!(params1.algorithm, params2.algorithm);
    }

    #[test]
    fn test_evaluate_params_debug_format() {
        let params = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        let debug_str = format!("{:?}", params);
        assert!(debug_str.contains("EvaluateParams"));
        assert!(debug_str.contains("as"));
    }

    // ============================================================================
    // Default Value Consistency Tests
    // ============================================================================

    #[test]
    fn test_evaluate_params_defaults_consistency() {
        // Test that defaults match expected values from CLI
        let params1 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build()
            .unwrap();

        let params2 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(2.0)
            .skew(0.5)
            .max_inventory(0.1)
            .quote_size(0.001)
            .fee_rate(0.0001)
            .naive_fills(false)
            .fill_prob(0.10)
            .queue_pos(0.5)
            .high_entropy(0.7)
            .low_entropy(0.4)
            .regime_params(false)
            .high_spread(1.0)
            .med_spread(2.5)
            .low_spread(5.0)
            .high_skew(0.3)
            .med_skew(0.5)
            .low_skew(1.0)
            .quote_low_entropy(false)
            .json(false)
            .quiet(false)
            .stats(false)
            .build()
            .unwrap();

        assert_eq!(params1.spread, params2.spread);
        assert_eq!(params1.skew, params2.skew);
        assert_eq!(params1.max_inventory, params2.max_inventory);
        assert_eq!(params1.quote_size, params2.quote_size);
        assert_eq!(params1.fee_rate, params2.fee_rate);
        assert_eq!(params1.naive_fills, params2.naive_fills);
        assert_eq!(params1.fill_prob, params2.fill_prob);
        assert_eq!(params1.queue_pos, params2.queue_pos);
        assert_eq!(params1.high_entropy, params2.high_entropy);
        assert_eq!(params1.low_entropy, params2.low_entropy);
    }

    #[test]
    fn test_evaluate_params_builder_reuse() {
        // Build first params
        let params1 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(2.0)
            .build()
            .unwrap();

        // Build second params with different values (new builder)
        let params2 = EvaluateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread(3.0)
            .build()
            .unwrap();

        assert_eq!(params1.spread, 2.0);
        assert_eq!(params2.spread, 3.0);
        assert_eq!(params1.algorithm, "as");
        assert_eq!(params2.algorithm, "ml");
    }
}


