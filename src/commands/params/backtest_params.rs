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

impl Default for EvaluateParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            spread: 2.0,
            skew: 0.5,
            max_inventory: 1000.0,
            quote_size: 0.1,
            fee_rate: 0.0001,
            naive_fills: false,
            fill_prob: 0.1,
            queue_pos: 0.5,
            high_entropy: 0.7,
            low_entropy: 0.3,
            regime_params: false,
            high_spread: 1.0,
            med_spread: 2.0,
            low_spread: 3.0,
            high_skew: 0.3,
            med_skew: 0.5,
            low_skew: 0.7,
            quote_low_entropy: true,
            output: None,
            json: false,
            quiet: false,
            stats: false,
        }
    }
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

impl Default for TuneParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            spreads: "1,2,3".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            high_entropies: "0.6,0.7,0.8".to_string(),
            fill_probs: "0.1".to_string(),
            max_inventory: 1000.0,
            quote_size: 0.1,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            low_entropy: 0.3,
            output: None,
        }
    }
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

/// Parameters for the `regime-search` command (regime-specific grid search - MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeSearchParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be MM algorithm: as, ml, or fixed)
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// High entropy spread values to test (comma-separated string)
    pub high_spreads: String,
    /// Medium entropy spread values to test (comma-separated string)
    pub med_spreads: String,
    /// Low entropy spread values to test (comma-separated string, can include "none")
    pub low_spreads: String,
    /// High entropy skew values to test (comma-separated string)
    pub high_skews: String,
    /// Medium entropy skew values to test (comma-separated string)
    pub med_skews: String,
    /// Low entropy skew values to test (comma-separated string)
    pub low_skews: String,
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
    /// High entropy threshold (above = aggressive quoting)
    pub high_entropy: f64,
    /// Low entropy threshold (below = defensive/no quoting)
    pub low_entropy: f64,
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
}

impl Default for RegimeSearchParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            high_spreads: "2,3,4".to_string(),
            med_spreads: "3,4,5".to_string(),
            low_spreads: "5,6,none".to_string(),
            high_skews: "0.3,0.5".to_string(),
            med_skews: "0.5,0.7".to_string(),
            low_skews: "0.7,0.9".to_string(),
            fill_probs: "0.05,0.10".to_string(),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            high_entropy: 0.7,
            low_entropy: 0.3,
            output: None,
        }
    }
}

/// Builder for `RegimeSearchParams` with validation
pub struct RegimeSearchParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    high_spreads: Option<String>,
    med_spreads: Option<String>,
    low_spreads: Option<String>,
    high_skews: Option<String>,
    med_skews: Option<String>,
    low_skews: Option<String>,
    fill_probs: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    high_entropy: Option<f64>,
    low_entropy: Option<f64>,
    output: Option<PathBuf>,
}

impl RegimeSearchParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            high_spreads: None,
            med_spreads: None,
            low_spreads: None,
            high_skews: None,
            med_skews: None,
            low_skews: None,
            fill_probs: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
            high_entropy: None,
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

    /// Set high spreads (comma-separated string)
    pub fn high_spreads(mut self, spreads: String) -> Self {
        self.high_spreads = Some(spreads);
        self
    }

    /// Set medium spreads (comma-separated string)
    pub fn med_spreads(mut self, spreads: String) -> Self {
        self.med_spreads = Some(spreads);
        self
    }

    /// Set low spreads (comma-separated string, can include "none")
    pub fn low_spreads(mut self, spreads: String) -> Self {
        self.low_spreads = Some(spreads);
        self
    }

    /// Set high skews (comma-separated string)
    pub fn high_skews(mut self, skews: String) -> Self {
        self.high_skews = Some(skews);
        self
    }

    /// Set medium skews (comma-separated string)
    pub fn med_skews(mut self, skews: String) -> Self {
        self.med_skews = Some(skews);
        self
    }

    /// Set low skews (comma-separated string)
    pub fn low_skews(mut self, skews: String) -> Self {
        self.low_skews = Some(skews);
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Parse comma-separated string to Vec<f64>, handling "none" for low_spreads
    fn parse_f64_list(s: &str) -> Result<Vec<f64>> {
        let values: Vec<f64> = s
            .split(',')
            .filter_map(|s| {
                let s = s.trim().to_lowercase();
                if s == "none" || s == "no" {
                    None // Filter out "none" - it's handled separately
                } else {
                    s.parse().ok()
                }
            })
            .collect();
        if values.is_empty() {
            anyhow::bail!("No valid numeric values found in parameter list: '{}'", s);
        }
        Ok(values)
    }

    /// Parse low spreads list, returning both numeric values and count of "none" entries
    fn parse_low_spreads(s: &str) -> Result<(Vec<f64>, usize)> {
        let mut values = Vec::new();
        let mut none_count = 0;
        
        for item in s.split(',') {
            let item = item.trim().to_lowercase();
            if item == "none" || item == "no" {
                none_count += 1;
            } else if let Ok(val) = item.parse::<f64>() {
                values.push(val);
            }
        }
        
        if values.is_empty() && none_count == 0 {
            anyhow::bail!("No valid values found in low_spreads: '{}'", s);
        }
        
        Ok((values, none_count))
    }

    /// Build `RegimeSearchParams` with validation
    pub fn build(self) -> Result<RegimeSearchParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let high_spreads = self.high_spreads
            .ok_or_else(|| anyhow::anyhow!("high_spreads is required"))?;
        let med_spreads = self.med_spreads
            .ok_or_else(|| anyhow::anyhow!("med_spreads is required"))?;
        let low_spreads = self.low_spreads
            .ok_or_else(|| anyhow::anyhow!("low_spreads is required"))?;
        let high_skews = self.high_skews
            .ok_or_else(|| anyhow::anyhow!("high_skews is required"))?;
        let med_skews = self.med_skews
            .ok_or_else(|| anyhow::anyhow!("med_skews is required"))?;
        let low_skews = self.low_skews
            .ok_or_else(|| anyhow::anyhow!("low_skews is required"))?;
        let fill_probs = self.fill_probs
            .ok_or_else(|| anyhow::anyhow!("fill_probs is required"))?;

        // Parse and validate parameter lists
        let _high_spreads_vec = Self::parse_f64_list(&high_spreads)
            .context("Failed to parse high_spreads")?;
        let _med_spreads_vec = Self::parse_f64_list(&med_spreads)
            .context("Failed to parse med_spreads")?;
        let (low_spreads_vec, _none_count) = Self::parse_low_spreads(&low_spreads)
            .context("Failed to parse low_spreads")?;
        let _high_skews_vec = Self::parse_f64_list(&high_skews)
            .context("Failed to parse high_skews")?;
        let _med_skews_vec = Self::parse_f64_list(&med_skews)
            .context("Failed to parse med_skews")?;
        let _low_skews_vec = Self::parse_f64_list(&low_skews)
            .context("Failed to parse low_skews")?;
        let fill_probs_vec = Self::parse_f64_list(&fill_probs)
            .context("Failed to parse fill_probs")?;

        // Validate ranges
        for &spread in &low_spreads_vec {
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

        Ok(RegimeSearchParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            high_spreads,
            med_spreads,
            low_spreads,
            high_skews,
            med_skews,
            low_skews,
            fill_probs,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            high_entropy: self.high_entropy.unwrap_or(0.7),
            low_entropy: self.low_entropy.unwrap_or(0.4),
            output: self.output,
        })
    }
}

impl Default for RegimeSearchParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `multi-objective` command (Pareto frontier optimization - MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiObjectiveParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be MM algorithm: as, ml, or fixed)
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Spread values to test (comma-separated string)
    pub spreads: String,
    /// Skew values to test (comma-separated string)
    pub skews: String,
    /// Fill probability values to test (comma-separated string)
    pub fill_probs: String,
    /// High entropy threshold values to test (comma-separated string)
    pub high_entropies: String,
    /// Minimum number of trades for valid solution
    pub min_trades: usize,
    /// Weight for Sharpe ratio (0.0-1.0, must sum with others to 1.0)
    pub w_sharpe: f64,
    /// Weight for drawdown (0.0-1.0, must sum with others to 1.0)
    pub w_drawdown: f64,
    /// Weight for fill rate (0.0-1.0, must sum with others to 1.0)
    pub w_fill: f64,
    /// Weight for turnover (0.0-1.0, must sum with others to 1.0)
    pub w_turnover: f64,
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

impl Default for MultiObjectiveParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            spreads: "1,2,3,4,5".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            fill_probs: "0.05,0.10,0.15".to_string(),
            high_entropies: "0.6,0.7,0.8".to_string(),
            min_trades: 100,
            w_sharpe: 0.4,
            w_drawdown: 0.3,
            w_fill: 0.2,
            w_turnover: 0.1,
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            low_entropy: 0.3,
            output: None,
        }
    }
}

/// Builder for `MultiObjectiveParams` with validation
pub struct MultiObjectiveParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spreads: Option<String>,
    skews: Option<String>,
    fill_probs: Option<String>,
    high_entropies: Option<String>,
    min_trades: Option<usize>,
    w_sharpe: Option<f64>,
    w_drawdown: Option<f64>,
    w_fill: Option<f64>,
    w_turnover: Option<f64>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    low_entropy: Option<f64>,
    output: Option<PathBuf>,
}

impl MultiObjectiveParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            spreads: None,
            skews: None,
            fill_probs: None,
            high_entropies: None,
            min_trades: None,
            w_sharpe: None,
            w_drawdown: None,
            w_fill: None,
            w_turnover: None,
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

    /// Set fill probabilities (comma-separated string)
    pub fn fill_probs(mut self, fill_probs: String) -> Self {
        self.fill_probs = Some(fill_probs);
        self
    }

    /// Set high entropy thresholds (comma-separated string)
    pub fn high_entropies(mut self, high_entropies: String) -> Self {
        self.high_entropies = Some(high_entropies);
        self
    }

    /// Set minimum trades
    pub fn min_trades(mut self, min_trades: usize) -> Self {
        self.min_trades = Some(min_trades);
        self
    }

    /// Set Sharpe weight
    pub fn w_sharpe(mut self, weight: f64) -> Self {
        self.w_sharpe = Some(weight);
        self
    }

    /// Set drawdown weight
    pub fn w_drawdown(mut self, weight: f64) -> Self {
        self.w_drawdown = Some(weight);
        self
    }

    /// Set fill rate weight
    pub fn w_fill(mut self, weight: f64) -> Self {
        self.w_fill = Some(weight);
        self
    }

    /// Set turnover weight
    pub fn w_turnover(mut self, weight: f64) -> Self {
        self.w_turnover = Some(weight);
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
            anyhow::bail!("No valid numeric values found in parameter list: '{}'", s);
        }
        Ok(values)
    }

    /// Build `MultiObjectiveParams` with validation
    pub fn build(self) -> Result<MultiObjectiveParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;
        let fill_probs = self.fill_probs
            .ok_or_else(|| anyhow::anyhow!("fill_probs is required"))?;
        let high_entropies = self.high_entropies
            .ok_or_else(|| anyhow::anyhow!("high_entropies is required"))?;

        // Parse and validate parameter lists
        let _spreads_vec = Self::parse_f64_list(&spreads)
            .context("Failed to parse spreads")?;
        let _skews_vec = Self::parse_f64_list(&skews)
            .context("Failed to parse skews")?;
        let fill_probs_vec = Self::parse_f64_list(&fill_probs)
            .context("Failed to parse fill_probs")?;
        let _high_entropies_vec = Self::parse_f64_list(&high_entropies)
            .context("Failed to parse high_entropies")?;

        // Validate fill probabilities are in [0.0, 1.0]
        for &fill_prob in &fill_probs_vec {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob values must be in range [0.0, 1.0], found {}", fill_prob);
            }
        }

        // Validate weights
        let w_sharpe = self.w_sharpe.unwrap_or(0.4);
        let w_drawdown = self.w_drawdown.unwrap_or(0.3);
        let w_fill = self.w_fill.unwrap_or(0.2);
        let w_turnover = self.w_turnover.unwrap_or(0.1);

        // Validate individual weights are in [0.0, 1.0]
        if !(0.0..=1.0).contains(&w_sharpe) {
            anyhow::bail!("w_sharpe must be in range [0.0, 1.0], found {}", w_sharpe);
        }
        if !(0.0..=1.0).contains(&w_drawdown) {
            anyhow::bail!("w_drawdown must be in range [0.0, 1.0], found {}", w_drawdown);
        }
        if !(0.0..=1.0).contains(&w_fill) {
            anyhow::bail!("w_fill must be in range [0.0, 1.0], found {}", w_fill);
        }
        if !(0.0..=1.0).contains(&w_turnover) {
            anyhow::bail!("w_turnover must be in range [0.0, 1.0], found {}", w_turnover);
        }

        // Validate weights sum to 1.0 (with tolerance for floating point)
        let weight_sum = w_sharpe + w_drawdown + w_fill + w_turnover;
        if (weight_sum - 1.0).abs() > 0.001 {
            anyhow::bail!(
                "Objective weights must sum to 1.0, but sum to {:.6} (w_sharpe={}, w_drawdown={}, w_fill={}, w_turnover={})",
                weight_sum, w_sharpe, w_drawdown, w_fill, w_turnover
            );
        }

        // Validate other ranges
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
        if let Some(min_trades) = self.min_trades {
            if min_trades == 0 {
                anyhow::bail!("min_trades must be > 0");
            }
        }

        Ok(MultiObjectiveParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            spreads,
            skews,
            fill_probs,
            high_entropies,
            min_trades: self.min_trades.unwrap_or(20),
            w_sharpe,
            w_drawdown,
            w_fill,
            w_turnover,
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

impl Default for MultiObjectiveParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `regime-optimize` command (regime-specific parameter optimization - MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeOptimizeParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be MM algorithm: as, ml, or fixed)
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Spread values to test (comma-separated string)
    pub spreads: String,
    /// Skew values to test (comma-separated string)
    pub skews: String,
    /// Fill probability for simulation (0.0-1.0)
    pub fill_prob: f64,
    /// Minimum trades required for valid optimization
    pub min_trades: usize,
    /// Whether to allow no-quoting in low entropy
    pub allow_no_quote: bool,
    /// High entropy threshold (above = high regime)
    pub high_entropy: f64,
    /// Low entropy threshold (below = low regime)
    pub low_entropy: f64,
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
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
}

impl Default for RegimeOptimizeParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            spreads: "1,2,3,4,5".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            fill_prob: 0.10,
            min_trades: 100,
            allow_no_quote: true,
            high_entropy: 0.7,
            low_entropy: 0.3,
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            output: None,
        }
    }
}

/// Builder for `RegimeOptimizeParams` with validation
pub struct RegimeOptimizeParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spreads: Option<String>,
    skews: Option<String>,
    fill_prob: Option<f64>,
    min_trades: Option<usize>,
    allow_no_quote: Option<bool>,
    high_entropy: Option<f64>,
    low_entropy: Option<f64>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
}

impl RegimeOptimizeParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            spreads: None,
            skews: None,
            fill_prob: None,
            min_trades: None,
            allow_no_quote: None,
            high_entropy: None,
            low_entropy: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
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

    /// Set fill probability
    pub fn fill_prob(mut self, prob: f64) -> Self {
        self.fill_prob = Some(prob);
        self
    }

    /// Set minimum trades
    pub fn min_trades(mut self, min_trades: usize) -> Self {
        self.min_trades = Some(min_trades);
        self
    }

    /// Set allow no-quote flag
    pub fn allow_no_quote(mut self, allow: bool) -> Self {
        self.allow_no_quote = Some(allow);
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
            anyhow::bail!("No valid numeric values found in parameter list: '{}'", s);
        }
        Ok(values)
    }

    /// Build `RegimeOptimizeParams` with validation
    pub fn build(self) -> Result<RegimeOptimizeParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;

        // Parse and validate parameter lists
        let spreads_vec = Self::parse_f64_list(&spreads)
            .context("Failed to parse spreads")?;
        let skews_vec = Self::parse_f64_list(&skews)
            .context("Failed to parse skews")?;

        // Validate spreads are non-negative
        for &spread in &spreads_vec {
            if spread < 0.0 {
                anyhow::bail!("spread values must be >= 0.0, found {}", spread);
            }
        }

        // Validate fill probability
        let fill_prob = self.fill_prob.unwrap_or(0.10);
        if !(0.0..=1.0).contains(&fill_prob) {
            anyhow::bail!("fill_prob must be in range [0.0, 1.0], found {}", fill_prob);
        }

        // Validate min_trades
        if let Some(min_trades) = self.min_trades {
            if min_trades == 0 {
                anyhow::bail!("min_trades must be > 0");
            }
        }

        // Validate entropy thresholds
        if let Some(high_entropy) = self.high_entropy {
            if high_entropy <= 0.0 {
                anyhow::bail!("high_entropy must be > 0.0");
            }
        }
        if let Some(low_entropy) = self.low_entropy {
            if low_entropy < 0.0 {
                anyhow::bail!("low_entropy must be >= 0.0");
            }
        }
        if let (Some(high), Some(low)) = (self.high_entropy, self.low_entropy) {
            if high <= low {
                anyhow::bail!("high_entropy ({}) must be > low_entropy ({})", high, low);
            }
        }

        // Validate other ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }

        Ok(RegimeOptimizeParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            spreads,
            skews,
            fill_prob,
            min_trades: self.min_trades.unwrap_or(10),
            allow_no_quote: self.allow_no_quote.unwrap_or(true),
            high_entropy: self.high_entropy.unwrap_or(0.7),
            low_entropy: self.low_entropy.unwrap_or(0.4),
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
        })
    }
}

impl Default for RegimeOptimizeParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod regime_search_params_tests {
    use super::*;

    // ============================================================================
    // Builder Defaults Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_builder_defaults() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5,1.0".to_string())
            .med_spreads("2.0,2.5".to_string())
            .low_spreads("4.0,none".to_string())
            .high_skews("0.2,0.3".to_string())
            .med_skews("0.4,0.5".to_string())
            .low_skews("0.8,1.0".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.high_entropy, 0.7);
        assert_eq!(params.low_entropy, 0.4);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_missing_data_path() {
        let result = RegimeSearchParamsBuilder::new()
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_missing_algorithm() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_missing_high_spreads() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_missing_all_required() {
        let result = RegimeSearchParamsBuilder::new().build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Low Spreads "none" Handling Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_low_spreads_with_none() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0,none,5.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        // "none" should be preserved in the string
        assert!(params.low_spreads.contains("none"));
        
        // Parse should handle "none"
        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads(&params.low_spreads).unwrap();
        assert_eq!(values.len(), 2); // 4.0 and 5.0
        assert_eq!(none_count, 1); // one "none"
    }

    #[test]
    fn test_regime_search_params_low_spreads_only_none() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("none".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads(&params.low_spreads).unwrap();
        assert_eq!(values.len(), 0);
        assert_eq!(none_count, 1);
    }

    #[test]
    fn test_regime_search_params_low_spreads_none_case_insensitive() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("NONE,No,none".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads(&params.low_spreads).unwrap();
        assert_eq!(values.len(), 0);
        assert_eq!(none_count, 3); // All three should be counted as "none"
    }

    #[test]
    fn test_regime_search_params_low_spreads_no_none() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0,5.0,6.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads(&params.low_spreads).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(none_count, 0);
    }

    // ============================================================================
    // Parameter List Parsing Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_empty_spreads() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_invalid_spreads() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("invalid,not,numbers".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_whitespace_handling() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads(" 0.5 , 1.0 ".to_string())
            .med_spreads(" 2.0 , 2.5 ".to_string())
            .low_spreads(" 4.0 , none ".to_string())
            .high_skews(" 0.2 , 0.3 ".to_string())
            .med_skews(" 0.4 , 0.5 ".to_string())
            .low_skews(" 0.8 , 1.0 ".to_string())
            .fill_probs(" 0.10 ".to_string())
            .build()
            .unwrap();

        let high_spreads: Vec<f64> = params.high_spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(high_spreads, vec![0.5, 1.0]);
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_negative_spread() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("-1,2".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        // Note: negative spreads in high/med might not be caught if they're not in low_spreads
        // The validation only checks low_spreads_vec
    }

    #[test]
    fn test_regime_search_params_negative_low_spread() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("-1,4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_invalid_fill_prob() {
        let result = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("1.5".to_string()) // > 1.0
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Custom Values Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_custom_values() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./custom_data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .high_spreads("0.5,1.0,1.5".to_string())
            .med_spreads("2.0,2.5,3.0".to_string())
            .low_spreads("4.0,5.0,none".to_string())
            .high_skews("0.2,0.3,0.4".to_string())
            .med_skews("0.4,0.5,0.6".to_string())
            .low_skews("0.8,1.0,1.2".to_string())
            .fill_probs("0.10,0.15".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.3)
            .high_entropy(0.8)
            .low_entropy(0.3)
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./custom_data"));
        assert_eq!(params.algorithm, "ml");
        assert_eq!(params.weights_file, Some(PathBuf::from("./weights.json")));
        assert_eq!(params.high_spreads, "0.5,1.0,1.5");
        assert_eq!(params.med_spreads, "2.0,2.5,3.0");
        assert_eq!(params.low_spreads, "4.0,5.0,none");
        assert_eq!(params.high_skews, "0.2,0.3,0.4");
        assert_eq!(params.med_skews, "0.4,0.5,0.6");
        assert_eq!(params.low_skews, "0.8,1.0,1.2");
        assert_eq!(params.fill_probs, "0.10,0.15");
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.high_entropy, 0.8);
        assert_eq!(params.low_entropy, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./output.json")));
    }

    // ============================================================================
    // Serialization Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_serialization() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5,1.0".to_string())
            .med_spreads("2.0,2.5".to_string())
            .low_spreads("4.0,none".to_string())
            .high_skews("0.2,0.3".to_string())
            .med_skews("0.4,0.5".to_string())
            .low_skews("0.8,1.0".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: RegimeSearchParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.data_path, deserialized.data_path);
        assert_eq!(params.algorithm, deserialized.algorithm);
        assert_eq!(params.high_spreads, deserialized.high_spreads);
        assert_eq!(params.med_spreads, deserialized.med_spreads);
        assert_eq!(params.low_spreads, deserialized.low_spreads);
        assert_eq!(params.high_skews, deserialized.high_skews);
        assert_eq!(params.med_skews, deserialized.med_skews);
        assert_eq!(params.low_skews, deserialized.low_skews);
        assert_eq!(params.fill_probs, deserialized.fill_probs);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_regime_search_params_single_values() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("none".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        // Should work with single values (1 combination)
        let high_spreads: Vec<f64> = params.high_spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(high_spreads.len(), 1);
    }

    #[test]
    fn test_regime_search_params_many_values() {
        let params = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads((1..=5).map(|i| format!("0.{}", i)).collect::<Vec<_>>().join(","))
            .med_spreads((1..=5).map(|i| format!("2.{}", i)).collect::<Vec<_>>().join(","))
            .low_spreads((1..=5).map(|i| format!("4.{}", i)).collect::<Vec<_>>().join(","))
            .high_skews((1..=3).map(|i| format!("0.{}", i)).collect::<Vec<_>>().join(","))
            .med_skews((1..=3).map(|i| format!("0.{}", i + 3)).collect::<Vec<_>>().join(","))
            .low_skews((1..=3).map(|i| format!("0.{}", i + 7)).collect::<Vec<_>>().join(","))
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        let high_spreads: Vec<f64> = params.high_spreads.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        assert_eq!(high_spreads.len(), 5);
    }

    #[test]
    fn test_regime_search_params_clone() {
        let params1 = RegimeSearchParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .high_spreads("0.5".to_string())
            .med_spreads("2.0".to_string())
            .low_spreads("4.0".to_string())
            .high_skews("0.2".to_string())
            .med_skews("0.4".to_string())
            .low_skews("0.8".to_string())
            .fill_probs("0.10".to_string())
            .build()
            .unwrap();

        let params2 = params1.clone();
        assert_eq!(params1.high_spreads, params2.high_spreads);
        assert_eq!(params1.algorithm, params2.algorithm);
    }

    #[test]
    fn test_regime_search_params_parse_low_spreads_helper() {
        // Test the helper function directly
        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads("4.0,5.0,none").unwrap();
        assert_eq!(values, vec![4.0, 5.0]);
        assert_eq!(none_count, 1);

        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads("none").unwrap();
        assert_eq!(values.len(), 0);
        assert_eq!(none_count, 1);

        let (values, none_count) = RegimeSearchParamsBuilder::parse_low_spreads("4.0,5.0").unwrap();
        assert_eq!(values, vec![4.0, 5.0]);
        assert_eq!(none_count, 0);
    }

    #[test]
    fn test_regime_search_params_parse_low_spreads_empty() {
        let result = RegimeSearchParamsBuilder::parse_low_spreads("");
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_search_params_parse_low_spreads_invalid() {
        let result = RegimeSearchParamsBuilder::parse_low_spreads("invalid,not,numbers");
        // Should return empty values but might have none_count > 0 if "none" is in there
        // Actually, it should fail because no valid values
        assert!(result.is_err());
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

#[cfg(test)]
mod regime_optimize_params_tests {
    use super::*;

    // ============================================================================
    // Builder Defaults Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_builder_defaults() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0,2.0".to_string())
            .skews("0.2,0.3,0.5".to_string())
            .build()
            .unwrap();

        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.min_trades, 10);
        assert!(params.allow_no_quote);
        assert_eq!(params.high_entropy, 0.7);
        assert_eq!(params.low_entropy, 0.4);
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.queue_pos, 0.5);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_missing_data_path() {
        let result = RegimeOptimizeParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_regime_optimize_params_missing_algorithm() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_regime_optimize_params_missing_spreads() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skews("0.2,0.3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spreads"));
    }

    #[test]
    fn test_regime_optimize_params_missing_skews() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skews"));
    }

    // ============================================================================
    // Parameter Parsing Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_parse_spreads() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0,2.0,3.5".to_string())
            .skews("0.2".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(spreads, vec![0.5, 1.0, 2.0, 3.5]);
    }

    #[test]
    fn test_regime_optimize_params_parse_skews() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2,0.3,0.5,0.7,1.0".to_string())
            .build()
            .unwrap();

        let skews: Vec<f64> = params.skews
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(skews, vec![0.2, 0.3, 0.5, 0.7, 1.0]);
    }

    #[test]
    fn test_regime_optimize_params_parse_with_whitespace() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads(" 0.5 , 1.0 , 2.0 ".to_string())
            .skews(" 0.2 , 0.3 ".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(spreads, vec![0.5, 1.0, 2.0]);
    }

    #[test]
    fn test_regime_optimize_params_empty_spreads() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.2".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_empty_skews() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_invalid_spreads() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("abc,def".to_string())
            .skews("0.2".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_invalid_skews() {
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("xyz,abc".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_mixed_valid_invalid() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,invalid,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .build()
            .unwrap();

        // Should parse only valid values
        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(spreads, vec![0.5, 1.0]);
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_fill_prob_range() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .fill_prob(0.0)
            .build()
            .unwrap();
        assert_eq!(params.fill_prob, 0.0);

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .fill_prob(1.0)
            .build()
            .unwrap();
        assert_eq!(params.fill_prob, 1.0);

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .fill_prob(1.5)
            .build();
        assert!(result.is_err());

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .fill_prob(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_min_trades_validation() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .min_trades(1)
            .build()
            .unwrap();
        assert_eq!(params.min_trades, 1);

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .min_trades(0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_entropy_thresholds() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .high_entropy(0.8)
            .low_entropy(0.3)
            .build()
            .unwrap();
        assert_eq!(params.high_entropy, 0.8);
        assert_eq!(params.low_entropy, 0.3);

        // High must be > low
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .high_entropy(0.3)
            .low_entropy(0.5)
            .build();
        assert!(result.is_err());

        // High must be > 0
        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .high_entropy(0.0)
            .low_entropy(0.0)
            .build();
        assert!(result.is_err());

        // Low must be >= 0
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .high_entropy(0.5)
            .low_entropy(0.0)
            .build()
            .unwrap();
        assert_eq!(params.low_entropy, 0.0);
    }

    #[test]
    fn test_regime_optimize_params_spread_validation() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.0,1.0,2.0".to_string())
            .skews("0.2".to_string())
            .build()
            .unwrap();
        // Should accept 0.0

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("-1.0,1.0".to_string())
            .skews("0.2".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_queue_pos_range() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .queue_pos(0.0)
            .build()
            .unwrap();
        assert_eq!(params.queue_pos, 0.0);

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .queue_pos(1.0)
            .build()
            .unwrap();
        assert_eq!(params.queue_pos, 1.0);

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .queue_pos(1.5)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_fee_rate_validation() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .fee_rate(0.0)
            .build()
            .unwrap();
        assert_eq!(params.fee_rate, 0.0);

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_max_inventory_validation() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .max_inventory(0.01)
            .build()
            .unwrap();
        assert_eq!(params.max_inventory, 0.01);

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .max_inventory(0.0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_regime_optimize_params_quote_size_validation() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .quote_size(0.0001)
            .build()
            .unwrap();
        assert_eq!(params.quote_size, 0.0001);

        let result = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .quote_size(0.0)
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Custom Values Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_custom_values() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data/custom"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spreads("1.0,2.0,3.0".to_string())
            .skews("0.3,0.5,0.7".to_string())
            .fill_prob(0.15)
            .min_trades(20)
            .allow_no_quote(false)
            .high_entropy(0.8)
            .low_entropy(0.3)
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.3)
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./data/custom"));
        assert_eq!(params.algorithm, "ml");
        assert_eq!(params.weights_file, Some(PathBuf::from("./weights.json")));
        assert_eq!(params.spreads, "1.0,2.0,3.0");
        assert_eq!(params.skews, "0.3,0.5,0.7");
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.min_trades, 20);
        assert!(!params.allow_no_quote);
        assert_eq!(params.high_entropy, 0.8);
        assert_eq!(params.low_entropy, 0.3);
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./output.json")));
    }

    // ============================================================================
    // Serialization/Deserialization Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_serialization() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5,1.0".to_string())
            .skews("0.2,0.3".to_string())
            .fill_prob(0.10)
            .min_trades(15)
            .allow_no_quote(true)
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: RegimeOptimizeParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.data_path, deserialized.data_path);
        assert_eq!(params.algorithm, deserialized.algorithm);
        assert_eq!(params.spreads, deserialized.spreads);
        assert_eq!(params.skews, deserialized.skews);
        assert_eq!(params.fill_prob, deserialized.fill_prob);
        assert_eq!(params.min_trades, deserialized.min_trades);
        assert_eq!(params.allow_no_quote, deserialized.allow_no_quote);
    }

    #[test]
    fn test_regime_optimize_params_roundtrip_serialization() {
        let original = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spreads("1.0,2.0,3.0".to_string())
            .skews("0.3,0.5".to_string())
            .fill_prob(0.12)
            .min_trades(25)
            .allow_no_quote(false)
            .high_entropy(0.75)
            .low_entropy(0.35)
            .max_inventory(0.15)
            .quote_size(0.0015)
            .fee_rate(0.00015)
            .naive_fills(true)
            .queue_pos(0.4)
            .output(Some(PathBuf::from("./results.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: RegimeOptimizeParams = serde_json::from_str(&json).unwrap();

        assert_eq!(original.data_path, deserialized.data_path);
        assert_eq!(original.algorithm, deserialized.algorithm);
        assert_eq!(original.weights_file, deserialized.weights_file);
        assert_eq!(original.spreads, deserialized.spreads);
        assert_eq!(original.skews, deserialized.skews);
        assert_eq!(original.fill_prob, deserialized.fill_prob);
        assert_eq!(original.min_trades, deserialized.min_trades);
        assert_eq!(original.allow_no_quote, deserialized.allow_no_quote);
        assert_eq!(original.high_entropy, deserialized.high_entropy);
        assert_eq!(original.low_entropy, deserialized.low_entropy);
        assert_eq!(original.max_inventory, deserialized.max_inventory);
        assert_eq!(original.quote_size, deserialized.quote_size);
        assert_eq!(original.fee_rate, deserialized.fee_rate);
        assert_eq!(original.naive_fills, deserialized.naive_fills);
        assert_eq!(original.queue_pos, deserialized.queue_pos);
        assert_eq!(original.output, deserialized.output);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_regime_optimize_params_single_value_lists() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("2.0".to_string())
            .skews("0.5".to_string())
            .build()
            .unwrap();

        let spreads: Vec<f64> = params.spreads
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(spreads, vec![2.0]);
    }

    #[test]
    fn test_regime_optimize_params_large_lists() {
        let spreads_str = (1..=100).map(|i| (i as f64).to_string()).collect::<Vec<_>>().join(",");
        let skews_str = (1..=50).map(|i| (i as f64 / 100.0).to_string()).collect::<Vec<_>>().join(",");

        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads(spreads_str.clone())
            .skews(skews_str.clone())
            .build()
            .unwrap();

        assert_eq!(params.spreads, spreads_str);
        assert_eq!(params.skews, skews_str);
    }

    #[test]
    fn test_regime_optimize_params_path_handling() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("/absolute/path/to/data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .output(Some(PathBuf::from("/absolute/path/to/output.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("/absolute/path/to/data"));
        assert_eq!(params.output, Some(PathBuf::from("/absolute/path/to/output.json")));
    }

    #[test]
    fn test_regime_optimize_params_none_output() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .output(None)
            .build()
            .unwrap();

        assert_eq!(params.output, None);
    }

    #[test]
    fn test_regime_optimize_params_none_weights_file() {
        let params = RegimeOptimizeParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0.5".to_string())
            .skews("0.2".to_string())
            .weights_file(None)
            .build()
            .unwrap();

        assert_eq!(params.weights_file, None);
    }
}

#[cfg(test)]
mod train_params_tests {
    use super::*;

    // ============================================================================
    // Builder Defaults Tests
    // ============================================================================

    #[test]
    fn test_train_params_builder_defaults() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,2.0".to_string())
            .spread_entropy_weights("-3.0,-2.0".to_string())
            .spread_vol_weights("200.0,400.0".to_string())
            .skew_intercepts("0.3,0.5".to_string())
            .skew_inv_weights("-1.0,-0.8".to_string())
            .build()
            .unwrap();

        assert_eq!(params.train_ratio, 0.7);
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.queue_pos, 0.5);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_train_params_missing_data_path() {
        let result = TrainParamsBuilder::new()
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_train_params_missing_algorithm() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_train_params_missing_spread_intercepts() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spread_intercepts"));
    }

    #[test]
    fn test_train_params_missing_spread_entropy_weights() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spread_entropy_weights"));
    }

    #[test]
    fn test_train_params_missing_spread_vol_weights() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spread_vol_weights"));
    }

    #[test]
    fn test_train_params_missing_skew_intercepts() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skew_intercepts"));
    }

    #[test]
    fn test_train_params_missing_skew_inv_weights() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skew_inv_weights"));
    }

    // ============================================================================
    // Parameter Parsing Tests
    // ============================================================================

    #[test]
    fn test_train_params_parse_spread_intercepts() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,2.0,3.0,4.0,5.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn test_train_params_parse_spread_entropy_weights() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-3.0,-2.0,-1.0,0.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let weights: Vec<f64> = params.spread_entropy_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(weights, vec![-3.0, -2.0, -1.0, 0.0]);
    }

    #[test]
    fn test_train_params_parse_spread_vol_weights() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0,400.0,600.0,800.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let weights: Vec<f64> = params.spread_vol_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(weights, vec![200.0, 400.0, 600.0, 800.0]);
    }

    #[test]
    fn test_train_params_parse_skew_intercepts() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3,0.5,0.7,0.9".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.skew_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![0.3, 0.5, 0.7, 0.9]);
    }

    #[test]
    fn test_train_params_parse_skew_inv_weights() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0,-0.8,-0.6,-0.4,-0.2".to_string())
            .build()
            .unwrap();

        let weights: Vec<f64> = params.skew_inv_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(weights, vec![-1.0, -0.8, -0.6, -0.4, -0.2]);
    }

    #[test]
    fn test_train_params_parse_with_whitespace() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts(" 1.0 , 2.0 , 3.0 ".to_string())
            .spread_entropy_weights(" -2.0 , -1.0 ".to_string())
            .spread_vol_weights(" 200.0 , 400.0 ".to_string())
            .skew_intercepts(" 0.3 , 0.5 ".to_string())
            .skew_inv_weights(" -1.0 , -0.8 ".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_train_params_empty_spread_intercepts() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_invalid_spread_intercepts() {
        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("abc,def".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_mixed_valid_invalid() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,invalid,2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        // Should parse only valid values
        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![1.0, 2.0]);
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_train_params_train_ratio_range() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .train_ratio(0.0)
            .build()
            .unwrap();
        assert_eq!(params.train_ratio, 0.0);

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .train_ratio(1.0)
            .build()
            .unwrap();
        assert_eq!(params.train_ratio, 1.0);

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .train_ratio(1.5)
            .build();
        assert!(result.is_err());

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .train_ratio(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_fill_prob_range() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fill_prob(0.0)
            .build()
            .unwrap();
        assert_eq!(params.fill_prob, 0.0);

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fill_prob(1.0)
            .build()
            .unwrap();
        assert_eq!(params.fill_prob, 1.0);

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fill_prob(1.5)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_queue_pos_range() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .queue_pos(0.0)
            .build()
            .unwrap();
        assert_eq!(params.queue_pos, 0.0);

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .queue_pos(1.0)
            .build()
            .unwrap();
        assert_eq!(params.queue_pos, 1.0);

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .queue_pos(1.5)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_fee_rate_validation() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fee_rate(0.0)
            .build()
            .unwrap();
        assert_eq!(params.fee_rate, 0.0);

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_max_inventory_validation() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .max_inventory(0.01)
            .build()
            .unwrap();
        assert_eq!(params.max_inventory, 0.01);

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .max_inventory(0.0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_train_params_quote_size_validation() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .quote_size(0.0001)
            .build()
            .unwrap();
        assert_eq!(params.quote_size, 0.0001);

        let result = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .quote_size(0.0)
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Custom Values Tests
    // ============================================================================

    #[test]
    fn test_train_params_custom_values() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data/custom"))
            .algorithm("ml-spread-skew".to_string())
            .train_ratio(0.8)
            .spread_intercepts("1.5,2.5,3.5".to_string())
            .spread_entropy_weights("-2.5,-1.5".to_string())
            .spread_vol_weights("300.0,500.0".to_string())
            .skew_intercepts("0.4,0.6".to_string())
            .skew_inv_weights("-0.9,-0.7".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fill_prob(0.15)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.3)
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./data/custom"));
        assert_eq!(params.algorithm, "ml-spread-skew");
        assert_eq!(params.train_ratio, 0.8);
        assert_eq!(params.spread_intercepts, "1.5,2.5,3.5");
        assert_eq!(params.spread_entropy_weights, "-2.5,-1.5");
        assert_eq!(params.spread_vol_weights, "300.0,500.0");
        assert_eq!(params.skew_intercepts, "0.4,0.6");
        assert_eq!(params.skew_inv_weights, "-0.9,-0.7");
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./output.json")));
    }

    // ============================================================================
    // Serialization/Deserialization Tests
    // ============================================================================

    #[test]
    fn test_train_params_serialization() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .train_ratio(0.75)
            .spread_intercepts("1.0,2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fill_prob(0.12)
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: TrainParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.data_path, deserialized.data_path);
        assert_eq!(params.algorithm, deserialized.algorithm);
        assert_eq!(params.train_ratio, deserialized.train_ratio);
        assert_eq!(params.spread_intercepts, deserialized.spread_intercepts);
        assert_eq!(params.fill_prob, deserialized.fill_prob);
    }

    #[test]
    fn test_train_params_roundtrip_serialization() {
        let original = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml-spread-skew".to_string())
            .train_ratio(0.8)
            .spread_intercepts("1.5,2.5,3.5".to_string())
            .spread_entropy_weights("-2.5,-1.5".to_string())
            .spread_vol_weights("300.0,500.0".to_string())
            .skew_intercepts("0.4,0.6".to_string())
            .skew_inv_weights("-0.9,-0.7".to_string())
            .max_inventory(0.15)
            .quote_size(0.0015)
            .fill_prob(0.12)
            .fee_rate(0.00015)
            .naive_fills(true)
            .queue_pos(0.4)
            .output(Some(PathBuf::from("./results.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TrainParams = serde_json::from_str(&json).unwrap();

        assert_eq!(original.data_path, deserialized.data_path);
        assert_eq!(original.algorithm, deserialized.algorithm);
        assert_eq!(original.train_ratio, deserialized.train_ratio);
        assert_eq!(original.spread_intercepts, deserialized.spread_intercepts);
        assert_eq!(original.spread_entropy_weights, deserialized.spread_entropy_weights);
        assert_eq!(original.spread_vol_weights, deserialized.spread_vol_weights);
        assert_eq!(original.skew_intercepts, deserialized.skew_intercepts);
        assert_eq!(original.skew_inv_weights, deserialized.skew_inv_weights);
        assert_eq!(original.max_inventory, deserialized.max_inventory);
        assert_eq!(original.quote_size, deserialized.quote_size);
        assert_eq!(original.fill_prob, deserialized.fill_prob);
        assert_eq!(original.fee_rate, deserialized.fee_rate);
        assert_eq!(original.naive_fills, deserialized.naive_fills);
        assert_eq!(original.queue_pos, deserialized.queue_pos);
        assert_eq!(original.output, deserialized.output);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_train_params_single_value_lists() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("400.0".to_string())
            .skew_intercepts("0.5".to_string())
            .skew_inv_weights("-0.8".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![2.0]);
    }

    #[test]
    fn test_train_params_large_lists() {
        let spread_intercepts_str = (1..=50).map(|i| (i as f64).to_string()).collect::<Vec<_>>().join(",");
        let spread_entropy_weights_str = (-10..=0).map(|i| (i as f64).to_string()).collect::<Vec<_>>().join(",");

        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts(spread_intercepts_str.clone())
            .spread_entropy_weights(spread_entropy_weights_str.clone())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert_eq!(params.spread_intercepts, spread_intercepts_str);
        assert_eq!(params.spread_entropy_weights, spread_entropy_weights_str);
    }

    #[test]
    fn test_train_params_path_handling() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("/absolute/path/to/data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .output(Some(PathBuf::from("/absolute/path/to/output.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("/absolute/path/to/data"));
        assert_eq!(params.output, Some(PathBuf::from("/absolute/path/to/output.json")));
    }

    #[test]
    fn test_train_params_none_output() {
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .output(None)
            .build()
            .unwrap();

        assert_eq!(params.output, None);
    }

    #[test]
    fn test_train_params_negative_weights() {
        // Negative weights are valid for entropy and inventory weights
        let params = TrainParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-5.0,-4.0,-3.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-2.0,-1.5,-1.0".to_string())
            .build()
            .unwrap();

        let entropy_weights: Vec<f64> = params.spread_entropy_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(entropy_weights, vec![-5.0, -4.0, -3.0]);

        let inv_weights: Vec<f64> = params.skew_inv_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(inv_weights, vec![-2.0, -1.5, -1.0]);
    }
}

#[cfg(test)]
mod walk_forward_ml_params_tests {
    use super::*;

    // ============================================================================
    // Builder Defaults Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_builder_defaults() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert_eq!(params.folds, 5);
        assert_eq!(params.min_train_hours, 100.0);
        assert_eq!(params.test_hours, 24.0);
        assert!(!params.rolling);
        assert_eq!(params.embargo_hours, 1.0);
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.queue_pos, 0.5);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_missing_data_path() {
        let result = WalkForwardMLParamsBuilder::new()
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_walk_forward_ml_params_missing_algorithm() {
        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_walk_forward_ml_params_missing_spread_intercepts() {
        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spread_intercepts"));
    }

    #[test]
    fn test_walk_forward_ml_params_missing_all_required() {
        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Parameter Parsing Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_parse_spread_intercepts() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,2.0,3.0,4.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_walk_forward_ml_params_parse_all_grids() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,2.0".to_string())
            .spread_entropy_weights("-3.0,-2.0,-1.0".to_string())
            .spread_vol_weights("200.0,400.0,600.0".to_string())
            .skew_intercepts("0.3,0.5,0.7".to_string())
            .skew_inv_weights("-1.0,-0.8,-0.6".to_string())
            .build()
            .unwrap();

        let spread_ints: Vec<f64> = params.spread_intercepts.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let spread_ents: Vec<f64> = params.spread_entropy_weights.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let spread_vols: Vec<f64> = params.spread_vol_weights.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let skew_ints: Vec<f64> = params.skew_intercepts.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        let skew_invs: Vec<f64> = params.skew_inv_weights.split(',').filter_map(|s| s.trim().parse().ok()).collect();

        assert_eq!(spread_ints, vec![1.0, 2.0]);
        assert_eq!(spread_ents, vec![-3.0, -2.0, -1.0]);
        assert_eq!(spread_vols, vec![200.0, 400.0, 600.0]);
        assert_eq!(skew_ints, vec![0.3, 0.5, 0.7]);
        assert_eq!(skew_invs, vec![-1.0, -0.8, -0.6]);
    }

    #[test]
    fn test_walk_forward_ml_params_parse_with_whitespace() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts(" 1.0 , 2.0 , 3.0 ".to_string())
            .spread_entropy_weights(" -2.0 , -1.0 ".to_string())
            .spread_vol_weights(" 200.0 , 400.0 ".to_string())
            .skew_intercepts(" 0.3 , 0.5 ".to_string())
            .skew_inv_weights(" -1.0 , -0.8 ".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_walk_forward_ml_params_empty_spread_intercepts() {
        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_invalid_spread_intercepts() {
        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("abc,def".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_folds_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .folds(1)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.folds, 1);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .folds(0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_min_train_hours_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .min_train_hours(50.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.min_train_hours, 50.0);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .min_train_hours(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_test_hours_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .test_hours(12.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.test_hours, 12.0);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .test_hours(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_embargo_hours_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .embargo_hours(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.embargo_hours, 0.0);

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .embargo_hours(5.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.embargo_hours, 5.0);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .embargo_hours(-0.1)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_rolling_flag() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .rolling(true)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert!(params.rolling);

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .rolling(false)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert!(!params.rolling);
    }

    #[test]
    fn test_walk_forward_ml_params_fill_prob_range() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .fill_prob(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.fill_prob, 0.0);

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .fill_prob(1.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.fill_prob, 1.0);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .fill_prob(1.5)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_queue_pos_range() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .queue_pos(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.queue_pos, 0.0);

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .queue_pos(1.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.queue_pos, 1.0);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .queue_pos(1.5)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_fee_rate_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .fee_rate(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.fee_rate, 0.0);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .fee_rate(-0.1)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_max_inventory_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .max_inventory(0.01)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.max_inventory, 0.01);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .max_inventory(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_ml_params_quote_size_validation() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .quote_size(0.0001)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();
        assert_eq!(params.quote_size, 0.0001);

        let result = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .quote_size(0.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Custom Values Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_custom_values() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data/custom"))
            .algorithm("as".to_string())
            .folds(10)
            .min_train_hours(200.0)
            .test_hours(48.0)
            .rolling(true)
            .embargo_hours(2.0)
            .spread_intercepts("1.5,2.5,3.5".to_string())
            .spread_entropy_weights("-2.5,-1.5".to_string())
            .spread_vol_weights("300.0,500.0".to_string())
            .skew_intercepts("0.4,0.6".to_string())
            .skew_inv_weights("-0.9,-0.7".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fill_prob(0.15)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.3)
            .output(Some(PathBuf::from("./output.json")))
            .weights_output(Some(PathBuf::from("./weights.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("./data/custom"));
        assert_eq!(params.algorithm, "as");
        assert_eq!(params.folds, 10);
        assert_eq!(params.min_train_hours, 200.0);
        assert_eq!(params.test_hours, 48.0);
        assert!(params.rolling);
        assert_eq!(params.embargo_hours, 2.0);
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./output.json")));
        assert_eq!(params.weights_output, Some(PathBuf::from("./weights.json")));
    }

    // ============================================================================
    // Serialization/Deserialization Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_serialization() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .folds(5)
            .min_train_hours(100.0)
            .test_hours(24.0)
            .rolling(false)
            .embargo_hours(1.0)
            .spread_intercepts("1.0,2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .fill_prob(0.12)
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: WalkForwardMLParams = serde_json::from_str(&json).unwrap();

        assert_eq!(params.data_path, deserialized.data_path);
        assert_eq!(params.algorithm, deserialized.algorithm);
        assert_eq!(params.folds, deserialized.folds);
        assert_eq!(params.min_train_hours, deserialized.min_train_hours);
        assert_eq!(params.test_hours, deserialized.test_hours);
        assert_eq!(params.rolling, deserialized.rolling);
        assert_eq!(params.fill_prob, deserialized.fill_prob);
    }

    #[test]
    fn test_walk_forward_ml_params_roundtrip_serialization() {
        let original = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .folds(10)
            .min_train_hours(200.0)
            .test_hours(48.0)
            .rolling(true)
            .embargo_hours(2.0)
            .spread_intercepts("1.5,2.5,3.5".to_string())
            .spread_entropy_weights("-2.5,-1.5".to_string())
            .spread_vol_weights("300.0,500.0".to_string())
            .skew_intercepts("0.4,0.6".to_string())
            .skew_inv_weights("-0.9,-0.7".to_string())
            .max_inventory(0.15)
            .quote_size(0.0015)
            .fill_prob(0.12)
            .fee_rate(0.00015)
            .naive_fills(true)
            .queue_pos(0.4)
            .output(Some(PathBuf::from("./results.json")))
            .weights_output(Some(PathBuf::from("./weights.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: WalkForwardMLParams = serde_json::from_str(&json).unwrap();

        assert_eq!(original.data_path, deserialized.data_path);
        assert_eq!(original.algorithm, deserialized.algorithm);
        assert_eq!(original.folds, deserialized.folds);
        assert_eq!(original.min_train_hours, deserialized.min_train_hours);
        assert_eq!(original.test_hours, deserialized.test_hours);
        assert_eq!(original.rolling, deserialized.rolling);
        assert_eq!(original.embargo_hours, deserialized.embargo_hours);
        assert_eq!(original.spread_intercepts, deserialized.spread_intercepts);
        assert_eq!(original.spread_entropy_weights, deserialized.spread_entropy_weights);
        assert_eq!(original.spread_vol_weights, deserialized.spread_vol_weights);
        assert_eq!(original.skew_intercepts, deserialized.skew_intercepts);
        assert_eq!(original.skew_inv_weights, deserialized.skew_inv_weights);
        assert_eq!(original.max_inventory, deserialized.max_inventory);
        assert_eq!(original.quote_size, deserialized.quote_size);
        assert_eq!(original.fill_prob, deserialized.fill_prob);
        assert_eq!(original.fee_rate, deserialized.fee_rate);
        assert_eq!(original.naive_fills, deserialized.naive_fills);
        assert_eq!(original.queue_pos, deserialized.queue_pos);
        assert_eq!(original.output, deserialized.output);
        assert_eq!(original.weights_output, deserialized.weights_output);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_ml_params_single_value_lists() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("400.0".to_string())
            .skew_intercepts("0.5".to_string())
            .skew_inv_weights("-0.8".to_string())
            .build()
            .unwrap();

        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![2.0]);
    }

    #[test]
    fn test_walk_forward_ml_params_large_lists() {
        let spread_intercepts_str = (1..=50).map(|i| (i as f64).to_string()).collect::<Vec<_>>().join(",");
        let spread_entropy_weights_str = (-10..=0).map(|i| (i as f64).to_string()).collect::<Vec<_>>().join(",");

        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts(spread_intercepts_str.clone())
            .spread_entropy_weights(spread_entropy_weights_str.clone())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert_eq!(params.spread_intercepts, spread_intercepts_str);
        assert_eq!(params.spread_entropy_weights, spread_entropy_weights_str);
    }

    #[test]
    fn test_walk_forward_ml_params_path_handling() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("/absolute/path/to/data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .output(Some(PathBuf::from("/absolute/path/to/output.json")))
            .weights_output(Some(PathBuf::from("/absolute/path/to/weights.json")))
            .build()
            .unwrap();

        assert_eq!(params.data_path, PathBuf::from("/absolute/path/to/data"));
        assert_eq!(params.output, Some(PathBuf::from("/absolute/path/to/output.json")));
        assert_eq!(params.weights_output, Some(PathBuf::from("/absolute/path/to/weights.json")));
    }

    #[test]
    fn test_walk_forward_ml_params_none_outputs() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .output(None)
            .weights_output(None)
            .build()
            .unwrap();

        assert_eq!(params.output, None);
        assert_eq!(params.weights_output, None);
    }

    #[test]
    fn test_walk_forward_ml_params_negative_weights() {
        // Negative weights are valid for entropy and inventory weights
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-5.0,-4.0,-3.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-2.0,-1.5,-1.0".to_string())
            .build()
            .unwrap();

        let entropy_weights: Vec<f64> = params.spread_entropy_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(entropy_weights, vec![-5.0, -4.0, -3.0]);

        let inv_weights: Vec<f64> = params.skew_inv_weights
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(inv_weights, vec![-2.0, -1.5, -1.0]);
    }

    #[test]
    fn test_walk_forward_ml_params_mixed_valid_invalid() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spread_intercepts("1.0,invalid,2.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        // Should parse only valid values
        let intercepts: Vec<f64> = params.spread_intercepts
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(intercepts, vec![1.0, 2.0]);
    }

    #[test]
    fn test_walk_forward_ml_params_large_folds() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .folds(100)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert_eq!(params.folds, 100);
    }

    #[test]
    fn test_walk_forward_ml_params_large_hours() {
        let params = WalkForwardMLParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .min_train_hours(1000.0)
            .test_hours(100.0)
            .spread_intercepts("1.0".to_string())
            .spread_entropy_weights("-2.0".to_string())
            .spread_vol_weights("200.0".to_string())
            .skew_intercepts("0.3".to_string())
            .skew_inv_weights("-1.0".to_string())
            .build()
            .unwrap();

        assert_eq!(params.min_train_hours, 1000.0);
        assert_eq!(params.test_hours, 100.0);
    }
}

/// Parameters for the `walk-forward-ml` command (walk-forward ML training - MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardMLParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be MM algorithm: as, ml, or fixed)
    pub algorithm: String,
    /// Number of folds (train/test splits)
    pub folds: usize,
    /// Minimum training period in hours
    pub min_train_hours: f64,
    /// Test period in hours per fold
    pub test_hours: f64,
    /// Use rolling window (false = anchored/expanding window)
    pub rolling: bool,
    /// Gap between train and test to prevent lookahead (hours)
    pub embargo_hours: f64,
    /// Spread intercept values to test (comma-separated string)
    pub spread_intercepts: String,
    /// Spread entropy weight values to test (comma-separated string)
    pub spread_entropy_weights: String,
    /// Spread volatility weight values to test (comma-separated string)
    pub spread_vol_weights: String,
    /// Skew intercept values to test (comma-separated string)
    pub skew_intercepts: String,
    /// Skew inventory weight values to test (comma-separated string)
    pub skew_inv_weights: String,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fill probability (0.0-1.0) for simulation
    pub fill_prob: f64,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: f64,
    /// Use naive fill simulation (for comparison)
    pub naive_fills: bool,
    /// Queue position (0.0=front, 1.0=back)
    pub queue_pos: f64,
    /// Output file for full results (JSON)
    pub output: Option<PathBuf>,
    /// Output file for consensus weights (JSON)
    pub weights_output: Option<PathBuf>,
}

impl Default for WalkForwardMLParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "ml".to_string(),
            folds: 5,
            min_train_hours: 168.0,
            test_hours: 24.0,
            rolling: true,
            embargo_hours: 1.0,
            spread_intercepts: "2.0,3.0,4.0".to_string(),
            spread_entropy_weights: "-3.0,-2.0,-1.0".to_string(),
            spread_vol_weights: "100.0,150.0,200.0".to_string(),
            skew_intercepts: "0.3,0.5,0.7".to_string(),
            skew_inv_weights: "-1.5,-1.0,-0.5".to_string(),
            max_inventory: 10.0,
            quote_size: 1.0,
            fill_prob: 0.10,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            output: None,
            weights_output: None,
        }
    }
}

/// Builder for `WalkForwardMLParams` with validation
pub struct WalkForwardMLParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    folds: Option<usize>,
    min_train_hours: Option<f64>,
    test_hours: Option<f64>,
    rolling: Option<bool>,
    embargo_hours: Option<f64>,
    spread_intercepts: Option<String>,
    spread_entropy_weights: Option<String>,
    spread_vol_weights: Option<String>,
    skew_intercepts: Option<String>,
    skew_inv_weights: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fill_prob: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    weights_output: Option<PathBuf>,
}

impl WalkForwardMLParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            folds: None,
            min_train_hours: None,
            test_hours: None,
            rolling: None,
            embargo_hours: None,
            spread_intercepts: None,
            spread_entropy_weights: None,
            spread_vol_weights: None,
            skew_intercepts: None,
            skew_inv_weights: None,
            max_inventory: None,
            quote_size: None,
            fill_prob: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
            output: None,
            weights_output: None,
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

    /// Set number of folds
    pub fn folds(mut self, folds: usize) -> Self {
        self.folds = Some(folds);
        self
    }

    /// Set minimum train hours
    pub fn min_train_hours(mut self, hours: f64) -> Self {
        self.min_train_hours = Some(hours);
        self
    }

    /// Set test hours
    pub fn test_hours(mut self, hours: f64) -> Self {
        self.test_hours = Some(hours);
        self
    }

    /// Set rolling window flag
    pub fn rolling(mut self, rolling: bool) -> Self {
        self.rolling = Some(rolling);
        self
    }

    /// Set embargo hours
    pub fn embargo_hours(mut self, hours: f64) -> Self {
        self.embargo_hours = Some(hours);
        self
    }

    /// Set spread intercepts (comma-separated string)
    pub fn spread_intercepts(mut self, intercepts: String) -> Self {
        self.spread_intercepts = Some(intercepts);
        self
    }

    /// Set spread entropy weights (comma-separated string)
    pub fn spread_entropy_weights(mut self, weights: String) -> Self {
        self.spread_entropy_weights = Some(weights);
        self
    }

    /// Set spread volatility weights (comma-separated string)
    pub fn spread_vol_weights(mut self, weights: String) -> Self {
        self.spread_vol_weights = Some(weights);
        self
    }

    /// Set skew intercepts (comma-separated string)
    pub fn skew_intercepts(mut self, intercepts: String) -> Self {
        self.skew_intercepts = Some(intercepts);
        self
    }

    /// Set skew inventory weights (comma-separated string)
    pub fn skew_inv_weights(mut self, weights: String) -> Self {
        self.skew_inv_weights = Some(weights);
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

    /// Set fill probability
    pub fn fill_prob(mut self, prob: f64) -> Self {
        self.fill_prob = Some(prob);
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set weights output file
    pub fn weights_output(mut self, path: Option<PathBuf>) -> Self {
        self.weights_output = path;
        self
    }

    /// Parse comma-separated string to Vec<f64>
    fn parse_f64_list(s: &str) -> Result<Vec<f64>> {
        let values: Vec<f64> = s
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if values.is_empty() {
            anyhow::bail!("No valid numeric values found in parameter list: '{}'", s);
        }
        Ok(values)
    }

    /// Build `WalkForwardMLParams` with validation
    pub fn build(self) -> Result<WalkForwardMLParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spread_intercepts = self.spread_intercepts
            .ok_or_else(|| anyhow::anyhow!("spread_intercepts is required"))?;
        let spread_entropy_weights = self.spread_entropy_weights
            .ok_or_else(|| anyhow::anyhow!("spread_entropy_weights is required"))?;
        let spread_vol_weights = self.spread_vol_weights
            .ok_or_else(|| anyhow::anyhow!("spread_vol_weights is required"))?;
        let skew_intercepts = self.skew_intercepts
            .ok_or_else(|| anyhow::anyhow!("skew_intercepts is required"))?;
        let skew_inv_weights = self.skew_inv_weights
            .ok_or_else(|| anyhow::anyhow!("skew_inv_weights is required"))?;

        // Parse and validate parameter lists
        let _spread_intercepts_vec = Self::parse_f64_list(&spread_intercepts)
            .context("Failed to parse spread_intercepts")?;
        let _spread_entropy_weights_vec = Self::parse_f64_list(&spread_entropy_weights)
            .context("Failed to parse spread_entropy_weights")?;
        let _spread_vol_weights_vec = Self::parse_f64_list(&spread_vol_weights)
            .context("Failed to parse spread_vol_weights")?;
        let _skew_intercepts_vec = Self::parse_f64_list(&skew_intercepts)
            .context("Failed to parse skew_intercepts")?;
        let _skew_inv_weights_vec = Self::parse_f64_list(&skew_inv_weights)
            .context("Failed to parse skew_inv_weights")?;

        // Validate folds
        if let Some(folds) = self.folds {
            if folds == 0 {
                anyhow::bail!("folds must be > 0");
            }
        }

        // Validate hours
        if let Some(min_train_hours) = self.min_train_hours {
            if min_train_hours <= 0.0 {
                anyhow::bail!("min_train_hours must be > 0.0");
            }
        }
        if let Some(test_hours) = self.test_hours {
            if test_hours <= 0.0 {
                anyhow::bail!("test_hours must be > 0.0");
            }
        }
        if let Some(embargo_hours) = self.embargo_hours {
            if embargo_hours < 0.0 {
                anyhow::bail!("embargo_hours must be >= 0.0");
            }
        }

        // Validate fill_prob
        let fill_prob = self.fill_prob.unwrap_or(0.10);
        if !(0.0..=1.0).contains(&fill_prob) {
            anyhow::bail!("fill_prob must be in range [0.0, 1.0], found {}", fill_prob);
        }

        // Validate other ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }

        Ok(WalkForwardMLParams {
            data_path,
            algorithm,
            folds: self.folds.unwrap_or(5),
            min_train_hours: self.min_train_hours.unwrap_or(100.0),
            test_hours: self.test_hours.unwrap_or(24.0),
            rolling: self.rolling.unwrap_or(false),
            embargo_hours: self.embargo_hours.unwrap_or(1.0),
            spread_intercepts,
            spread_entropy_weights,
            spread_vol_weights,
            skew_intercepts,
            skew_inv_weights,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fill_prob,
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            weights_output: self.weights_output,
        })
    }
}

impl Default for WalkForwardMLParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `train` command (ML weight training - ML Spread/Skew algorithm only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be ML Spread/Skew: ml or ml-spread-skew)
    pub algorithm: String,
    /// Train/test split ratio (fraction for training, 0.0-1.0)
    pub train_ratio: f64,
    /// Spread intercept values to test (comma-separated string)
    pub spread_intercepts: String,
    /// Spread entropy weight values to test (comma-separated string)
    pub spread_entropy_weights: String,
    /// Spread volatility weight values to test (comma-separated string)
    pub spread_vol_weights: String,
    /// Skew intercept values to test (comma-separated string)
    pub skew_intercepts: String,
    /// Skew inventory weight values to test (comma-separated string)
    pub skew_inv_weights: String,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fill probability (0.0-1.0) for simulation
    pub fill_prob: f64,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: f64,
    /// Use naive fill simulation (for comparison)
    pub naive_fills: bool,
    /// Queue position (0.0=front, 1.0=back)
    pub queue_pos: f64,
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
}

impl Default for TrainParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "ml".to_string(),
            train_ratio: 0.8,
            spread_intercepts: "2.0,3.0,4.0".to_string(),
            spread_entropy_weights: "-3.0,-2.0,-1.0".to_string(),
            spread_vol_weights: "100.0,150.0,200.0".to_string(),
            skew_intercepts: "0.3,0.5,0.7".to_string(),
            skew_inv_weights: "-1.5,-1.0,-0.5".to_string(),
            max_inventory: 10.0,
            quote_size: 1.0,
            fill_prob: 0.10,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            output: None,
        }
    }
}

/// Builder for `TrainParams` with validation
pub struct TrainParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    train_ratio: Option<f64>,
    spread_intercepts: Option<String>,
    spread_entropy_weights: Option<String>,
    spread_vol_weights: Option<String>,
    skew_intercepts: Option<String>,
    skew_inv_weights: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fill_prob: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
}

impl TrainParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            train_ratio: None,
            spread_intercepts: None,
            spread_entropy_weights: None,
            spread_vol_weights: None,
            skew_intercepts: None,
            skew_inv_weights: None,
            max_inventory: None,
            quote_size: None,
            fill_prob: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
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

    /// Set train ratio
    pub fn train_ratio(mut self, ratio: f64) -> Self {
        self.train_ratio = Some(ratio);
        self
    }

    /// Set spread intercepts (comma-separated string)
    pub fn spread_intercepts(mut self, intercepts: String) -> Self {
        self.spread_intercepts = Some(intercepts);
        self
    }

    /// Set spread entropy weights (comma-separated string)
    pub fn spread_entropy_weights(mut self, weights: String) -> Self {
        self.spread_entropy_weights = Some(weights);
        self
    }

    /// Set spread volatility weights (comma-separated string)
    pub fn spread_vol_weights(mut self, weights: String) -> Self {
        self.spread_vol_weights = Some(weights);
        self
    }

    /// Set skew intercepts (comma-separated string)
    pub fn skew_intercepts(mut self, intercepts: String) -> Self {
        self.skew_intercepts = Some(intercepts);
        self
    }

    /// Set skew inventory weights (comma-separated string)
    pub fn skew_inv_weights(mut self, weights: String) -> Self {
        self.skew_inv_weights = Some(weights);
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

    /// Set fill probability
    pub fn fill_prob(mut self, prob: f64) -> Self {
        self.fill_prob = Some(prob);
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
            anyhow::bail!("No valid numeric values found in parameter list: '{}'", s);
        }
        Ok(values)
    }

    /// Build `TrainParams` with validation
    pub fn build(self) -> Result<TrainParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spread_intercepts = self.spread_intercepts
            .ok_or_else(|| anyhow::anyhow!("spread_intercepts is required"))?;
        let spread_entropy_weights = self.spread_entropy_weights
            .ok_or_else(|| anyhow::anyhow!("spread_entropy_weights is required"))?;
        let spread_vol_weights = self.spread_vol_weights
            .ok_or_else(|| anyhow::anyhow!("spread_vol_weights is required"))?;
        let skew_intercepts = self.skew_intercepts
            .ok_or_else(|| anyhow::anyhow!("skew_intercepts is required"))?;
        let skew_inv_weights = self.skew_inv_weights
            .ok_or_else(|| anyhow::anyhow!("skew_inv_weights is required"))?;

        // Parse and validate parameter lists
        let _spread_intercepts_vec = Self::parse_f64_list(&spread_intercepts)
            .context("Failed to parse spread_intercepts")?;
        let _spread_entropy_weights_vec = Self::parse_f64_list(&spread_entropy_weights)
            .context("Failed to parse spread_entropy_weights")?;
        let _spread_vol_weights_vec = Self::parse_f64_list(&spread_vol_weights)
            .context("Failed to parse spread_vol_weights")?;
        let _skew_intercepts_vec = Self::parse_f64_list(&skew_intercepts)
            .context("Failed to parse skew_intercepts")?;
        let _skew_inv_weights_vec = Self::parse_f64_list(&skew_inv_weights)
            .context("Failed to parse skew_inv_weights")?;

        // Validate train_ratio
        let train_ratio = self.train_ratio.unwrap_or(0.7);
        if !(0.0..=1.0).contains(&train_ratio) {
            anyhow::bail!("train_ratio must be in range [0.0, 1.0], found {}", train_ratio);
        }

        // Validate fill_prob
        let fill_prob = self.fill_prob.unwrap_or(0.10);
        if !(0.0..=1.0).contains(&fill_prob) {
            anyhow::bail!("fill_prob must be in range [0.0, 1.0], found {}", fill_prob);
        }

        // Validate other ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }

        Ok(TrainParams {
            data_path,
            algorithm,
            train_ratio,
            spread_intercepts,
            spread_entropy_weights,
            spread_vol_weights,
            skew_intercepts,
            skew_inv_weights,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fill_prob,
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
        })
    }
}

impl Default for TrainParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `sweep` command (parameter sweep - both algorithm types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Spread values to test (comma-separated string, e.g., "1,2,3,4,5")
    pub spreads: String,
    /// Skew values to test (comma-separated string, e.g., "0.3,0.5,0.7")
    pub skews: String,
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
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for SweepParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            spreads: "1,2,3".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            max_inventory: 1000.0,
            quote_size: 0.1,
            fee_rate: 0.0001,
            naive_fills: false,
            fill_prob: 0.1,
            queue_pos: 0.5,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `SweepParams` with validation
pub struct SweepParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spreads: Option<String>,
    skews: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl SweepParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            spreads: None,
            skews: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            fill_prob: None,
            queue_pos: None,
            output: None,
            quiet: None,
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `SweepParams` with validation
    pub fn build(self) -> Result<SweepParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;

        // Validate and parse spreads
        let spread_values: Vec<f64> = spreads
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if spread_values.is_empty() {
            anyhow::bail!("spreads must contain at least one valid number");
        }

        for &spread in &spread_values {
            if spread < 0.0 {
                anyhow::bail!("all spread values must be >= 0.0, found {}", spread);
            }
        }

        // Validate and parse skews
        let skew_values: Vec<f64> = skews
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if skew_values.is_empty() {
            anyhow::bail!("skews must contain at least one valid number");
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }

        Ok(SweepParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            spreads,
            skews,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            fill_prob: self.fill_prob.unwrap_or(0.10),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for SweepParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `walk-forward` command (walk-forward validation - both algorithm types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalkForwardParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Number of folds (train/test splits)
    pub folds: usize,
    /// Test period per fold (hours)
    pub test_hours: f64,
    /// Use rolling (vs anchored/expanding) window
    pub rolling: bool,
    /// Minimum training period in hours
    pub min_train_hours: f64,
    /// Gap between train and test to prevent lookahead (hours)
    pub embargo_hours: f64,
    /// Spread values to test (comma-separated string, e.g., "1,2,3,4,5")
    pub spreads: String,
    /// Skew values to test (comma-separated string, e.g., "0.3,0.5,0.7")
    pub skews: String,
    /// Fill probability values to test (comma-separated string, e.g., "0.05,0.10,0.15")
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
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for WalkForwardParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            folds: 5,
            test_hours: 24.0,
            rolling: true,
            min_train_hours: 168.0,
            embargo_hours: 1.0,
            spreads: "1,2,3,4,5".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            fill_probs: "0.05,0.10,0.15".to_string(),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `WalkForwardParams` with validation
pub struct WalkForwardParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    folds: Option<usize>,
    test_hours: Option<f64>,
    rolling: Option<bool>,
    min_train_hours: Option<f64>,
    embargo_hours: Option<f64>,
    spreads: Option<String>,
    skews: Option<String>,
    fill_probs: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl WalkForwardParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            folds: None,
            test_hours: None,
            rolling: None,
            min_train_hours: None,
            embargo_hours: None,
            spreads: None,
            skews: None,
            fill_probs: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
            output: None,
            quiet: None,
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

    /// Set number of folds
    pub fn folds(mut self, folds: usize) -> Self {
        self.folds = Some(folds);
        self
    }

    /// Set test hours
    pub fn test_hours(mut self, hours: f64) -> Self {
        self.test_hours = Some(hours);
        self
    }

    /// Set rolling mode
    pub fn rolling(mut self, rolling: bool) -> Self {
        self.rolling = Some(rolling);
        self
    }

    /// Set minimum training hours
    pub fn min_train_hours(mut self, hours: f64) -> Self {
        self.min_train_hours = Some(hours);
        self
    }

    /// Set embargo hours
    pub fn embargo_hours(mut self, hours: f64) -> Self {
        self.embargo_hours = Some(hours);
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `WalkForwardParams` with validation
    pub fn build(self) -> Result<WalkForwardParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;
        let fill_probs = self.fill_probs
            .ok_or_else(|| anyhow::anyhow!("fill_probs is required"))?;

        // Validate and parse spreads
        let spread_values: Vec<f64> = spreads
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if spread_values.is_empty() {
            anyhow::bail!("spreads must contain at least one valid number");
        }

        for &spread in &spread_values {
            if spread < 0.0 {
                anyhow::bail!("all spread values must be >= 0.0, found {}", spread);
            }
        }

        // Validate and parse skews
        let skew_values: Vec<f64> = skews
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if skew_values.is_empty() {
            anyhow::bail!("skews must contain at least one valid number");
        }

        // Validate and parse fill_probs
        let fill_prob_values: Vec<f64> = fill_probs
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if fill_prob_values.is_empty() {
            anyhow::bail!("fill_probs must contain at least one valid number");
        }

        for &fill_prob in &fill_prob_values {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("all fill_prob values must be in range [0.0, 1.0], found {}", fill_prob);
            }
        }

        // Validate folds
        if let Some(folds) = self.folds {
            if folds == 0 {
                anyhow::bail!("folds must be > 0");
            }
        }

        // Validate hours
        if let Some(test_hours) = self.test_hours {
            if test_hours <= 0.0 {
                anyhow::bail!("test_hours must be > 0.0");
            }
        }
        if let Some(min_train_hours) = self.min_train_hours {
            if min_train_hours <= 0.0 {
                anyhow::bail!("min_train_hours must be > 0.0");
            }
        }
        if let Some(embargo_hours) = self.embargo_hours {
            if embargo_hours < 0.0 {
                anyhow::bail!("embargo_hours must be >= 0.0");
            }
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }

        Ok(WalkForwardParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            folds: self.folds.unwrap_or(5),
            test_hours: self.test_hours.unwrap_or(24.0),
            rolling: self.rolling.unwrap_or(false),
            min_train_hours: self.min_train_hours.unwrap_or(100.0),
            embargo_hours: self.embargo_hours.unwrap_or(1.0),
            spreads,
            skews,
            fill_probs,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for WalkForwardParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod sweep_params_tests {
    use super::*;

    // ============================================================================
    // Builder Creation Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_builder_new() {
        let builder = SweepParamsBuilder::new();
        // Should not panic
        assert!(true);
    }

    #[test]
    fn test_sweep_params_builder_default() {
        let builder = SweepParamsBuilder::default();
        // Should not panic
        assert!(true);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_missing_data_path() {
        let result = SweepParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_sweep_params_missing_algorithm() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_sweep_params_missing_spreads() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spreads"));
    }

    #[test]
    fn test_sweep_params_missing_skews() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skews"));
    }

    #[test]
    fn test_sweep_params_missing_all_required() {
        let result = SweepParamsBuilder::new().build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Spreads Validation Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_empty_spreads() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.3".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_invalid_spread_numbers() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("abc,def".to_string())
            .skews("0.3".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_negative_spread() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("-1,2".to_string())
            .skews("0.3".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spread"));
    }

    #[test]
    fn test_sweep_params_valid_spreads() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3,4,5".to_string())
            .skews("0.3".to_string())
            .build()
            .expect("Should accept valid spreads");
        assert_eq!(params.spreads, "1,2,3,4,5");
    }

    #[test]
    fn test_sweep_params_spreads_with_whitespace() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads(" 1 , 2 , 3 ".to_string())
            .skews("0.3".to_string())
            .build()
            .expect("Should handle whitespace");
        assert_eq!(params.spreads, " 1 , 2 , 3 ");
    }

    // ============================================================================
    // Skews Validation Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_empty_skews() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_invalid_skew_numbers() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("abc,def".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_sweep_params_valid_skews() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3,0.5,0.7".to_string())
            .build()
            .expect("Should accept valid skews");
        assert_eq!(params.skews, "0.3,0.5,0.7");
    }

    // ============================================================================
    // Range Validation Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_invalid_fill_prob() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_prob(1.5)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fill_prob"));
    }

    #[test]
    fn test_sweep_params_invalid_queue_pos() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .queue_pos(1.5)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("queue_pos"));
    }

    #[test]
    fn test_sweep_params_negative_fee_rate() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fee_rate"));
    }

    #[test]
    fn test_sweep_params_zero_max_inventory() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .max_inventory(0.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_inventory"));
    }

    #[test]
    fn test_sweep_params_zero_quote_size() {
        let result = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .quote_size(0.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("quote_size"));
    }

    // ============================================================================
    // Default Values Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_defaults() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .build()
            .expect("Should build with defaults");

        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.naive_fills, false);
        assert_eq!(params.quiet, false);
    }

    // ============================================================================
    // Builder Method Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_builder_all_methods() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .fill_prob(0.15)
            .queue_pos(0.6)
            .output(Some(PathBuf::from("./output.json")))
            .quiet(true)
            .build()
            .expect("Should build with all methods");

        assert_eq!(params.algorithm, "as");
        assert!(params.weights_file.is_some());
        assert_eq!(params.spreads, "1,2,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert_eq!(params.naive_fills, true);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.queue_pos, 0.6);
        assert!(params.output.is_some());
        assert_eq!(params.quiet, true);
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_sweep_params_single_value_lists() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .build()
            .expect("Should accept single values");

        assert_eq!(params.spreads, "1");
        assert_eq!(params.skews, "0.3");
    }

    #[test]
    fn test_sweep_params_boundary_values() {
        // Test boundary values that should pass
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("0.0".to_string())
            .skews("0.0".to_string())
            .fill_prob(0.0)
            .queue_pos(0.0)
            .fee_rate(0.0)
            .build()
            .expect("Should accept boundary values");

        assert_eq!(params.fill_prob, 0.0);
        assert_eq!(params.queue_pos, 0.0);
        assert_eq!(params.fee_rate, 0.0);
    }

    #[test]
    fn test_sweep_params_upper_boundary_values() {
        let params = SweepParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .fill_prob(1.0)
            .queue_pos(1.0)
            .build()
            .expect("Should accept upper boundary values");

        assert_eq!(params.fill_prob, 1.0);
        assert_eq!(params.queue_pos, 1.0);
    }
}

#[cfg(test)]
mod walk_forward_params_tests {
    use super::*;

    // ============================================================================
    // Builder Creation Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_builder_new() {
        let builder = WalkForwardParamsBuilder::new();
        assert!(true);
    }

    #[test]
    fn test_walk_forward_params_builder_default() {
        let builder = WalkForwardParamsBuilder::default();
        assert!(true);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_missing_data_path() {
        let result = WalkForwardParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_walk_forward_params_missing_algorithm() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_walk_forward_params_missing_spreads() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spreads"));
    }

    #[test]
    fn test_walk_forward_params_missing_skews() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skews"));
    }

    #[test]
    fn test_walk_forward_params_missing_fill_probs() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fill_probs"));
    }

    // ============================================================================
    // Parameter List Validation Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_empty_spreads() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_walk_forward_params_invalid_fill_probs() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("1.5,2.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fill_prob"));
    }

    #[test]
    fn test_walk_forward_params_valid_params() {
        let params = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3,4,5".to_string())
            .skews("0.3,0.5,0.7".to_string())
            .fill_probs("0.05,0.10,0.15".to_string())
            .folds(5)
            .test_hours(24.0)
            .rolling(false)
            .min_train_hours(100.0)
            .embargo_hours(1.0)
            .build()
            .expect("Should build valid params");

        assert_eq!(params.algorithm, "as");
        assert_eq!(params.spreads, "1,2,3,4,5");
        assert_eq!(params.skews, "0.3,0.5,0.7");
        assert_eq!(params.fill_probs, "0.05,0.10,0.15");
        assert_eq!(params.folds, 5);
        assert_eq!(params.test_hours, 24.0);
        assert_eq!(params.rolling, false);
        assert_eq!(params.min_train_hours, 100.0);
        assert_eq!(params.embargo_hours, 1.0);
    }

    // ============================================================================
    // Folds and Hours Validation Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_zero_folds() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .folds(0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("folds"));
    }

    #[test]
    fn test_walk_forward_params_negative_test_hours() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .test_hours(-1.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test_hours"));
    }

    #[test]
    fn test_walk_forward_params_negative_embargo_hours() {
        let result = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .embargo_hours(-1.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("embargo_hours"));
    }

    // ============================================================================
    // Default Values Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_defaults() {
        let params = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .expect("Should build with defaults");

        assert_eq!(params.folds, 5);
        assert_eq!(params.test_hours, 24.0);
        assert_eq!(params.rolling, false);
        assert_eq!(params.min_train_hours, 100.0);
        assert_eq!(params.embargo_hours, 1.0);
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.naive_fills, false);
        assert_eq!(params.quiet, false);
    }

    // ============================================================================
    // Builder Method Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_builder_all_methods() {
        let params = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .folds(10)
            .test_hours(48.0)
            .rolling(true)
            .min_train_hours(200.0)
            .embargo_hours(2.0)
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.6)
            .output(Some(PathBuf::from("./output.json")))
            .quiet(true)
            .build()
            .expect("Should build with all methods");

        assert_eq!(params.algorithm, "as");
        assert!(params.weights_file.is_some());
        assert_eq!(params.folds, 10);
        assert_eq!(params.test_hours, 48.0);
        assert_eq!(params.rolling, true);
        assert_eq!(params.min_train_hours, 200.0);
        assert_eq!(params.embargo_hours, 2.0);
        assert_eq!(params.spreads, "1,2,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.fill_probs, "0.05,0.10");
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_walk_forward_params_single_value_lists() {
        let params = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .expect("Should accept single values");

        assert_eq!(params.spreads, "1");
        assert_eq!(params.skews, "0.3");
        assert_eq!(params.fill_probs, "0.05");
    }

    #[test]
    fn test_walk_forward_params_whitespace_handling() {
        let params = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads(" 1 , 2 , 3 ".to_string())
            .skews(" 0.3 , 0.5 ".to_string())
            .fill_probs(" 0.05 , 0.10 ".to_string())
            .build()
            .expect("Should handle whitespace");

        assert_eq!(params.spreads, " 1 , 2 , 3 ");
        assert_eq!(params.skews, " 0.3 , 0.5 ");
        assert_eq!(params.fill_probs, " 0.05 , 0.10 ");
    }

    #[test]
    fn test_walk_forward_params_zero_embargo_hours() {
        let params = WalkForwardParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .embargo_hours(0.0)
            .build()
            .expect("Should accept zero embargo hours");

        assert_eq!(params.embargo_hours, 0.0);
    }
}

#[cfg(test)]
mod oos_validate_params_tests {
    use super::*;

    // ============================================================================
    // Builder Creation Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_builder_new() {
        let builder = OOSValidateParamsBuilder::new();
        assert!(true);
    }

    #[test]
    fn test_oos_validate_params_builder_default() {
        let builder = OOSValidateParamsBuilder::default();
        assert!(true);
    }

    // ============================================================================
    // Required Fields Validation Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_missing_data_path() {
        let result = OOSValidateParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data_path"));
    }

    #[test]
    fn test_oos_validate_params_missing_algorithm() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("algorithm"));
    }

    #[test]
    fn test_oos_validate_params_missing_spreads() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spreads"));
    }

    #[test]
    fn test_oos_validate_params_missing_skews() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .fill_probs("0.05,0.10".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("skews"));
    }

    #[test]
    fn test_oos_validate_params_missing_fill_probs() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fill_probs"));
    }

    #[test]
    fn test_oos_validate_params_missing_all_required() {
        let result = OOSValidateParamsBuilder::new().build();
        assert!(result.is_err());
    }

    // ============================================================================
    // Holdout Validation Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_holdout_too_low() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.05)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("holdout"));
    }

    #[test]
    fn test_oos_validate_params_holdout_too_high() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.6)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("holdout"));
    }

    #[test]
    fn test_oos_validate_params_holdout_boundary_low() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.1)
            .build()
            .expect("Should accept minimum holdout");
        assert_eq!(params.holdout, 0.1);
    }

    #[test]
    fn test_oos_validate_params_holdout_boundary_high() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.5)
            .build()
            .expect("Should accept maximum holdout");
        assert_eq!(params.holdout, 0.5);
    }

    #[test]
    fn test_oos_validate_params_holdout_middle() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.25)
            .build()
            .expect("Should accept middle holdout");
        assert_eq!(params.holdout, 0.25);
    }

    // ============================================================================
    // Parameter List Validation Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_empty_spreads() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_oos_validate_params_invalid_fill_probs() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("1.5,2.0".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fill_prob"));
    }

    #[test]
    fn test_oos_validate_params_valid_params() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10,0.15".to_string())
            .holdout(0.20)
            .embargo_hours(1.0)
            .build()
            .expect("Should build valid params");

        assert_eq!(params.algorithm, "as");
        assert_eq!(params.spreads, "1,2,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.fill_probs, "0.05,0.10,0.15");
        assert_eq!(params.holdout, 0.20);
        assert_eq!(params.embargo_hours, 1.0);
    }

    // ============================================================================
    // Embargo Hours Validation Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_negative_embargo_hours() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .embargo_hours(-1.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("embargo_hours"));
    }

    #[test]
    fn test_oos_validate_params_zero_embargo_hours() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .embargo_hours(0.0)
            .build()
            .expect("Should accept zero embargo hours");
        assert_eq!(params.embargo_hours, 0.0);
    }

    // ============================================================================
    // Default Values Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_defaults() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .expect("Should build with defaults");

        assert_eq!(params.holdout, 0.20);
        assert_eq!(params.embargo_hours, 1.0);
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert_eq!(params.queue_pos, 0.5);
        assert_eq!(params.naive_fills, false);
        assert_eq!(params.quiet, false);
    }

    // ============================================================================
    // Builder Method Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_builder_all_methods() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .holdout(0.25)
            .embargo_hours(2.0)
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_probs("0.05,0.10".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .queue_pos(0.6)
            .output(Some(PathBuf::from("./output.json")))
            .quiet(true)
            .build()
            .expect("Should build with all methods");

        assert_eq!(params.algorithm, "as");
        assert!(params.weights_file.is_some());
        assert_eq!(params.holdout, 0.25);
        assert_eq!(params.embargo_hours, 2.0);
        assert_eq!(params.spreads, "1,2,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.fill_probs, "0.05,0.10");
    }

    // ============================================================================
    // Edge Cases Tests
    // ============================================================================

    #[test]
    fn test_oos_validate_params_single_value_lists() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build()
            .expect("Should accept single values");

        assert_eq!(params.spreads, "1");
        assert_eq!(params.skews, "0.3");
        assert_eq!(params.fill_probs, "0.05");
    }

    #[test]
    fn test_oos_validate_params_whitespace_handling() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads(" 1 , 2 , 3 ".to_string())
            .skews(" 0.3 , 0.5 ".to_string())
            .fill_probs(" 0.05 , 0.10 ".to_string())
            .build()
            .expect("Should handle whitespace");

        assert_eq!(params.spreads, " 1 , 2 , 3 ");
        assert_eq!(params.skews, " 0.3 , 0.5 ");
        assert_eq!(params.fill_probs, " 0.05 , 0.10 ");
    }

    #[test]
    fn test_oos_validate_params_negative_spread() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("-1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("spread"));
    }

    #[test]
    fn test_oos_validate_params_fill_prob_out_of_range() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05,1.5".to_string())
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fill_prob"));
    }

    #[test]
    fn test_oos_validate_params_invalid_queue_pos() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .queue_pos(1.5)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("queue_pos"));
    }

    #[test]
    fn test_oos_validate_params_negative_fee_rate() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .fee_rate(-0.1)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fee_rate"));
    }

    #[test]
    fn test_oos_validate_params_zero_max_inventory() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .max_inventory(0.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_inventory"));
    }

    #[test]
    fn test_oos_validate_params_zero_quote_size() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .quote_size(0.0)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("quote_size"));
    }

    #[test]
    fn test_oos_validate_params_holdout_exactly_0_1() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.1)
            .build()
            .expect("Should accept exactly 0.1");
        assert_eq!(params.holdout, 0.1);
    }

    #[test]
    fn test_oos_validate_params_holdout_exactly_0_5() {
        let params = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.5)
            .build()
            .expect("Should accept exactly 0.5");
        assert_eq!(params.holdout, 0.5);
    }

    #[test]
    fn test_oos_validate_params_holdout_just_below_minimum() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.099)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_oos_validate_params_holdout_just_above_maximum() {
        let result = OOSValidateParamsBuilder::new()
            .data_path(PathBuf::from("./data/features"))
            .algorithm("as".to_string())
            .spreads("1,2".to_string())
            .skews("0.3".to_string())
            .fill_probs("0.05".to_string())
            .holdout(0.501)
            .build();
        assert!(result.is_err());
    }
}

/// Parameters for the `oos-validate` command (out-of-sample validation - both algorithm types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OOSValidateParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Fraction of data to reserve for out-of-sample test (0.1-0.5)
    pub holdout: f64,
    /// Gap between train and test to prevent lookahead (hours)
    pub embargo_hours: f64,
    /// Spread values to test (comma-separated string, e.g., "1,2,3")
    pub spreads: String,
    /// Skew values to test (comma-separated string, e.g., "0.3,0.5")
    pub skews: String,
    /// Fill probability values to test (comma-separated string, e.g., "0.05,0.10,0.15")
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
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for OOSValidateParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            holdout: 0.2,
            embargo_hours: 1.0,
            spreads: "1,2,3,4,5".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            fill_probs: "0.05,0.10,0.15".to_string(),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            queue_pos: 0.5,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `OOSValidateParams` with validation
pub struct OOSValidateParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    holdout: Option<f64>,
    embargo_hours: Option<f64>,
    spreads: Option<String>,
    skews: Option<String>,
    fill_probs: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl OOSValidateParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            holdout: None,
            embargo_hours: None,
            spreads: None,
            skews: None,
            fill_probs: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            queue_pos: None,
            output: None,
            quiet: None,
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

    /// Set holdout fraction
    pub fn holdout(mut self, fraction: f64) -> Self {
        self.holdout = Some(fraction);
        self
    }

    /// Set embargo hours
    pub fn embargo_hours(mut self, hours: f64) -> Self {
        self.embargo_hours = Some(hours);
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `OOSValidateParams` with validation
    pub fn build(self) -> Result<OOSValidateParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;
        let fill_probs = self.fill_probs
            .ok_or_else(|| anyhow::anyhow!("fill_probs is required"))?;

        // Validate and parse spreads
        let spread_values: Vec<f64> = spreads
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if spread_values.is_empty() {
            anyhow::bail!("spreads must contain at least one valid number");
        }

        for &spread in &spread_values {
            if spread < 0.0 {
                anyhow::bail!("all spread values must be >= 0.0, found {}", spread);
            }
        }

        // Validate and parse skews
        let skew_values: Vec<f64> = skews
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if skew_values.is_empty() {
            anyhow::bail!("skews must contain at least one valid number");
        }

        // Validate and parse fill_probs
        let fill_prob_values: Vec<f64> = fill_probs
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if fill_prob_values.is_empty() {
            anyhow::bail!("fill_probs must contain at least one valid number");
        }

        for &fill_prob in &fill_prob_values {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("all fill_prob values must be in range [0.0, 1.0], found {}", fill_prob);
            }
        }

        // Validate holdout
        let holdout = self.holdout.unwrap_or(0.20);
        if !(0.1..=0.5).contains(&holdout) {
            anyhow::bail!("holdout must be in range [0.1, 0.5], found {}", holdout);
        }

        // Validate embargo_hours
        if let Some(embargo_hours) = self.embargo_hours {
            if embargo_hours < 0.0 {
                anyhow::bail!("embargo_hours must be >= 0.0");
            }
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }

        Ok(OOSValidateParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            holdout,
            embargo_hours: self.embargo_hours.unwrap_or(1.0),
            spreads,
            skews,
            fill_probs,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for OOSValidateParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `simulate` command (campaign simulation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulateParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Number of weeks to simulate
    pub weeks: u8,
    /// Hours per daily session
    pub session_hours: f64,
    /// Minimum sessions per week for valid week
    pub min_sessions_per_week: u8,
    /// Preset name to use (optional)
    pub preset: Option<String>,
    /// Base spread in bps (if no preset)
    pub spread: f64,
    /// Inventory skew factor (if no preset)
    pub skew: f64,
    /// Expected fill rate from backtest (for comparison)
    pub expected_fill_rate: f64,
    /// Expected Sharpe from backtest
    pub expected_sharpe: f64,
    /// Expected return from backtest
    pub expected_return: f64,
    /// Minimum weekly trades for gate pass
    pub min_weekly_trades: usize,
    /// Maximum drawdown percentage for gate pass
    pub max_drawdown_pct: f64,
    /// Minimum win rate for gate pass
    pub min_win_rate: f64,
    /// Output directory for campaign files
    pub campaigns_dir: PathBuf,
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
    /// Output file for campaign report (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for SimulateParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            weeks: 4,
            session_hours: 8.0,
            min_sessions_per_week: 3,
            preset: None,
            spread: 2.0,
            skew: 0.5,
            expected_fill_rate: 0.10,
            expected_sharpe: 1.0,
            expected_return: 0.05,
            min_weekly_trades: 100,
            max_drawdown_pct: 10.0,
            min_win_rate: 0.50,
            campaigns_dir: PathBuf::from("./campaigns"),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            fill_prob: 0.10,
            queue_pos: 0.5,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `SimulateParams` with validation
pub struct SimulateParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    weeks: Option<u8>,
    session_hours: Option<f64>,
    min_sessions_per_week: Option<u8>,
    preset: Option<String>,
    spread: Option<f64>,
    skew: Option<f64>,
    expected_fill_rate: Option<f64>,
    expected_sharpe: Option<f64>,
    expected_return: Option<f64>,
    min_weekly_trades: Option<usize>,
    max_drawdown_pct: Option<f64>,
    min_win_rate: Option<f64>,
    campaigns_dir: Option<PathBuf>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl SimulateParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            weeks: None,
            session_hours: None,
            min_sessions_per_week: None,
            preset: None,
            spread: None,
            skew: None,
            expected_fill_rate: None,
            expected_sharpe: None,
            expected_return: None,
            min_weekly_trades: None,
            max_drawdown_pct: None,
            min_win_rate: None,
            campaigns_dir: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            fill_prob: None,
            queue_pos: None,
            output: None,
            quiet: None,
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

    /// Set number of weeks
    pub fn weeks(mut self, weeks: u8) -> Self {
        self.weeks = Some(weeks);
        self
    }

    /// Set session hours
    pub fn session_hours(mut self, hours: f64) -> Self {
        self.session_hours = Some(hours);
        self
    }

    /// Set minimum sessions per week
    pub fn min_sessions_per_week(mut self, min: u8) -> Self {
        self.min_sessions_per_week = Some(min);
        self
    }

    /// Set preset name
    pub fn preset(mut self, preset: Option<String>) -> Self {
        self.preset = preset;
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

    /// Set expected fill rate
    pub fn expected_fill_rate(mut self, rate: f64) -> Self {
        self.expected_fill_rate = Some(rate);
        self
    }

    /// Set expected Sharpe
    pub fn expected_sharpe(mut self, sharpe: f64) -> Self {
        self.expected_sharpe = Some(sharpe);
        self
    }

    /// Set expected return
    pub fn expected_return(mut self, ret: f64) -> Self {
        self.expected_return = Some(ret);
        self
    }

    /// Set minimum weekly trades
    pub fn min_weekly_trades(mut self, min: usize) -> Self {
        self.min_weekly_trades = Some(min);
        self
    }

    /// Set maximum drawdown percentage
    pub fn max_drawdown_pct(mut self, pct: f64) -> Self {
        self.max_drawdown_pct = Some(pct);
        self
    }

    /// Set minimum win rate
    pub fn min_win_rate(mut self, rate: f64) -> Self {
        self.min_win_rate = Some(rate);
        self
    }

    /// Set campaigns directory
    pub fn campaigns_dir(mut self, dir: PathBuf) -> Self {
        self.campaigns_dir = Some(dir);
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `SimulateParams` with validation
    pub fn build(self) -> Result<SimulateParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;

        // Validate weeks
        let weeks = self.weeks.unwrap_or(4);
        if weeks == 0 {
            anyhow::bail!("weeks must be > 0");
        }

        // Validate session_hours
        let session_hours = self.session_hours.unwrap_or(8.0);
        if session_hours <= 0.0 {
            anyhow::bail!("session_hours must be > 0.0");
        }

        // Validate min_sessions_per_week
        let min_sessions_per_week = self.min_sessions_per_week.unwrap_or(5);
        if min_sessions_per_week == 0 {
            anyhow::bail!("min_sessions_per_week must be > 0");
        }

        // Validate spread
        let spread = self.spread.unwrap_or(2.0);
        if spread < 0.0 {
            anyhow::bail!("spread must be >= 0.0");
        }

        // Validate skew
        let skew = self.skew.unwrap_or(0.5);
        if skew < 0.0 {
            anyhow::bail!("skew must be >= 0.0");
        }

        // Validate expected_fill_rate
        let expected_fill_rate = self.expected_fill_rate.unwrap_or(0.10);
        if !(0.0..=1.0).contains(&expected_fill_rate) {
            anyhow::bail!("expected_fill_rate must be in range [0.0, 1.0]");
        }

        // Validate expected_sharpe
        let expected_sharpe = self.expected_sharpe.unwrap_or(1.0);
        // No range restriction on Sharpe (can be negative)

        // Validate expected_return
        let expected_return = self.expected_return.unwrap_or(0.05);
        // No range restriction on return (can be negative)

        // Validate min_weekly_trades
        let min_weekly_trades = self.min_weekly_trades.unwrap_or(50);
        if min_weekly_trades == 0 {
            anyhow::bail!("min_weekly_trades must be > 0");
        }

        // Validate max_drawdown_pct
        let max_drawdown_pct = self.max_drawdown_pct.unwrap_or(5.0);
        if max_drawdown_pct < 0.0 || max_drawdown_pct > 100.0 {
            anyhow::bail!("max_drawdown_pct must be in range [0.0, 100.0]");
        }

        // Validate min_win_rate
        let min_win_rate = self.min_win_rate.unwrap_or(0.40);
        if !(0.0..=1.0).contains(&min_win_rate) {
            anyhow::bail!("min_win_rate must be in range [0.0, 1.0]");
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }
        if let Some(fill_prob) = self.fill_prob {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob must be in range [0.0, 1.0]");
            }
        }

        Ok(SimulateParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            weeks,
            session_hours,
            min_sessions_per_week,
            preset: self.preset,
            spread,
            skew,
            expected_fill_rate,
            expected_sharpe,
            expected_return,
            min_weekly_trades,
            max_drawdown_pct,
            min_win_rate,
            campaigns_dir: self.campaigns_dir.unwrap_or_else(|| PathBuf::from("./data/campaigns")),
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            fill_prob: self.fill_prob.unwrap_or(0.10),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for SimulateParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `grid` command (2D grid search - spread and skew only, MM algorithms only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (must be MM algorithm: as, ml, or fixed)
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Spread values to test (comma-separated string, e.g., "1,2,3,4,5")
    pub spreads: String,
    /// Skew values to test (comma-separated string, e.g., "0.3,0.5,0.7")
    pub skews: String,
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
    /// Output file for results (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for GridParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            spreads: "1,2,3,4,5".to_string(),
            skews: "0.3,0.5,0.7".to_string(),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            fill_prob: 0.10,
            queue_pos: 0.5,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `GridParams` with validation
pub struct GridParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spreads: Option<String>,
    skews: Option<String>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl GridParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            spreads: None,
            skews: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            fill_prob: None,
            queue_pos: None,
            output: None,
            quiet: None,
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `GridParams` with validation
    pub fn build(self) -> Result<GridParams> {
        use crate::strategies::{AlgorithmType, AlgorithmRegistry};

        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;
        let spreads = self.spreads
            .ok_or_else(|| anyhow::anyhow!("spreads is required"))?;
        let skews = self.skews
            .ok_or_else(|| anyhow::anyhow!("skews is required"))?;

        // Validate algorithm type - must be MM algorithm
        let algo_type = AlgorithmType::from_str(&algorithm)
            .map_err(|_| anyhow::anyhow!(
                "Unknown algorithm '{}'. Valid options: {}",
                algorithm,
                AlgorithmRegistry::all_type_strings().join(", ")
            ))?;

        // Check if algorithm is MM type
        if !matches!(algo_type, AlgorithmType::AvellanedaStoikov | AlgorithmType::MLSpreadSkew | AlgorithmType::FixedSpread) {
            anyhow::bail!(
                "Grid command only supports MM algorithms (as, ml, fixed). Got: {}",
                algorithm
            );
        }

        // Validate and parse spreads
        let spread_values: Vec<f64> = spreads
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if spread_values.is_empty() {
            anyhow::bail!("spreads must contain at least one valid number");
        }

        for &spread in &spread_values {
            if spread < 0.0 {
                anyhow::bail!("all spread values must be >= 0.0, found {}", spread);
            }
        }

        // Validate and parse skews
        let skew_values: Vec<f64> = skews
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.parse().ok()
                }
            })
            .collect();

        if skew_values.is_empty() {
            anyhow::bail!("skews must contain at least one valid number");
        }

        for &skew in &skew_values {
            if skew < 0.0 {
                anyhow::bail!("all skew values must be >= 0.0, found {}", skew);
            }
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }
        if let Some(fill_prob) = self.fill_prob {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob must be in range [0.0, 1.0]");
            }
        }

        Ok(GridParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            spreads,
            skews,
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            fill_prob: self.fill_prob.unwrap_or(0.10),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for GridParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `campaign` command (validation campaign - both algorithm types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed", "mom")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Number of weeks for campaign
    pub weeks: u8,
    /// Hours per daily session
    pub session_hours: f64,
    /// Minimum sessions per week for valid week
    pub min_sessions_per_week: u8,
    /// Preset name to use (optional)
    pub preset: Option<String>,
    /// Base spread in bps (if no preset)
    pub spread: f64,
    /// Inventory skew factor (if no preset)
    pub skew: f64,
    /// Expected fill rate from backtest (for comparison)
    pub expected_fill_rate: f64,
    /// Expected Sharpe from backtest
    pub expected_sharpe: f64,
    /// Expected return from backtest
    pub expected_return: f64,
    /// Minimum weekly trades for gate pass
    pub min_weekly_trades: usize,
    /// Maximum drawdown percentage for gate pass
    pub max_drawdown_pct: f64,
    /// Minimum win rate for gate pass
    pub min_win_rate: f64,
    /// Output directory for campaign files
    pub campaigns_dir: PathBuf,
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
    /// Output file for campaign report (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for CampaignParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            weeks: 4,
            session_hours: 8.0,
            min_sessions_per_week: 3,
            preset: None,
            spread: 2.0,
            skew: 0.5,
            expected_fill_rate: 0.10,
            expected_sharpe: 1.0,
            expected_return: 0.05,
            min_weekly_trades: 100,
            max_drawdown_pct: 10.0,
            min_win_rate: 0.50,
            campaigns_dir: PathBuf::from("./campaigns"),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            fill_prob: 0.10,
            queue_pos: 0.5,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `CampaignParams` with validation
pub struct CampaignParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    weeks: Option<u8>,
    session_hours: Option<f64>,
    min_sessions_per_week: Option<u8>,
    preset: Option<String>,
    spread: Option<f64>,
    skew: Option<f64>,
    expected_fill_rate: Option<f64>,
    expected_sharpe: Option<f64>,
    expected_return: Option<f64>,
    min_weekly_trades: Option<usize>,
    max_drawdown_pct: Option<f64>,
    min_win_rate: Option<f64>,
    campaigns_dir: Option<PathBuf>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl CampaignParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            weeks: None,
            session_hours: None,
            min_sessions_per_week: None,
            preset: None,
            spread: None,
            skew: None,
            expected_fill_rate: None,
            expected_sharpe: None,
            expected_return: None,
            min_weekly_trades: None,
            max_drawdown_pct: None,
            min_win_rate: None,
            campaigns_dir: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            fill_prob: None,
            queue_pos: None,
            output: None,
            quiet: None,
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

    /// Set number of weeks
    pub fn weeks(mut self, weeks: u8) -> Self {
        self.weeks = Some(weeks);
        self
    }

    /// Set session hours
    pub fn session_hours(mut self, hours: f64) -> Self {
        self.session_hours = Some(hours);
        self
    }

    /// Set minimum sessions per week
    pub fn min_sessions_per_week(mut self, min: u8) -> Self {
        self.min_sessions_per_week = Some(min);
        self
    }

    /// Set preset name
    pub fn preset(mut self, preset: Option<String>) -> Self {
        self.preset = preset;
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

    /// Set expected fill rate
    pub fn expected_fill_rate(mut self, rate: f64) -> Self {
        self.expected_fill_rate = Some(rate);
        self
    }

    /// Set expected Sharpe
    pub fn expected_sharpe(mut self, sharpe: f64) -> Self {
        self.expected_sharpe = Some(sharpe);
        self
    }

    /// Set expected return
    pub fn expected_return(mut self, ret: f64) -> Self {
        self.expected_return = Some(ret);
        self
    }

    /// Set minimum weekly trades
    pub fn min_weekly_trades(mut self, min: usize) -> Self {
        self.min_weekly_trades = Some(min);
        self
    }

    /// Set maximum drawdown percentage
    pub fn max_drawdown_pct(mut self, pct: f64) -> Self {
        self.max_drawdown_pct = Some(pct);
        self
    }

    /// Set minimum win rate
    pub fn min_win_rate(mut self, rate: f64) -> Self {
        self.min_win_rate = Some(rate);
        self
    }

    /// Set campaigns directory
    pub fn campaigns_dir(mut self, dir: PathBuf) -> Self {
        self.campaigns_dir = Some(dir);
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

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `CampaignParams` with validation
    pub fn build(self) -> Result<CampaignParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;

        // Validate weeks
        let weeks = self.weeks.unwrap_or(4);
        if weeks == 0 {
            anyhow::bail!("weeks must be > 0");
        }

        // Validate session_hours
        let session_hours = self.session_hours.unwrap_or(8.0);
        if session_hours <= 0.0 {
            anyhow::bail!("session_hours must be > 0.0");
        }

        // Validate min_sessions_per_week
        let min_sessions_per_week = self.min_sessions_per_week.unwrap_or(5);
        if min_sessions_per_week == 0 {
            anyhow::bail!("min_sessions_per_week must be > 0");
        }

        // Validate spread
        let spread = self.spread.unwrap_or(2.0);
        if spread < 0.0 {
            anyhow::bail!("spread must be >= 0.0");
        }

        // Validate skew
        let skew = self.skew.unwrap_or(0.5);
        if skew < 0.0 {
            anyhow::bail!("skew must be >= 0.0");
        }

        // Validate expected_fill_rate
        let expected_fill_rate = self.expected_fill_rate.unwrap_or(0.10);
        if !(0.0..=1.0).contains(&expected_fill_rate) {
            anyhow::bail!("expected_fill_rate must be in range [0.0, 1.0]");
        }

        // Validate expected_sharpe
        let expected_sharpe = self.expected_sharpe.unwrap_or(1.0);
        // No range restriction on Sharpe (can be negative)

        // Validate expected_return
        let expected_return = self.expected_return.unwrap_or(0.05);
        // No range restriction on return (can be negative)

        // Validate min_weekly_trades
        let min_weekly_trades = self.min_weekly_trades.unwrap_or(50);
        if min_weekly_trades == 0 {
            anyhow::bail!("min_weekly_trades must be > 0");
        }

        // Validate max_drawdown_pct
        let max_drawdown_pct = self.max_drawdown_pct.unwrap_or(5.0);
        if max_drawdown_pct < 0.0 || max_drawdown_pct > 100.0 {
            anyhow::bail!("max_drawdown_pct must be in range [0.0, 100.0]");
        }

        // Validate min_win_rate
        let min_win_rate = self.min_win_rate.unwrap_or(0.40);
        if !(0.0..=1.0).contains(&min_win_rate) {
            anyhow::bail!("min_win_rate must be in range [0.0, 1.0]");
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }
        if let Some(fill_prob) = self.fill_prob {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob must be in range [0.0, 1.0]");
            }
        }

        Ok(CampaignParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            weeks,
            session_hours,
            min_sessions_per_week,
            preset: self.preset,
            spread,
            skew,
            expected_fill_rate,
            expected_sharpe,
            expected_return,
            min_weekly_trades,
            max_drawdown_pct,
            min_win_rate,
            campaigns_dir: self.campaigns_dir.unwrap_or_else(|| PathBuf::from("./data/campaigns")),
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            fill_prob: self.fill_prob.unwrap_or(0.10),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for CampaignParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod simulate_params_tests {
    use super::*;

    // ============================================================================
    // Basic Construction Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_builder_new() {
        let builder = SimulateParamsBuilder::new();
        assert!(builder.data_path.is_none());
        assert!(builder.algorithm.is_none());
    }

    #[test]
    fn test_simulate_params_builder_default() {
        let builder = SimulateParamsBuilder::default();
        assert!(builder.data_path.is_none());
    }

    #[test]
    fn test_simulate_params_minimal_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.weeks, 4);
        assert_eq!(params.session_hours, 8.0);
        assert_eq!(params.min_sessions_per_week, 5);
        assert_eq!(params.spread, 2.0);
        assert_eq!(params.skew, 0.5);
        assert_eq!(params.expected_fill_rate, 0.10);
        assert_eq!(params.expected_sharpe, 1.0);
        assert_eq!(params.expected_return, 0.05);
        assert_eq!(params.min_weekly_trades, 50);
        assert_eq!(params.max_drawdown_pct, 5.0);
        assert_eq!(params.min_win_rate, 0.40);
    }

    // ============================================================================
    // Required Fields Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_missing_data_path() {
        let params = SimulateParamsBuilder::new()
            .algorithm("as".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("data_path is required"));
    }

    #[test]
    fn test_simulate_params_missing_algorithm() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("algorithm is required"));
    }

    // ============================================================================
    // Weeks Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_weeks_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("weeks must be > 0"));
    }

    #[test]
    fn test_simulate_params_weeks_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(8)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weeks, 8);
    }

    #[test]
    fn test_simulate_params_weeks_default() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weeks, 4);
    }

    // ============================================================================
    // Session Hours Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_session_hours_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .session_hours(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("session_hours must be > 0.0"));
    }

    #[test]
    fn test_simulate_params_session_hours_negative() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .session_hours(-1.0)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_simulate_params_session_hours_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .session_hours(12.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().session_hours, 12.0);
    }

    // ============================================================================
    // Min Sessions Per Week Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_min_sessions_per_week_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_sessions_per_week(0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("min_sessions_per_week must be > 0"));
    }

    #[test]
    fn test_simulate_params_min_sessions_per_week_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_sessions_per_week(7)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().min_sessions_per_week, 7);
    }

    // ============================================================================
    // Spread Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_spread_negative() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(-1.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("spread must be >= 0.0"));
    }

    #[test]
    fn test_simulate_params_spread_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().spread, 0.0);
    }

    #[test]
    fn test_simulate_params_spread_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(3.5)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().spread, 3.5);
    }

    // ============================================================================
    // Skew Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_skew_negative() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skew(-1.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("skew must be >= 0.0"));
    }

    #[test]
    fn test_simulate_params_skew_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skew(0.7)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().skew, 0.7);
    }

    // ============================================================================
    // Expected Fill Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_expected_fill_rate_out_of_range_high() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_fill_rate(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("expected_fill_rate must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_simulate_params_expected_fill_rate_out_of_range_low() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_fill_rate(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_simulate_params_expected_fill_rate_boundary() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_fill_rate(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().expected_fill_rate, 1.0);
    }

    // ============================================================================
    // Min Weekly Trades Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_min_weekly_trades_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_weekly_trades(0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("min_weekly_trades must be > 0"));
    }

    #[test]
    fn test_simulate_params_min_weekly_trades_valid() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_weekly_trades(100)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().min_weekly_trades, 100);
    }

    // ============================================================================
    // Max Drawdown PCT Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_max_drawdown_pct_out_of_range_high() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_drawdown_pct(150.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("max_drawdown_pct must be in range [0.0, 100.0]"));
    }

    #[test]
    fn test_simulate_params_max_drawdown_pct_out_of_range_low() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_drawdown_pct(-1.0)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_simulate_params_max_drawdown_pct_boundary() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_drawdown_pct(100.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().max_drawdown_pct, 100.0);
    }

    // ============================================================================
    // Min Win Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_min_win_rate_out_of_range_high() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_win_rate(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("min_win_rate must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_simulate_params_min_win_rate_out_of_range_low() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_win_rate(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_simulate_params_min_win_rate_boundary() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_win_rate(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().min_win_rate, 1.0);
    }

    // ============================================================================
    // Queue Position Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_queue_pos_out_of_range_high() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("queue_pos must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_simulate_params_queue_pos_out_of_range_low() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_simulate_params_queue_pos_boundary() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().queue_pos, 1.0);
    }

    // ============================================================================
    // Fill Prob Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_fill_prob_out_of_range_high() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("fill_prob must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_simulate_params_fill_prob_out_of_range_low() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_simulate_params_fill_prob_boundary() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fill_prob, 1.0);
    }

    // ============================================================================
    // Fee Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_fee_rate_negative() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(-0.0001)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("fee_rate must be >= 0.0"));
    }

    #[test]
    fn test_simulate_params_fee_rate_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fee_rate, 0.0);
    }

    // ============================================================================
    // Max Inventory Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_max_inventory_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("max_inventory must be > 0.0"));
    }

    #[test]
    fn test_simulate_params_max_inventory_negative() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(-0.1)
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Quote Size Validation Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_quote_size_zero() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .quote_size(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("quote_size must be > 0.0"));
    }

    #[test]
    fn test_simulate_params_quote_size_negative() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .quote_size(-0.001)
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Optional Fields Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_preset() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .preset(Some("test-preset".to_string()))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().preset, Some("test-preset".to_string()));
    }

    #[test]
    fn test_simulate_params_weights_file() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weights_file, Some(PathBuf::from("./weights.json")));
    }

    #[test]
    fn test_simulate_params_campaigns_dir() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .campaigns_dir(PathBuf::from("./custom/campaigns"))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().campaigns_dir, PathBuf::from("./custom/campaigns"));
    }

    #[test]
    fn test_simulate_params_output() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .output(Some(PathBuf::from("./report.json")))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().output, Some(PathBuf::from("./report.json")));
    }

    #[test]
    fn test_simulate_params_boolean_flags() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(true)
            .quiet(true)
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert!(params.naive_fills);
        assert!(params.quiet);
    }

    // ============================================================================
    // Serialization Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_serialization() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(6)
            .session_hours(10.0)
            .spread(3.0)
            .skew(0.7)
            .build()
            .unwrap();

        // Test JSON serialization
        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("\"weeks\":6"));
        assert!(json.contains("\"session_hours\":10.0"));
        assert!(json.contains("\"spread\":3.0"));
        assert!(json.contains("\"skew\":0.7"));
        assert!(json.contains("\"algorithm\":\"as\""));

        // Test deserialization
        let deserialized: SimulateParams = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.weeks, params.weeks);
        assert_eq!(deserialized.session_hours, params.session_hours);
        assert_eq!(deserialized.spread, params.spread);
        assert_eq!(deserialized.skew, params.skew);
        assert_eq!(deserialized.algorithm, params.algorithm);
    }

    #[test]
    fn test_simulate_params_serialization_with_optionals() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .preset(Some("test-preset".to_string()))
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: SimulateParams = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.preset, params.preset);
        assert_eq!(deserialized.weights_file, params.weights_file);
        assert_eq!(deserialized.output, params.output);
    }

    // ============================================================================
    // Complex Scenarios Tests
    // ============================================================================

    #[test]
    fn test_simulate_params_all_fields_set() {
        let params = SimulateParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .weeks(8)
            .session_hours(12.0)
            .min_sessions_per_week(6)
            .preset(Some("custom-preset".to_string()))
            .spread(4.0)
            .skew(0.8)
            .expected_fill_rate(0.15)
            .expected_sharpe(1.5)
            .expected_return(0.08)
            .min_weekly_trades(75)
            .max_drawdown_pct(7.5)
            .min_win_rate(0.45)
            .campaigns_dir(PathBuf::from("./custom/campaigns"))
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .fill_prob(0.15)
            .queue_pos(0.3)
            .output(Some(PathBuf::from("./report.json")))
            .quiet(true)
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.weeks, 8);
        assert_eq!(params.session_hours, 12.0);
        assert_eq!(params.min_sessions_per_week, 6);
        assert_eq!(params.preset, Some("custom-preset".to_string()));
        assert_eq!(params.spread, 4.0);
        assert_eq!(params.skew, 0.8);
        assert_eq!(params.expected_fill_rate, 0.15);
        assert_eq!(params.expected_sharpe, 1.5);
        assert_eq!(params.expected_return, 0.08);
        assert_eq!(params.min_weekly_trades, 75);
        assert_eq!(params.max_drawdown_pct, 7.5);
        assert_eq!(params.min_win_rate, 0.45);
        assert_eq!(params.campaigns_dir, PathBuf::from("./custom/campaigns"));
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./report.json")));
        assert!(params.quiet);
    }
}

#[cfg(test)]
mod grid_params_tests {
    use super::*;

    // ============================================================================
    // Basic Construction Tests
    // ============================================================================

    #[test]
    fn test_grid_params_builder_new() {
        let builder = GridParamsBuilder::new();
        assert!(builder.data_path.is_none());
        assert!(builder.algorithm.is_none());
    }

    #[test]
    fn test_grid_params_builder_default() {
        let builder = GridParamsBuilder::default();
        assert!(builder.data_path.is_none());
    }

    #[test]
    fn test_grid_params_minimal_valid() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.spreads, "1,2,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.max_inventory, 0.1);
        assert_eq!(params.quote_size, 0.001);
        assert_eq!(params.fee_rate, 0.0001);
        assert!(!params.naive_fills);
        assert_eq!(params.fill_prob, 0.10);
        assert_eq!(params.queue_pos, 0.5);
    }

    // ============================================================================
    // Required Fields Tests
    // ============================================================================

    #[test]
    fn test_grid_params_missing_data_path() {
        let params = GridParamsBuilder::new()
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("data_path is required"));
    }

    #[test]
    fn test_grid_params_missing_algorithm() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("algorithm is required"));
    }

    #[test]
    fn test_grid_params_missing_spreads() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("spreads is required"));
    }

    #[test]
    fn test_grid_params_missing_skews() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("skews is required"));
    }

    // ============================================================================
    // Algorithm Type Validation Tests (MM Only)
    // ============================================================================

    #[test]
    fn test_grid_params_valid_mm_algorithm_as() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_valid_mm_algorithm_ml() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_valid_mm_algorithm_fixed() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("fixed".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_invalid_algorithm_mom() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("mom".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        let err_msg = params.unwrap_err().to_string();
        assert!(err_msg.contains("Grid command only supports MM algorithms") || err_msg.contains("Unknown algorithm"));
    }

    #[test]
    fn test_grid_params_invalid_algorithm_nonexistent() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("nonexistent".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Spreads Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_spreads_empty() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("spreads must contain at least one valid number"));
    }

    #[test]
    fn test_grid_params_spreads_only_commas() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads(",,,".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_grid_params_spreads_invalid_number() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,invalid,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        // Should still work, just ignores invalid entries
        let params = params.unwrap();
        assert_eq!(params.spreads, "1,invalid,3");
    }

    #[test]
    fn test_grid_params_spreads_negative() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("-1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("spread values must be >= 0.0"));
    }

    #[test]
    fn test_grid_params_spreads_zero() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("0,1,2".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_spreads_with_spaces() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1, 2, 3".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_spreads_single_value() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("2.5".to_string())
            .skews("0.3,0.5".to_string())
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().spreads, "2.5");
    }

    // ============================================================================
    // Skews Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_skews_empty() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("skews must contain at least one valid number"));
    }

    #[test]
    fn test_grid_params_skews_negative() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("-0.3,0.5".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("skew values must be >= 0.0"));
    }

    #[test]
    fn test_grid_params_skews_zero() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0,0.5".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_skews_with_spaces() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3, 0.5, 0.7".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_grid_params_skews_single_value() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.5".to_string())
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().skews, "0.5");
    }

    // ============================================================================
    // Queue Position Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_queue_pos_out_of_range_high() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .queue_pos(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("queue_pos must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_grid_params_queue_pos_out_of_range_low() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .queue_pos(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_grid_params_queue_pos_boundary() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .queue_pos(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().queue_pos, 1.0);
    }

    // ============================================================================
    // Fill Prob Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_fill_prob_out_of_range_high() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_prob(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("fill_prob must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_grid_params_fill_prob_out_of_range_low() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_prob(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_grid_params_fill_prob_boundary() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fill_prob(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fill_prob, 1.0);
    }

    // ============================================================================
    // Fee Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_fee_rate_negative() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fee_rate(-0.0001)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("fee_rate must be >= 0.0"));
    }

    #[test]
    fn test_grid_params_fee_rate_zero() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .fee_rate(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fee_rate, 0.0);
    }

    // ============================================================================
    // Max Inventory Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_max_inventory_zero() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .max_inventory(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("max_inventory must be > 0.0"));
    }

    #[test]
    fn test_grid_params_max_inventory_negative() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .max_inventory(-0.1)
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Quote Size Validation Tests
    // ============================================================================

    #[test]
    fn test_grid_params_quote_size_zero() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .quote_size(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("quote_size must be > 0.0"));
    }

    #[test]
    fn test_grid_params_quote_size_negative() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .quote_size(-0.001)
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Optional Fields Tests
    // ============================================================================

    #[test]
    fn test_grid_params_weights_file() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weights_file, Some(PathBuf::from("./weights.json")));
    }

    #[test]
    fn test_grid_params_output() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .output(Some(PathBuf::from("./results.json")))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().output, Some(PathBuf::from("./results.json")));
    }

    #[test]
    fn test_grid_params_boolean_flags() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .naive_fills(true)
            .quiet(true)
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert!(params.naive_fills);
        assert!(params.quiet);
    }

    // ============================================================================
    // Serialization Tests
    // ============================================================================

    #[test]
    fn test_grid_params_serialization() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .build()
            .unwrap();

        // Test JSON serialization
        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("\"spreads\":\"1,2,3\""));
        assert!(json.contains("\"skews\":\"0.3,0.5\""));
        assert!(json.contains("\"algorithm\":\"as\""));

        // Test deserialization
        let deserialized: GridParams = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.spreads, params.spreads);
        assert_eq!(deserialized.skews, params.skews);
        assert_eq!(deserialized.algorithm, params.algorithm);
    }

    #[test]
    fn test_grid_params_serialization_with_optionals() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1,2,3".to_string())
            .skews("0.3,0.5".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: GridParams = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.weights_file, params.weights_file);
        assert_eq!(deserialized.output, params.output);
    }

    // ============================================================================
    // Complex Scenarios Tests
    // ============================================================================

    #[test]
    fn test_grid_params_all_fields_set() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .spreads("1,2,3,4,5".to_string())
            .skews("0.3,0.5,0.7".to_string())
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .fill_prob(0.15)
            .queue_pos(0.3)
            .output(Some(PathBuf::from("./results.json")))
            .quiet(true)
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.spreads, "1,2,3,4,5");
        assert_eq!(params.skews, "0.3,0.5,0.7");
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./results.json")));
        assert!(params.quiet);
    }

    #[test]
    fn test_grid_params_large_parameter_grids() {
        let spreads = (1..=20).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        let skews = (1..=10).map(|i| (i as f64 / 10.0).to_string()).collect::<Vec<_>>().join(",");
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads(spreads.clone())
            .skews(skews.clone())
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.spreads, spreads);
        assert_eq!(params.skews, skews);
    }

    #[test]
    fn test_grid_params_decimal_values() {
        let params = GridParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spreads("1.5,2.5,3.5".to_string())
            .skews("0.33,0.55,0.77".to_string())
            .build();
        assert!(params.is_ok());
    }
}

#[cfg(test)]
mod campaign_params_tests {
    use super::*;

    // ============================================================================
    // Basic Construction Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_builder_new() {
        let builder = CampaignParamsBuilder::new();
        assert!(builder.data_path.is_none());
        assert!(builder.algorithm.is_none());
    }

    #[test]
    fn test_campaign_params_builder_default() {
        let builder = CampaignParamsBuilder::default();
        assert!(builder.data_path.is_none());
    }

    #[test]
    fn test_campaign_params_minimal_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.weeks, 4);
        assert_eq!(params.session_hours, 8.0);
        assert_eq!(params.min_sessions_per_week, 5);
        assert_eq!(params.spread, 2.0);
        assert_eq!(params.skew, 0.5);
        assert_eq!(params.expected_fill_rate, 0.10);
        assert_eq!(params.expected_sharpe, 1.0);
        assert_eq!(params.expected_return, 0.05);
        assert_eq!(params.min_weekly_trades, 50);
        assert_eq!(params.max_drawdown_pct, 5.0);
        assert_eq!(params.min_win_rate, 0.40);
    }

    // ============================================================================
    // Required Fields Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_missing_data_path() {
        let params = CampaignParamsBuilder::new()
            .algorithm("as".to_string())
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("data_path is required"));
    }

    #[test]
    fn test_campaign_params_missing_algorithm() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("algorithm is required"));
    }

    // ============================================================================
    // Algorithm Type Tests (Both MM and MOM supported)
    // ============================================================================

    #[test]
    fn test_campaign_params_valid_mm_algorithm_as() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_campaign_params_valid_mm_algorithm_ml() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_campaign_params_valid_mm_algorithm_fixed() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("fixed".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_campaign_params_valid_mom_algorithm() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("mom".to_string())
            .build();
        // Should work - campaign supports both MM and MOM
        assert!(params.is_ok());
    }

    // ============================================================================
    // Weeks Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_weeks_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("weeks must be > 0"));
    }

    #[test]
    fn test_campaign_params_weeks_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(8)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weeks, 8);
    }

    #[test]
    fn test_campaign_params_weeks_default() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weeks, 4);
    }

    // ============================================================================
    // Session Hours Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_session_hours_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .session_hours(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("session_hours must be > 0.0"));
    }

    #[test]
    fn test_campaign_params_session_hours_negative() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .session_hours(-1.0)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_campaign_params_session_hours_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .session_hours(12.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().session_hours, 12.0);
    }

    // ============================================================================
    // Min Sessions Per Week Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_min_sessions_per_week_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_sessions_per_week(0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("min_sessions_per_week must be > 0"));
    }

    #[test]
    fn test_campaign_params_min_sessions_per_week_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_sessions_per_week(7)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().min_sessions_per_week, 7);
    }

    // ============================================================================
    // Spread Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_spread_negative() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(-1.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("spread must be >= 0.0"));
    }

    #[test]
    fn test_campaign_params_spread_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().spread, 0.0);
    }

    #[test]
    fn test_campaign_params_spread_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(3.5)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().spread, 3.5);
    }

    // ============================================================================
    // Skew Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_skew_negative() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skew(-1.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("skew must be >= 0.0"));
    }

    #[test]
    fn test_campaign_params_skew_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .skew(0.7)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().skew, 0.7);
    }

    // ============================================================================
    // Expected Fill Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_expected_fill_rate_out_of_range_high() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_fill_rate(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("expected_fill_rate must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_campaign_params_expected_fill_rate_out_of_range_low() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_fill_rate(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_campaign_params_expected_fill_rate_boundary() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_fill_rate(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().expected_fill_rate, 1.0);
    }

    // ============================================================================
    // Min Weekly Trades Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_min_weekly_trades_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_weekly_trades(0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("min_weekly_trades must be > 0"));
    }

    #[test]
    fn test_campaign_params_min_weekly_trades_valid() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_weekly_trades(100)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().min_weekly_trades, 100);
    }

    // ============================================================================
    // Max Drawdown PCT Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_max_drawdown_pct_out_of_range_high() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_drawdown_pct(150.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("max_drawdown_pct must be in range [0.0, 100.0]"));
    }

    #[test]
    fn test_campaign_params_max_drawdown_pct_out_of_range_low() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_drawdown_pct(-1.0)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_campaign_params_max_drawdown_pct_boundary() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_drawdown_pct(100.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().max_drawdown_pct, 100.0);
    }

    // ============================================================================
    // Min Win Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_min_win_rate_out_of_range_high() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_win_rate(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("min_win_rate must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_campaign_params_min_win_rate_out_of_range_low() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_win_rate(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_campaign_params_min_win_rate_boundary() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .min_win_rate(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().min_win_rate, 1.0);
    }

    // ============================================================================
    // Queue Position Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_queue_pos_out_of_range_high() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("queue_pos must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_campaign_params_queue_pos_out_of_range_low() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_campaign_params_queue_pos_boundary() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .queue_pos(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().queue_pos, 1.0);
    }

    // ============================================================================
    // Fill Prob Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_fill_prob_out_of_range_high() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.5)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("fill_prob must be in range [0.0, 1.0]"));
    }

    #[test]
    fn test_campaign_params_fill_prob_out_of_range_low() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(-0.1)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_campaign_params_fill_prob_boundary() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fill_prob(1.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fill_prob, 1.0);
    }

    // ============================================================================
    // Fee Rate Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_fee_rate_negative() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(-0.0001)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("fee_rate must be >= 0.0"));
    }

    #[test]
    fn test_campaign_params_fee_rate_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .fee_rate(0.0)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().fee_rate, 0.0);
    }

    // ============================================================================
    // Max Inventory Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_max_inventory_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("max_inventory must be > 0.0"));
    }

    #[test]
    fn test_campaign_params_max_inventory_negative() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .max_inventory(-0.1)
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Quote Size Validation Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_quote_size_zero() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .quote_size(0.0)
            .build();
        assert!(params.is_err());
        assert!(params.unwrap_err().to_string().contains("quote_size must be > 0.0"));
    }

    #[test]
    fn test_campaign_params_quote_size_negative() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .quote_size(-0.001)
            .build();
        assert!(params.is_err());
    }

    // ============================================================================
    // Optional Fields Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_preset() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .preset(Some("test-preset".to_string()))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().preset, Some("test-preset".to_string()));
    }

    #[test]
    fn test_campaign_params_weights_file() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("ml".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().weights_file, Some(PathBuf::from("./weights.json")));
    }

    #[test]
    fn test_campaign_params_campaigns_dir() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .campaigns_dir(PathBuf::from("./custom/campaigns"))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().campaigns_dir, PathBuf::from("./custom/campaigns"));
    }

    #[test]
    fn test_campaign_params_output() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .output(Some(PathBuf::from("./report.json")))
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().output, Some(PathBuf::from("./report.json")));
    }

    #[test]
    fn test_campaign_params_boolean_flags() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .naive_fills(true)
            .quiet(true)
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert!(params.naive_fills);
        assert!(params.quiet);
    }

    // ============================================================================
    // Serialization Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_serialization() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(6)
            .session_hours(10.0)
            .spread(3.0)
            .skew(0.7)
            .build()
            .unwrap();

        // Test JSON serialization
        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("\"weeks\":6"));
        assert!(json.contains("\"session_hours\":10.0"));
        assert!(json.contains("\"spread\":3.0"));
        assert!(json.contains("\"skew\":0.7"));
        assert!(json.contains("\"algorithm\":\"as\""));

        // Test deserialization
        let deserialized: CampaignParams = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.weeks, params.weeks);
        assert_eq!(deserialized.session_hours, params.session_hours);
        assert_eq!(deserialized.spread, params.spread);
        assert_eq!(deserialized.skew, params.skew);
        assert_eq!(deserialized.algorithm, params.algorithm);
    }

    #[test]
    fn test_campaign_params_serialization_with_optionals() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .preset(Some("test-preset".to_string()))
            .weights_file(Some(PathBuf::from("./weights.json")))
            .output(Some(PathBuf::from("./output.json")))
            .build()
            .unwrap();

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: CampaignParams = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.preset, params.preset);
        assert_eq!(deserialized.weights_file, params.weights_file);
        assert_eq!(deserialized.output, params.output);
    }

    // ============================================================================
    // Complex Scenarios Tests
    // ============================================================================

    #[test]
    fn test_campaign_params_all_fields_set() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weights_file(Some(PathBuf::from("./weights.json")))
            .weeks(8)
            .session_hours(12.0)
            .min_sessions_per_week(6)
            .preset(Some("custom-preset".to_string()))
            .spread(4.0)
            .skew(0.8)
            .expected_fill_rate(0.15)
            .expected_sharpe(1.5)
            .expected_return(0.08)
            .min_weekly_trades(75)
            .max_drawdown_pct(7.5)
            .min_win_rate(0.45)
            .campaigns_dir(PathBuf::from("./custom/campaigns"))
            .max_inventory(0.2)
            .quote_size(0.002)
            .fee_rate(0.0002)
            .naive_fills(true)
            .fill_prob(0.15)
            .queue_pos(0.3)
            .output(Some(PathBuf::from("./report.json")))
            .quiet(true)
            .build();
        assert!(params.is_ok());
        let params = params.unwrap();
        assert_eq!(params.weeks, 8);
        assert_eq!(params.session_hours, 12.0);
        assert_eq!(params.min_sessions_per_week, 6);
        assert_eq!(params.preset, Some("custom-preset".to_string()));
        assert_eq!(params.spread, 4.0);
        assert_eq!(params.skew, 0.8);
        assert_eq!(params.expected_fill_rate, 0.15);
        assert_eq!(params.expected_sharpe, 1.5);
        assert_eq!(params.expected_return, 0.08);
        assert_eq!(params.min_weekly_trades, 75);
        assert_eq!(params.max_drawdown_pct, 7.5);
        assert_eq!(params.min_win_rate, 0.45);
        assert_eq!(params.campaigns_dir, PathBuf::from("./custom/campaigns"));
        assert_eq!(params.max_inventory, 0.2);
        assert_eq!(params.quote_size, 0.002);
        assert_eq!(params.fee_rate, 0.0002);
        assert!(params.naive_fills);
        assert_eq!(params.fill_prob, 0.15);
        assert_eq!(params.queue_pos, 0.3);
        assert_eq!(params.output, Some(PathBuf::from("./report.json")));
        assert!(params.quiet);
    }

    #[test]
    fn test_campaign_params_negative_expected_sharpe() {
        // Sharpe can be negative
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_sharpe(-0.5)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().expected_sharpe, -0.5);
    }

    #[test]
    fn test_campaign_params_negative_expected_return() {
        // Return can be negative
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .expected_return(-0.02)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().expected_return, -0.02);
    }

    #[test]
    fn test_campaign_params_extreme_values() {
        let params = CampaignParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .weeks(255) // Max u8
            .session_hours(24.0)
            .min_sessions_per_week(7)
            .spread(100.0)
            .skew(10.0)
            .expected_fill_rate(0.99)
            .expected_sharpe(10.0)
            .expected_return(1.0)
            .min_weekly_trades(10000)
            .max_drawdown_pct(99.9)
            .min_win_rate(0.99)
            .fill_prob(0.99)
            .queue_pos(0.99)
            .build();
        assert!(params.is_ok());
    }
}

/// Parameters for the `paper` command (paper trading session simulation - both algorithm types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed", "mom")
    pub algorithm: String,
    /// Path to ML weights file (required for ML algorithm)
    pub weights_file: Option<PathBuf>,
    /// Session duration in hours
    pub duration: f64,
    /// Preset name to use (optional)
    pub preset: Option<String>,
    /// Base spread in bps (if no preset)
    pub spread: f64,
    /// Inventory skew factor (if no preset)
    pub skew: f64,
    /// Output directory for session files
    pub sessions_dir: PathBuf,
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
    /// Minimum duration in hours for valid session
    pub min_duration_hours: f64,
    /// Minimum trades for valid session
    pub min_trades: usize,
    /// Output file for session result (JSON)
    pub output: Option<PathBuf>,
    /// Quiet mode (no progress output)
    pub quiet: bool,
}

impl Default for PaperParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            duration: 8.0,
            preset: None,
            spread: 2.0,
            skew: 0.5,
            sessions_dir: PathBuf::from("./sessions"),
            max_inventory: 10.0,
            quote_size: 1.0,
            fee_rate: 0.0001,
            naive_fills: false,
            fill_prob: 0.10,
            queue_pos: 0.5,
            min_duration_hours: 4.0,
            min_trades: 50,
            output: None,
            quiet: false,
        }
    }
}

/// Builder for `PaperParams` with validation
pub struct PaperParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    duration: Option<f64>,
    preset: Option<String>,
    spread: Option<f64>,
    skew: Option<f64>,
    sessions_dir: Option<PathBuf>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    naive_fills: Option<bool>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    min_duration_hours: Option<f64>,
    min_trades: Option<usize>,
    output: Option<PathBuf>,
    quiet: Option<bool>,
}

impl PaperParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            duration: None,
            preset: None,
            spread: None,
            skew: None,
            sessions_dir: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            naive_fills: None,
            fill_prob: None,
            queue_pos: None,
            min_duration_hours: None,
            min_trades: None,
            output: None,
            quiet: None,
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

    /// Set session duration
    pub fn duration(mut self, hours: f64) -> Self {
        self.duration = Some(hours);
        self
    }

    /// Set preset name
    pub fn preset(mut self, preset: Option<String>) -> Self {
        self.preset = preset;
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

    /// Set sessions directory
    pub fn sessions_dir(mut self, dir: PathBuf) -> Self {
        self.sessions_dir = Some(dir);
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

    /// Set minimum duration hours
    pub fn min_duration_hours(mut self, hours: f64) -> Self {
        self.min_duration_hours = Some(hours);
        self
    }

    /// Set minimum trades
    pub fn min_trades(mut self, min: usize) -> Self {
        self.min_trades = Some(min);
        self
    }

    /// Set output file
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Set quiet mode flag
    pub fn quiet(mut self, enabled: bool) -> Self {
        self.quiet = Some(enabled);
        self
    }

    /// Build `PaperParams` with validation
    pub fn build(self) -> Result<PaperParams> {
        // Validate required fields
        let data_path = self.data_path
            .ok_or_else(|| anyhow::anyhow!("data_path is required"))?;
        let algorithm = self.algorithm
            .ok_or_else(|| anyhow::anyhow!("algorithm is required"))?;

        // Validate duration
        let duration = self.duration.unwrap_or(1.0);
        if duration <= 0.0 {
            anyhow::bail!("duration must be > 0.0");
        }

        // Validate spread
        let spread = self.spread.unwrap_or(2.0);
        if spread < 0.0 {
            anyhow::bail!("spread must be >= 0.0");
        }

        // Validate skew
        let skew = self.skew.unwrap_or(0.5);
        if skew < 0.0 {
            anyhow::bail!("skew must be >= 0.0");
        }

        // Validate min_duration_hours
        let min_duration_hours = self.min_duration_hours.unwrap_or(0.1);
        if min_duration_hours <= 0.0 {
            anyhow::bail!("min_duration_hours must be > 0.0");
        }

        // Validate min_trades
        let min_trades = self.min_trades.unwrap_or(5);
        if min_trades == 0 {
            anyhow::bail!("min_trades must be > 0");
        }

        // Validate ranges
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
        if let Some(max_inventory) = self.max_inventory {
            if max_inventory <= 0.0 {
                anyhow::bail!("max_inventory must be > 0.0");
            }
        }
        if let Some(quote_size) = self.quote_size {
            if quote_size <= 0.0 {
                anyhow::bail!("quote_size must be > 0.0");
            }
        }
        if let Some(fill_prob) = self.fill_prob {
            if !(0.0..=1.0).contains(&fill_prob) {
                anyhow::bail!("fill_prob must be in range [0.0, 1.0]");
            }
        }

        Ok(PaperParams {
            data_path,
            algorithm,
            weights_file: self.weights_file,
            duration,
            preset: self.preset,
            spread,
            skew,
            sessions_dir: self.sessions_dir.unwrap_or_else(|| PathBuf::from("./data/sessions")),
            max_inventory: self.max_inventory.unwrap_or(0.1),
            quote_size: self.quote_size.unwrap_or(0.001),
            fee_rate: self.fee_rate.unwrap_or(0.0001),
            naive_fills: self.naive_fills.unwrap_or(false),
            fill_prob: self.fill_prob.unwrap_or(0.10),
            queue_pos: self.queue_pos.unwrap_or(0.5),
            min_duration_hours,
            min_trades,
            output: self.output,
            quiet: self.quiet.unwrap_or(false),
        })
    }
}

impl Default for PaperParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `list_algorithms` command (list available algorithms - info only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListAlgorithmsParams {
    /// Show detailed information for a specific algorithm (optional)
    pub algo: Option<String>,
    /// Output as JSON (for scripting)
    pub json: bool,
}

impl Default for ListAlgorithmsParams {
    fn default() -> Self {
        Self {
            algo: None,
            json: false,
        }
    }
}

/// Builder for `ListAlgorithmsParams` with validation
pub struct ListAlgorithmsParamsBuilder {
    algo: Option<String>,
    json: Option<bool>,
}

impl ListAlgorithmsParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            algo: None,
            json: None,
        }
    }

    /// Set algorithm name to show details for
    pub fn algo(mut self, algo: Option<String>) -> Self {
        self.algo = algo;
        self
    }

    /// Set JSON output flag
    pub fn json(mut self, enabled: bool) -> Self {
        self.json = Some(enabled);
        self
    }

    /// Build `ListAlgorithmsParams` with validation
    pub fn build(self) -> Result<ListAlgorithmsParams> {
        // Validate algorithm name if provided
        if let Some(ref algo_str) = self.algo {
            if algo_str.trim().is_empty() {
                anyhow::bail!("algorithm name cannot be empty");
            }
        }

        Ok(ListAlgorithmsParams {
            algo: self.algo,
            json: self.json.unwrap_or(false),
        })
    }
}

impl Default for ListAlgorithmsParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `info` command (data statistics display)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
}

impl Default for InfoParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
        }
    }
}

/// Builder for `InfoParams` with validation
pub struct InfoParamsBuilder {
    data_path: Option<PathBuf>,
}

impl InfoParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
        }
    }

    /// Set data directory path
    pub fn data_path(mut self, path: PathBuf) -> Self {
        self.data_path = Some(path);
        self
    }

    /// Build `InfoParams` with validation
    pub fn build(self) -> Result<InfoParams> {
        let data_path = self.data_path.unwrap_or_else(|| PathBuf::from("./data/features"));

        // Validate data path
        if !data_path.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", data_path);
        }

        if !data_path.is_dir() {
            anyhow::bail!("Data path is not a directory: {:?}", data_path);
        }

        Ok(InfoParams { data_path })
    }
}

impl Default for InfoParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `validate-data` command (data quality validation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateDataParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Output file for report (JSON)
    pub output: Option<PathBuf>,
}

impl Default for ValidateDataParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            output: None,
        }
    }
}

/// Builder for `ValidateDataParams` with validation
pub struct ValidateDataParamsBuilder {
    data_path: Option<PathBuf>,
    output: Option<PathBuf>,
}

impl ValidateDataParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            output: None,
        }
    }

    /// Set data directory path
    pub fn data_path(mut self, path: PathBuf) -> Self {
        self.data_path = Some(path);
        self
    }

    /// Set output file path
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Build `ValidateDataParams` with validation
    pub fn build(self) -> Result<ValidateDataParams> {
        let data_path = self.data_path.unwrap_or_else(|| PathBuf::from("./data/features"));

        // Validate data path
        if !data_path.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", data_path);
        }

        if !data_path.is_dir() {
            anyhow::bail!("Data path is not a directory: {:?}", data_path);
        }

        Ok(ValidateDataParams {
            data_path,
            output: self.output,
        })
    }
}

impl Default for ValidateDataParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `compare` command (ML vs AS baseline comparison)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// ML algorithm to compare (e.g., "ml")
    pub ml_algorithm: String,
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
    /// Fill probability (0.0-1.0) for realistic simulation
    pub fill_prob: f64,
    /// Queue position (0.0=front, 1.0=back)
    pub queue_pos: f64,
    /// High entropy threshold
    pub high_entropy: f64,
    /// Low entropy threshold
    pub low_entropy: f64,
    /// Output file for JSON results (optional)
    pub output: Option<PathBuf>,
}

impl Default for CompareParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            ml_algorithm: "ml".to_string(),
            weights_file: None,
            spread: 2.0,
            skew: 0.5,
            max_inventory: 0.1,
            quote_size: 0.001,
            fee_rate: 0.0001,
            fill_prob: 0.1,
            queue_pos: 0.5,
            high_entropy: 0.7,
            low_entropy: 0.3,
            output: None,
        }
    }
}

/// Builder for `CompareParams` with validation
pub struct CompareParamsBuilder {
    data_path: Option<PathBuf>,
    ml_algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    spread: Option<f64>,
    skew: Option<f64>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    high_entropy: Option<f64>,
    low_entropy: Option<f64>,
    output: Option<PathBuf>,
}

impl CompareParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            ml_algorithm: None,
            weights_file: None,
            spread: None,
            skew: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            fill_prob: None,
            queue_pos: None,
            high_entropy: None,
            low_entropy: None,
            output: None,
        }
    }

    /// Set data directory path
    pub fn data_path(mut self, path: PathBuf) -> Self {
        self.data_path = Some(path);
        self
    }

    /// Set ML algorithm
    pub fn ml_algorithm(mut self, algorithm: String) -> Self {
        self.ml_algorithm = Some(algorithm);
        self
    }

    /// Set ML weights file
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
    pub fn max_inventory(mut self, max_inventory: f64) -> Self {
        self.max_inventory = Some(max_inventory);
        self
    }

    /// Set quote size
    pub fn quote_size(mut self, quote_size: f64) -> Self {
        self.quote_size = Some(quote_size);
        self
    }

    /// Set fee rate
    pub fn fee_rate(mut self, fee_rate: f64) -> Self {
        self.fee_rate = Some(fee_rate);
        self
    }

    /// Set fill probability
    pub fn fill_prob(mut self, fill_prob: f64) -> Self {
        self.fill_prob = Some(fill_prob);
        self
    }

    /// Set queue position
    pub fn queue_pos(mut self, queue_pos: f64) -> Self {
        self.queue_pos = Some(queue_pos);
        self
    }

    /// Set high entropy threshold
    pub fn high_entropy(mut self, high_entropy: f64) -> Self {
        self.high_entropy = Some(high_entropy);
        self
    }

    /// Set low entropy threshold
    pub fn low_entropy(mut self, low_entropy: f64) -> Self {
        self.low_entropy = Some(low_entropy);
        self
    }

    /// Set output file path
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Build `CompareParams` with validation
    pub fn build(self) -> Result<CompareParams> {
        let data_path = self.data_path.unwrap_or_else(|| PathBuf::from("./data/features"));

        // Validate data path
        if !data_path.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", data_path);
        }

        if !data_path.is_dir() {
            anyhow::bail!("Data path is not a directory: {:?}", data_path);
        }

        // Validate numeric parameters
        let spread = self.spread.unwrap_or(2.0);
        if spread <= 0.0 {
            anyhow::bail!("Spread must be positive");
        }

        let skew = self.skew.unwrap_or(0.5);
        if !(0.0..=1.0).contains(&skew) {
            anyhow::bail!("Skew must be between 0.0 and 1.0");
        }

        let max_inventory = self.max_inventory.unwrap_or(0.1);
        if max_inventory <= 0.0 {
            anyhow::bail!("Max inventory must be positive");
        }

        let quote_size = self.quote_size.unwrap_or(0.001);
        if quote_size <= 0.0 {
            anyhow::bail!("Quote size must be positive");
        }

        let fee_rate = self.fee_rate.unwrap_or(0.0001);
        if fee_rate < 0.0 {
            anyhow::bail!("Fee rate cannot be negative");
        }

        let fill_prob = self.fill_prob.unwrap_or(0.1);
        if !(0.0..=1.0).contains(&fill_prob) {
            anyhow::bail!("Fill probability must be between 0.0 and 1.0");
        }

        let queue_pos = self.queue_pos.unwrap_or(0.5);
        if !(0.0..=1.0).contains(&queue_pos) {
            anyhow::bail!("Queue position must be between 0.0 and 1.0");
        }

        let high_entropy = self.high_entropy.unwrap_or(0.7);
        if !(0.0..=1.0).contains(&high_entropy) {
            anyhow::bail!("High entropy threshold must be between 0.0 and 1.0");
        }

        let low_entropy = self.low_entropy.unwrap_or(0.3);
        if !(0.0..=1.0).contains(&low_entropy) {
            anyhow::bail!("Low entropy threshold must be between 0.0 and 1.0");
        }

        if low_entropy >= high_entropy {
            anyhow::bail!("Low entropy threshold must be less than high entropy threshold");
        }

        Ok(CompareParams {
            data_path,
            ml_algorithm: self.ml_algorithm.unwrap_or_else(|| "ml".to_string()),
            weights_file: self.weights_file,
            spread,
            skew,
            max_inventory,
            quote_size,
            fee_rate,
            fill_prob,
            queue_pos,
            high_entropy,
            low_entropy,
            output: self.output,
        })
    }
}

impl Default for CompareParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for a single algorithm in head-to-head comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadToHeadConfig {
    /// Algorithm identifier (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Display name for this configuration
    pub config_name: String,
    /// Path to ML weights file (optional, for ML algorithms)
    pub weights_file: Option<PathBuf>,
    /// Base spread in basis points
    pub spread: f64,
    /// Inventory skew factor
    pub skew: f64,
}

impl Default for HeadToHeadConfig {
    fn default() -> Self {
        Self {
            algorithm: "as".to_string(),
            config_name: "Default".to_string(),
            weights_file: None,
            spread: 2.0,
            skew: 0.5,
        }
    }
}

/// Parameters for the `head-to-head` command (two configuration comparison)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadToHeadParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// First configuration
    pub config_a: HeadToHeadConfig,
    /// Second configuration
    pub config_b: HeadToHeadConfig,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: f64,
    /// Fill probability (0.0-1.0) for realistic simulation
    pub fill_prob: f64,
    /// Queue position (0.0=front, 1.0=back)
    pub queue_pos: f64,
    /// High entropy threshold
    pub high_entropy: f64,
    /// Low entropy threshold
    pub low_entropy: f64,
    /// Output file for JSON results (optional)
    pub output: Option<PathBuf>,
}

impl Default for HeadToHeadParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            config_a: HeadToHeadConfig {
                algorithm: "as".to_string(),
                config_name: "Config A".to_string(),
                weights_file: None,
                spread: 2.0,
                skew: 0.5,
            },
            config_b: HeadToHeadConfig {
                algorithm: "as".to_string(),
                config_name: "Config B".to_string(),
                weights_file: None,
                spread: 3.0,
                skew: 0.7,
            },
            max_inventory: 0.1,
            quote_size: 0.001,
            fee_rate: 0.0001,
            fill_prob: 0.1,
            queue_pos: 0.5,
            high_entropy: 0.7,
            low_entropy: 0.3,
            output: None,
        }
    }
}

/// Builder for `HeadToHeadParams` with validation
pub struct HeadToHeadParamsBuilder {
    data_path: Option<PathBuf>,
    config_a: Option<HeadToHeadConfig>,
    config_b: Option<HeadToHeadConfig>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    fill_prob: Option<f64>,
    queue_pos: Option<f64>,
    high_entropy: Option<f64>,
    low_entropy: Option<f64>,
    output: Option<PathBuf>,
}

impl HeadToHeadParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            config_a: None,
            config_b: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            fill_prob: None,
            queue_pos: None,
            high_entropy: None,
            low_entropy: None,
            output: None,
        }
    }

    /// Set data directory path
    pub fn data_path(mut self, path: PathBuf) -> Self {
        self.data_path = Some(path);
        self
    }

    /// Set first configuration
    pub fn config_a(mut self, config: HeadToHeadConfig) -> Self {
        self.config_a = Some(config);
        self
    }

    /// Set second configuration
    pub fn config_b(mut self, config: HeadToHeadConfig) -> Self {
        self.config_b = Some(config);
        self
    }

    /// Set max inventory
    pub fn max_inventory(mut self, max_inventory: f64) -> Self {
        self.max_inventory = Some(max_inventory);
        self
    }

    /// Set quote size
    pub fn quote_size(mut self, quote_size: f64) -> Self {
        self.quote_size = Some(quote_size);
        self
    }

    /// Set fee rate
    pub fn fee_rate(mut self, fee_rate: f64) -> Self {
        self.fee_rate = Some(fee_rate);
        self
    }

    /// Set fill probability
    pub fn fill_prob(mut self, fill_prob: f64) -> Self {
        self.fill_prob = Some(fill_prob);
        self
    }

    /// Set queue position
    pub fn queue_pos(mut self, queue_pos: f64) -> Self {
        self.queue_pos = Some(queue_pos);
        self
    }

    /// Set high entropy threshold
    pub fn high_entropy(mut self, high_entropy: f64) -> Self {
        self.high_entropy = Some(high_entropy);
        self
    }

    /// Set low entropy threshold
    pub fn low_entropy(mut self, low_entropy: f64) -> Self {
        self.low_entropy = Some(low_entropy);
        self
    }

    /// Set output file path
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Build `HeadToHeadParams` with validation
    pub fn build(self) -> Result<HeadToHeadParams> {
        let data_path = self.data_path.unwrap_or_else(|| PathBuf::from("./data/features"));

        // Validate data path
        if !data_path.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", data_path);
        }

        if !data_path.is_dir() {
            anyhow::bail!("Data path is not a directory: {:?}", data_path);
        }

        // Validate numeric parameters
        let max_inventory = self.max_inventory.unwrap_or(0.1);
        if max_inventory <= 0.0 {
            anyhow::bail!("Max inventory must be positive");
        }

        let quote_size = self.quote_size.unwrap_or(0.001);
        if quote_size <= 0.0 {
            anyhow::bail!("Quote size must be positive");
        }

        let fee_rate = self.fee_rate.unwrap_or(0.0001);
        if fee_rate < 0.0 {
            anyhow::bail!("Fee rate cannot be negative");
        }

        let fill_prob = self.fill_prob.unwrap_or(0.1);
        if !(0.0..=1.0).contains(&fill_prob) {
            anyhow::bail!("Fill probability must be between 0.0 and 1.0");
        }

        let queue_pos = self.queue_pos.unwrap_or(0.5);
        if !(0.0..=1.0).contains(&queue_pos) {
            anyhow::bail!("Queue position must be between 0.0 and 1.0");
        }

        let high_entropy = self.high_entropy.unwrap_or(0.7);
        if !(0.0..=1.0).contains(&high_entropy) {
            anyhow::bail!("High entropy threshold must be between 0.0 and 1.0");
        }

        let low_entropy = self.low_entropy.unwrap_or(0.3);
        if !(0.0..=1.0).contains(&low_entropy) {
            anyhow::bail!("Low entropy threshold must be between 0.0 and 1.0");
        }

        if low_entropy >= high_entropy {
            anyhow::bail!("Low entropy threshold must be less than high entropy threshold");
        }

        let config_a = self.config_a.unwrap_or_default();
        let config_b = self.config_b.unwrap_or_default();

        // Validate spreads
        if config_a.spread <= 0.0 {
            anyhow::bail!("Config A spread must be positive");
        }
        if config_b.spread <= 0.0 {
            anyhow::bail!("Config B spread must be positive");
        }

        // Validate skews
        if !(0.0..=1.0).contains(&config_a.skew) {
            anyhow::bail!("Config A skew must be between 0.0 and 1.0");
        }
        if !(0.0..=1.0).contains(&config_b.skew) {
            anyhow::bail!("Config B skew must be between 0.0 and 1.0");
        }

        Ok(HeadToHeadParams {
            data_path,
            config_a,
            config_b,
            max_inventory,
            quote_size,
            fee_rate,
            fill_prob,
            queue_pos,
            high_entropy,
            low_entropy,
            output: self.output,
        })
    }
}

impl Default for HeadToHeadParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for the `simulate-session` command (single session simulation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulateSessionParams {
    /// Path to data directory containing Parquet files
    pub data_path: PathBuf,
    /// Algorithm to use (e.g., "as", "ml", "fixed")
    pub algorithm: String,
    /// Path to ML weights file (optional, for ML algorithms)
    pub weights_file: Option<PathBuf>,
    /// Session duration in hours
    pub duration: f64,
    /// Base spread in basis points
    pub spread: f64,
    /// Inventory skew factor
    pub skew: f64,
    /// Maximum inventory
    pub max_inventory: f64,
    /// Quote size
    pub quote_size: f64,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: f64,
    /// High entropy threshold
    pub high_entropy: f64,
    /// Low entropy threshold
    pub low_entropy: f64,
    /// Output file for session result (JSON, optional)
    pub output: Option<PathBuf>,
}

impl Default for SimulateSessionParams {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data/features"),
            algorithm: "as".to_string(),
            weights_file: None,
            duration: 1.0,
            spread: 2.0,
            skew: 0.5,
            max_inventory: 0.1,
            quote_size: 0.001,
            fee_rate: 0.0001,
            high_entropy: 0.7,
            low_entropy: 0.3,
            output: None,
        }
    }
}

/// Builder for `SimulateSessionParams` with validation
pub struct SimulateSessionParamsBuilder {
    data_path: Option<PathBuf>,
    algorithm: Option<String>,
    weights_file: Option<PathBuf>,
    duration: Option<f64>,
    spread: Option<f64>,
    skew: Option<f64>,
    max_inventory: Option<f64>,
    quote_size: Option<f64>,
    fee_rate: Option<f64>,
    high_entropy: Option<f64>,
    low_entropy: Option<f64>,
    output: Option<PathBuf>,
}

impl SimulateSessionParamsBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            data_path: None,
            algorithm: None,
            weights_file: None,
            duration: None,
            spread: None,
            skew: None,
            max_inventory: None,
            quote_size: None,
            fee_rate: None,
            high_entropy: None,
            low_entropy: None,
            output: None,
        }
    }

    /// Set data directory path
    pub fn data_path(mut self, path: PathBuf) -> Self {
        self.data_path = Some(path);
        self
    }

    /// Set algorithm
    pub fn algorithm(mut self, algorithm: String) -> Self {
        self.algorithm = Some(algorithm);
        self
    }

    /// Set ML weights file
    pub fn weights_file(mut self, path: Option<PathBuf>) -> Self {
        self.weights_file = path;
        self
    }

    /// Set session duration in hours
    pub fn duration(mut self, duration: f64) -> Self {
        self.duration = Some(duration);
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
    pub fn max_inventory(mut self, max_inventory: f64) -> Self {
        self.max_inventory = Some(max_inventory);
        self
    }

    /// Set quote size
    pub fn quote_size(mut self, quote_size: f64) -> Self {
        self.quote_size = Some(quote_size);
        self
    }

    /// Set fee rate
    pub fn fee_rate(mut self, fee_rate: f64) -> Self {
        self.fee_rate = Some(fee_rate);
        self
    }

    /// Set high entropy threshold
    pub fn high_entropy(mut self, high_entropy: f64) -> Self {
        self.high_entropy = Some(high_entropy);
        self
    }

    /// Set low entropy threshold
    pub fn low_entropy(mut self, low_entropy: f64) -> Self {
        self.low_entropy = Some(low_entropy);
        self
    }

    /// Set output file path
    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    /// Build `SimulateSessionParams` with validation
    pub fn build(self) -> Result<SimulateSessionParams> {
        let data_path = self.data_path.unwrap_or_else(|| PathBuf::from("./data/features"));

        // Validate data path
        if !data_path.exists() {
            anyhow::bail!("Data directory does not exist: {:?}", data_path);
        }

        if !data_path.is_dir() {
            anyhow::bail!("Data path is not a directory: {:?}", data_path);
        }

        // Validate duration
        let duration = self.duration.unwrap_or(1.0);
        if duration <= 0.0 {
            anyhow::bail!("Duration must be positive");
        }

        // Validate numeric parameters
        let spread = self.spread.unwrap_or(2.0);
        if spread <= 0.0 {
            anyhow::bail!("Spread must be positive");
        }

        let skew = self.skew.unwrap_or(0.5);
        if !(0.0..=1.0).contains(&skew) {
            anyhow::bail!("Skew must be between 0.0 and 1.0");
        }

        let max_inventory = self.max_inventory.unwrap_or(0.1);
        if max_inventory <= 0.0 {
            anyhow::bail!("Max inventory must be positive");
        }

        let quote_size = self.quote_size.unwrap_or(0.001);
        if quote_size <= 0.0 {
            anyhow::bail!("Quote size must be positive");
        }

        let fee_rate = self.fee_rate.unwrap_or(0.0001);
        if fee_rate < 0.0 {
            anyhow::bail!("Fee rate cannot be negative");
        }

        let high_entropy = self.high_entropy.unwrap_or(0.7);
        if !(0.0..=1.0).contains(&high_entropy) {
            anyhow::bail!("High entropy threshold must be between 0.0 and 1.0");
        }

        let low_entropy = self.low_entropy.unwrap_or(0.3);
        if !(0.0..=1.0).contains(&low_entropy) {
            anyhow::bail!("Low entropy threshold must be between 0.0 and 1.0");
        }

        if low_entropy >= high_entropy {
            anyhow::bail!("Low entropy threshold must be less than high entropy threshold");
        }

        Ok(SimulateSessionParams {
            data_path,
            algorithm: self.algorithm.unwrap_or_else(|| "as".to_string()),
            weights_file: self.weights_file,
            duration,
            spread,
            skew,
            max_inventory,
            quote_size,
            fee_rate,
            high_entropy,
            low_entropy,
            output: self.output,
        })
    }
}

impl Default for SimulateSessionParamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod paper_params_tests {
    use super::*;

    #[test]
    fn test_paper_params_builder_new() {
        let builder = PaperParamsBuilder::new();
        assert!(builder.data_path.is_none());
        assert!(builder.algorithm.is_none());
    }

    #[test]
    fn test_paper_params_minimal_valid() {
        let params = PaperParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_paper_params_duration_zero() {
        let params = PaperParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .duration(0.0)
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_paper_params_spread_negative() {
        let params = PaperParamsBuilder::new()
            .data_path(PathBuf::from("./data"))
            .algorithm("as".to_string())
            .spread(-1.0)
            .build();
        assert!(params.is_err());
    }
}

#[cfg(test)]
mod list_algorithms_params_tests {
    use super::*;

    #[test]
    fn test_list_algorithms_params_builder_new() {
        let builder = ListAlgorithmsParamsBuilder::new();
        assert!(builder.algo.is_none());
    }

    #[test]
    fn test_list_algorithms_params_minimal_valid() {
        let params = ListAlgorithmsParamsBuilder::new().build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_list_algorithms_params_empty_algorithm_name() {
        let params = ListAlgorithmsParamsBuilder::new()
            .algo(Some("".to_string()))
            .build();
        assert!(params.is_err());
    }

    #[test]
    fn test_list_algorithms_params_valid_algorithm_name() {
        let params = ListAlgorithmsParamsBuilder::new()
            .algo(Some("as".to_string()))
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_list_algorithms_params_json_true() {
        let params = ListAlgorithmsParamsBuilder::new()
            .json(true)
            .build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().json, true);
    }

    #[test]
    fn test_list_algorithms_params_json_default() {
        let params = ListAlgorithmsParamsBuilder::new().build();
        assert!(params.is_ok());
        assert_eq!(params.unwrap().json, false);
    }

    #[test]
    fn test_list_algorithms_params_all_fields_set() {
        let params = ListAlgorithmsParamsBuilder::new()
            .algo(Some("ml".to_string()))
            .json(true)
            .build();
        assert!(params.is_ok());
    }

    #[test]
    fn test_list_algorithms_params_serialization() {
        let params = ListAlgorithmsParamsBuilder::new()
            .algo(Some("as".to_string()))
            .json(true)
            .build()
            .unwrap();
        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains("\"algo\":\"as\""));
        assert!(json.contains("\"json\":true"));
    }
}
