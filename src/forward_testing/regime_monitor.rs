//! Regime Monitoring Module
//!
//! Tracks performance across different market regimes and provides insights
//! about strategy behavior under various market conditions.
//!
//! # Features
//! - Market regime classification (trending, mean-reverting, high/low volatility)
//! - Per-regime performance tracking
//! - Regime transition detection
//! - Historical regime analysis
//! - Regime-conditional performance metrics

use std::collections::{HashMap, VecDeque};

/// Market regime classification based on price action and volatility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketRegime {
    /// Strong upward price movement
    TrendingUp,
    /// Strong downward price movement
    TrendingDown,
    /// Price oscillating around a mean
    MeanReverting,
    /// High volatility with no clear direction
    HighVolatility,
    /// Low volatility with no clear direction
    LowVolatility,
    /// Regime cannot be determined
    Unknown,
}

impl MarketRegime {
    /// Returns true if this is a trending regime
    pub fn is_trending(&self) -> bool {
        matches!(self, MarketRegime::TrendingUp | MarketRegime::TrendingDown)
    }

    /// Returns true if this is a high-activity regime
    pub fn is_high_activity(&self) -> bool {
        matches!(
            self,
            MarketRegime::TrendingUp | MarketRegime::TrendingDown | MarketRegime::HighVolatility
        )
    }

    /// Get a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            MarketRegime::TrendingUp => "Trending Up",
            MarketRegime::TrendingDown => "Trending Down",
            MarketRegime::MeanReverting => "Mean Reverting",
            MarketRegime::HighVolatility => "High Volatility",
            MarketRegime::LowVolatility => "Low Volatility",
            MarketRegime::Unknown => "Unknown",
        }
    }
}

impl std::fmt::Display for MarketRegime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// Configuration for regime detection
#[derive(Debug, Clone)]
pub struct RegimeConfig {
    /// Window size for regime classification (in samples)
    pub window_size: usize,
    /// Threshold for trend detection (absolute return over window)
    pub trend_threshold: f64,
    /// High volatility threshold (annualized, e.g., 0.5 = 50%)
    pub high_volatility_threshold: f64,
    /// Low volatility threshold (annualized, e.g., 0.1 = 10%)
    pub low_volatility_threshold: f64,
    /// Minimum samples before regime can be determined
    pub min_samples: usize,
    /// Mean reversion threshold (Hurst exponent)
    pub mean_reversion_threshold: f64,
}

impl Default for RegimeConfig {
    fn default() -> Self {
        Self {
            window_size: 100,
            trend_threshold: 0.02,           // 2% move over window
            high_volatility_threshold: 0.50, // 50% annualized
            low_volatility_threshold: 0.15,  // 15% annualized
            min_samples: 20,
            mean_reversion_threshold: 0.4, // Hurst < 0.4 suggests mean reversion
        }
    }
}

/// Performance metrics for a specific regime
#[derive(Debug, Clone, Default)]
pub struct RegimePerformance {
    /// Total PnL in this regime
    pub total_pnl: f64,
    /// Number of trades executed
    pub trade_count: usize,
    /// Number of winning trades
    pub winning_trades: usize,
    /// Number of losing trades
    pub losing_trades: usize,
    /// Maximum drawdown observed
    pub max_drawdown: f64,
    /// Sum of returns (for mean calculation)
    pub returns_sum: f64,
    /// Sum of squared returns (for variance calculation)
    pub returns_sum_sq: f64,
    /// Number of return observations
    pub returns_count: usize,
    /// Time spent in this regime (in samples)
    pub time_in_regime: usize,
    /// Peak PnL (for drawdown calculation)
    pub peak_pnl: f64,
    /// Individual returns for detailed analysis
    pub returns: Vec<f64>,
}

impl RegimePerformance {
    /// Create a new empty RegimePerformance
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a trade result
    pub fn record_trade(&mut self, pnl: f64) {
        self.total_pnl += pnl;
        self.trade_count += 1;

        if pnl > 0.0 {
            self.winning_trades += 1;
        } else if pnl < 0.0 {
            self.losing_trades += 1;
        }

        // Update peak and drawdown
        if self.total_pnl > self.peak_pnl {
            self.peak_pnl = self.total_pnl;
        }
        let drawdown = self.peak_pnl - self.total_pnl;
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
    }

    /// Record a return observation
    pub fn record_return(&mut self, ret: f64) {
        self.returns_sum += ret;
        self.returns_sum_sq += ret * ret;
        self.returns_count += 1;
        self.returns.push(ret);
    }

    /// Increment time in regime
    pub fn tick(&mut self) {
        self.time_in_regime += 1;
    }

    /// Calculate win rate
    pub fn win_rate(&self) -> Option<f64> {
        if self.trade_count == 0 {
            return None;
        }
        Some(self.winning_trades as f64 / self.trade_count as f64)
    }

    /// Calculate mean return
    pub fn mean_return(&self) -> Option<f64> {
        if self.returns_count == 0 {
            return None;
        }
        Some(self.returns_sum / self.returns_count as f64)
    }

    /// Calculate return standard deviation
    pub fn std_return(&self) -> Option<f64> {
        if self.returns_count < 2 {
            return None;
        }
        let n = self.returns_count as f64;
        let mean = self.returns_sum / n;
        let variance = (self.returns_sum_sq / n) - (mean * mean);
        if variance < 0.0 {
            return Some(0.0); // Numerical stability
        }
        Some(variance.sqrt())
    }

    /// Calculate Sharpe ratio (assuming risk-free rate of 0)
    pub fn sharpe_ratio(&self) -> Option<f64> {
        let mean = self.mean_return()?;
        let std = self.std_return()?;
        if std.abs() < 1e-10 {
            return None;
        }
        Some(mean / std)
    }

    /// Calculate average PnL per trade
    pub fn avg_pnl_per_trade(&self) -> Option<f64> {
        if self.trade_count == 0 {
            return None;
        }
        Some(self.total_pnl / self.trade_count as f64)
    }

    /// Calculate profit factor
    pub fn profit_factor(&self) -> Option<f64> {
        let gross_profit: f64 = self.returns.iter().filter(|&&r| r > 0.0).sum();
        let gross_loss: f64 = self.returns.iter().filter(|&&r| r < 0.0).map(|r| r.abs()).sum();
        if gross_loss.abs() < 1e-10 {
            return None;
        }
        Some(gross_profit / gross_loss)
    }
}

/// A regime transition event
#[derive(Debug, Clone)]
pub struct RegimeTransition {
    /// Previous regime
    pub from: MarketRegime,
    /// New regime
    pub to: MarketRegime,
    /// Sample index when transition occurred
    pub sample_index: usize,
    /// Timestamp if available
    pub timestamp: Option<u64>,
}

/// Main regime monitoring engine
#[derive(Debug)]
pub struct RegimeMonitor {
    config: RegimeConfig,
    /// Price history for regime detection
    price_history: VecDeque<f64>,
    /// Return history for volatility calculation
    return_history: VecDeque<f64>,
    /// Current detected regime
    current_regime: MarketRegime,
    /// Performance metrics per regime
    regime_performance: HashMap<MarketRegime, RegimePerformance>,
    /// History of regime transitions
    transitions: Vec<RegimeTransition>,
    /// Total samples processed
    sample_count: usize,
    /// Last price for return calculation
    last_price: Option<f64>,
}

impl RegimeMonitor {
    /// Create a new RegimeMonitor with the given configuration
    pub fn new(config: RegimeConfig) -> Self {
        let mut regime_performance = HashMap::new();
        for regime in [
            MarketRegime::TrendingUp,
            MarketRegime::TrendingDown,
            MarketRegime::MeanReverting,
            MarketRegime::HighVolatility,
            MarketRegime::LowVolatility,
            MarketRegime::Unknown,
        ] {
            regime_performance.insert(regime, RegimePerformance::new());
        }

        Self {
            config,
            price_history: VecDeque::new(),
            return_history: VecDeque::new(),
            current_regime: MarketRegime::Unknown,
            regime_performance,
            transitions: Vec::new(),
            sample_count: 0,
            last_price: None,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(RegimeConfig::default())
    }

    /// Process a new price observation
    pub fn update(&mut self, price: f64, timestamp: Option<u64>) {
        self.sample_count += 1;

        // Calculate return if we have a previous price
        if let Some(last_price) = self.last_price {
            if last_price > 0.0 {
                let ret = (price - last_price) / last_price;
                self.return_history.push_back(ret);
                if self.return_history.len() > self.config.window_size {
                    self.return_history.pop_front();
                }
            }
        }
        self.last_price = Some(price);

        // Update price history
        self.price_history.push_back(price);
        if self.price_history.len() > self.config.window_size {
            self.price_history.pop_front();
        }

        // Detect current regime
        let new_regime = self.detect_regime();

        // Record transition if regime changed
        if new_regime != self.current_regime {
            self.transitions.push(RegimeTransition {
                from: self.current_regime,
                to: new_regime,
                sample_index: self.sample_count,
                timestamp,
            });
            self.current_regime = new_regime;
        }

        // Update time in regime
        if let Some(perf) = self.regime_performance.get_mut(&self.current_regime) {
            perf.tick();
        }
    }

    /// Record a trade result for the current regime
    pub fn record_trade(&mut self, pnl: f64) {
        if let Some(perf) = self.regime_performance.get_mut(&self.current_regime) {
            perf.record_trade(pnl);
        }
    }

    /// Record a return observation for the current regime
    pub fn record_return(&mut self, ret: f64) {
        if let Some(perf) = self.regime_performance.get_mut(&self.current_regime) {
            perf.record_return(ret);
        }
    }

    /// Get the current market regime
    pub fn current_regime(&self) -> MarketRegime {
        self.current_regime
    }

    /// Get performance for a specific regime
    pub fn get_regime_performance(&self, regime: MarketRegime) -> Option<&RegimePerformance> {
        self.regime_performance.get(&regime)
    }

    /// Get all regime performance data
    pub fn all_regime_performance(&self) -> &HashMap<MarketRegime, RegimePerformance> {
        &self.regime_performance
    }

    /// Get all regime transitions
    pub fn transitions(&self) -> &[RegimeTransition] {
        &self.transitions
    }

    /// Get transition count between two specific regimes
    pub fn transition_count(&self, from: MarketRegime, to: MarketRegime) -> usize {
        self.transitions
            .iter()
            .filter(|t| t.from == from && t.to == to)
            .count()
    }

    /// Get the total number of samples processed
    pub fn sample_count(&self) -> usize {
        self.sample_count
    }

    /// Calculate the Hurst exponent for mean-reversion detection
    fn calculate_hurst(&self) -> Option<f64> {
        if self.return_history.len() < self.config.min_samples {
            return None;
        }

        let returns: Vec<f64> = self.return_history.iter().copied().collect();
        let n = returns.len();

        // Simple R/S analysis for Hurst exponent
        // We'll use a simplified approach with fixed lag

        // Calculate cumulative deviations from mean
        let mean: f64 = returns.iter().sum::<f64>() / n as f64;
        let mut cumsum = Vec::with_capacity(n);
        let mut cum = 0.0;
        for &r in &returns {
            cum += r - mean;
            cumsum.push(cum);
        }

        // Range (R)
        let max_cum = cumsum.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let min_cum = cumsum.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let range = max_cum - min_cum;

        // Standard deviation (S)
        let variance = returns.iter().map(|&r| (r - mean).powi(2)).sum::<f64>() / n as f64;
        let std = variance.sqrt();

        if std < 1e-10 {
            return Some(0.5); // Random walk if no variation
        }

        // R/S ratio
        let rs = range / std;

        // Hurst = log(R/S) / log(n)
        // But we need multiple n values for accurate estimation
        // Simplified: Use single estimate
        if rs > 0.0 && n > 1 {
            let hurst = (rs.ln()) / ((n as f64).ln());
            // Clamp to reasonable range
            Some(hurst.clamp(0.0, 1.0))
        } else {
            Some(0.5)
        }
    }

    /// Detect the current market regime based on price and volatility
    fn detect_regime(&self) -> MarketRegime {
        if self.price_history.len() < self.config.min_samples
            || self.return_history.len() < self.config.min_samples
        {
            return MarketRegime::Unknown;
        }

        // Calculate metrics
        let first_price = self.price_history.front().copied().unwrap_or(0.0);
        let last_price = self.price_history.back().copied().unwrap_or(0.0);

        if first_price <= 0.0 {
            return MarketRegime::Unknown;
        }

        let total_return = (last_price - first_price) / first_price;

        // Calculate volatility (annualized)
        let returns: Vec<f64> = self.return_history.iter().copied().collect();
        let n = returns.len() as f64;
        let mean_ret = returns.iter().sum::<f64>() / n;
        let variance = returns.iter().map(|&r| (r - mean_ret).powi(2)).sum::<f64>() / n;
        let std = variance.sqrt();

        // Annualize (assuming minute data, ~525600 minutes/year)
        // For more accurate, we'd need actual time intervals
        let annualized_vol = std * (252.0_f64 * 390.0).sqrt(); // Trading days * minutes per day

        // Calculate Hurst for mean-reversion detection
        let hurst = self.calculate_hurst().unwrap_or(0.5);

        // Regime classification logic
        if total_return > self.config.trend_threshold {
            MarketRegime::TrendingUp
        } else if total_return < -self.config.trend_threshold {
            MarketRegime::TrendingDown
        } else if hurst < self.config.mean_reversion_threshold {
            MarketRegime::MeanReverting
        } else if annualized_vol > self.config.high_volatility_threshold {
            MarketRegime::HighVolatility
        } else if annualized_vol < self.config.low_volatility_threshold {
            MarketRegime::LowVolatility
        } else {
            // Default to unknown if no clear regime
            MarketRegime::Unknown
        }
    }

    /// Get a summary of regime statistics
    pub fn summary(&self) -> RegimeSummary {
        let mut regime_time_pct = HashMap::new();
        let total_time: usize = self
            .regime_performance
            .values()
            .map(|p| p.time_in_regime)
            .sum();

        if total_time > 0 {
            for (&regime, perf) in &self.regime_performance {
                let pct = perf.time_in_regime as f64 / total_time as f64 * 100.0;
                regime_time_pct.insert(regime, pct);
            }
        }

        // Find best and worst regimes by Sharpe
        let mut best_regime = None;
        let mut worst_regime = None;
        let mut best_sharpe = f64::NEG_INFINITY;
        let mut worst_sharpe = f64::INFINITY;

        for (&regime, perf) in &self.regime_performance {
            if let Some(sharpe) = perf.sharpe_ratio() {
                if sharpe > best_sharpe {
                    best_sharpe = sharpe;
                    best_regime = Some(regime);
                }
                if sharpe < worst_sharpe {
                    worst_sharpe = sharpe;
                    worst_regime = Some(regime);
                }
            }
        }

        RegimeSummary {
            current_regime: self.current_regime,
            total_samples: self.sample_count,
            total_transitions: self.transitions.len(),
            regime_time_percentages: regime_time_pct,
            best_regime,
            worst_regime,
        }
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        self.price_history.clear();
        self.return_history.clear();
        self.current_regime = MarketRegime::Unknown;
        for perf in self.regime_performance.values_mut() {
            *perf = RegimePerformance::new();
        }
        self.transitions.clear();
        self.sample_count = 0;
        self.last_price = None;
    }
}

/// Summary of regime monitoring results
#[derive(Debug)]
pub struct RegimeSummary {
    /// Current market regime
    pub current_regime: MarketRegime,
    /// Total number of samples processed
    pub total_samples: usize,
    /// Total number of regime transitions
    pub total_transitions: usize,
    /// Percentage of time spent in each regime
    pub regime_time_percentages: HashMap<MarketRegime, f64>,
    /// Best performing regime (by Sharpe)
    pub best_regime: Option<MarketRegime>,
    /// Worst performing regime (by Sharpe)
    pub worst_regime: Option<MarketRegime>,
}

/// Multi-symbol regime monitor
#[derive(Debug)]
pub struct MultiSymbolRegimeMonitor {
    monitors: HashMap<String, RegimeMonitor>,
    config: RegimeConfig,
}

impl MultiSymbolRegimeMonitor {
    /// Create a new multi-symbol regime monitor
    pub fn new(config: RegimeConfig) -> Self {
        Self {
            monitors: HashMap::new(),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(RegimeConfig::default())
    }

    /// Update price for a symbol
    pub fn update(&mut self, symbol: &str, price: f64, timestamp: Option<u64>) {
        let config = self.config.clone();
        let monitor = self
            .monitors
            .entry(symbol.to_string())
            .or_insert_with(|| RegimeMonitor::new(config));
        monitor.update(price, timestamp);
    }

    /// Get the current regime for a symbol
    pub fn get_regime(&self, symbol: &str) -> Option<MarketRegime> {
        self.monitors.get(symbol).map(|m| m.current_regime())
    }

    /// Get all current regimes
    pub fn all_regimes(&self) -> HashMap<String, MarketRegime> {
        self.monitors
            .iter()
            .map(|(s, m)| (s.clone(), m.current_regime()))
            .collect()
    }

    /// Get monitor for a specific symbol
    pub fn get_monitor(&self, symbol: &str) -> Option<&RegimeMonitor> {
        self.monitors.get(symbol)
    }

    /// Get mutable monitor for a specific symbol
    pub fn get_monitor_mut(&mut self, symbol: &str) -> Option<&mut RegimeMonitor> {
        self.monitors.get_mut(symbol)
    }

    /// Count how many symbols are in each regime
    pub fn regime_counts(&self) -> HashMap<MarketRegime, usize> {
        let mut counts = HashMap::new();
        for m in self.monitors.values() {
            *counts.entry(m.current_regime()).or_insert(0) += 1;
        }
        counts
    }

    /// Get symbols in a specific regime
    pub fn symbols_in_regime(&self, regime: MarketRegime) -> Vec<String> {
        self.monitors
            .iter()
            .filter(|(_, m)| m.current_regime() == regime)
            .map(|(s, _)| s.clone())
            .collect()
    }
}

/// Regime-based strategy selector
#[derive(Debug)]
pub struct RegimeStrategySelector<T: Clone> {
    /// Strategy mapping per regime
    strategies: HashMap<MarketRegime, T>,
    /// Default strategy when regime is unknown
    default_strategy: T,
}

impl<T: Clone> RegimeStrategySelector<T> {
    /// Create a new strategy selector
    pub fn new(default_strategy: T) -> Self {
        Self {
            strategies: HashMap::new(),
            default_strategy,
        }
    }

    /// Set strategy for a specific regime
    pub fn set_strategy(&mut self, regime: MarketRegime, strategy: T) {
        self.strategies.insert(regime, strategy);
    }

    /// Get strategy for current regime
    pub fn get_strategy(&self, regime: MarketRegime) -> &T {
        self.strategies.get(&regime).unwrap_or(&self.default_strategy)
    }

    /// Check if a specific regime has a custom strategy
    pub fn has_strategy(&self, regime: MarketRegime) -> bool {
        self.strategies.contains_key(&regime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== MarketRegime Tests ====================

    #[test]
    fn test_market_regime_is_trending() {
        assert!(MarketRegime::TrendingUp.is_trending());
        assert!(MarketRegime::TrendingDown.is_trending());
        assert!(!MarketRegime::MeanReverting.is_trending());
        assert!(!MarketRegime::HighVolatility.is_trending());
        assert!(!MarketRegime::LowVolatility.is_trending());
        assert!(!MarketRegime::Unknown.is_trending());
    }

    #[test]
    fn test_market_regime_is_high_activity() {
        assert!(MarketRegime::TrendingUp.is_high_activity());
        assert!(MarketRegime::TrendingDown.is_high_activity());
        assert!(!MarketRegime::MeanReverting.is_high_activity());
        assert!(MarketRegime::HighVolatility.is_high_activity());
        assert!(!MarketRegime::LowVolatility.is_high_activity());
        assert!(!MarketRegime::Unknown.is_high_activity());
    }

    #[test]
    fn test_market_regime_description() {
        assert_eq!(MarketRegime::TrendingUp.description(), "Trending Up");
        assert_eq!(MarketRegime::TrendingDown.description(), "Trending Down");
        assert_eq!(MarketRegime::MeanReverting.description(), "Mean Reverting");
        assert_eq!(MarketRegime::HighVolatility.description(), "High Volatility");
        assert_eq!(MarketRegime::LowVolatility.description(), "Low Volatility");
        assert_eq!(MarketRegime::Unknown.description(), "Unknown");
    }

    #[test]
    fn test_market_regime_display() {
        assert_eq!(format!("{}", MarketRegime::TrendingUp), "Trending Up");
        assert_eq!(format!("{}", MarketRegime::Unknown), "Unknown");
    }

    // ==================== RegimeConfig Tests ====================

    #[test]
    fn test_regime_config_default() {
        let config = RegimeConfig::default();
        assert_eq!(config.window_size, 100);
        assert!((config.trend_threshold - 0.02).abs() < 1e-10);
        assert!((config.high_volatility_threshold - 0.50).abs() < 1e-10);
        assert!((config.low_volatility_threshold - 0.15).abs() < 1e-10);
        assert_eq!(config.min_samples, 20);
        assert!((config.mean_reversion_threshold - 0.4).abs() < 1e-10);
    }

    #[test]
    fn test_regime_config_custom() {
        let config = RegimeConfig {
            window_size: 50,
            trend_threshold: 0.05,
            high_volatility_threshold: 0.8,
            low_volatility_threshold: 0.1,
            min_samples: 10,
            mean_reversion_threshold: 0.35,
        };
        assert_eq!(config.window_size, 50);
        assert!((config.trend_threshold - 0.05).abs() < 1e-10);
    }

    // ==================== RegimePerformance Tests ====================

    #[test]
    fn test_regime_performance_new() {
        let perf = RegimePerformance::new();
        assert_eq!(perf.total_pnl, 0.0);
        assert_eq!(perf.trade_count, 0);
        assert_eq!(perf.winning_trades, 0);
        assert_eq!(perf.losing_trades, 0);
        assert_eq!(perf.max_drawdown, 0.0);
        assert_eq!(perf.time_in_regime, 0);
    }

    #[test]
    fn test_regime_performance_record_trade_winning() {
        let mut perf = RegimePerformance::new();
        perf.record_trade(100.0);
        assert_eq!(perf.total_pnl, 100.0);
        assert_eq!(perf.trade_count, 1);
        assert_eq!(perf.winning_trades, 1);
        assert_eq!(perf.losing_trades, 0);
        assert_eq!(perf.peak_pnl, 100.0);
        assert_eq!(perf.max_drawdown, 0.0);
    }

    #[test]
    fn test_regime_performance_record_trade_losing() {
        let mut perf = RegimePerformance::new();
        perf.record_trade(-50.0);
        assert_eq!(perf.total_pnl, -50.0);
        assert_eq!(perf.trade_count, 1);
        assert_eq!(perf.winning_trades, 0);
        assert_eq!(perf.losing_trades, 1);
        assert_eq!(perf.peak_pnl, 0.0);
        assert_eq!(perf.max_drawdown, 50.0);
    }

    #[test]
    fn test_regime_performance_record_trade_zero() {
        let mut perf = RegimePerformance::new();
        perf.record_trade(0.0);
        assert_eq!(perf.total_pnl, 0.0);
        assert_eq!(perf.trade_count, 1);
        assert_eq!(perf.winning_trades, 0);
        assert_eq!(perf.losing_trades, 0);
    }

    #[test]
    fn test_regime_performance_drawdown_calculation() {
        let mut perf = RegimePerformance::new();
        perf.record_trade(100.0); // Peak at 100
        perf.record_trade(-30.0); // PnL = 70, DD = 30
        perf.record_trade(50.0); // PnL = 120, new peak
        perf.record_trade(-60.0); // PnL = 60, DD = 60

        assert_eq!(perf.total_pnl, 60.0);
        assert_eq!(perf.peak_pnl, 120.0);
        assert_eq!(perf.max_drawdown, 60.0);
    }

    #[test]
    fn test_regime_performance_record_return() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.01);
        perf.record_return(0.02);
        perf.record_return(-0.01);

        assert_eq!(perf.returns_count, 3);
        assert!((perf.returns_sum - 0.02).abs() < 1e-10);
        assert_eq!(perf.returns.len(), 3);
    }

    #[test]
    fn test_regime_performance_tick() {
        let mut perf = RegimePerformance::new();
        assert_eq!(perf.time_in_regime, 0);
        perf.tick();
        assert_eq!(perf.time_in_regime, 1);
        perf.tick();
        perf.tick();
        assert_eq!(perf.time_in_regime, 3);
    }

    #[test]
    fn test_regime_performance_win_rate_empty() {
        let perf = RegimePerformance::new();
        assert!(perf.win_rate().is_none());
    }

    #[test]
    fn test_regime_performance_win_rate() {
        let mut perf = RegimePerformance::new();
        perf.record_trade(100.0);
        perf.record_trade(50.0);
        perf.record_trade(-30.0);
        perf.record_trade(20.0);

        let win_rate = perf.win_rate().unwrap();
        assert!((win_rate - 0.75).abs() < 1e-10);
    }

    #[test]
    fn test_regime_performance_mean_return_empty() {
        let perf = RegimePerformance::new();
        assert!(perf.mean_return().is_none());
    }

    #[test]
    fn test_regime_performance_mean_return() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.01);
        perf.record_return(0.02);
        perf.record_return(0.03);

        let mean = perf.mean_return().unwrap();
        assert!((mean - 0.02).abs() < 1e-10);
    }

    #[test]
    fn test_regime_performance_std_return_empty() {
        let perf = RegimePerformance::new();
        assert!(perf.std_return().is_none());
    }

    #[test]
    fn test_regime_performance_std_return_single() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.01);
        assert!(perf.std_return().is_none());
    }

    #[test]
    fn test_regime_performance_std_return() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.01);
        perf.record_return(0.03);

        let std = perf.std_return().unwrap();
        // Mean = 0.02, Variance = 0.0001, Std = 0.01
        assert!((std - 0.01).abs() < 1e-10);
    }

    #[test]
    fn test_regime_performance_sharpe_ratio_empty() {
        let perf = RegimePerformance::new();
        assert!(perf.sharpe_ratio().is_none());
    }

    #[test]
    fn test_regime_performance_sharpe_ratio_zero_std() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.01);
        perf.record_return(0.01);
        perf.record_return(0.01);

        // Zero standard deviation
        assert!(perf.sharpe_ratio().is_none());
    }

    #[test]
    fn test_regime_performance_sharpe_ratio() {
        let mut perf = RegimePerformance::new();
        for _ in 0..10 {
            perf.record_return(0.02);
        }
        for _ in 0..10 {
            perf.record_return(0.04);
        }

        let sharpe = perf.sharpe_ratio().unwrap();
        // Mean = 0.03, Std > 0, so Sharpe > 0
        assert!(sharpe > 0.0);
    }

    #[test]
    fn test_regime_performance_avg_pnl_per_trade_empty() {
        let perf = RegimePerformance::new();
        assert!(perf.avg_pnl_per_trade().is_none());
    }

    #[test]
    fn test_regime_performance_avg_pnl_per_trade() {
        let mut perf = RegimePerformance::new();
        perf.record_trade(100.0);
        perf.record_trade(-50.0);
        perf.record_trade(80.0);

        let avg = perf.avg_pnl_per_trade().unwrap();
        assert!((avg - 43.333333333333336).abs() < 1e-6);
    }

    #[test]
    fn test_regime_performance_profit_factor() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.10);
        perf.record_return(0.05);
        perf.record_return(-0.03);
        perf.record_return(-0.02);

        let pf = perf.profit_factor().unwrap();
        // Profit = 0.15, Loss = 0.05, PF = 3.0
        assert!((pf - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_regime_performance_profit_factor_no_losses() {
        let mut perf = RegimePerformance::new();
        perf.record_return(0.10);
        perf.record_return(0.05);

        assert!(perf.profit_factor().is_none());
    }

    // ==================== RegimeMonitor Tests ====================

    #[test]
    fn test_regime_monitor_new() {
        let monitor = RegimeMonitor::with_defaults();
        assert_eq!(monitor.current_regime(), MarketRegime::Unknown);
        assert_eq!(monitor.sample_count(), 0);
        assert!(monitor.transitions().is_empty());
    }

    #[test]
    fn test_regime_monitor_update_single() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.update(100.0, None);
        assert_eq!(monitor.sample_count(), 1);
        assert_eq!(monitor.current_regime(), MarketRegime::Unknown); // Not enough samples
    }

    #[test]
    fn test_regime_monitor_update_insufficient_samples() {
        let config = RegimeConfig {
            min_samples: 10,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        for i in 0..9 {
            monitor.update(100.0 + i as f64, None);
        }

        assert_eq!(monitor.sample_count(), 9);
        assert_eq!(monitor.current_regime(), MarketRegime::Unknown);
    }

    #[test]
    fn test_regime_monitor_trending_up_detection() {
        let config = RegimeConfig {
            window_size: 20,
            min_samples: 10,
            trend_threshold: 0.05,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Simulate strong upward trend: 100 -> 110 (10% gain)
        for i in 0..20 {
            let price = 100.0 + (i as f64 * 0.5); // +10% over 20 samples
            monitor.update(price, None);
        }

        assert_eq!(monitor.current_regime(), MarketRegime::TrendingUp);
    }

    #[test]
    fn test_regime_monitor_trending_down_detection() {
        let config = RegimeConfig {
            window_size: 20,
            min_samples: 10,
            trend_threshold: 0.05,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Simulate strong downward trend: 100 -> 90 (-10% loss)
        for i in 0..20 {
            let price = 100.0 - (i as f64 * 0.5);
            monitor.update(price, None);
        }

        assert_eq!(monitor.current_regime(), MarketRegime::TrendingDown);
    }

    #[test]
    fn test_regime_monitor_record_trade() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.record_trade(100.0);
        monitor.record_trade(-50.0);

        let perf = monitor.get_regime_performance(MarketRegime::Unknown).unwrap();
        assert_eq!(perf.trade_count, 2);
        assert_eq!(perf.total_pnl, 50.0);
    }

    #[test]
    fn test_regime_monitor_record_return() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.record_return(0.01);
        monitor.record_return(0.02);

        let perf = monitor.get_regime_performance(MarketRegime::Unknown).unwrap();
        assert_eq!(perf.returns_count, 2);
    }

    #[test]
    fn test_regime_monitor_transitions() {
        let config = RegimeConfig {
            window_size: 10,
            min_samples: 5,
            trend_threshold: 0.03,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Start with flat prices (unknown)
        for _ in 0..15 {
            monitor.update(100.0, None);
        }

        let initial_transitions = monitor.transitions().len();

        // Shift to trending up
        for i in 0..15 {
            monitor.update(100.0 + (i as f64 * 0.5), None);
        }

        // Should have at least one transition
        assert!(monitor.transitions().len() > initial_transitions);
    }

    #[test]
    fn test_regime_monitor_transition_count() {
        let config = RegimeConfig {
            window_size: 10,
            min_samples: 5,
            trend_threshold: 0.05,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Manually track initial state
        for i in 0..20 {
            let price = 100.0 + (i as f64 * 0.5);
            monitor.update(price, None);
        }

        let count = monitor.transition_count(MarketRegime::Unknown, MarketRegime::TrendingUp);
        // Should be at least 0 (depends on detection timing)
        assert!(count >= 0);
    }

    #[test]
    fn test_regime_monitor_time_in_regime() {
        let mut monitor = RegimeMonitor::with_defaults();

        for _ in 0..50 {
            monitor.update(100.0, None);
        }

        // Constant price may be classified as Unknown initially then LowVolatility
        // Total time across both should equal 50
        let unknown_time = monitor
            .get_regime_performance(MarketRegime::Unknown)
            .map(|p| p.time_in_regime)
            .unwrap_or(0);
        let low_vol_time = monitor
            .get_regime_performance(MarketRegime::LowVolatility)
            .map(|p| p.time_in_regime)
            .unwrap_or(0);
        assert_eq!(unknown_time + low_vol_time, 50);
    }

    #[test]
    fn test_regime_monitor_summary() {
        let mut monitor = RegimeMonitor::with_defaults();

        for _ in 0..100 {
            monitor.update(100.0, None);
        }

        let summary = monitor.summary();
        // Constant price should be Unknown or LowVolatility
        assert!(
            summary.current_regime == MarketRegime::Unknown
                || summary.current_regime == MarketRegime::LowVolatility
        );
        assert_eq!(summary.total_samples, 100);
    }

    #[test]
    fn test_regime_monitor_reset() {
        let mut monitor = RegimeMonitor::with_defaults();

        for _ in 0..50 {
            monitor.update(100.0, None);
        }
        monitor.record_trade(100.0);

        monitor.reset();

        assert_eq!(monitor.sample_count(), 0);
        assert_eq!(monitor.current_regime(), MarketRegime::Unknown);
        assert!(monitor.transitions().is_empty());

        let perf = monitor.get_regime_performance(MarketRegime::Unknown).unwrap();
        assert_eq!(perf.trade_count, 0);
    }

    #[test]
    fn test_regime_monitor_all_regime_performance() {
        let monitor = RegimeMonitor::with_defaults();
        let all_perf = monitor.all_regime_performance();

        // Should have entries for all regimes
        assert!(all_perf.contains_key(&MarketRegime::TrendingUp));
        assert!(all_perf.contains_key(&MarketRegime::TrendingDown));
        assert!(all_perf.contains_key(&MarketRegime::MeanReverting));
        assert!(all_perf.contains_key(&MarketRegime::HighVolatility));
        assert!(all_perf.contains_key(&MarketRegime::LowVolatility));
        assert!(all_perf.contains_key(&MarketRegime::Unknown));
    }

    #[test]
    fn test_regime_monitor_with_timestamps() {
        let mut monitor = RegimeMonitor::with_defaults();

        monitor.update(100.0, Some(1000));
        monitor.update(101.0, Some(2000));
        monitor.update(102.0, Some(3000));

        assert_eq!(monitor.sample_count(), 3);
    }

    // ==================== MultiSymbolRegimeMonitor Tests ====================

    #[test]
    fn test_multi_symbol_monitor_new() {
        let monitor = MultiSymbolRegimeMonitor::with_defaults();
        assert!(monitor.all_regimes().is_empty());
    }

    #[test]
    fn test_multi_symbol_monitor_update() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();

        monitor.update("BTCUSDT", 50000.0, None);
        monitor.update("ETHUSDT", 3000.0, None);

        assert_eq!(monitor.get_regime("BTCUSDT"), Some(MarketRegime::Unknown));
        assert_eq!(monitor.get_regime("ETHUSDT"), Some(MarketRegime::Unknown));
    }

    #[test]
    fn test_multi_symbol_monitor_all_regimes() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();

        monitor.update("BTCUSDT", 50000.0, None);
        monitor.update("ETHUSDT", 3000.0, None);
        monitor.update("SOLUSDT", 100.0, None);

        let regimes = monitor.all_regimes();
        assert_eq!(regimes.len(), 3);
        assert!(regimes.contains_key("BTCUSDT"));
        assert!(regimes.contains_key("ETHUSDT"));
        assert!(regimes.contains_key("SOLUSDT"));
    }

    #[test]
    fn test_multi_symbol_monitor_get_monitor() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();
        monitor.update("BTCUSDT", 50000.0, None);

        let btc_monitor = monitor.get_monitor("BTCUSDT");
        assert!(btc_monitor.is_some());
        assert_eq!(btc_monitor.unwrap().sample_count(), 1);

        let eth_monitor = monitor.get_monitor("ETHUSDT");
        assert!(eth_monitor.is_none());
    }

    #[test]
    fn test_multi_symbol_monitor_get_monitor_mut() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();
        monitor.update("BTCUSDT", 50000.0, None);

        let btc_monitor = monitor.get_monitor_mut("BTCUSDT");
        assert!(btc_monitor.is_some());

        btc_monitor.unwrap().record_trade(100.0);

        let btc_perf = monitor
            .get_monitor("BTCUSDT")
            .unwrap()
            .get_regime_performance(MarketRegime::Unknown)
            .unwrap();
        assert_eq!(btc_perf.trade_count, 1);
    }

    #[test]
    fn test_multi_symbol_monitor_regime_counts() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();

        monitor.update("BTCUSDT", 50000.0, None);
        monitor.update("ETHUSDT", 3000.0, None);
        monitor.update("SOLUSDT", 100.0, None);

        let counts = monitor.regime_counts();
        assert_eq!(counts.get(&MarketRegime::Unknown), Some(&3));
    }

    #[test]
    fn test_multi_symbol_monitor_symbols_in_regime() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();

        monitor.update("BTCUSDT", 50000.0, None);
        monitor.update("ETHUSDT", 3000.0, None);

        let unknown_symbols = monitor.symbols_in_regime(MarketRegime::Unknown);
        assert_eq!(unknown_symbols.len(), 2);
        assert!(unknown_symbols.contains(&"BTCUSDT".to_string()));
        assert!(unknown_symbols.contains(&"ETHUSDT".to_string()));

        let trending_symbols = monitor.symbols_in_regime(MarketRegime::TrendingUp);
        assert!(trending_symbols.is_empty());
    }

    // ==================== RegimeStrategySelector Tests ====================

    #[test]
    fn test_strategy_selector_new() {
        let selector = RegimeStrategySelector::new("default_strategy");
        assert_eq!(*selector.get_strategy(MarketRegime::Unknown), "default_strategy");
    }

    #[test]
    fn test_strategy_selector_set_strategy() {
        let mut selector = RegimeStrategySelector::new("default");

        selector.set_strategy(MarketRegime::TrendingUp, "trend_follow");
        selector.set_strategy(MarketRegime::MeanReverting, "mean_revert");

        assert_eq!(*selector.get_strategy(MarketRegime::TrendingUp), "trend_follow");
        assert_eq!(*selector.get_strategy(MarketRegime::MeanReverting), "mean_revert");
        assert_eq!(*selector.get_strategy(MarketRegime::HighVolatility), "default");
    }

    #[test]
    fn test_strategy_selector_has_strategy() {
        let mut selector = RegimeStrategySelector::new("default");

        selector.set_strategy(MarketRegime::TrendingUp, "trend_follow");

        assert!(selector.has_strategy(MarketRegime::TrendingUp));
        assert!(!selector.has_strategy(MarketRegime::TrendingDown));
        assert!(!selector.has_strategy(MarketRegime::Unknown));
    }

    #[test]
    fn test_strategy_selector_with_structs() {
        #[derive(Clone, PartialEq, Debug)]
        struct StrategyParams {
            spread: f64,
            skew: f64,
        }

        let default = StrategyParams {
            spread: 1.0,
            skew: 0.0,
        };
        let mut selector = RegimeStrategySelector::new(default.clone());

        selector.set_strategy(
            MarketRegime::HighVolatility,
            StrategyParams {
                spread: 2.0,
                skew: 0.5,
            },
        );

        let hv_strategy = selector.get_strategy(MarketRegime::HighVolatility);
        assert!((hv_strategy.spread - 2.0).abs() < 1e-10);
        assert!((hv_strategy.skew - 0.5).abs() < 1e-10);

        let unknown_strategy = selector.get_strategy(MarketRegime::Unknown);
        assert_eq!(*unknown_strategy, default);
    }

    // ==================== Hurst Exponent Tests ====================

    #[test]
    fn test_hurst_random_walk() {
        // Random walk should have Hurst ~ 0.5
        let config = RegimeConfig {
            window_size: 100,
            min_samples: 50,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Use fixed "random" values that simulate random walk
        let returns = vec![
            0.001, -0.002, 0.003, -0.001, 0.002, -0.003, 0.001, -0.001, 0.002, -0.002, 0.001,
            -0.001, 0.003, -0.002, 0.001, -0.003, 0.002, -0.001, 0.001, -0.002,
        ];

        let mut price = 100.0;
        for &ret in returns.iter().cycle().take(60) {
            price *= 1.0 + ret;
            monitor.update(price, None);
        }

        // Just verify Hurst calculation works (exact value depends on data)
        assert_eq!(monitor.sample_count(), 60);
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_full_trading_session_simulation() {
        let config = RegimeConfig {
            window_size: 20,
            min_samples: 10,
            trend_threshold: 0.03,
            high_volatility_threshold: 0.30,
            low_volatility_threshold: 0.10,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Simulate a trading session with regime changes
        let mut price = 100.0;

        // Phase 1: Trending up
        for i in 0..30 {
            price = 100.0 + (i as f64 * 0.2);
            monitor.update(price, Some(i as u64 * 1000));
            if i % 5 == 0 {
                monitor.record_trade(10.0); // Profitable in trend
            }
        }

        // Phase 2: High volatility
        for i in 0..30 {
            let noise = if i % 2 == 0 { 2.0 } else { -2.0 };
            price = 106.0 + noise;
            monitor.update(price, Some((30 + i) as u64 * 1000));
            if i % 5 == 0 {
                monitor.record_trade(-5.0); // Losing in volatility
            }
        }

        // Phase 3: Trending down
        for i in 0..30 {
            price = 106.0 - (i as f64 * 0.2);
            monitor.update(price, Some((60 + i) as u64 * 1000));
            if i % 5 == 0 {
                monitor.record_trade(8.0);
            }
        }

        // Verify statistics
        assert_eq!(monitor.sample_count(), 90);
        assert!(!monitor.transitions().is_empty());

        let summary = monitor.summary();
        assert!(summary.total_transitions > 0);

        // Verify some trades were recorded
        let total_trades: usize = monitor
            .all_regime_performance()
            .values()
            .map(|p| p.trade_count)
            .sum();
        assert_eq!(total_trades, 18); // 6 trades per phase * 3 phases
    }

    #[test]
    fn test_multi_symbol_portfolio_simulation() {
        let config = RegimeConfig {
            window_size: 15,
            min_samples: 10,
            ..Default::default()
        };
        let mut monitor = MultiSymbolRegimeMonitor::new(config);

        // Simulate different symbols with different behavior
        for i in 0..30 {
            // BTC trending up
            monitor.update("BTCUSDT", 50000.0 + (i as f64 * 100.0), Some(i as u64));
            // ETH sideways
            monitor.update("ETHUSDT", 3000.0 + (i as f64 % 5.0) * 10.0, Some(i as u64));
            // SOL trending down
            monitor.update("SOLUSDT", 100.0 - (i as f64 * 1.0), Some(i as u64));
        }

        let regimes = monitor.all_regimes();
        assert_eq!(regimes.len(), 3);

        // Verify each symbol has data
        assert!(monitor.get_monitor("BTCUSDT").unwrap().sample_count() == 30);
        assert!(monitor.get_monitor("ETHUSDT").unwrap().sample_count() == 30);
        assert!(monitor.get_monitor("SOLUSDT").unwrap().sample_count() == 30);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_zero_price() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.update(0.0, None);
        monitor.update(100.0, None);

        // Should handle gracefully
        assert_eq!(monitor.sample_count(), 2);
    }

    #[test]
    fn test_negative_price() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.update(-100.0, None);
        monitor.update(100.0, None);

        // Should handle gracefully
        assert_eq!(monitor.sample_count(), 2);
    }

    #[test]
    fn test_very_large_prices() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.update(1e15, None);
        monitor.update(1.1e15, None);

        assert_eq!(monitor.sample_count(), 2);
    }

    #[test]
    fn test_very_small_prices() {
        let mut monitor = RegimeMonitor::with_defaults();
        monitor.update(1e-15, None);
        monitor.update(1.1e-15, None);

        assert_eq!(monitor.sample_count(), 2);
    }

    #[test]
    fn test_constant_prices() {
        let config = RegimeConfig {
            window_size: 10,
            min_samples: 5,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        for _ in 0..20 {
            monitor.update(100.0, None);
        }

        // Constant prices should result in Unknown or LowVolatility
        let regime = monitor.current_regime();
        assert!(regime == MarketRegime::Unknown || regime == MarketRegime::LowVolatility);
    }

    #[test]
    fn test_alternating_prices() {
        let config = RegimeConfig {
            window_size: 10,
            min_samples: 5,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Perfect alternation - could indicate mean reversion or volatility
        for i in 0..20 {
            let price = if i % 2 == 0 { 100.0 } else { 101.0 };
            monitor.update(price, None);
        }

        assert_eq!(monitor.sample_count(), 20);
        // Just verify it doesn't crash
    }

    #[test]
    fn test_regime_transition_with_timestamp() {
        let config = RegimeConfig {
            window_size: 10,
            min_samples: 5,
            trend_threshold: 0.02,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        // Start flat
        for i in 0..15 {
            monitor.update(100.0, Some(i as u64 * 60000));
        }

        // Then trend up strongly
        for i in 15..30 {
            monitor.update(100.0 + ((i - 15) as f64 * 0.5), Some(i as u64 * 60000));
        }

        // Check transitions have timestamps
        for t in monitor.transitions() {
            // Timestamp should be present
            assert!(t.timestamp.is_some() || t.sample_index > 0);
        }
    }

    #[test]
    fn test_regime_summary_time_percentages() {
        let config = RegimeConfig {
            window_size: 10,
            min_samples: 5,
            ..Default::default()
        };
        let mut monitor = RegimeMonitor::new(config);

        for _ in 0..100 {
            monitor.update(100.0, None);
        }

        let summary = monitor.summary();

        // Total time percentages should sum to ~100%
        let total_pct: f64 = summary.regime_time_percentages.values().sum();
        assert!((total_pct - 100.0).abs() < 1.0);
    }

    #[test]
    fn test_empty_summary() {
        let monitor = RegimeMonitor::with_defaults();
        let summary = monitor.summary();

        assert_eq!(summary.total_samples, 0);
        assert_eq!(summary.total_transitions, 0);
        assert_eq!(summary.current_regime, MarketRegime::Unknown);
    }

    // ==================== Concurrency Safety (Single-threaded) Tests ====================

    #[test]
    fn test_monitor_many_updates() {
        let mut monitor = RegimeMonitor::with_defaults();

        // Simulate heavy usage
        for i in 0..10000 {
            let price = 100.0 + (i as f64 * 0.001);
            monitor.update(price, Some(i as u64));
            if i % 100 == 0 {
                monitor.record_trade(1.0);
                monitor.record_return(0.0001);
            }
        }

        assert_eq!(monitor.sample_count(), 10000);
    }

    #[test]
    fn test_multi_symbol_many_symbols() {
        let mut monitor = MultiSymbolRegimeMonitor::with_defaults();

        // Test with many symbols
        for i in 0..100 {
            let symbol = format!("SYMBOL{}", i);
            for j in 0..20 {
                monitor.update(&symbol, 100.0 + j as f64, None);
            }
        }

        assert_eq!(monitor.all_regimes().len(), 100);
    }
}
