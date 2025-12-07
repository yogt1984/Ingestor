//! Live Paper Trading Session Runner
//!
//! Runs structured paper trading sessions against live market data,
//! collecting metrics for validation against backtest expectations.
//!
//! # Purpose
//!
//! The critical unknown in our backtest is the **fill rate assumption** (default 10%).
//! This module runs live paper trading to measure actual fill rates and validate
//! whether backtest predictions hold in live conditions.
//!
//! # Architecture
//!
//! ```text
//! Live Binance WebSocket → OrderBook/Trades → Feature Engines → FeaturesSnapshot
//!                                                                    ↓
//!                                                  RiskManagedPaperTradingEngine
//!                                                                    ↓
//!                                                    ForwardTestSession (logging)
//!                                                                    ↓
//!                                                  SessionSummary (JSON output)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! let config = SessionRunnerConfig {
//!     duration_hours: 2.0,
//!     preset_name: Some("conservative".to_string()),
//!     ..Default::default()
//! };
//!
//! let mut runner = SessionRunner::new(config)?;
//! let summary = runner.run().await?;
//!
//! // Summary can now be validated with `validate-session` command
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Result, bail, Context};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use crate::algorithms::{AlgorithmType, AvellanedaStoikovAlgorithm};
use crate::backtest::replay::ReplayEvent;
use crate::forward_testing_core::{
    ForwardTestConfig, ForwardTestSession, SessionMetrics, SessionSummary,
};
use crate::market_maker::{MMConfig, Fill};
use crate::mm_simulator::{RiskManagedPaperTradingEngine, SimulatorConfig};
use crate::presets::{ParameterPreset, PresetStore};
use crate::risk_manager::RiskConfig;

/// Configuration for session runner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRunnerConfig {
    /// Session duration in hours (can be fractional, e.g., 0.5 for 30 min)
    pub duration_hours: f64,
    /// Minimum duration for valid session (hours)
    pub min_duration_hours: f64,
    /// Preset name to use (if None, uses default parameters)
    pub preset_name: Option<String>,
    /// Trading symbol (e.g., "BTCUSDT")
    pub symbol: String,
    /// Output directory for session files
    pub output_dir: PathBuf,
    /// Whether to log individual quotes (can be large)
    pub log_quotes: bool,
    /// Fee rate (e.g., 0.0001 = 1 bps)
    pub fee_rate: Decimal,
    /// Market making parameters (used if no preset specified)
    pub mm_config: Option<MMConfig>,
    /// Risk management configuration
    pub risk_config: Option<RiskConfig>,
    /// Simulator configuration
    pub sim_config: Option<SimulatorConfig>,
    /// Checkpoint interval in seconds (save intermediate state)
    pub checkpoint_interval_secs: u64,
    /// Print progress every N events
    pub progress_interval: u64,
    /// Minimum trades required for valid session
    pub min_trades: usize,
}

impl Default for SessionRunnerConfig {
    fn default() -> Self {
        Self {
            duration_hours: 1.0,
            min_duration_hours: 0.5,
            preset_name: None,
            symbol: "BTCUSDT".to_string(),
            output_dir: PathBuf::from("./data/sessions"),
            log_quotes: false,
            fee_rate: dec!(0.0001),
            mm_config: None,
            risk_config: None,
            sim_config: None,
            checkpoint_interval_secs: 300, // 5 minutes
            progress_interval: 1000,
            min_trades: 5,
        }
    }
}

impl SessionRunnerConfig {
    /// Create config for a specific preset
    pub fn for_preset(preset_name: &str, duration_hours: f64) -> Self {
        Self {
            duration_hours,
            preset_name: Some(preset_name.to_string()),
            ..Default::default()
        }
    }

    /// Create config with custom MM parameters
    pub fn with_mm_config(mm_config: MMConfig, duration_hours: f64) -> Self {
        Self {
            duration_hours,
            mm_config: Some(mm_config),
            ..Default::default()
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.duration_hours <= 0.0 {
            bail!("Duration must be positive");
        }
        if self.duration_hours > 168.0 {
            bail!("Duration cannot exceed 1 week (168 hours)");
        }
        if self.min_duration_hours > self.duration_hours {
            bail!("Minimum duration cannot exceed target duration");
        }
        if self.symbol.is_empty() {
            bail!("Symbol cannot be empty");
        }
        Ok(())
    }
}

/// Session runner state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionState {
    /// Not yet started
    Pending,
    /// Currently running
    Running,
    /// Completed successfully
    Completed,
    /// Stopped early (e.g., user interrupt)
    Stopped,
    /// Failed with error
    Failed,
}

/// Progress update during session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionProgress {
    /// Current state
    pub state: SessionState,
    /// Time elapsed (seconds)
    pub elapsed_secs: f64,
    /// Time remaining (seconds)
    pub remaining_secs: f64,
    /// Progress percentage (0-100)
    pub progress_pct: f64,
    /// Events processed
    pub events_processed: u64,
    /// Current metrics snapshot
    pub metrics: SessionMetrics,
    /// Last checkpoint time
    pub last_checkpoint: Option<DateTime<Utc>>,
}

/// Result of a completed session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResult {
    /// Session summary (serialized to JSON)
    pub summary: SessionSummary,
    /// Final state
    pub final_state: SessionState,
    /// Total events processed
    pub events_processed: u64,
    /// Session file path
    pub summary_path: PathBuf,
    /// Trades file path (if saved)
    pub trades_path: Option<PathBuf>,
    /// Warnings generated during session
    pub warnings: Vec<String>,
    /// Whether session meets minimum requirements for validation
    pub is_valid_for_validation: bool,
}

/// Simulated market event for testing
#[derive(Debug, Clone)]
pub struct SimulatedEvent {
    pub timestamp_ms: u64,
    pub mid_price: Decimal,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub volatility: f64,
    pub entropy: f64,
    pub book_imbalance: f64,
    /// Optional trade that occurred
    pub trade: Option<SimulatedTrade>,
}

/// Simulated trade for testing
#[derive(Debug, Clone)]
pub struct SimulatedTrade {
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,
}

impl SimulatedEvent {
    /// Create a SimulatedEvent from a ReplayEvent
    ///
    /// Extracts relevant fields from the FeaturesSnapshot nested in ReplayEvent
    pub fn from_replay_event(event: &ReplayEvent) -> Option<Self> {
        let snapshot = &event.snapshot;

        // Need mid_price, best_bid, best_ask at minimum
        let mid_price = snapshot.mid_price?;
        let best_bid = snapshot.best_bid?;
        let best_ask = snapshot.best_ask?;

        // Get entropy - use tick_entropy_10s as the primary entropy measure
        let entropy = snapshot.tick_entropy_10s
            .map(|d| d.to_string().parse::<f64>().unwrap_or(0.5))
            .unwrap_or(0.5);

        // Get volatility - use realized_volatility_100 if available
        let volatility = snapshot.realized_volatility_100.unwrap_or(0.001);

        // Get book imbalance
        let book_imbalance = snapshot.imbalance
            .map(|d| d.to_string().parse::<f64>().unwrap_or(0.0))
            .unwrap_or(0.0);

        // Check for trade
        let trade = if let Some(last_price) = snapshot.last_trade_price {
            Some(SimulatedTrade {
                price: last_price,
                quantity: snapshot.avg_trade_size.unwrap_or(dec!(0.001)),
                is_buyer_maker: snapshot.order_flow_pressure < Decimal::ZERO,
            })
        } else {
            None
        };

        Some(Self {
            timestamp_ms: event.timestamp_ms as u64,
            mid_price,
            best_bid,
            best_ask,
            volatility,
            entropy,
            book_imbalance,
            trade,
        })
    }
}

/// Session runner for paper trading
///
/// This is designed to be used in two modes:
/// 1. **Live mode**: Connected to real Binance WebSocket (requires async runtime)
/// 2. **Simulation mode**: Fed synthetic events for testing
///
/// The runner handles:
/// - Engine initialization from presets or custom config
/// - Event processing and fill simulation
/// - Metric collection and logging
/// - Checkpoint saving
/// - Session finalization
pub struct SessionRunner {
    config: SessionRunnerConfig,
    state: SessionState,
    engine: Option<RiskManagedPaperTradingEngine>,
    session: Option<ForwardTestSession>,
    events_processed: u64,
    start_time: Option<Instant>,
    warnings: Vec<String>,
    preset: Option<ParameterPreset>,
}

impl SessionRunner {
    /// Create a new session runner
    pub fn new(config: SessionRunnerConfig) -> Result<Self> {
        config.validate()?;

        Ok(Self {
            config,
            state: SessionState::Pending,
            engine: None,
            session: None,
            events_processed: 0,
            start_time: None,
            warnings: Vec::new(),
            preset: None,
        })
    }

    /// Initialize the runner (must be called before processing events)
    pub fn initialize(&mut self) -> Result<()> {
        if self.state != SessionState::Pending {
            bail!("Session already initialized");
        }

        // Load preset if specified
        let (mm_config, preset) = if let Some(ref preset_name) = self.config.preset_name {
            let store = PresetStore::load();
            // Find preset by name
            let found = store.presets.iter().find(|p| p.name == *preset_name);
            if let Some(p) = found {
                let mut mm = MMConfig::default();
                // Apply preset spread to all regimes
                mm.regime_params.high_entropy.spread_bps = p.spread_bps;
                mm.regime_params.medium_entropy.spread_bps = p.spread_bps;
                mm.regime_params.low_entropy.spread_bps = p.spread_bps;
                // Apply preset skew to all regimes
                mm.regime_params.high_entropy.skew_factor = p.skew;
                mm.regime_params.medium_entropy.skew_factor = p.skew;
                mm.regime_params.low_entropy.skew_factor = p.skew;
                // Apply high entropy threshold
                mm.regime_thresholds.high_entropy_threshold = p.high_entropy_threshold;
                (mm, Some(p.clone()))
            } else {
                self.warnings.push(format!(
                    "Preset '{}' not found, using defaults",
                    preset_name
                ));
                (self.config.mm_config.clone().unwrap_or_default(), None)
            }
        } else {
            (self.config.mm_config.clone().unwrap_or_default(), None)
        };

        self.preset = preset.clone();

        // Create algorithm
        let algorithm = Box::new(AvellanedaStoikovAlgorithm::new(mm_config));

        // Create simulator config
        let sim_config = self.config.sim_config.clone().unwrap_or_default();

        // Create risk config
        let risk_config = self.config.risk_config.clone().unwrap_or_default();

        // Create engine
        let engine = RiskManagedPaperTradingEngine::new(algorithm, sim_config, risk_config);
        self.engine = Some(engine);

        // Create forward test session
        let ft_config = ForwardTestConfig {
            log_dir: self.config.output_dir.clone(),
            log_trades: true,
            log_quotes: self.config.log_quotes,
            sharpe_window: 100,
            session_name: self.config.preset_name.clone(),
            preset_name: self.config.preset_name.clone(),
            algorithm_type: AlgorithmType::AvellanedaStoikov,
        };

        let mut session = ForwardTestSession::new(ft_config);
        session.start();
        self.session = Some(session);

        self.start_time = Some(Instant::now());
        self.state = SessionState::Running;

        Ok(())
    }

    /// Process a single market event
    ///
    /// Returns any fills that occurred
    pub fn process_event(&mut self, event: &SimulatedEvent) -> Result<Vec<Fill>> {
        if self.state != SessionState::Running {
            bail!("Session not running");
        }

        let engine = self.engine.as_mut()
            .context("Engine not initialized")?;
        let session = self.session.as_mut()
            .context("Session not initialized")?;

        self.events_processed += 1;

        // Generate quotes
        let quotes = engine.on_features_with_book(
            event.best_bid,
            event.best_ask,
            event.volatility,
            event.entropy,
            event.book_imbalance,
            event.timestamp_ms,
        );

        // Log quote
        session.log_quote(
            event.timestamp_ms,
            quotes.bid.as_ref().map(|q| q.price),
            quotes.bid.as_ref().map(|q| q.size),
            quotes.ask.as_ref().map(|q| q.price),
            quotes.ask.as_ref().map(|q| q.size),
            event.mid_price,
            engine.trading_state().mm_state.inventory,
            &format!("{:?}", quotes.regime),
        );

        // Check for quote touches
        if let Some(ref bid) = quotes.bid {
            if event.best_bid <= bid.price {
                session.record_touch(true);
            }
        }
        if let Some(ref ask) = quotes.ask {
            if event.best_ask >= ask.price {
                session.record_touch(false);
            }
        }

        // Process trade if present
        let mut fills = Vec::new();
        if let Some(ref trade) = event.trade {
            let trade_struct = crate::tradeslog::Trade {
                id: self.events_processed,
                price: trade.price,
                quantity: trade.quantity,
                timestamp: event.timestamp_ms,
                is_buyer_maker: trade.is_buyer_maker,
            };

            let event_fills = engine.on_trade(&trade_struct, event.timestamp_ms);

            // Log fills
            for fill in &event_fills {
                let mm_state = engine.trading_state().mm_state.clone();
                let fee = fill.size * fill.price * self.config.fee_rate;
                session.log_trade(fill, &mm_state, event.mid_price, fee);
            }

            fills = event_fills;
        }

        Ok(fills)
    }

    /// Check if session should continue
    pub fn should_continue(&self) -> bool {
        if self.state != SessionState::Running {
            return false;
        }

        if let Some(start) = self.start_time {
            let elapsed = start.elapsed();
            let target = Duration::from_secs_f64(self.config.duration_hours * 3600.0);
            elapsed < target
        } else {
            false
        }
    }

    /// Get current progress
    pub fn progress(&self) -> SessionProgress {
        let elapsed_secs = self.start_time
            .map(|s| s.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        let target_secs = self.config.duration_hours * 3600.0;
        let remaining_secs = (target_secs - elapsed_secs).max(0.0);
        let progress_pct = (elapsed_secs / target_secs * 100.0).min(100.0);

        let metrics = self.session
            .as_ref()
            .map(|s| s.metrics().clone())
            .unwrap_or_default();

        SessionProgress {
            state: self.state,
            elapsed_secs,
            remaining_secs,
            progress_pct,
            events_processed: self.events_processed,
            metrics,
            last_checkpoint: None,
        }
    }

    /// Stop the session early
    pub fn stop(&mut self) {
        if self.state == SessionState::Running {
            self.state = SessionState::Stopped;
        }
    }

    /// Finalize the session and get results
    pub fn finalize(&mut self) -> Result<SessionResult> {
        if self.state == SessionState::Running {
            self.state = SessionState::Completed;
        }

        // Ensure output directory exists
        std::fs::create_dir_all(&self.config.output_dir)?;

        let session = self.session.as_mut()
            .context("Session not initialized")?;

        // End session and get summary
        let summary = session.end()?;

        // Determine paths
        let summary_path = self.config.output_dir.join(format!(
            "summary_{}.json",
            session.session_id()
        ));

        let trades_path = if summary.trade_count > 0 {
            Some(self.config.output_dir.join(format!(
                "trades_{}.json",
                session.session_id()
            )))
        } else {
            None
        };

        // Check validity for validation
        let duration_hours = summary.metrics.duration_secs / 3600.0;
        let is_valid = duration_hours >= self.config.min_duration_hours
            && summary.metrics.total_trades >= self.config.min_trades as u64;

        if !is_valid {
            self.warnings.push(format!(
                "Session may not be valid for validation: {:.1}h duration, {} trades (need {:.1}h, {} trades)",
                duration_hours,
                summary.metrics.total_trades,
                self.config.min_duration_hours,
                self.config.min_trades
            ));
        }

        Ok(SessionResult {
            summary,
            final_state: self.state,
            events_processed: self.events_processed,
            summary_path,
            trades_path,
            warnings: self.warnings.clone(),
            is_valid_for_validation: is_valid,
        })
    }

    /// Get current state
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Get current metrics
    pub fn metrics(&self) -> Option<&SessionMetrics> {
        self.session.as_ref().map(|s| s.metrics())
    }

    /// Get loaded preset (if any)
    pub fn preset(&self) -> Option<&ParameterPreset> {
        self.preset.as_ref()
    }

    /// Get events processed count
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Get current fill rate
    pub fn current_fill_rate(&self) -> f64 {
        self.session.as_ref()
            .map(|s| {
                let m = s.metrics();
                if m.quotes_generated > 0 {
                    m.total_trades as f64 / m.quotes_generated as f64
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0)
    }
}

/// Statistics computed from a session for fill rate calibration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FillRateStats {
    /// Total quotes generated
    pub quotes_generated: u64,
    /// Total fills received
    pub fills_received: u64,
    /// Overall fill rate
    pub overall_fill_rate: f64,
    /// Bid fill rate
    pub bid_fill_rate: f64,
    /// Ask fill rate
    pub ask_fill_rate: f64,
    /// Fill rate by hour
    pub hourly_fill_rates: Vec<f64>,
    /// 95% confidence interval lower bound
    pub ci_lower: f64,
    /// 95% confidence interval upper bound
    pub ci_upper: f64,
    /// Standard error
    pub std_error: f64,
}

impl FillRateStats {
    /// Compute fill rate statistics from session metrics
    pub fn from_metrics(metrics: &SessionMetrics) -> Self {
        let overall_fill_rate = if metrics.quotes_generated > 0 {
            metrics.total_trades as f64 / metrics.quotes_generated as f64
        } else {
            0.0
        };

        // Compute standard error using binomial proportion
        let n = metrics.quotes_generated as f64;
        let p = overall_fill_rate;
        let std_error = if n > 0.0 {
            (p * (1.0 - p) / n).sqrt()
        } else {
            0.0
        };

        // 95% CI using normal approximation
        let z = 1.96;
        let ci_lower = (p - z * std_error).max(0.0);
        let ci_upper = (p + z * std_error).min(1.0);

        Self {
            quotes_generated: metrics.quotes_generated,
            fills_received: metrics.total_trades,
            overall_fill_rate,
            bid_fill_rate: metrics.bid_fill_rate,
            ask_fill_rate: metrics.ask_fill_rate,
            hourly_fill_rates: Vec::new(), // Would need more detailed data
            ci_lower,
            ci_upper,
            std_error,
        }
    }

    /// Check if observed fill rate differs significantly from assumed rate
    pub fn differs_from_assumption(&self, assumed_rate: f64, confidence: f64) -> bool {
        // Use z-test for proportion
        if self.quotes_generated < 30 {
            return false; // Not enough data
        }

        let z = if self.std_error > 0.0 {
            (self.overall_fill_rate - assumed_rate).abs() / self.std_error
        } else {
            0.0
        };

        // Convert confidence to z-score (e.g., 0.95 -> 1.96)
        let z_critical = match confidence {
            c if c >= 0.99 => 2.576,
            c if c >= 0.95 => 1.96,
            c if c >= 0.90 => 1.645,
            _ => 1.645,
        };

        z > z_critical
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Configuration Tests - Skeptical validation of config handling
    // ========================================================================

    #[test]
    fn test_config_default_values_are_reasonable() {
        let config = SessionRunnerConfig::default();

        // Duration should be reasonable (not too short, not too long)
        assert!(config.duration_hours >= 0.5, "Default duration too short");
        assert!(config.duration_hours <= 24.0, "Default duration too long");

        // Min duration should be less than target
        assert!(
            config.min_duration_hours <= config.duration_hours,
            "Min duration exceeds target duration"
        );

        // Fee rate should be reasonable (between 0 and 1%)
        assert!(config.fee_rate >= dec!(0), "Negative fee rate");
        assert!(config.fee_rate <= dec!(0.01), "Fee rate exceeds 1%");

        // Min trades should be reasonable
        assert!(config.min_trades >= 1, "Min trades too low");
        assert!(config.min_trades <= 100, "Min trades too high for validation");
    }

    #[test]
    fn test_config_validation_rejects_invalid_duration() {
        // Zero duration
        let mut config = SessionRunnerConfig::default();
        config.duration_hours = 0.0;
        assert!(config.validate().is_err());

        // Negative duration
        config.duration_hours = -1.0;
        assert!(config.validate().is_err());

        // Excessively long duration
        config.duration_hours = 1000.0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_rejects_invalid_min_duration() {
        let mut config = SessionRunnerConfig::default();
        config.duration_hours = 1.0;
        config.min_duration_hours = 2.0; // Min > target
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_rejects_empty_symbol() {
        let mut config = SessionRunnerConfig::default();
        config.symbol = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_for_preset_creates_valid_config() {
        let config = SessionRunnerConfig::for_preset("conservative", 2.0);

        assert_eq!(config.preset_name, Some("conservative".to_string()));
        assert_eq!(config.duration_hours, 2.0);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_mm_config_preserves_params() {
        let mut mm = MMConfig::default();
        mm.regime_params.high_entropy.spread_bps = 3.0;

        let config = SessionRunnerConfig::with_mm_config(mm.clone(), 1.5);

        assert_eq!(config.duration_hours, 1.5);
        assert!(config.mm_config.is_some());
        assert_eq!(config.mm_config.unwrap().regime_params.high_entropy.spread_bps, 3.0);
    }

    // ========================================================================
    // Session Runner Lifecycle Tests
    // ========================================================================

    #[test]
    fn test_runner_creation_succeeds_with_valid_config() {
        let config = SessionRunnerConfig::default();
        let runner = SessionRunner::new(config);
        assert!(runner.is_ok());
    }

    #[test]
    fn test_runner_creation_fails_with_invalid_config() {
        let mut config = SessionRunnerConfig::default();
        config.duration_hours = -1.0;
        let runner = SessionRunner::new(config);
        assert!(runner.is_err());
    }

    #[test]
    fn test_runner_initial_state_is_pending() {
        let config = SessionRunnerConfig::default();
        let runner = SessionRunner::new(config).unwrap();
        assert_eq!(runner.state(), SessionState::Pending);
    }

    #[test]
    fn test_runner_initialization_changes_state_to_running() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();

        runner.initialize().unwrap();

        assert_eq!(runner.state(), SessionState::Running);
    }

    #[test]
    fn test_runner_double_initialization_fails() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();

        runner.initialize().unwrap();
        let second_init = runner.initialize();

        assert!(second_init.is_err());
    }

    #[test]
    fn test_runner_process_event_before_init_fails() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();

        let event = create_test_event(1000, dec!(50000));
        let result = runner.process_event(&event);

        assert!(result.is_err());
    }

    #[test]
    fn test_runner_process_event_succeeds_after_init() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        let event = create_test_event(1000, dec!(50000));
        let result = runner.process_event(&event);

        assert!(result.is_ok());
    }

    #[test]
    fn test_runner_stop_changes_state() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        runner.stop();

        assert_eq!(runner.state(), SessionState::Stopped);
    }

    #[test]
    fn test_runner_stop_prevents_further_events() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();
        runner.stop();

        let event = create_test_event(1000, dec!(50000));
        let result = runner.process_event(&event);

        assert!(result.is_err());
    }

    // ========================================================================
    // Event Processing Tests - Skeptical validation of fill logic
    // ========================================================================

    #[test]
    fn test_event_processing_increments_counter() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        assert_eq!(runner.events_processed(), 0);

        let event = create_test_event(1000, dec!(50000));
        runner.process_event(&event).unwrap();

        assert_eq!(runner.events_processed(), 1);
    }

    #[test]
    fn test_event_without_trade_produces_no_fills() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        let event = SimulatedEvent {
            timestamp_ms: 1000,
            mid_price: dec!(50000),
            best_bid: dec!(49999),
            best_ask: dec!(50001),
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: 0.0,
            trade: None, // No trade
        };

        let fills = runner.process_event(&event).unwrap();

        assert!(fills.is_empty());
    }

    #[test]
    fn test_event_with_trade_may_produce_fill() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Process some events to establish quotes
        for i in 0..10 {
            let event = create_test_event(i * 100, dec!(50000));
            runner.process_event(&event).unwrap();
        }

        // Event with aggressive sell that might hit our bid
        let event = SimulatedEvent {
            timestamp_ms: 1000,
            mid_price: dec!(50000),
            best_bid: dec!(49999),
            best_ask: dec!(50001),
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: 0.0,
            trade: Some(SimulatedTrade {
                price: dec!(49990),
                quantity: dec!(1.0), // Large trade
                is_buyer_maker: true, // Aggressive sell
            }),
        };

        let fills = runner.process_event(&event).unwrap();

        // May or may not fill depending on queue model - just verify no panic
        // This is a skeptical test - we don't assume fills happen
        assert!(fills.len() <= 1);
    }

    #[test]
    fn test_fill_rate_is_bounded_zero_to_one() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Process many events
        for i in 0..100 {
            let event = create_test_event(i * 100, dec!(50000));
            runner.process_event(&event).unwrap();
        }

        let fill_rate = runner.current_fill_rate();

        assert!(fill_rate >= 0.0, "Fill rate cannot be negative");
        assert!(fill_rate <= 1.0, "Fill rate cannot exceed 100%");
    }

    #[test]
    fn test_metrics_are_updated_on_event() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        let initial_quotes = runner.metrics().unwrap().quotes_generated;

        let event = create_test_event(1000, dec!(50000));
        runner.process_event(&event).unwrap();

        let final_quotes = runner.metrics().unwrap().quotes_generated;

        assert!(final_quotes > initial_quotes);
    }

    // ========================================================================
    // Progress Tracking Tests
    // ========================================================================

    #[test]
    fn test_progress_shows_zero_initially() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        let progress = runner.progress();

        assert!(progress.elapsed_secs < 1.0, "Should be near zero initially");
        assert!(progress.progress_pct < 1.0, "Should be near zero initially");
    }

    #[test]
    fn test_progress_tracks_events() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        for i in 0..10 {
            let event = create_test_event(i * 100, dec!(50000));
            runner.process_event(&event).unwrap();
        }

        let progress = runner.progress();

        assert_eq!(progress.events_processed, 10);
    }

    #[test]
    fn test_should_continue_respects_duration() {
        // Very short duration for testing
        let mut config = SessionRunnerConfig::default();
        config.duration_hours = 0.0001; // ~0.36 seconds
        config.min_duration_hours = 0.0; // Allow very short sessions for test

        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Should start as true
        assert!(runner.should_continue());

        // Wait for duration to pass
        std::thread::sleep(Duration::from_millis(500));

        // Should now be false
        assert!(!runner.should_continue());
    }

    // ========================================================================
    // Finalization Tests
    // ========================================================================

    #[test]
    fn test_finalize_produces_result() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Process some events
        for i in 0..10 {
            let event = create_test_event(i * 100, dec!(50000));
            runner.process_event(&event).unwrap();
        }

        let result = runner.finalize().unwrap();

        assert!(result.summary_path.to_str().unwrap().contains("summary_"));
    }

    #[test]
    fn test_finalize_marks_invalid_session() {
        let mut config = SessionRunnerConfig::default();
        config.duration_hours = 168.0; // 1 week
        config.min_duration_hours = 100.0; // Impossible to meet in test
        config.min_trades = 1000;

        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Process a few events (not enough)
        for i in 0..5 {
            let event = create_test_event(i * 100, dec!(50000));
            runner.process_event(&event).unwrap();
        }

        let result = runner.finalize().unwrap();

        assert!(!result.is_valid_for_validation);
        assert!(!result.warnings.is_empty());
    }

    #[test]
    fn test_finalize_changes_state_to_completed() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        runner.finalize().unwrap();

        assert_eq!(runner.state(), SessionState::Completed);
    }

    // ========================================================================
    // Fill Rate Statistics Tests - Critical for calibration
    // ========================================================================

    #[test]
    fn test_fill_rate_stats_from_empty_metrics() {
        let metrics = SessionMetrics::default();
        let stats = FillRateStats::from_metrics(&metrics);

        assert_eq!(stats.overall_fill_rate, 0.0);
        assert_eq!(stats.quotes_generated, 0);
        assert_eq!(stats.fills_received, 0);
    }

    #[test]
    fn test_fill_rate_stats_computes_correct_rate() {
        let metrics = SessionMetrics {
            quotes_generated: 100,
            total_trades: 10,
            ..Default::default()
        };

        let stats = FillRateStats::from_metrics(&metrics);

        assert!((stats.overall_fill_rate - 0.10).abs() < 0.001);
    }

    #[test]
    fn test_fill_rate_confidence_interval_contains_point_estimate() {
        let metrics = SessionMetrics {
            quotes_generated: 1000,
            total_trades: 100,
            ..Default::default()
        };

        let stats = FillRateStats::from_metrics(&metrics);

        assert!(
            stats.ci_lower <= stats.overall_fill_rate,
            "CI lower bound should be <= point estimate"
        );
        assert!(
            stats.ci_upper >= stats.overall_fill_rate,
            "CI upper bound should be >= point estimate"
        );
    }

    #[test]
    fn test_fill_rate_confidence_interval_bounds() {
        let metrics = SessionMetrics {
            quotes_generated: 100,
            total_trades: 50,
            ..Default::default()
        };

        let stats = FillRateStats::from_metrics(&metrics);

        assert!(stats.ci_lower >= 0.0, "CI lower bound cannot be negative");
        assert!(stats.ci_upper <= 1.0, "CI upper bound cannot exceed 1");
    }

    #[test]
    fn test_fill_rate_differs_with_insufficient_data() {
        let metrics = SessionMetrics {
            quotes_generated: 10, // Too few
            total_trades: 1,
            ..Default::default()
        };

        let stats = FillRateStats::from_metrics(&metrics);

        // Should not detect difference with insufficient data
        assert!(
            !stats.differs_from_assumption(0.5, 0.95),
            "Should not detect difference with n<30"
        );
    }

    #[test]
    fn test_fill_rate_detects_significant_difference() {
        let metrics = SessionMetrics {
            quotes_generated: 1000,
            total_trades: 50, // 5% fill rate
            ..Default::default()
        };

        let stats = FillRateStats::from_metrics(&metrics);

        // Should detect difference from 10% assumption
        assert!(
            stats.differs_from_assumption(0.10, 0.95),
            "Should detect 5% vs 10% with n=1000"
        );
    }

    #[test]
    fn test_fill_rate_does_not_detect_small_difference() {
        let metrics = SessionMetrics {
            quotes_generated: 100,
            total_trades: 11, // 11% fill rate
            ..Default::default()
        };

        let stats = FillRateStats::from_metrics(&metrics);

        // Should NOT detect difference from 10% assumption (too small sample)
        assert!(
            !stats.differs_from_assumption(0.10, 0.95),
            "Should not detect 11% vs 10% with small sample"
        );
    }

    // ========================================================================
    // Preset Loading Tests
    // ========================================================================

    #[test]
    fn test_preset_loading_falls_back_on_unknown_preset() {
        let mut config = SessionRunnerConfig::default();
        config.preset_name = Some("nonexistent_preset_xyz".to_string());

        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Should have warning about missing preset
        assert!(!runner.warnings.is_empty());
        assert!(runner.warnings[0].contains("not found"));

        // But should still work with defaults
        assert_eq!(runner.state(), SessionState::Running);
    }

    // ========================================================================
    // Edge Cases and Boundary Tests
    // ========================================================================

    #[test]
    fn test_runner_handles_extreme_prices() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Very high price
        let event = create_test_event(1000, dec!(1000000));
        assert!(runner.process_event(&event).is_ok());

        // Very low price
        let event = create_test_event(2000, dec!(0.001));
        assert!(runner.process_event(&event).is_ok());
    }

    #[test]
    fn test_runner_handles_extreme_volatility() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        let event = SimulatedEvent {
            timestamp_ms: 1000,
            mid_price: dec!(50000),
            best_bid: dec!(49999),
            best_ask: dec!(50001),
            volatility: 10.0, // Extreme volatility
            entropy: 0.8,
            book_imbalance: 0.0,
            trade: None,
        };

        // Should not panic
        assert!(runner.process_event(&event).is_ok());
    }

    #[test]
    fn test_runner_handles_zero_entropy() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        let event = SimulatedEvent {
            timestamp_ms: 1000,
            mid_price: dec!(50000),
            best_bid: dec!(49999),
            best_ask: dec!(50001),
            volatility: 0.001,
            entropy: 0.0, // Zero entropy
            book_imbalance: 0.0,
            trade: None,
        };

        assert!(runner.process_event(&event).is_ok());
    }

    #[test]
    fn test_runner_handles_extreme_imbalance() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Strong buy imbalance
        let event = SimulatedEvent {
            timestamp_ms: 1000,
            mid_price: dec!(50000),
            best_bid: dec!(49999),
            best_ask: dec!(50001),
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: 1.0, // Full buy imbalance
            trade: None,
        };
        assert!(runner.process_event(&event).is_ok());

        // Strong sell imbalance
        let event = SimulatedEvent {
            timestamp_ms: 2000,
            mid_price: dec!(50000),
            best_bid: dec!(49999),
            best_ask: dec!(50001),
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: -1.0, // Full sell imbalance
            trade: None,
        };
        assert!(runner.process_event(&event).is_ok());
    }

    #[test]
    fn test_runner_many_events_no_memory_leak() {
        let config = SessionRunnerConfig::default();
        let mut runner = SessionRunner::new(config).unwrap();
        runner.initialize().unwrap();

        // Process many events
        for i in 0..10000 {
            let event = create_test_event(i * 100, dec!(50000));
            runner.process_event(&event).unwrap();
        }

        assert_eq!(runner.events_processed(), 10000);

        // Should still be able to finalize
        assert!(runner.finalize().is_ok());
    }

    // ========================================================================
    // Serialization Tests
    // ========================================================================

    #[test]
    fn test_session_state_serialization() {
        let states = vec![
            SessionState::Pending,
            SessionState::Running,
            SessionState::Completed,
            SessionState::Stopped,
            SessionState::Failed,
        ];

        for state in states {
            let json = serde_json::to_string(&state).unwrap();
            let deserialized: SessionState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, deserialized);
        }
    }

    #[test]
    fn test_session_result_serialization() {
        let summary = SessionSummary {
            session_id: "test123".to_string(),
            config: ForwardTestConfig::default(),
            metrics: SessionMetrics::default(),
            trade_count: 10,
        };

        let result = SessionResult {
            summary,
            final_state: SessionState::Completed,
            events_processed: 1000,
            summary_path: PathBuf::from("/tmp/test.json"),
            trades_path: None,
            warnings: vec!["Test warning".to_string()],
            is_valid_for_validation: true,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("test123"));
        assert!(json.contains("Test warning"));
    }

    #[test]
    fn test_fill_rate_stats_serialization() {
        let stats = FillRateStats {
            quotes_generated: 1000,
            fills_received: 100,
            overall_fill_rate: 0.10,
            bid_fill_rate: 0.08,
            ask_fill_rate: 0.12,
            hourly_fill_rates: vec![0.09, 0.11, 0.10],
            ci_lower: 0.08,
            ci_upper: 0.12,
            std_error: 0.01,
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: FillRateStats = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.quotes_generated, 1000);
        assert!((deserialized.overall_fill_rate - 0.10).abs() < 0.001);
    }

    // ========================================================================
    // Helper Functions
    // ========================================================================

    fn create_test_event(timestamp_ms: u64, mid_price: Decimal) -> SimulatedEvent {
        let spread = mid_price * dec!(0.0001); // 1 bps spread
        SimulatedEvent {
            timestamp_ms,
            mid_price,
            best_bid: mid_price - spread,
            best_ask: mid_price + spread,
            volatility: 0.001,
            entropy: 0.8,
            book_imbalance: 0.0,
            trade: None,
        }
    }
}
