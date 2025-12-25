//! Algorithm Factory Implementation (Task 3.3)
//!
//! Factory that creates trading algorithm instances from research state.
//! This module bridges the gap between research findings and executable algorithms.
//!
//! # Key Features
//!
//! - **Research-Driven**: Takes `ResearchState` as input and creates appropriate algorithm
//! - **Strategy Selection**: Automatically selects Momentum, MarketMaking, or None based on assessment
//! - **Config Generation**: Uses `AlgorithmConfig::from_research` for parameterization
//! - **Edge Detection**: Returns None if no exploitable edge is detected
//!
//! # Usage
//!
//! ```ignore
//! use crate::strategies::AlgorithmFactory;
//! use crate::core::ResearchState;
//!
//! let factory = AlgorithmFactory::new();
//! let research = load_research_state();
//!
//! if let Some(algorithm) = factory.create(&research) {
//!     // Algorithm is ready for trading
//!     let decision = algorithm.decide(&input);
//! } else {
//!     // No edge detected, don't trade
//! }
//! ```

use crate::core::{
    AlgorithmConfig, ResearchState, RecommendedStrategy, StrategyType,
};
use super::trading_algorithm::TradingAlgorithm;
use super::momentum::MomentumAlgorithmFactory;
use super::market_making_trading::MarketMakingTradingAlgorithmFactory;

// ============================================================================
// Algorithm Factory Error
// ============================================================================

/// Errors that can occur during algorithm creation
#[derive(Debug, Clone)]
pub enum AlgorithmFactoryError {
    /// No edge detected in research state
    NoEdgeDetected(String),
    /// Research state is incomplete
    IncompleteResearch(String),
    /// Strategy type not supported
    UnsupportedStrategy(StrategyType),
    /// Algorithm creation failed
    CreationFailed(String),
}

impl std::fmt::Display for AlgorithmFactoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlgorithmFactoryError::NoEdgeDetected(s) => write!(f, "No edge detected: {}", s),
            AlgorithmFactoryError::IncompleteResearch(s) => write!(f, "Incomplete research: {}", s),
            AlgorithmFactoryError::UnsupportedStrategy(s) => write!(f, "Unsupported strategy: {:?}", s),
            AlgorithmFactoryError::CreationFailed(s) => write!(f, "Creation failed: {}", s),
        }
    }
}

impl std::error::Error for AlgorithmFactoryError {}

// ============================================================================
// Algorithm Factory
// ============================================================================

/// Factory for creating trading algorithms from research state.
///
/// The factory analyzes the research state to determine:
/// 1. Whether an exploitable edge exists
/// 2. Which strategy type is most appropriate
/// 3. How to parameterize the algorithm
///
/// # Strategy Selection Logic
///
/// ```text
/// ResearchState
///       │
///       ▼
/// ┌─────────────────────┐
/// │ Check assessment    │
/// │ is_tradeable?       │
/// └─────────────────────┘
///       │
///       ├── Not tradeable ──────────────────┐
///       │                                    │
///       ▼                                    ▼
/// ┌─────────────────────┐          ┌────────────────┐
/// │ Check recommended   │          │ Return None    │
/// │ strategy            │          │ (no edge)      │
/// └─────────────────────┘          └────────────────┘
///       │
///       ├── Momentum ────────▶ Create MomentumAlgorithm
///       │
///       ├── MarketMaking ────▶ Create MarketMakingTradingAlgorithm
///       │
///       └── Hybrid ──────────▶ Create MomentumAlgorithm (default)
/// ```
#[derive(Debug, Clone, Default)]
pub struct AlgorithmFactory {
    /// Minimum edge required to create an algorithm (default: 0)
    pub min_edge_threshold: f64,
    /// Whether to allow trading in uncertain regimes
    pub allow_uncertain: bool,
    /// Whether to require all assessment criteria to pass
    pub require_all_criteria: bool,
}

impl AlgorithmFactory {
    /// Create a new algorithm factory with default settings
    pub fn new() -> Self {
        Self {
            min_edge_threshold: 0.0,
            allow_uncertain: false,
            require_all_criteria: false,
        }
    }

    /// Create with strict requirements (all criteria must pass)
    pub fn strict() -> Self {
        Self {
            min_edge_threshold: 0.001, // 0.1% minimum edge
            allow_uncertain: false,
            require_all_criteria: true,
        }
    }

    /// Create with relaxed requirements (for research/testing)
    pub fn relaxed() -> Self {
        Self {
            min_edge_threshold: 0.0,
            allow_uncertain: true,
            require_all_criteria: false,
        }
    }

    /// Set minimum edge threshold
    pub fn with_min_edge(mut self, threshold: f64) -> Self {
        self.min_edge_threshold = threshold;
        self
    }

    /// Set whether to allow uncertain regimes
    pub fn with_allow_uncertain(mut self, allow: bool) -> Self {
        self.allow_uncertain = allow;
        self
    }

    /// Set whether to require all criteria
    pub fn with_require_all_criteria(mut self, require: bool) -> Self {
        self.require_all_criteria = require;
        self
    }

    /// Create an algorithm from research state.
    ///
    /// Returns `None` if no exploitable edge is detected.
    ///
    /// # Arguments
    /// * `research` - The research state to create an algorithm from
    ///
    /// # Returns
    /// `Some(Box<dyn TradingAlgorithm>)` if an algorithm can be created,
    /// `None` if no edge is detected or research state is insufficient.
    pub fn create(&self, research: &ResearchState) -> Option<Box<dyn TradingAlgorithm>> {
        // Check if edge exists
        if !self.has_exploitable_edge(research) {
            return None;
        }

        // Generate config from research
        let config = AlgorithmConfig::from_research(research);

        // Select and create appropriate algorithm
        match self.select_strategy(research) {
            Some(StrategyType::Momentum) => {
                self.create_momentum_algorithm(config).ok()
            }
            Some(StrategyType::MarketMaking) => {
                self.create_market_making_algorithm(config).ok()
            }
            Some(StrategyType::Hybrid) => {
                // For hybrid, default to momentum as primary strategy
                self.create_momentum_algorithm(config).ok()
            }
            None => None,
        }
    }

    /// Create an algorithm with detailed error information.
    ///
    /// Unlike `create()`, this method returns an error with details
    /// about why algorithm creation failed.
    pub fn try_create(&self, research: &ResearchState) -> Result<Box<dyn TradingAlgorithm>, AlgorithmFactoryError> {
        // Validate research state
        self.validate_research(research)?;

        // Check if edge exists
        if !self.has_exploitable_edge(research) {
            return Err(AlgorithmFactoryError::NoEdgeDetected(
                research.assessment.reasoning.clone()
            ));
        }

        // Generate config from research
        let config = AlgorithmConfig::from_research(research);

        // Select strategy
        let strategy = self.select_strategy(research)
            .ok_or_else(|| AlgorithmFactoryError::NoEdgeDetected(
                "No viable strategy identified".to_string()
            ))?;

        // Create algorithm
        match strategy {
            StrategyType::Momentum => {
                self.create_momentum_algorithm(config)
            }
            StrategyType::MarketMaking => {
                self.create_market_making_algorithm(config)
            }
            StrategyType::Hybrid => {
                // Default to momentum for hybrid
                self.create_momentum_algorithm(config)
            }
        }
    }

    /// Check if research state has an exploitable edge
    pub fn has_exploitable_edge(&self, research: &ResearchState) -> bool {
        let assessment = &research.assessment;

        // If require_all_criteria, check all flags
        if self.require_all_criteria {
            if !assessment.midc_ok || !assessment.entropy_ok ||
               !assessment.persistence_ok || !assessment.signals_ok {
                return false;
            }
        }

        // Check tradeable status
        if !assessment.is_tradeable && !self.allow_uncertain {
            // Even if not tradeable, market making might be possible
            if assessment.recommended_strategy != RecommendedStrategy::MarketMaking {
                return false;
            }
        }

        // Check position scale
        if assessment.position_scale <= 0.0 && !self.allow_uncertain {
            return false;
        }

        // Check recommended strategy
        matches!(
            assessment.recommended_strategy,
            RecommendedStrategy::Momentum |
            RecommendedStrategy::MarketMaking |
            RecommendedStrategy::Hybrid |
            RecommendedStrategy::TSMOM |
            RecommendedStrategy::MACrossover
        )
    }

    /// Select the appropriate strategy type based on research
    pub fn select_strategy(&self, research: &ResearchState) -> Option<StrategyType> {
        let assessment = &research.assessment;

        match assessment.recommended_strategy {
            RecommendedStrategy::Momentum |
            RecommendedStrategy::TSMOM |
            RecommendedStrategy::MACrossover => {
                Some(StrategyType::Momentum)
            }
            RecommendedStrategy::MarketMaking => {
                Some(StrategyType::MarketMaking)
            }
            RecommendedStrategy::Hybrid => {
                Some(StrategyType::Hybrid)
            }
            RecommendedStrategy::None => {
                if self.allow_uncertain {
                    // Default to market making in uncertain conditions
                    Some(StrategyType::MarketMaking)
                } else {
                    None
                }
            }
        }
    }

    /// Validate that research state has sufficient data
    fn validate_research(&self, research: &ResearchState) -> Result<(), AlgorithmFactoryError> {
        // Check for minimum data
        if research.snapshots_processed < 100 {
            return Err(AlgorithmFactoryError::IncompleteResearch(
                format!("Insufficient data: {} snapshots (need >= 100)", research.snapshots_processed)
            ));
        }

        // Check MIDC validity
        if !research.midc.is_valid() && self.require_all_criteria {
            return Err(AlgorithmFactoryError::IncompleteResearch(
                "MIDC estimate is not valid".to_string()
            ));
        }

        Ok(())
    }

    /// Create a momentum algorithm from config
    fn create_momentum_algorithm(&self, config: AlgorithmConfig) -> Result<Box<dyn TradingAlgorithm>, AlgorithmFactoryError> {
        MomentumAlgorithmFactory::create(config)
            .map_err(|e| AlgorithmFactoryError::CreationFailed(e.to_string()))
    }

    /// Create a market making algorithm from config
    fn create_market_making_algorithm(&self, config: AlgorithmConfig) -> Result<Box<dyn TradingAlgorithm>, AlgorithmFactoryError> {
        MarketMakingTradingAlgorithmFactory::create(config)
            .map_err(|e| AlgorithmFactoryError::CreationFailed(e.to_string()))
    }

    /// Get information about what algorithm would be created
    pub fn preview(&self, research: &ResearchState) -> AlgorithmPreview {
        let has_edge = self.has_exploitable_edge(research);
        let strategy = self.select_strategy(research);
        let config = AlgorithmConfig::from_research(research);

        AlgorithmPreview {
            would_create: has_edge && strategy.is_some(),
            selected_strategy: strategy,
            config_id: config.id.clone(),
            reasoning: if has_edge {
                format!("Edge detected: {}", research.assessment.reasoning)
            } else {
                format!("No edge: {}", research.assessment.reasoning)
            },
            position_scale: research.assessment.position_scale,
            research_id: research.id.clone(),
        }
    }
}

/// Preview of what algorithm would be created
#[derive(Debug, Clone)]
pub struct AlgorithmPreview {
    /// Whether an algorithm would be created
    pub would_create: bool,
    /// Selected strategy type (if any)
    pub selected_strategy: Option<StrategyType>,
    /// Config ID that would be used
    pub config_id: String,
    /// Reasoning for the decision
    pub reasoning: String,
    /// Recommended position scale
    pub position_scale: f64,
    /// Research state ID
    pub research_id: String,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::research_state::{
        MIDCEstimate, PersistenceStats, TradeableAssessment,
    };
    use chrono::Utc;

    /// Create a research state that should produce momentum algorithm
    fn create_momentum_research() -> ResearchState {
        let mut research = ResearchState::new("BTCUSDT");
        research.midc = MIDCEstimate::new(0.05, 0.8, 0.9, 1000);
        research.entropy = 0.3;
        research.persistence = PersistenceStats {
            mean_duration_seconds: 120.0,
            median_duration_seconds: 100.0,
            std_duration_seconds: 30.0,
            percentile_25: 60.0,
            percentile_75: 150.0,
            sample_count: 500,
            updated_at: Utc::now(),
        };
        research.assessment = TradeableAssessment {
            midc_ok: true,
            entropy_ok: true,
            persistence_ok: true,
            signals_ok: true,
            is_tradeable: true,
            recommended_strategy: RecommendedStrategy::Momentum,
            position_scale: 1.0,
            reasoning: "All conditions favorable for momentum".to_string(),
            assessed_at: Utc::now(),
        };
        research.snapshots_processed = 1000;
        research
    }

    /// Create a research state that should produce market making algorithm
    fn create_market_making_research() -> ResearchState {
        let mut research = ResearchState::new("BTCUSDT");
        research.midc = MIDCEstimate::new(0.15, 0.4, 0.6, 1000);
        research.entropy = 0.7;
        research.persistence = PersistenceStats {
            mean_duration_seconds: 30.0,
            median_duration_seconds: 25.0,
            std_duration_seconds: 10.0,
            percentile_25: 15.0,
            percentile_75: 40.0,
            sample_count: 500,
            updated_at: Utc::now(),
        };
        research.assessment = TradeableAssessment {
            midc_ok: false,
            entropy_ok: true,
            persistence_ok: false,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: RecommendedStrategy::MarketMaking,
            position_scale: 0.5,
            reasoning: "Market making recommended due to mean reversion".to_string(),
            assessed_at: Utc::now(),
        };
        research.snapshots_processed = 1000;
        research
    }

    /// Create a research state with no edge
    fn create_no_edge_research() -> ResearchState {
        let mut research = ResearchState::new("BTCUSDT");
        research.midc = MIDCEstimate::default();
        research.entropy = 0.9;
        research.assessment = TradeableAssessment {
            midc_ok: false,
            entropy_ok: false,
            persistence_ok: false,
            signals_ok: false,
            is_tradeable: false,
            recommended_strategy: RecommendedStrategy::None,
            position_scale: 0.0,
            reasoning: "No exploitable edge detected".to_string(),
            assessed_at: Utc::now(),
        };
        research.snapshots_processed = 1000;
        research
    }

    /// Create a research state with insufficient data
    fn create_insufficient_data_research() -> ResearchState {
        let mut research = create_momentum_research();
        research.snapshots_processed = 50;
        research
    }

    // ========================================================================
    // Factory Creation Tests
    // ========================================================================

    #[test]
    fn test_factory_new_default_settings() {
        let factory = AlgorithmFactory::new();
        assert_eq!(factory.min_edge_threshold, 0.0);
        assert!(!factory.allow_uncertain);
        assert!(!factory.require_all_criteria);
    }

    #[test]
    fn test_factory_strict_settings() {
        let factory = AlgorithmFactory::strict();
        assert!(factory.min_edge_threshold > 0.0);
        assert!(!factory.allow_uncertain);
        assert!(factory.require_all_criteria);
    }

    #[test]
    fn test_factory_relaxed_settings() {
        let factory = AlgorithmFactory::relaxed();
        assert_eq!(factory.min_edge_threshold, 0.0);
        assert!(factory.allow_uncertain);
        assert!(!factory.require_all_criteria);
    }

    #[test]
    fn test_factory_builder_pattern() {
        let factory = AlgorithmFactory::new()
            .with_min_edge(0.002)
            .with_allow_uncertain(true)
            .with_require_all_criteria(true);

        assert_eq!(factory.min_edge_threshold, 0.002);
        assert!(factory.allow_uncertain);
        assert!(factory.require_all_criteria);
    }

    // ========================================================================
    // Algorithm Creation Tests
    // ========================================================================

    #[test]
    fn test_create_momentum_algorithm() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let algo = factory.create(&research);
        assert!(algo.is_some());

        let algo = algo.unwrap();
        assert_eq!(algo.strategy_type(), StrategyType::Momentum);
    }

    #[test]
    fn test_create_market_making_algorithm() {
        let factory = AlgorithmFactory::new();
        let research = create_market_making_research();

        let algo = factory.create(&research);
        assert!(algo.is_some());

        let algo = algo.unwrap();
        assert_eq!(algo.strategy_type(), StrategyType::MarketMaking);
    }

    #[test]
    fn test_create_returns_none_for_no_edge() {
        let factory = AlgorithmFactory::new();
        let research = create_no_edge_research();

        let algo = factory.create(&research);
        assert!(algo.is_none());
    }

    #[test]
    fn test_try_create_returns_error_for_no_edge() {
        let factory = AlgorithmFactory::new();
        let research = create_no_edge_research();

        let result = factory.try_create(&research);
        assert!(result.is_err());

        match result {
            Err(AlgorithmFactoryError::NoEdgeDetected(_)) => {}
            _ => panic!("Expected NoEdgeDetected error"),
        }
    }

    #[test]
    fn test_try_create_returns_error_for_insufficient_data() {
        let factory = AlgorithmFactory::strict();
        let research = create_insufficient_data_research();

        let result = factory.try_create(&research);
        assert!(result.is_err());

        match result {
            Err(AlgorithmFactoryError::IncompleteResearch(_)) => {}
            _ => panic!("Expected IncompleteResearch error"),
        }
    }

    // ========================================================================
    // Edge Detection Tests
    // ========================================================================

    #[test]
    fn test_has_exploitable_edge_momentum() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        assert!(factory.has_exploitable_edge(&research));
    }

    #[test]
    fn test_has_exploitable_edge_market_making() {
        let factory = AlgorithmFactory::new();
        let research = create_market_making_research();

        assert!(factory.has_exploitable_edge(&research));
    }

    #[test]
    fn test_no_exploitable_edge() {
        let factory = AlgorithmFactory::new();
        let research = create_no_edge_research();

        assert!(!factory.has_exploitable_edge(&research));
    }

    #[test]
    fn test_strict_factory_requires_all_criteria() {
        let factory = AlgorithmFactory::strict();

        // Create research with one failing criterion
        let mut research = create_momentum_research();
        research.assessment.signals_ok = false;

        assert!(!factory.has_exploitable_edge(&research));
    }

    #[test]
    fn test_relaxed_factory_allows_uncertain() {
        let factory = AlgorithmFactory::relaxed();
        let mut research = create_no_edge_research();

        // Still no edge due to RecommendedStrategy::None
        assert!(!factory.has_exploitable_edge(&research));

        // But uncertain regime with position_scale > 0 should allow
        research.assessment.position_scale = 0.5;
        research.assessment.recommended_strategy = RecommendedStrategy::MarketMaking;
        assert!(factory.has_exploitable_edge(&research));
    }

    // ========================================================================
    // Strategy Selection Tests
    // ========================================================================

    #[test]
    fn test_select_strategy_momentum() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, Some(StrategyType::Momentum));
    }

    #[test]
    fn test_select_strategy_market_making() {
        let factory = AlgorithmFactory::new();
        let research = create_market_making_research();

        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, Some(StrategyType::MarketMaking));
    }

    #[test]
    fn test_select_strategy_none() {
        let factory = AlgorithmFactory::new();
        let research = create_no_edge_research();

        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, None);
    }

    #[test]
    fn test_select_strategy_tsmom() {
        let factory = AlgorithmFactory::new();
        let mut research = create_momentum_research();
        research.assessment.recommended_strategy = RecommendedStrategy::TSMOM;

        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, Some(StrategyType::Momentum));
    }

    #[test]
    fn test_select_strategy_ma_crossover() {
        let factory = AlgorithmFactory::new();
        let mut research = create_momentum_research();
        research.assessment.recommended_strategy = RecommendedStrategy::MACrossover;

        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, Some(StrategyType::Momentum));
    }

    #[test]
    fn test_select_strategy_hybrid() {
        let factory = AlgorithmFactory::new();
        let mut research = create_momentum_research();
        research.assessment.recommended_strategy = RecommendedStrategy::Hybrid;

        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, Some(StrategyType::Hybrid));
    }

    #[test]
    fn test_select_strategy_none_with_uncertain_allowed() {
        let factory = AlgorithmFactory::relaxed();
        let research = create_no_edge_research();

        // With allow_uncertain, should default to MarketMaking
        let strategy = factory.select_strategy(&research);
        assert_eq!(strategy, Some(StrategyType::MarketMaking));
    }

    // ========================================================================
    // Preview Tests
    // ========================================================================

    #[test]
    fn test_preview_momentum() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let preview = factory.preview(&research);
        assert!(preview.would_create);
        assert_eq!(preview.selected_strategy, Some(StrategyType::Momentum));
        assert_eq!(preview.position_scale, 1.0);
        assert!(preview.reasoning.contains("Edge detected"));
    }

    #[test]
    fn test_preview_no_edge() {
        let factory = AlgorithmFactory::new();
        let research = create_no_edge_research();

        let preview = factory.preview(&research);
        assert!(!preview.would_create);
        assert!(preview.selected_strategy.is_none());
        assert!(preview.reasoning.contains("No edge"));
    }

    #[test]
    fn test_preview_contains_research_id() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let preview = factory.preview(&research);
        assert_eq!(preview.research_id, research.id);
    }

    // ========================================================================
    // Error Type Tests
    // ========================================================================

    #[test]
    fn test_error_display_no_edge() {
        let err = AlgorithmFactoryError::NoEdgeDetected("test reason".to_string());
        let display = format!("{}", err);
        assert!(display.contains("No edge detected"));
        assert!(display.contains("test reason"));
    }

    #[test]
    fn test_error_display_incomplete() {
        let err = AlgorithmFactoryError::IncompleteResearch("missing data".to_string());
        let display = format!("{}", err);
        assert!(display.contains("Incomplete research"));
        assert!(display.contains("missing data"));
    }

    #[test]
    fn test_error_display_unsupported() {
        let err = AlgorithmFactoryError::UnsupportedStrategy(StrategyType::Hybrid);
        let display = format!("{}", err);
        assert!(display.contains("Unsupported strategy"));
    }

    #[test]
    fn test_error_display_creation_failed() {
        let err = AlgorithmFactoryError::CreationFailed("internal error".to_string());
        let display = format!("{}", err);
        assert!(display.contains("Creation failed"));
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_research_sufficient_data() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let result = factory.validate_research(&research);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_research_insufficient_snapshots() {
        let factory = AlgorithmFactory::strict();
        let mut research = create_momentum_research();
        research.snapshots_processed = 50;

        let result = factory.validate_research(&research);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_research_invalid_midc_strict() {
        let factory = AlgorithmFactory::strict();
        let mut research = create_momentum_research();
        research.midc = MIDCEstimate::default(); // Invalid MIDC

        let result = factory.validate_research(&research);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_research_invalid_midc_relaxed() {
        let factory = AlgorithmFactory::relaxed();
        let mut research = create_momentum_research();
        research.midc = MIDCEstimate::default(); // Invalid MIDC

        // Relaxed factory doesn't require valid MIDC
        let result = factory.validate_research(&research);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Integration Tests
    // ========================================================================

    #[test]
    fn test_created_algorithm_has_correct_config() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let algo = factory.create(&research).unwrap();

        // Config should be derived from research
        let config = algo.config();
        assert_eq!(config.symbol, "BTCUSDT");
    }

    #[test]
    fn test_created_algorithm_is_ready_to_trade() {
        let factory = AlgorithmFactory::new();
        let research = create_momentum_research();

        let algo = factory.create(&research).unwrap();

        // Algorithm should start in flat state
        assert!(algo.state().is_flat());
        assert_eq!(algo.trade_count(), 0);
    }

    #[test]
    fn test_factory_respects_position_scale() {
        let factory = AlgorithmFactory::new();

        // High position scale
        let research_high = create_momentum_research();
        assert_eq!(research_high.assessment.position_scale, 1.0);

        // Low position scale
        let mut research_low = create_momentum_research();
        research_low.assessment.position_scale = 0.3;

        // Both should create algorithms (position scale doesn't prevent creation)
        assert!(factory.create(&research_high).is_some());
        assert!(factory.create(&research_low).is_some());
    }

    #[test]
    fn test_zero_position_scale_prevents_creation() {
        let factory = AlgorithmFactory::new();
        let mut research = create_momentum_research();
        research.assessment.position_scale = 0.0;
        research.assessment.is_tradeable = false;
        research.assessment.recommended_strategy = RecommendedStrategy::None;

        assert!(factory.create(&research).is_none());
    }
}
