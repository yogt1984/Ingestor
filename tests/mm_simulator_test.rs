//! Tests for the mm_simulator module

use ingestor::market_maker::{MarketMakerEngine, MMConfig};
use ingestor::mm_simulator::{PaperTradingEngine, SimulatorConfig};
use rust_decimal_macros::dec;

#[test]
fn test_simulator_config_default() {
    let config = SimulatorConfig::default();

    assert_eq!(config.fee_rate, dec!(0.0001)); // 1 bps
    assert_eq!(config.min_quote_age_ms, 100);
    assert!(config.use_queue_model);
    assert_eq!(config.queue_position_fraction, 0.5);
}

#[test]
fn test_paper_trading_creation() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig::default();

    let engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    let state = engine.state();
    assert_eq!(state.mm_state.inventory, dec!(0));
    assert_eq!(state.mm_state.pnl.total_pnl, dec!(0));
}

#[test]
fn test_paper_trading_state_mm_state() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig::default();

    let engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    let state = engine.state();
    assert_eq!(state.mm_state.inventory, dec!(0));
    assert_eq!(state.mm_state.avg_entry_price, dec!(0));
}

#[test]
fn test_paper_trading_reset() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig::default();

    let mut engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    // Reset
    engine.reset();

    let state = engine.state();
    assert_eq!(state.mm_state.inventory, dec!(0));
}

#[test]
fn test_simulator_with_queue_model() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig {
        use_queue_model: true,
        queue_position_fraction: 0.5,
        ..Default::default()
    };

    let engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    let state = engine.state();
    assert_eq!(state.mm_state.inventory, dec!(0));
}

#[test]
fn test_simulator_without_queue_model() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig {
        use_queue_model: false,
        ..Default::default()
    };

    let engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    let state = engine.state();
    assert_eq!(state.mm_state.inventory, dec!(0));
}

#[test]
fn test_fee_rate_configuration() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig {
        fee_rate: dec!(0.0005), // 5 bps
        ..Default::default()
    };

    let engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    let _state = engine.state();
}

#[test]
fn test_min_quote_age_configuration() {
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig {
        min_quote_age_ms: 200, // 200ms minimum
        ..Default::default()
    };

    let engine = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    let _state = engine.state();
}

#[test]
fn test_queue_position_fraction() {
    // Test different queue positions
    let front = SimulatorConfig {
        queue_position_fraction: 0.0, // Front of queue
        ..Default::default()
    };
    let middle = SimulatorConfig {
        queue_position_fraction: 0.5, // Middle
        ..Default::default()
    };
    let back = SimulatorConfig {
        queue_position_fraction: 1.0, // Back of queue
        ..Default::default()
    };

    assert_eq!(front.queue_position_fraction, 0.0);
    assert_eq!(middle.queue_position_fraction, 0.5);
    assert_eq!(back.queue_position_fraction, 1.0);
}
