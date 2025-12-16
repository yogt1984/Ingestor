//! Tests for the market_maker module

use ingestor::trading::market_maker::{
    MarketMakerEngine, MMConfig, MMState, Quote, QuoteSide, Fill, MarketRegime, RegimeThresholds,
    PnLTracker, RegimeParams, AvellanedaStoikovConfig, AvellanedaStoikovMM,
};
use rust_decimal_macros::dec;

#[test]
fn test_mm_config_default() {
    let config = MMConfig::default();

    assert_eq!(config.max_inventory, dec!(0.1));
    assert_eq!(config.quote_size, dec!(0.001));
    // Default regime params: high entropy spread = 1.0 bps, low entropy should_quote = false
    assert_eq!(config.regime_params.high_entropy.spread_bps, 1.0);
    assert!(!config.regime_params.low_entropy.should_quote);
}

#[test]
fn test_mm_engine_creation() {
    let config = MMConfig::default();
    let engine = MarketMakerEngine::new(config);
    let state = engine.get_state();

    assert_eq!(state.inventory, dec!(0));
    assert_eq!(state.pnl.total_pnl, dec!(0));
    assert_eq!(state.pnl.realized_pnl, dec!(0));
    assert_eq!(state.pnl.num_trades, 0);
}

#[test]
fn test_regime_thresholds_default() {
    let thresholds = RegimeThresholds::default();

    assert_eq!(thresholds.high_entropy_threshold, 0.7);
    assert_eq!(thresholds.low_entropy_threshold, 0.4);
}

#[test]
fn test_pnl_tracker_default() {
    let pnl = PnLTracker::default();

    assert_eq!(pnl.realized_pnl, dec!(0));
    assert_eq!(pnl.unrealized_pnl, dec!(0));
    assert_eq!(pnl.total_pnl, dec!(0));
    assert_eq!(pnl.fees_paid, dec!(0));
    assert_eq!(pnl.num_trades, 0);
    assert_eq!(pnl.total_volume, dec!(0));
}

#[test]
fn test_quote_creation() {
    let quote = Quote {
        price: dec!(50000),
        size: dec!(0.01),
        side: QuoteSide::Bid,
        timestamp_ms: 1000,
    };

    assert_eq!(quote.price, dec!(50000));
    assert_eq!(quote.size, dec!(0.01));
    assert_eq!(quote.timestamp_ms, 1000);
    assert!(matches!(quote.side, QuoteSide::Bid));
}

#[test]
fn test_fill_creation() {
    let fill = Fill {
        side: QuoteSide::Bid,
        price: dec!(50000),
        size: dec!(0.01),
        timestamp_ms: 1000,
    };

    assert_eq!(fill.price, dec!(50000));
    assert_eq!(fill.size, dec!(0.01));
    assert!(matches!(fill.side, QuoteSide::Bid));
}

#[test]
fn test_quote_side_variants() {
    let bid = QuoteSide::Bid;
    let ask = QuoteSide::Ask;

    assert!(matches!(bid, QuoteSide::Bid));
    assert!(matches!(ask, QuoteSide::Ask));
}

#[test]
fn test_market_regime_variants() {
    let high = MarketRegime::HighEntropy;
    let medium = MarketRegime::MediumEntropy;
    let low = MarketRegime::LowEntropy;

    assert!(matches!(high, MarketRegime::HighEntropy));
    assert!(matches!(medium, MarketRegime::MediumEntropy));
    assert!(matches!(low, MarketRegime::LowEntropy));
}

#[test]
fn test_mm_state_creation() {
    let state = MMState {
        inventory: dec!(0),
        avg_entry_price: dec!(0),
        pnl: PnLTracker::default(),
        current_bid: Some(Quote {
            price: dec!(49995),
            size: dec!(0.01),
            side: QuoteSide::Bid,
            timestamp_ms: 1000,
        }),
        current_ask: Some(Quote {
            price: dec!(50005),
            size: dec!(0.01),
            side: QuoteSide::Ask,
            timestamp_ms: 1000,
        }),
    };

    assert_eq!(state.inventory, dec!(0));
    assert!(state.current_bid.is_some());
    assert!(state.current_ask.is_some());
}

#[test]
fn test_mm_config_entropy_gate() {
    // Entropy gate is now controlled via regime_params.low_entropy.should_quote
    let mut regime_params = RegimeParams::default();
    regime_params.low_entropy.should_quote = false; // Gate = no quoting in low entropy

    let config = MMConfig {
        regime_params,
        ..Default::default()
    };

    assert!(!config.regime_params.low_entropy.should_quote);
}

#[test]
fn test_mm_config_custom_thresholds() {
    let config = MMConfig {
        regime_thresholds: RegimeThresholds {
            high_entropy_threshold: 0.8,
            low_entropy_threshold: 0.3,
        },
        ..Default::default()
    };

    assert_eq!(config.regime_thresholds.high_entropy_threshold, 0.8);
    assert_eq!(config.regime_thresholds.low_entropy_threshold, 0.3);
}

#[test]
fn test_mm_engine_get_state() {
    let config = MMConfig::default();
    let engine = MarketMakerEngine::new(config);

    let state = engine.get_state();
    assert_eq!(state.pnl.num_trades, 0);
    assert_eq!(state.pnl.total_volume, dec!(0));
}

#[test]
fn test_mm_engine_inventory() {
    let config = MMConfig::default();
    let engine = MarketMakerEngine::new(config);

    assert_eq!(engine.inventory(), dec!(0));
}

#[test]
fn test_mm_engine_pnl() {
    let config = MMConfig::default();
    let engine = MarketMakerEngine::new(config);

    let pnl = engine.pnl();
    assert_eq!(pnl.total_pnl, dec!(0));
}

#[test]
fn test_mm_engine_config() {
    let config = MMConfig {
        regime_params: RegimeParams::uniform(3.0, 0.5),
        ..Default::default()
    };
    let engine = MarketMakerEngine::new(config);

    assert_eq!(engine.config().regime_params.high_entropy.spread_bps, 3.0);
}

#[test]
fn test_mm_engine_reset() {
    let config = MMConfig::default();
    let mut engine = MarketMakerEngine::new(config);

    // Reset should work without error
    engine.reset();

    let state = engine.get_state();
    assert_eq!(state.inventory, dec!(0));
    assert_eq!(state.pnl.num_trades, 0);
}

#[test]
fn test_mm_config_spread_values() {
    let narrow = MMConfig {
        regime_params: RegimeParams::uniform(0.5, 0.5),
        ..Default::default()
    };
    let wide = MMConfig {
        regime_params: RegimeParams::uniform(10.0, 0.5),
        ..Default::default()
    };

    assert_eq!(narrow.regime_params.high_entropy.spread_bps, 0.5);
    assert_eq!(wide.regime_params.high_entropy.spread_bps, 10.0);
}

#[test]
fn test_mm_config_inventory_limit() {
    let small = MMConfig {
        max_inventory: dec!(0.01),
        ..Default::default()
    };
    let large = MMConfig {
        max_inventory: dec!(1.0),
        ..Default::default()
    };

    assert_eq!(small.max_inventory, dec!(0.01));
    assert_eq!(large.max_inventory, dec!(1.0));
}

#[test]
fn test_mm_config_skew_factor() {
    let low_skew = MMConfig {
        regime_params: RegimeParams::uniform(2.0, 0.1),
        ..Default::default()
    };
    let high_skew = MMConfig {
        regime_params: RegimeParams::uniform(2.0, 2.0),
        ..Default::default()
    };

    assert_eq!(low_skew.regime_params.high_entropy.skew_factor, 0.1);
    assert_eq!(high_skew.regime_params.high_entropy.skew_factor, 2.0);
}

#[test]
fn test_mm_engine_compute_quotes() {
    let config = MMConfig::default();
    let mut engine = MarketMakerEngine::new(config);

    let quotes = engine.compute_quotes(
        dec!(50000), // microprice
        dec!(50000), // mid_price
        0.001,       // volatility
        0.8,         // entropy_score (high)
        0.0,         // flow_imbalance
        1000,        // timestamp_ms
    );

    // Should have both bid and ask in high entropy
    assert!(quotes.bid.is_some());
    assert!(quotes.ask.is_some());
    assert_eq!(quotes.regime, MarketRegime::HighEntropy);

    // Bid should be below fair value, ask above
    let bid = quotes.bid.unwrap();
    let ask = quotes.ask.unwrap();
    assert!(bid.price < dec!(50000));
    assert!(ask.price > dec!(50000));
}

#[test]
fn test_mm_engine_process_fill() {
    let config = MMConfig::default();
    let mut engine = MarketMakerEngine::new(config);

    // Process a buy fill
    let fill = Fill {
        side: QuoteSide::Bid,
        price: dec!(50000),
        size: dec!(0.001),
        timestamp_ms: 1000,
    };
    engine.process_fill(fill, dec!(0.0001)); // 1 bps fee

    let state = engine.get_state();
    assert_eq!(state.inventory, dec!(0.001));
    assert_eq!(state.pnl.num_trades, 1);
}

#[test]
fn test_mm_engine_mark_to_market() {
    let config = MMConfig::default();
    let mut engine = MarketMakerEngine::new(config);

    // Buy some inventory
    let fill = Fill {
        side: QuoteSide::Bid,
        price: dec!(50000),
        size: dec!(0.001),
        timestamp_ms: 1000,
    };
    engine.process_fill(fill, dec!(0.0001));

    // Mark to market at higher price
    engine.update_mark_to_market(dec!(51000));

    let pnl = engine.pnl();
    // Unrealized PnL should be positive (bought at 50000, now at 51000)
    assert!(pnl.unrealized_pnl > dec!(0));
}

#[test]
fn test_regime_from_entropy_score() {
    let thresholds = RegimeThresholds::default();

    // High entropy (>= 0.7)
    assert_eq!(
        MarketRegime::from_entropy_score(0.8, &thresholds),
        MarketRegime::HighEntropy
    );
    assert_eq!(
        MarketRegime::from_entropy_score(0.7, &thresholds),
        MarketRegime::HighEntropy
    );

    // Medium entropy (>= 0.4 and < 0.7)
    assert_eq!(
        MarketRegime::from_entropy_score(0.5, &thresholds),
        MarketRegime::MediumEntropy
    );
    assert_eq!(
        MarketRegime::from_entropy_score(0.4, &thresholds),
        MarketRegime::MediumEntropy
    );

    // Low entropy (< 0.4)
    assert_eq!(
        MarketRegime::from_entropy_score(0.3, &thresholds),
        MarketRegime::LowEntropy
    );
    assert_eq!(
        MarketRegime::from_entropy_score(0.0, &thresholds),
        MarketRegime::LowEntropy
    );
}

// ============================================================================
// Tests for AvellanedaStoikov naming and RegimeParams
// ============================================================================

#[test]
fn test_avellaneda_stoikov_config_alias() {
    // AvellanedaStoikovConfig should be the same as MMConfig
    let config1 = AvellanedaStoikovConfig::default();
    let config2 = MMConfig::default();

    assert_eq!(config1.max_inventory, config2.max_inventory);
    assert_eq!(config1.quote_size, config2.quote_size);
}

#[test]
fn test_avellaneda_stoikov_mm_alias() {
    // AvellanedaStoikovMM should be the same as MarketMakerEngine
    let config = AvellanedaStoikovConfig::default();
    let engine = AvellanedaStoikovMM::new(config);

    assert_eq!(engine.inventory(), dec!(0));
}

#[test]
fn test_regime_params_uniform() {
    let params = RegimeParams::uniform(2.0, 0.5);

    // High entropy should have base spread
    assert_eq!(params.high_entropy.spread_bps, 2.0);
    assert_eq!(params.high_entropy.skew_factor, 0.5);
    assert!(params.high_entropy.should_quote);

    // Medium entropy should have 1.5x spread
    assert_eq!(params.medium_entropy.spread_bps, 3.0);
    assert!(params.medium_entropy.should_quote);

    // Low entropy should have 3x spread
    assert_eq!(params.low_entropy.spread_bps, 6.0);
    assert!(params.low_entropy.should_quote);
}

#[test]
fn test_regime_params_for_regime() {
    let params = RegimeParams::default();

    let high = params.for_regime(MarketRegime::HighEntropy);
    let medium = params.for_regime(MarketRegime::MediumEntropy);
    let low = params.for_regime(MarketRegime::LowEntropy);

    // High entropy should be most aggressive
    assert!(high.spread_bps < medium.spread_bps);
    assert!(medium.spread_bps < low.spread_bps);
}

#[test]
fn test_avellaneda_stoikov_with_regime_params() {
    let regime_params = RegimeParams {
        high_entropy: ingestor::trading::market_maker::RegimeConfig {
            spread_bps: 0.5,
            skew_factor: 0.2,
            size_mult: 1.0,
            should_quote: true,
        },
        medium_entropy: ingestor::trading::market_maker::RegimeConfig {
            spread_bps: 1.5,
            skew_factor: 0.4,
            size_mult: 0.8,
            should_quote: true,
        },
        low_entropy: ingestor::trading::market_maker::RegimeConfig {
            spread_bps: 3.0,
            skew_factor: 0.8,
            size_mult: 0.5,
            should_quote: false,
        },
    };

    let config = AvellanedaStoikovConfig::with_regime_params(regime_params);

    assert_eq!(config.regime_params.high_entropy.spread_bps, 0.5);
    assert_eq!(config.regime_params.low_entropy.spread_bps, 3.0);
    assert!(!config.regime_params.low_entropy.should_quote);
}

#[test]
fn test_avellaneda_stoikov_with_uniform_params() {
    let config = AvellanedaStoikovConfig::with_uniform_params(2.0, 0.5);

    // Should create uniform params with scaling
    assert_eq!(config.regime_params.high_entropy.spread_bps, 2.0);
    assert_eq!(config.regime_params.medium_entropy.spread_bps, 3.0);
    assert_eq!(config.regime_params.low_entropy.spread_bps, 6.0);
}
