//! Tests for the market_maker module

use ingestor::market_maker::{
    MarketMakerEngine, MMConfig, MMState, Quote, QuoteSide, Fill, MarketRegime, RegimeThresholds,
    PnLTracker,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn test_mm_config_default() {
    let config = MMConfig::default();

    assert_eq!(config.base_spread_bps, 2.0);
    assert_eq!(config.inventory_skew_factor, 0.5);
    assert_eq!(config.max_inventory, dec!(0.1));
    assert_eq!(config.quote_size, dec!(0.001));
    assert!(!config.pull_quotes_in_low_entropy);
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
    let config = MMConfig {
        pull_quotes_in_low_entropy: true,
        ..Default::default()
    };

    assert!(config.pull_quotes_in_low_entropy);
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
        base_spread_bps: 3.0,
        ..Default::default()
    };
    let engine = MarketMakerEngine::new(config);

    assert_eq!(engine.config().base_spread_bps, 3.0);
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
        base_spread_bps: 0.5,
        ..Default::default()
    };
    let wide = MMConfig {
        base_spread_bps: 10.0,
        ..Default::default()
    };

    assert_eq!(narrow.base_spread_bps, 0.5);
    assert_eq!(wide.base_spread_bps, 10.0);
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
        inventory_skew_factor: 0.1,
        ..Default::default()
    };
    let high_skew = MMConfig {
        inventory_skew_factor: 2.0,
        ..Default::default()
    };

    assert_eq!(low_skew.inventory_skew_factor, 0.1);
    assert_eq!(high_skew.inventory_skew_factor, 2.0);
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
