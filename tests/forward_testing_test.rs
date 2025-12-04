//! Tests for the forward_testing module

use ingestor::forward_testing::{
    ForwardTestSession, ForwardTestConfig, SessionMetrics,
    ComparisonMetrics, ComparisonDiff,
};
use rust_decimal_macros::dec;

#[test]
fn test_forward_config_default() {
    let config = ForwardTestConfig::default();

    assert!(config.log_trades);
    assert!(!config.log_quotes);
    assert_eq!(config.sharpe_window, 100);
    assert!(config.session_name.is_none());
}

#[test]
fn test_session_creation() {
    let config = ForwardTestConfig::default();
    let session = ForwardTestSession::new(config);

    assert!(!session.is_active());
    assert_eq!(session.metrics().total_trades, 0);
}

#[test]
fn test_session_start() {
    let config = ForwardTestConfig::default();
    let mut session = ForwardTestSession::new(config);

    session.start();

    assert!(session.is_active());
    assert!(session.metrics().start_time.is_some());
}

#[test]
fn test_session_id_format() {
    let config = ForwardTestConfig::default();
    let session = ForwardTestSession::new(config);

    let id = session.session_id();
    // Should be in format YYYYMMDD_HHMMSS
    assert!(id.len() >= 15);
    assert!(id.contains('_'));
}

#[test]
fn test_session_metrics_default() {
    let metrics = SessionMetrics::default();

    assert_eq!(metrics.total_trades, 0);
    assert_eq!(metrics.buy_trades, 0);
    assert_eq!(metrics.sell_trades, 0);
    assert_eq!(metrics.net_pnl, dec!(0));
    assert_eq!(metrics.realized_pnl, dec!(0));
    assert_eq!(metrics.unrealized_pnl, dec!(0));
    assert_eq!(metrics.max_drawdown, 0.0);
    assert_eq!(metrics.win_rate, 0.0);
    assert_eq!(metrics.sharpe_ratio, 0.0);
}

#[test]
fn test_comparison_metrics_default() {
    let metrics = ComparisonMetrics::default();

    assert_eq!(metrics.sharpe_ratio, 0.0);
    assert_eq!(metrics.total_return_pct, 0.0);
    assert_eq!(metrics.max_drawdown_pct, 0.0);
    assert_eq!(metrics.win_rate, 0.0);
    assert_eq!(metrics.profit_factor, 0.0);
}

#[test]
fn test_comparison_diff_default() {
    let diff = ComparisonDiff::default();

    assert_eq!(diff.sharpe_diff, 0.0);
    assert_eq!(diff.return_diff_pct, 0.0);
    assert_eq!(diff.drawdown_diff_pct, 0.0);
    assert_eq!(diff.win_rate_diff, 0.0);
    assert_eq!(diff.fill_rate_diff, 0.0);
}

#[test]
fn test_forward_config_custom() {
    let config = ForwardTestConfig {
        log_dir: std::path::PathBuf::from("./test_sessions"),
        log_trades: true,
        log_quotes: true,
        sharpe_window: 50,
        session_name: Some("TestSession".to_string()),
    };

    assert!(config.log_quotes);
    assert_eq!(config.sharpe_window, 50);
    assert_eq!(config.session_name.as_ref().unwrap(), "TestSession");
}

#[test]
fn test_trades_list_empty() {
    let config = ForwardTestConfig::default();
    let session = ForwardTestSession::new(config);

    assert!(session.trades().is_empty());
}

#[test]
fn test_session_inactive_by_default() {
    let config = ForwardTestConfig::default();
    let session = ForwardTestSession::new(config);

    assert!(!session.is_active());
}
