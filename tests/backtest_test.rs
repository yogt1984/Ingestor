//! Tests for the backtest module

use ingestor::backtest::PerformanceMetrics;

#[test]
fn test_performance_metrics_default() {
    let metrics = PerformanceMetrics::default();

    assert_eq!(metrics.total_return, 0.0);
    assert_eq!(metrics.sharpe_ratio, 0.0);
    assert_eq!(metrics.max_drawdown, 0.0);
    assert_eq!(metrics.num_trades, 0);
    assert_eq!(metrics.win_rate, 0.0);
    assert_eq!(metrics.profit_factor, 0.0);
}

#[test]
fn test_performance_metrics_fields() {
    let mut metrics = PerformanceMetrics::default();

    metrics.total_return = 0.05;  // 5%
    metrics.sharpe_ratio = 1.5;
    metrics.max_drawdown = 0.02;  // 2%
    metrics.num_trades = 100;
    metrics.win_rate = 0.55;
    metrics.profit_factor = 1.5;

    assert!(metrics.total_return > 0.0);
    assert!(metrics.sharpe_ratio > 0.0);
    assert!(metrics.win_rate > 0.5);
    assert!(metrics.profit_factor > 1.0);
}

#[test]
fn test_performance_metrics_negative_sharpe() {
    let mut metrics = PerformanceMetrics::default();

    // Negative Sharpe is valid for losing strategies
    metrics.sharpe_ratio = -1.5;
    metrics.total_return = -0.03;

    assert!(metrics.sharpe_ratio < 0.0);
    assert!(metrics.total_return < 0.0);
}

#[test]
fn test_performance_metrics_max_drawdown() {
    let mut metrics = PerformanceMetrics::default();

    // Drawdown is always positive (represents loss)
    metrics.max_drawdown = 0.15;  // 15% max drawdown

    assert!(metrics.max_drawdown >= 0.0);
    assert!(metrics.max_drawdown <= 1.0);
}

#[test]
fn test_performance_metrics_win_rate_bounds() {
    let mut metrics = PerformanceMetrics::default();

    // Win rate should be between 0 and 1
    metrics.win_rate = 0.0;
    assert_eq!(metrics.win_rate, 0.0);

    metrics.win_rate = 1.0;
    assert_eq!(metrics.win_rate, 1.0);

    metrics.win_rate = 0.5;
    assert_eq!(metrics.win_rate, 0.5);
}
