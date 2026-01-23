//! Backtest Compare Results Screen (T-3.3)
//!
//! Displays side-by-side comparison of ML algorithm vs Avellaneda-Stoikov baseline.

use ratatui::{
    Frame,
    layout::{Rect, Layout, Constraint, Direction},
    widgets::{Block, Borders},
    style::{Color, Style},
};
use crossterm::event::KeyEvent;

use crate::commands::backtest::CompareResult;
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

/// View modes for the compare results screen
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareViewMode {
    Summary,
    SideBySide,
    Relative,
}

impl CompareViewMode {
    /// Get all view modes
    pub fn all() -> Vec<Self> {
        vec![
            Self::Summary,
            Self::SideBySide,
            Self::Relative,
        ]
    }

    /// Get the display name for this view mode
    pub fn name(&self) -> &'static str {
        match self {
            Self::Summary => "Summary",
            Self::SideBySide => "Side-by-Side",
            Self::Relative => "Relative Performance",
        }
    }

    /// Get the next view mode
    pub fn next(&self) -> Self {
        let modes = Self::all();
        let current_idx = modes.iter().position(|m| m == self).unwrap_or(0);
        modes[(current_idx + 1) % modes.len()]
    }

    /// Get the previous view mode
    pub fn previous(&self) -> Self {
        let modes = Self::all();
        let current_idx = modes.iter().position(|m| m == self).unwrap_or(0);
        modes[(current_idx + modes.len() - 1) % modes.len()]
    }
}

/// Backtest compare results screen
pub struct BacktestCompareResultsScreen {
    result: CompareResult,
    view_mode: CompareViewMode,
    focused: bool,
}

impl BacktestCompareResultsScreen {
    /// Create a new compare results screen
    pub fn new(result: CompareResult) -> Self {
        Self {
            result,
            view_mode: CompareViewMode::Summary,
            focused: true,
        }
    }

    /// Get the compare result
    pub fn result(&self) -> &CompareResult {
        &self.result
    }

    /// Get the current view mode
    pub fn view_mode(&self) -> CompareViewMode {
        self.view_mode
    }

    /// Set the view mode
    pub fn set_view_mode(&mut self, mode: CompareViewMode) {
        self.view_mode = mode;
    }

    /// Check if the screen is focused
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Set the focused state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Handle key event
    /// Returns true if the event was handled
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            crossterm::event::KeyCode::Tab => {
                self.view_mode = self.view_mode.next();
                true
            }
            crossterm::event::KeyCode::BackTab => {
                self.view_mode = self.view_mode.previous();
                true
            }
            _ => false,
        }
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        match self.view_mode {
            CompareViewMode::Summary => self.render_summary(f, area),
            CompareViewMode::SideBySide => self.render_side_by_side(f, area),
            CompareViewMode::Relative => self.render_relative(f, area),
        }
    }

    /// Render the summary view
    fn render_summary(&self, f: &mut Frame, area: Rect) {
        let rel = &self.result.relative_performance;

        // Determine winner color
        let winner_color = if rel.sharpe_improvement_pct > 0.0 {
            Color::Green
        } else if rel.sharpe_improvement_pct < 0.0 {
            Color::Red
        } else {
            Color::Yellow
        };

        // Create metrics
        let metrics = vec![
            Metric::new("Winner", MetricValue::String(rel.winner_name.clone()))
                .with_color(winner_color),
            Metric::new("Sharpe Improvement", MetricValue::Percentage(rel.sharpe_improvement_pct))
                .with_format(MetricFormat::Decimal(1))
                .with_color(if rel.sharpe_improvement_pct > 0.0 { Color::Green } else { Color::Red }),
            Metric::new("Return Improvement", MetricValue::Percentage(rel.return_improvement_pct))
                .with_format(MetricFormat::Decimal(1))
                .with_color(if rel.return_improvement_pct > 0.0 { Color::Green } else { Color::Red }),
            Metric::new("ML Sharpe", MetricValue::Number(self.result.ml_metrics.sharpe_ratio))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("AS Sharpe", MetricValue::Number(self.result.as_metrics.sharpe_ratio))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("ML Return", MetricValue::Percentage(self.result.ml_metrics.total_return))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("AS Return", MetricValue::Percentage(self.result.as_metrics.total_return))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Events Processed", MetricValue::Integer(self.result.events_processed as i64)),
        ];

        // Create dashboard widget
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(metrics);

        dashboard.render(area, f.buffer_mut());
    }

    /// Render the side-by-side view
    fn render_side_by_side(&self, f: &mut Frame, area: Rect) {
        // Create table headers
        let headers = vec![
            TableHeader::new("Metric".to_string()).with_width(25),
            TableHeader::new(self.result.ml_metrics.algorithm_name.clone()).with_width(20),
            TableHeader::new(self.result.as_metrics.algorithm_name.clone()).with_width(20),
        ];

        // Create table rows
        let rows = vec![
            TableRow::new(vec![
                "Sharpe Ratio".to_string(),
                format!("{:.2}", self.result.ml_metrics.sharpe_ratio),
                format!("{:.2}", self.result.as_metrics.sharpe_ratio),
            ]),
            TableRow::new(vec![
                "Total Return (%)".to_string(),
                format!("{:.2}", self.result.ml_metrics.total_return),
                format!("{:.2}", self.result.as_metrics.total_return),
            ]),
            TableRow::new(vec![
                "Max Drawdown (%)".to_string(),
                format!("{:.2}", self.result.ml_metrics.max_drawdown),
                format!("{:.2}", self.result.as_metrics.max_drawdown),
            ]),
            TableRow::new(vec![
                "Number of Trades".to_string(),
                self.result.ml_metrics.num_trades.to_string(),
                self.result.as_metrics.num_trades.to_string(),
            ]),
            TableRow::new(vec![
                "Win Rate (%)".to_string(),
                format!("{:.1}", self.result.ml_metrics.win_rate),
                format!("{:.1}", self.result.as_metrics.win_rate),
            ]),
            TableRow::new(vec![
                "Avg Trade PnL".to_string(),
                format!("{:.4}", self.result.ml_metrics.avg_trade_pnl),
                format!("{:.4}", self.result.as_metrics.avg_trade_pnl),
            ]),
            TableRow::new(vec![
                "Annualized Return (%)".to_string(),
                format!("{:.2}", self.result.ml_metrics.annualized_return),
                format!("{:.2}", self.result.as_metrics.annualized_return),
            ]),
            TableRow::new(vec![
                "Sortino Ratio".to_string(),
                format!("{:.2}", self.result.ml_metrics.sortino_ratio),
                format!("{:.2}", self.result.as_metrics.sortino_ratio),
            ]),
            TableRow::new(vec![
                "Calmar Ratio".to_string(),
                format!("{:.2}", self.result.ml_metrics.calmar_ratio),
                format!("{:.2}", self.result.as_metrics.calmar_ratio),
            ]),
            TableRow::new(vec![
                "Profit Factor".to_string(),
                format!("{:.2}", self.result.ml_metrics.profit_factor),
                format!("{:.2}", self.result.as_metrics.profit_factor),
            ]),
        ];

        // Create table widget
        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title("ML vs AS Comparison"));
        table.set_focused(self.focused);

        table.render(area, f.buffer_mut());
    }

    /// Render the relative performance view
    fn render_relative(&self, f: &mut Frame, area: Rect) {
        let rel = &self.result.relative_performance;

        // Create metrics showing differences
        let metrics = vec![
            Metric::new("Winner", MetricValue::String(rel.winner_name.clone()))
                .with_color(Color::Green),
            Metric::new("Sharpe Difference", MetricValue::Number(rel.sharpe_diff))
                .with_format(MetricFormat::Decimal(3))
                .with_color(if rel.sharpe_diff > 0.0 { Color::Green } else { Color::Red }),
            Metric::new("Sharpe Improvement", MetricValue::Percentage(rel.sharpe_improvement_pct))
                .with_format(MetricFormat::Decimal(1))
                .with_color(if rel.sharpe_improvement_pct > 0.0 { Color::Green } else { Color::Red }),
            Metric::new("Return Difference", MetricValue::Percentage(rel.return_diff))
                .with_format(MetricFormat::Decimal(2))
                .with_color(if rel.return_diff > 0.0 { Color::Green } else { Color::Red }),
            Metric::new("Return Improvement", MetricValue::Percentage(rel.return_improvement_pct))
                .with_format(MetricFormat::Decimal(1))
                .with_color(if rel.return_improvement_pct > 0.0 { Color::Green } else { Color::Red }),
            Metric::new("Drawdown Difference", MetricValue::Percentage(rel.drawdown_diff))
                .with_format(MetricFormat::Decimal(2))
                .with_color(if rel.drawdown_diff < 0.0 { Color::Green } else { Color::Red }),
            Metric::new("Trade Count Difference", MetricValue::Integer(rel.trade_diff))
                .with_format(MetricFormat::Default),
            Metric::new("Time Span (hours)", MetricValue::Number(self.result.time_span_hours))
                .with_format(MetricFormat::Decimal(1)),
        ];

        // Create dashboard widget
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(metrics);

        dashboard.render(area, f.buffer_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::backtest::{CompareMetrics, RelativePerformance};
    use crate::commands::params::backtest_params::CompareParams;
    use std::path::PathBuf;

    fn create_test_result() -> CompareResult {
        let ml_metrics = CompareMetrics {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            sharpe_ratio: 1.5,
            total_return: 5.2,
            max_drawdown: -2.1,
            num_trades: 450,
            win_rate: 58.5,
            avg_trade_pnl: 0.0012,
            annualized_return: 12.5,
            sortino_ratio: 1.8,
            calmar_ratio: 0.9,
            profit_factor: 1.3,
        };

        let as_metrics = CompareMetrics {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            sharpe_ratio: 1.2,
            total_return: 4.1,
            max_drawdown: -2.5,
            num_trades: 420,
            win_rate: 55.0,
            avg_trade_pnl: 0.0010,
            annualized_return: 10.0,
            sortino_ratio: 1.5,
            calmar_ratio: 0.7,
            profit_factor: 1.2,
        };

        let relative_performance = RelativePerformance {
            sharpe_diff: 0.3,
            sharpe_improvement_pct: 25.0,
            return_diff: 1.1,
            return_improvement_pct: 26.8,
            drawdown_diff: 0.4,
            trade_diff: 30,
            winner: "ml".to_string(),
            winner_name: "ML Spread/Skew".to_string(),
        };

        CompareResult {
            ml_metrics,
            as_metrics,
            relative_performance,
            params: CompareParams::default(),
            events_processed: 50000,
            time_span_hours: 240.0,
        }
    }

    #[test]
    fn test_compare_view_mode_all() {
        let modes = CompareViewMode::all();
        assert_eq!(modes.len(), 3);
        assert_eq!(modes[0], CompareViewMode::Summary);
        assert_eq!(modes[1], CompareViewMode::SideBySide);
        assert_eq!(modes[2], CompareViewMode::Relative);
    }

    #[test]
    fn test_compare_view_mode_names() {
        assert_eq!(CompareViewMode::Summary.name(), "Summary");
        assert_eq!(CompareViewMode::SideBySide.name(), "Side-by-Side");
        assert_eq!(CompareViewMode::Relative.name(), "Relative Performance");
    }

    #[test]
    fn test_compare_view_mode_navigation() {
        assert_eq!(CompareViewMode::Summary.next(), CompareViewMode::SideBySide);
        assert_eq!(CompareViewMode::SideBySide.next(), CompareViewMode::Relative);
        assert_eq!(CompareViewMode::Relative.next(), CompareViewMode::Summary);

        assert_eq!(CompareViewMode::Summary.previous(), CompareViewMode::Relative);
        assert_eq!(CompareViewMode::Relative.previous(), CompareViewMode::SideBySide);
        assert_eq!(CompareViewMode::SideBySide.previous(), CompareViewMode::Summary);
    }

    #[test]
    fn test_backtest_compare_results_screen_new() {
        let result = create_test_result();
        let screen = BacktestCompareResultsScreen::new(result.clone());

        assert_eq!(screen.view_mode(), CompareViewMode::Summary);
        assert!(screen.is_focused());
        assert_eq!(screen.result().ml_metrics.sharpe_ratio, 1.5);
        assert_eq!(screen.result().as_metrics.sharpe_ratio, 1.2);
    }

    #[test]
    fn test_backtest_compare_results_screen_set_view_mode() {
        let result = create_test_result();
        let mut screen = BacktestCompareResultsScreen::new(result);

        screen.set_view_mode(CompareViewMode::SideBySide);
        assert_eq!(screen.view_mode(), CompareViewMode::SideBySide);

        screen.set_view_mode(CompareViewMode::Relative);
        assert_eq!(screen.view_mode(), CompareViewMode::Relative);
    }

    #[test]
    fn test_backtest_compare_results_screen_set_focused() {
        let result = create_test_result();
        let mut screen = BacktestCompareResultsScreen::new(result);

        assert!(screen.is_focused());

        screen.set_focused(false);
        assert!(!screen.is_focused());

        screen.set_focused(true);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_backtest_compare_results_screen_handle_key_tab() {
        let result = create_test_result();
        let mut screen = BacktestCompareResultsScreen::new(result);

        assert_eq!(screen.view_mode(), CompareViewMode::Summary);

        let handled = screen.handle_key(crossterm::event::KeyEvent::from(crossterm::event::KeyCode::Tab));
        assert!(handled);
        assert_eq!(screen.view_mode(), CompareViewMode::SideBySide);
    }

    #[test]
    fn test_backtest_compare_results_screen_handle_key_backtab() {
        let result = create_test_result();
        let mut screen = BacktestCompareResultsScreen::new(result);

        assert_eq!(screen.view_mode(), CompareViewMode::Summary);

        let handled = screen.handle_key(crossterm::event::KeyEvent::from(crossterm::event::KeyCode::BackTab));
        assert!(handled);
        assert_eq!(screen.view_mode(), CompareViewMode::Relative);
    }

    #[test]
    fn test_backtest_compare_results_screen_result_access() {
        let result = create_test_result();
        let screen = BacktestCompareResultsScreen::new(result.clone());

        assert_eq!(screen.result().ml_metrics.algorithm, "ml");
        assert_eq!(screen.result().as_metrics.algorithm, "as");
        assert_eq!(screen.result().relative_performance.winner, "ml");
        assert_eq!(screen.result().events_processed, 50000);
    }

    #[test]
    fn test_compare_result_relative_performance_calculations() {
        let result = create_test_result();

        // Verify relative performance calculations are sensible
        assert!(result.relative_performance.sharpe_diff > 0.0);
        assert!(result.relative_performance.sharpe_improvement_pct > 0.0);
        assert_eq!(result.relative_performance.winner, "ml");
    }
}
