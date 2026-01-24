//! Backtest Head-to-Head Results Screen (T-3.4)
//!
//! Displays side-by-side comparison of two algorithm configurations.

use ratatui::{
    Frame,
    layout::Rect,
    widgets::{Block, Borders},
    style::Color,
};
use crossterm::event::KeyEvent;

use crate::commands::backtest::HeadToHeadResult;
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

/// View modes for the head-to-head results screen
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadToHeadViewMode {
    Summary,
    SideBySide,
    Relative,
}

impl HeadToHeadViewMode {
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

/// Backtest head-to-head results screen
pub struct BacktestHeadToHeadResultsScreen {
    result: HeadToHeadResult,
    view_mode: HeadToHeadViewMode,
    focused: bool,
}

impl BacktestHeadToHeadResultsScreen {
    /// Create a new head-to-head results screen
    pub fn new(result: HeadToHeadResult) -> Self {
        Self {
            result,
            view_mode: HeadToHeadViewMode::Summary,
            focused: true,
        }
    }

    /// Get the head-to-head result
    pub fn result(&self) -> &HeadToHeadResult {
        &self.result
    }

    /// Get the current view mode
    pub fn view_mode(&self) -> HeadToHeadViewMode {
        self.view_mode
    }

    /// Set the view mode
    pub fn set_view_mode(&mut self, mode: HeadToHeadViewMode) {
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
            HeadToHeadViewMode::Summary => self.render_summary(f, area),
            HeadToHeadViewMode::SideBySide => self.render_side_by_side(f, area),
            HeadToHeadViewMode::Relative => self.render_relative(f, area),
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
            Metric::new(
                &format!("{} Sharpe", self.result.config_a_metrics.algorithm_name),
                MetricValue::Number(self.result.config_a_metrics.sharpe_ratio)
            )
                .with_format(MetricFormat::Decimal(2)),
            Metric::new(
                &format!("{} Sharpe", self.result.config_b_metrics.algorithm_name),
                MetricValue::Number(self.result.config_b_metrics.sharpe_ratio)
            )
                .with_format(MetricFormat::Decimal(2)),
            Metric::new(
                &format!("{} Return", self.result.config_a_metrics.algorithm_name),
                MetricValue::Percentage(self.result.config_a_metrics.total_return)
            )
                .with_format(MetricFormat::Decimal(2)),
            Metric::new(
                &format!("{} Return", self.result.config_b_metrics.algorithm_name),
                MetricValue::Percentage(self.result.config_b_metrics.total_return)
            )
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
            TableHeader::new(self.result.config_a_metrics.algorithm_name.clone()).with_width(25),
            TableHeader::new(self.result.config_b_metrics.algorithm_name.clone()).with_width(25),
        ];

        // Create table rows
        let rows = vec![
            TableRow::new(vec![
                "Sharpe Ratio".to_string(),
                format!("{:.2}", self.result.config_a_metrics.sharpe_ratio),
                format!("{:.2}", self.result.config_b_metrics.sharpe_ratio),
            ]),
            TableRow::new(vec![
                "Total Return (%)".to_string(),
                format!("{:.2}", self.result.config_a_metrics.total_return),
                format!("{:.2}", self.result.config_b_metrics.total_return),
            ]),
            TableRow::new(vec![
                "Max Drawdown (%)".to_string(),
                format!("{:.2}", self.result.config_a_metrics.max_drawdown),
                format!("{:.2}", self.result.config_b_metrics.max_drawdown),
            ]),
            TableRow::new(vec![
                "Number of Trades".to_string(),
                self.result.config_a_metrics.num_trades.to_string(),
                self.result.config_b_metrics.num_trades.to_string(),
            ]),
            TableRow::new(vec![
                "Win Rate (%)".to_string(),
                format!("{:.1}", self.result.config_a_metrics.win_rate),
                format!("{:.1}", self.result.config_b_metrics.win_rate),
            ]),
            TableRow::new(vec![
                "Avg Trade PnL".to_string(),
                format!("{:.4}", self.result.config_a_metrics.avg_trade_pnl),
                format!("{:.4}", self.result.config_b_metrics.avg_trade_pnl),
            ]),
            TableRow::new(vec![
                "Annualized Return (%)".to_string(),
                format!("{:.2}", self.result.config_a_metrics.annualized_return),
                format!("{:.2}", self.result.config_b_metrics.annualized_return),
            ]),
            TableRow::new(vec![
                "Sortino Ratio".to_string(),
                format!("{:.2}", self.result.config_a_metrics.sortino_ratio),
                format!("{:.2}", self.result.config_b_metrics.sortino_ratio),
            ]),
            TableRow::new(vec![
                "Calmar Ratio".to_string(),
                format!("{:.2}", self.result.config_a_metrics.calmar_ratio),
                format!("{:.2}", self.result.config_b_metrics.calmar_ratio),
            ]),
            TableRow::new(vec![
                "Profit Factor".to_string(),
                format!("{:.2}", self.result.config_a_metrics.profit_factor),
                format!("{:.2}", self.result.config_b_metrics.profit_factor),
            ]),
        ];

        // Create table widget
        let title = format!(
            "{} vs {}",
            self.result.params.config_a.config_name,
            self.result.params.config_b.config_name
        );

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title(title));
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
    use crate::commands::params::backtest_params::{HeadToHeadParams, HeadToHeadConfig};
    use std::path::PathBuf;

    fn create_test_result() -> HeadToHeadResult {
        let config_a = HeadToHeadConfig {
            algorithm: "as".to_string(),
            config_name: "Config A".to_string(),
            weights_file: None,
            spread: 2.0,
            skew: 0.5,
        };

        let config_b = HeadToHeadConfig {
            algorithm: "as".to_string(),
            config_name: "Config B".to_string(),
            weights_file: None,
            spread: 3.0,
            skew: 0.7,
        };

        let config_a_metrics = CompareMetrics {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov (Config A)".to_string(),
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

        let config_b_metrics = CompareMetrics {
            algorithm: "as".to_string(),
            algorithm_name: "Avellaneda-Stoikov (Config B)".to_string(),
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
            winner: "as".to_string(),
            winner_name: "Avellaneda-Stoikov (Config A)".to_string(),
        };

        let mut params = HeadToHeadParams::default();
        params.config_a = config_a;
        params.config_b = config_b;

        HeadToHeadResult {
            config_a_metrics,
            config_b_metrics,
            relative_performance,
            params,
            events_processed: 50000,
            time_span_hours: 240.0,
        }
    }

    #[test]
    fn test_head_to_head_view_mode_all() {
        let modes = HeadToHeadViewMode::all();
        assert_eq!(modes.len(), 3);
        assert_eq!(modes[0], HeadToHeadViewMode::Summary);
        assert_eq!(modes[1], HeadToHeadViewMode::SideBySide);
        assert_eq!(modes[2], HeadToHeadViewMode::Relative);
    }

    #[test]
    fn test_head_to_head_view_mode_names() {
        assert_eq!(HeadToHeadViewMode::Summary.name(), "Summary");
        assert_eq!(HeadToHeadViewMode::SideBySide.name(), "Side-by-Side");
        assert_eq!(HeadToHeadViewMode::Relative.name(), "Relative Performance");
    }

    #[test]
    fn test_head_to_head_view_mode_navigation() {
        assert_eq!(HeadToHeadViewMode::Summary.next(), HeadToHeadViewMode::SideBySide);
        assert_eq!(HeadToHeadViewMode::SideBySide.next(), HeadToHeadViewMode::Relative);
        assert_eq!(HeadToHeadViewMode::Relative.next(), HeadToHeadViewMode::Summary);

        assert_eq!(HeadToHeadViewMode::Summary.previous(), HeadToHeadViewMode::Relative);
        assert_eq!(HeadToHeadViewMode::Relative.previous(), HeadToHeadViewMode::SideBySide);
        assert_eq!(HeadToHeadViewMode::SideBySide.previous(), HeadToHeadViewMode::Summary);
    }

    #[test]
    fn test_backtest_head_to_head_results_screen_new() {
        let result = create_test_result();
        let screen = BacktestHeadToHeadResultsScreen::new(result.clone());

        assert_eq!(screen.view_mode(), HeadToHeadViewMode::Summary);
        assert!(screen.is_focused());
        assert_eq!(screen.result().config_a_metrics.sharpe_ratio, 1.5);
        assert_eq!(screen.result().config_b_metrics.sharpe_ratio, 1.2);
    }

    #[test]
    fn test_backtest_head_to_head_results_screen_set_view_mode() {
        let result = create_test_result();
        let mut screen = BacktestHeadToHeadResultsScreen::new(result);

        screen.set_view_mode(HeadToHeadViewMode::SideBySide);
        assert_eq!(screen.view_mode(), HeadToHeadViewMode::SideBySide);

        screen.set_view_mode(HeadToHeadViewMode::Relative);
        assert_eq!(screen.view_mode(), HeadToHeadViewMode::Relative);
    }

    #[test]
    fn test_backtest_head_to_head_results_screen_set_focused() {
        let result = create_test_result();
        let mut screen = BacktestHeadToHeadResultsScreen::new(result);

        assert!(screen.is_focused());

        screen.set_focused(false);
        assert!(!screen.is_focused());

        screen.set_focused(true);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_backtest_head_to_head_results_screen_handle_key_tab() {
        let result = create_test_result();
        let mut screen = BacktestHeadToHeadResultsScreen::new(result);

        assert_eq!(screen.view_mode(), HeadToHeadViewMode::Summary);

        let handled = screen.handle_key(crossterm::event::KeyEvent::from(crossterm::event::KeyCode::Tab));
        assert!(handled);
        assert_eq!(screen.view_mode(), HeadToHeadViewMode::SideBySide);
    }

    #[test]
    fn test_backtest_head_to_head_results_screen_handle_key_backtab() {
        let result = create_test_result();
        let mut screen = BacktestHeadToHeadResultsScreen::new(result);

        assert_eq!(screen.view_mode(), HeadToHeadViewMode::Summary);

        let handled = screen.handle_key(crossterm::event::KeyEvent::from(crossterm::event::KeyCode::BackTab));
        assert!(handled);
        assert_eq!(screen.view_mode(), HeadToHeadViewMode::Relative);
    }

    #[test]
    fn test_backtest_head_to_head_results_screen_result_access() {
        let result = create_test_result();
        let screen = BacktestHeadToHeadResultsScreen::new(result.clone());

        assert_eq!(screen.result().config_a_metrics.algorithm, "as");
        assert_eq!(screen.result().config_b_metrics.algorithm, "as");
        assert_eq!(screen.result().events_processed, 50000);
        assert_eq!(screen.result().time_span_hours, 240.0);
    }

    #[test]
    fn test_head_to_head_result_configurations() {
        let result = create_test_result();

        // Verify configurations are different
        assert_ne!(result.params.config_a.spread, result.params.config_b.spread);
        assert_ne!(result.params.config_a.skew, result.params.config_b.skew);
        assert_eq!(result.params.config_a.config_name, "Config A");
        assert_eq!(result.params.config_b.config_name, "Config B");
    }

    #[test]
    fn test_head_to_head_relative_performance() {
        let result = create_test_result();

        // Verify relative performance calculations are sensible
        assert!(result.relative_performance.sharpe_diff > 0.0);
        assert!(result.relative_performance.sharpe_improvement_pct > 0.0);
        assert!(result.relative_performance.winner_name.contains("Config A"));
    }

    #[test]
    fn test_head_to_head_different_algorithms() {
        let mut result = create_test_result();

        // Change config B to use a different algorithm
        result.params.config_b.algorithm = "ml".to_string();
        result.config_b_metrics.algorithm = "ml".to_string();
        result.config_b_metrics.algorithm_name = "ML Spread/Skew (Config B)".to_string();

        // Verify we can compare different algorithms
        assert_ne!(result.config_a_metrics.algorithm, result.config_b_metrics.algorithm);
        assert!(result.config_a_metrics.algorithm_name.contains("Config A"));
        assert!(result.config_b_metrics.algorithm_name.contains("Config B"));
    }
}
