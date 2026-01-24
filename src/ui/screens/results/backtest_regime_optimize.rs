//! Backtest Regime Optimize Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest regime_optimize command results.
//! Supports multiple view modes: Summary, Regimes, Comparison, Parameters.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{
    RegimeOptimizeResult, RegimeOptimizeMetrics, OptimalRegimeParams, StrategyComparison,
};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for regime optimize results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegimeOptimizeViewMode {
    /// Summary view with key metrics
    Summary,
    /// Regimes table view (all three regimes)
    Regimes,
    /// Comparison view (uniform vs regime-specific)
    Comparison,
    /// Parameters view (optimal parameters per regime)
    Parameters,
}

impl RegimeOptimizeViewMode {
    /// Get all view modes
    pub fn all() -> Vec<RegimeOptimizeViewMode> {
        vec![
            RegimeOptimizeViewMode::Summary,
            RegimeOptimizeViewMode::Regimes,
            RegimeOptimizeViewMode::Comparison,
            RegimeOptimizeViewMode::Parameters,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            RegimeOptimizeViewMode::Summary => "Summary",
            RegimeOptimizeViewMode::Regimes => "Regimes",
            RegimeOptimizeViewMode::Comparison => "Comparison",
            RegimeOptimizeViewMode::Parameters => "Parameters",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> RegimeOptimizeViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> RegimeOptimizeViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let prev_idx = if current_idx == 0 {
            all.len() - 1
        } else {
            current_idx - 1
        };
        all[prev_idx]
    }
}

/// Backtest regime optimize results screen
pub struct BacktestRegimeOptimizeResultsScreen {
    /// Regime optimize result data
    result: RegimeOptimizeResult,
    /// Current view mode
    view_mode: RegimeOptimizeViewMode,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestRegimeOptimizeResultsScreen {
    /// Create a new results screen from RegimeOptimizeResult
    pub fn new(result: RegimeOptimizeResult) -> Self {
        Self {
            result,
            view_mode: RegimeOptimizeViewMode::Summary,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &RegimeOptimizeResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> RegimeOptimizeViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: RegimeOptimizeViewMode) {
        self.view_mode = mode;
    }

    /// Check if focused
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Set focused state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Handle key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !self.focused {
            return false;
        }

        match key.code {
            KeyCode::Tab => {
                self.view_mode = self.view_mode.next();
                true
            }
            KeyCode::BackTab => {
                self.view_mode = self.view_mode.previous();
                true
            }
            KeyCode::Char('e') => {
                self.export_path = Some("export.json".to_string());
                true
            }
            _ => false,
        }
    }

    /// Create regimes table
    fn create_regimes_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Regime".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Events".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Event %".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Time (h)".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Spread".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Skew".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Quote".to_string()).with_width(8).with_sortable(false),
            TableHeader::new("Sharpe".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Return".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Drawdown".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Trades".to_string()).with_width(8).with_sortable(false),
            TableHeader::new("Win Rate".to_string()).with_width(10).with_sortable(false),
        ];

        let rows = vec![
            self.create_regime_row(&self.result.high_entropy),
            self.create_regime_row(&self.result.medium_entropy),
            self.create_regime_row(&self.result.low_entropy),
        ];

        (headers, rows)
    }

    /// Create a table row for a regime
    fn create_regime_row(&self, metrics: &RegimeOptimizeMetrics) -> TableRow {
        TableRow::new(vec![
            metrics.regime.clone(),
            format!("{}", metrics.event_count),
            format!("{:.2}%", metrics.event_fraction * 100.0),
            format!("{:.2}", metrics.time_hours),
            format!("{:.2}", metrics.optimal_spread),
            format!("{:.2}", metrics.optimal_skew),
            if metrics.should_quote { "Yes".to_string() } else { "No".to_string() },
            format!("{:.4}", metrics.best_sharpe),
            format!("{:.2}%", metrics.best_return * 100.0),
            format!("{:.2}%", metrics.best_drawdown * 100.0),
            format!("{}", metrics.best_trades),
            format!("{:.2}%", metrics.best_win_rate * 100.0),
        ])
    }

    /// Create comparison table
    fn create_comparison_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Metric".to_string()).with_width(20).with_sortable(false),
            TableHeader::new("Uniform".to_string()).with_width(15).with_sortable(false),
            TableHeader::new("Regime-Specific".to_string()).with_width(15).with_sortable(false),
            TableHeader::new("Improvement".to_string()).with_width(15).with_sortable(false),
        ];

        let comp = &self.result.comparison;
        let rows = vec![
            TableRow::new(vec![
                "Sharpe Ratio".to_string(),
                format!("{:.4}", comp.uniform_sharpe),
                format!("{:.4}", comp.regime_specific_sharpe),
                format!("{:.4}", comp.sharpe_improvement),
            ]),
            TableRow::new(vec![
                "Total Return".to_string(),
                format!("{:.2}%", comp.uniform_return * 100.0),
                format!("{:.2}%", comp.regime_specific_return * 100.0),
                format!("{:.2}%", comp.return_improvement * 100.0),
            ]),
            TableRow::new(vec![
                "Max Drawdown".to_string(),
                format!("{:.2}%", comp.uniform_drawdown * 100.0),
                format!("{:.2}%", comp.regime_specific_drawdown * 100.0),
                format!("{:.2}%", comp.drawdown_improvement * 100.0),
            ]),
            TableRow::new(vec![
                "Number of Trades".to_string(),
                format!("{}", comp.uniform_trades),
                format!("{}", comp.regime_specific_trades),
                format!("{}", comp.trade_count_diff),
            ]),
            TableRow::new(vec![
                "Win Rate".to_string(),
                format!("{:.2}%", comp.uniform_win_rate * 100.0),
                format!("{:.2}%", comp.regime_specific_win_rate * 100.0),
                format!("{:.2}%", (comp.regime_specific_win_rate - comp.uniform_win_rate) * 100.0),
            ]),
        ];

        (headers, rows)
    }

    /// Create parameters table
    fn create_parameters_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Regime".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Spread (bps)".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Skew Factor".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Should Quote".to_string()).with_width(12).with_sortable(false),
        ];

        let params = &self.result.optimal_regime_params;
        let rows = vec![
            TableRow::new(vec![
                "High".to_string(),
                format!("{:.2}", params.high.spread_bps),
                format!("{:.2}", params.high.skew_factor),
                if params.high.should_quote { "Yes".to_string() } else { "No".to_string() },
            ]),
            TableRow::new(vec![
                "Medium".to_string(),
                format!("{:.2}", params.medium.spread_bps),
                format!("{:.2}", params.medium.skew_factor),
                if params.medium.should_quote { "Yes".to_string() } else { "No".to_string() },
            ]),
            TableRow::new(vec![
                "Low".to_string(),
                format!("{:.2}", params.low.spread_bps),
                format!("{:.2}", params.low.skew_factor),
                if params.low.should_quote { "Yes".to_string() } else { "No".to_string() },
            ]),
        ];

        (headers, rows)
    }

    /// Export to JSON
    pub fn export_to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(&self.result)?)
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Tabs
                Constraint::Min(0),    // Content
            ])
            .split(area);

        // Render tabs
        let tab_titles: Vec<Line> = RegimeOptimizeViewMode::all()
            .iter()
            .map(|mode| {
                let style = if *mode == self.view_mode {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(mode.name(), style))
            })
            .collect();

        let tabs = Tabs::new(tab_titles)
            .block(Block::default().borders(Borders::ALL).title("View Mode"))
            .select(RegimeOptimizeViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            RegimeOptimizeViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            RegimeOptimizeViewMode::Regimes => {
                self.render_regimes(f, chunks[1]);
            }
            RegimeOptimizeViewMode::Comparison => {
                self.render_comparison(f, chunks[1]);
            }
            RegimeOptimizeViewMode::Parameters => {
                self.render_parameters(f, chunks[1]);
            }
        }
    }

    /// Render summary view
    fn render_summary(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(0),
            ])
            .split(area);

        // Overall metrics
        let metrics = vec![
            Metric::new("Total Events".to_string(), MetricValue::Number(self.result.total_events as f64)),
            Metric::new("Time Span (hours)".to_string(), MetricValue::Number(self.result.time_span_hours)),
            Metric::new("Regime-Specific Sharpe".to_string(), MetricValue::Number(self.result.comparison.regime_specific_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Uniform Sharpe".to_string(), MetricValue::Number(self.result.comparison.uniform_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Sharpe Improvement".to_string(), MetricValue::Number(self.result.comparison.sharpe_improvement)).with_format(MetricFormat::Decimal(4)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Best regime summary
        let best_regime = if self.result.high_entropy.best_sharpe >= self.result.medium_entropy.best_sharpe
            && self.result.high_entropy.best_sharpe >= self.result.low_entropy.best_sharpe {
            &self.result.high_entropy
        } else if self.result.medium_entropy.best_sharpe >= self.result.low_entropy.best_sharpe {
            &self.result.medium_entropy
        } else {
            &self.result.low_entropy
        };

        let details = vec![
            format!("Best Regime: {}", best_regime.regime),
            format!("Optimal Spread: {:.2} bps", best_regime.optimal_spread),
            format!("Optimal Skew: {:.2}", best_regime.optimal_skew),
            format!("Should Quote: {}", if best_regime.should_quote { "Yes" } else { "No" }),
            format!("Best Sharpe: {:.4}", best_regime.best_sharpe),
            format!("Best Return: {:.2}%", best_regime.best_return * 100.0),
            format!("Best Drawdown: {:.2}%", best_regime.best_drawdown * 100.0),
            format!("Best Trades: {}", best_regime.best_trades),
            format!("Best Win Rate: {:.2}%", best_regime.best_win_rate * 100.0),
        ];

        let text: Vec<Line> = details.iter().map(|s| Line::from(s.as_str())).collect();
        let paragraph = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Best Regime Summary"))
            .alignment(Alignment::Left);

        f.render_widget(paragraph, chunks[1]);
    }

    /// Render regimes view
    fn render_regimes(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_regimes_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render comparison view
    fn render_comparison(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_comparison_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render parameters view
    fn render_parameters(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_parameters_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_regime_optimize_result() -> RegimeOptimizeResult {
        RegimeOptimizeResult {
            algorithm: "AvellanedaStoikov".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            high_entropy: RegimeOptimizeMetrics {
                regime: "High".to_string(),
                event_count: 5000,
                event_fraction: 0.5,
                time_hours: 360.0,
                optimal_spread: 2.0,
                optimal_skew: 1.5,
                should_quote: true,
                best_sharpe: 2.5,
                best_return: 0.20,
                best_drawdown: -0.08,
                best_trades: 150,
                best_win_rate: 0.60,
            },
            medium_entropy: RegimeOptimizeMetrics {
                regime: "Medium".to_string(),
                event_count: 3000,
                event_fraction: 0.3,
                time_hours: 216.0,
                optimal_spread: 1.5,
                optimal_skew: 1.2,
                should_quote: true,
                best_sharpe: 2.0,
                best_return: 0.15,
                best_drawdown: -0.06,
                best_trades: 100,
                best_win_rate: 0.55,
            },
            low_entropy: RegimeOptimizeMetrics {
                regime: "Low".to_string(),
                event_count: 2000,
                event_fraction: 0.2,
                time_hours: 144.0,
                optimal_spread: 1.0,
                optimal_skew: 1.0,
                should_quote: false,
                best_sharpe: 1.5,
                best_return: 0.10,
                best_drawdown: -0.04,
                best_trades: 50,
                best_win_rate: 0.50,
            },
            optimal_regime_params: OptimalRegimeParams {
                high: crate::commands::backtest::RegimeParamSet {
                    spread_bps: 2.0,
                    skew_factor: 1.5,
                    should_quote: true,
                },
                medium: crate::commands::backtest::RegimeParamSet {
                    spread_bps: 1.5,
                    skew_factor: 1.2,
                    should_quote: true,
                },
                low: crate::commands::backtest::RegimeParamSet {
                    spread_bps: 1.0,
                    skew_factor: 1.0,
                    should_quote: false,
                },
            },
            comparison: StrategyComparison {
                uniform_sharpe: 1.8,
                uniform_return: 0.12,
                uniform_drawdown: -0.06,
                uniform_trades: 200,
                uniform_win_rate: 0.52,
                regime_specific_sharpe: 2.2,
                regime_specific_return: 0.18,
                regime_specific_drawdown: -0.07,
                regime_specific_trades: 300,
                regime_specific_win_rate: 0.58,
                sharpe_improvement: 0.4,
                return_improvement: 0.06,
                drawdown_improvement: -0.01,
                trade_count_diff: 100,
            },
            total_events: 10000,
            time_span_hours: 720.0,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = RegimeOptimizeViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(RegimeOptimizeViewMode::Summary.name(), "Summary");
        assert_eq!(RegimeOptimizeViewMode::Regimes.name(), "Regimes");
        assert_eq!(RegimeOptimizeViewMode::Comparison.name(), "Comparison");
        assert_eq!(RegimeOptimizeViewMode::Parameters.name(), "Parameters");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(RegimeOptimizeViewMode::Summary.next(), RegimeOptimizeViewMode::Regimes);
        assert_eq!(RegimeOptimizeViewMode::Regimes.next(), RegimeOptimizeViewMode::Comparison);
        assert_eq!(RegimeOptimizeViewMode::Comparison.next(), RegimeOptimizeViewMode::Parameters);
        assert_eq!(RegimeOptimizeViewMode::Parameters.next(), RegimeOptimizeViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        assert_eq!(screen.view_mode(), RegimeOptimizeViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_regime_optimize_result();
        let mut screen = BacktestRegimeOptimizeResultsScreen::new(result);
        screen.set_view_mode(RegimeOptimizeViewMode::Comparison);
        assert_eq!(screen.view_mode(), RegimeOptimizeViewMode::Comparison);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_regime_optimize_result();
        let mut screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), RegimeOptimizeViewMode::Regimes);
    }

    #[test]
    fn test_create_regimes_table() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let (headers, rows) = screen.create_regimes_table();
        assert_eq!(headers.len(), 12);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_create_comparison_table() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let (headers, rows) = screen.create_comparison_table();
        assert_eq!(headers.len(), 4);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_create_parameters_table() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let (headers, rows) = screen.create_parameters_table();
        assert_eq!(headers.len(), 4);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_create_regime_row() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result.clone());
        let row = screen.create_regime_row(&result.high_entropy);
        assert_eq!(row.cells().len(), 12);
        assert_eq!(row.cells()[0], "High");
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = RegimeOptimizeViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_regime_metrics_access() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        assert_eq!(screen.result().high_entropy.regime, "High");
        assert_eq!(screen.result().medium_entropy.regime, "Medium");
        assert_eq!(screen.result().low_entropy.regime, "Low");
    }

    #[test]
    fn test_comparison_metrics() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let comp = &screen.result().comparison;
        assert!(comp.sharpe_improvement > 0.0);
        assert!(comp.regime_specific_sharpe > comp.uniform_sharpe);
    }

    #[test]
    fn test_optimal_params_access() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result);
        let params = &screen.result().optimal_regime_params;
        assert_eq!(params.high.spread_bps, 2.0);
        assert_eq!(params.medium.spread_bps, 1.5);
        assert_eq!(params.low.spread_bps, 1.0);
    }

    #[test]
    fn test_should_quote_display() {
        let result = create_test_regime_optimize_result();
        let screen = BacktestRegimeOptimizeResultsScreen::new(result.clone());
        let row = screen.create_regime_row(&result.high_entropy);
        assert_eq!(row.cells()[6], "Yes");
        let row = screen.create_regime_row(&result.low_entropy);
        assert_eq!(row.cells()[6], "No");
    }
}
