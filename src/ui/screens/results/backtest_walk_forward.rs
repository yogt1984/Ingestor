//! Backtest Walk-Forward Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest walk-forward command results.
//! Supports multiple view modes: Summary, Folds, Aggregate, Parameters.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{
    WalkForwardResult, WalkForwardFoldResult, WalkForwardAggregate,
    WalkForwardOptimizedParams, WalkForwardFoldMetrics,
};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for walk-forward results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkForwardViewMode {
    /// Summary view with key metrics
    Summary,
    /// Folds table view (all folds)
    Folds,
    /// Aggregate view (aggregated metrics)
    Aggregate,
    /// Parameters view (optimized parameters across folds)
    Parameters,
}

impl WalkForwardViewMode {
    /// Get all view modes
    pub fn all() -> Vec<WalkForwardViewMode> {
        vec![
            WalkForwardViewMode::Summary,
            WalkForwardViewMode::Folds,
            WalkForwardViewMode::Aggregate,
            WalkForwardViewMode::Parameters,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            WalkForwardViewMode::Summary => "Summary",
            WalkForwardViewMode::Folds => "Folds",
            WalkForwardViewMode::Aggregate => "Aggregate",
            WalkForwardViewMode::Parameters => "Parameters",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> WalkForwardViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> WalkForwardViewMode {
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

/// Backtest walk-forward results screen
pub struct BacktestWalkForwardResultsScreen {
    /// Walk-forward result data
    result: WalkForwardResult,
    /// Current view mode
    view_mode: WalkForwardViewMode,
    /// Selected fold index (for Folds view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestWalkForwardResultsScreen {
    /// Create a new results screen from WalkForwardResult
    pub fn new(result: WalkForwardResult) -> Self {
        Self {
            result,
            view_mode: WalkForwardViewMode::Summary,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &WalkForwardResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> WalkForwardViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: WalkForwardViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.result.fold_results.len() {
                self.selected_index = Some(idx);
            } else {
                self.selected_index = None;
            }
        } else {
            self.selected_index = None;
        }
    }

    /// Get selected fold
    pub fn selected_fold(&self) -> Option<&WalkForwardFoldResult> {
        self.selected_index
            .and_then(|idx| self.result.fold_results.get(idx))
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
            KeyCode::Down | KeyCode::Char('j') => {
                if self.view_mode == WalkForwardViewMode::Folds {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.fold_results.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == WalkForwardViewMode::Folds {
                    if let Some(idx) = self.selected_index {
                        if idx > 0 {
                            self.set_selected_index(Some(idx - 1));
                        }
                    }
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Create folds table
    fn create_folds_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Fold".to_string()).with_width(6).with_sortable(false),
            TableHeader::new("Train Sharpe".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Test Sharpe".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Train Return".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Test Return".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Train Trades".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Test Trades".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Train Win Rate".to_string()).with_width(14).with_sortable(false),
            TableHeader::new("Test Win Rate".to_string()).with_width(14).with_sortable(false),
        ];

        let rows: Vec<TableRow> = self.result.fold_results
            .iter()
            .map(|fold| {
                TableRow::new(vec![
                    format!("{}", fold.fold_num),
                    format!("{:.4}", fold.train_metrics.sharpe),
                    format!("{:.4}", fold.test_metrics.sharpe),
                    format!("{:.2}%", fold.train_metrics.total_return * 100.0),
                    format!("{:.2}%", fold.test_metrics.total_return * 100.0),
                    format!("{}", fold.train_metrics.num_trades),
                    format!("{}", fold.test_metrics.num_trades),
                    format!("{:.2}%", fold.train_metrics.win_rate * 100.0),
                    format!("{:.2}%", fold.test_metrics.win_rate * 100.0),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create aggregate table
    fn create_aggregate_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Metric".to_string()).with_width(25).with_sortable(false),
            TableHeader::new("Value".to_string()).with_width(20).with_sortable(false),
        ];

        let agg = &self.result.aggregate;
        let rows = vec![
            TableRow::new(vec![
                "Avg OOS Sharpe".to_string(),
                format!("{:.4}", agg.avg_oos_sharpe),
            ]),
            TableRow::new(vec![
                "Std OOS Sharpe".to_string(),
                format!("{:.4}", agg.std_oos_sharpe),
            ]),
            TableRow::new(vec![
                "Avg OOS Return".to_string(),
                format!("{:.2}%", agg.avg_oos_return * 100.0),
            ]),
            TableRow::new(vec![
                "Total OOS Trades".to_string(),
                format!("{}", agg.total_oos_trades),
            ]),
            TableRow::new(vec![
                "Avg Win Rate".to_string(),
                format!("{:.2}%", agg.avg_win_rate * 100.0),
            ]),
            TableRow::new(vec![
                "% Profitable Folds".to_string(),
                format!("{:.2}%", agg.pct_profitable_folds * 100.0),
            ]),
            TableRow::new(vec![
                "IS/OOS Sharpe Ratio".to_string(),
                format!("{:.4}", agg.is_oos_sharpe_ratio),
            ]),
            TableRow::new(vec![
                "Prob Sharpe > 0".to_string(),
                format!("{:.4}", agg.prob_sharpe_gt_zero),
            ]),
        ];

        (headers, rows)
    }

    /// Create parameters table
    fn create_parameters_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Fold".to_string()).with_width(6).with_sortable(false),
            TableHeader::new("Spread".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Skew".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Fill Prob".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Train Sharpe".to_string()).with_width(14).with_sortable(false),
        ];

        let rows: Vec<TableRow> = self.result.fold_results
            .iter()
            .map(|fold| {
                let params = &fold.best_params;
                TableRow::new(vec![
                    format!("{}", fold.fold_num),
                    format!("{:.4}", params.spread),
                    format!("{:.4}", params.skew),
                    format!("{:.4}", params.fill_prob),
                    format!("{:.4}", params.train_sharpe),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create Sharpe chart series
    fn create_sharpe_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.result.fold_results
            .iter()
            .map(|fold| {
                let x = fold.fold_num as f64;
                let y = fold.test_metrics.sharpe;
                let label = format!("{:.4}", fold.test_metrics.sharpe);
                DataPoint::new(x, y).with_label(label)
            })
            .collect();

        DataSeries::new("OOS Sharpe".to_string())
            .with_points(points)
            .with_color(Color::Green)
    }

    /// Create return chart series
    fn create_return_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.result.fold_results
            .iter()
            .map(|fold| {
                let x = fold.fold_num as f64;
                let y = fold.test_metrics.total_return * 100.0;
                let label = format!("{:.2}%", fold.test_metrics.total_return * 100.0);
                DataPoint::new(x, y).with_label(label)
            })
            .collect();

        DataSeries::new("OOS Return %".to_string())
            .with_points(points)
            .with_color(Color::Cyan)
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
        let tab_titles: Vec<Line> = WalkForwardViewMode::all()
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
            .select(WalkForwardViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            WalkForwardViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            WalkForwardViewMode::Folds => {
                self.render_folds(f, chunks[1]);
            }
            WalkForwardViewMode::Aggregate => {
                self.render_aggregate(f, chunks[1]);
            }
            WalkForwardViewMode::Parameters => {
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

        // Key metrics
        let agg = &self.result.aggregate;
        let metrics = vec![
            Metric::new("Folds".to_string(), MetricValue::Number(self.result.folds as f64)),
            Metric::new("Avg OOS Sharpe".to_string(), MetricValue::Number(agg.avg_oos_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Std OOS Sharpe".to_string(), MetricValue::Number(agg.std_oos_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Prob Sharpe > 0".to_string(), MetricValue::Number(agg.prob_sharpe_gt_zero)).with_format(MetricFormat::Decimal(4)),
            Metric::new("% Profitable Folds".to_string(), MetricValue::Number(agg.pct_profitable_folds * 100.0)).with_format(MetricFormat::Decimal(2)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Chart showing Sharpe across folds
        let series = self.create_sharpe_series();
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Line)
            .with_series(vec![series])
            .with_x_axis(AxisConfig::default().with_label("Fold Number"))
            .with_y_axis(AxisConfig::default().with_label("OOS Sharpe"));

        chart.render(area, f.buffer_mut());
    }

    /// Render folds view
    fn render_folds(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_folds_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render aggregate view
    fn render_aggregate(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_aggregate_table();

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

    fn create_test_walk_forward_result() -> WalkForwardResult {
        let mut fold_results = Vec::new();
        for i in 0..5 {
            fold_results.push(WalkForwardFoldResult {
                fold_num: i + 1,
                train_start_ms: 1000 + i * 1000,
                train_end_ms: 2000 + i * 1000,
                test_start_ms: 2000 + i * 1000,
                test_end_ms: 3000 + i * 1000,
                best_params: WalkForwardOptimizedParams {
                    spread: 0.001 + i as f64 * 0.0001,
                    skew: 0.002 + i as f64 * 0.0001,
                    fill_prob: 0.8 + i as f64 * 0.01,
                    train_sharpe: 2.0 - i as f64 * 0.1,
                },
                train_metrics: WalkForwardFoldMetrics {
                    sharpe: 2.0 - i as f64 * 0.1,
                    total_return: 0.15 - i as f64 * 0.01,
                    max_drawdown: 0.05 + i as f64 * 0.01,
                    num_trades: 200 - i * 10,
                    win_rate: 0.55 - i as f64 * 0.01,
                    profit_factor: 1.5 - i as f64 * 0.05,
                },
                test_metrics: WalkForwardFoldMetrics {
                    sharpe: 1.8 - i as f64 * 0.1,
                    total_return: 0.12 - i as f64 * 0.01,
                    max_drawdown: 0.06 + i as f64 * 0.01,
                    num_trades: 150 - i * 10,
                    win_rate: 0.52 - i as f64 * 0.01,
                    profit_factor: 1.4 - i as f64 * 0.05,
                },
            });
        }

        WalkForwardResult {
            algorithm: "mm".to_string(),
            algorithm_name: "Market Making".to_string(),
            folds: 5,
            fold_results,
            aggregate: WalkForwardAggregate {
                avg_oos_sharpe: 1.5,
                std_oos_sharpe: 0.25,
                avg_oos_return: 0.10,
                total_oos_trades: 600,
                avg_win_rate: 0.52,
                pct_profitable_folds: 0.8,
                is_oos_sharpe_ratio: 1.33,
                prob_sharpe_gt_zero: 0.95,
            },
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = WalkForwardViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(WalkForwardViewMode::Summary.name(), "Summary");
        assert_eq!(WalkForwardViewMode::Folds.name(), "Folds");
        assert_eq!(WalkForwardViewMode::Aggregate.name(), "Aggregate");
        assert_eq!(WalkForwardViewMode::Parameters.name(), "Parameters");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(WalkForwardViewMode::Summary.next(), WalkForwardViewMode::Folds);
        assert_eq!(WalkForwardViewMode::Folds.next(), WalkForwardViewMode::Aggregate);
        assert_eq!(WalkForwardViewMode::Aggregate.next(), WalkForwardViewMode::Parameters);
        assert_eq!(WalkForwardViewMode::Parameters.next(), WalkForwardViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        assert_eq!(screen.view_mode(), WalkForwardViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_walk_forward_result();
        let mut screen = BacktestWalkForwardResultsScreen::new(result);
        screen.set_view_mode(WalkForwardViewMode::Folds);
        assert_eq!(screen.view_mode(), WalkForwardViewMode::Folds);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_walk_forward_result();
        let mut screen = BacktestWalkForwardResultsScreen::new(result);
        screen.set_selected_index(Some(2));
        assert_eq!(screen.selected_index(), Some(2));
    }

    #[test]
    fn test_selected_fold() {
        let result = create_test_walk_forward_result();
        let mut screen = BacktestWalkForwardResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_fold().is_some());
    }

    #[test]
    fn test_export_json() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_walk_forward_result();
        let mut screen = BacktestWalkForwardResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), WalkForwardViewMode::Folds);
    }

    #[test]
    fn test_create_folds_table() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let (headers, rows) = screen.create_folds_table();
        assert_eq!(headers.len(), 9);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_create_aggregate_table() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let (headers, rows) = screen.create_aggregate_table();
        assert_eq!(headers.len(), 2);
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn test_create_parameters_table() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let (headers, rows) = screen.create_parameters_table();
        assert_eq!(headers.len(), 5);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_create_sharpe_series() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let series = screen.create_sharpe_series();
        assert_eq!(series.points.len(), 5);
    }

    #[test]
    fn test_create_return_series() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let series = screen.create_return_series();
        assert_eq!(series.points.len(), 5);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = WalkForwardViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_fold_results_access() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        assert_eq!(screen.result().fold_results.len(), 5);
        assert_eq!(screen.result().folds, 5);
    }

    #[test]
    fn test_aggregate_metrics() {
        let result = create_test_walk_forward_result();
        let screen = BacktestWalkForwardResultsScreen::new(result);
        let agg = &screen.result().aggregate;
        assert!(agg.avg_oos_sharpe > 0.0);
        assert!(agg.prob_sharpe_gt_zero > 0.0);
    }
}
