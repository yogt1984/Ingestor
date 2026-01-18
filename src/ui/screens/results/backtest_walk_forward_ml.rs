//! Backtest Walk-Forward ML Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest walk-forward-ml command results.
//! Supports multiple view modes: Summary, Folds, Aggregate, Weights.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{
    WalkForwardMLResult, WalkForwardMLFoldResult, WalkForwardMLAggregate, WeightStability,
};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for walk-forward ML results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkForwardMLViewMode {
    /// Summary view with key metrics
    Summary,
    /// Folds table view (all folds)
    Folds,
    /// Aggregate view (aggregated metrics)
    Aggregate,
    /// Weights view (consensus weights)
    Weights,
}

impl WalkForwardMLViewMode {
    /// Get all view modes
    pub fn all() -> Vec<WalkForwardMLViewMode> {
        vec![
            WalkForwardMLViewMode::Summary,
            WalkForwardMLViewMode::Folds,
            WalkForwardMLViewMode::Aggregate,
            WalkForwardMLViewMode::Weights,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            WalkForwardMLViewMode::Summary => "Summary",
            WalkForwardMLViewMode::Folds => "Folds",
            WalkForwardMLViewMode::Aggregate => "Aggregate",
            WalkForwardMLViewMode::Weights => "Weights",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> WalkForwardMLViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> WalkForwardMLViewMode {
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

/// Backtest walk-forward ML results screen
pub struct BacktestWalkForwardMLResultsScreen {
    /// Walk-forward ML result data
    result: WalkForwardMLResult,
    /// Current view mode
    view_mode: WalkForwardMLViewMode,
    /// Selected fold index (for Folds view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestWalkForwardMLResultsScreen {
    /// Create a new results screen from WalkForwardMLResult
    pub fn new(result: WalkForwardMLResult) -> Self {
        Self {
            result,
            view_mode: WalkForwardMLViewMode::Summary,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &WalkForwardMLResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> WalkForwardMLViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: WalkForwardMLViewMode) {
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
    pub fn selected_fold(&self) -> Option<&WalkForwardMLFoldResult> {
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
                if self.view_mode == WalkForwardMLViewMode::Folds {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.fold_results.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == WalkForwardMLViewMode::Folds {
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
            TableHeader::new("Train Events".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Test Events".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Train Sharpe".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Test Sharpe".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Train Return".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Test Return".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Gap".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Configs".to_string()).with_width(10).with_sortable(false),
        ];

        let rows: Vec<TableRow> = self.result.fold_results
            .iter()
            .map(|fold| {
                TableRow::new(vec![
                    format!("{}", fold.fold_num),
                    format!("{}", fold.train_events),
                    format!("{}", fold.test_events),
                    format!("{:.4}", fold.train_sharpe),
                    format!("{:.4}", fold.test_sharpe),
                    format!("{:.2}%", fold.train_return * 100.0),
                    format!("{:.2}%", fold.test_return * 100.0),
                    format!("{:.4}", fold.generalization_gap),
                    format!("{}/{}", fold.valid_configs, fold.configs_evaluated),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create aggregate metrics table
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
                "Avg Generalization Gap".to_string(),
                format!("{:.4}", agg.avg_generalization_gap),
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
            TableRow::new(vec![
                "Weight Stability Score".to_string(),
                format!("{:.4}", agg.weight_stability.stability_score),
            ]),
        ];

        (headers, rows)
    }

    /// Create weights table
    fn create_weights_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Weight Type".to_string()).with_width(20).with_sortable(false),
            TableHeader::new("Intercept".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Entropy".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Volatility".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Imbalance".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Interaction/Inventory".to_string()).with_width(20).with_sortable(false),
        ];

        let weights = &self.result.consensus_weights;
        let rows = vec![
            TableRow::new(vec![
                "Spread Weights".to_string(),
                format!("{:.4}", weights.spread.intercept),
                format!("{:.4}", weights.spread.w_entropy),
                format!("{:.4}", weights.spread.w_volatility),
                format!("{:.4}", weights.spread.w_imbalance),
                format!("{:.4}", weights.spread.w_interaction),
            ]),
            TableRow::new(vec![
                "Skew Weights".to_string(),
                format!("{:.4}", weights.skew.intercept),
                format!("{:.4}", weights.skew.w_entropy),
                format!("{:.4}", weights.skew.w_volatility),
                format!("{:.4}", weights.skew.w_imbalance),
                format!("{:.4}", weights.skew.w_inventory),
            ]),
        ];

        (headers, rows)
    }

    /// Create Sharpe chart series
    fn create_sharpe_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.result.fold_results
            .iter()
            .map(|fold| {
                let x = fold.fold_num as f64;
                let y = fold.test_sharpe;
                let label = format!("{:.4}", fold.test_sharpe);
                DataPoint::new(x, y).with_label(label)
            })
            .collect();

        DataSeries::new("OOS Sharpe".to_string())
            .with_points(points)
            .with_color(Color::Green)
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
        let tab_titles: Vec<Line> = WalkForwardMLViewMode::all()
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
            .select(WalkForwardMLViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            WalkForwardMLViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            WalkForwardMLViewMode::Folds => {
                self.render_folds(f, chunks[1]);
            }
            WalkForwardMLViewMode::Aggregate => {
                self.render_aggregate(f, chunks[1]);
            }
            WalkForwardMLViewMode::Weights => {
                self.render_weights(f, chunks[1]);
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

    /// Render weights view
    fn render_weights(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(0),
            ])
            .split(area);

        // Consensus weights table
        let (headers, rows) = self.create_weights_table();
        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(chunks[0], f.buffer_mut());

        // Weight stability metrics
        let stability = &self.result.aggregate.weight_stability;
        let stability_info = vec![
            format!("Stability Score: {:.4}", stability.stability_score),
            format!("Spread Intercept Std: {:.4}", stability.spread_intercept_std),
            format!("Spread Entropy Std: {:.4}", stability.spread_entropy_std),
            format!("Spread Volatility Std: {:.4}", stability.spread_volatility_std),
            format!("Skew Intercept Std: {:.4}", stability.skew_intercept_std),
            format!("Skew Inventory Std: {:.4}", stability.skew_inventory_std),
        ];

        let text: Vec<Line> = stability_info.iter().map(|s| Line::from(s.as_str())).collect();
        let paragraph = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Weight Stability"))
            .alignment(Alignment::Left);

        f.render_widget(paragraph, chunks[1]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use crate::strategies::{MLModelWeights, SpreadWeights, SkewWeights};

    fn create_test_walk_forward_ml_result() -> WalkForwardMLResult {
        let mut fold_results = Vec::new();
        for i in 0..5 {
            fold_results.push(WalkForwardMLFoldResult {
                fold_num: i + 1,
                train_start_ms: 1000 + i * 1000,
                train_end_ms: 2000 + i * 1000,
                test_start_ms: 2000 + i * 1000,
                test_end_ms: 3000 + i * 1000,
                train_events: 5000 + i * 1000,
                test_events: 2000 + i * 500,
                best_weights: MLModelWeights {
                    spread: SpreadWeights::default(),
                    skew: SkewWeights::default(),
                    version: "1.0".to_string(),
                    training_info: None,
                },
                train_sharpe: 2.0 - i as f64 * 0.1,
                train_return: 0.15 - i as f64 * 0.01,
                train_trades: 200 - i * 10,
                test_sharpe: 1.8 - i as f64 * 0.1,
                test_return: 0.12 - i as f64 * 0.01,
                test_trades: 150 - i * 10,
                generalization_gap: 0.2,
                configs_evaluated: 100,
                valid_configs: 80,
            });
        }

        WalkForwardMLResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            folds: 5,
            fold_results,
            aggregate: WalkForwardMLAggregate {
                avg_oos_sharpe: 1.5,
                std_oos_sharpe: 0.25,
                avg_oos_return: 0.10,
                total_oos_trades: 600,
                avg_generalization_gap: 0.2,
                pct_profitable_folds: 0.8,
                is_oos_sharpe_ratio: 1.33,
                prob_sharpe_gt_zero: 0.95,
                weight_stability: WeightStability {
                    spread_intercept_std: 0.1,
                    spread_entropy_std: 0.05,
                    spread_volatility_std: 10.0,
                    skew_intercept_std: 0.02,
                    skew_inventory_std: 0.03,
                    stability_score: 0.85,
                },
            },
            consensus_weights: MLModelWeights {
                spread: SpreadWeights::default(),
                skew: SkewWeights::default(),
                version: "1.0".to_string(),
                training_info: None,
            },
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = WalkForwardMLViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(WalkForwardMLViewMode::Summary.name(), "Summary");
        assert_eq!(WalkForwardMLViewMode::Folds.name(), "Folds");
        assert_eq!(WalkForwardMLViewMode::Aggregate.name(), "Aggregate");
        assert_eq!(WalkForwardMLViewMode::Weights.name(), "Weights");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(WalkForwardMLViewMode::Summary.next(), WalkForwardMLViewMode::Folds);
        assert_eq!(WalkForwardMLViewMode::Folds.next(), WalkForwardMLViewMode::Aggregate);
        assert_eq!(WalkForwardMLViewMode::Aggregate.next(), WalkForwardMLViewMode::Weights);
        assert_eq!(WalkForwardMLViewMode::Weights.next(), WalkForwardMLViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        assert_eq!(screen.view_mode(), WalkForwardMLViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_walk_forward_ml_result();
        let mut screen = BacktestWalkForwardMLResultsScreen::new(result);
        screen.set_view_mode(WalkForwardMLViewMode::Folds);
        assert_eq!(screen.view_mode(), WalkForwardMLViewMode::Folds);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_walk_forward_ml_result();
        let mut screen = BacktestWalkForwardMLResultsScreen::new(result);
        screen.set_selected_index(Some(2));
        assert_eq!(screen.selected_index(), Some(2));
    }

    #[test]
    fn test_selected_fold() {
        let result = create_test_walk_forward_ml_result();
        let mut screen = BacktestWalkForwardMLResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_fold().is_some());
    }

    #[test]
    fn test_export_json() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_walk_forward_ml_result();
        let mut screen = BacktestWalkForwardMLResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), WalkForwardMLViewMode::Folds);
    }

    #[test]
    fn test_create_folds_table() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let (headers, rows) = screen.create_folds_table();
        assert_eq!(headers.len(), 9);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_create_aggregate_table() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let (headers, rows) = screen.create_aggregate_table();
        assert_eq!(headers.len(), 2);
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn test_create_weights_table() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let (headers, rows) = screen.create_weights_table();
        assert_eq!(headers.len(), 6);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_create_sharpe_series() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let series = screen.create_sharpe_series();
        assert_eq!(series.points.len(), 5);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = WalkForwardMLViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_fold_results_access() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        assert_eq!(screen.result().fold_results.len(), 5);
        assert_eq!(screen.result().folds, 5);
    }

    #[test]
    fn test_aggregate_metrics() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let agg = &screen.result().aggregate;
        assert!(agg.avg_oos_sharpe > 0.0);
        assert!(agg.prob_sharpe_gt_zero > 0.0);
    }

    #[test]
    fn test_weight_stability() {
        let result = create_test_walk_forward_ml_result();
        let screen = BacktestWalkForwardMLResultsScreen::new(result);
        let stability = &screen.result().aggregate.weight_stability;
        assert!(stability.stability_score > 0.0);
    }
}
