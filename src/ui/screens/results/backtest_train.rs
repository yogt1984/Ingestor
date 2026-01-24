//! Backtest Train Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest train command results (ML weight training).
//! Supports multiple view modes: Summary, Weights, Comparison, Metrics.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::TrainResult;
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for train results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrainViewMode {
    /// Summary view with key metrics
    Summary,
    /// Weights view (spread and skew weights)
    Weights,
    /// Comparison view (train vs test)
    Comparison,
    /// Metrics view (detailed metrics)
    Metrics,
}

impl TrainViewMode {
    /// Get all view modes
    pub fn all() -> Vec<TrainViewMode> {
        vec![
            TrainViewMode::Summary,
            TrainViewMode::Weights,
            TrainViewMode::Comparison,
            TrainViewMode::Metrics,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            TrainViewMode::Summary => "Summary",
            TrainViewMode::Weights => "Weights",
            TrainViewMode::Comparison => "Comparison",
            TrainViewMode::Metrics => "Metrics",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> TrainViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> TrainViewMode {
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

/// Backtest train results screen
pub struct BacktestTrainResultsScreen {
    /// Train result data
    result: TrainResult,
    /// Current view mode
    view_mode: TrainViewMode,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestTrainResultsScreen {
    /// Create a new results screen from TrainResult
    pub fn new(result: TrainResult) -> Self {
        Self {
            result,
            view_mode: TrainViewMode::Summary,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &TrainResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> TrainViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: TrainViewMode) {
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

    /// Create weights table
    fn create_weights_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Weight Type".to_string()).with_width(20).with_sortable(false),
            TableHeader::new("Intercept".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Entropy".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Volatility".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Imbalance".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Interaction/Inventory".to_string()).with_width(18).with_sortable(false),
        ];

        let weights = &self.result.optimal_weights;
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

    /// Create comparison table
    fn create_comparison_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Metric".to_string()).with_width(20).with_sortable(false),
            TableHeader::new("Training".to_string()).with_width(15).with_sortable(false),
            TableHeader::new("Test".to_string()).with_width(15).with_sortable(false),
            TableHeader::new("Gap".to_string()).with_width(15).with_sortable(false),
        ];

        let rows = vec![
            TableRow::new(vec![
                "Sharpe Ratio".to_string(),
                format!("{:.4}", self.result.train_sharpe),
                format!("{:.4}", self.result.test_sharpe),
                format!("{:.4}", self.result.generalization_gap),
            ]),
            TableRow::new(vec![
                "Total Return".to_string(),
                format!("{:.2}%", self.result.train_return * 100.0),
                format!("{:.2}%", self.result.test_return * 100.0),
                format!("{:.2}%", (self.result.train_return - self.result.test_return) * 100.0),
            ]),
            TableRow::new(vec![
                "Number of Trades".to_string(),
                format!("{}", self.result.train_trades),
                format!("{}", self.result.test_trades),
                format!("{}", self.result.train_trades as i64 - self.result.test_trades as i64),
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
        let tab_titles: Vec<Line> = TrainViewMode::all()
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
            .select(TrainViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            TrainViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            TrainViewMode::Weights => {
                self.render_weights(f, chunks[1]);
            }
            TrainViewMode::Comparison => {
                self.render_comparison(f, chunks[1]);
            }
            TrainViewMode::Metrics => {
                self.render_metrics(f, chunks[1]);
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
        let metrics = vec![
            Metric::new("Train Sharpe".to_string(), MetricValue::Number(self.result.train_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Test Sharpe".to_string(), MetricValue::Number(self.result.test_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Generalization Gap".to_string(), MetricValue::Number(self.result.generalization_gap)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Valid Configs".to_string(), MetricValue::Number(self.result.valid_configurations as f64)),
            Metric::new("Total Configs".to_string(), MetricValue::Number(self.result.total_configurations as f64)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Training info
        let info = vec![
            format!("Algorithm: {}", self.result.algorithm_name),
            format!("Train Return: {:.2}%", self.result.train_return * 100.0),
            format!("Train Trades: {}", self.result.train_trades),
            format!("Test Return: {:.2}%", self.result.test_return * 100.0),
            format!("Test Trades: {}", self.result.test_trades),
            format!("Weights Version: {}", self.result.optimal_weights.version),
        ];

        let text: Vec<Line> = info.iter().map(|s| Line::from(s.as_str())).collect();
        let paragraph = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Training Information"))
            .alignment(Alignment::Left);

        f.render_widget(paragraph, chunks[1]);
    }

    /// Render weights view
    fn render_weights(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_weights_table();

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

    /// Render metrics view
    fn render_metrics(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(0),
            ])
            .split(area);

        // Detailed metrics
        let metrics = vec![
            Metric::new("Train Sharpe".to_string(), MetricValue::Number(self.result.train_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Train Return".to_string(), MetricValue::Number(self.result.train_return * 100.0)).with_format(MetricFormat::Decimal(2)),
            Metric::new("Train Trades".to_string(), MetricValue::Number(self.result.train_trades as f64)),
            Metric::new("Test Sharpe".to_string(), MetricValue::Number(self.result.test_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Test Return".to_string(), MetricValue::Number(self.result.test_return * 100.0)).with_format(MetricFormat::Decimal(2)),
            Metric::new("Test Trades".to_string(), MetricValue::Number(self.result.test_trades as f64)),
            Metric::new("Generalization Gap".to_string(), MetricValue::Number(self.result.generalization_gap)).with_format(MetricFormat::Decimal(4)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Configuration info
        let config_info = vec![
            format!("Valid Configurations: {}", self.result.valid_configurations),
            format!("Total Configurations: {}", self.result.total_configurations),
            format!("Success Rate: {:.2}%", (self.result.valid_configurations as f64 / self.result.total_configurations as f64) * 100.0),
        ];

        let text: Vec<Line> = config_info.iter().map(|s| Line::from(s.as_str())).collect();
        let paragraph = Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Configuration Information"))
            .alignment(Alignment::Left);

        f.render_widget(paragraph, chunks[1]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use crate::strategies::{MLModelWeights, SpreadWeights, SkewWeights};

    fn create_test_train_result() -> TrainResult {
        TrainResult {
            algorithm: "ml".to_string(),
            algorithm_name: "ML Spread/Skew".to_string(),
            optimal_weights: MLModelWeights {
                spread: SpreadWeights {
                    intercept: 2.5,
                    w_entropy: -2.0,
                    w_volatility: 500.0,
                    w_imbalance: 1.0,
                    w_interaction: -100.0,
                },
                skew: SkewWeights {
                    intercept: 0.5,
                    w_entropy: -0.2,
                    w_volatility: 50.0,
                    w_imbalance: 0.1,
                    w_inventory: -0.8,
                },
                version: "1.0".to_string(),
                training_info: None,
            },
            train_sharpe: 2.0,
            train_return: 0.15,
            train_trades: 200,
            test_sharpe: 1.8,
            test_return: 0.12,
            test_trades: 150,
            generalization_gap: 0.2,
            valid_configurations: 50,
            total_configurations: 100,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = TrainViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(TrainViewMode::Summary.name(), "Summary");
        assert_eq!(TrainViewMode::Weights.name(), "Weights");
        assert_eq!(TrainViewMode::Comparison.name(), "Comparison");
        assert_eq!(TrainViewMode::Metrics.name(), "Metrics");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(TrainViewMode::Summary.next(), TrainViewMode::Weights);
        assert_eq!(TrainViewMode::Weights.next(), TrainViewMode::Comparison);
        assert_eq!(TrainViewMode::Comparison.next(), TrainViewMode::Metrics);
        assert_eq!(TrainViewMode::Metrics.next(), TrainViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        assert_eq!(screen.view_mode(), TrainViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_train_result();
        let mut screen = BacktestTrainResultsScreen::new(result);
        screen.set_view_mode(TrainViewMode::Weights);
        assert_eq!(screen.view_mode(), TrainViewMode::Weights);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_train_result();
        let mut screen = BacktestTrainResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), TrainViewMode::Weights);
    }

    #[test]
    fn test_create_weights_table() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        let (headers, rows) = screen.create_weights_table();
        assert_eq!(headers.len(), 6);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_create_comparison_table() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        let (headers, rows) = screen.create_comparison_table();
        assert_eq!(headers.len(), 4);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_generalization_gap_calculation() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        assert!((screen.result().generalization_gap - 0.2).abs() < 1e-10);
        // Floating-point comparison for train_sharpe - test_sharpe
        let computed_gap = screen.result().train_sharpe - screen.result().test_sharpe;
        assert!((computed_gap - 0.2).abs() < 1e-10);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = TrainViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_weights_access() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        assert_eq!(screen.result().optimal_weights.spread.intercept, 2.5);
        assert_eq!(screen.result().optimal_weights.skew.intercept, 0.5);
    }

    #[test]
    fn test_configuration_metrics() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        assert_eq!(screen.result().valid_configurations, 50);
        assert_eq!(screen.result().total_configurations, 100);
    }

    #[test]
    fn test_train_vs_test_comparison() {
        let result = create_test_train_result();
        let screen = BacktestTrainResultsScreen::new(result);
        assert!(screen.result().train_sharpe > screen.result().test_sharpe);
        assert!(screen.result().train_return > screen.result().test_return);
    }
}
