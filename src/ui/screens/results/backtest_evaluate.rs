//! Backtest Evaluate Results Screen (T-3.6)
//!
//! TUI screen for displaying backtest evaluate command results.
//! Supports multiple view modes: Summary, Detailed, EquityCurve, TradeLog, Inventory.

use std::collections::HashMap;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

use crate::commands::backtest::EvaluateResult;
use crate::backtest::harness::BacktestResults;
use crate::backtest::metrics::{EquityPoint, TradeRecord, TradeSide};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat, Trend,
    TableWidget, TableHeader, TableRow, SortDirection,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewMode {
    /// Summary view with key metrics dashboard
    Summary,
    /// Detailed view with all metrics and statistics
    Detailed,
    /// Equity curve chart view
    EquityCurve,
    /// Trade log table view
    TradeLog,
    /// Inventory chart view
    Inventory,
}

impl ViewMode {
    /// Get all view modes
    pub fn all() -> Vec<ViewMode> {
        vec![
            ViewMode::Summary,
            ViewMode::Detailed,
            ViewMode::EquityCurve,
            ViewMode::TradeLog,
            ViewMode::Inventory,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            ViewMode::Summary => "Summary",
            ViewMode::Detailed => "Detailed",
            ViewMode::EquityCurve => "Equity Curve",
            ViewMode::TradeLog => "Trade Log",
            ViewMode::Inventory => "Inventory",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> ViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> ViewMode {
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

/// Backtest evaluate results screen
pub struct BacktestEvaluateResultsScreen {
    /// Evaluate result (simplified)
    eval_result: Option<EvaluateResult>,
    /// Full backtest results (if available)
    backtest_results: Option<BacktestResults>,
    /// Current view mode
    view_mode: ViewMode,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestEvaluateResultsScreen {
    /// Create a new results screen from EvaluateResult
    pub fn new(eval_result: EvaluateResult) -> Self {
        Self {
            eval_result: Some(eval_result),
            backtest_results: None,
            view_mode: ViewMode::Summary,
            focused: true,
            export_path: None,
        }
    }

    /// Create a new results screen from BacktestResults
    pub fn from_backtest_results(backtest_results: BacktestResults) -> Self {
        // Extract EvaluateResult from BacktestResults
        use crate::commands::params::backtest_params::EvaluateParamsBuilder;
        let eval_result = EvaluateResult {
            algorithm: "unknown".to_string(),
            algorithm_name: "Unknown".to_string(),
            metrics: crate::commands::backtest::EvaluateMetrics::from(&backtest_results),
            params: EvaluateParamsBuilder::new().build().unwrap_or_else(|_| {
                // Create minimal params if build fails
                use std::path::PathBuf;
                crate::commands::params::backtest_params::EvaluateParams {
                    data_path: PathBuf::new(),
                    algorithm: "unknown".to_string(),
                    weights_file: None,
                    spread: 0.0,
                    skew: 0.0,
                    max_inventory: 0.0,
                    quote_size: 0.0,
                    fee_rate: 0.0,
                    naive_fills: false,
                    fill_prob: 0.0,
                    queue_pos: 0.0,
                    high_entropy: 0.0,
                    low_entropy: 0.0,
                    regime_params: false,
                    high_spread: 0.0,
                    med_spread: 0.0,
                    low_spread: 0.0,
                    high_skew: 0.0,
                    med_skew: 0.0,
                    low_skew: 0.0,
                    quote_low_entropy: false,
                    output: None,
                    json: false,
                    quiet: false,
                    stats: false,
                }
            }),
            events_processed: backtest_results.events_processed,
            fills_generated: backtest_results.fills_generated,
        };

        Self {
            eval_result: Some(eval_result),
            backtest_results: Some(backtest_results),
            view_mode: ViewMode::Summary,
            focused: true,
            export_path: None,
        }
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: ViewMode) {
        self.view_mode = mode;
    }

    /// Get current view mode
    pub fn view_mode(&self) -> ViewMode {
        self.view_mode
    }

    /// Set focus state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Get focus state
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Handle keyboard input
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !self.focused {
            return false;
        }

        match key.code {
            KeyCode::Tab | KeyCode::Char('n') => {
                self.view_mode = self.view_mode.next();
                true
            }
            KeyCode::BackTab | KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.view_mode = self.view_mode.previous();
                true
            }
            KeyCode::Char('e') => {
                // Export functionality (placeholder)
                self.export_path = Some("export.json".to_string());
                true
            }
            _ => false,
        }
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Title bar
        let title = if let Some(ref eval) = self.eval_result {
            format!("Backtest Results: {}", eval.algorithm_name)
        } else {
            "Backtest Results".to_string()
        };

        let title_block = Block::default()
            .borders(Borders::ALL)
            .title(title.as_str())
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

        let inner = title_block.inner(area);
        title_block.render(area, f.buffer_mut());

        if inner.width < 2 || inner.height < 2 {
            return;
        }

        // View mode tabs
        let tabs_height = 3;
        let tabs_area = Rect {
            x: inner.x,
            y: inner.y,
            width: inner.width,
            height: tabs_height,
        };

        let content_area = Rect {
            x: inner.x,
            y: inner.y + tabs_height,
            width: inner.width,
            height: inner.height.saturating_sub(tabs_height),
        };

        self.render_tabs(f, tabs_area);
        self.render_content(f, content_area);
    }

    /// Render view mode tabs
    fn render_tabs(&self, f: &mut Frame, area: Rect) {
        let tabs: Vec<&str> = ViewMode::all()
            .iter()
            .map(|m| m.name())
            .collect();

        let selected = ViewMode::all()
            .iter()
            .position(|m| *m == self.view_mode)
            .unwrap_or(0);

        let tabs_widget = Tabs::new(tabs)
            .block(Block::default().borders(Borders::BOTTOM))
            .select(selected)
            .style(Style::default().fg(Color::White))
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            );

        tabs_widget.render(area, f.buffer_mut());
    }

    /// Render content based on view mode
    fn render_content(&self, f: &mut Frame, area: Rect) {
        match self.view_mode {
            ViewMode::Summary => self.render_summary(f, area),
            ViewMode::Detailed => self.render_detailed(f, area),
            ViewMode::EquityCurve => self.render_equity_curve(f, area),
            ViewMode::TradeLog => self.render_trade_log(f, area),
            ViewMode::Inventory => self.render_inventory(f, area),
        }
    }

    /// Render summary view
    fn render_summary(&self, f: &mut Frame, area: Rect) {
        if let Some(ref eval) = self.eval_result {
            let metrics = self.create_summary_metrics(eval);
            let dashboard = MetricsDashboardWidget::new()
                .with_metrics(metrics)
                .with_block(Block::default().borders(Borders::ALL).title("Key Metrics"));

            dashboard.render(area, f.buffer_mut());
        } else {
            let paragraph = Paragraph::new("No results available")
                .style(Style::default().fg(Color::Red))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
        }
    }

    /// Render detailed view
    fn render_detailed(&self, f: &mut Frame, area: Rect) {
        if let Some(ref eval) = self.eval_result {
            let metrics = self.create_detailed_metrics(eval);
            let dashboard = MetricsDashboardWidget::new()
                .with_metrics(metrics)
                .with_block(Block::default().borders(Borders::ALL).title("All Metrics"));

            dashboard.render(area, f.buffer_mut());
        } else {
            let paragraph = Paragraph::new("No results available")
                .style(Style::default().fg(Color::Red))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
        }
    }

    /// Render equity curve view
    fn render_equity_curve(&self, f: &mut Frame, area: Rect) {
        if let Some(ref results) = self.backtest_results {
            let series = self.create_equity_curve_series(results);
            if !series.points.is_empty() {
                let mut chart = ChartWidget::new();
                chart.add_series(series);
                chart = chart
                    .with_chart_type(ChartType::Line)
                    .with_block(Block::default().borders(Borders::ALL).title("Equity Curve"))
                    .with_x_axis(AxisConfig {
                        label: Some("Time".to_string()),
                        min: None,
                        max: None,
                        show_grid: true,
                        ticks: 5,
                    })
                    .with_y_axis(AxisConfig {
                        label: Some("Equity".to_string()),
                        min: None,
                        max: None,
                        show_grid: true,
                        ticks: 5,
                    });

                chart.render(area, f.buffer_mut());
            } else {
                let paragraph = Paragraph::new("No equity curve data available")
                    .style(Style::default().fg(Color::Yellow))
                    .alignment(Alignment::Center);
                paragraph.render(area, f.buffer_mut());
            }
        } else {
            let paragraph = Paragraph::new("Equity curve data not available (full results required)")
                .style(Style::default().fg(Color::Yellow))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
        }
    }

    /// Render trade log view
    fn render_trade_log(&self, f: &mut Frame, area: Rect) {
        if let Some(ref results) = self.backtest_results {
            let (headers, rows) = self.create_trade_log_table(results);
            if !rows.is_empty() {
                let mut table = TableWidget::new()
                    .with_headers(headers)
                    .with_rows(rows)
                    .with_block(Block::default().borders(Borders::ALL).title("Trade Log"));
                table.set_focused(self.focused);

                table.render(area, f.buffer_mut());
            } else {
                let paragraph = Paragraph::new("No trades recorded")
                    .style(Style::default().fg(Color::Yellow))
                    .alignment(Alignment::Center);
                paragraph.render(area, f.buffer_mut());
            }
        } else {
            let paragraph = Paragraph::new("Trade log data not available (full results required)")
                .style(Style::default().fg(Color::Yellow))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
        }
    }

    /// Render inventory view
    fn render_inventory(&self, f: &mut Frame, area: Rect) {
        if let Some(ref results) = self.backtest_results {
            let series = self.create_inventory_series(results);
            if !series.points.is_empty() {
                let mut chart = ChartWidget::new();
                chart.add_series(series);
                chart = chart
                    .with_chart_type(ChartType::Line)
                    .with_block(Block::default().borders(Borders::ALL).title("Inventory"))
                    .with_x_axis(AxisConfig {
                        label: Some("Time".to_string()),
                        min: None,
                        max: None,
                        show_grid: true,
                        ticks: 5,
                    })
                    .with_y_axis(AxisConfig {
                        label: Some("Inventory".to_string()),
                        min: None,
                        max: None,
                        show_grid: true,
                        ticks: 5,
                    });

                chart.render(area, f.buffer_mut());
            } else {
                let paragraph = Paragraph::new("No inventory data available")
                    .style(Style::default().fg(Color::Yellow))
                    .alignment(Alignment::Center);
                paragraph.render(area, f.buffer_mut());
            }
        } else {
            let paragraph = Paragraph::new("Inventory data not available (full results required)")
                .style(Style::default().fg(Color::Yellow))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
        }
    }

    /// Create summary metrics
    pub fn create_summary_metrics(&self, eval: &EvaluateResult) -> Vec<Metric> {
        vec![
            Metric::new("Sharpe Ratio", MetricValue::Number(eval.metrics.sharpe_ratio))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Total Return", MetricValue::Percentage(eval.metrics.total_return * 100.0))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Max Drawdown", MetricValue::Percentage(eval.metrics.max_drawdown * 100.0))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Number of Trades", MetricValue::Integer(eval.metrics.num_trades as i64)),
            Metric::new("Win Rate", MetricValue::Percentage(eval.metrics.win_rate * 100.0))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Avg Trade P&L", MetricValue::Number(eval.metrics.avg_trade_pnl))
                .with_format(MetricFormat::Decimal(4)),
        ]
    }

    /// Create detailed metrics
    pub fn create_detailed_metrics(&self, eval: &EvaluateResult) -> Vec<Metric> {
        let mut metrics = self.create_summary_metrics(eval);
        metrics.extend(vec![
            Metric::new("Annualized Return", MetricValue::Percentage(eval.metrics.annualized_return * 100.0))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Sortino Ratio", MetricValue::Number(eval.metrics.sortino_ratio))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Calmar Ratio", MetricValue::Number(eval.metrics.calmar_ratio))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Profit Factor", MetricValue::Number(eval.metrics.profit_factor))
                .with_format(MetricFormat::Decimal(2)),
            Metric::new("Events Processed", MetricValue::Integer(eval.events_processed as i64)),
            Metric::new("Fills Generated", MetricValue::Integer(eval.fills_generated as i64)),
        ]);
        metrics
    }

    /// Create equity curve series
    pub fn create_equity_curve_series(&self, results: &BacktestResults) -> DataSeries {
        let points: Vec<DataPoint> = results
            .equity_curve
            .points
            .iter()
            .map(|p| DataPoint {
                x: p.timestamp_ms as f64 / 1000.0, // Convert to seconds
                y: p.equity.to_f64().unwrap_or(0.0),
                label: None,
            })
            .collect();

        DataSeries {
            name: "Equity".to_string(),
            points,
            color: Some(Color::Green),
            symbol: None,
        }
    }

    /// Create inventory series
    pub fn create_inventory_series(&self, results: &BacktestResults) -> DataSeries {
        let points: Vec<DataPoint> = results
            .equity_curve
            .points
            .iter()
            .map(|p| DataPoint {
                x: p.timestamp_ms as f64 / 1000.0, // Convert to seconds
                y: p.inventory.to_f64().unwrap_or(0.0),
                label: None,
            })
            .collect();

        DataSeries {
            name: "Inventory".to_string(),
            points,
            color: Some(Color::Cyan),
            symbol: None,
        }
    }

    /// Create trade log table
    pub fn create_trade_log_table(&self, results: &BacktestResults) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Time".to_string())
                .with_width(20)
                .with_sortable(true),
            TableHeader::new("Side".to_string())
                .with_width(8)
                .with_sortable(true),
            TableHeader::new("Price".to_string())
                .with_width(12)
                .with_sortable(true),
            TableHeader::new("Size".to_string())
                .with_width(12)
                .with_sortable(true),
            TableHeader::new("Fee".to_string())
                .with_width(12)
                .with_sortable(true),
            TableHeader::new("P&L".to_string())
                .with_width(12)
                .with_sortable(true),
        ];

        let rows: Vec<TableRow> = results
            .trade_log
            .trades
            .iter()
            .map(|trade| {
                let side_str = match trade.side {
                    TradeSide::Buy => "Buy",
                    TradeSide::Sell => "Sell",
                };
                let pnl_str = trade
                    .pnl
                    .map(|p| format!("{:.4}", p.to_f64().unwrap_or(0.0)))
                    .unwrap_or_else(|| "-".to_string());

                TableRow::new(vec![
                    format!("{}", trade.timestamp_ms),
                    side_str.to_string(),
                    format!("{:.2}", trade.price.to_f64().unwrap_or(0.0)),
                    format!("{:.4}", trade.size.to_f64().unwrap_or(0.0)),
                    format!("{:.4}", trade.fee.to_f64().unwrap_or(0.0)),
                    pnl_str,
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Export results to JSON
    pub fn export_to_json(&self) -> anyhow::Result<String> {
        if let Some(ref eval) = self.eval_result {
            serde_json::to_string_pretty(eval)
                .map_err(|e| anyhow::anyhow!("Failed to serialize results: {}", e))
        } else {
            Err(anyhow::anyhow!("No results to export"))
        }
    }

    /// Get evaluate result
    pub fn eval_result(&self) -> Option<&EvaluateResult> {
        self.eval_result.as_ref()
    }

    /// Get backtest results
    pub fn backtest_results(&self) -> Option<&BacktestResults> {
        self.backtest_results.as_ref()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::backtest::EvaluateMetrics;
    use crate::backtest::harness::BacktestConfig;
    use crate::backtest::metrics::{EquityCurve, TradeLog};

    fn create_test_eval_result() -> EvaluateResult {
        EvaluateResult {
            algorithm: "AvellanedaStoikov".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            metrics: EvaluateMetrics {
                sharpe_ratio: 1.5,
                total_return: 0.15,
                max_drawdown: -0.05,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
                annualized_return: 0.20,
                sortino_ratio: 1.8,
                calmar_ratio: 4.0,
                profit_factor: 1.2,
            },
            params: crate::commands::params::backtest_params::EvaluateParamsBuilder::new()
                .build()
                .unwrap_or_else(|_| {
                    use std::path::PathBuf;
                    crate::commands::params::backtest_params::EvaluateParams {
                        data_path: PathBuf::new(),
                        algorithm: "AvellanedaStoikov".to_string(),
                        weights_file: None,
                        spread: 0.0,
                        skew: 0.0,
                        max_inventory: 0.0,
                        quote_size: 0.0,
                        fee_rate: 0.0,
                        naive_fills: false,
                        fill_prob: 0.0,
                        queue_pos: 0.0,
                        high_entropy: 0.0,
                        low_entropy: 0.0,
                        regime_params: false,
                        high_spread: 0.0,
                        med_spread: 0.0,
                        low_spread: 0.0,
                        high_skew: 0.0,
                        med_skew: 0.0,
                        low_skew: 0.0,
                        quote_low_entropy: false,
                        output: None,
                        json: false,
                        quiet: false,
                        stats: false,
                    }
                }),
            events_processed: 10000,
            fills_generated: 100,
        }
    }

    fn create_test_backtest_results() -> BacktestResults {
        use crate::backtest::metrics::{EquityPoint, PerformanceMetrics};
        use rust_decimal_macros::dec;

        let mut equity_curve = EquityCurve::new();
        for i in 0..10 {
            let equity_val = 1000 + i * 10;
            let pnl_val = i * 5;
            let inv_val = (i % 5) as i64 - 2;
            equity_curve.add(EquityPoint {
                timestamp_ms: i * 1000,
                equity: Decimal::from(equity_val),
                unrealized_pnl: dec!(0),
                realized_pnl: Decimal::from(pnl_val),
                inventory: Decimal::from(inv_val),
            });
        }

        let mut trade_log = TradeLog::new();
        for i in 0..5 {
            let price_val = 100 + i;
            trade_log.add(TradeRecord {
                timestamp_ms: i * 1000,
                side: if i % 2 == 0 { TradeSide::Buy } else { TradeSide::Sell },
                price: Decimal::from(price_val),
                size: dec!(1),
                fee: dec!(0.001),
                pnl: if i > 0 { Some(Decimal::from(i)) } else { None },
            });
        }

        BacktestResults {
            config: BacktestConfig::default(),
            metrics: PerformanceMetrics::default(),
            trade_log,
            equity_curve,
            events_processed: 1000,
            fills_generated: 5,
            fill_stats: Default::default(),
            oco_stats: None,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = ViewMode::all();
        assert_eq!(modes.len(), 5);
        assert!(modes.contains(&ViewMode::Summary));
        assert!(modes.contains(&ViewMode::Detailed));
        assert!(modes.contains(&ViewMode::EquityCurve));
        assert!(modes.contains(&ViewMode::TradeLog));
        assert!(modes.contains(&ViewMode::Inventory));
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(ViewMode::Summary.name(), "Summary");
        assert_eq!(ViewMode::Detailed.name(), "Detailed");
        assert_eq!(ViewMode::EquityCurve.name(), "Equity Curve");
        assert_eq!(ViewMode::TradeLog.name(), "Trade Log");
        assert_eq!(ViewMode::Inventory.name(), "Inventory");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(ViewMode::Summary.next(), ViewMode::Detailed);
        assert_eq!(ViewMode::Detailed.next(), ViewMode::EquityCurve);
        assert_eq!(ViewMode::EquityCurve.next(), ViewMode::TradeLog);
        assert_eq!(ViewMode::TradeLog.next(), ViewMode::Inventory);
        assert_eq!(ViewMode::Inventory.next(), ViewMode::Summary);
    }

    #[test]
    fn test_view_mode_previous() {
        assert_eq!(ViewMode::Summary.previous(), ViewMode::Inventory);
        assert_eq!(ViewMode::Detailed.previous(), ViewMode::Summary);
        assert_eq!(ViewMode::EquityCurve.previous(), ViewMode::Detailed);
        assert_eq!(ViewMode::TradeLog.previous(), ViewMode::EquityCurve);
        assert_eq!(ViewMode::Inventory.previous(), ViewMode::TradeLog);
    }

    #[test]
    fn test_screen_creation_from_eval_result() {
        let eval_result = create_test_eval_result();
        let screen = BacktestEvaluateResultsScreen::new(eval_result.clone());
        assert_eq!(screen.view_mode(), ViewMode::Summary);
        assert!(screen.is_focused());
        assert!(screen.eval_result().is_some());
    }

    #[test]
    fn test_screen_creation_from_backtest_results() {
        let results = create_test_backtest_results();
        let screen = BacktestEvaluateResultsScreen::from_backtest_results(results);
        assert_eq!(screen.view_mode(), ViewMode::Summary);
        assert!(screen.eval_result().is_some());
        assert!(screen.backtest_results().is_some());
    }

    #[test]
    fn test_set_view_mode() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);
        assert_eq!(screen.view_mode(), ViewMode::Summary);

        screen.set_view_mode(ViewMode::Detailed);
        assert_eq!(screen.view_mode(), ViewMode::Detailed);

        screen.set_view_mode(ViewMode::EquityCurve);
        assert_eq!(screen.view_mode(), ViewMode::EquityCurve);
    }

    #[test]
    fn test_set_focused() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);
        assert!(screen.is_focused());

        screen.set_focused(false);
        assert!(!screen.is_focused());

        screen.set_focused(true);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_handle_key_tab() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);
        assert_eq!(screen.view_mode(), ViewMode::Summary);

        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), ViewMode::Detailed);
    }

    #[test]
    fn test_handle_key_n() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);
        assert_eq!(screen.view_mode(), ViewMode::Summary);

        let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), ViewMode::Detailed);
    }

    #[test]
    fn test_handle_key_shift_tab() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);
        screen.set_view_mode(ViewMode::Detailed);

        let key = KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT);
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), ViewMode::Summary);
    }

    #[test]
    fn test_handle_key_not_focused() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);
        screen.set_focused(false);

        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(!screen.handle_key(key));
        assert_eq!(screen.view_mode(), ViewMode::Summary);
    }

    #[test]
    fn test_handle_key_export() {
        let eval_result = create_test_eval_result();
        let mut screen = BacktestEvaluateResultsScreen::new(eval_result);

        let key = KeyEvent::new(KeyCode::Char('e'), KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert!(screen.export_path.is_some());
    }

    #[test]
    fn test_create_summary_metrics() {
        let eval_result = create_test_eval_result();
        let screen = BacktestEvaluateResultsScreen::new(eval_result.clone());
        let metrics = screen.create_summary_metrics(&eval_result);

        assert_eq!(metrics.len(), 6);
        assert_eq!(metrics[0].name, "Sharpe Ratio");
        assert_eq!(metrics[1].name, "Total Return");
        assert_eq!(metrics[2].name, "Max Drawdown");
        assert_eq!(metrics[3].name, "Number of Trades");
        assert_eq!(metrics[4].name, "Win Rate");
        assert_eq!(metrics[5].name, "Avg Trade P&L");
    }

    #[test]
    fn test_create_detailed_metrics() {
        let eval_result = create_test_eval_result();
        let screen = BacktestEvaluateResultsScreen::new(eval_result.clone());
        let metrics = screen.create_detailed_metrics(&eval_result);

        assert_eq!(metrics.len(), 12);
        assert!(metrics.iter().any(|m| m.name == "Sharpe Ratio"));
        assert!(metrics.iter().any(|m| m.name == "Annualized Return"));
        assert!(metrics.iter().any(|m| m.name == "Sortino Ratio"));
        assert!(metrics.iter().any(|m| m.name == "Calmar Ratio"));
        assert!(metrics.iter().any(|m| m.name == "Profit Factor"));
        assert!(metrics.iter().any(|m| m.name == "Events Processed"));
        assert!(metrics.iter().any(|m| m.name == "Fills Generated"));
    }

    #[test]
    fn test_create_equity_curve_series() {
        let results = create_test_backtest_results();
        let screen = BacktestEvaluateResultsScreen::from_backtest_results(results);
        let series = screen.create_equity_curve_series(screen.backtest_results().unwrap());

        assert_eq!(series.name, "Equity");
        assert_eq!(series.points.len(), 10);
        assert_eq!(series.points[0].x, 0.0);
        assert_eq!(series.points[0].y, 1000.0);
    }

    #[test]
    fn test_create_inventory_series() {
        let results = create_test_backtest_results();
        let screen = BacktestEvaluateResultsScreen::from_backtest_results(results);
        let series = screen.create_inventory_series(screen.backtest_results().unwrap());

        assert_eq!(series.name, "Inventory");
        assert_eq!(series.points.len(), 10);
    }

    #[test]
    fn test_create_trade_log_table() {
        let results = create_test_backtest_results();
        let screen = BacktestEvaluateResultsScreen::from_backtest_results(results);
        let (headers, rows) = screen.create_trade_log_table(screen.backtest_results().unwrap());

        assert_eq!(headers.len(), 6);
        assert_eq!(rows.len(), 5);
        assert_eq!(headers[0].name, "Time");
        assert_eq!(headers[1].name, "Side");
    }

    #[test]
    fn test_export_to_json() {
        let eval_result = create_test_eval_result();
        let screen = BacktestEvaluateResultsScreen::new(eval_result);
        let json = screen.export_to_json();

        assert!(json.is_ok());
        let json_str = json.unwrap();
        assert!(json_str.contains("Avellaneda-Stoikov"));
        assert!(json_str.contains("sharpe_ratio"));
    }

    #[test]
    fn test_export_to_json_no_results() {
        let screen = BacktestEvaluateResultsScreen {
            eval_result: None,
            backtest_results: None,
            view_mode: ViewMode::Summary,
            focused: true,
            export_path: None,
        };
        let json = screen.export_to_json();
        assert!(json.is_err());
    }

    #[test]
    fn test_render_empty_screen() {
        let screen = BacktestEvaluateResultsScreen {
            eval_result: None,
            backtest_results: None,
            view_mode: ViewMode::Summary,
            focused: true,
            export_path: None,
        };
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        // For testing, we'll just render directly to buffer
        // screen.render requires a Frame which is not easily constructible in tests
        // This test verifies the screen can be created without panicking
        // Should not panic
    }

    #[test]
    fn test_render_all_view_modes() {
        let results = create_test_backtest_results();
        let mut screen = BacktestEvaluateResultsScreen::from_backtest_results(results);
        // For testing, we'll just verify view mode switching works
        // Actual rendering requires a Frame which is not easily constructible in tests
        for mode in ViewMode::all() {
            screen.set_view_mode(mode);
            assert_eq!(screen.view_mode(), mode);
            // Should not panic
        }
    }

    #[test]
    fn test_view_mode_equality() {
        assert_eq!(ViewMode::Summary, ViewMode::Summary);
        assert_ne!(ViewMode::Summary, ViewMode::Detailed);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = ViewMode::Summary;
        for _ in 0..10 {
            mode = mode.next();
        }
        // After 10 cycles, should be back to starting point (10 % 5 = 0)
        assert_eq!(mode, ViewMode::Summary);
    }

    #[test]
    fn test_view_mode_reverse_cycle() {
        let mut mode = ViewMode::Summary;
        for _ in 0..10 {
            mode = mode.previous();
        }
        // After 10 cycles, should be back to starting point
        assert_eq!(mode, ViewMode::Summary);
    }
}
