//! Backtest Simulate Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest simulate command results.
//! Supports multiple view modes: Summary, Weekly, Metrics, Verdict.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::SimulateResult;
use crate::backtest::validation_campaign::{CampaignReport, ValidationVerdict};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for simulate results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulateViewMode {
    /// Summary view with key metrics
    Summary,
    /// Weekly summaries table view
    Weekly,
    /// Campaign metrics view
    Metrics,
    /// Verdict and recommendations view
    Verdict,
}

impl SimulateViewMode {
    /// Get all view modes
    pub fn all() -> Vec<SimulateViewMode> {
        vec![
            SimulateViewMode::Summary,
            SimulateViewMode::Weekly,
            SimulateViewMode::Metrics,
            SimulateViewMode::Verdict,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            SimulateViewMode::Summary => "Summary",
            SimulateViewMode::Weekly => "Weekly",
            SimulateViewMode::Metrics => "Metrics",
            SimulateViewMode::Verdict => "Verdict",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> SimulateViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> SimulateViewMode {
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

/// Backtest simulate results screen
pub struct BacktestSimulateResultsScreen {
    /// Simulate result data
    result: SimulateResult,
    /// Current view mode
    view_mode: SimulateViewMode,
    /// Selected week index (for Weekly view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestSimulateResultsScreen {
    /// Create a new results screen from SimulateResult
    pub fn new(result: SimulateResult) -> Self {
        Self {
            result,
            view_mode: SimulateViewMode::Summary,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &SimulateResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> SimulateViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: SimulateViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.result.campaign_report.weekly_summaries.len() {
                self.selected_index = Some(idx);
            } else {
                self.selected_index = None;
            }
        } else {
            self.selected_index = None;
        }
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
                if self.view_mode == SimulateViewMode::Weekly {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.campaign_report.weekly_summaries.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == SimulateViewMode::Weekly {
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

    /// Format verdict
    fn format_verdict(verdict: &ValidationVerdict) -> &'static str {
        match verdict {
            ValidationVerdict::GoLive => "Go Live",
            ValidationVerdict::Recalibrate => "Recalibrate",
            ValidationVerdict::Reject => "Reject",
            ValidationVerdict::Incomplete => "Incomplete",
        }
    }

    /// Create weekly summaries table
    fn create_weekly_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Week".to_string()).with_width(6).with_sortable(false),
            TableHeader::new("Sessions".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Trades".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("PnL".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Sharpe".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Win Rate".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Fill Rate".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Max DD".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Gate".to_string()).with_width(10).with_sortable(false),
        ];

        let rows: Vec<TableRow> = self.result.campaign_report.weekly_summaries
            .iter()
            .map(|week| {
                let gate_str = match &week.gate_result {
                    crate::backtest::validation_campaign::GateResult::Pass => "Pass",
                    crate::backtest::validation_campaign::GateResult::Warning { .. } => "Warning",
                    crate::backtest::validation_campaign::GateResult::Fail { .. } => "Fail",
                };
                TableRow::new(vec![
                    format!("{}", week.week_number),
                    format!("{}", week.session_count),
                    format!("{}", week.total_trades),
                    format!("{:.2}", week.cumulative_pnl),
                    format!("{:.2}", week.weekly_sharpe),
                    format!("{:.1}%", week.win_rate * 100.0),
                    format!("{:.1}%", week.avg_fill_rate * 100.0),
                    format!("{:.2}%", week.max_drawdown),
                    gate_str.to_string(),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create campaign metrics table
    fn create_metrics_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Metric".to_string()).with_width(30).with_sortable(false),
            TableHeader::new("Value".to_string()).with_width(20).with_sortable(false),
        ];

        let metrics = &self.result.campaign_report.campaign_metrics;
        let rows = vec![
            TableRow::new(vec![
                "Total Sessions".to_string(),
                format!("{}", metrics.total_sessions),
            ]),
            TableRow::new(vec![
                "Weeks Completed".to_string(),
                format!("{}", metrics.weeks_completed),
            ]),
            TableRow::new(vec![
                "Total Trades".to_string(),
                format!("{}", metrics.total_trades),
            ]),
            TableRow::new(vec![
                "Total PnL".to_string(),
                format!("{:.2}", metrics.total_pnl),
            ]),
            TableRow::new(vec![
                "Overall Sharpe".to_string(),
                format!("{:.4}", metrics.overall_sharpe),
            ]),
            TableRow::new(vec![
                "Overall Win Rate".to_string(),
                format!("{:.2}%", metrics.overall_win_rate * 100.0),
            ]),
            TableRow::new(vec![
                "Overall Fill Rate".to_string(),
                format!("{:.2}%", metrics.overall_fill_rate * 100.0),
            ]),
            TableRow::new(vec![
                "Max Drawdown".to_string(),
                format!("{:.2}%", metrics.max_drawdown),
            ]),
            TableRow::new(vec![
                "Fill Rate Calibration".to_string(),
                format!("{:.2}", metrics.fill_rate_calibration),
            ]),
            TableRow::new(vec![
                "Probabilistic Sharpe Ratio".to_string(),
                format!("{:.4}", metrics.psr),
            ]),
        ];

        (headers, rows)
    }

    /// Create weekly PnL chart series
    fn create_pnl_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.result.campaign_report.weekly_summaries
            .iter()
            .map(|week| {
                let x = week.week_number as f64;
                let y = week.cumulative_pnl;
                let label = format!("{:.2}", y);
                DataPoint::new(x, y).with_label(label)
            })
            .collect();

        DataSeries::new("Cumulative PnL".to_string())
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
        let tab_titles: Vec<Line> = SimulateViewMode::all()
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
            .select(SimulateViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            SimulateViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            SimulateViewMode::Weekly => {
                self.render_weekly(f, chunks[1]);
            }
            SimulateViewMode::Metrics => {
                self.render_metrics(f, chunks[1]);
            }
            SimulateViewMode::Verdict => {
                self.render_verdict(f, chunks[1]);
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
        let metrics = &self.result.campaign_report.campaign_metrics;
        let dashboard_metrics = vec![
            Metric::new("Total Sessions".to_string(), MetricValue::Number(metrics.total_sessions as f64)),
            Metric::new("Weeks Completed".to_string(), MetricValue::Number(metrics.weeks_completed as f64)),
            Metric::new("Total Trades".to_string(), MetricValue::Number(metrics.total_trades as f64)),
            Metric::new("Overall Sharpe".to_string(), MetricValue::Number(metrics.overall_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("PSR".to_string(), MetricValue::Number(metrics.psr)).with_format(MetricFormat::Decimal(4)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(dashboard_metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Chart showing weekly PnL
        let series = self.create_pnl_series();
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Line)
            .with_series(vec![series])
            .with_x_axis(AxisConfig::default().with_label("Week Number"))
            .with_y_axis(AxisConfig::default().with_label("Cumulative PnL"));

        chart.render(area, f.buffer_mut());
    }

    /// Render weekly view
    fn render_weekly(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_weekly_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render metrics view
    fn render_metrics(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_metrics_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render verdict view
    fn render_verdict(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(0),
            ])
            .split(area);

        let report = &self.result.campaign_report;
        
        // Verdict
        let verdict_text = vec![
            format!("Verdict: {}", Self::format_verdict(&report.verdict)),
        ];
        let verdict_lines: Vec<Line> = verdict_text.iter().map(|s| Line::from(s.as_str())).collect();
        let verdict_para = Paragraph::new(verdict_lines)
            .block(Block::default().borders(Borders::ALL).title("Final Verdict"))
            .alignment(Alignment::Left);
        f.render_widget(verdict_para, chunks[0]);

        // Verdict reasons
        let reasons_text: Vec<Line> = report.verdict_reasons.iter()
            .map(|r| Line::from(format!("• {}", r).as_str()))
            .collect();
        let reasons_para = Paragraph::new(reasons_text)
            .block(Block::default().borders(Borders::ALL).title("Verdict Reasons"))
            .alignment(Alignment::Left);
        f.render_widget(reasons_para, chunks[1]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use chrono::{Utc, NaiveDate};
    use crate::backtest::validation_campaign::{
        CampaignConfig, CampaignMetrics, WeeklyMetrics, ValidationGates,
        GateResult, DailyMetrics,
    };

    fn create_test_simulate_result() -> SimulateResult {
        let mut weekly_summaries = Vec::new();
        for i in 0..4 {
            weekly_summaries.push(WeeklyMetrics {
                week_number: (i + 1) as u8,
                start_date: NaiveDate::from_ymd_opt(2024, 1, 1 + i * 7).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2024, 1, 7 + i * 7).unwrap(),
                days: vec![],
                session_count: 5 + i,
                total_trades: 100 + i * 20,
                cumulative_pnl: 100.0 + i as f64 * 50.0,
                avg_fill_rate: 0.08 + i as f64 * 0.01,
                weekly_sharpe: 1.5 - i as f64 * 0.1,
                win_rate: 0.55 - i as f64 * 0.02,
                max_drawdown: 1.0 + i as f64 * 0.5,
                total_volume: 1000.0 + i as f64 * 200.0,
                total_hours: 40.0 + i as f64 * 8.0,
                pnl_vs_expected: 0.9 - i as f64 * 0.05,
                fill_rate_vs_expected: 0.8 + i as f64 * 0.05,
                gate_result: if i == 0 {
                    GateResult::Pass
                } else if i == 1 {
                    GateResult::Warning { reasons: vec!["Low fill rate".to_string()] }
                } else {
                    GateResult::Pass
                },
            });
        }

        SimulateResult {
            algorithm: "mm".to_string(),
            algorithm_name: "Market Making".to_string(),
            campaign_report: CampaignReport {
                campaign_id: "test-campaign-1".to_string(),
                config: CampaignConfig::default(),
                status: crate::backtest::validation_campaign::CampaignStatus::Completed,
                start_time: Utc::now(),
                end_time: Some(Utc::now()),
                weekly_summaries,
                campaign_metrics: CampaignMetrics {
                    total_sessions: 20,
                    weeks_completed: 4,
                    total_trades: 400,
                    total_pnl: 500.0,
                    overall_sharpe: 1.2,
                    overall_win_rate: 0.52,
                    overall_fill_rate: 0.09,
                    max_drawdown: 2.5,
                    total_volume: 5000.0,
                    total_hours: 160.0,
                    fill_rate_calibration: 0.9,
                    fill_rate_ci_lower: 0.08,
                    fill_rate_ci_upper: 0.10,
                    psr: 0.95,
                    sharpe_ci_lower: 1.0,
                    sharpe_ci_upper: 1.4,
                },
                verdict: ValidationVerdict::GoLive,
                verdict_reasons: vec![
                    "All weekly gates passed".to_string(),
                    "PSR > 0.95".to_string(),
                ],
                recommendations: vec![
                    "Ready for live trading".to_string(),
                ],
            },
            total_sessions: 20,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = SimulateViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(SimulateViewMode::Summary.name(), "Summary");
        assert_eq!(SimulateViewMode::Weekly.name(), "Weekly");
        assert_eq!(SimulateViewMode::Metrics.name(), "Metrics");
        assert_eq!(SimulateViewMode::Verdict.name(), "Verdict");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(SimulateViewMode::Summary.next(), SimulateViewMode::Weekly);
        assert_eq!(SimulateViewMode::Weekly.next(), SimulateViewMode::Metrics);
        assert_eq!(SimulateViewMode::Metrics.next(), SimulateViewMode::Verdict);
        assert_eq!(SimulateViewMode::Verdict.next(), SimulateViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        assert_eq!(screen.view_mode(), SimulateViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_simulate_result();
        let mut screen = BacktestSimulateResultsScreen::new(result);
        screen.set_view_mode(SimulateViewMode::Weekly);
        assert_eq!(screen.view_mode(), SimulateViewMode::Weekly);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_simulate_result();
        let mut screen = BacktestSimulateResultsScreen::new(result);
        screen.set_selected_index(Some(2));
        assert_eq!(screen.selected_index(), Some(2));
    }

    #[test]
    fn test_export_json() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_simulate_result();
        let mut screen = BacktestSimulateResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), SimulateViewMode::Weekly);
    }

    #[test]
    fn test_format_verdict() {
        assert_eq!(BacktestSimulateResultsScreen::format_verdict(&ValidationVerdict::GoLive), "Go Live");
        assert_eq!(BacktestSimulateResultsScreen::format_verdict(&ValidationVerdict::Recalibrate), "Recalibrate");
    }

    #[test]
    fn test_create_weekly_table() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        let (headers, rows) = screen.create_weekly_table();
        assert_eq!(headers.len(), 9);
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn test_create_metrics_table() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        let (headers, rows) = screen.create_metrics_table();
        assert_eq!(headers.len(), 2);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_create_pnl_series() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        let series = screen.create_pnl_series();
        assert_eq!(series.points.len(), 4);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = SimulateViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_campaign_report_access() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        assert_eq!(screen.result().campaign_report.weekly_summaries.len(), 4);
        assert_eq!(screen.result().total_sessions, 20);
    }

    #[test]
    fn test_campaign_metrics() {
        let result = create_test_simulate_result();
        let screen = BacktestSimulateResultsScreen::new(result);
        let metrics = &screen.result().campaign_report.campaign_metrics;
        assert!(metrics.overall_sharpe > 0.0);
        assert!(metrics.psr > 0.0);
    }
}
