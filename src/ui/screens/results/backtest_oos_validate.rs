//! Backtest OOS Validate Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest oos_validate command results.
//! Supports multiple view modes: Summary, Reports, Best, Verdicts.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{
    OOSValidateResult, OOSValidateReport, OOSValidateVerdictSummary,
    OOSValidateOverfitVerdict, OOSValidateRecommendation,
};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for OOS validate results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OOSValidateViewMode {
    /// Summary view with key metrics
    Summary,
    /// Reports table view (all validation reports)
    Reports,
    /// Best configuration view
    Best,
    /// Verdicts summary view
    Verdicts,
}

impl OOSValidateViewMode {
    /// Get all view modes
    pub fn all() -> Vec<OOSValidateViewMode> {
        vec![
            OOSValidateViewMode::Summary,
            OOSValidateViewMode::Reports,
            OOSValidateViewMode::Best,
            OOSValidateViewMode::Verdicts,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            OOSValidateViewMode::Summary => "Summary",
            OOSValidateViewMode::Reports => "Reports",
            OOSValidateViewMode::Best => "Best",
            OOSValidateViewMode::Verdicts => "Verdicts",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> OOSValidateViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> OOSValidateViewMode {
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

/// Backtest OOS validate results screen
pub struct BacktestOOSValidateResultsScreen {
    /// OOS validate result data
    result: OOSValidateResult,
    /// Current view mode
    view_mode: OOSValidateViewMode,
    /// Selected report index (for Reports view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestOOSValidateResultsScreen {
    /// Create a new results screen from OOSValidateResult
    pub fn new(result: OOSValidateResult) -> Self {
        Self {
            result,
            view_mode: OOSValidateViewMode::Summary,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &OOSValidateResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> OOSValidateViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: OOSValidateViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.result.all_reports.len() {
                self.selected_index = Some(idx);
            } else {
                self.selected_index = None;
            }
        } else {
            self.selected_index = None;
        }
    }

    /// Get selected report
    pub fn selected_report(&self) -> Option<&OOSValidateReport> {
        self.selected_index
            .and_then(|idx| self.result.all_reports.get(idx))
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
                if self.view_mode == OOSValidateViewMode::Reports {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.all_reports.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == OOSValidateViewMode::Reports {
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

    /// Format overfit verdict
    fn format_verdict(verdict: OOSValidateOverfitVerdict) -> &'static str {
        match verdict {
            OOSValidateOverfitVerdict::Robust => "Robust",
            OOSValidateOverfitVerdict::MildOverfit => "Mild Overfit",
            OOSValidateOverfitVerdict::ModerateOverfit => "Moderate Overfit",
            OOSValidateOverfitVerdict::SevereOverfit => "Severe Overfit",
            OOSValidateOverfitVerdict::Inconclusive => "Inconclusive",
        }
    }

    /// Format recommendation
    fn format_recommendation(rec: OOSValidateRecommendation) -> &'static str {
        match rec {
            OOSValidateRecommendation::ReadyForPaperTrading => "Ready for Paper Trading",
            OOSValidateRecommendation::NeedsMoreData => "Needs More Data",
            OOSValidateRecommendation::SimplifyStrategy => "Simplify Strategy",
            OOSValidateRecommendation::ReconsiderApproach => "Reconsider Approach",
            OOSValidateRecommendation::StatisticallyInsignificant => "Statistically Insignificant",
        }
    }

    /// Create reports table
    fn create_reports_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Rank".to_string()).with_width(6).with_sortable(false),
            TableHeader::new("Spread".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Skew".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Fill Prob".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("IS Sharpe".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("OOS Sharpe".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Sharpe Deg".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Verdict".to_string()).with_width(16).with_sortable(false),
            TableHeader::new("Recommendation".to_string()).with_width(20).with_sortable(false),
        ];

        let rows: Vec<TableRow> = self.result.all_reports
            .iter()
            .enumerate()
            .map(|(idx, report)| {
                TableRow::new(vec![
                    format!("{}", idx + 1),
                    format!("{:.1}", report.params_tested.spread_bps),
                    format!("{:.2}", report.params_tested.skew_factor),
                    format!("{:.2}", report.params_tested.fill_probability),
                    format!("{:.4}", report.in_sample_metrics.sharpe_ratio),
                    format!("{:.4}", report.out_of_sample_metrics.sharpe_ratio),
                    format!("{:.3}", report.comparison.sharpe_degradation),
                    Self::format_verdict(report.overfit_verdict).to_string(),
                    Self::format_recommendation(report.recommendation).to_string(),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create verdicts summary table
    fn create_verdicts_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Verdict".to_string()).with_width(20).with_sortable(false),
            TableHeader::new("Count".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Percentage".to_string()).with_width(12).with_sortable(false),
        ];

        let summary = &self.result.verdict_summary;
        let total = summary.robust_count + summary.mild_overfit_count + summary.moderate_overfit_count 
            + summary.severe_overfit_count + summary.inconclusive_count;

        let rows = vec![
            TableRow::new(vec![
                "Robust".to_string(),
                format!("{}", summary.robust_count),
                format!("{:.1}%", if total > 0 { summary.robust_count as f64 / total as f64 * 100.0 } else { 0.0 }),
            ]),
            TableRow::new(vec![
                "Mild Overfit".to_string(),
                format!("{}", summary.mild_overfit_count),
                format!("{:.1}%", if total > 0 { summary.mild_overfit_count as f64 / total as f64 * 100.0 } else { 0.0 }),
            ]),
            TableRow::new(vec![
                "Moderate Overfit".to_string(),
                format!("{}", summary.moderate_overfit_count),
                format!("{:.1}%", if total > 0 { summary.moderate_overfit_count as f64 / total as f64 * 100.0 } else { 0.0 }),
            ]),
            TableRow::new(vec![
                "Severe Overfit".to_string(),
                format!("{}", summary.severe_overfit_count),
                format!("{:.1}%", if total > 0 { summary.severe_overfit_count as f64 / total as f64 * 100.0 } else { 0.0 }),
            ]),
            TableRow::new(vec![
                "Inconclusive".to_string(),
                format!("{}", summary.inconclusive_count),
                format!("{:.1}%", if total > 0 { summary.inconclusive_count as f64 / total as f64 * 100.0 } else { 0.0 }),
            ]),
        ];

        (headers, rows)
    }

    /// Create Sharpe degradation chart series
    fn create_sharpe_degradation_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.result.all_reports
            .iter()
            .enumerate()
            .map(|(idx, report)| {
                let x = (idx + 1) as f64;
                let y = report.comparison.sharpe_degradation;
                let label = format!("{:.3}", y);
                DataPoint::new(x, y).with_label(label)
            })
            .collect();

        DataSeries::new("Sharpe Degradation".to_string())
            .with_points(points)
            .with_color(Color::Yellow)
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
        let tab_titles: Vec<Line> = OOSValidateViewMode::all()
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
            .select(OOSValidateViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            OOSValidateViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            OOSValidateViewMode::Reports => {
                self.render_reports(f, chunks[1]);
            }
            OOSValidateViewMode::Best => {
                self.render_best(f, chunks[1]);
            }
            OOSValidateViewMode::Verdicts => {
                self.render_verdicts(f, chunks[1]);
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
        let best_sharpe = self.result.best.as_ref()
            .map(|b| b.out_of_sample_metrics.sharpe_ratio)
            .unwrap_or(0.0);
        let avg_sharpe_deg = if !self.result.all_reports.is_empty() {
            self.result.all_reports.iter()
                .map(|r| r.comparison.sharpe_degradation)
                .sum::<f64>() / self.result.all_reports.len() as f64
        } else {
            0.0
        };

        let metrics = vec![
            Metric::new("Total Combinations".to_string(), MetricValue::Number(self.result.total_combinations as f64)),
            Metric::new("Best OOS Sharpe".to_string(), MetricValue::Number(best_sharpe)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Avg Sharpe Degradation".to_string(), MetricValue::Number(avg_sharpe_deg)).with_format(MetricFormat::Decimal(3)),
            Metric::new("Holdout Fraction".to_string(), MetricValue::Number(self.result.holdout * 100.0)).with_format(MetricFormat::Decimal(1)),
            Metric::new("Embargo Hours".to_string(), MetricValue::Number(self.result.embargo_hours)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Chart showing Sharpe degradation across reports
        let series = self.create_sharpe_degradation_series();
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Line)
            .with_series(vec![series])
            .with_x_axis(AxisConfig::default().with_label("Report Rank"))
            .with_y_axis(AxisConfig::default().with_label("Sharpe Degradation"));

        chart.render(area, f.buffer_mut());
    }

    /// Render reports view
    fn render_reports(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_reports_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render best view
    fn render_best(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(8),
                Constraint::Min(0),
            ])
            .split(area);

        if let Some(best) = &self.result.best {
            // Parameters
            let params_text = vec![
                format!("Spread (bps): {:.1}", best.params_tested.spread_bps),
                format!("Skew Factor: {:.2}", best.params_tested.skew_factor),
                format!("Fill Probability: {:.2}", best.params_tested.fill_probability),
                format!("High Entropy Threshold: {:.2}", best.params_tested.high_entropy_threshold),
            ];

            let params_lines: Vec<Line> = params_text.iter().map(|s| Line::from(s.as_str())).collect();
            let params_para = Paragraph::new(params_lines)
                .block(Block::default().borders(Borders::ALL).title("Best Parameters"))
                .alignment(Alignment::Left);
            f.render_widget(params_para, chunks[0]);

            // Performance comparison
            let comp = &best.comparison;
            let comp_text = vec![
                format!("Sharpe Degradation: {:.3}", comp.sharpe_degradation),
                format!("Return Degradation: {:.3}", comp.return_degradation),
                format!("Win Rate Drop: {:.3}", comp.win_rate_drop),
                format!("Trade Frequency Ratio: {:.3}", comp.trade_frequency_ratio),
                format!("Verdict: {}", Self::format_verdict(best.overfit_verdict)),
                format!("Recommendation: {}", Self::format_recommendation(best.recommendation)),
            ];

            let comp_lines: Vec<Line> = comp_text.iter().map(|s| Line::from(s.as_str())).collect();
            let comp_para = Paragraph::new(comp_lines)
                .block(Block::default().borders(Borders::ALL).title("Performance Comparison"))
                .alignment(Alignment::Left);
            f.render_widget(comp_para, chunks[1]);
        } else {
            let text = vec![Line::from("No best configuration found.")];
            let para = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Best Configuration"))
                .alignment(Alignment::Center);
            f.render_widget(para, area);
        }
    }

    /// Render verdicts view
    fn render_verdicts(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_verdicts_table();

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
    use crate::commands::backtest::{
        OOSValidateTestedParams, OOSValidatePerformanceComparison,
        OOSValidateSampleMetrics,
    };

    fn create_test_oos_validate_result() -> OOSValidateResult {
        let mut reports = Vec::new();
        for i in 0..5 {
            reports.push(OOSValidateReport {
                params_tested: OOSValidateTestedParams {
                    spread_bps: 1.0 + i as f64 * 0.5,
                    skew_factor: 0.5 + i as f64 * 0.1,
                    fill_probability: 0.8 + i as f64 * 0.02,
                    high_entropy_threshold: 0.6 + i as f64 * 0.05,
                },
                comparison: OOSValidatePerformanceComparison {
                    sharpe_degradation: 1.0 - i as f64 * 0.1,
                    return_degradation: 0.9 - i as f64 * 0.05,
                    win_rate_drop: 0.02 + i as f64 * 0.01,
                    trade_frequency_ratio: 0.95 - i as f64 * 0.02,
                },
                overfit_verdict: if i == 0 {
                    OOSValidateOverfitVerdict::Robust
                } else if i == 1 {
                    OOSValidateOverfitVerdict::MildOverfit
                } else {
                    OOSValidateOverfitVerdict::ModerateOverfit
                },
                recommendation: if i == 0 {
                    OOSValidateRecommendation::ReadyForPaperTrading
                } else {
                    OOSValidateRecommendation::NeedsMoreData
                },
                in_sample_metrics: OOSValidateSampleMetrics {
                    sharpe_ratio: 2.0 - i as f64 * 0.1,
                    total_return: 0.15 - i as f64 * 0.01,
                    max_drawdown: 0.05,
                    num_trades: 200 - i * 10,
                    win_rate: 0.55,
                    profit_factor: 1.5,
                    avg_trade_pnl: 0.001,
                    time_span_hours: 720.0,
                    num_events: 10000,
                },
                out_of_sample_metrics: OOSValidateSampleMetrics {
                    sharpe_ratio: 1.8 - i as f64 * 0.1,
                    total_return: 0.12 - i as f64 * 0.01,
                    max_drawdown: 0.06,
                    num_trades: 180 - i * 10,
                    win_rate: 0.53,
                    profit_factor: 1.4,
                    avg_trade_pnl: 0.0008,
                    time_span_hours: 180.0,
                    num_events: 2500,
                },
            });
        }

        OOSValidateResult {
            algorithm: "mm".to_string(),
            algorithm_name: "Market Making".to_string(),
            holdout: 0.2,
            embargo_hours: 24.0,
            all_reports: reports.clone(),
            best: reports.first().cloned(),
            total_combinations: 5,
            verdict_summary: OOSValidateVerdictSummary {
                robust_count: 1,
                mild_overfit_count: 1,
                moderate_overfit_count: 3,
                severe_overfit_count: 0,
                inconclusive_count: 0,
                total_count: 5,
            },
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = OOSValidateViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(OOSValidateViewMode::Summary.name(), "Summary");
        assert_eq!(OOSValidateViewMode::Reports.name(), "Reports");
        assert_eq!(OOSValidateViewMode::Best.name(), "Best");
        assert_eq!(OOSValidateViewMode::Verdicts.name(), "Verdicts");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(OOSValidateViewMode::Summary.next(), OOSValidateViewMode::Reports);
        assert_eq!(OOSValidateViewMode::Reports.next(), OOSValidateViewMode::Best);
        assert_eq!(OOSValidateViewMode::Best.next(), OOSValidateViewMode::Verdicts);
        assert_eq!(OOSValidateViewMode::Verdicts.next(), OOSValidateViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        assert_eq!(screen.view_mode(), OOSValidateViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_oos_validate_result();
        let mut screen = BacktestOOSValidateResultsScreen::new(result);
        screen.set_view_mode(OOSValidateViewMode::Reports);
        assert_eq!(screen.view_mode(), OOSValidateViewMode::Reports);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_oos_validate_result();
        let mut screen = BacktestOOSValidateResultsScreen::new(result);
        screen.set_selected_index(Some(2));
        assert_eq!(screen.selected_index(), Some(2));
    }

    #[test]
    fn test_selected_report() {
        let result = create_test_oos_validate_result();
        let mut screen = BacktestOOSValidateResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_report().is_some());
    }

    #[test]
    fn test_export_json() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_oos_validate_result();
        let mut screen = BacktestOOSValidateResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), OOSValidateViewMode::Reports);
    }

    #[test]
    fn test_format_verdict() {
        assert_eq!(BacktestOOSValidateResultsScreen::format_verdict(OOSValidateOverfitVerdict::Robust), "Robust");
        assert_eq!(BacktestOOSValidateResultsScreen::format_verdict(OOSValidateOverfitVerdict::MildOverfit), "Mild Overfit");
    }

    #[test]
    fn test_format_recommendation() {
        assert_eq!(BacktestOOSValidateResultsScreen::format_recommendation(OOSValidateRecommendation::ReadyForPaperTrading), "Ready for Paper Trading");
    }

    #[test]
    fn test_create_reports_table() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        let (headers, rows) = screen.create_reports_table();
        assert_eq!(headers.len(), 9);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_create_verdicts_table() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        let (headers, rows) = screen.create_verdicts_table();
        assert_eq!(headers.len(), 3);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_create_sharpe_degradation_series() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        let series = screen.create_sharpe_degradation_series();
        assert_eq!(series.points.len(), 5);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = OOSValidateViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_reports_access() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        assert_eq!(screen.result().all_reports.len(), 5);
        assert_eq!(screen.result().total_combinations, 5);
    }

    #[test]
    fn test_best_configuration() {
        let result = create_test_oos_validate_result();
        let screen = BacktestOOSValidateResultsScreen::new(result);
        assert!(screen.result().best.is_some());
    }
}
