//! Backtest Paper Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest paper command results.
//! Supports multiple view modes: Summary, Metrics, Details, Warnings.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rust_decimal::prelude::*;

use crate::commands::backtest::PaperResult;
use crate::backtest::session_runner::{SessionResult, SessionState};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for paper results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PaperViewMode {
    /// Summary view with key metrics
    Summary,
    /// Detailed metrics view
    Metrics,
    /// Session details view
    Details,
    /// Warnings view
    Warnings,
}

impl PaperViewMode {
    /// Get all view modes
    pub fn all() -> Vec<PaperViewMode> {
        vec![
            PaperViewMode::Summary,
            PaperViewMode::Metrics,
            PaperViewMode::Details,
            PaperViewMode::Warnings,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            PaperViewMode::Summary => "Summary",
            PaperViewMode::Metrics => "Metrics",
            PaperViewMode::Details => "Details",
            PaperViewMode::Warnings => "Warnings",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> PaperViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> PaperViewMode {
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

/// Backtest paper results screen
pub struct BacktestPaperResultsScreen {
    /// Paper result data
    result: PaperResult,
    /// Current view mode
    view_mode: PaperViewMode,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestPaperResultsScreen {
    /// Create a new results screen from PaperResult
    pub fn new(result: PaperResult) -> Self {
        Self {
            result,
            view_mode: PaperViewMode::Summary,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &PaperResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> PaperViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: PaperViewMode) {
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

    /// Format session state
    fn format_state(state: &SessionState) -> &'static str {
        match state {
            SessionState::Pending => "Pending",
            SessionState::Running => "Running",
            SessionState::Completed => "Completed",
            SessionState::Stopped => "Stopped",
            SessionState::Failed => "Failed",
        }
    }

    /// Create metrics table
    fn create_metrics_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Metric".to_string()).with_width(30).with_sortable(false),
            TableHeader::new("Value".to_string()).with_width(20).with_sortable(false),
        ];

        let metrics = &self.result.session_result.summary.metrics;
        let rows = vec![
            TableRow::new(vec![
                "Total Trades".to_string(),
                format!("{}", metrics.total_trades),
            ]),
            TableRow::new(vec![
                "Buy / Sell Trades".to_string(),
                format!("{} / {}", metrics.buy_trades, metrics.sell_trades),
            ]),
            TableRow::new(vec![
                "Net PnL".to_string(),
                format!("{:.4}", metrics.net_pnl.to_f64().unwrap_or(0.0)),
            ]),
            TableRow::new(vec![
                "Realized PnL".to_string(),
                format!("{:.4}", metrics.realized_pnl.to_f64().unwrap_or(0.0)),
            ]),
            TableRow::new(vec![
                "Unrealized PnL".to_string(),
                format!("{:.4}", metrics.unrealized_pnl.to_f64().unwrap_or(0.0)),
            ]),
            TableRow::new(vec![
                "Total Fees".to_string(),
                format!("{:.4}", metrics.total_fees.to_f64().unwrap_or(0.0)),
            ]),
            TableRow::new(vec![
                "Sharpe Ratio".to_string(),
                format!("{:.4}", metrics.sharpe_ratio),
            ]),
            TableRow::new(vec![
                "Win Rate".to_string(),
                format!("{:.2}%", metrics.win_rate * 100.0),
            ]),
            TableRow::new(vec![
                "Profit Factor".to_string(),
                format!("{:.2}", metrics.profit_factor),
            ]),
            TableRow::new(vec![
                "Max Drawdown".to_string(),
                format!("{:.2}%", metrics.max_drawdown * 100.0),
            ]),
            TableRow::new(vec![
                "Bid Fill Rate".to_string(),
                format!("{:.2}%", metrics.bid_fill_rate * 100.0),
            ]),
            TableRow::new(vec![
                "Ask Fill Rate".to_string(),
                format!("{:.2}%", metrics.ask_fill_rate * 100.0),
            ]),
            TableRow::new(vec![
                "Avg Slippage".to_string(),
                format!("{:.2} bps", metrics.avg_slippage_bps),
            ]),
            TableRow::new(vec![
                "Current Inventory".to_string(),
                format!("{:.4}", metrics.inventory.to_f64().unwrap_or(0.0)),
            ]),
            TableRow::new(vec![
                "Peak Inventory".to_string(),
                format!("{:.4}", metrics.peak_inventory.to_f64().unwrap_or(0.0)),
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
        let tab_titles: Vec<Line> = PaperViewMode::all()
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
            .select(PaperViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            PaperViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            PaperViewMode::Metrics => {
                self.render_metrics(f, chunks[1]);
            }
            PaperViewMode::Details => {
                self.render_details(f, chunks[1]);
            }
            PaperViewMode::Warnings => {
                self.render_warnings(f, chunks[1]);
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
        let metrics = &self.result.session_result.summary.metrics;
        let dashboard_metrics = vec![
            Metric::new("Total Trades".to_string(), MetricValue::Number(metrics.total_trades as f64)),
            Metric::new("Net PnL".to_string(), MetricValue::Number(metrics.net_pnl.to_f64().unwrap_or(0.0))).with_format(MetricFormat::Decimal(4)),
            Metric::new("Sharpe Ratio".to_string(), MetricValue::Number(metrics.sharpe_ratio)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Win Rate".to_string(), MetricValue::Number(metrics.win_rate * 100.0)).with_format(MetricFormat::Decimal(2)),
            Metric::new("Fill Rate".to_string(), MetricValue::Number((metrics.bid_fill_rate + metrics.ask_fill_rate) / 2.0 * 100.0)).with_format(MetricFormat::Decimal(2)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(dashboard_metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Session info
        let session = &self.result.session_result;
        let info_text = vec![
            format!("Session ID: {}", session.summary.session_id),
            format!("State: {}", Self::format_state(&session.final_state)),
            format!("Events Processed: {}", session.events_processed),
            format!("Duration: {:.1} minutes", metrics.duration_secs / 60.0),
            format!("Valid for Validation: {}", if self.result.is_valid_for_validation { "Yes" } else { "No" }),
        ];

        let info_lines: Vec<Line> = info_text.iter().map(|s| Line::from(s.as_str())).collect();
        let info_para = Paragraph::new(info_lines)
            .block(Block::default().borders(Borders::ALL).title("Session Information"))
            .alignment(Alignment::Left);
        f.render_widget(info_para, chunks[1]);
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

    /// Render details view
    fn render_details(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(8),
                Constraint::Min(0),
            ])
            .split(area);

        let session = &self.result.session_result;
        let metrics = &session.summary.metrics;

        // Session details
        let details_text = vec![
            format!("Algorithm: {} ({})", self.result.algorithm_name, self.result.algorithm),
            format!("Session ID: {}", session.summary.session_id),
            format!("Final State: {}", Self::format_state(&session.final_state)),
            format!("Events Processed: {}", session.events_processed),
            format!("Trade Count: {}", session.summary.trade_count),
            format!("Duration: {:.2} hours", metrics.duration_secs / 3600.0),
            format!("Summary Path: {}", session.summary_path.display()),
        ];

        let details_lines: Vec<Line> = details_text.iter().map(|s| Line::from(s.as_str())).collect();
        let details_para = Paragraph::new(details_lines)
            .block(Block::default().borders(Borders::ALL).title("Session Details"))
            .alignment(Alignment::Left);
        f.render_widget(details_para, chunks[0]);

        // Trading activity
        let activity_text = vec![
            format!("Total Volume: {:.4}", metrics.total_volume.to_f64().unwrap_or(0.0)),
            format!("Quotes Generated: {}", metrics.quotes_generated),
            format!("Bid Touches: {}", metrics.bid_touches),
            format!("Ask Touches: {}", metrics.ask_touches),
            format!("Winning Trades: {}", metrics.winning_trades),
            format!("Losing Trades: {}", metrics.losing_trades),
            format!("Avg Trade PnL: {:.4}", metrics.avg_trade_pnl.to_f64().unwrap_or(0.0)),
        ];

        let activity_lines: Vec<Line> = activity_text.iter().map(|s| Line::from(s.as_str())).collect();
        let activity_para = Paragraph::new(activity_lines)
            .block(Block::default().borders(Borders::ALL).title("Trading Activity"))
            .alignment(Alignment::Left);
        f.render_widget(activity_para, chunks[1]);
    }

    /// Render warnings view
    fn render_warnings(&self, f: &mut Frame, area: Rect) {
        let warnings = &self.result.session_result.warnings;
        
        if warnings.is_empty() {
            let text = vec![Line::from("No warnings.")];
            let para = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Warnings"))
                .alignment(Alignment::Center);
            f.render_widget(para, area);
        } else {
            let warning_strings: Vec<String> = warnings.iter()
                .enumerate()
                .map(|(i, w)| format!("{}. {}", i + 1, w))
                .collect();
            let warning_lines: Vec<Line> = warning_strings.iter()
                .map(|s| Line::from(s.as_str()))
                .collect();
            let para = Paragraph::new(warning_lines)
                .block(Block::default().borders(Borders::ALL).title("Warnings"))
                .alignment(Alignment::Left);
            f.render_widget(para, area);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use crate::forward_testing::{SessionSummary, SessionMetrics, ForwardTestConfig};

    fn create_test_paper_result() -> PaperResult {
        let metrics = SessionMetrics {
            start_time: Some(Utc::now()),
            duration_secs: 3600.0,
            total_trades: 50,
            buy_trades: 25,
            sell_trades: 25,
            total_volume: dec!(10.0),
            gross_pnl: dec!(0.1),
            total_fees: dec!(0.01),
            net_pnl: dec!(0.09),
            realized_pnl: dec!(0.08),
            unrealized_pnl: dec!(0.01),
            inventory: dec!(0.5),
            peak_inventory: dec!(1.0),
            max_drawdown: 0.02,
            peak_equity: dec!(1.0),
            win_rate: 0.55,
            winning_trades: 28,
            losing_trades: 22,
            avg_trade_pnl: dec!(0.0018),
            sharpe_ratio: 1.5,
            profit_factor: 1.3,
            avg_slippage_bps: 0.5,
            bid_fill_rate: 0.08,
            ask_fill_rate: 0.09,
            quotes_generated: 500,
            bid_touches: 200,
            ask_touches: 180,
        };

        PaperResult {
            algorithm: "mm".to_string(),
            algorithm_name: "Market Making".to_string(),
            session_result: SessionResult {
                summary: SessionSummary {
                    session_id: "test-session-1".to_string(),
                    config: ForwardTestConfig::default(),
                    metrics,
                    trade_count: 50,
                },
                final_state: SessionState::Completed,
                events_processed: 10000,
                summary_path: PathBuf::from("./data/sessions/test.json"),
                trades_path: Some(PathBuf::from("./data/sessions/test_trades.json")),
                warnings: vec![
                    "Low fill rate detected".to_string(),
                ],
                is_valid_for_validation: true,
            },
            events_processed: 10000,
            is_valid_for_validation: true,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = PaperViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(PaperViewMode::Summary.name(), "Summary");
        assert_eq!(PaperViewMode::Metrics.name(), "Metrics");
        assert_eq!(PaperViewMode::Details.name(), "Details");
        assert_eq!(PaperViewMode::Warnings.name(), "Warnings");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(PaperViewMode::Summary.next(), PaperViewMode::Metrics);
        assert_eq!(PaperViewMode::Metrics.next(), PaperViewMode::Details);
        assert_eq!(PaperViewMode::Details.next(), PaperViewMode::Warnings);
        assert_eq!(PaperViewMode::Warnings.next(), PaperViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_paper_result();
        let screen = BacktestPaperResultsScreen::new(result);
        assert_eq!(screen.view_mode(), PaperViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_paper_result();
        let mut screen = BacktestPaperResultsScreen::new(result);
        screen.set_view_mode(PaperViewMode::Metrics);
        assert_eq!(screen.view_mode(), PaperViewMode::Metrics);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_paper_result();
        let screen = BacktestPaperResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_paper_result();
        let mut screen = BacktestPaperResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), PaperViewMode::Metrics);
    }

    #[test]
    fn test_format_state() {
        assert_eq!(BacktestPaperResultsScreen::format_state(&SessionState::Completed), "Completed");
        assert_eq!(BacktestPaperResultsScreen::format_state(&SessionState::Failed), "Failed");
    }

    #[test]
    fn test_create_metrics_table() {
        let result = create_test_paper_result();
        let screen = BacktestPaperResultsScreen::new(result);
        let (headers, rows) = screen.create_metrics_table();
        assert_eq!(headers.len(), 2);
        assert_eq!(rows.len(), 15);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = PaperViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_session_result_access() {
        let result = create_test_paper_result();
        let screen = BacktestPaperResultsScreen::new(result);
        assert_eq!(screen.result().session_result.summary.metrics.total_trades, 50);
        assert_eq!(screen.result().events_processed, 10000);
    }

    #[test]
    fn test_session_metrics() {
        let result = create_test_paper_result();
        let screen = BacktestPaperResultsScreen::new(result);
        let metrics = &screen.result().session_result.summary.metrics;
        assert!(metrics.sharpe_ratio > 0.0);
        assert!(metrics.win_rate > 0.0);
    }
}
