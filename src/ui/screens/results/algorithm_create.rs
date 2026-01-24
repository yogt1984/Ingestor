//! Algorithm Create Results Screen (T-3.8)
//!
//! TUI screen for displaying algorithm create command results.
//! Supports multiple view modes: Summary, Config, Validation, Details.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::algorithm::{CreateResult, ValidationSummary};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for algorithm create results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlgorithmCreateViewMode {
    /// Summary view with key metrics
    Summary,
    /// Configuration details view
    Config,
    /// Validation results view
    Validation,
    /// Detailed information view
    Details,
}

impl AlgorithmCreateViewMode {
    /// Get all view modes
    pub fn all() -> Vec<AlgorithmCreateViewMode> {
        vec![
            AlgorithmCreateViewMode::Summary,
            AlgorithmCreateViewMode::Config,
            AlgorithmCreateViewMode::Validation,
            AlgorithmCreateViewMode::Details,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            AlgorithmCreateViewMode::Summary => "Summary",
            AlgorithmCreateViewMode::Config => "Config",
            AlgorithmCreateViewMode::Validation => "Validation",
            AlgorithmCreateViewMode::Details => "Details",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> AlgorithmCreateViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> AlgorithmCreateViewMode {
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

/// Algorithm create results screen
pub struct AlgorithmCreateResultsScreen {
    /// Algorithm create result data
    result: CreateResult,
    /// Current view mode
    view_mode: AlgorithmCreateViewMode,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl AlgorithmCreateResultsScreen {
    /// Create a new results screen from CreateResult
    pub fn new(result: CreateResult) -> Self {
        Self {
            result,
            view_mode: AlgorithmCreateViewMode::Summary,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &CreateResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> AlgorithmCreateViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: AlgorithmCreateViewMode) {
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

    /// Create validation stages table
    fn create_validation_stages_table(&self) -> Option<(Vec<TableHeader>, Vec<TableRow>)> {
        let validation = self.result.validation_result.as_ref()?;
        
        let headers = vec![
            TableHeader::new("Stage".to_string()).with_width(30).with_sortable(false),
        ];

        let rows: Vec<TableRow> = validation.stages_run
            .iter()
            .map(|stage| {
                TableRow::new(vec![stage.clone()])
            })
            .collect();

        Some((headers, rows))
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
        let tab_titles: Vec<Line> = AlgorithmCreateViewMode::all()
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
            .select(AlgorithmCreateViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            AlgorithmCreateViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            AlgorithmCreateViewMode::Config => {
                self.render_config(f, chunks[1]);
            }
            AlgorithmCreateViewMode::Validation => {
                self.render_validation(f, chunks[1]);
            }
            AlgorithmCreateViewMode::Details => {
                self.render_details(f, chunks[1]);
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
        let mut dashboard_metrics = vec![
            Metric::new("Success".to_string(), MetricValue::Number(if self.result.success { 1.0 } else { 0.0 })),
            Metric::new("Version".to_string(), MetricValue::Number(self.result.version as f64)),
            Metric::new("Duration".to_string(), MetricValue::Number(self.result.duration_seconds)).with_format(MetricFormat::Decimal(2)),
        ];

        if let Some(validation) = &self.result.validation_result {
            if let Some(sharpe) = validation.sharpe {
                dashboard_metrics.push(Metric::new("Validation Sharpe".to_string(), MetricValue::Number(sharpe)).with_format(MetricFormat::Decimal(4)));
            }
        }

        let dashboard = MetricsDashboardWidget::new().with_metrics(dashboard_metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Summary info
        let info_text = vec![
            format!("Config ID: {}", self.result.config_id),
            format!("Config Name: {}", self.result.config_name),
            format!("Strategy Type: {}", self.result.strategy_type),
            format!("Symbol: {}", self.result.symbol),
            format!("Message: {}", self.result.message),
        ];

        let info_lines: Vec<Line> = info_text.iter().map(|s| Line::from(s.as_str())).collect();
        let info_para = Paragraph::new(info_lines)
            .block(Block::default().borders(Borders::ALL).title("Summary"))
            .alignment(Alignment::Left);
        f.render_widget(info_para, chunks[1]);
    }

    /// Render config view
    fn render_config(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(10),
                Constraint::Min(0),
            ])
            .split(area);

        // Configuration details
        let config_text = vec![
            format!("Config ID: {}", self.result.config_id),
            format!("Config Name: {}", self.result.config_name),
            format!("Strategy Type: {}", self.result.strategy_type),
            format!("Symbol: {}", self.result.symbol),
            format!("Version: {}", self.result.version),
            format!("Source Research ID: {}", self.result.source_research_id.as_ref().map(|s| s.as_str()).unwrap_or("N/A")),
            format!("Saved Path: {}", self.result.saved_path.as_ref().map(|s| s.as_str()).unwrap_or("N/A")),
        ];

        let config_lines: Vec<Line> = config_text.iter().map(|s| Line::from(s.as_str())).collect();
        let config_para = Paragraph::new(config_lines)
            .block(Block::default().borders(Borders::ALL).title("Configuration"))
            .alignment(Alignment::Left);
        f.render_widget(config_para, chunks[0]);

        // Status
        let status_text = vec![
            format!("Success: {}", if self.result.success { "Yes" } else { "No" }),
            format!("Message: {}", self.result.message),
        ];

        let status_lines: Vec<Line> = status_text.iter().map(|s| Line::from(s.as_str())).collect();
        let status_para = Paragraph::new(status_lines)
            .block(Block::default().borders(Borders::ALL).title("Status"))
            .alignment(Alignment::Left);
        f.render_widget(status_para, chunks[1]);
    }

    /// Render validation view
    fn render_validation(&self, f: &mut Frame, area: Rect) {
        if let Some(validation) = &self.result.validation_result {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(8),
                    Constraint::Min(0),
                ])
                .split(area);

            // Validation metrics
            let mut metrics_text = vec![
                format!("Passed: {}", if validation.passed { "Yes" } else { "No" }),
                format!("Message: {}", validation.message),
            ];

            if let Some(sharpe) = validation.sharpe {
                metrics_text.push(format!("Sharpe Ratio: {:.4}", sharpe));
            }
            if let Some(dd) = validation.max_drawdown {
                metrics_text.push(format!("Max Drawdown: {:.2}%", dd * 100.0));
            }
            if let Some(trades) = validation.trade_count {
                metrics_text.push(format!("Trade Count: {}", trades));
            }

            let metrics_lines: Vec<Line> = metrics_text.iter().map(|s| Line::from(s.as_str())).collect();
            let metrics_para = Paragraph::new(metrics_lines)
                .block(Block::default().borders(Borders::ALL).title("Validation Metrics"))
                .alignment(Alignment::Left);
            f.render_widget(metrics_para, chunks[0]);

            // Stages run
            if let Some((headers, rows)) = self.create_validation_stages_table() {
                let mut table = TableWidget::new()
                    .with_headers(headers)
                    .with_rows(rows);
                table.set_focused(self.focused);
                table.render(chunks[1], f.buffer_mut());
            } else {
                let text = vec![Line::from("No validation stages data.")];
                let para = Paragraph::new(text)
                    .block(Block::default().borders(Borders::ALL).title("Validation Stages"))
                    .alignment(Alignment::Center);
                f.render_widget(para, chunks[1]);
            }
        } else {
            let text = vec![Line::from("No validation was performed.")];
            let para = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Validation Results"))
                .alignment(Alignment::Center);
            f.render_widget(para, area);
        }
    }

    /// Render details view
    fn render_details(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(12),
                Constraint::Min(0),
            ])
            .split(area);

        // All details
        let mut details_text = vec![
            format!("Config ID: {}", self.result.config_id),
            format!("Config Name: {}", self.result.config_name),
            format!("Strategy Type: {}", self.result.strategy_type),
            format!("Symbol: {}", self.result.symbol),
            format!("Version: {}", self.result.version),
            format!("Source Research ID: {}", self.result.source_research_id.as_ref().map(|s| s.as_str()).unwrap_or("N/A")),
            format!("Saved Path: {}", self.result.saved_path.as_ref().map(|s| s.as_str()).unwrap_or("N/A")),
            format!("Success: {}", if self.result.success { "Yes" } else { "No" }),
            format!("Duration: {:.2} seconds", self.result.duration_seconds),
            format!("Message: {}", self.result.message),
        ];

        if let Some(validation) = &self.result.validation_result {
            details_text.push(format!("Validation Passed: {}", if validation.passed { "Yes" } else { "No" }));
            if let Some(sharpe) = validation.sharpe {
                details_text.push(format!("Validation Sharpe: {:.4}", sharpe));
            }
        }

        let details_lines: Vec<Line> = details_text.iter().map(|s| Line::from(s.as_str())).collect();
        let details_para = Paragraph::new(details_lines)
            .block(Block::default().borders(Borders::ALL).title("All Details"))
            .alignment(Alignment::Left);
        f.render_widget(details_para, chunks[0]);

        // Validation details if available
        if let Some(validation) = &self.result.validation_result {
            let mut validation_text = vec![
                format!("Validation Message: {}", validation.message),
            ];

            if !validation.stages_run.is_empty() {
                validation_text.push("Stages Run:".to_string());
                for stage in &validation.stages_run {
                    validation_text.push(format!("  - {}", stage));
                }
            }

            let validation_lines: Vec<Line> = validation_text.iter().map(|s| Line::from(s.as_str())).collect();
            let validation_para = Paragraph::new(validation_lines)
                .block(Block::default().borders(Borders::ALL).title("Validation Details"))
                .alignment(Alignment::Left);
            f.render_widget(validation_para, chunks[1]);
        } else {
            let text = vec![Line::from("No validation details available.")];
            let para = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Validation Details"))
                .alignment(Alignment::Center);
            f.render_widget(para, chunks[1]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_algorithm_create_result() -> CreateResult {
        CreateResult {
            success: true,
            config_id: "test-config-1".to_string(),
            config_name: "Test Algorithm".to_string(),
            strategy_type: "mm".to_string(),
            symbol: "BTCUSDT".to_string(),
            version: 1,
            source_research_id: Some("research-1".to_string()),
            saved_path: Some("./data/configs/test-config-1.json".to_string()),
            validation_result: Some(ValidationSummary {
                passed: true,
                stages_run: vec!["Backtest".to_string(), "Forward".to_string()],
                sharpe: Some(1.5),
                max_drawdown: Some(0.05),
                trade_count: Some(100),
                message: "Validation passed".to_string(),
            }),
            duration_seconds: 30.0,
            message: "Config created successfully".to_string(),
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = AlgorithmCreateViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(AlgorithmCreateViewMode::Summary.name(), "Summary");
        assert_eq!(AlgorithmCreateViewMode::Config.name(), "Config");
        assert_eq!(AlgorithmCreateViewMode::Validation.name(), "Validation");
        assert_eq!(AlgorithmCreateViewMode::Details.name(), "Details");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(AlgorithmCreateViewMode::Summary.next(), AlgorithmCreateViewMode::Config);
        assert_eq!(AlgorithmCreateViewMode::Config.next(), AlgorithmCreateViewMode::Validation);
        assert_eq!(AlgorithmCreateViewMode::Validation.next(), AlgorithmCreateViewMode::Details);
        assert_eq!(AlgorithmCreateViewMode::Details.next(), AlgorithmCreateViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_algorithm_create_result();
        let screen = AlgorithmCreateResultsScreen::new(result);
        assert_eq!(screen.view_mode(), AlgorithmCreateViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_algorithm_create_result();
        let mut screen = AlgorithmCreateResultsScreen::new(result);
        screen.set_view_mode(AlgorithmCreateViewMode::Config);
        assert_eq!(screen.view_mode(), AlgorithmCreateViewMode::Config);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_algorithm_create_result();
        let screen = AlgorithmCreateResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"config_id\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_algorithm_create_result();
        let mut screen = AlgorithmCreateResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), AlgorithmCreateViewMode::Config);
    }

    #[test]
    fn test_create_validation_stages_table() {
        let result = create_test_algorithm_create_result();
        let screen = AlgorithmCreateResultsScreen::new(result);
        let table = screen.create_validation_stages_table();
        assert!(table.is_some());
        let (headers, rows) = table.unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = AlgorithmCreateViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_result_access() {
        let result = create_test_algorithm_create_result();
        let screen = AlgorithmCreateResultsScreen::new(result);
        assert_eq!(screen.result().config_id, "test-config-1");
        assert!(screen.result().success);
    }

    #[test]
    fn test_validation_result() {
        let result = create_test_algorithm_create_result();
        let screen = AlgorithmCreateResultsScreen::new(result);
        assert!(screen.result().validation_result.is_some());
        let validation = screen.result().validation_result.as_ref().unwrap();
        assert!(validation.passed);
    }
}
