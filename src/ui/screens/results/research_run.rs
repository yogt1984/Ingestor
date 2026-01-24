//! Research Run Results Screen (T-3.8)
//!
//! TUI screen for displaying research run command results.
//! Supports multiple view modes: Summary, Signals, MIDC, Persistence.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::research::{RunResult, SignalSummary};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for research run results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResearchRunViewMode {
    /// Summary view with key metrics
    Summary,
    /// Top signals table view
    Signals,
    /// MIDC analysis view
    MIDC,
    /// Persistence analysis view
    Persistence,
}

impl ResearchRunViewMode {
    /// Get all view modes
    pub fn all() -> Vec<ResearchRunViewMode> {
        vec![
            ResearchRunViewMode::Summary,
            ResearchRunViewMode::Signals,
            ResearchRunViewMode::MIDC,
            ResearchRunViewMode::Persistence,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            ResearchRunViewMode::Summary => "Summary",
            ResearchRunViewMode::Signals => "Signals",
            ResearchRunViewMode::MIDC => "MIDC",
            ResearchRunViewMode::Persistence => "Persistence",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> ResearchRunViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> ResearchRunViewMode {
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

/// Research run results screen
pub struct ResearchRunResultsScreen {
    /// Research run result data
    result: RunResult,
    /// Current view mode
    view_mode: ResearchRunViewMode,
    /// Selected signal index (for Signals view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl ResearchRunResultsScreen {
    /// Create a new results screen from RunResult
    pub fn new(result: RunResult) -> Self {
        Self {
            result,
            view_mode: ResearchRunViewMode::Summary,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &RunResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> ResearchRunViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: ResearchRunViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.result.top_signals.len() {
                self.selected_index = Some(idx);
            } else {
                self.selected_index = None;
            }
        } else {
            self.selected_index = None;
        }
    }

    /// Get selected signal
    pub fn selected_signal(&self) -> Option<&SignalSummary> {
        self.selected_index
            .and_then(|idx| self.result.top_signals.get(idx))
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
                if self.view_mode == ResearchRunViewMode::Signals {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.top_signals.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == ResearchRunViewMode::Signals {
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

    /// Create signals table
    fn create_signals_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Rank".to_string()).with_width(6).with_sortable(false),
            TableHeader::new("Signature".to_string()).with_width(30).with_sortable(false),
            TableHeader::new("P(Continuation)".to_string()).with_width(16).with_sortable(false),
            TableHeader::new("Samples".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("CI Lower".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("CI Upper".to_string()).with_width(12).with_sortable(false),
        ];

        let rows: Vec<TableRow> = self.result.top_signals
            .iter()
            .enumerate()
            .map(|(idx, signal)| {
                TableRow::new(vec![
                    format!("{}", idx + 1),
                    signal.signature.clone(),
                    format!("{:.4}", signal.p_continuation),
                    format!("{}", signal.sample_count),
                    format!("{:.4}", signal.confidence_lower),
                    format!("{:.4}", signal.confidence_upper),
                ])
            })
            .collect();

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
        let tab_titles: Vec<Line> = ResearchRunViewMode::all()
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
            .select(ResearchRunViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            ResearchRunViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            ResearchRunViewMode::Signals => {
                self.render_signals(f, chunks[1]);
            }
            ResearchRunViewMode::MIDC => {
                self.render_midc(f, chunks[1]);
            }
            ResearchRunViewMode::Persistence => {
                self.render_persistence(f, chunks[1]);
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
        let dashboard_metrics = vec![
            Metric::new("Samples Processed".to_string(), MetricValue::Number(self.result.samples_processed as f64)),
            Metric::new("Duration".to_string(), MetricValue::Number(self.result.duration_seconds / 60.0)).with_format(MetricFormat::Decimal(2)),
            Metric::new("MIDC Kappa".to_string(), MetricValue::Number(self.result.midc_kappa)).with_format(MetricFormat::Decimal(4)),
            Metric::new("MIDC Confidence".to_string(), MetricValue::Number(self.result.midc_confidence)).with_format(MetricFormat::Decimal(4)),
            Metric::new("Top Signals".to_string(), MetricValue::Number(self.result.top_signals.len() as f64)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(dashboard_metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Tradeable assessment
        let assessment_text = vec![
            format!("Tradeable: {}", if self.result.is_tradeable { "Yes" } else { "No" }),
            format!("Reason: {}", self.result.tradeable_reason),
            format!("MIDC Regime: {}", self.result.midc_regime),
            format!("Checkpoints Saved: {}", self.result.checkpoints_saved),
        ];

        let assessment_lines: Vec<Line> = assessment_text.iter().map(|s| Line::from(s.as_str())).collect();
        let assessment_para = Paragraph::new(assessment_lines)
            .block(Block::default().borders(Borders::ALL).title("Assessment"))
            .alignment(Alignment::Left);
        f.render_widget(assessment_para, chunks[1]);
    }

    /// Render signals view
    fn render_signals(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_signals_table();

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render MIDC view
    fn render_midc(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(8),
                Constraint::Min(0),
            ])
            .split(area);

        // MIDC metrics
        let midc_text = vec![
            format!("Kappa (Diffusion Rate): {:.4}", self.result.midc_kappa),
            format!("Confidence Level: {:.4}", self.result.midc_confidence),
            format!("Regime Classification: {}", self.result.midc_regime),
        ];

        let midc_lines: Vec<Line> = midc_text.iter().map(|s| Line::from(s.as_str())).collect();
        let midc_para = Paragraph::new(midc_lines)
            .block(Block::default().borders(Borders::ALL).title("MIDC Analysis"))
            .alignment(Alignment::Left);
        f.render_widget(midc_para, chunks[0]);

        // Interpretation
        let interpretation = match self.result.midc_regime.as_str() {
            "high" => "High diffusion rate indicates efficient market with rapid information incorporation.",
            "medium" => "Medium diffusion rate suggests moderate market efficiency.",
            "low" => "Low diffusion rate indicates potential market inefficiencies and trading opportunities.",
            _ => "Regime classification pending analysis.",
        };

        let interp_text = vec![Line::from(interpretation)];
        let interp_para = Paragraph::new(interp_text)
            .block(Block::default().borders(Borders::ALL).title("Interpretation"))
            .alignment(Alignment::Left);
        f.render_widget(interp_para, chunks[1]);
    }

    /// Render persistence view
    fn render_persistence(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(0),
            ])
            .split(area);

        // Persistence metrics
        let persistence_text = vec![
            format!("Mean Duration: {:.2} seconds", self.result.persistence_mean_seconds),
            format!("Sample Count: {}", self.result.persistence_sample_count),
        ];

        let persistence_lines: Vec<Line> = persistence_text.iter().map(|s| Line::from(s.as_str())).collect();
        let persistence_para = Paragraph::new(persistence_lines)
            .block(Block::default().borders(Borders::ALL).title("Persistence Analysis"))
            .alignment(Alignment::Left);
        f.render_widget(persistence_para, chunks[0]);

        // Interpretation
        let interpretation = if self.result.persistence_sample_count > 0 {
            format!(
                "Mean persistence duration of {:.2} seconds based on {} samples. \
                Higher persistence suggests more predictable market behavior.",
                self.result.persistence_mean_seconds,
                self.result.persistence_sample_count
            )
        } else {
            "Insufficient data for persistence analysis.".to_string()
        };

        let interp_text = vec![Line::from(interpretation.as_str())];
        let interp_para = Paragraph::new(interp_text)
            .block(Block::default().borders(Borders::ALL).title("Interpretation"))
            .alignment(Alignment::Left);
        f.render_widget(interp_para, chunks[1]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_research_run_result() -> RunResult {
        let mut top_signals = Vec::new();
        for i in 0..5 {
            top_signals.push(SignalSummary {
                signature: format!("signal_{}", i + 1),
                p_continuation: 0.6 + i as f64 * 0.05,
                sample_count: 100 + i * 20,
                confidence_lower: 0.55 + i as f64 * 0.05,
                confidence_upper: 0.65 + i as f64 * 0.05,
            });
        }

        RunResult {
            samples_processed: 10000,
            duration_seconds: 300.0,
            midc_kappa: 0.5,
            midc_confidence: 0.95,
            midc_regime: "medium".to_string(),
            persistence_mean_seconds: 2.5,
            persistence_sample_count: 500,
            top_signals,
            is_tradeable: true,
            tradeable_reason: "Market shows sufficient signals and acceptable MIDC regime".to_string(),
            checkpoints_saved: 10,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = ResearchRunViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(ResearchRunViewMode::Summary.name(), "Summary");
        assert_eq!(ResearchRunViewMode::Signals.name(), "Signals");
        assert_eq!(ResearchRunViewMode::MIDC.name(), "MIDC");
        assert_eq!(ResearchRunViewMode::Persistence.name(), "Persistence");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(ResearchRunViewMode::Summary.next(), ResearchRunViewMode::Signals);
        assert_eq!(ResearchRunViewMode::Signals.next(), ResearchRunViewMode::MIDC);
        assert_eq!(ResearchRunViewMode::MIDC.next(), ResearchRunViewMode::Persistence);
        assert_eq!(ResearchRunViewMode::Persistence.next(), ResearchRunViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_research_run_result();
        let screen = ResearchRunResultsScreen::new(result);
        assert_eq!(screen.view_mode(), ResearchRunViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_research_run_result();
        let mut screen = ResearchRunResultsScreen::new(result);
        screen.set_view_mode(ResearchRunViewMode::Signals);
        assert_eq!(screen.view_mode(), ResearchRunViewMode::Signals);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_research_run_result();
        let mut screen = ResearchRunResultsScreen::new(result);
        screen.set_selected_index(Some(2));
        assert_eq!(screen.selected_index(), Some(2));
    }

    #[test]
    fn test_selected_signal() {
        let result = create_test_research_run_result();
        let mut screen = ResearchRunResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_signal().is_some());
    }

    #[test]
    fn test_export_json() {
        let result = create_test_research_run_result();
        let screen = ResearchRunResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"samples_processed\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_research_run_result();
        let mut screen = ResearchRunResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), ResearchRunViewMode::Signals);
    }

    #[test]
    fn test_create_signals_table() {
        let result = create_test_research_run_result();
        let screen = ResearchRunResultsScreen::new(result);
        let (headers, rows) = screen.create_signals_table();
        assert_eq!(headers.len(), 6);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = ResearchRunViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_result_access() {
        let result = create_test_research_run_result();
        let screen = ResearchRunResultsScreen::new(result);
        assert_eq!(screen.result().samples_processed, 10000);
        assert_eq!(screen.result().top_signals.len(), 5);
    }

    #[test]
    fn test_midc_metrics() {
        let result = create_test_research_run_result();
        let screen = ResearchRunResultsScreen::new(result);
        assert!(screen.result().midc_kappa > 0.0);
        assert!(screen.result().midc_confidence > 0.0);
    }
}
