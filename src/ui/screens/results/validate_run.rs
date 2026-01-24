//! Validate Run Results Screen (T-3.8)
//!
//! TUI screen for displaying validate run command results.
//! Supports multiple view modes: Summary, Stages, Details, Warnings.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::validate::RunResult;
use crate::validation::{PipelineResult, PipelineStatus, StageOutcome};
use crate::core::ValidationStageType;
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for validate run results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidateRunViewMode {
    /// Summary view with key metrics
    Summary,
    /// Stages table view
    Stages,
    /// Pipeline details view
    Details,
    /// Warnings view
    Warnings,
}

impl ValidateRunViewMode {
    /// Get all view modes
    pub fn all() -> Vec<ValidateRunViewMode> {
        vec![
            ValidateRunViewMode::Summary,
            ValidateRunViewMode::Stages,
            ValidateRunViewMode::Details,
            ValidateRunViewMode::Warnings,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            ValidateRunViewMode::Summary => "Summary",
            ValidateRunViewMode::Stages => "Stages",
            ValidateRunViewMode::Details => "Details",
            ValidateRunViewMode::Warnings => "Warnings",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> ValidateRunViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> ValidateRunViewMode {
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

/// Validate run results screen
pub struct ValidateRunResultsScreen {
    /// Validate run result data
    result: RunResult,
    /// Current view mode
    view_mode: ValidateRunViewMode,
    /// Selected stage index (for Stages view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl ValidateRunResultsScreen {
    /// Create a new results screen from RunResult
    pub fn new(result: RunResult) -> Self {
        Self {
            result,
            view_mode: ValidateRunViewMode::Summary,
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
    pub fn view_mode(&self) -> ValidateRunViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: ValidateRunViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            let stages = &self.result.pipeline_result.execution_order;
            if idx < stages.len() {
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
                if self.view_mode == ValidateRunViewMode::Stages {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    let max_idx = self.result.pipeline_result.execution_order.len().saturating_sub(1);
                    self.set_selected_index(Some(new_idx.min(max_idx)));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == ValidateRunViewMode::Stages {
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

    /// Format pipeline status
    fn format_status(status: &PipelineStatus) -> &'static str {
        match status {
            PipelineStatus::Pending => "Pending",
            PipelineStatus::Running => "Running",
            PipelineStatus::Passed => "Passed",
            PipelineStatus::Failed => "Failed",
            PipelineStatus::Cancelled => "Cancelled",
            PipelineStatus::Error => "Error",
        }
    }

    /// Format stage type
    fn format_stage_type(stage_type: &ValidationStageType) -> &'static str {
        match stage_type {
            ValidationStageType::Backtest => "Backtest",
            ValidationStageType::Forward => "Forward",
            ValidationStageType::OutOfSample => "OutOfSample",
            ValidationStageType::Paper => "Paper",
            ValidationStageType::Live => "Live",
        }
    }

    /// Format stage outcome
    fn format_outcome(outcome: &StageOutcome) -> &'static str {
        match outcome {
            StageOutcome::Passed(_) => "Passed",
            StageOutcome::Failed(_) => "Failed",
            StageOutcome::Error(_) => "Error",
            StageOutcome::Skipped(_) => "Skipped",
            StageOutcome::Pending => "Pending",
        }
    }

    /// Create stages table
    fn create_stages_table(&self) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Order".to_string()).with_width(6).with_sortable(false),
            TableHeader::new("Stage".to_string()).with_width(12).with_sortable(false),
            TableHeader::new("Outcome".to_string()).with_width(10).with_sortable(false),
            TableHeader::new("Status".to_string()).with_width(20).with_sortable(false),
        ];

        let pipeline = &self.result.pipeline_result;
        let rows: Vec<TableRow> = pipeline.execution_order
            .iter()
            .enumerate()
            .map(|(idx, stage_type)| {
                let outcome = pipeline.stage_outcomes.get(stage_type);
                let outcome_str = outcome.map(|o| Self::format_outcome(o)).unwrap_or("N/A");
                let status_str = match outcome {
                    Some(StageOutcome::Passed(_)) => "All thresholds met",
                    Some(StageOutcome::Failed(_)) => "Thresholds not met",
                    Some(StageOutcome::Error(msg)) => msg.as_str(),
                    Some(StageOutcome::Skipped(msg)) => msg.as_str(),
                    Some(StageOutcome::Pending) => "Pending execution",
                    None => "Not executed",
                };

                TableRow::new(vec![
                    format!("{}", idx + 1),
                    Self::format_stage_type(stage_type).to_string(),
                    outcome_str.to_string(),
                    status_str.to_string(),
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
        let tab_titles: Vec<Line> = ValidateRunViewMode::all()
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
            .select(ValidateRunViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            ValidateRunViewMode::Summary => {
                self.render_summary(f, chunks[1]);
            }
            ValidateRunViewMode::Stages => {
                self.render_stages(f, chunks[1]);
            }
            ValidateRunViewMode::Details => {
                self.render_details(f, chunks[1]);
            }
            ValidateRunViewMode::Warnings => {
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
        let pipeline = &self.result.pipeline_result;
        let dashboard_metrics = vec![
            Metric::new("Stages Passed".to_string(), MetricValue::Number(pipeline.stages_passed as f64)),
            Metric::new("Stages Failed".to_string(), MetricValue::Number(pipeline.stages_failed as f64)),
            Metric::new("Stages Skipped".to_string(), MetricValue::Number(pipeline.stages_skipped as f64)),
            Metric::new("Duration".to_string(), MetricValue::Number(self.result.duration_seconds / 60.0)).with_format(MetricFormat::Decimal(2)),
            Metric::new("Total Stages".to_string(), MetricValue::Number(pipeline.execution_order.len() as f64)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(dashboard_metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Pipeline info
        let info_text = vec![
            format!("Pipeline ID: {}", pipeline.id),
            format!("Status: {}", Self::format_status(&pipeline.status)),
            format!("Algorithm: {} ({})", self.result.algorithm_name, self.result.algorithm_config_id),
            format!("Summary: {}", pipeline.summary),
        ];

        let info_lines: Vec<Line> = info_text.iter().map(|s| Line::from(s.as_str())).collect();
        let info_para = Paragraph::new(info_lines)
            .block(Block::default().borders(Borders::ALL).title("Pipeline Information"))
            .alignment(Alignment::Left);
        f.render_widget(info_para, chunks[1]);
    }

    /// Render stages view
    fn render_stages(&self, f: &mut Frame, area: Rect) {
        let (headers, rows) = self.create_stages_table();

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
                Constraint::Length(10),
                Constraint::Min(0),
            ])
            .split(area);

        let pipeline = &self.result.pipeline_result;

        // Pipeline details
        let details_text = vec![
            format!("Pipeline ID: {}", pipeline.id),
            format!("Config ID: {}", pipeline.config_id),
            format!("Algorithm Config ID: {}", pipeline.algorithm_config_id),
            format!("Status: {}", Self::format_status(&pipeline.status)),
            format!("Started At: {}", pipeline.started_at.format("%Y-%m-%d %H:%M:%S UTC")),
            format!("Completed At: {}", pipeline.completed_at.map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string()).unwrap_or_else(|| "N/A".to_string())),
            format!("Duration: {:.2} seconds", pipeline.duration_seconds),
            format!("Execution Order: {:?}", pipeline.execution_order),
        ];

        let details_lines: Vec<Line> = details_text.iter().map(|s| Line::from(s.as_str())).collect();
        let details_para = Paragraph::new(details_lines)
            .block(Block::default().borders(Borders::ALL).title("Pipeline Details"))
            .alignment(Alignment::Left);
        f.render_widget(details_para, chunks[0]);

        // Stage outcomes summary
        let mut outcomes_text = vec![
            format!("Total Stages: {}", pipeline.execution_order.len()),
            format!("Passed: {}", pipeline.stages_passed),
            format!("Failed: {}", pipeline.stages_failed),
            format!("Skipped: {}", pipeline.stages_skipped),
        ];

        for (stage_type, outcome) in &pipeline.stage_outcomes {
            outcomes_text.push(format!(
                "{}: {}",
                Self::format_stage_type(stage_type),
                Self::format_outcome(outcome)
            ));
        }

        let outcomes_lines: Vec<Line> = outcomes_text.iter().map(|s| Line::from(s.as_str())).collect();
        let outcomes_para = Paragraph::new(outcomes_lines)
            .block(Block::default().borders(Borders::ALL).title("Stage Outcomes"))
            .alignment(Alignment::Left);
        f.render_widget(outcomes_para, chunks[1]);
    }

    /// Render warnings view
    fn render_warnings(&self, f: &mut Frame, area: Rect) {
        let warnings = &self.result.pipeline_result.warnings;
        
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
    use std::collections::HashMap;
    use chrono::Utc;
    use crate::validation::PipelineResult;
    use crate::core::ValidationResult;

    fn create_test_validate_run_result() -> RunResult {
        let mut stage_outcomes = HashMap::new();
        stage_outcomes.insert(
            ValidationStageType::Backtest,
            StageOutcome::Passed(ValidationResult::default()),
        );
        stage_outcomes.insert(
            ValidationStageType::Forward,
            StageOutcome::Passed(ValidationResult::default()),
        );

        let mut execution_order = Vec::new();
        execution_order.push(ValidationStageType::Backtest);
        execution_order.push(ValidationStageType::Forward);

        let mut pipeline_result = PipelineResult::new("config-1".to_string(), "algo-1".to_string());
        pipeline_result.status = PipelineStatus::Passed;
        pipeline_result.stage_outcomes = stage_outcomes;
        pipeline_result.execution_order = execution_order;
        pipeline_result.stages_passed = 2;
        pipeline_result.stages_failed = 0;
        pipeline_result.stages_skipped = 0;
        pipeline_result.duration_seconds = 120.0;
        pipeline_result.summary = "All stages passed".to_string();
        pipeline_result.warnings = vec!["Minor warning".to_string()];

        RunResult {
            pipeline_result,
            algorithm_config_id: "algo-1".to_string(),
            algorithm_name: "Test Algorithm".to_string(),
            duration_seconds: 120.0,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = ValidateRunViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(ValidateRunViewMode::Summary.name(), "Summary");
        assert_eq!(ValidateRunViewMode::Stages.name(), "Stages");
        assert_eq!(ValidateRunViewMode::Details.name(), "Details");
        assert_eq!(ValidateRunViewMode::Warnings.name(), "Warnings");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(ValidateRunViewMode::Summary.next(), ValidateRunViewMode::Stages);
        assert_eq!(ValidateRunViewMode::Stages.next(), ValidateRunViewMode::Details);
        assert_eq!(ValidateRunViewMode::Details.next(), ValidateRunViewMode::Warnings);
        assert_eq!(ValidateRunViewMode::Warnings.next(), ValidateRunViewMode::Summary);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_validate_run_result();
        let screen = ValidateRunResultsScreen::new(result);
        assert_eq!(screen.view_mode(), ValidateRunViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_validate_run_result();
        let mut screen = ValidateRunResultsScreen::new(result);
        screen.set_view_mode(ValidateRunViewMode::Stages);
        assert_eq!(screen.view_mode(), ValidateRunViewMode::Stages);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_validate_run_result();
        let mut screen = ValidateRunResultsScreen::new(result);
        screen.set_selected_index(Some(1));
        assert_eq!(screen.selected_index(), Some(1));
    }

    #[test]
    fn test_export_json() {
        let result = create_test_validate_run_result();
        let screen = ValidateRunResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"pipeline_result\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_validate_run_result();
        let mut screen = ValidateRunResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), ValidateRunViewMode::Stages);
    }

    #[test]
    fn test_format_status() {
        assert_eq!(ValidateRunResultsScreen::format_status(&PipelineStatus::Passed), "Passed");
        assert_eq!(ValidateRunResultsScreen::format_status(&PipelineStatus::Failed), "Failed");
    }

    #[test]
    fn test_format_stage_type() {
        assert_eq!(ValidateRunResultsScreen::format_stage_type(&ValidationStageType::Backtest), "Backtest");
        assert_eq!(ValidateRunResultsScreen::format_stage_type(&ValidationStageType::Forward), "Forward");
    }

    #[test]
    fn test_format_outcome() {
        assert_eq!(ValidateRunResultsScreen::format_outcome(&StageOutcome::Passed(ValidationResult::default())), "Passed");
        assert_eq!(ValidateRunResultsScreen::format_outcome(&StageOutcome::Failed(ValidationResult::default())), "Failed");
    }

    #[test]
    fn test_create_stages_table() {
        let result = create_test_validate_run_result();
        let screen = ValidateRunResultsScreen::new(result);
        let (headers, rows) = screen.create_stages_table();
        assert_eq!(headers.len(), 4);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = ValidateRunViewMode::Summary;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_pipeline_result_access() {
        let result = create_test_validate_run_result();
        let screen = ValidateRunResultsScreen::new(result);
        assert_eq!(screen.result().pipeline_result.execution_order.len(), 2);
        assert_eq!(screen.result().duration_seconds, 120.0);
    }
}
