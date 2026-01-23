//! Backtest Validate Data Results Screen
//!
//! Displays data quality validation results including missing values, price anomalies,
//! timestamp issues, and data gaps.

use ratatui::{
    Frame,
    layout::{Rect, Layout, Constraint, Direction},
    widgets::{Block, Borders, Paragraph, Wrap},
    style::{Color, Style, Modifier},
};
use crossterm::event::KeyEvent;

use crate::commands::backtest::ValidateDataResult;
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
};

/// View modes for the validate data results screen
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateDataViewMode {
    Summary,
    MissingValues,
    Anomalies,
    Issues,
}

impl ValidateDataViewMode {
    /// Get all view modes
    pub fn all() -> Vec<Self> {
        vec![
            Self::Summary,
            Self::MissingValues,
            Self::Anomalies,
            Self::Issues,
        ]
    }

    /// Get the display name for this view mode
    pub fn name(&self) -> &'static str {
        match self {
            Self::Summary => "Summary",
            Self::MissingValues => "Missing Values",
            Self::Anomalies => "Anomalies",
            Self::Issues => "Issues",
        }
    }

    /// Get the next view mode
    pub fn next(&self) -> Self {
        let modes = Self::all();
        let current_idx = modes.iter().position(|m| m == self).unwrap_or(0);
        modes[(current_idx + 1) % modes.len()]
    }

    /// Get the previous view mode
    pub fn previous(&self) -> Self {
        let modes = Self::all();
        let current_idx = modes.iter().position(|m| m == self).unwrap_or(0);
        modes[(current_idx + modes.len() - 1) % modes.len()]
    }
}

/// Backtest validate data results screen
pub struct BacktestValidateDataResultsScreen {
    result: ValidateDataResult,
    view_mode: ValidateDataViewMode,
    focused: bool,
}

impl BacktestValidateDataResultsScreen {
    /// Create a new validate data results screen
    pub fn new(result: ValidateDataResult) -> Self {
        Self {
            result,
            view_mode: ValidateDataViewMode::Summary,
            focused: true,
        }
    }

    /// Get the validate data result
    pub fn result(&self) -> &ValidateDataResult {
        &self.result
    }

    /// Get the current view mode
    pub fn view_mode(&self) -> ValidateDataViewMode {
        self.view_mode
    }

    /// Set the view mode
    pub fn set_view_mode(&mut self, mode: ValidateDataViewMode) {
        self.view_mode = mode;
    }

    /// Check if the screen is focused
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Set the focused state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Handle key event
    /// Returns true if the event was handled
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            crossterm::event::KeyCode::Tab => {
                self.view_mode = self.view_mode.next();
                true
            }
            crossterm::event::KeyCode::BackTab => {
                self.view_mode = self.view_mode.previous();
                true
            }
            _ => false,
        }
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        match self.view_mode {
            ValidateDataViewMode::Summary => self.render_summary(f, area),
            ValidateDataViewMode::MissingValues => self.render_missing_values(f, area),
            ValidateDataViewMode::Anomalies => self.render_anomalies(f, area),
            ValidateDataViewMode::Issues => self.render_issues(f, area),
        }
    }

    /// Render the summary view
    fn render_summary(&self, f: &mut Frame, area: Rect) {
        let report = &self.result.report;

        // Create metrics
        let mut metrics = vec![];

        // Quality score (highlighted)
        let quality_color = if report.quality_score >= 0.95 {
            Color::Green
        } else if report.quality_score >= 0.85 {
            Color::Yellow
        } else {
            Color::Red
        };

        metrics.push(
            Metric::new("Quality Score", MetricValue::Percentage(report.quality_score * 100.0))
                .with_format(MetricFormat::Decimal(1))
                .with_color(quality_color)
        );

        // Total events
        metrics.push(
            Metric::new("Total Events", MetricValue::Integer(report.total_events as i64))
        );

        // Valid events
        let valid_pct = if report.total_events > 0 {
            (report.valid_events as f64 / report.total_events as f64) * 100.0
        } else {
            0.0
        };
        metrics.push(
            Metric::new(
                "Valid Events",
                MetricValue::String(format!("{} ({:.1}%)", report.valid_events, valid_pct))
            )
        );

        // Invalid events
        metrics.push(
            Metric::new("Invalid Events", MetricValue::Integer(report.invalid_events as i64))
                .with_color(if report.invalid_events > 0 { Color::Red } else { Color::Green })
        );

        // Missing fields count
        let missing_fields = report.missing_stats.len();
        metrics.push(
            Metric::new("Fields with Missing Data", MetricValue::Integer(missing_fields as i64))
        );

        // Price anomalies
        metrics.push(
            Metric::new("Price Anomalies", MetricValue::Integer(report.price_anomalies.len() as i64))
                .with_color(if report.price_anomalies.is_empty() { Color::Green } else { Color::Yellow })
        );

        // Timestamp issues
        metrics.push(
            Metric::new("Timestamp Issues", MetricValue::Integer(report.timestamp_issues.len() as i64))
                .with_color(if report.timestamp_issues.is_empty() { Color::Green } else { Color::Yellow })
        );

        // Data gaps
        metrics.push(
            Metric::new("Data Gaps", MetricValue::Integer(report.data_gaps.len() as i64))
                .with_color(if report.data_gaps.is_empty() { Color::Green } else { Color::Yellow })
        );

        // Create dashboard widget
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(metrics);

        // Split area for dashboard and recommendations
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(10),
                Constraint::Length(if report.recommendations.is_empty() { 0 } else { 8 }),
            ])
            .split(area);

        dashboard.render(chunks[0], f.buffer_mut());

        // Render recommendations if any
        if !report.recommendations.is_empty() && chunks.len() > 1 {
            let rec_text = report.recommendations.join("\n• ");
            let rec_para = Paragraph::new(format!("• {}", rec_text))
                .block(
                    Block::default()
                        .title("Recommendations")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Yellow))
                )
                .wrap(Wrap { trim: true });
            f.render_widget(rec_para, chunks[1]);
        }
    }

    /// Render the missing values view
    fn render_missing_values(&self, f: &mut Frame, area: Rect) {
        let report = &self.result.report;

        // Create table headers
        let headers = vec![
            TableHeader::new("Field".to_string()).with_width(30),
            TableHeader::new("Missing".to_string()).with_width(15),
            TableHeader::new("Percentage".to_string()).with_width(15),
        ];

        // Create table rows
        let mut items: Vec<_> = report.missing_stats.iter().collect();
        items.sort_by(|a, b| b.1.missing_count.cmp(&a.1.missing_count));

        let rows: Vec<TableRow> = items
            .iter()
            .filter(|(_, stats)| stats.missing_count > 0)
            .map(|(field, stats)| {
                TableRow::new(vec![
                    field.to_string(),
                    stats.missing_count.to_string(),
                    format!("{:.1}%", stats.missing_pct * 100.0),
                ])
            })
            .collect();

        // Create table widget
        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title("Missing Values"));
        table.set_focused(self.focused);

        table.render(area, f.buffer_mut());
    }

    /// Render the anomalies view
    fn render_anomalies(&self, f: &mut Frame, area: Rect) {
        let report = &self.result.report;

        // Create table headers
        let headers = vec![
            TableHeader::new("Type".to_string()).with_width(15),
            TableHeader::new("Field".to_string()).with_width(25),
            TableHeader::new("Value".to_string()).with_width(20),
            TableHeader::new("Timestamp".to_string()).with_width(20),
        ];

        // Create table rows
        let rows: Vec<TableRow> = report
            .price_anomalies
            .iter()
            .take(100) // Limit to first 100
            .map(|anomaly| {
                let anomaly_type = format!("{:?}", anomaly.anomaly_type);
                let timestamp_str = chrono::DateTime::from_timestamp_millis(anomaly.timestamp_ms)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| "Unknown".to_string());

                TableRow::new(vec![
                    anomaly_type,
                    anomaly.field.clone(),
                    format!("{:.6}", anomaly.value),
                    timestamp_str,
                ])
            })
            .collect();

        // Create table widget
        let title = if report.price_anomalies.len() > 100 {
            format!("Price Anomalies (showing 100 of {})", report.price_anomalies.len())
        } else {
            format!("Price Anomalies ({})", report.price_anomalies.len())
        };

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title(title));
        table.set_focused(self.focused);

        table.render(area, f.buffer_mut());
    }

    /// Render the issues view
    fn render_issues(&self, f: &mut Frame, area: Rect) {
        let report = &self.result.report;

        // Create table headers
        let headers = vec![
            TableHeader::new("Type".to_string()).with_width(20),
            TableHeader::new("Description".to_string()).with_width(40),
            TableHeader::new("Timestamp".to_string()).with_width(20),
        ];

        // Combine timestamp issues and data gaps
        let mut rows: Vec<TableRow> = Vec::new();

        // Add timestamp issues
        for issue in report.timestamp_issues.iter().take(50) {
            let issue_type = format!("{:?}", issue.issue_type);
            let timestamp_str = chrono::DateTime::from_timestamp_millis(issue.timestamp_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

            rows.push(TableRow::new(vec![
                issue_type,
                issue.description.clone(),
                timestamp_str,
            ]));
        }

        // Add data gaps
        for gap in report.data_gaps.iter().take(50) {
            let gap_type = "DataGap".to_string();
            let description = format!("Gap of {:.1} hours", gap.duration_hours);
            let timestamp_str = chrono::DateTime::from_timestamp_millis(gap.start_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "Unknown".to_string());

            rows.push(TableRow::new(vec![
                gap_type,
                description,
                timestamp_str,
            ]));
        }

        // Create table widget
        let total_issues = report.timestamp_issues.len() + report.data_gaps.len();
        let title = if rows.len() < total_issues {
            format!("Timestamp & Data Issues (showing {} of {})", rows.len(), total_issues)
        } else {
            format!("Timestamp & Data Issues ({})", total_issues)
        };

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title(title));
        table.set_focused(self.focused);

        table.render(area, f.buffer_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::data_quality::DataQualityReport;
    use std::path::PathBuf;

    #[test]
    fn test_validate_data_view_mode_all() {
        let modes = ValidateDataViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_validate_data_view_mode_names() {
        assert_eq!(ValidateDataViewMode::Summary.name(), "Summary");
        assert_eq!(ValidateDataViewMode::MissingValues.name(), "Missing Values");
        assert_eq!(ValidateDataViewMode::Anomalies.name(), "Anomalies");
        assert_eq!(ValidateDataViewMode::Issues.name(), "Issues");
    }

    #[test]
    fn test_validate_data_view_mode_navigation() {
        assert_eq!(ValidateDataViewMode::Summary.next(), ValidateDataViewMode::MissingValues);
        assert_eq!(ValidateDataViewMode::MissingValues.next(), ValidateDataViewMode::Anomalies);
        assert_eq!(ValidateDataViewMode::Anomalies.next(), ValidateDataViewMode::Issues);
        assert_eq!(ValidateDataViewMode::Issues.next(), ValidateDataViewMode::Summary);
    }

    #[test]
    fn test_backtest_validate_data_results_screen_new() {
        let report = DataQualityReport::default();
        let result = ValidateDataResult {
            report,
            output_file: None,
        };

        let screen = BacktestValidateDataResultsScreen::new(result);
        assert_eq!(screen.view_mode(), ValidateDataViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_backtest_validate_data_results_screen_set_view_mode() {
        let report = DataQualityReport::default();
        let result = ValidateDataResult {
            report,
            output_file: Some(PathBuf::from("report.json")),
        };

        let mut screen = BacktestValidateDataResultsScreen::new(result);
        screen.set_view_mode(ValidateDataViewMode::MissingValues);
        assert_eq!(screen.view_mode(), ValidateDataViewMode::MissingValues);
    }
}
