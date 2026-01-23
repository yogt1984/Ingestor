//! Backtest Info Results Screen
//!
//! Displays data statistics including file count, date range, event count, and event rate.

use ratatui::{
    Frame,
    layout::{Rect},
    widgets::Block,
};
use crossterm::event::KeyEvent;

use crate::commands::backtest::InfoResult;
use crate::ui::widgets::{MetricsDashboardWidget, Metric, MetricValue, MetricFormat};

/// View modes for the info results screen
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfoViewMode {
    Summary,
}

impl InfoViewMode {
    /// Get all view modes
    pub fn all() -> Vec<Self> {
        vec![Self::Summary]
    }

    /// Get the display name for this view mode
    pub fn name(&self) -> &'static str {
        match self {
            Self::Summary => "Summary",
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

/// Backtest info results screen
pub struct BacktestInfoResultsScreen {
    result: InfoResult,
    view_mode: InfoViewMode,
    focused: bool,
}

impl BacktestInfoResultsScreen {
    /// Create a new info results screen
    pub fn new(result: InfoResult) -> Self {
        Self {
            result,
            view_mode: InfoViewMode::Summary,
            focused: true,
        }
    }

    /// Get the info result
    pub fn result(&self) -> &InfoResult {
        &self.result
    }

    /// Get the current view mode
    pub fn view_mode(&self) -> InfoViewMode {
        self.view_mode
    }

    /// Set the view mode
    pub fn set_view_mode(&mut self, mode: InfoViewMode) {
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
    pub fn handle_key(&mut self, _key: KeyEvent) -> bool {
        // Info screen has only one view mode, so Tab/BackTab do nothing
        // ESC is handled by the parent to go back
        false
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        self.render_summary(f, area);
    }

    /// Render the summary view
    fn render_summary(&self, f: &mut Frame, area: Rect) {
        // Create metrics
        let mut metrics = vec![];

        // Total events
        metrics.push(
            Metric::new("Total Events", MetricValue::Integer(self.result.total_events as i64))
        );

        // Number of files
        metrics.push(
            Metric::new("Files", MetricValue::Integer(self.result.num_files as i64))
        );

        // Duration in days
        if let Some(days) = self.result.duration_days {
            metrics.push(
                Metric::new("Duration (days)", MetricValue::Number(days))
                    .with_format(MetricFormat::Decimal(2))
            );
        }

        // Duration in hours
        if let Some(hours) = self.result.duration_hours {
            metrics.push(
                Metric::new("Duration (hours)", MetricValue::Number(hours))
                    .with_format(MetricFormat::Decimal(1))
            );
        }

        // Event rate
        if let Some(rate) = self.result.event_rate {
            metrics.push(
                Metric::new("Event Rate (events/s)", MetricValue::Number(rate))
                    .with_format(MetricFormat::Decimal(1))
            );
        }

        // Time range start
        if let Some(start_ms) = self.result.time_start_ms {
            let start_dt = chrono::DateTime::from_timestamp_millis(start_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            metrics.push(
                Metric::new("Start Time", MetricValue::String(start_dt))
            );
        }

        // Time range end
        if let Some(end_ms) = self.result.time_end_ms {
            let end_dt = chrono::DateTime::from_timestamp_millis(end_ms)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            metrics.push(
                Metric::new("End Time", MetricValue::String(end_dt))
            );
        }

        // Data path
        let data_path_str = self.result.data_path.to_string_lossy().to_string();
        metrics.push(
            Metric::new("Data Directory", MetricValue::String(data_path_str))
        );

        // Create dashboard widget
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(metrics);

        dashboard.render(area, f.buffer_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_info_view_mode_all() {
        let modes = InfoViewMode::all();
        assert_eq!(modes.len(), 1);
        assert_eq!(modes[0], InfoViewMode::Summary);
    }

    #[test]
    fn test_info_view_mode_name() {
        assert_eq!(InfoViewMode::Summary.name(), "Summary");
    }

    #[test]
    fn test_info_view_mode_next() {
        assert_eq!(InfoViewMode::Summary.next(), InfoViewMode::Summary);
    }

    #[test]
    fn test_info_view_mode_previous() {
        assert_eq!(InfoViewMode::Summary.previous(), InfoViewMode::Summary);
    }

    #[test]
    fn test_backtest_info_results_screen_new() {
        let result = InfoResult {
            data_path: PathBuf::from("./data/features"),
            total_events: 1000,
            time_start_ms: Some(1000000),
            time_end_ms: Some(2000000),
            duration_hours: Some(24.0),
            duration_days: Some(1.0),
            event_rate: Some(41.67),
            num_files: 10,
        };

        let screen = BacktestInfoResultsScreen::new(result.clone());
        assert_eq!(screen.result().total_events, 1000);
        assert_eq!(screen.view_mode(), InfoViewMode::Summary);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_backtest_info_results_screen_set_focused() {
        let result = InfoResult {
            data_path: PathBuf::from("./data/features"),
            total_events: 1000,
            time_start_ms: None,
            time_end_ms: None,
            duration_hours: None,
            duration_days: None,
            event_rate: None,
            num_files: 10,
        };

        let mut screen = BacktestInfoResultsScreen::new(result);
        assert!(screen.is_focused());

        screen.set_focused(false);
        assert!(!screen.is_focused());
    }
}
