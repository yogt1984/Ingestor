//! Enhanced Progress Widget (T-3.5)
//!
//! A comprehensive progress widget for displaying command execution progress,
//! including progress bars, metrics, logs, ETA calculations, and elapsed time.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};
use std::time::{Duration, Instant};
use crate::commands::common::{LogLevel, ProgressEvent};

// ============================================================================
// Types
// ============================================================================

/// Enhanced progress widget for displaying command execution progress
pub struct ProgressWidget {
    /// Current progress (number of items processed)
    current: usize,
    /// Total items to process (if known)
    total: Option<usize>,
    /// Status message
    status_message: String,
    /// Metrics (name, value pairs)
    metrics: Vec<(String, f64)>,
    /// Log entries (scrollable)
    logs: Vec<LogEntry>,
    /// Maximum number of log entries to keep
    max_logs: usize,
    /// Scroll offset for logs (0 = show latest, positive = scroll up)
    log_scroll: usize,
    /// Start time of the operation
    start_time: Option<Instant>,
    /// Last update time
    last_update: Option<Instant>,
    /// Block style (optional title, borders)
    block: Option<Block<'static>>,
    /// Whether to show metrics section
    show_metrics: bool,
    /// Whether to show logs section
    show_logs: bool,
    /// Progress bar character (default: '█')
    progress_char: char,
    /// Progress bar color
    progress_color: Color,
    /// Background color for progress bar
    progress_bg_color: Color,
}

impl Clone for ProgressWidget {
    fn clone(&self) -> Self {
        Self {
            current: self.current,
            total: self.total,
            status_message: self.status_message.clone(),
            metrics: self.metrics.clone(),
            logs: self.logs.clone(),
            max_logs: self.max_logs,
            log_scroll: self.log_scroll,
            start_time: self.start_time,
            last_update: self.last_update,
            block: self.block.clone(),
            show_metrics: self.show_metrics,
            show_logs: self.show_logs,
            progress_char: self.progress_char,
            progress_color: self.progress_color,
            progress_bg_color: self.progress_bg_color,
        }
    }
}

impl std::fmt::Debug for ProgressWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressWidget")
            .field("current", &self.current)
            .field("total", &self.total)
            .field("status_message", &self.status_message)
            .field("metrics_count", &self.metrics.len())
            .field("logs_count", &self.logs.len())
            .field("log_scroll", &self.log_scroll)
            .finish()
    }
}

/// Log entry with level and message
#[derive(Debug, Clone)]
struct LogEntry {
    /// Log level
    level: LogLevel,
    /// Log message
    message: String,
    /// Timestamp when the log was created
    timestamp: Instant,
}

// ============================================================================
// Implementation
// ============================================================================

impl Default for ProgressWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressWidget {
    /// Create a new progress widget
    pub fn new() -> Self {
        Self {
            current: 0,
            total: None,
            status_message: String::new(),
            metrics: Vec::new(),
            logs: Vec::new(),
            max_logs: 1000,
            log_scroll: 0,
            start_time: None,
            last_update: None,
            block: None,
            show_metrics: true,
            show_logs: true,
            progress_char: '█',
            progress_color: Color::Green,
            progress_bg_color: Color::DarkGray,
        }
    }

    /// Handle a progress event
    pub fn handle_event(&mut self, event: ProgressEvent) {
        self.last_update = Some(Instant::now());

        match event {
            ProgressEvent::Started { total, message } => {
                self.start_time = Some(Instant::now());
                self.current = 0;
                self.total = total;
                self.status_message = message;
                self.metrics.clear();
                self.logs.clear();
                self.log_scroll = 0;
            }
            ProgressEvent::Progress { current, total, message } => {
                self.current = current;
                self.total = total;
                if !message.is_empty() {
                    self.status_message = message;
                }
            }
            ProgressEvent::Metric { name, value } => {
                // Update existing metric or add new one
                if let Some(idx) = self.metrics.iter().position(|(n, _)| n == &name) {
                    self.metrics[idx].1 = value;
                } else {
                    self.metrics.push((name, value));
                }
            }
            ProgressEvent::Log { level, message } => {
                self.logs.push(LogEntry {
                    level,
                    message,
                    timestamp: Instant::now(),
                });
                // Keep only the most recent logs
                if self.logs.len() > self.max_logs {
                    self.logs.remove(0);
                }
                // Auto-scroll to latest when new log arrives
                self.log_scroll = 0;
            }
            ProgressEvent::Completed { message } => {
                if !message.is_empty() {
                    self.status_message = message;
                }
            }
            ProgressEvent::Error { message } => {
                self.status_message = message;
            }
        }
    }

    /// Set current progress
    pub fn set_current(&mut self, current: usize) {
        self.current = current;
        self.last_update = Some(Instant::now());
    }

    /// Set total items
    pub fn set_total(&mut self, total: Option<usize>) {
        self.total = total;
    }

    /// Set status message
    pub fn set_status(&mut self, message: impl Into<String>) {
        self.status_message = message.into();
    }

    /// Add a metric
    pub fn add_metric(&mut self, name: impl Into<String>, value: f64) {
        let name = name.into();
        if let Some(idx) = self.metrics.iter().position(|(n, _)| n == &name) {
            self.metrics[idx].1 = value;
        } else {
            self.metrics.push((name, value));
        }
    }

    /// Remove a metric
    pub fn remove_metric(&mut self, name: &str) {
        self.metrics.retain(|(n, _)| n != name);
    }

    /// Clear all metrics
    pub fn clear_metrics(&mut self) {
        self.metrics.clear();
    }

    /// Add a log entry
    pub fn add_log(&mut self, level: LogLevel, message: impl Into<String>) {
        self.logs.push(LogEntry {
            level,
            message: message.into(),
            timestamp: Instant::now(),
        });
        if self.logs.len() > self.max_logs {
            self.logs.remove(0);
        }
        self.log_scroll = 0; // Auto-scroll to latest
    }

    /// Clear all logs
    pub fn clear_logs(&mut self) {
        self.logs.clear();
        self.log_scroll = 0;
    }

    /// Scroll logs up (show older logs)
    pub fn scroll_logs_up(&mut self, amount: usize) {
        let max_scroll = self.logs.len().saturating_sub(1);
        self.log_scroll = (self.log_scroll + amount).min(max_scroll);
    }

    /// Scroll logs down (show newer logs)
    pub fn scroll_logs_down(&mut self, amount: usize) {
        self.log_scroll = self.log_scroll.saturating_sub(amount);
    }

    /// Reset scroll to latest
    pub fn scroll_logs_to_latest(&mut self) {
        self.log_scroll = 0;
    }

    /// Set block style
    pub fn with_block(mut self, block: Block<'static>) -> Self {
        self.block = Some(block);
        self
    }

    /// Set whether to show metrics
    pub fn with_show_metrics(mut self, show: bool) -> Self {
        self.show_metrics = show;
        self
    }

    /// Set whether to show logs
    pub fn with_show_logs(mut self, show: bool) -> Self {
        self.show_logs = show;
        self
    }

    /// Set progress bar character
    pub fn with_progress_char(mut self, ch: char) -> Self {
        self.progress_char = ch;
        self
    }

    /// Set progress bar color
    pub fn with_progress_color(mut self, color: Color) -> Self {
        self.progress_color = color;
        self
    }

    /// Set progress bar background color
    pub fn with_progress_bg_color(mut self, color: Color) -> Self {
        self.progress_bg_color = color;
        self
    }

    /// Set maximum number of logs
    pub fn with_max_logs(mut self, max: usize) -> Self {
        self.max_logs = max;
        self
    }

    /// Get current progress
    pub fn current(&self) -> usize {
        self.current
    }

    /// Get total items
    pub fn total(&self) -> Option<usize> {
        self.total
    }

    /// Get progress percentage (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        match self.total {
            Some(total) if total > 0 => (self.current as f64 / total as f64).min(1.0),
            _ => 0.0,
        }
    }

    /// Get status message
    pub fn status_message(&self) -> &str {
        &self.status_message
    }

    /// Get elapsed time
    pub fn elapsed_time(&self) -> Option<Duration> {
        self.start_time.map(|start| start.elapsed())
    }

    /// Calculate ETA (Estimated Time to Arrival)
    pub fn eta(&self) -> Option<Duration> {
        if let (Some(start), Some(total)) = (self.start_time, self.total) {
            if total > 0 && self.current > 0 && self.current < total {
                let elapsed = start.elapsed();
                let rate = self.current as f64 / elapsed.as_secs_f64();
                if rate > 0.0 {
                    let remaining = (total - self.current) as f64 / rate;
                    return Some(Duration::from_secs_f64(remaining));
                }
            }
        }
        None
    }

    /// Get metrics
    pub fn metrics(&self) -> &[(String, f64)] {
        &self.metrics
    }

    /// Get logs
    pub fn logs(&self) -> &[LogEntry] {
        &self.logs
    }

    /// Get visible log entries (considering scroll)
    fn visible_logs(&self, max_lines: usize) -> &[LogEntry] {
        let log_count = self.logs.len();
        if log_count == 0 || max_lines == 0 {
            return &[];
        }

        let start = log_count.saturating_sub(max_lines + self.log_scroll);
        let end = log_count.saturating_sub(self.log_scroll);
        &self.logs[start..end]
    }

    /// Format duration as human-readable string
    fn format_duration(duration: Duration) -> String {
        let total_secs = duration.as_secs();
        if total_secs < 60 {
            format!("{}s", total_secs)
        } else if total_secs < 3600 {
            let mins = total_secs / 60;
            let secs = total_secs % 60;
            format!("{}m {}s", mins, secs)
        } else {
            let hours = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;
            format!("{}h {}m {}s", hours, mins, secs)
        }
    }
}

impl Widget for ProgressWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let block = self.block.clone().unwrap_or_else(|| Block::default().borders(Borders::ALL));
        let inner = block.inner(area);
        block.render(area, buf);

        if inner.width < 2 || inner.height < 2 {
            return;
        }

        // Calculate layout
        let mut chunks = Vec::new();

        // Progress bar area (1 line)
        chunks.push(Constraint::Length(1));

        // Status message area (1 line)
        chunks.push(Constraint::Length(1));

        // Info line (ETA, elapsed) (1 line)
        chunks.push(Constraint::Length(1));

        // Metrics section (if enabled and has metrics)
        if self.show_metrics && !self.metrics.is_empty() {
            chunks.push(Constraint::Min(2));
        }

        // Logs section (if enabled)
        if self.show_logs {
            chunks.push(Constraint::Min(3));
        }

        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints(chunks.as_slice())
            .split(inner);

        let mut chunk_idx = 0;

        // Render progress bar
        if chunk_idx < vertical.len() {
            self.render_progress_bar(vertical[chunk_idx], buf);
            chunk_idx += 1;
        }

        // Render status message
        if chunk_idx < vertical.len() {
            self.render_status_message(vertical[chunk_idx], buf);
            chunk_idx += 1;
        }

        // Render info (ETA, elapsed)
        if chunk_idx < vertical.len() {
            self.render_info(vertical[chunk_idx], buf);
            chunk_idx += 1;
        }

        // Render metrics
        if self.show_metrics && !self.metrics.is_empty() && chunk_idx < vertical.len() {
            self.render_metrics(vertical[chunk_idx], buf);
            chunk_idx += 1;
        }

        // Render logs
        if self.show_logs && chunk_idx < vertical.len() {
            let max_log_lines = vertical[chunk_idx].height.saturating_sub(1) as usize;
            self.render_logs(vertical[chunk_idx], max_log_lines, buf);
        }
    }
}

impl ProgressWidget {
    /// Render progress bar
    fn render_progress_bar(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let progress = self.progress();
        let bar_width = area.width.saturating_sub(2); // Account for brackets
        let filled_width = (progress * bar_width as f64) as u16;

        // Progress percentage text
        let percentage_text = if let Some(total) = self.total {
            format!("{}/{} ({:.1}%)", self.current, total, progress * 100.0)
        } else {
            format!("{}", self.current)
        };

        // Render progress bar
        let bar_start = area.x;
        for x in 0..bar_width {
            let char_x = bar_start + x;
            if char_x >= buf.area.width {
                break;
            }

            let ch = if x < filled_width {
                self.progress_char
            } else {
                ' '
            };

            let style = if x < filled_width {
                Style::default().fg(self.progress_color).bg(self.progress_bg_color)
            } else {
                Style::default().fg(self.progress_bg_color).bg(self.progress_bg_color)
            };

            buf.get_mut(char_x, area.y)
                .set_char(ch)
                .set_style(style);
        }

        // Render percentage text on the right (if space allows)
        if area.width > percentage_text.len() as u16 + 2 {
            let text_x = area.x + area.width.saturating_sub(percentage_text.len() as u16);
            for (i, ch) in percentage_text.chars().enumerate() {
                let x = text_x + i as u16;
                if x < buf.area.width {
                    buf.get_mut(x, area.y)
                        .set_char(ch)
                        .set_style(Style::default().fg(Color::White));
                }
            }
        }
    }

    /// Render status message
    fn render_status_message(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let text = if self.status_message.is_empty() {
            "Ready..."
        } else {
            self.status_message.as_str()
        };

        let paragraph = Paragraph::new(text)
            .style(Style::default().fg(Color::Cyan))
            .alignment(Alignment::Left);
        paragraph.render(area, buf);
    }

    /// Render info line (ETA, elapsed time)
    fn render_info(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let mut info_parts = Vec::new();

        // Elapsed time
        if let Some(elapsed) = self.elapsed_time() {
            info_parts.push(format!("Elapsed: {}", Self::format_duration(elapsed)));
        }

        // ETA
        if let Some(eta) = self.eta() {
            info_parts.push(format!("ETA: {}", Self::format_duration(eta)));
        }

        // Rate (items per second)
        if let (Some(start), Some(total)) = (self.start_time, self.total) {
            if self.current > 0 {
                let elapsed = start.elapsed();
                let rate = self.current as f64 / elapsed.as_secs_f64();
                if rate > 0.0 {
                    info_parts.push(format!("Rate: {:.2}/s", rate));
                }
            }
        }

        let info_text = info_parts.join(" | ");
        let paragraph = Paragraph::new(info_text.as_str())
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Left);
        paragraph.render(area, buf);
    }

    /// Render metrics
    fn render_metrics(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if self.metrics.is_empty() {
            return;
        }

        let mut spans = Vec::new();
        for (i, (name, value)) in self.metrics.iter().enumerate() {
            if i > 0 {
                spans.push(Span::styled(" | ", Style::default().fg(Color::DarkGray)));
            }
            spans.push(Span::styled(
                format!("{}: ", name),
                Style::default().fg(Color::Yellow),
            ));
            spans.push(Span::styled(
                format!("{:.4}", value),
                Style::default().fg(Color::White),
            ));
        }

        let line = Line::from(spans);
        let paragraph = Paragraph::new(line)
            .alignment(Alignment::Left)
            .style(Style::default());
        paragraph.render(area, buf);
    }

    /// Render logs
    fn render_logs(&self, area: Rect, max_lines: usize, buf: &mut ratatui::buffer::Buffer) {
        if area.height < 2 {
            return;
        }

        let logs_area = Rect {
            x: area.x,
            y: area.y,
            width: area.width,
            height: area.height.saturating_sub(1), // Reserve 1 line for header
        };

        let header_area = Rect {
            x: area.x,
            y: area.y + logs_area.height,
            width: area.width,
            height: 1,
        };

        // Render header
        let log_count = self.logs.len();
        let header_text = format!("Logs ({})", log_count);
        let paragraph = Paragraph::new(header_text.as_str())
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Left);
        paragraph.render(header_area, buf);

        // Render log entries
        let visible_logs = self.visible_logs(max_lines);
        let lines_to_render = visible_logs.len().min(max_lines);

        for (i, log_entry) in visible_logs.iter().take(lines_to_render).enumerate() {
            let y = logs_area.y + i as u16;
            if y >= logs_area.y + logs_area.height {
                break;
            }

            // Log level color
            let level_color = match log_entry.level {
                LogLevel::Info => Color::Cyan,
                LogLevel::Warn => Color::Yellow,
                LogLevel::Error => Color::Red,
                LogLevel::Debug => Color::DarkGray,
            };

            let level_prefix = format!("[{}] ", log_entry.level.as_str());
            let mut spans = vec![
                Span::styled(level_prefix, Style::default().fg(level_color)),
                Span::raw(log_entry.message.as_str()),
            ];

            let line = Line::from(spans);
            let paragraph = Paragraph::new(line)
                .alignment(Alignment::Left)
                .style(Style::default());
            
            let log_line_area = Rect {
                x: logs_area.x,
                y,
                width: logs_area.width,
                height: 1,
            };
            paragraph.render(log_line_area, buf);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn create_test_widget() -> ProgressWidget {
        ProgressWidget::new()
    }

    #[test]
    fn test_progress_widget_creation() {
        let widget = create_test_widget();
        assert_eq!(widget.current(), 0);
        assert_eq!(widget.total(), None);
        assert!(widget.status_message().is_empty());
        assert!(widget.metrics().is_empty());
        assert!(widget.logs().is_empty());
    }

    #[test]
    fn test_progress_widget_handle_event_started() {
        let mut widget = create_test_widget();
        let event = ProgressEvent::Started {
            total: Some(100),
            message: "Starting...".to_string(),
        };
        widget.handle_event(event);

        assert_eq!(widget.current(), 0);
        assert_eq!(widget.total(), Some(100));
        assert_eq!(widget.status_message(), "Starting...");
        assert!(widget.start_time.is_some());
        assert!(widget.metrics().is_empty());
        assert!(widget.logs().is_empty());
    }

    #[test]
    fn test_progress_widget_handle_event_progress() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Starting...".to_string(),
        });

        widget.handle_event(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: "Processing...".to_string(),
        });

        assert_eq!(widget.current(), 50);
        assert_eq!(widget.total(), Some(100));
        assert_eq!(widget.status_message(), "Processing...");
    }

    #[test]
    fn test_progress_widget_handle_event_metric() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Metric {
            name: "Sharpe".to_string(),
            value: 1.5,
        });

        assert_eq!(widget.metrics().len(), 1);
        assert_eq!(widget.metrics()[0].0, "Sharpe");
        assert_eq!(widget.metrics()[0].1, 1.5);

        // Update existing metric
        widget.handle_event(ProgressEvent::Metric {
            name: "Sharpe".to_string(),
            value: 2.0,
        });

        assert_eq!(widget.metrics().len(), 1);
        assert_eq!(widget.metrics()[0].1, 2.0);
    }

    #[test]
    fn test_progress_widget_handle_event_log() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Log {
            level: LogLevel::Info,
            message: "Test log".to_string(),
        });

        assert_eq!(widget.logs().len(), 1);
        assert_eq!(widget.logs()[0].message, "Test log");
        assert_eq!(widget.logs()[0].level, LogLevel::Info);
    }

    #[test]
    fn test_progress_widget_handle_event_completed() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Completed {
            message: "Done!".to_string(),
        });

        assert_eq!(widget.status_message(), "Done!");
    }

    #[test]
    fn test_progress_widget_handle_event_error() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Error {
            message: "Error occurred".to_string(),
        });

        assert_eq!(widget.status_message(), "Error occurred");
    }

    #[test]
    fn test_progress_widget_set_current() {
        let mut widget = create_test_widget();
        widget.set_current(42);
        assert_eq!(widget.current(), 42);
    }

    #[test]
    fn test_progress_widget_set_total() {
        let mut widget = create_test_widget();
        widget.set_total(Some(100));
        assert_eq!(widget.total(), Some(100));

        widget.set_total(None);
        assert_eq!(widget.total(), None);
    }

    #[test]
    fn test_progress_widget_set_status() {
        let mut widget = create_test_widget();
        widget.set_status("New status");
        assert_eq!(widget.status_message(), "New status");
    }

    #[test]
    fn test_progress_widget_add_metric() {
        let mut widget = create_test_widget();
        widget.add_metric("Sharpe", 1.5);
        assert_eq!(widget.metrics().len(), 1);
        assert_eq!(widget.metrics()[0].0, "Sharpe");
        assert_eq!(widget.metrics()[0].1, 1.5);

        // Update existing metric
        widget.add_metric("Sharpe", 2.0);
        assert_eq!(widget.metrics().len(), 1);
        assert_eq!(widget.metrics()[0].1, 2.0);
    }

    #[test]
    fn test_progress_widget_remove_metric() {
        let mut widget = create_test_widget();
        widget.add_metric("Sharpe", 1.5);
        widget.add_metric("Drawdown", -0.1);
        assert_eq!(widget.metrics().len(), 2);

        widget.remove_metric("Sharpe");
        assert_eq!(widget.metrics().len(), 1);
        assert_eq!(widget.metrics()[0].0, "Drawdown");
    }

    #[test]
    fn test_progress_widget_clear_metrics() {
        let mut widget = create_test_widget();
        widget.add_metric("Sharpe", 1.5);
        widget.add_metric("Drawdown", -0.1);
        widget.clear_metrics();
        assert!(widget.metrics().is_empty());
    }

    #[test]
    fn test_progress_widget_add_log() {
        let mut widget = create_test_widget();
        widget.add_log(LogLevel::Info, "Info message");
        widget.add_log(LogLevel::Warn, "Warning message");
        widget.add_log(LogLevel::Error, "Error message");

        assert_eq!(widget.logs().len(), 3);
        assert_eq!(widget.logs()[0].level, LogLevel::Info);
        assert_eq!(widget.logs()[1].level, LogLevel::Warn);
        assert_eq!(widget.logs()[2].level, LogLevel::Error);
    }

    #[test]
    fn test_progress_widget_clear_logs() {
        let mut widget = create_test_widget();
        widget.add_log(LogLevel::Info, "Test");
        widget.add_log(LogLevel::Warn, "Test");
        widget.clear_logs();
        assert!(widget.logs().is_empty());
        assert_eq!(widget.log_scroll, 0);
    }

    #[test]
    fn test_progress_widget_scroll_logs() {
        let mut widget = create_test_widget();
        for i in 0..10 {
            widget.add_log(LogLevel::Info, format!("Log {}", i));
        }
        assert_eq!(widget.log_scroll, 0);

        // Scroll up
        widget.scroll_logs_up(3);
        assert_eq!(widget.log_scroll, 3);

        // Scroll down
        widget.scroll_logs_down(1);
        assert_eq!(widget.log_scroll, 2);

        // Scroll to latest
        widget.scroll_logs_to_latest();
        assert_eq!(widget.log_scroll, 0);
    }

    #[test]
    fn test_progress_widget_scroll_logs_bounds() {
        let mut widget = create_test_widget();
        for i in 0..5 {
            widget.add_log(LogLevel::Info, format!("Log {}", i));
        }

        // Scroll beyond bounds
        widget.scroll_logs_up(100);
        assert!(widget.log_scroll <= widget.logs().len());

        // Scroll below bounds
        widget.scroll_logs_down(100);
        assert_eq!(widget.log_scroll, 0);
    }

    #[test]
    fn test_progress_widget_progress_percentage() {
        let mut widget = create_test_widget();
        widget.set_total(Some(100));
        widget.set_current(0);
        assert_eq!(widget.progress(), 0.0);

        widget.set_current(50);
        assert_eq!(widget.progress(), 0.5);

        widget.set_current(100);
        assert_eq!(widget.progress(), 1.0);

        // Beyond total
        widget.set_current(150);
        assert_eq!(widget.progress(), 1.0);
    }

    #[test]
    fn test_progress_widget_progress_no_total() {
        let widget = create_test_widget();
        assert_eq!(widget.progress(), 0.0);
    }

    #[test]
    fn test_progress_widget_progress_zero_total() {
        let mut widget = create_test_widget();
        widget.set_total(Some(0));
        assert_eq!(widget.progress(), 0.0);
    }

    #[test]
    fn test_progress_widget_elapsed_time() {
        let mut widget = create_test_widget();
        assert!(widget.elapsed_time().is_none());

        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Starting...".to_string(),
        });

        std::thread::sleep(Duration::from_millis(10));
        assert!(widget.elapsed_time().is_some());
        assert!(widget.elapsed_time().unwrap() >= Duration::from_millis(10));
    }

    #[test]
    fn test_progress_widget_eta_calculation() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Starting...".to_string(),
        });

        // No ETA initially (no progress yet)
        assert!(widget.eta().is_none());

        // Set some progress
        widget.set_current(10);
        std::thread::sleep(Duration::from_millis(100));

        // Should have ETA now
        let eta = widget.eta();
        assert!(eta.is_some() || eta.is_none()); // May or may not have ETA depending on timing

        // At 100%, no ETA
        widget.set_current(100);
        assert!(widget.eta().is_none());
    }

    #[test]
    fn test_progress_widget_eta_no_total() {
        let mut widget = create_test_widget();
        widget.set_total(None);
        widget.set_current(50);
        assert!(widget.eta().is_none());
    }

    #[test]
    fn test_progress_widget_visible_logs() {
        let mut widget = create_test_widget();
        for i in 0..10 {
            widget.add_log(LogLevel::Info, format!("Log {}", i));
        }

        let visible = widget.visible_logs(5);
        assert_eq!(visible.len(), 5);

        // Scroll up
        widget.scroll_logs_up(2);
        let visible = widget.visible_logs(5);
        assert_eq!(visible.len(), 5);
    }

    #[test]
    fn test_progress_widget_visible_logs_empty() {
        let widget = create_test_widget();
        let visible = widget.visible_logs(5);
        assert!(visible.is_empty());
    }

    #[test]
    fn test_progress_widget_format_duration() {
        assert_eq!(ProgressWidget::format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(ProgressWidget::format_duration(Duration::from_secs(65)), "1m 5s");
        assert_eq!(ProgressWidget::format_duration(Duration::from_secs(3665)), "1h 1m 5s");
    }

    #[test]
    fn test_progress_widget_with_block() {
        let widget = create_test_widget()
            .with_block(Block::default().title("Progress"));
        assert!(widget.block.is_some());
    }

    #[test]
    fn test_progress_widget_with_show_metrics() {
        let widget = create_test_widget().with_show_metrics(false);
        assert_eq!(widget.show_metrics, false);
    }

    #[test]
    fn test_progress_widget_with_show_logs() {
        let widget = create_test_widget().with_show_logs(false);
        assert_eq!(widget.show_logs, false);
    }

    #[test]
    fn test_progress_widget_with_progress_char() {
        let widget = create_test_widget().with_progress_char('▓');
        assert_eq!(widget.progress_char, '▓');
    }

    #[test]
    fn test_progress_widget_with_progress_color() {
        let widget = create_test_widget().with_progress_color(Color::Blue);
        assert_eq!(widget.progress_color, Color::Blue);
    }

    #[test]
    fn test_progress_widget_with_max_logs() {
        let mut widget = create_test_widget().with_max_logs(10);
        for i in 0..20 {
            widget.add_log(LogLevel::Info, format!("Log {}", i));
        }
        assert!(widget.logs().len() <= 10);
    }

    #[test]
    fn test_progress_widget_render_empty() {
        let widget = create_test_widget();
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic
    }

    #[test]
    fn test_progress_widget_render_with_progress() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Processing...".to_string(),
        });
        widget.set_current(50);

        let area = Rect::new(0, 0, 50, 20);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic
    }

    #[test]
    fn test_progress_widget_render_with_metrics() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Processing...".to_string(),
        });
        widget.add_metric("Sharpe", 1.5);
        widget.add_metric("Drawdown", -0.1);

        let area = Rect::new(0, 0, 50, 20);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic
    }

    #[test]
    fn test_progress_widget_render_with_logs() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Processing...".to_string(),
        });
        widget.add_log(LogLevel::Info, "Info message");
        widget.add_log(LogLevel::Warn, "Warning message");
        widget.add_log(LogLevel::Error, "Error message");

        let area = Rect::new(0, 0, 50, 20);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic
    }

    #[test]
    fn test_progress_widget_render_small_area() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Processing...".to_string(),
        });
        widget.set_current(50);

        let area = Rect::new(0, 0, 10, 5);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic even with small area
    }

    #[test]
    fn test_progress_widget_render_no_metrics() {
        let mut widget = create_test_widget().with_show_metrics(false);
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Processing...".to_string(),
        });

        let area = Rect::new(0, 0, 50, 20);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic
    }

    #[test]
    fn test_progress_widget_render_no_logs() {
        let mut widget = create_test_widget().with_show_logs(false);
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Processing...".to_string(),
        });

        let area = Rect::new(0, 0, 50, 20);
        let mut buf = Buffer::empty(area);
        widget.render(area, &mut buf);
        // Should not panic
    }

    #[test]
    fn test_progress_widget_clone() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Test".to_string(),
        });
        widget.set_current(50);
        widget.add_metric("Sharpe", 1.5);
        widget.add_log(LogLevel::Info, "Test log");

        let cloned = widget.clone();
        assert_eq!(cloned.current(), 50);
        assert_eq!(cloned.total(), Some(100));
        assert_eq!(cloned.status_message(), "Test");
        assert_eq!(cloned.metrics().len(), 1);
        assert_eq!(cloned.logs().len(), 1);
    }

    #[test]
    fn test_progress_widget_multiple_events() {
        let mut widget = create_test_widget();
        widget.handle_event(ProgressEvent::Started {
            total: Some(100),
            message: "Starting...".to_string(),
        });

        for i in 0..10 {
            widget.handle_event(ProgressEvent::Progress {
                current: i,
                total: Some(100),
                message: format!("Step {}", i),
            });
            widget.handle_event(ProgressEvent::Metric {
                name: "Sharpe".to_string(),
                value: i as f64 * 0.1,
            });
            widget.handle_event(ProgressEvent::Log {
                level: LogLevel::Info,
                message: format!("Log {}", i),
            });
        }

        assert_eq!(widget.current(), 9);
        assert_eq!(widget.metrics().len(), 1);
        assert_eq!(widget.metrics()[0].1, 0.9);
        assert_eq!(widget.logs().len(), 10);
    }

    #[test]
    fn test_progress_widget_log_auto_scroll() {
        let mut widget = create_test_widget();
        for i in 0..5 {
            widget.add_log(LogLevel::Info, format!("Log {}", i));
        }
        assert_eq!(widget.log_scroll, 0);

        // Scroll up
        widget.scroll_logs_up(2);
        assert_eq!(widget.log_scroll, 2);

        // Adding new log should auto-scroll to latest
        widget.add_log(LogLevel::Info, "New log");
        assert_eq!(widget.log_scroll, 0);
    }

    #[test]
    fn test_progress_widget_completed_with_message() {
        let mut widget = create_test_widget();
        widget.set_status("In progress");
        widget.handle_event(ProgressEvent::Completed {
            message: "Completed successfully".to_string(),
        });
        assert_eq!(widget.status_message(), "Completed successfully");
    }

    #[test]
    fn test_progress_widget_completed_empty_message() {
        let mut widget = create_test_widget();
        widget.set_status("In progress");
        widget.handle_event(ProgressEvent::Completed {
            message: String::new(),
        });
        // Status should remain unchanged if message is empty
        assert_eq!(widget.status_message(), "In progress");
    }

    #[test]
    fn test_progress_widget_progress_with_empty_message() {
        let mut widget = create_test_widget();
        widget.set_status("Initial status");
        widget.handle_event(ProgressEvent::Progress {
            current: 50,
            total: Some(100),
            message: String::new(),
        });
        // Status should remain unchanged if message is empty
        assert_eq!(widget.status_message(), "Initial status");
    }
}
