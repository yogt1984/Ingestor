//! Slider Widget
//!
//! A reusable slider widget for TUI parameter configuration.
//! Supports horizontal slider bar, keyboard/mouse adjustment, value display,
//! and min/max labels for range selection (e.g., objective weights).

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

// ============================================================================
// SliderWidget
// ============================================================================

/// Slider widget for TUI parameter configuration
pub struct SliderWidget {
    /// Current numeric value
    value: f64,
    /// Minimum allowed value (required for slider)
    min: f64,
    /// Maximum allowed value (required for slider)
    max: f64,
    /// Step size for increment/decrement
    step: f64,
    /// Number of decimal places to display
    decimals: usize,
    /// Display format
    format: SliderFormat,
    /// Label for the slider (optional)
    label: Option<String>,
    /// Whether to show min/max labels
    show_min_max_labels: bool,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
    /// Slider bar width (for rendering)
    bar_width: usize,
}

impl Clone for SliderWidget {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            min: self.min,
            max: self.max,
            step: self.step,
            decimals: self.decimals,
            format: self.format.clone(),
            label: self.label.clone(),
            show_min_max_labels: self.show_min_max_labels,
            focused: self.focused,
            read_only: self.read_only,
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
            bar_width: self.bar_width,
        }
    }
}

impl std::fmt::Debug for SliderWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SliderWidget")
            .field("value", &self.value)
            .field("min", &self.min)
            .field("max", &self.max)
            .field("step", &self.step)
            .field("decimals", &self.decimals)
            .field("format", &self.format)
            .field("label", &self.label)
            .field("focused", &self.focused)
            .field("read_only", &self.read_only)
            .field("validation_state", &self.validation_state)
            .finish()
    }
}

/// Display format for slider values
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SliderFormat {
    /// Decimal format (e.g., 123.45)
    Decimal,
    /// Percentage format (e.g., 12.34%)
    Percentage,
    /// Basis points format (e.g., 1234 bps)
    BasisPoints,
    /// Integer format (no decimals)
    Integer,
}

/// Current validation state
#[derive(Debug, Clone, PartialEq, Eq)]
enum ValidationState {
    /// Not yet validated
    Unvalidated,
    /// Valid
    Valid,
    /// Invalid with error message
    Invalid(String),
}

impl Default for SliderWidget {
    fn default() -> Self {
        Self::new(0.0, 100.0)
    }
}

impl SliderWidget {
    /// Create a new slider widget with min and max values
    pub fn new(min: f64, max: f64) -> Self {
        let value = (min + max) / 2.0;
        Self {
            value: value.max(min).min(max),
            min,
            max,
            step: (max - min) / 100.0, // Default to 100 steps
            decimals: 2,
            format: SliderFormat::Decimal,
            label: None,
            show_min_max_labels: true,
            focused: false,
            read_only: false,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
            bar_width: 20, // Default bar width
        }
    }

    /// Set the initial value
    pub fn with_value(mut self, value: f64) -> Self {
        self.value = value.max(self.min).min(self.max);
        self.validate();
        self
    }

    /// Set the step size
    pub fn with_step(mut self, step: f64) -> Self {
        self.step = step.max(f64::EPSILON);
        self
    }

    /// Set number of decimal places
    pub fn with_decimals(mut self, decimals: usize) -> Self {
        self.decimals = decimals;
        self
    }

    /// Set display format
    pub fn with_format(mut self, format: SliderFormat) -> Self {
        self.format = format;
        self
    }

    /// Set label text
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set whether to show min/max labels
    pub fn with_show_min_max_labels(mut self, show: bool) -> Self {
        self.show_min_max_labels = show;
        self
    }

    /// Set slider bar width
    pub fn with_bar_width(mut self, width: usize) -> Self {
        self.bar_width = width.max(1);
        self
    }

    /// Set whether the widget is focused
    pub fn set_focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    /// Set whether the input is read-only
    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set whether to show validation errors
    pub fn set_show_validation(mut self, show: bool) -> Self {
        self.show_validation = show;
        self
    }

    /// Get current value
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Get minimum value
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Get maximum value
    pub fn max(&self) -> f64 {
        self.max
    }

    /// Get step size
    pub fn step(&self) -> f64 {
        self.step
    }

    /// Check if the input is valid
    pub fn is_valid(&self) -> bool {
        matches!(self.validation_state, ValidationState::Valid)
    }

    /// Get validation error message if invalid
    pub fn validation_error(&self) -> Option<&str> {
        match &self.validation_state {
            ValidationState::Invalid(msg) => Some(msg),
            _ => None,
        }
    }

    /// Check if the widget is focused
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Increment value by step
    pub fn increment(&mut self) {
        if self.read_only {
            return;
        }
        let new_value = self.value + self.step;
        self.set_value(new_value);
    }

    /// Decrement value by step
    pub fn decrement(&mut self) {
        if self.read_only {
            return;
        }
        let new_value = self.value - self.step;
        self.set_value(new_value);
    }

    /// Set value (clamped to min/max)
    pub fn set_value(&mut self, value: f64) {
        if self.read_only {
            return;
        }
        self.value = value.max(self.min).min(self.max);
        // Snap to step if step > 0 and step is reasonable relative to range
        if self.step > f64::EPSILON {
            let range = self.max - self.min;
            // Only snap if step is not larger than the range
            if self.step <= range {
                let steps = ((self.value - self.min) / self.step).round();
                self.value = self.min + steps * self.step;
                // Ensure still within bounds after snapping
                self.value = self.value.max(self.min).min(self.max);
            }
        }
        self.validate();
    }

    /// Set value to minimum
    pub fn set_to_min(&mut self) {
        if self.read_only {
            return;
        }
        self.set_value(self.min);
    }

    /// Set value to maximum
    pub fn set_to_max(&mut self) {
        if self.read_only {
            return;
        }
        self.set_value(self.max);
    }

    /// Set value based on position (0.0 to 1.0)
    pub fn set_position(&mut self, position: f64) {
        if self.read_only {
            return;
        }
        let position = position.max(0.0).min(1.0);
        let new_value = self.min + position * (self.max - self.min);
        self.set_value(new_value);
    }

    /// Get position as ratio (0.0 to 1.0)
    pub fn position(&self) -> f64 {
        if (self.max - self.min).abs() < f64::EPSILON {
            return 0.0;
        }
        ((self.value - self.min) / (self.max - self.min)).max(0.0).min(1.0)
    }

    /// Validate the current value
    pub fn validate(&mut self) {
        if self.value < self.min || self.value > self.max {
            self.validation_state = ValidationState::Invalid(
                format!("Value must be between {} and {}", self.min, self.max)
            );
        } else {
            self.validation_state = ValidationState::Valid;
        }
    }

    /// Handle a key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.read_only {
            return false;
        }

        match key.code {
            KeyCode::Left | KeyCode::Char('h') => {
                self.decrement();
                true
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.increment();
                true
            }
            KeyCode::Home => {
                self.set_to_min();
                true
            }
            KeyCode::End => {
                self.set_to_max();
                true
            }
            KeyCode::Up | KeyCode::Char('k') => {
                // Increment by larger step (10x)
                let old_step = self.step;
                self.step = old_step * 10.0;
                self.increment();
                self.step = old_step;
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                // Decrement by larger step (10x)
                let old_step = self.step;
                self.step = old_step * 10.0;
                self.decrement();
                self.step = old_step;
                true
            }
            _ => false,
        }
    }

    /// Format value for display
    fn format_value(&self) -> String {
        match self.format {
            SliderFormat::Decimal => {
                format!("{:.*}", self.decimals, self.value)
            }
            SliderFormat::Percentage => {
                format!("{:.*}%", self.decimals, self.value * 100.0)
            }
            SliderFormat::BasisPoints => {
                format!("{:.*} bps", self.decimals, self.value * 10000.0)
            }
            SliderFormat::Integer => {
                format!("{}", self.value.round() as i64)
            }
        }
    }

    /// Format min value for display
    fn format_min(&self) -> String {
        match self.format {
            SliderFormat::Decimal => {
                format!("{:.*}", self.decimals, self.min)
            }
            SliderFormat::Percentage => {
                format!("{:.*}%", self.decimals, self.min * 100.0)
            }
            SliderFormat::BasisPoints => {
                format!("{:.*} bps", self.decimals, self.min * 10000.0)
            }
            SliderFormat::Integer => {
                format!("{}", self.min.round() as i64)
            }
        }
    }

    /// Format max value for display
    fn format_max(&self) -> String {
        match self.format {
            SliderFormat::Decimal => {
                format!("{:.*}", self.decimals, self.max)
            }
            SliderFormat::Percentage => {
                format!("{:.*}%", self.decimals, self.max * 100.0)
            }
            SliderFormat::BasisPoints => {
                format!("{:.*} bps", self.decimals, self.max * 10000.0)
            }
            SliderFormat::Integer => {
                format!("{}", self.max.round() as i64)
            }
        }
    }

    /// Render the widget to the frame
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Determine style based on state
        let (text_style, border_style) = self.get_styles();

        // Create block with borders
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);

        // Build display text
        let display_text = self.build_display_text(area.width);

        // Create paragraph with text
        let paragraph = Paragraph::new(display_text)
            .style(text_style)
            .block(block);

        f.render_widget(paragraph, area);

        // Render validation error if shown
        if self.show_validation && self.focused {
            if let ValidationState::Invalid(ref error) = self.validation_state {
                self.render_validation_error(f, area, error);
            }
        }
    }

    /// Build display text with slider bar
    fn build_display_text(&self, width: u16) -> Line {
        let mut spans = Vec::new();

        // Add label if present
        if let Some(ref label) = self.label {
            spans.push(Span::styled(
                format!("{}: ", label),
                Style::default().fg(Color::White),
            ));
        }

        // Add min label if enabled
        if self.show_min_max_labels {
            spans.push(Span::styled(
                format!("{} ", self.format_min()),
                Style::default().fg(Color::DarkGray),
            ));
        }

        // Calculate slider bar
        let available_width = width.saturating_sub(
            if self.show_min_max_labels { 20 } else { 5 } as u16
        );
        let bar_width = (self.bar_width as u16).min(available_width);
        let position = self.position();
        let filled_width = (bar_width as f64 * position).round() as u16;
        let empty_width = bar_width.saturating_sub(filled_width);

        // Render slider bar
        let filled_char = if self.focused { "█" } else { "▓" };
        let empty_char = if self.focused { "░" } else { "▒" };

        spans.push(Span::styled(
            filled_char.repeat(filled_width as usize),
            Style::default().fg(if self.focused { Color::Cyan } else { Color::Blue }),
        ));
        spans.push(Span::styled(
            empty_char.repeat(empty_width as usize),
            Style::default().fg(Color::DarkGray),
        ));

        // Add max label if enabled
        if self.show_min_max_labels {
            spans.push(Span::styled(
                format!(" {}", self.format_max()),
                Style::default().fg(Color::DarkGray),
            ));
        }

        // Add current value
        spans.push(Span::styled(
            format!(" [{}]", self.format_value()),
            Style::default()
                .fg(if self.focused { Color::Yellow } else { Color::White })
                .add_modifier(Modifier::BOLD),
        ));

        Line::from(spans)
    }

    /// Get styles based on widget state
    fn get_styles(&self) -> (Style, Style) {
        let text_style = if self.read_only {
            Style::default().fg(Color::DarkGray)
        } else {
            Style::default().fg(Color::White)
        };

        let border_style = if !self.focused {
            Style::default().fg(Color::DarkGray)
        } else if let ValidationState::Invalid(_) = self.validation_state {
            Style::default().fg(Color::Red)
        } else if let ValidationState::Valid = self.validation_state {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::Cyan)
        };

        (text_style, border_style)
    }

    /// Render validation error message
    fn render_validation_error(&self, f: &mut Frame, area: Rect, error: &str) {
        let error_y = area.y + area.height + 1;

        if error_y < f.area().height {
            let error_area = Rect {
                x: area.x,
                y: error_y,
                width: area.width,
                height: 1,
            };

            let error_span = Span::styled(
                format!("⚠ {}", error),
                Style::default().fg(Color::Red),
            );
            let error_line = Line::from(vec![error_span]);
            let error_paragraph = Paragraph::new(error_line);
            f.render_widget(error_paragraph, error_area);
        }
    }
}

impl Widget for SliderWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
        let display_text = self.build_display_text(area.width);
        let (text_style, border_style) = self.get_styles();

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);

        let paragraph = Paragraph::new(display_text)
            .style(text_style)
            .block(block);

        paragraph.render(area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    // ========================================================================
    // Construction Tests
    // ========================================================================

    #[test]
    fn test_new_widget() {
        let widget = SliderWidget::new(0.0, 100.0);
        assert_eq!(widget.value(), 50.0);
        assert_eq!(widget.min(), 0.0);
        assert_eq!(widget.max(), 100.0);
        assert!(!widget.is_focused());
        assert!(!widget.read_only);
    }

    #[test]
    fn test_new_widget_negative_range() {
        let widget = SliderWidget::new(-100.0, 100.0);
        assert_eq!(widget.value(), 0.0);
        assert_eq!(widget.min(), -100.0);
        assert_eq!(widget.max(), 100.0);
    }

    #[test]
    fn test_new_widget_small_range() {
        let widget = SliderWidget::new(0.0, 1.0);
        assert_eq!(widget.value(), 0.5);
        assert_eq!(widget.min(), 0.0);
        assert_eq!(widget.max(), 1.0);
    }

    #[test]
    fn test_with_value() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(75.0);
        assert_eq!(widget.value(), 75.0);
    }

    #[test]
    fn test_with_value_clamps_to_min() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(-10.0);
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_with_value_clamps_to_max() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(150.0);
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_with_step() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_step(5.0);
        assert_eq!(widget.step(), 5.0);
    }

    #[test]
    fn test_with_step_minimum() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_step(0.0);
        assert!(widget.step() >= f64::EPSILON);
    }

    #[test]
    fn test_with_decimals() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_decimals(3);
        assert_eq!(widget.decimals, 3);
    }

    #[test]
    fn test_with_format() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_format(SliderFormat::Percentage);
        assert_eq!(widget.format, SliderFormat::Percentage);
    }

    #[test]
    fn test_with_label() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_label("Weight");
        assert_eq!(widget.label, Some("Weight".to_string()));
    }

    #[test]
    fn test_with_show_min_max_labels() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_show_min_max_labels(false);
        assert!(!widget.show_min_max_labels);
    }

    #[test]
    fn test_with_bar_width() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_bar_width(30);
        assert_eq!(widget.bar_width, 30);
    }

    #[test]
    fn test_with_bar_width_minimum() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_bar_width(0);
        assert_eq!(widget.bar_width, 1); // Minimum is 1
    }

    #[test]
    fn test_set_focused() {
        let widget = SliderWidget::new(0.0, 100.0)
            .set_focused(true);
        assert!(widget.is_focused());
    }

    #[test]
    fn test_set_read_only() {
        let widget = SliderWidget::new(0.0, 100.0)
            .set_read_only(true);
        assert!(widget.read_only);
    }

    #[test]
    fn test_chained_builders() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(5.0)
            .with_decimals(1)
            .with_format(SliderFormat::Percentage)
            .with_label("Weight")
            .set_focused(true);

        assert_eq!(widget.value(), 50.0);
        assert_eq!(widget.step(), 5.0);
        assert_eq!(widget.decimals, 1);
        assert_eq!(widget.format, SliderFormat::Percentage);
        assert_eq!(widget.label, Some("Weight".to_string()));
        assert!(widget.is_focused());
    }

    // ========================================================================
    // Value Manipulation Tests
    // ========================================================================

    #[test]
    fn test_increment() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0);
        widget.increment();
        assert_eq!(widget.value(), 60.0);
    }

    #[test]
    fn test_increment_clamps_to_max() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(95.0)
            .with_step(10.0);
        widget.increment();
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_decrement() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0);
        widget.decrement();
        assert_eq!(widget.value(), 40.0);
    }

    #[test]
    fn test_decrement_clamps_to_min() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(5.0)
            .with_step(10.0);
        widget.decrement();
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_increment_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        widget.increment();
        assert_eq!(widget.value(), 50.0); // Should not change
    }

    #[test]
    fn test_decrement_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        widget.decrement();
        assert_eq!(widget.value(), 50.0); // Should not change
    }

    #[test]
    fn test_set_value() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_value(75.0);
        assert_eq!(widget.value(), 75.0);
    }

    #[test]
    fn test_set_value_clamps() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_value(150.0);
        assert_eq!(widget.value(), 100.0);
        widget.set_value(-10.0);
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_set_value_snaps_to_step() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_step(5.0);
        widget.set_value(47.0);
        // Should snap to nearest step (45.0 or 50.0)
        assert!((widget.value() - 45.0).abs() < 0.01 || (widget.value() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_set_value_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        widget.set_value(75.0);
        assert_eq!(widget.value(), 50.0); // Should not change
    }

    #[test]
    fn test_set_to_min() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        widget.set_to_min();
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_set_to_max() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        widget.set_to_max();
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_set_to_min_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        widget.set_to_min();
        assert_eq!(widget.value(), 50.0); // Should not change
    }

    #[test]
    fn test_set_to_max_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        widget.set_to_max();
        assert_eq!(widget.value(), 50.0); // Should not change
    }

    #[test]
    fn test_set_position() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_position(0.5);
        assert!((widget.value() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_set_position_zero() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_position(0.0);
        assert!((widget.value() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_set_position_one() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_position(1.0);
        assert!((widget.value() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_set_position_clamps() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_position(1.5);
        assert!((widget.value() - 100.0).abs() < 0.01);
        widget.set_position(-0.5);
        assert!((widget.value() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_set_position_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        widget.set_position(0.75);
        assert!((widget.value() - 50.0).abs() < 0.01); // Should not change
    }

    #[test]
    fn test_position() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        assert!((widget.position() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_position_at_min() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(0.0);
        assert!((widget.position() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_position_at_max() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(100.0);
        assert!((widget.position() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_position_negative_range() {
        let widget = SliderWidget::new(-100.0, 100.0)
            .with_value(0.0);
        assert!((widget.position() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_position_zero_range() {
        let widget = SliderWidget::new(10.0, 10.0);
        assert!((widget.position() - 0.0).abs() < 0.01);
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_left() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0);
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 40.0);
    }

    #[test]
    fn test_handle_key_right() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0);
        let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 60.0);
    }

    #[test]
    fn test_handle_key_h() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0);
        let key = KeyEvent::new(KeyCode::Char('h'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 40.0);
    }

    #[test]
    fn test_handle_key_l() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0);
        let key = KeyEvent::new(KeyCode::Char('l'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 60.0);
    }

    #[test]
    fn test_handle_key_home() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        let key = KeyEvent::new(KeyCode::Home, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_handle_key_end() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        let key = KeyEvent::new(KeyCode::End, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_handle_key_up() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(1.0);
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        widget.handle_key(key);
        // Should increment by 10x step (10.0)
        assert_eq!(widget.value(), 60.0);
    }

    #[test]
    fn test_handle_key_down() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(1.0);
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        widget.handle_key(key);
        // Should decrement by 10x step (10.0)
        assert_eq!(widget.value(), 40.0);
    }

    #[test]
    fn test_handle_key_k() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(1.0);
        let key = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 60.0);
    }

    #[test]
    fn test_handle_key_j() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(1.0);
        let key = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.value(), 40.0);
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert_eq!(widget.value(), 50.0);
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        let key = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert_eq!(widget.value(), 50.0);
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_valid() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_at_min() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(0.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_at_max() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(100.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_invalid_below_min() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.value = -10.0;
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().is_some());
    }

    #[test]
    fn test_validate_invalid_above_max() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.value = 150.0;
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().is_some());
    }

    // ========================================================================
    // Format Tests
    // ========================================================================

    #[test]
    fn test_format_decimal() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(12.345)
            .with_decimals(2)
            .with_format(SliderFormat::Decimal);
        let formatted = widget.format_value();
        assert_eq!(formatted, "12.35"); // Rounded
    }

    #[test]
    fn test_format_percentage() {
        let widget = SliderWidget::new(0.0, 1.0)
            .with_value(0.1234)
            .with_decimals(2)
            .with_format(SliderFormat::Percentage);
        let formatted = widget.format_value();
        assert_eq!(formatted, "12.34%");
    }

    #[test]
    fn test_format_basis_points() {
        let widget = SliderWidget::new(0.0, 1.0)
            .with_value(0.0125)
            .with_decimals(0)
            .with_format(SliderFormat::BasisPoints);
        let formatted = widget.format_value();
        assert_eq!(formatted, "125 bps");
    }

    #[test]
    fn test_format_integer() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(12.7)
            .with_format(SliderFormat::Integer);
        let formatted = widget.format_value();
        assert_eq!(formatted, "13"); // Rounded
    }

    #[test]
    fn test_format_min_decimal() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_decimals(2)
            .with_format(SliderFormat::Decimal);
        let formatted = widget.format_min();
        assert_eq!(formatted, "0.00");
    }

    #[test]
    fn test_format_max_percentage() {
        let widget = SliderWidget::new(0.0, 1.0)
            .with_decimals(1)
            .with_format(SliderFormat::Percentage);
        let formatted = widget.format_max();
        assert_eq!(formatted, "100.0%");
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_very_small_range() {
        let widget = SliderWidget::new(0.0, 0.001);
        assert!(widget.value() >= widget.min());
        assert!(widget.value() <= widget.max());
    }

    #[test]
    fn test_very_large_range() {
        let widget = SliderWidget::new(0.0, 1_000_000.0);
        assert!(widget.value() >= widget.min());
        assert!(widget.value() <= widget.max());
    }

    #[test]
    fn test_negative_range() {
        let widget = SliderWidget::new(-100.0, 100.0);
        assert!(widget.value() >= widget.min());
        assert!(widget.value() <= widget.max());
    }

    #[test]
    fn test_zero_range() {
        let widget = SliderWidget::new(10.0, 10.0);
        assert_eq!(widget.value(), 10.0);
    }

    #[test]
    fn test_very_small_step() {
        let mut widget = SliderWidget::new(0.0, 1.0)
            .with_step(f64::EPSILON);
        widget.increment();
        // Should not panic
        assert!(widget.value() >= widget.min());
        assert!(widget.value() <= widget.max());
    }

    #[test]
    fn test_very_large_step() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_step(1000.0);
        widget.increment();
        assert_eq!(widget.value(), 100.0); // Clamped to max
    }

    #[test]
    fn test_rapid_increment() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_step(1.0);
        for _ in 0..1000 {
            widget.increment();
        }
        assert_eq!(widget.value(), 100.0); // Clamped to max
    }

    #[test]
    fn test_rapid_decrement() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(100.0)
            .with_step(1.0);
        for _ in 0..1000 {
            widget.decrement();
        }
        assert_eq!(widget.value(), 0.0); // Clamped to min
    }

    #[test]
    fn test_precision_handling() {
        let mut widget = SliderWidget::new(0.0, 1.0)
            .with_step(0.1)
            .with_decimals(10);
        widget.set_value(0.3333333333);
        // Should handle floating point precision
        assert!(widget.value() >= widget.min());
        assert!(widget.value() <= widget.max());
    }

    #[test]
    fn test_step_snapping_precision() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_step(0.01);
        widget.set_value(50.123456);
        // Should snap to nearest step
        let steps = ((widget.value() - widget.min()) / widget.step()).round();
        let expected = widget.min() + steps * widget.step();
        assert!((widget.value() - expected).abs() < f64::EPSILON * 10.0);
    }

    #[test]
    fn test_position_precision() {
        let widget = SliderWidget::new(0.0, 100.0)
            .with_value(33.333333);
        let pos = widget.position();
        assert!(pos >= 0.0);
        assert!(pos <= 1.0);
    }

    #[test]
    fn test_set_position_precision() {
        let mut widget = SliderWidget::new(0.0, 100.0);
        widget.set_position(0.3333333333);
        assert!(widget.value() >= widget.min());
        assert!(widget.value() <= widget.max());
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_adjustment_workflow() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_step(1.0) // Use step of 1.0 to allow precise positioning
            .set_focused(true);

        // Start at middle
        assert!((widget.value() - 50.0).abs() < 0.01);

        // Increment
        widget.increment();
        assert_eq!(widget.value(), 51.0);

        // Decrement
        widget.decrement();
        assert_eq!(widget.value(), 50.0);

        // Set to min
        widget.set_to_min();
        assert_eq!(widget.value(), 0.0);

        // Set to max
        widget.set_to_max();
        assert_eq!(widget.value(), 100.0);

        // Set position (with step 1.0, 25.0 should snap correctly)
        widget.set_position(0.25);
        assert!((widget.value() - 25.0).abs() < 0.01);
    }

    #[test]
    fn test_keyboard_navigation_workflow() {
        let mut widget = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0)
            .set_focused(true);

        // Navigate left
        widget.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::empty()));
        assert_eq!(widget.value(), 40.0);

        // Navigate right
        widget.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::empty()));
        assert_eq!(widget.value(), 50.0);

        // Go to home
        widget.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::empty()));
        assert_eq!(widget.value(), 0.0);

        // Go to end
        widget.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::empty()));
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_percentage_format_workflow() {
        let mut widget = SliderWidget::new(0.0, 1.0)
            .with_value(0.5)
            .with_format(SliderFormat::Percentage)
            .with_decimals(1);

        let formatted = widget.format_value();
        assert_eq!(formatted, "50.0%");

        widget.set_value(0.75);
        let formatted = widget.format_value();
        assert_eq!(formatted, "75.0%");
    }

    #[test]
    fn test_basis_points_workflow() {
        let mut widget = SliderWidget::new(0.0, 1.0)
            .with_value(0.0125)
            .with_format(SliderFormat::BasisPoints)
            .with_decimals(0);

        let formatted = widget.format_value();
        assert_eq!(formatted, "125 bps");

        widget.set_value(0.05);
        let formatted = widget.format_value();
        assert_eq!(formatted, "500 bps");
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = SliderWidget::new(0.0, 100.0)
            .with_value(75.0)
            .with_step(5.0)
            .with_format(SliderFormat::Percentage)
            .with_label("Weight")
            .set_focused(true);

        let widget2 = widget1.clone();
        assert_eq!(widget1.value(), widget2.value());
        assert_eq!(widget1.min(), widget2.min());
        assert_eq!(widget1.max(), widget2.max());
        assert_eq!(widget1.step(), widget2.step());
        assert_eq!(widget1.format, widget2.format);
        assert_eq!(widget1.label, widget2.label);
        assert_eq!(widget1.focused, widget2.focused);
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = SliderWidget::new(0.0, 100.0)
            .with_value(50.0)
            .with_step(10.0); // Explicit step to avoid default step issues
        let mut widget2 = widget1.clone();

        widget1.increment();
        widget2.decrement();

        assert_eq!(widget1.value(), 60.0);
        assert_eq!(widget2.value(), 40.0);
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1: SliderWidget = SliderWidget::default();
        let widget2 = SliderWidget::new(0.0, 100.0);
        assert_eq!(widget1.min(), widget2.min());
        assert_eq!(widget1.max(), widget2.max());
    }
}
