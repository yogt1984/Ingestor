//! Number Input Widget
//!
//! A reusable number input widget for TUI parameter configuration.
//! Supports increment/decrement, min/max validation, step snapping,
//! format display (decimals, percentage, basis points), and optional slider mode.

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fmt;

// ============================================================================
// NumberInputWidget
// ============================================================================

/// Number input widget for TUI parameter configuration
pub struct NumberInputWidget {
    /// Current numeric value
    value: f64,
    /// Minimum allowed value (None = no minimum)
    min: Option<f64>,
    /// Maximum allowed value (None = no maximum)
    max: Option<f64>,
    /// Step size for increment/decrement
    step: f64,
    /// Number of decimal places to display
    decimals: usize,
    /// Display format
    format: NumberFormat,
    /// Placeholder text shown when value is empty/invalid
    placeholder: Option<String>,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Whether slider mode is enabled
    slider_mode: bool,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
    /// Internal text buffer for direct editing
    text_buffer: String,
    /// Whether we're in text editing mode
    editing: bool,
}

impl Clone for NumberInputWidget {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            min: self.min,
            max: self.max,
            step: self.step,
            decimals: self.decimals,
            format: self.format.clone(),
            placeholder: self.placeholder.clone(),
            focused: self.focused,
            read_only: self.read_only,
            slider_mode: self.slider_mode,
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
            text_buffer: self.text_buffer.clone(),
            editing: self.editing,
        }
    }
}

impl std::fmt::Debug for NumberInputWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumberInputWidget")
            .field("value", &self.value)
            .field("min", &self.min)
            .field("max", &self.max)
            .field("step", &self.step)
            .field("decimals", &self.decimals)
            .field("format", &self.format)
            .field("placeholder", &self.placeholder)
            .field("focused", &self.focused)
            .field("read_only", &self.read_only)
            .field("slider_mode", &self.slider_mode)
            .field("validation_state", &self.validation_state)
            .field("editing", &self.editing)
            .finish()
    }
}

/// Display format for numbers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumberFormat {
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

impl Default for NumberInputWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl NumberInputWidget {
    /// Create a new number input widget
    pub fn new() -> Self {
        Self {
            value: 0.0,
            min: None,
            max: None,
            step: 1.0,
            decimals: 2,
            format: NumberFormat::Decimal,
            placeholder: None,
            focused: false,
            read_only: false,
            slider_mode: false,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
            text_buffer: String::new(),
            editing: false,
        }
    }

    /// Set the initial value
    pub fn with_value(mut self, value: f64) -> Self {
        self.value = value;
        self.validate();
        self
    }

    /// Set minimum value
    pub fn with_min(mut self, min: f64) -> Self {
        self.min = Some(min);
        self.validate();
        self
    }

    /// Set maximum value
    pub fn with_max(mut self, max: f64) -> Self {
        self.max = Some(max);
        self.validate();
        self
    }

    /// Set step size
    pub fn with_step(mut self, step: f64) -> Self {
        self.step = step.max(0.0); // Ensure non-negative
        self
    }

    /// Set number of decimal places
    pub fn with_decimals(mut self, decimals: usize) -> Self {
        self.decimals = decimals;
        self
    }

    /// Set display format
    pub fn with_format(mut self, format: NumberFormat) -> Self {
        self.format = format;
        self
    }

    /// Set placeholder text
    pub fn with_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Set whether the widget is focused
    pub fn set_focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        if !focused {
            self.editing = false;
            self.text_buffer.clear();
        }
        self
    }

    /// Set whether the input is read-only
    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        if read_only {
            self.editing = false;
            self.text_buffer.clear();
        }
        self
    }

    /// Enable or disable slider mode
    pub fn set_slider_mode(mut self, enabled: bool) -> Self {
        self.slider_mode = enabled;
        self
    }

    /// Set whether to show validation errors
    pub fn set_show_validation(mut self, show: bool) -> Self {
        self.show_validation = show;
        self
    }

    /// Get the current value
    pub fn value(&self) -> f64 {
        self.value
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

    /// Check if the value is at minimum
    pub fn is_at_min(&self) -> bool {
        self.min.map_or(false, |min| (self.value - min).abs() < f64::EPSILON)
    }

    /// Check if the value is at maximum
    pub fn is_at_max(&self) -> bool {
        self.max.map_or(false, |max| (self.value - max).abs() < f64::EPSILON)
    }

    /// Increment the value by step
    pub fn increment(&mut self) {
        if self.read_only {
            return;
        }

        let new_value = self.value + self.step;
        self.set_value(new_value);
    }

    /// Decrement the value by step
    pub fn decrement(&mut self) {
        if self.read_only {
            return;
        }

        let new_value = self.value - self.step;
        self.set_value(new_value);
    }

    /// Set the value (with validation and snapping)
    pub fn set_value(&mut self, value: f64) {
        if self.read_only {
            return;
        }

        // Apply min/max constraints
        let mut clamped_value = value;
        if let Some(min) = self.min {
            clamped_value = clamped_value.max(min);
        }
        if let Some(max) = self.max {
            clamped_value = clamped_value.min(max);
        }

        // Snap to step only if step is meaningful (> 0)
        let final_value = if self.step > 0.0 {
            self.snap_to_step(clamped_value)
        } else {
            clamped_value
        };

        self.value = final_value;
        self.validate();
    }

    /// Snap value to nearest step
    fn snap_to_step(&self, value: f64) -> f64 {
        if self.step <= 0.0 {
            return value;
        }

        let rounded = (value / self.step).round() * self.step;
        
        // Round to appropriate decimal places
        let multiplier = 10_f64.powi(self.decimals as i32);
        (rounded * multiplier).round() / multiplier
    }

    /// Validate the current value
    pub fn validate(&mut self) {
        let mut errors = Vec::new();

        if let Some(min) = self.min {
            if self.value < min - f64::EPSILON {
                errors.push(format!("Value must be at least {}", self.format_value(min)));
            }
        }

        if let Some(max) = self.max {
            if self.value > max + f64::EPSILON {
                errors.push(format!("Value must be at most {}", self.format_value(max)));
            }
        }

        if errors.is_empty() {
            self.validation_state = ValidationState::Valid;
        } else {
            self.validation_state = ValidationState::Invalid(errors.join(", "));
        }
    }

    /// Handle a key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.read_only {
            return false;
        }

        match key.code {
            KeyCode::Up | KeyCode::Char('+') => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    // Shift+Up or Shift++: increment by 10 steps
                    for _ in 0..10 {
                        self.increment();
                    }
                } else {
                    self.increment();
                }
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            KeyCode::Down => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    // Shift+Down: decrement by 10 steps
                    for _ in 0..10 {
                        self.decrement();
                    }
                } else {
                    self.decrement();
                }
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            KeyCode::Char(c) => {
                // Check if valid char for number input
                let is_valid = c.is_ascii_digit() || c == '.' || c == '-';
                
                if !is_valid {
                    return false; // Invalid char, don't enter editing mode
                }
                
                // Enter editing mode if not already
                if !self.editing {
                    self.editing = true;
                    self.text_buffer.clear();
                }
                
                // Handle the char
                if c == '.' && self.text_buffer.contains('.') {
                    // Already has decimal point
                    return true;
                }
                if c == '-' && !self.text_buffer.is_empty() {
                    // Minus only allowed at start
                    return false;
                }
                self.text_buffer.push(c);
                true
            }
            KeyCode::Backspace if self.editing => {
                self.text_buffer.pop();
                true
            }
            KeyCode::Enter if self.editing => {
                // Apply text buffer value
                if !self.text_buffer.is_empty() {
                    if let Ok(parsed) = self.text_buffer.parse::<f64>() {
                        self.set_value(parsed);
                    }
                }
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            KeyCode::Esc if self.editing => {
                // Cancel editing, don't apply value
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            _ => false,
        }
    }

    /// Format value for display
    fn format_value(&self, value: f64) -> String {
        match self.format {
            NumberFormat::Decimal => {
                format!("{:.*}", self.decimals, value)
            }
            NumberFormat::Percentage => {
                format!("{:.*}%", self.decimals, value * 100.0)
            }
            NumberFormat::BasisPoints => {
                format!("{:.*} bps", self.decimals, value * 10000.0)
            }
            NumberFormat::Integer => {
                format!("{}", value.round() as i64)
            }
        }
    }

    /// Get display text
    fn display_text(&self) -> String {
        if self.editing && !self.text_buffer.is_empty() {
            self.text_buffer.clone()
        } else if self.editing && self.text_buffer.is_empty() {
            self.placeholder.as_deref().unwrap_or("").to_string()
        } else {
            self.format_value(self.value)
        }
    }

    /// Render the widget to the frame
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Determine display text
        let display_text = self.display_text();

        // Determine style based on state
        let (text_style, border_style) = self.get_styles();

        // Create block with borders
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);

        // Create paragraph with text
        let paragraph = Paragraph::new(display_text)
            .style(text_style)
            .block(block);

        f.render_widget(paragraph, area);

        // Render slider if enabled
        if self.slider_mode && self.focused {
            self.render_slider(f, area);
        }

        // Render validation error if shown
        if self.show_validation && self.focused {
            if let ValidationState::Invalid(ref error) = self.validation_state {
                self.render_validation_error(f, area, error);
            }
        }
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

    /// Render slider visualization
    fn render_slider(&self, f: &mut Frame, area: Rect) {
        if let (Some(min), Some(max)) = (self.min, self.max) {
            if max <= min {
                return;
            }

            let range = max - min;
            let position = ((self.value - min) / range).clamp(0.0, 1.0);
            let slider_width = (area.width.saturating_sub(2)).max(1);
            let slider_pos = (position * slider_width as f64) as u16;

            // Render slider bar below the input if there's space
            if area.y + area.height + 1 < f.area().height {
                let slider_area = Rect {
                    x: area.x + 1,
                    y: area.y + area.height,
                    width: slider_width,
                    height: 1,
                };

                // Create slider visualization
                let mut slider_text = String::new();
                for i in 0..slider_width {
                    if i == slider_pos {
                        slider_text.push('█');
                    } else if i < slider_pos {
                        slider_text.push('━');
                    } else {
                        slider_text.push('─');
                    }
                }

                let slider_span = Span::styled(
                    slider_text,
                    Style::default().fg(Color::Cyan),
                );
                let slider_line = Line::from(vec![slider_span]);
                let slider_paragraph = Paragraph::new(slider_line);
                f.render_widget(slider_paragraph, slider_area);
            }
        }
    }

    /// Render validation error message below the input
    fn render_validation_error(&self, f: &mut Frame, area: Rect, error: &str) {
        // Render error below the input area if there's space
        let error_y = if self.slider_mode {
            area.y + area.height + 2
        } else {
            area.y + area.height + 1
        };

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

impl Widget for NumberInputWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
        // For Widget trait implementation, we use a simpler rendering
        let display_text = self.display_text();

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
        let widget = NumberInputWidget::new();
        assert_eq!(widget.value(), 0.0);
        assert!(widget.min.is_none());
        assert!(widget.max.is_none());
        assert_eq!(widget.step, 1.0);
        assert_eq!(widget.decimals, 2);
        assert_eq!(widget.format, NumberFormat::Decimal);
        assert!(!widget.focused);
        assert!(!widget.read_only);
        assert!(!widget.slider_mode);
    }

    #[test]
    fn test_with_value() {
        let widget = NumberInputWidget::new().with_value(42.5);
        assert_eq!(widget.value(), 42.5);
    }

    #[test]
    fn test_with_min() {
        let widget = NumberInputWidget::new().with_min(10.0);
        assert_eq!(widget.min, Some(10.0));
    }

    #[test]
    fn test_with_max() {
        let widget = NumberInputWidget::new().with_max(100.0);
        assert_eq!(widget.max, Some(100.0));
    }

    #[test]
    fn test_with_step() {
        let widget = NumberInputWidget::new().with_step(0.5);
        assert_eq!(widget.step, 0.5);
    }

    #[test]
    fn test_with_step_negative_becomes_zero() {
        let widget = NumberInputWidget::new().with_step(-1.0);
        assert_eq!(widget.step, 0.0);
    }

    #[test]
    fn test_with_decimals() {
        let widget = NumberInputWidget::new().with_decimals(4);
        assert_eq!(widget.decimals, 4);
    }

    #[test]
    fn test_with_format() {
        let widget = NumberInputWidget::new().with_format(NumberFormat::Percentage);
        assert_eq!(widget.format, NumberFormat::Percentage);
    }

    #[test]
    fn test_with_placeholder() {
        let widget = NumberInputWidget::new().with_placeholder("Enter number...");
        assert_eq!(widget.placeholder, Some("Enter number...".to_string()));
    }

    #[test]
    fn test_chained_builders() {
        let widget = NumberInputWidget::new()
            .with_value(50.0)
            .with_min(0.0)
            .with_max(100.0)
            .with_step(5.0)
            .with_decimals(1)
            .with_format(NumberFormat::Percentage)
            .set_focused(true)
            .set_read_only(false)
            .set_slider_mode(true);

        assert_eq!(widget.value(), 50.0);
        assert_eq!(widget.min, Some(0.0));
        assert_eq!(widget.max, Some(100.0));
        assert_eq!(widget.step, 5.0);
        assert_eq!(widget.decimals, 1);
        assert_eq!(widget.format, NumberFormat::Percentage);
        assert!(widget.focused);
        assert!(!widget.read_only);
        assert!(widget.slider_mode);
    }

    // ========================================================================
    // Value Manipulation Tests
    // ========================================================================

    #[test]
    fn test_increment() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        widget.increment();
        assert_eq!(widget.value(), 11.0);
    }

    #[test]
    fn test_decrement() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        widget.decrement();
        assert_eq!(widget.value(), 9.0);
    }

    #[test]
    fn test_increment_with_step() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_step(5.0);
        widget.increment();
        assert_eq!(widget.value(), 15.0);
    }

    #[test]
    fn test_decrement_with_step() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_step(5.0);
        widget.decrement();
        assert_eq!(widget.value(), 5.0);
    }

    #[test]
    fn test_increment_respects_max() {
        let mut widget = NumberInputWidget::new()
            .with_value(95.0)
            .with_max(100.0)
            .with_step(10.0);
        widget.increment();
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_decrement_respects_min() {
        let mut widget = NumberInputWidget::new()
            .with_value(5.0)
            .with_min(0.0)
            .with_step(10.0);
        widget.decrement();
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_increment_read_only() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .set_read_only(true);
        widget.increment();
        assert_eq!(widget.value(), 10.0); // Should not change
    }

    #[test]
    fn test_decrement_read_only() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .set_read_only(true);
        widget.decrement();
        assert_eq!(widget.value(), 10.0); // Should not change
    }

    #[test]
    fn test_set_value() {
        let mut widget = NumberInputWidget::new().with_step(0.0); // No snapping
        widget.set_value(42.5);
        assert_eq!(widget.value(), 42.5);
    }

    #[test]
    fn test_set_value_respects_min() {
        let mut widget = NumberInputWidget::new()
            .with_min(10.0);
        widget.set_value(5.0);
        assert_eq!(widget.value(), 10.0);
    }

    #[test]
    fn test_set_value_respects_max() {
        let mut widget = NumberInputWidget::new()
            .with_max(100.0);
        widget.set_value(150.0);
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_set_value_respects_both_min_max() {
        let mut widget = NumberInputWidget::new()
            .with_min(10.0)
            .with_max(100.0);
        widget.set_value(5.0);
        assert_eq!(widget.value(), 10.0);
        widget.set_value(150.0);
        assert_eq!(widget.value(), 100.0);
    }

    #[test]
    fn test_set_value_read_only() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .set_read_only(true);
        widget.set_value(20.0);
        assert_eq!(widget.value(), 10.0); // Should not change
    }

    #[test]
    fn test_set_value_snaps_to_step() {
        let mut widget = NumberInputWidget::new()
            .with_step(5.0)
            .with_decimals(0);
        widget.set_value(12.0);
        assert_eq!(widget.value(), 10.0); // Snaps to nearest step
    }

    #[test]
    fn test_set_value_snaps_to_step_with_decimals() {
        let mut widget = NumberInputWidget::new()
            .with_step(0.25)
            .with_decimals(2);
        widget.set_value(0.13);
        assert_eq!(widget.value(), 0.25); // Snaps to nearest step
    }

    #[test]
    fn test_snap_to_step_zero_step() {
        let widget = NumberInputWidget::new().with_step(0.0);
        assert_eq!(widget.snap_to_step(42.5), 42.5);
    }

    #[test]
    fn test_snap_to_step_negative_step() {
        let widget = NumberInputWidget::new().with_step(-1.0);
        assert_eq!(widget.snap_to_step(42.5), 42.5);
    }

    #[test]
    fn test_snap_to_step_exact() {
        let widget = NumberInputWidget::new().with_step(5.0);
        assert_eq!(widget.snap_to_step(15.0), 15.0);
    }

    #[test]
    fn test_snap_to_step_rounds_up() {
        let widget = NumberInputWidget::new().with_step(5.0).with_decimals(0);
        assert_eq!(widget.snap_to_step(12.0), 10.0);
        assert_eq!(widget.snap_to_step(13.0), 15.0);
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_no_constraints() {
        let mut widget = NumberInputWidget::new().with_value(42.5);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_within_range() {
        let mut widget = NumberInputWidget::new()
            .with_value(50.0)
            .with_min(0.0)
            .with_max(100.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_below_min() {
        let mut widget = NumberInputWidget::new()
            .with_value(5.0)
            .with_min(10.0);
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().is_some());
    }

    #[test]
    fn test_validate_above_max() {
        let mut widget = NumberInputWidget::new()
            .with_value(150.0)
            .with_max(100.0);
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().is_some());
    }

    #[test]
    fn test_validate_at_min_boundary() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_min(10.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_at_max_boundary() {
        let mut widget = NumberInputWidget::new()
            .with_value(100.0)
            .with_max(100.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_epsilon_tolerance() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0 + f64::EPSILON * 0.5)
            .with_min(10.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_is_at_min() {
        let widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_min(10.0);
        assert!(widget.is_at_min());
    }

    #[test]
    fn test_is_at_max() {
        let widget = NumberInputWidget::new()
            .with_value(100.0)
            .with_max(100.0);
        assert!(widget.is_at_max());
    }

    #[test]
    fn test_is_at_min_false() {
        let widget = NumberInputWidget::new()
            .with_value(15.0)
            .with_min(10.0);
        assert!(!widget.is_at_min());
    }

    #[test]
    fn test_is_at_max_false() {
        let widget = NumberInputWidget::new()
            .with_value(90.0)
            .with_max(100.0);
        assert!(!widget.is_at_max());
    }

    #[test]
    fn test_is_at_min_no_min() {
        let widget = NumberInputWidget::new().with_value(10.0);
        assert!(!widget.is_at_min());
    }

    #[test]
    fn test_is_at_max_no_max() {
        let widget = NumberInputWidget::new().with_value(100.0);
        assert!(!widget.is_at_max());
    }

    // ========================================================================
    // Format Display Tests
    // ========================================================================

    #[test]
    fn test_format_decimal() {
        let widget = NumberInputWidget::new()
            .with_value(123.456)
            .with_decimals(2)
            .with_format(NumberFormat::Decimal);
        assert_eq!(widget.format_value(123.456), "123.46");
    }

    #[test]
    fn test_format_percentage() {
        let widget = NumberInputWidget::new()
            .with_value(0.1234)
            .with_decimals(2)
            .with_format(NumberFormat::Percentage);
        assert_eq!(widget.format_value(0.1234), "12.34%");
    }

    #[test]
    fn test_format_basis_points() {
        let widget = NumberInputWidget::new()
            .with_value(0.01234)
            .with_decimals(1)
            .with_format(NumberFormat::BasisPoints);
        assert_eq!(widget.format_value(0.01234), "123.4 bps");
    }

    #[test]
    fn test_format_integer() {
        let widget = NumberInputWidget::new()
            .with_value(123.456)
            .with_format(NumberFormat::Integer);
        assert_eq!(widget.format_value(123.456), "123");
    }

    #[test]
    fn test_format_integer_rounds() {
        let widget = NumberInputWidget::new()
            .with_value(123.7)
            .with_format(NumberFormat::Integer);
        assert_eq!(widget.format_value(123.7), "124");
    }

    #[test]
    fn test_format_zero() {
        let widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_format(NumberFormat::Decimal);
        assert_eq!(widget.format_value(0.0), "0.00");
    }

    #[test]
    fn test_format_negative() {
        let widget = NumberInputWidget::new()
            .with_value(-42.5)
            .with_format(NumberFormat::Decimal);
        assert_eq!(widget.format_value(-42.5), "-42.50");
    }

    #[test]
    fn test_format_large_number() {
        let widget = NumberInputWidget::new()
            .with_value(1234567.89)
            .with_decimals(2)
            .with_format(NumberFormat::Decimal);
        assert_eq!(widget.format_value(1234567.89), "1234567.89");
    }

    #[test]
    fn test_format_small_number() {
        let widget = NumberInputWidget::new()
            .with_value(0.0001)
            .with_decimals(6)
            .with_format(NumberFormat::Decimal);
        assert_eq!(widget.format_value(0.0001), "0.000100");
    }

    #[test]
    fn test_format_percentage_100() {
        let widget = NumberInputWidget::new()
            .with_value(1.0)
            .with_decimals(0)
            .with_format(NumberFormat::Percentage);
        assert_eq!(widget.format_value(1.0), "100%");
    }

    #[test]
    fn test_format_basis_points_100() {
        let widget = NumberInputWidget::new()
            .with_value(0.01)
            .with_decimals(0)
            .with_format(NumberFormat::BasisPoints);
        assert_eq!(widget.format_value(0.01), "100 bps");
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_up() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.value(), 11.0);
    }

    #[test]
    fn test_handle_key_down() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.value(), 9.0);
    }

    #[test]
    fn test_handle_key_plus() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key = KeyEvent::new(KeyCode::Char('+'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.value(), 11.0);
    }

    #[test]
    fn test_handle_key_minus() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        // Use Down key for decrement, '-' is now for text editing
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.value(), 9.0);
    }

    #[test]
    fn test_handle_key_shift_up() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_step(1.0);
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::SHIFT);
        assert!(widget.handle_key(key));
        assert_eq!(widget.value(), 20.0); // 10 steps
    }

    #[test]
    fn test_handle_key_shift_down() {
        let mut widget = NumberInputWidget::new()
            .with_value(20.0)
            .with_step(1.0);
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::SHIFT);
        assert!(widget.handle_key(key));
        assert_eq!(widget.value(), 10.0); // 10 steps
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert_eq!(widget.value(), 10.0);
    }

    #[test]
    fn test_handle_key_text_editing_digit() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key = KeyEvent::new(KeyCode::Char('5'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.editing);
        assert_eq!(widget.text_buffer, "5");
    }

    #[test]
    fn test_handle_key_text_editing_decimal() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('1'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('.'), KeyModifiers::empty());
        let key3 = KeyEvent::new(KeyCode::Char('5'), KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        widget.handle_key(key3);
        assert_eq!(widget.text_buffer, "1.5");
    }

    #[test]
    fn test_handle_key_text_editing_negative() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('-'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('5'), KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        assert_eq!(widget.text_buffer, "-5");
    }

    #[test]
    fn test_handle_key_text_editing_double_decimal() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('1'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('.'), KeyModifiers::empty());
        let key3 = KeyEvent::new(KeyCode::Char('.'), KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        widget.handle_key(key3); // Should be ignored
        assert_eq!(widget.text_buffer, "1.");
    }

    #[test]
    fn test_handle_key_text_editing_backspace() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('1'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('2'), KeyModifiers::empty());
        let key3 = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        widget.handle_key(key3);
        assert_eq!(widget.text_buffer, "1");
    }

    #[test]
    fn test_handle_key_text_editing_enter_applies() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_min(0.0)
            .with_max(100.0);
        let key1 = KeyEvent::new(KeyCode::Char('4'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('2'), KeyModifiers::empty());
        let key3 = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        widget.handle_key(key3);
        assert_eq!(widget.value(), 42.0);
        assert!(!widget.editing);
    }

    #[test]
    fn test_handle_key_text_editing_esc_cancels() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('4'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Esc, KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        assert_eq!(widget.value(), 10.0); // Original value preserved
        assert!(!widget.editing);
    }

    #[test]
    fn test_handle_key_text_editing_invalid_char() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        assert!(!widget.handle_key(key1));
        assert!(!widget.editing);
    }

    #[test]
    fn test_handle_key_text_editing_negative_not_first() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        let key1 = KeyEvent::new(KeyCode::Char('1'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('-'), KeyModifiers::empty());
        widget.handle_key(key1);
        assert!(!widget.handle_key(key2)); // Minus not allowed after digits
    }

    // ========================================================================
    // Focus and Read-Only Tests
    // ========================================================================

    #[test]
    fn test_set_focused() {
        let widget = NumberInputWidget::new().set_focused(true);
        assert!(widget.focused);
    }

    #[test]
    fn test_set_focused_false_clears_editing() {
        let mut widget = NumberInputWidget::new().set_focused(true);
        widget.editing = true;
        widget.text_buffer = "123".to_string();
        widget = widget.set_focused(false);
        assert!(!widget.editing);
        assert!(widget.text_buffer.is_empty());
    }

    #[test]
    fn test_set_read_only() {
        let widget = NumberInputWidget::new().set_read_only(true);
        assert!(widget.read_only);
    }

    #[test]
    fn test_set_read_only_clears_editing() {
        let mut widget = NumberInputWidget::new();
        widget.editing = true;
        widget.text_buffer = "123".to_string();
        widget = widget.set_read_only(true);
        assert!(!widget.editing);
        assert!(widget.text_buffer.is_empty());
    }

    // ========================================================================
    // Slider Mode Tests
    // ========================================================================

    #[test]
    fn test_set_slider_mode() {
        let widget = NumberInputWidget::new().set_slider_mode(true);
        assert!(widget.slider_mode);
    }

    #[test]
    fn test_slider_mode_disabled_by_default() {
        let widget = NumberInputWidget::new();
        assert!(!widget.slider_mode);
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_very_large_value() {
        let mut widget = NumberInputWidget::new().with_value(1e10);
        widget.increment();
        assert_eq!(widget.value(), 1e10 + 1.0);
    }

    #[test]
    fn test_very_small_value() {
        let mut widget = NumberInputWidget::new()
            .with_value(1e-10)
            .with_step(1e-10); // Use very small step
        widget.decrement();
        // Decrementing by step should give 0.0
        assert!((widget.value() - 0.0).abs() < 1e-15);
    }

    #[test]
    fn test_very_small_step() {
        let mut widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_step(1e-10)
            .with_decimals(10);
        widget.increment();
        // Allow for floating point precision issues
        assert!((widget.value() - 1e-10).abs() < 1e-15);
    }

    #[test]
    fn test_very_large_step() {
        let mut widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_step(1e10);
        widget.increment();
        assert_eq!(widget.value(), 1e10);
    }

    #[test]
    fn test_zero_step() {
        let mut widget = NumberInputWidget::new()
            .with_value(10.0)
            .with_step(0.0);
        widget.increment();
        assert_eq!(widget.value(), 10.0); // No change with zero step
    }

    #[test]
    fn test_negative_value() {
        let mut widget = NumberInputWidget::new().with_value(-10.0);
        widget.increment();
        assert_eq!(widget.value(), -9.0);
    }

    #[test]
    fn test_negative_value_with_min() {
        let mut widget = NumberInputWidget::new()
            .with_value(-5.0)
            .with_min(-10.0);
        widget.decrement();
        assert_eq!(widget.value(), -6.0);
    }

    #[test]
    fn test_increment_at_max() {
        let mut widget = NumberInputWidget::new()
            .with_value(100.0)
            .with_max(100.0);
        widget.increment();
        assert_eq!(widget.value(), 100.0); // Should not exceed max
    }

    #[test]
    fn test_decrement_at_min() {
        let mut widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_min(0.0);
        widget.decrement();
        assert_eq!(widget.value(), 0.0); // Should not go below min
    }

    #[test]
    fn test_min_equals_max() {
        let mut widget = NumberInputWidget::new()
            .with_value(50.0)
            .with_min(50.0)
            .with_max(50.0);
        widget.increment();
        widget.decrement();
        assert_eq!(widget.value(), 50.0);
    }

    #[test]
    fn test_min_greater_than_max() {
        // This is an invalid state, but widget should handle it gracefully
        let mut widget = NumberInputWidget::new()
            .with_value(50.0)
            .with_min(100.0)
            .with_max(0.0);
        widget.set_value(75.0);
        // Value should be clamped to max (0.0) since max < min
        assert_eq!(widget.value(), 0.0);
    }

    #[test]
    fn test_rapid_increment_decrement() {
        let mut widget = NumberInputWidget::new().with_value(0.0);
        for _ in 0..100 {
            widget.increment();
        }
        assert_eq!(widget.value(), 100.0);
        for _ in 0..50 {
            widget.decrement();
        }
        assert_eq!(widget.value(), 50.0);
    }

    #[test]
    fn test_snap_to_step_precision() {
        let widget = NumberInputWidget::new()
            .with_step(0.1)
            .with_decimals(1);
        let snapped = widget.snap_to_step(0.123456);
        assert!((snapped - 0.1).abs() < 0.01);
    }

    #[test]
    fn test_format_nan() {
        let widget = NumberInputWidget::new();
        // NaN should not panic
        let formatted = widget.format_value(f64::NAN);
        assert!(formatted.contains("NaN") || formatted.contains("nan"));
    }

    #[test]
    fn test_format_infinity() {
        let widget = NumberInputWidget::new();
        // Infinity should not panic
        let formatted = widget.format_value(f64::INFINITY);
        assert!(formatted.contains("inf") || formatted.contains("Inf"));
    }

    #[test]
    fn test_format_negative_infinity() {
        let widget = NumberInputWidget::new();
        // Negative infinity should not panic
        let formatted = widget.format_value(f64::NEG_INFINITY);
        assert!(formatted.contains("inf") || formatted.contains("Inf"));
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_editing_workflow() {
        let mut widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_min(0.0)
            .with_max(100.0)
            .with_step(1.0)
            .with_decimals(2)
            .set_focused(true);

        // Increment
        widget.increment();
        assert_eq!(widget.value(), 1.0);

        // Decrement
        widget.decrement();
        assert_eq!(widget.value(), 0.0);

        // Text editing
        let key1 = KeyEvent::new(KeyCode::Char('4'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('2'), KeyModifiers::empty());
        let key3 = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        widget.handle_key(key3);
        assert_eq!(widget.value(), 42.0);
    }

    #[test]
    fn test_percentage_workflow() {
        let mut widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_min(0.0)
            .with_max(1.0)
            .with_step(0.01)
            .with_decimals(2)
            .with_format(NumberFormat::Percentage)
            .set_focused(true);

        widget.set_value(0.5);
        assert_eq!(widget.value(), 0.5);
        assert_eq!(widget.format_value(0.5), "50.00%");
    }

    #[test]
    fn test_basis_points_workflow() {
        let mut widget = NumberInputWidget::new()
            .with_value(0.0)
            .with_min(0.0)
            .with_max(0.1)
            .with_step(0.0) // No snapping to preserve exact value
            .with_decimals(1)
            .with_format(NumberFormat::BasisPoints)
            .set_focused(true);

        widget.set_value(0.0125);
        // Value might be snapped, so check it's close
        assert!((widget.value() - 0.0125).abs() < 0.0001);
        assert_eq!(widget.format_value(0.0125), "125.0 bps");
    }

    #[test]
    fn test_slider_workflow() {
        let mut widget = NumberInputWidget::new()
            .with_value(50.0)
            .with_min(0.0)
            .with_max(100.0)
            .with_step(1.0)
            .set_slider_mode(true)
            .set_focused(true);

        widget.increment();
        assert_eq!(widget.value(), 51.0);
        assert!(widget.slider_mode);
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = NumberInputWidget::new()
            .with_value(42.5)
            .with_min(0.0)
            .with_max(100.0)
            .with_step(5.0)
            .with_decimals(2)
            .with_format(NumberFormat::Percentage)
            .set_focused(true)
            .set_slider_mode(true);

        let widget2 = widget1.clone();
        assert_eq!(widget1.value(), widget2.value());
        assert_eq!(widget1.min, widget2.min);
        assert_eq!(widget1.max, widget2.max);
        assert_eq!(widget1.step, widget2.step);
        assert_eq!(widget1.decimals, widget2.decimals);
        assert_eq!(widget1.format, widget2.format);
        assert_eq!(widget1.focused, widget2.focused);
        assert_eq!(widget1.slider_mode, widget2.slider_mode);
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = NumberInputWidget::new().with_value(10.0);
        let mut widget2 = widget1.clone();

        widget1.increment();
        widget2.decrement();

        assert_eq!(widget1.value(), 11.0);
        assert_eq!(widget2.value(), 9.0);
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1 = NumberInputWidget::default();
        let widget2 = NumberInputWidget::new();
        assert_eq!(widget1.value(), widget2.value());
        assert_eq!(widget1.step, widget2.step);
        assert_eq!(widget1.decimals, widget2.decimals);
    }

    // ========================================================================
    // Display Text Tests
    // ========================================================================

    #[test]
    fn test_display_text_normal() {
        let widget = NumberInputWidget::new()
            .with_value(42.5)
            .with_decimals(1);
        assert_eq!(widget.display_text(), "42.5");
    }

    #[test]
    fn test_display_text_editing() {
        let mut widget = NumberInputWidget::new().with_value(10.0);
        widget.editing = true;
        widget.text_buffer = "123".to_string();
        assert_eq!(widget.display_text(), "123");
    }

    #[test]
    fn test_display_text_editing_empty_with_placeholder() {
        let mut widget = NumberInputWidget::new()
            .with_placeholder("Enter number...");
        widget.editing = true;
        assert_eq!(widget.display_text(), "Enter number...");
    }

    #[test]
    fn test_display_text_editing_empty_no_placeholder() {
        let mut widget = NumberInputWidget::new();
        widget.editing = true;
        assert_eq!(widget.display_text(), "");
    }
}
