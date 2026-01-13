//! Toggle Widget
//!
//! A reusable boolean toggle widget for TUI parameter configuration.
//! Supports checkbox and switch visual styles, keyboard toggling (space/enter),
//! and read-only mode.

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

// ============================================================================
// ToggleWidget
// ============================================================================

/// Toggle widget for TUI parameter configuration
pub struct ToggleWidget {
    /// Current boolean value
    value: bool,
    /// Visual style (checkbox or switch)
    style: ToggleStyle,
    /// Label text displayed next to toggle
    label: Option<String>,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
}

impl Clone for ToggleWidget {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            style: self.style.clone(),
            label: self.label.clone(),
            focused: self.focused,
            read_only: self.read_only,
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
        }
    }
}

impl std::fmt::Debug for ToggleWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToggleWidget")
            .field("value", &self.value)
            .field("style", &self.style)
            .field("label", &self.label)
            .field("focused", &self.focused)
            .field("read_only", &self.read_only)
            .field("validation_state", &self.validation_state)
            .finish()
    }
}

/// Visual style for toggle widget
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToggleStyle {
    /// Checkbox style: [ ] or [X]
    Checkbox,
    /// Switch style: OFF or ON
    Switch,
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

impl Default for ToggleWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl ToggleWidget {
    /// Create a new toggle widget
    pub fn new() -> Self {
        Self {
            value: false,
            style: ToggleStyle::Checkbox,
            label: None,
            focused: false,
            read_only: false,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
        }
    }

    /// Set the initial value
    pub fn with_value(mut self, value: bool) -> Self {
        self.value = value;
        self.validate();
        self
    }

    /// Set the visual style
    pub fn with_style(mut self, style: ToggleStyle) -> Self {
        self.style = style;
        self
    }

    /// Set label text
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
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

    /// Get the current value
    pub fn value(&self) -> bool {
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

    /// Toggle the value
    pub fn toggle(&mut self) {
        if self.read_only {
            return;
        }
        self.value = !self.value;
        self.validate();
    }

    /// Set the value directly
    pub fn set_value(&mut self, value: bool) {
        if self.read_only {
            return;
        }
        self.value = value;
        self.validate();
    }

    /// Validate the current value
    pub fn validate(&mut self) {
        // Toggle widgets are always valid (boolean can only be true or false)
        self.validation_state = ValidationState::Valid;
    }

    /// Handle a key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.read_only {
            return false;
        }

        match key.code {
            KeyCode::Char(' ') | KeyCode::Enter => {
                self.toggle();
                true
            }
            KeyCode::Char('t') | KeyCode::Char('T') => {
                // 't' for toggle
                self.toggle();
                true
            }
            KeyCode::Char('y') | KeyCode::Char('Y') => {
                // 'y' for yes (set to true)
                self.set_value(true);
                true
            }
            KeyCode::Char('n') | KeyCode::Char('N') => {
                // 'n' for no (set to false)
                self.set_value(false);
                true
            }
            _ => false,
        }
    }

    /// Get display text for the toggle
    fn display_text(&self) -> String {
        let indicator = match self.style {
            ToggleStyle::Checkbox => {
                if self.value {
                    "[X]"
                } else {
                    "[ ]"
                }
            }
            ToggleStyle::Switch => {
                if self.value {
                    "ON"
                } else {
                    "OFF"
                }
            }
        };

        if let Some(ref label) = self.label {
            format!("{} {}", indicator, label)
        } else {
            indicator.to_string()
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
        } else if self.value {
            Style::default().fg(Color::Green)
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

    /// Render validation error message below the input
    fn render_validation_error(&self, f: &mut Frame, area: Rect, error: &str) {
        // Render error below the input area if there's space
        if area.y + area.height + 1 < f.area().height {
            let error_area = Rect {
                x: area.x,
                y: area.y + area.height,
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

impl Widget for ToggleWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
        // For Widget trait implementation, simpler rendering
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
        let widget = ToggleWidget::new();
        assert!(!widget.value());
        assert_eq!(widget.style, ToggleStyle::Checkbox);
        assert!(widget.label.is_none());
        assert!(!widget.focused);
        assert!(!widget.read_only);
    }

    #[test]
    fn test_with_value() {
        let widget = ToggleWidget::new().with_value(true);
        assert!(widget.value());
    }

    #[test]
    fn test_with_style() {
        let widget = ToggleWidget::new().with_style(ToggleStyle::Switch);
        assert_eq!(widget.style, ToggleStyle::Switch);
    }

    #[test]
    fn test_with_label() {
        let widget = ToggleWidget::new().with_label("Enable feature");
        assert_eq!(widget.label, Some("Enable feature".to_string()));
    }

    #[test]
    fn test_chained_builders() {
        let widget = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Switch)
            .with_label("Test")
            .set_focused(true)
            .set_read_only(false);

        assert!(widget.value());
        assert_eq!(widget.style, ToggleStyle::Switch);
        assert_eq!(widget.label, Some("Test".to_string()));
        assert!(widget.focused);
        assert!(!widget.read_only);
    }

    // ========================================================================
    // Value Manipulation Tests
    // ========================================================================

    #[test]
    fn test_toggle() {
        let mut widget = ToggleWidget::new();
        assert!(!widget.value());
        widget.toggle();
        assert!(widget.value());
        widget.toggle();
        assert!(!widget.value());
    }

    #[test]
    fn test_toggle_read_only() {
        let mut widget = ToggleWidget::new()
            .with_value(false)
            .set_read_only(true);
        widget.toggle();
        assert!(!widget.value()); // Should not change
    }

    #[test]
    fn test_set_value() {
        let mut widget = ToggleWidget::new();
        widget.set_value(true);
        assert!(widget.value());
        widget.set_value(false);
        assert!(!widget.value());
    }

    #[test]
    fn test_set_value_read_only() {
        let mut widget = ToggleWidget::new()
            .with_value(false)
            .set_read_only(true);
        widget.set_value(true);
        assert!(!widget.value()); // Should not change
    }

    #[test]
    fn test_multiple_toggles() {
        let mut widget = ToggleWidget::new();
        for _ in 0..10 {
            widget.toggle();
        }
        assert!(!widget.value()); // Even number of toggles
    }

    #[test]
    fn test_rapid_toggle() {
        let mut widget = ToggleWidget::new();
        for _ in 0..1000 {
            widget.toggle();
        }
        assert!(!widget.value()); // Even number of toggles
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate() {
        let mut widget = ToggleWidget::new();
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_always_valid() {
        let mut widget = ToggleWidget::new().with_value(true);
        widget.validate();
        assert!(widget.is_valid());

        widget.set_value(false);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validation_error_none() {
        let widget = ToggleWidget::new();
        assert!(widget.validation_error().is_none());
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_space() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.value());
    }

    #[test]
    fn test_handle_key_enter() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.value());
    }

    #[test]
    fn test_handle_key_t() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Char('t'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.value());
    }

    #[test]
    fn test_handle_key_t_uppercase() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Char('T'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.value());
    }

    #[test]
    fn test_handle_key_y() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Char('y'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.value());
    }

    #[test]
    fn test_handle_key_y_uppercase() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.value());
    }

    #[test]
    fn test_handle_key_n() {
        let mut widget = ToggleWidget::new().with_value(true);
        let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(!widget.value());
    }

    #[test]
    fn test_handle_key_n_uppercase() {
        let mut widget = ToggleWidget::new().with_value(true);
        let key = KeyEvent::new(KeyCode::Char('N'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(!widget.value());
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut widget = ToggleWidget::new();
        let key = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert!(!widget.value());
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = ToggleWidget::new()
            .with_value(false)
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert!(!widget.value());
    }

    #[test]
    fn test_handle_key_sequence() {
        let mut widget = ToggleWidget::new();
        let key1 = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key1);
        assert!(widget.value());
        widget.handle_key(key2);
        assert!(!widget.value());
    }

    #[test]
    fn test_handle_key_y_when_true() {
        let mut widget = ToggleWidget::new().with_value(true);
        let key = KeyEvent::new(KeyCode::Char('y'), KeyModifiers::empty());
        widget.handle_key(key);
        assert!(widget.value()); // Should stay true
    }

    #[test]
    fn test_handle_key_n_when_false() {
        let mut widget = ToggleWidget::new().with_value(false);
        let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::empty());
        widget.handle_key(key);
        assert!(!widget.value()); // Should stay false
    }

    // ========================================================================
    // Display Text Tests
    // ========================================================================

    #[test]
    fn test_display_text_checkbox_false() {
        let widget = ToggleWidget::new()
            .with_value(false)
            .with_style(ToggleStyle::Checkbox);
        assert_eq!(widget.display_text(), "[ ]");
    }

    #[test]
    fn test_display_text_checkbox_true() {
        let widget = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Checkbox);
        assert_eq!(widget.display_text(), "[X]");
    }

    #[test]
    fn test_display_text_switch_false() {
        let widget = ToggleWidget::new()
            .with_value(false)
            .with_style(ToggleStyle::Switch);
        assert_eq!(widget.display_text(), "OFF");
    }

    #[test]
    fn test_display_text_switch_true() {
        let widget = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Switch);
        assert_eq!(widget.display_text(), "ON");
    }

    #[test]
    fn test_display_text_with_label_checkbox() {
        let widget = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Checkbox)
            .with_label("Enable");
        assert_eq!(widget.display_text(), "[X] Enable");
    }

    #[test]
    fn test_display_text_with_label_switch() {
        let widget = ToggleWidget::new()
            .with_value(false)
            .with_style(ToggleStyle::Switch)
            .with_label("Feature");
        assert_eq!(widget.display_text(), "OFF Feature");
    }

    #[test]
    fn test_display_text_empty_label() {
        let widget = ToggleWidget::new()
            .with_value(true)
            .with_label("");
        assert_eq!(widget.display_text(), "[X] ");
    }

    #[test]
    fn test_display_text_long_label() {
        let widget = ToggleWidget::new()
            .with_value(false)
            .with_label("This is a very long label text");
        assert_eq!(widget.display_text(), "[ ] This is a very long label text");
    }

    // ========================================================================
    // Focus and Read-Only Tests
    // ========================================================================

    #[test]
    fn test_set_focused() {
        let widget = ToggleWidget::new().set_focused(true);
        assert!(widget.focused);
    }

    #[test]
    fn test_set_read_only() {
        let widget = ToggleWidget::new().set_read_only(true);
        assert!(widget.read_only);
    }

    #[test]
    fn test_read_only_prevents_toggle() {
        let mut widget = ToggleWidget::new()
            .with_value(false)
            .set_read_only(true);
        widget.toggle();
        assert!(!widget.value());
    }

    #[test]
    fn test_read_only_prevents_set_value() {
        let mut widget = ToggleWidget::new()
            .with_value(false)
            .set_read_only(true);
        widget.set_value(true);
        assert!(!widget.value());
    }

    #[test]
    fn test_read_only_prevents_key_handling() {
        let mut widget = ToggleWidget::new()
            .with_value(false)
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert!(!widget.value());
    }

    // ========================================================================
    // Style Tests
    // ========================================================================

    #[test]
    fn test_style_checkbox_default() {
        let widget = ToggleWidget::new();
        assert_eq!(widget.style, ToggleStyle::Checkbox);
    }

    #[test]
    fn test_style_switch() {
        let widget = ToggleWidget::new().with_style(ToggleStyle::Switch);
        assert_eq!(widget.style, ToggleStyle::Switch);
    }

    #[test]
    fn test_style_switch_toggle() {
        let mut widget = ToggleWidget::new()
            .with_style(ToggleStyle::Switch)
            .with_value(false);
        widget.toggle();
        assert!(widget.value());
        assert_eq!(widget.display_text(), "ON");
    }

    #[test]
    fn test_style_checkbox_toggle() {
        let mut widget = ToggleWidget::new()
            .with_style(ToggleStyle::Checkbox)
            .with_value(false);
        widget.toggle();
        assert!(widget.value());
        assert_eq!(widget.display_text(), "[X]");
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_very_long_label() {
        let long_label = "A".repeat(1000);
        let widget = ToggleWidget::new().with_label(&long_label);
        assert_eq!(widget.label, Some(long_label));
    }

    #[test]
    fn test_unicode_label() {
        let widget = ToggleWidget::new().with_label("启用功能 🚀");
        assert_eq!(widget.label, Some("启用功能 🚀".to_string()));
    }

    #[test]
    fn test_special_characters_label() {
        let widget = ToggleWidget::new().with_label("Enable & Feature (v2.0)");
        assert_eq!(widget.label, Some("Enable & Feature (v2.0)".to_string()));
    }

    #[test]
    fn test_empty_string_label() {
        let widget = ToggleWidget::new().with_label("");
        assert_eq!(widget.label, Some("".to_string()));
    }

    #[test]
    fn test_alternating_toggles() {
        let mut widget = ToggleWidget::new();
        for i in 0..100 {
            widget.toggle();
            assert_eq!(widget.value(), i % 2 == 0);
        }
    }

    #[test]
    fn test_set_same_value() {
        let mut widget = ToggleWidget::new().with_value(true);
        widget.set_value(true);
        assert!(widget.value());
        widget.set_value(true);
        assert!(widget.value());
    }

    #[test]
    fn test_toggle_after_set() {
        let mut widget = ToggleWidget::new();
        widget.set_value(true);
        assert!(widget.value());
        widget.toggle();
        assert!(!widget.value());
        widget.set_value(false);
        assert!(!widget.value());
        widget.toggle();
        assert!(widget.value());
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_toggle_workflow() {
        let mut widget = ToggleWidget::new()
            .with_style(ToggleStyle::Checkbox)
            .with_label("Enable feature")
            .set_focused(true);

        // Start false
        assert!(!widget.value());
        assert_eq!(widget.display_text(), "[ ] Enable feature");

        // Toggle to true
        widget.toggle();
        assert!(widget.value());
        assert_eq!(widget.display_text(), "[X] Enable feature");

        // Toggle back to false
        widget.toggle();
        assert!(!widget.value());
        assert_eq!(widget.display_text(), "[ ] Enable feature");
    }

    #[test]
    fn test_keyboard_workflow() {
        let mut widget = ToggleWidget::new()
            .with_style(ToggleStyle::Switch)
            .set_focused(true);

        // Space to toggle
        let key1 = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        widget.handle_key(key1);
        assert!(widget.value());
        assert_eq!(widget.display_text(), "ON");

        // Enter to toggle back
        let key2 = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key2);
        assert!(!widget.value());
        assert_eq!(widget.display_text(), "OFF");

        // 'y' to set true
        let key3 = KeyEvent::new(KeyCode::Char('y'), KeyModifiers::empty());
        widget.handle_key(key3);
        assert!(widget.value());

        // 'n' to set false
        let key4 = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::empty());
        widget.handle_key(key4);
        assert!(!widget.value());
    }

    #[test]
    fn test_read_only_workflow() {
        let mut widget = ToggleWidget::new()
            .with_value(true)
            .set_read_only(true)
            .set_focused(true);

        // Try to toggle
        widget.toggle();
        assert!(widget.value()); // Should not change

        // Try to set value
        widget.set_value(false);
        assert!(widget.value()); // Should not change

        // Try keyboard
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert!(widget.value()); // Should not change
    }

    #[test]
    fn test_style_switching() {
        let mut widget = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Checkbox);
        assert_eq!(widget.display_text(), "[X]");

        widget = widget.with_style(ToggleStyle::Switch);
        assert_eq!(widget.display_text(), "ON");

        widget.toggle();
        assert_eq!(widget.display_text(), "OFF");
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Switch)
            .with_label("Test")
            .set_focused(true)
            .set_read_only(false);

        let widget2 = widget1.clone();
        assert_eq!(widget1.value(), widget2.value());
        assert_eq!(widget1.style, widget2.style);
        assert_eq!(widget1.label, widget2.label);
        assert_eq!(widget1.focused, widget2.focused);
        assert_eq!(widget1.read_only, widget2.read_only);
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = ToggleWidget::new().with_value(false);
        let mut widget2 = widget1.clone();

        widget1.toggle();
        widget2.set_value(true);

        assert!(widget1.value());
        assert!(widget2.value());
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1 = ToggleWidget::default();
        let widget2 = ToggleWidget::new();
        assert_eq!(widget1.value(), widget2.value());
        assert_eq!(widget1.style, widget2.style);
    }

    // ========================================================================
    // Validation State Tests
    // ========================================================================

    #[test]
    fn test_validation_after_toggle() {
        let mut widget = ToggleWidget::new();
        widget.toggle();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validation_after_set_value() {
        let mut widget = ToggleWidget::new();
        widget.set_value(true);
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validation_multiple_operations() {
        let mut widget = ToggleWidget::new();
        for _ in 0..100 {
            widget.toggle();
            assert!(widget.is_valid());
        }
    }

    // ========================================================================
    // Display Consistency Tests
    // ========================================================================

    #[test]
    fn test_display_consistency_checkbox() {
        let widget_false = ToggleWidget::new()
            .with_value(false)
            .with_style(ToggleStyle::Checkbox);
        let widget_true = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Checkbox);

        assert_eq!(widget_false.display_text(), "[ ]");
        assert_eq!(widget_true.display_text(), "[X]");
    }

    #[test]
    fn test_display_consistency_switch() {
        let widget_false = ToggleWidget::new()
            .with_value(false)
            .with_style(ToggleStyle::Switch);
        let widget_true = ToggleWidget::new()
            .with_value(true)
            .with_style(ToggleStyle::Switch);

        assert_eq!(widget_false.display_text(), "OFF");
        assert_eq!(widget_true.display_text(), "ON");
    }

    #[test]
    fn test_display_with_label_consistency() {
        let widget1 = ToggleWidget::new()
            .with_value(false)
            .with_label("Test");
        let widget2 = ToggleWidget::new()
            .with_value(true)
            .with_label("Test");

        assert_eq!(widget1.display_text(), "[ ] Test");
        assert_eq!(widget2.display_text(), "[X] Test");
    }
}
