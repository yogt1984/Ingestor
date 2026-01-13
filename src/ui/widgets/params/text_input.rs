//! Text Input Widget
//!
//! A reusable text input widget for TUI parameter configuration.
//! Supports cursor movement, character editing, placeholder text, max length,
//! and validation callbacks.

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent};

// ============================================================================
// TextInputWidget
// ============================================================================

/// Text input widget for TUI parameter configuration
pub struct TextInputWidget {
    /// Current text content
    text: String,
    /// Cursor position (0-based index into text)
    cursor_pos: usize,
    /// Placeholder text shown when input is empty
    placeholder: Option<String>,
    /// Maximum allowed length (None = unlimited)
    max_length: Option<usize>,
    /// Validation callback function (not cloned, reset on clone)
    validator: Option<Box<dyn Fn(&str) -> ValidationResult + Send + Sync>>,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
}

impl Clone for TextInputWidget {
    fn clone(&self) -> Self {
        Self {
            text: self.text.clone(),
            cursor_pos: self.cursor_pos,
            placeholder: self.placeholder.clone(),
            max_length: self.max_length,
            validator: None, // Validator cannot be cloned
            focused: self.focused,
            read_only: self.read_only,
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
        }
    }
}

impl std::fmt::Debug for TextInputWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextInputWidget")
            .field("text", &self.text)
            .field("cursor_pos", &self.cursor_pos)
            .field("placeholder", &self.placeholder)
            .field("max_length", &self.max_length)
            .field("validator", &self.validator.is_some())
            .field("focused", &self.focused)
            .field("read_only", &self.read_only)
            .field("validation_state", &self.validation_state)
            .field("show_validation", &self.show_validation)
            .finish()
    }
}

/// Validation result for text input
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationResult {
    /// Input is valid
    Valid,
    /// Input is invalid with error message
    Invalid(String),
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

impl Default for TextInputWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl TextInputWidget {
    /// Create a new text input widget
    pub fn new() -> Self {
        Self {
            text: String::new(),
            cursor_pos: 0,
            placeholder: None,
            max_length: None,
            validator: None,
            focused: false,
            read_only: false,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
        }
    }

    /// Set the initial text value
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = text.into();
        self.cursor_pos = self.text.len();
        self
    }

    /// Set placeholder text
    pub fn with_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Set maximum length constraint
    pub fn with_max_length(mut self, max_length: usize) -> Self {
        self.max_length = Some(max_length);
        self
    }

    /// Set validation callback
    pub fn with_validator<F>(mut self, validator: F) -> Self
    where
        F: Fn(&str) -> ValidationResult + Send + Sync + 'static,
    {
        self.validator = Some(Box::new(validator));
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

    /// Get the current text value
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Get the current text value as owned String
    pub fn text_owned(&self) -> String {
        self.text.clone()
    }

    /// Get cursor position
    pub fn cursor_pos(&self) -> usize {
        self.cursor_pos
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

    /// Check if the input is empty
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    /// Clear the input
    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor_pos = 0;
        self.validation_state = ValidationState::Unvalidated;
    }

    /// Set the text value programmatically
    pub fn set_text(&mut self, text: impl Into<String>) {
        if self.read_only {
            return;
        }

        let new_text = text.into();
        if let Some(max_len) = self.max_length {
            if new_text.chars().count() > max_len {
                return; // Don't set if exceeds max length
            }
        }
        self.text = new_text;
        let char_count = self.text.chars().count();
        self.cursor_pos = char_count; // Always move cursor to end after set_text
        self.validate();
    }

    /// Handle a key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.read_only {
            return false;
        }

        match key.code {
            KeyCode::Char(c) => {
                self.insert_char(c);
                true
            }
            KeyCode::Backspace => {
                self.delete_backward();
                true
            }
            KeyCode::Delete => {
                self.delete_forward();
                true
            }
            KeyCode::Left => {
                self.move_cursor_left();
                true
            }
            KeyCode::Right => {
                self.move_cursor_right();
                true
            }
            KeyCode::Home => {
                self.move_cursor_home();
                true
            }
            KeyCode::End => {
                self.move_cursor_end();
                true
            }
            _ => false,
        }
    }

    /// Insert a character at the cursor position
    pub fn insert_char(&mut self, c: char) {
        if self.read_only {
            return;
        }

        // Check max length (by character count, not bytes)
        if let Some(max_len) = self.max_length {
            if self.text.chars().count() >= max_len {
                return;
            }
        }

        // Convert character position to byte position
        let byte_pos = if self.cursor_pos >= self.text.chars().count() {
            self.text.len()
        } else {
            self.text
                .char_indices()
                .nth(self.cursor_pos)
                .map(|(i, _)| i)
                .unwrap_or(self.text.len())
        };

        // Insert character at byte position
        self.text.insert(byte_pos, c);
        self.cursor_pos += 1;
        self.validate();
    }

    /// Delete character before cursor (backspace)
    pub fn delete_backward(&mut self) {
        if self.read_only {
            return;
        }

        if self.cursor_pos > 0 {
            // Convert character position to byte position
            let byte_pos = self
                .text
                .char_indices()
                .nth(self.cursor_pos - 1)
                .map(|(i, _)| i)
                .unwrap_or(0);

            // Remove character at byte position
            let char_len = self.text[byte_pos..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(1);
            
            self.text.drain(byte_pos..byte_pos + char_len);
            self.cursor_pos -= 1;
            self.validate();
        }
    }

    /// Delete character at cursor (delete key)
    pub fn delete_forward(&mut self) {
        if self.read_only {
            return;
        }

        let char_count = self.text.chars().count();
        if self.cursor_pos < char_count {
            // Convert character position to byte position
            let byte_pos = self
                .text
                .char_indices()
                .nth(self.cursor_pos)
                .map(|(i, _)| i)
                .unwrap_or(self.text.len());

            // Remove character at byte position
            let char_len = self.text[byte_pos..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(1);
            
            self.text.drain(byte_pos..byte_pos + char_len);
            self.validate();
        }
    }

    /// Move cursor left
    pub fn move_cursor_left(&mut self) {
        if self.cursor_pos > 0 {
            self.cursor_pos -= 1;
        }
    }

    /// Move cursor right
    pub fn move_cursor_right(&mut self) {
        let char_count = self.text.chars().count();
        if self.cursor_pos < char_count {
            self.cursor_pos += 1;
        }
    }

    /// Move cursor to beginning (home)
    pub fn move_cursor_home(&mut self) {
        self.cursor_pos = 0;
    }

    /// Move cursor to end
    pub fn move_cursor_end(&mut self) {
        self.cursor_pos = self.text.chars().count();
    }

    /// Validate the current text
    pub fn validate(&mut self) {
        if let Some(ref validator) = self.validator {
            match validator(&self.text) {
                ValidationResult::Valid => {
                    self.validation_state = ValidationState::Valid;
                }
                ValidationResult::Invalid(msg) => {
                    self.validation_state = ValidationState::Invalid(msg);
                }
            }
        } else {
            self.validation_state = ValidationState::Unvalidated;
        }
    }

    /// Render the widget to the frame
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Determine display text
        let display_text = if self.text.is_empty() {
            self.placeholder.as_deref().unwrap_or("")
        } else {
            &self.text
        };

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

        // Render cursor if focused
        if self.focused && !self.read_only {
            self.render_cursor(f, area);
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
        let text_style = if self.text.is_empty() && self.placeholder.is_some() {
            // Placeholder text style
            Style::default().fg(Color::DarkGray)
        } else if self.read_only {
            // Read-only style
            Style::default().fg(Color::DarkGray)
        } else {
            // Normal text style
            Style::default().fg(Color::White)
        };

        let border_style = if !self.focused {
            // Unfocused border
            Style::default().fg(Color::DarkGray)
        } else if let ValidationState::Invalid(_) = self.validation_state {
            // Invalid border (red)
            Style::default().fg(Color::Red)
        } else if let ValidationState::Valid = self.validation_state {
            // Valid border (green)
            Style::default().fg(Color::Green)
        } else {
            // Focused but unvalidated (cyan)
            Style::default().fg(Color::Cyan)
        };

        (text_style, border_style)
    }

    /// Render cursor at current position
    fn render_cursor(&self, f: &mut Frame, area: Rect) {
        // Calculate cursor position in display coordinates
        let cursor_x = if self.text.is_empty() {
            1 // Start of input area
        } else {
            // Position within the text, accounting for borders
            let text_start_x = area.x + 1;
            let cursor_offset = self.cursor_pos.min(self.text.len());
            text_start_x + cursor_offset as u16
        };

        let cursor_y = area.y + 1; // Below top border

        // Ensure cursor is within bounds
        if cursor_x < area.x + area.width && cursor_y < area.y + area.height {
            f.set_cursor(cursor_x, cursor_y);
        }
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

impl Widget for TextInputWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
        // For Widget trait implementation, we use a simpler rendering
        let display_text = if self.text.is_empty() {
            self.placeholder.as_deref().unwrap_or("")
        } else {
            &self.text
        };

        let (text_style, border_style) = self.get_styles();

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);

        let paragraph = Paragraph::new(display_text)
            .style(text_style)
            .block(block);

        paragraph.render(area, buf);

        // Render cursor if focused
        if self.focused && !self.read_only {
            let cursor_x = if self.text.is_empty() {
                area.x + 1
            } else {
                let text_start_x = area.x + 1;
                let cursor_offset = self.cursor_pos.min(self.text.len());
                (text_start_x + cursor_offset as u16).min(area.x + area.width - 1)
            };

            let cursor_y = area.y + 1;
            if cursor_x < area.x + area.width && cursor_y < area.y + area.height {
                buf.set_style(
                    ratatui::layout::Rect {
                        x: cursor_x,
                        y: cursor_y,
                        width: 1,
                        height: 1,
                    },
                    Style::default().add_modifier(Modifier::REVERSED),
                );
            }
        }
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
        let widget = TextInputWidget::new();
        assert_eq!(widget.text(), "");
        assert_eq!(widget.cursor_pos(), 0);
        assert!(widget.placeholder.is_none());
        assert!(widget.max_length.is_none());
        assert!(!widget.focused);
        assert!(!widget.read_only);
    }

    #[test]
    fn test_with_text() {
        let widget = TextInputWidget::new().with_text("Hello");
        assert_eq!(widget.text(), "Hello");
        assert_eq!(widget.cursor_pos(), 5); // At end
    }

    #[test]
    fn test_with_placeholder() {
        let widget = TextInputWidget::new().with_placeholder("Enter text...");
        assert_eq!(widget.placeholder, Some("Enter text...".to_string()));
    }

    #[test]
    fn test_with_max_length() {
        let widget = TextInputWidget::new().with_max_length(10);
        assert_eq!(widget.max_length, Some(10));
    }

    #[test]
    fn test_with_validator() {
        let widget = TextInputWidget::new().with_validator(|s| {
            if s.len() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        });
        assert!(widget.validator.is_some());
    }

    #[test]
    fn test_chained_builders() {
        let widget = TextInputWidget::new()
            .with_text("test")
            .with_placeholder("Enter...")
            .with_max_length(20)
            .set_focused(true)
            .set_read_only(false);

        assert_eq!(widget.text(), "test");
        assert_eq!(widget.placeholder, Some("Enter...".to_string()));
        assert_eq!(widget.max_length, Some(20));
        assert!(widget.focused);
        assert!(!widget.read_only);
    }

    // ========================================================================
    // Text Manipulation Tests
    // ========================================================================

    #[test]
    fn test_insert_char_at_end() {
        let mut widget = TextInputWidget::new();
        widget.insert_char('H');
        widget.insert_char('i');
        assert_eq!(widget.text(), "Hi");
        assert_eq!(widget.cursor_pos(), 2);
    }

    #[test]
    fn test_insert_char_in_middle() {
        let mut widget = TextInputWidget::new().with_text("Hi");
        widget.move_cursor_left();
        widget.insert_char('e');
        assert_eq!(widget.text(), "Hei");
        assert_eq!(widget.cursor_pos(), 2);
    }

    #[test]
    fn test_insert_char_respects_max_length() {
        let mut widget = TextInputWidget::new().with_max_length(3);
        widget.insert_char('A');
        widget.insert_char('B');
        widget.insert_char('C');
        widget.insert_char('D'); // Should be ignored
        assert_eq!(widget.text(), "ABC");
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_insert_char_unicode() {
        let mut widget = TextInputWidget::new();
        widget.insert_char('🚀');
        widget.insert_char('中');
        widget.insert_char('文');
        assert_eq!(widget.text(), "🚀中文");
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_delete_backward_at_end() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.delete_backward();
        assert_eq!(widget.text(), "Hell");
        assert_eq!(widget.cursor_pos(), 4);
    }

    #[test]
    fn test_delete_backward_in_middle() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        // Start at end (pos 5), move left twice to pos 3 (before second 'l')
        widget.move_cursor_left(); // pos 4
        widget.move_cursor_left(); // pos 3
        widget.delete_backward(); // Delete 'l' at pos 2, cursor moves to pos 2
        assert_eq!(widget.text(), "Helo");
        assert_eq!(widget.cursor_pos(), 2);
    }

    #[test]
    fn test_delete_backward_at_start() {
        let mut widget = TextInputWidget::new().with_text("H");
        widget.delete_backward();
        assert_eq!(widget.text(), "");
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_delete_backward_empty() {
        let mut widget = TextInputWidget::new();
        widget.delete_backward(); // Should not panic
        assert_eq!(widget.text(), "");
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_delete_forward_at_start() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_home();
        widget.delete_forward();
        assert_eq!(widget.text(), "ello");
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_delete_forward_in_middle() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_left(); // Move to position 4 (before 'o')
        widget.delete_forward(); // Delete 'o' at pos 4
        assert_eq!(widget.text(), "Hell");
        assert_eq!(widget.cursor_pos(), 4);
    }

    #[test]
    fn test_delete_forward_at_end() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.delete_forward(); // Should do nothing
        assert_eq!(widget.text(), "Hello");
        assert_eq!(widget.cursor_pos(), 5);
    }

    #[test]
    fn test_delete_forward_empty() {
        let mut widget = TextInputWidget::new();
        widget.delete_forward(); // Should not panic
        assert_eq!(widget.text(), "");
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_clear() {
        let mut widget = TextInputWidget::new().with_text("Hello World");
        widget.clear();
        assert_eq!(widget.text(), "");
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_set_text() {
        let mut widget = TextInputWidget::new();
        widget.set_text("New Text");
        assert_eq!(widget.text(), "New Text");
        assert_eq!(widget.cursor_pos(), 8);
    }

    #[test]
    fn test_set_text_respects_max_length() {
        let mut widget = TextInputWidget::new().with_max_length(5);
        widget.set_text("Too Long Text");
        assert_eq!(widget.text(), ""); // Should not be set
    }

    #[test]
    fn test_set_text_updates_cursor() {
        let mut widget = TextInputWidget::new().with_text("Old");
        widget.move_cursor_left();
        widget.set_text("New");
        assert_eq!(widget.cursor_pos(), 3); // Should be at end of new text
    }

    // ========================================================================
    // Cursor Movement Tests
    // ========================================================================

    #[test]
    fn test_move_cursor_left() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_left();
        assert_eq!(widget.cursor_pos(), 4);
        widget.move_cursor_left();
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_move_cursor_left_at_start() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_home();
        widget.move_cursor_left(); // Should not go below 0
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_move_cursor_right() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_home();
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 1);
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 2);
    }

    #[test]
    fn test_move_cursor_right_at_end() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_right(); // Should not exceed text length
        assert_eq!(widget.cursor_pos(), 5);
    }

    #[test]
    fn test_move_cursor_home() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_home();
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_move_cursor_end() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_home();
        widget.move_cursor_end();
        assert_eq!(widget.cursor_pos(), 5);
    }

    #[test]
    fn test_cursor_movement_empty_text() {
        let mut widget = TextInputWidget::new();
        widget.move_cursor_left();
        widget.move_cursor_right();
        widget.move_cursor_home();
        widget.move_cursor_end();
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_cursor_movement_unicode() {
        let mut widget = TextInputWidget::new().with_text("🚀中文");
        widget.move_cursor_home();
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 1);
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 2);
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_cursor_movement_after_insert() {
        let mut widget = TextInputWidget::new();
        widget.insert_char('A');
        widget.insert_char('B');
        widget.move_cursor_left();
        widget.insert_char('C');
        assert_eq!(widget.text(), "ACB");
        assert_eq!(widget.cursor_pos(), 2);
    }

    #[test]
    fn test_cursor_movement_after_delete() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_left();
        widget.move_cursor_left();
        widget.delete_backward();
        // Cursor at pos 3 (before second 'l'), delete_backward removes first 'l' at pos 2
        assert_eq!(widget.text(), "Helo");
        assert_eq!(widget.cursor_pos(), 2);
    }

    // ========================================================================
    // Max Length Tests
    // ========================================================================

    #[test]
    fn test_max_length_enforced_on_insert() {
        let mut widget = TextInputWidget::new().with_max_length(5);
        for _ in 0..10 {
            widget.insert_char('A');
        }
        assert_eq!(widget.text().len(), 5);
        assert_eq!(widget.text(), "AAAAA");
    }

    #[test]
    fn test_max_length_allows_exact_length() {
        let mut widget = TextInputWidget::new().with_max_length(5);
        for _ in 0..5 {
            widget.insert_char('A');
        }
        assert_eq!(widget.text().len(), 5);
    }

    #[test]
    fn test_max_length_none_allows_unlimited() {
        let mut widget = TextInputWidget::new();
        for _ in 0..100 {
            widget.insert_char('A');
        }
        assert_eq!(widget.text().len(), 100);
    }

    #[test]
    fn test_max_length_zero() {
        let mut widget = TextInputWidget::new().with_max_length(0);
        widget.insert_char('A');
        assert_eq!(widget.text(), "");
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validation_valid() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.len() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        });

        widget.set_text("abc");
        widget.validate();
        assert!(widget.is_valid());
        assert!(widget.validation_error().is_none());
    }

    #[test]
    fn test_validation_invalid() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.len() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        });

        widget.set_text("ab");
        widget.validate();
        assert!(!widget.is_valid());
        assert_eq!(widget.validation_error(), Some("Too short"));
    }

    #[test]
    fn test_validation_auto_on_insert() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.len() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        });

        widget.insert_char('a');
        assert!(!widget.is_valid());
        widget.insert_char('b');
        assert!(!widget.is_valid());
        widget.insert_char('c');
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validation_auto_on_delete() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.chars().count() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        });

        widget.set_text("abc");
        widget.validate();
        assert!(widget.is_valid());

        widget.delete_backward();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validation_no_validator() {
        let mut widget = TextInputWidget::new();
        widget.set_text("anything");
        widget.validate();
        // Should be unvalidated, not invalid
        assert!(widget.validation_error().is_none());
    }

    #[test]
    fn test_validation_multiple_rules() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.is_empty() {
                ValidationResult::Invalid("Cannot be empty".to_string())
            } else if s.len() < 3 {
                ValidationResult::Invalid("Too short".to_string())
            } else if s.len() > 10 {
                ValidationResult::Invalid("Too long".to_string())
            } else {
                ValidationResult::Valid
            }
        });

        widget.set_text("");
        widget.validate();
        assert!(!widget.is_valid());

        widget.set_text("ab");
        widget.validate();
        assert!(!widget.is_valid());

        widget.set_text("abc");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("abcdefghijk");
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validation_email_format() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.contains('@') && s.contains('.') {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Invalid email format".to_string())
            }
        });

        widget.set_text("test@example.com");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("invalid");
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validation_numeric_only() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.chars().all(|c| c.is_ascii_digit()) {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Must be numeric".to_string())
            }
        });

        widget.set_text("12345");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("12a45");
        widget.validate();
        assert!(!widget.is_valid());
    }

    // ========================================================================
    // Placeholder Tests
    // ========================================================================

    #[test]
    fn test_placeholder_shown_when_empty() {
        let widget = TextInputWidget::new().with_placeholder("Enter text...");
        assert!(widget.is_empty());
        assert_eq!(widget.placeholder, Some("Enter text...".to_string()));
    }

    #[test]
    fn test_placeholder_not_shown_when_text_exists() {
        let widget = TextInputWidget::new()
            .with_placeholder("Enter text...")
            .with_text("Hello");
        assert!(!widget.is_empty());
    }

    #[test]
    fn test_placeholder_none() {
        let widget = TextInputWidget::new();
        assert!(widget.placeholder.is_none());
    }

    // ========================================================================
    // Focus Tests
    // ========================================================================

    #[test]
    fn test_set_focused() {
        let widget = TextInputWidget::new().set_focused(true);
        assert!(widget.focused);
    }

    #[test]
    fn test_unfocused() {
        let widget = TextInputWidget::new().set_focused(false);
        assert!(!widget.focused);
    }

    // ========================================================================
    // Read-Only Tests
    // ========================================================================

    #[test]
    fn test_read_only_prevents_insert() {
        let mut widget = TextInputWidget::new().set_read_only(true);
        let initial_text = widget.text_owned();
        widget.insert_char('X');
        assert_eq!(widget.text(), initial_text);
    }

    #[test]
    fn test_read_only_prevents_delete() {
        let mut widget = TextInputWidget::new()
            .with_text("Hello")
            .set_read_only(true);
        let initial_text = widget.text_owned();
        widget.delete_backward();
        assert_eq!(widget.text(), initial_text);
    }

    #[test]
    fn test_read_only_allows_cursor_movement() {
        let mut widget = TextInputWidget::new()
            .with_text("Hello")
            .set_read_only(true);
        widget.move_cursor_left();
        assert_eq!(widget.cursor_pos(), 4);
    }

    #[test]
    fn test_read_only_prevents_set_text() {
        let mut widget = TextInputWidget::new()
            .with_text("Hello")
            .set_read_only(true);
        widget.set_text("New");
        // Should still be "Hello" (read-only prevents changes)
        assert_eq!(widget.text(), "Hello");
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_char() {
        let mut widget = TextInputWidget::new();
        let key = KeyEvent::new(KeyCode::Char('H'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.text(), "H");
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut widget = TextInputWidget::new().with_text("Hi");
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.text(), "H");
    }

    #[test]
    fn test_handle_key_delete() {
        let mut widget = TextInputWidget::new().with_text("Hi");
        widget.move_cursor_home();
        let key = KeyEvent::new(KeyCode::Delete, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.text(), "i");
    }

    #[test]
    fn test_handle_key_left() {
        let mut widget = TextInputWidget::new().with_text("Hi");
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 1);
    }

    #[test]
    fn test_handle_key_right() {
        let mut widget = TextInputWidget::new().with_text("Hi");
        widget.move_cursor_home();
        let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 1);
    }

    #[test]
    fn test_handle_key_home() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        let key = KeyEvent::new(KeyCode::Home, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_handle_key_end() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        widget.move_cursor_home();
        let key = KeyEvent::new(KeyCode::End, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 5);
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut widget = TextInputWidget::new();
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::empty());
        assert!(!widget.handle_key(key));
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = TextInputWidget::new()
            .with_text("Hello")
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Char('X'), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert_eq!(widget.text(), "Hello");
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_very_long_text() {
        let mut widget = TextInputWidget::new();
        let long_text = "A".repeat(10000);
        widget.set_text(&long_text);
        assert_eq!(widget.text().len(), 10000);
    }

    #[test]
    fn test_unicode_handling() {
        let mut widget = TextInputWidget::new();
        widget.set_text("🚀中文🌍");
        assert_eq!(widget.text(), "🚀中文🌍");
        widget.move_cursor_home();
        widget.delete_forward();
        assert_eq!(widget.text(), "中文🌍");
    }

    #[test]
    fn test_special_characters() {
        let mut widget = TextInputWidget::new();
        widget.set_text("!@#$%^&*()_+-=[]{}|;':\",./<>?");
        assert_eq!(widget.text(), "!@#$%^&*()_+-=[]{}|;':\",./<>?");
    }

    #[test]
    fn test_newlines_handling() {
        let mut widget = TextInputWidget::new();
        widget.set_text("Line1\nLine2");
        // Text input typically doesn't handle newlines, but should not panic
        assert!(widget.text().contains('\n'));
    }

    #[test]
    fn test_tabs_handling() {
        let mut widget = TextInputWidget::new();
        widget.set_text("Tab\tHere");
        assert!(widget.text().contains('\t'));
    }

    #[test]
    fn test_empty_string_operations() {
        let mut widget = TextInputWidget::new();
        widget.insert_char('A');
        widget.clear();
        widget.insert_char('B');
        assert_eq!(widget.text(), "B");
    }

    #[test]
    fn test_rapid_insert_delete() {
        let mut widget = TextInputWidget::new();
        for i in 0..100 {
            widget.insert_char(char::from(b'A' + (i % 26) as u8));
            if i % 2 == 0 {
                widget.delete_backward();
            }
        }
        // Should not panic and should have some text
        assert!(widget.text().len() <= 100);
    }

    #[test]
    fn test_cursor_boundary_conditions() {
        let mut widget = TextInputWidget::new().with_text("Hi");
        // Move cursor beyond bounds
        for _ in 0..10 {
            widget.move_cursor_right();
        }
        assert_eq!(widget.cursor_pos(), 2); // Should cap at text length

        widget.move_cursor_home();
        for _ in 0..10 {
            widget.move_cursor_left();
        }
        assert_eq!(widget.cursor_pos(), 0); // Should not go below 0
    }

    #[test]
    fn test_max_length_with_unicode() {
        let mut widget = TextInputWidget::new().with_max_length(3);
        widget.insert_char('🚀');
        widget.insert_char('中');
        widget.insert_char('文');
        widget.insert_char('X'); // Should be ignored
        assert_eq!(widget.text().chars().count(), 3);
    }

    #[test]
    fn test_validation_state_transitions() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s == "valid" {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Invalid".to_string())
            }
        });

        widget.set_text("invalid");
        widget.validate();
        assert!(!widget.is_valid());

        widget.set_text("valid");
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_multiple_validators_chained() {
        // Test that setting a new validator replaces the old one
        let mut widget = TextInputWidget::new()
            .with_validator(|s| {
                if s.len() > 5 {
                    ValidationResult::Valid
                } else {
                    ValidationResult::Invalid("Too short".to_string())
                }
            });

        widget.set_text("short");
        widget.validate();
        assert!(!widget.is_valid());

        // Replace validator
        widget = widget.with_validator(|s| {
            if s.len() < 5 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too long".to_string())
            }
        });

        widget.set_text("short");
        widget.validate();
        assert!(!widget.is_valid()); // Now "short" is too long for new validator
    }

    #[test]
    fn test_text_owned_clone() {
        let widget = TextInputWidget::new().with_text("Hello");
        let text1 = widget.text_owned();
        let text2 = widget.text_owned();
        assert_eq!(text1, text2);
        assert_eq!(text1, "Hello");
    }

    #[test]
    fn test_is_empty() {
        let widget1 = TextInputWidget::new();
        assert!(widget1.is_empty());

        let widget2 = TextInputWidget::new().with_text("Hello");
        assert!(!widget2.is_empty());

        let mut widget3 = TextInputWidget::new().with_text("Hello");
        widget3.clear();
        assert!(widget3.is_empty());
    }

    #[test]
    fn test_show_validation_toggle() {
        let mut widget = TextInputWidget::new()
            .with_validator(|s| {
                if s.is_empty() {
                    ValidationResult::Invalid("Empty".to_string())
                } else {
                    ValidationResult::Valid
                }
            })
            .set_show_validation(true);

        widget.set_text("");
        widget.validate();
        assert!(!widget.is_valid());

        widget = widget.set_show_validation(false);
        // Should still validate, just not show error
        assert!(!widget.is_valid());
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_editing_workflow() {
        let mut widget = TextInputWidget::new()
            .with_placeholder("Enter name...")
            .with_max_length(20)
            .with_validator(|s| {
                if s.is_empty() {
                    ValidationResult::Invalid("Name required".to_string())
                } else if s.len() < 3 {
                    ValidationResult::Invalid("Name too short".to_string())
                } else {
                    ValidationResult::Valid
                }
            })
            .set_focused(true);

        // Start empty
        assert!(widget.is_empty());
        assert!(!widget.is_valid());

        // Type name
        widget.insert_char('J');
        widget.insert_char('o');
        widget.insert_char('h');
        widget.insert_char('n');
        widget.validate();
        assert_eq!(widget.text(), "John");
        assert!(widget.is_valid());

        // Edit in middle
        widget.move_cursor_left();
        widget.move_cursor_left();
        widget.insert_char('a');
        widget.validate();
        assert_eq!(widget.text(), "Joahn");
        assert!(widget.is_valid());

        // Delete character
        widget.delete_backward();
        widget.validate();
        assert_eq!(widget.text(), "John");
        assert!(widget.is_valid());
    }

    #[test]
    fn test_password_like_input() {
        let mut widget = TextInputWidget::new()
            .with_placeholder("Enter password...")
            .with_max_length(50)
            .with_validator(|s| {
                if s.len() < 8 {
                    ValidationResult::Invalid("Password must be at least 8 characters".to_string())
                } else {
                    ValidationResult::Valid
                }
            });

        widget.set_text("short");
        widget.validate();
        assert!(!widget.is_valid());

        widget.set_text("longpassword");
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_numeric_input_simulation() {
        let mut widget = TextInputWidget::new()
            .with_placeholder("Enter number...")
            .with_validator(|s| {
                if s.is_empty() {
                    ValidationResult::Invalid("Number required".to_string())
                } else if s.parse::<f64>().is_ok() {
                    ValidationResult::Valid
                } else {
                    ValidationResult::Invalid("Invalid number".to_string())
                }
            });

        widget.set_text("123.45");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("abc");
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_email_input_simulation() {
        let mut widget = TextInputWidget::new()
            .with_placeholder("Enter email...")
            .with_validator(|s| {
                if s.contains('@') && s.contains('.') && !s.starts_with('@') {
                    ValidationResult::Valid
                } else {
                    ValidationResult::Invalid("Invalid email".to_string())
                }
            });

        widget.set_text("user@example.com");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("invalid-email");
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_symbol_input_simulation() {
        let mut widget = TextInputWidget::new()
            .with_placeholder("Enter symbol...")
            .with_max_length(20)
            .with_validator(|s| {
                if s.is_empty() {
                    ValidationResult::Invalid("Symbol required".to_string())
                } else if s.len() > 20 {
                    ValidationResult::Invalid("Symbol too long".to_string())
                } else {
                    ValidationResult::Valid
                }
            });

        widget.set_text("BTCUSDT");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("");
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_path_input_simulation() {
        let mut widget = TextInputWidget::new()
            .with_placeholder("Enter path...")
            .with_validator(|s| {
                if s.starts_with('/') || s.starts_with("./") {
                    ValidationResult::Valid
                } else {
                    ValidationResult::Invalid("Invalid path".to_string())
                }
            });

        widget.set_text("./data/features");
        widget.validate();
        assert!(widget.is_valid());

        widget.set_text("relative/path");
        widget.validate();
        assert!(!widget.is_valid());
    }

    // ========================================================================
    // Stress Tests
    // ========================================================================

    #[test]
    fn test_stress_rapid_typing() {
        let mut widget = TextInputWidget::new();
        for i in 0..1000 {
            widget.insert_char(char::from(b'A' + (i % 26) as u8));
        }
        assert_eq!(widget.text().len(), 1000);
    }

    #[test]
    fn test_stress_rapid_cursor_movement() {
        let mut widget = TextInputWidget::new().with_text("A".repeat(100));
        for _ in 0..1000 {
            widget.move_cursor_right();
            widget.move_cursor_left();
        }
        // Cursor should still be valid
        assert!(widget.cursor_pos() <= widget.text().len());
    }

    #[test]
    fn test_stress_rapid_insert_delete() {
        let mut widget = TextInputWidget::new();
        for _ in 0..500 {
            widget.insert_char('A');
            widget.delete_backward();
        }
        assert_eq!(widget.text(), "");
    }

    #[test]
    fn test_stress_unicode_mixed() {
        let mut widget = TextInputWidget::new();
        let chars = vec!['A', '🚀', '中', 'B', '文', 'C', '🌍'];
        for c in chars.iter().cycle().take(100) {
            widget.insert_char(*c);
        }
        assert_eq!(widget.text().chars().count(), 100);
    }

    #[test]
    fn test_stress_validation_frequent() {
        let mut widget = TextInputWidget::new().with_validator(|s| {
            if s.len() % 2 == 0 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Odd length".to_string())
            }
        });

        for i in 0..100 {
            widget.insert_char('A');
            widget.validate();
            if i % 2 == 1 {
                assert!(widget.is_valid());
            } else {
                assert!(!widget.is_valid());
            }
        }
    }

    // ========================================================================
    // Boundary Condition Tests
    // ========================================================================

    #[test]
    fn test_cursor_at_exact_max_length() {
        let mut widget = TextInputWidget::new().with_max_length(5);
        for _ in 0..5 {
            widget.insert_char('A');
        }
        assert_eq!(widget.cursor_pos(), 5);
        assert_eq!(widget.text().len(), 5);
    }

    #[test]
    fn test_cursor_after_set_text_exceeds_max() {
        let mut widget = TextInputWidget::new().with_max_length(5);
        widget.set_text("TooLongText");
        // Should not be set
        assert_eq!(widget.text(), "");
    }

    #[test]
    fn test_empty_placeholder() {
        let widget = TextInputWidget::new().with_placeholder("");
        assert_eq!(widget.placeholder, Some("".to_string()));
    }

    #[test]
    fn test_very_long_placeholder() {
        let long_placeholder = "A".repeat(1000);
        let widget = TextInputWidget::new().with_placeholder(&long_placeholder);
        assert_eq!(widget.placeholder, Some(long_placeholder));
    }

    #[test]
    fn test_validator_returns_empty_error() {
        let mut widget = TextInputWidget::new().with_validator(|_| {
            ValidationResult::Invalid(String::new())
        });
        widget.set_text("test");
        widget.validate();
        assert!(!widget.is_valid());
        assert_eq!(widget.validation_error(), Some(""));
    }

    #[test]
    fn test_validator_returns_very_long_error() {
        let long_error = "A".repeat(1000);
        let mut widget = TextInputWidget::new().with_validator(move |_| {
            ValidationResult::Invalid(long_error.clone())
        });
        widget.set_text("test");
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().unwrap().len() == 1000);
    }

    // ========================================================================
    // State Consistency Tests
    // ========================================================================

    #[test]
    fn test_state_consistency_after_operations() {
        let mut widget = TextInputWidget::new()
            .with_text("Hello")
            .with_max_length(10);

        // Perform various operations
        widget.move_cursor_left();
        widget.insert_char('X');
        widget.delete_backward();
        widget.move_cursor_right();
        widget.delete_forward();

        // State should be consistent
        assert!(widget.cursor_pos() <= widget.text().len());
        assert!(widget.text().len() <= 10);
    }

    #[test]
    fn test_cursor_never_exceeds_text_length() {
        let mut widget = TextInputWidget::new().with_text("Hello");
        
        // Try to move cursor beyond text
        for _ in 0..100 {
            widget.move_cursor_right();
        }
        assert_eq!(widget.cursor_pos(), 5);

        // Try to move cursor below 0
        for _ in 0..100 {
            widget.move_cursor_left();
        }
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_text_length_never_exceeds_max() {
        let mut widget = TextInputWidget::new().with_max_length(5);
        
        // Try to insert many characters
        for _ in 0..100 {
            widget.insert_char('A');
        }
        assert_eq!(widget.text().len(), 5);
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1 = TextInputWidget::default();
        let widget2 = TextInputWidget::new();
        assert_eq!(widget1.text(), widget2.text());
        assert_eq!(widget1.cursor_pos(), widget2.cursor_pos());
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = TextInputWidget::new()
            .with_text("Hello")
            .with_placeholder("Enter...")
            .with_max_length(10)
            .set_focused(true);

        let widget2 = widget1.clone();
        assert_eq!(widget1.text(), widget2.text());
        assert_eq!(widget1.cursor_pos(), widget2.cursor_pos());
        assert_eq!(widget1.placeholder, widget2.placeholder);
        assert_eq!(widget1.max_length, widget2.max_length);
        assert_eq!(widget1.focused, widget2.focused);
        // Note: validator is not cloned (function pointers can't be cloned)
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = TextInputWidget::new().with_text("Hello");
        let mut widget2 = widget1.clone();

        widget1.insert_char('1');
        widget2.insert_char('2');

        assert_eq!(widget1.text(), "Hello1");
        assert_eq!(widget2.text(), "Hello2");
    }
}

