//! Comma-Separated List Widget
//!
//! A reusable widget for editing comma-separated value lists (e.g., "1,2,3,4,5").
//! Supports adding, removing, editing items, validation (no duplicates, sorted),
//! visual list display, and quick presets.

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

// ============================================================================
// CommaListWidget
// ============================================================================

/// Comma-separated list widget for TUI parameter configuration
pub struct CommaListWidget {
    /// Current list of values
    values: Vec<f64>,
    /// Currently selected index (None = no selection)
    selected: Option<usize>,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Whether duplicates are allowed
    allow_duplicates: bool,
    /// Whether list must be sorted
    require_sorted: bool,
    /// Minimum allowed value (None = no minimum)
    min: Option<f64>,
    /// Maximum allowed value (None = no maximum)
    max: Option<f64>,
    /// Number of decimal places to display
    decimals: usize,
    /// Placeholder text shown when list is empty
    placeholder: Option<String>,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
    /// Internal text buffer for editing items
    text_buffer: String,
    /// Whether we're in text editing mode
    editing: bool,
    /// Quick presets available
    presets: Vec<Preset>,
    /// List state for rendering
    list_state: ListState,
}

impl Clone for CommaListWidget {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            selected: self.selected,
            focused: self.focused,
            read_only: self.read_only,
            allow_duplicates: self.allow_duplicates,
            require_sorted: self.require_sorted,
            min: self.min,
            max: self.max,
            decimals: self.decimals,
            placeholder: self.placeholder.clone(),
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
            text_buffer: self.text_buffer.clone(),
            editing: self.editing,
            presets: self.presets.clone(),
            list_state: ListState::default(),
        }
    }
}

impl std::fmt::Debug for CommaListWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommaListWidget")
            .field("values", &self.values)
            .field("selected", &self.selected)
            .field("focused", &self.focused)
            .field("read_only", &self.read_only)
            .field("allow_duplicates", &self.allow_duplicates)
            .field("require_sorted", &self.require_sorted)
            .field("min", &self.min)
            .field("max", &self.max)
            .field("decimals", &self.decimals)
            .field("editing", &self.editing)
            .finish()
    }
}

/// Quick preset for common lists
#[derive(Debug, Clone, PartialEq)]
pub struct Preset {
    /// Preset name
    pub name: String,
    /// Preset values
    pub values: Vec<f64>,
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

impl Default for CommaListWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl CommaListWidget {
    /// Create a new comma list widget
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            selected: None,
            focused: false,
            read_only: false,
            allow_duplicates: true,
            require_sorted: false,
            min: None,
            max: None,
            decimals: 2,
            placeholder: None,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
            text_buffer: String::new(),
            editing: false,
            presets: Vec::new(),
            list_state: ListState::default(),
        }
    }

    /// Set initial values from comma-separated string
    pub fn with_values_str(mut self, values_str: &str) -> Self {
        self.set_values_from_str(values_str);
        self
    }

    /// Set initial values from vector
    pub fn with_values(mut self, values: Vec<f64>) -> Self {
        self.values = values;
        self.validate();
        self
    }

    /// Set whether duplicates are allowed
    pub fn set_allow_duplicates(mut self, allow: bool) -> Self {
        self.allow_duplicates = allow;
        self.validate();
        self
    }

    /// Set whether list must be sorted
    pub fn set_require_sorted(mut self, require: bool) -> Self {
        self.require_sorted = require;
        if require && !self.values.is_empty() {
            self.sort_values();
        }
        self.validate();
        self
    }

    /// Set minimum allowed value
    pub fn with_min(mut self, min: f64) -> Self {
        self.min = Some(min);
        self.validate();
        self
    }

    /// Set maximum allowed value
    pub fn with_max(mut self, max: f64) -> Self {
        self.max = Some(max);
        self.validate();
        self
    }

    /// Set number of decimal places
    pub fn with_decimals(mut self, decimals: usize) -> Self {
        self.decimals = decimals;
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

    /// Set whether to show validation errors
    pub fn set_show_validation(mut self, show: bool) -> Self {
        self.show_validation = show;
        self
    }

    /// Add a quick preset
    pub fn add_preset(mut self, name: impl Into<String>, values: Vec<f64>) -> Self {
        self.presets.push(Preset {
            name: name.into(),
            values,
        });
        self
    }

    /// Get current values
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    /// Get current values as comma-separated string
    pub fn values_str(&self) -> String {
        self.values
            .iter()
            .map(|v| format!("{:.*}", self.decimals, v))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Get number of items
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if list is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
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

    /// Get selected index
    pub fn selected(&self) -> Option<usize> {
        self.selected
    }

    /// Set selected index (for testing)
    pub fn set_selected(&mut self, index: Option<usize>) {
        self.selected = index;
        if let Some(idx) = index {
            self.list_state.select(Some(idx));
        } else {
            self.list_state.select(None);
        }
    }

    /// Check if widget is in editing mode
    pub fn is_editing(&self) -> bool {
        self.editing
    }

    /// Get the current text buffer content
    pub fn text_buffer(&self) -> &str {
        &self.text_buffer
    }

    /// Parse comma-separated string to Vec<f64>
    pub fn parse_str(&self, s: &str) -> Result<Vec<f64>, String> {
        if s.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut values = Vec::new();
        for part in s.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            match trimmed.parse::<f64>() {
                Ok(v) => values.push(v),
                Err(e) => return Err(format!("Invalid number '{}': {}", trimmed, e)),
            }
        }
        Ok(values)
    }

    /// Set values from comma-separated string
    pub fn set_values_from_str(&mut self, s: &str) {
        if let Ok(values) = self.parse_str(s) {
            self.values = values;
            if self.require_sorted {
                self.sort_values();
            }
            self.validate();
        }
    }

    /// Add a value to the list
    pub fn add_value(&mut self, value: f64) {
        if self.read_only {
            return;
        }

        // Check min/max
        let mut clamped_value = value;
        if let Some(min) = self.min {
            clamped_value = clamped_value.max(min);
        }
        if let Some(max) = self.max {
            clamped_value = clamped_value.min(max);
        }

        // Check duplicates
        if !self.allow_duplicates && self.values.contains(&clamped_value) {
            return;
        }

        if self.require_sorted {
            // Insert in sorted position
            let pos = self.values.binary_search_by(|a| {
                a.partial_cmp(&clamped_value).unwrap_or(std::cmp::Ordering::Equal)
            }).unwrap_or_else(|e| e);
            self.values.insert(pos, clamped_value);
        } else {
            self.values.push(clamped_value);
        }

        self.validate();
    }

    /// Remove value at index
    pub fn remove_at(&mut self, index: usize) {
        if self.read_only {
            return;
        }

        if index < self.values.len() {
            self.values.remove(index);
            if self.selected == Some(index) {
                self.selected = None;
            } else if let Some(sel) = self.selected {
                if sel > index {
                    self.selected = Some(sel - 1);
                }
            }
            self.validate();
        }
    }

    /// Update value at index
    pub fn update_at(&mut self, index: usize, value: f64) {
        if self.read_only {
            return;
        }

        if index < self.values.len() {
            // Check min/max
            let mut clamped_value = value;
            if let Some(min) = self.min {
                clamped_value = clamped_value.max(min);
            }
            if let Some(max) = self.max {
                clamped_value = clamped_value.min(max);
            }

            // Check duplicates
            if !self.allow_duplicates {
                for (i, &v) in self.values.iter().enumerate() {
                    if i != index && (v - clamped_value).abs() < f64::EPSILON {
                        return; // Duplicate found
                    }
                }
            }

            self.values[index] = clamped_value;

            if self.require_sorted {
                self.sort_values();
            }

            self.validate();
        }
    }

    /// Clear all values
    pub fn clear(&mut self) {
        if self.read_only {
            return;
        }

        self.values.clear();
        self.selected = None;
        self.editing = false;
        self.text_buffer.clear();
        self.validate();
    }

    /// Sort values
    fn sort_values(&mut self) {
        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    /// Apply a preset
    pub fn apply_preset(&mut self, index: usize) {
        if self.read_only {
            return;
        }

        if index < self.presets.len() {
            self.values = self.presets[index].values.clone();
            if self.require_sorted {
                self.sort_values();
            }
            self.validate();
        }
    }

    /// Validate the current list
    pub fn validate(&mut self) {
        let mut errors = Vec::new();

        // Check min/max
        for (i, &value) in self.values.iter().enumerate() {
            if let Some(min) = self.min {
                if value < min - f64::EPSILON {
                    errors.push(format!("Item {} ({}) below minimum {}", i, value, min));
                }
            }
            if let Some(max) = self.max {
                if value > max + f64::EPSILON {
                    errors.push(format!("Item {} ({}) above maximum {}", i, value, max));
                }
            }
        }

        // Check duplicates
        if !self.allow_duplicates {
            for i in 0..self.values.len() {
                for j in (i + 1)..self.values.len() {
                    if (self.values[i] - self.values[j]).abs() < f64::EPSILON {
                        errors.push(format!("Duplicate values found: {}", self.values[i]));
                        break;
                    }
                }
            }
        }

        // Check sorted
        if self.require_sorted {
            for i in 1..self.values.len() {
                if self.values[i - 1] > self.values[i] + f64::EPSILON {
                    errors.push("List is not sorted".to_string());
                    break;
                }
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
        if self.read_only && !matches!(key.code, KeyCode::Up | KeyCode::Down | KeyCode::PageUp | KeyCode::PageDown) {
            return false;
        }

        match key.code {
            KeyCode::Up => {
                if let Some(sel) = self.selected {
                    if sel > 0 {
                        self.selected = Some(sel - 1);
                    } else {
                        self.selected = Some(self.values.len().saturating_sub(1));
                    }
                } else if !self.values.is_empty() {
                    self.selected = Some(0);
                }
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            KeyCode::Down => {
                if let Some(sel) = self.selected {
                    if sel < self.values.len().saturating_sub(1) {
                        self.selected = Some(sel + 1);
                    } else {
                        self.selected = Some(0);
                    }
                } else if !self.values.is_empty() {
                    self.selected = Some(0);
                }
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            KeyCode::Enter => {
                if self.editing {
                    // Apply text buffer
                    if !self.text_buffer.is_empty() {
                        if let Ok(value) = self.text_buffer.parse::<f64>() {
                            if let Some(sel) = self.selected {
                                self.update_at(sel, value);
                            } else {
                                self.add_value(value);
                            }
                        }
                    }
                    self.editing = false;
                    self.text_buffer.clear();
                } else if let Some(sel) = self.selected {
                    // Start editing selected item
                    self.editing = true;
                    self.text_buffer = format!("{:.*}", self.decimals, self.values[sel]);
                }
                true
            }
            KeyCode::Esc => {
                self.editing = false;
                self.text_buffer.clear();
                true
            }
            KeyCode::Char('a') | KeyCode::Char('A') if !self.editing => {
                // Add new item
                self.editing = true;
                self.text_buffer.clear();
                self.selected = Some(self.values.len());
                true
            }
            KeyCode::Delete | KeyCode::Char('d') | KeyCode::Char('D') if !self.editing => {
                // Delete selected item
                if let Some(sel) = self.selected {
                    self.remove_at(sel);
                }
                true
            }
            KeyCode::Char(c) if self.editing => {
                // Text editing
                if c.is_ascii_digit() || c == '.' || (c == '-' && self.text_buffer.is_empty()) {
                    if c == '.' && self.text_buffer.contains('.') {
                        return true; // Already has decimal point
                    }
                    self.text_buffer.push(c);
                    true
                } else {
                    false
                }
            }
            KeyCode::Backspace if self.editing => {
                self.text_buffer.pop();
                true
            }
            _ => false,
        }
    }

    /// Format value for display
    fn format_value(&self, value: f64) -> String {
        format!("{:.*}", self.decimals, value)
    }

    /// Render the widget to the frame
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Determine style based on state
        let (text_style, border_style) = self.get_styles();

        // Create block with borders
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);

        // Prepare list items
        let items: Vec<ListItem> = if self.values.is_empty() {
            vec![ListItem::new(
                self.placeholder.as_deref().unwrap_or("(empty)")
            )]
        } else {
            self.values
                .iter()
                .enumerate()
                .map(|(i, &value)| {
                    let formatted = self.format_value(value);
                    let prefix = if Some(i) == self.selected {
                        if self.editing {
                            "> [editing] "
                        } else {
                            "> "
                        }
                    } else {
                        "  "
                    };
                    ListItem::new(format!("{}{}", prefix, formatted))
                })
                .collect()
        };

        // Create list
        let list = List::new(items)
            .block(block)
            .style(text_style);

        // Update list state
        let mut list_state = self.list_state.clone();
        if let Some(sel) = self.selected {
            list_state.select(Some(sel));
        }

        f.render_stateful_widget(list, area, &mut list_state);

        // Render editing input if in editing mode
        if self.editing && self.focused {
            self.render_editing_input(f, area);
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

    /// Render editing input
    fn render_editing_input(&self, f: &mut Frame, area: Rect) {
        if area.y + area.height + 1 < f.area().height {
            let input_area = Rect {
                x: area.x,
                y: area.y + area.height,
                width: area.width,
                height: 1,
            };

            let display_text = if self.text_buffer.is_empty() {
                self.placeholder.as_deref().unwrap_or("Enter value...")
            } else {
                &self.text_buffer
            };

            let input_span = Span::styled(
                format!("Edit: {}", display_text),
                Style::default().fg(Color::Cyan),
            );
            let input_line = Line::from(vec![input_span]);
            let input_paragraph = Paragraph::new(input_line);
            f.render_widget(input_paragraph, input_area);
        }
    }

    /// Render validation error message
    fn render_validation_error(&self, f: &mut Frame, area: Rect, error: &str) {
        let error_y = if self.editing {
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

impl Widget for CommaListWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
        // For Widget trait implementation, simpler rendering
        let (text_style, border_style) = self.get_styles();

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style);

        let items: Vec<ListItem> = if self.values.is_empty() {
            vec![ListItem::new(
                self.placeholder.as_deref().unwrap_or("(empty)")
            )]
        } else {
            self.values
                .iter()
                .map(|&value| {
                    ListItem::new(self.format_value(value))
                })
                .collect()
        };

        let list = List::new(items)
            .block(block)
            .style(text_style);

        let mut list_state = self.list_state.clone();
        if let Some(sel) = self.selected {
            list_state.select(Some(sel));
        }

        list.render(area, buf);
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
        let widget = CommaListWidget::new();
        assert!(widget.values().is_empty());
        assert!(widget.selected().is_none());
        assert!(!widget.focused);
        assert!(!widget.read_only);
        assert!(widget.allow_duplicates);
        assert!(!widget.require_sorted);
    }

    #[test]
    fn test_with_values_str() {
        let widget = CommaListWidget::new().with_values_str("1,2,3");
        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_with_values() {
        let widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_set_allow_duplicates() {
        let widget = CommaListWidget::new().set_allow_duplicates(false);
        assert!(!widget.allow_duplicates);
    }

    #[test]
    fn test_set_require_sorted() {
        let widget = CommaListWidget::new().set_require_sorted(true);
        assert!(widget.require_sorted);
    }

    #[test]
    fn test_with_min() {
        let widget = CommaListWidget::new().with_min(0.0);
        assert_eq!(widget.min, Some(0.0));
    }

    #[test]
    fn test_with_max() {
        let widget = CommaListWidget::new().with_max(100.0);
        assert_eq!(widget.max, Some(100.0));
    }

    #[test]
    fn test_with_decimals() {
        let widget = CommaListWidget::new().with_decimals(4);
        assert_eq!(widget.decimals, 4);
    }

    #[test]
    fn test_with_placeholder() {
        let widget = CommaListWidget::new().with_placeholder("Enter values...");
        assert_eq!(widget.placeholder, Some("Enter values...".to_string()));
    }

    #[test]
    fn test_chained_builders() {
        let widget = CommaListWidget::new()
            .with_values_str("1,2,3")
            .set_allow_duplicates(false)
            .set_require_sorted(true)
            .with_min(0.0)
            .with_max(100.0)
            .with_decimals(2)
            .set_focused(true);

        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
        assert!(!widget.allow_duplicates);
        assert!(widget.require_sorted);
        assert_eq!(widget.min, Some(0.0));
        assert_eq!(widget.max, Some(100.0));
        assert_eq!(widget.decimals, 2);
        assert!(widget.focused);
    }

    // ========================================================================
    // Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_str_simple() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1,2,3");
        assert_eq!(result.unwrap(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_str_with_spaces() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1, 2, 3");
        assert_eq!(result.unwrap(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_str_with_decimals() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1.5,2.7,3.9");
        assert_eq!(result.unwrap(), vec![1.5, 2.7, 3.9]);
    }

    #[test]
    fn test_parse_str_negative() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("-1,2,-3");
        assert_eq!(result.unwrap(), vec![-1.0, 2.0, -3.0]);
    }

    #[test]
    fn test_parse_str_empty() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("");
        assert_eq!(result.unwrap(), Vec::<f64>::new());
    }

    #[test]
    fn test_parse_str_whitespace_only() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("   ");
        assert_eq!(result.unwrap(), Vec::<f64>::new());
    }

    #[test]
    fn test_parse_str_single_value() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("42");
        assert_eq!(result.unwrap(), vec![42.0]);
    }

    #[test]
    fn test_parse_str_trailing_comma() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1,2,3,");
        assert_eq!(result.unwrap(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_str_leading_comma() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str(",1,2,3");
        assert_eq!(result.unwrap(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_str_multiple_commas() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1,,2,,3");
        assert_eq!(result.unwrap(), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_str_invalid_number() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1,abc,3");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_str_scientific_notation() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1e2,2e-3,3e4");
        assert_eq!(result.unwrap(), vec![100.0, 0.002, 30000.0]);
    }

    #[test]
    fn test_parse_str_very_large() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1e10,2e10");
        assert_eq!(result.unwrap(), vec![1e10, 2e10]);
    }

    #[test]
    fn test_parse_str_very_small() {
        let widget = CommaListWidget::new();
        let result = widget.parse_str("1e-10,2e-10");
        assert_eq!(result.unwrap(), vec![1e-10, 2e-10]);
    }

    // ========================================================================
    // Value Manipulation Tests
    // ========================================================================

    #[test]
    fn test_add_value() {
        let mut widget = CommaListWidget::new();
        widget.add_value(1.0);
        widget.add_value(2.0);
        widget.add_value(3.0);
        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_add_value_respects_min() {
        let mut widget = CommaListWidget::new().with_min(10.0);
        widget.add_value(5.0);
        assert_eq!(widget.values(), &[10.0]);
    }

    #[test]
    fn test_add_value_respects_max() {
        let mut widget = CommaListWidget::new().with_max(100.0);
        widget.add_value(150.0);
        assert_eq!(widget.values(), &[100.0]);
    }

    #[test]
    fn test_add_value_no_duplicates() {
        let mut widget = CommaListWidget::new().set_allow_duplicates(false);
        widget.add_value(1.0);
        widget.add_value(1.0); // Should be ignored
        assert_eq!(widget.values(), &[1.0]);
    }

    #[test]
    fn test_add_value_allows_duplicates() {
        let mut widget = CommaListWidget::new().set_allow_duplicates(true);
        widget.add_value(1.0);
        widget.add_value(1.0);
        assert_eq!(widget.values(), &[1.0, 1.0]);
    }

    #[test]
    fn test_add_value_sorted() {
        let mut widget = CommaListWidget::new().set_require_sorted(true);
        widget.add_value(3.0);
        widget.add_value(1.0);
        widget.add_value(2.0);
        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_remove_at() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.remove_at(1);
        assert_eq!(widget.values(), &[1.0, 3.0]);
    }

    #[test]
    fn test_remove_at_first() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.remove_at(0);
        assert_eq!(widget.values(), &[2.0, 3.0]);
    }

    #[test]
    fn test_remove_at_last() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.remove_at(2);
        assert_eq!(widget.values(), &[1.0, 2.0]);
    }

    #[test]
    fn test_remove_at_invalid_index() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        let original = widget.values().to_vec();
        widget.remove_at(10); // Invalid index
        assert_eq!(widget.values(), &original);
    }

    #[test]
    fn test_remove_at_updates_selection() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(2);
        widget.remove_at(1);
        assert_eq!(widget.selected, Some(1)); // Should be decremented
    }

    #[test]
    fn test_remove_at_clears_selection_if_removed() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(1);
        widget.remove_at(1);
        assert_eq!(widget.selected, None);
    }

    #[test]
    fn test_update_at() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.update_at(1, 5.0);
        assert_eq!(widget.values(), &[1.0, 5.0, 3.0]);
    }

    #[test]
    fn test_update_at_respects_min() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .with_min(10.0);
        widget.update_at(1, 5.0);
        assert_eq!(widget.values()[1], 10.0);
    }

    #[test]
    fn test_update_at_respects_max() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .with_max(100.0);
        widget.update_at(1, 150.0);
        assert_eq!(widget.values()[1], 100.0);
    }

    #[test]
    fn test_update_at_no_duplicates() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .set_allow_duplicates(false);
        widget.update_at(1, 1.0); // Try to make it duplicate of index 0
        assert_eq!(widget.values()[1], 2.0); // Should not change
    }

    #[test]
    fn test_update_at_sorted() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .set_require_sorted(true);
        widget.update_at(1, 5.0);
        assert_eq!(widget.values(), &[1.0, 3.0, 5.0]); // Should be sorted
    }

    #[test]
    fn test_clear() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.clear();
        assert!(widget.values().is_empty());
        assert_eq!(widget.selected, None);
    }

    #[test]
    fn test_clear_read_only() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .set_read_only(true);
        widget.clear();
        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]); // Should not clear
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_no_constraints() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_within_range() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![10.0, 20.0, 30.0])
            .with_min(0.0)
            .with_max(100.0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_below_min() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![5.0, 10.0, 15.0])
            .with_min(10.0);
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_above_max() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![50.0, 100.0, 150.0])
            .with_max(100.0);
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_duplicates_not_allowed() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 1.0])
            .set_allow_duplicates(false);
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_duplicates_allowed() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 1.0])
            .set_allow_duplicates(true);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_sorted_required() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![3.0, 1.0, 2.0]);
        // Set require_sorted after setting values to test validation
        widget.require_sorted = true;
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_sorted_not_required() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![3.0, 1.0, 2.0])
            .set_require_sorted(false);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_sorted_valid() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .set_require_sorted(true);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_empty_list() {
        let mut widget = CommaListWidget::new();
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_multiple_errors() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![5.0, 150.0, 5.0])
            .with_min(10.0)
            .with_max(100.0)
            .set_allow_duplicates(false);
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().is_some());
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_up() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.selected, Some(0));
    }

    #[test]
    fn test_handle_key_down() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.selected, Some(0));
    }

    #[test]
    fn test_handle_key_up_wraps() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(0);
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.selected, Some(2)); // Wraps to last
    }

    #[test]
    fn test_handle_key_down_wraps() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(2);
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.selected, Some(0)); // Wraps to first
    }

    #[test]
    fn test_handle_key_enter_starts_editing() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(1);
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.editing);
        assert_eq!(widget.text_buffer, "2.00");
    }

    #[test]
    fn test_handle_key_enter_applies_edit() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(1);
        widget.editing = true;
        widget.text_buffer = "5.0".to_string();
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.values()[1], 5.0);
        assert!(!widget.editing);
    }

    #[test]
    fn test_handle_key_esc_cancels_editing() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.editing = true;
        widget.text_buffer = "999".to_string();
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::empty());
        widget.handle_key(key);
        assert!(!widget.editing);
        assert!(widget.text_buffer.is_empty());
    }

    #[test]
    fn test_handle_key_add() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0]);
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.editing);
        assert_eq!(widget.selected, Some(2)); // New item at end
    }

    #[test]
    fn test_handle_key_delete() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(1);
        let key = KeyEvent::new(KeyCode::Delete, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.values(), &[1.0, 3.0]);
    }

    #[test]
    fn test_handle_key_delete_char() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.selected = Some(1);
        let key = KeyEvent::new(KeyCode::Char('d'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.values(), &[1.0, 3.0]);
    }

    #[test]
    fn test_handle_key_text_editing() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        widget.editing = true;
        let key1 = KeyEvent::new(KeyCode::Char('4'), KeyModifiers::empty());
        let key2 = KeyEvent::new(KeyCode::Char('2'), KeyModifiers::empty());
        widget.handle_key(key1);
        widget.handle_key(key2);
        assert_eq!(widget.text_buffer, "42");
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut widget = CommaListWidget::new();
        widget.editing = true;
        widget.text_buffer = "123".to_string();
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.text_buffer, "12");
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
    }

    // ========================================================================
    // Values String Tests
    // ========================================================================

    #[test]
    fn test_values_str() {
        let widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .with_decimals(0);
        assert_eq!(widget.values_str(), "1,2,3");
    }

    #[test]
    fn test_values_str_with_decimals() {
        let widget = CommaListWidget::new()
            .with_values(vec![1.5, 2.7, 3.9])
            .with_decimals(1);
        assert_eq!(widget.values_str(), "1.5,2.7,3.9");
    }

    #[test]
    fn test_values_str_empty() {
        let widget = CommaListWidget::new();
        assert_eq!(widget.values_str(), "");
    }

    // ========================================================================
    // Preset Tests
    // ========================================================================

    #[test]
    fn test_add_preset() {
        let widget = CommaListWidget::new()
            .add_preset("test", vec![1.0, 2.0, 3.0]);
        assert_eq!(widget.presets.len(), 1);
        assert_eq!(widget.presets[0].name, "test");
        assert_eq!(widget.presets[0].values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_apply_preset() {
        let mut widget = CommaListWidget::new()
            .add_preset("test", vec![10.0, 20.0, 30.0]);
        widget.apply_preset(0);
        assert_eq!(widget.values(), &[10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_apply_preset_sorted() {
        let mut widget = CommaListWidget::new()
            .add_preset("test", vec![30.0, 10.0, 20.0])
            .set_require_sorted(true);
        widget.apply_preset(0);
        assert_eq!(widget.values(), &[10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_apply_preset_invalid_index() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0]);
        let original = widget.values().to_vec();
        widget.apply_preset(10); // Invalid index
        assert_eq!(widget.values(), &original);
    }

    #[test]
    fn test_apply_preset_read_only() {
        let mut widget = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .add_preset("test", vec![10.0, 20.0, 30.0])
            .set_read_only(true);
        let original = widget.values().to_vec();
        widget.apply_preset(0);
        assert_eq!(widget.values(), &original); // Should not change
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_very_large_list() {
        let mut widget = CommaListWidget::new();
        for i in 0..1000 {
            widget.add_value(i as f64);
        }
        assert_eq!(widget.len(), 1000);
    }

    #[test]
    fn test_very_large_values() {
        let mut widget = CommaListWidget::new();
        widget.add_value(1e10);
        widget.add_value(2e10);
        assert_eq!(widget.values(), &[1e10, 2e10]);
    }

    #[test]
    fn test_very_small_values() {
        let mut widget = CommaListWidget::new();
        widget.add_value(1e-10);
        widget.add_value(2e-10);
        assert_eq!(widget.values(), &[1e-10, 2e-10]);
    }

    #[test]
    fn test_negative_values() {
        let mut widget = CommaListWidget::new();
        widget.add_value(-1.0);
        widget.add_value(-2.0);
        assert_eq!(widget.values(), &[-1.0, -2.0]);
    }

    #[test]
    fn test_zero_values() {
        let mut widget = CommaListWidget::new();
        widget.add_value(0.0);
        widget.add_value(0.0);
        assert_eq!(widget.values(), &[0.0, 0.0]);
    }

    #[test]
    fn test_duplicate_detection_epsilon() {
        let mut widget = CommaListWidget::new().set_allow_duplicates(false);
        widget.add_value(1.0);
        widget.add_value(1.0 + f64::EPSILON * 10.0); // Larger than epsilon
        // Should allow values that are different enough
        assert_eq!(widget.len(), 2);
    }

    #[test]
    fn test_duplicate_detection_exact() {
        let mut widget = CommaListWidget::new().set_allow_duplicates(false);
        widget.add_value(1.0);
        widget.add_value(1.0);
        assert_eq!(widget.len(), 1); // Duplicate should be rejected
    }

    #[test]
    fn test_sorted_with_duplicates() {
        let mut widget = CommaListWidget::new()
            .set_require_sorted(true)
            .set_allow_duplicates(true);
        widget.add_value(3.0);
        widget.add_value(1.0);
        widget.add_value(2.0);
        widget.add_value(1.0);
        assert_eq!(widget.values(), &[1.0, 1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_min_equals_max() {
        let mut widget = CommaListWidget::new()
            .with_min(50.0)
            .with_max(50.0);
        widget.add_value(50.0);
        widget.add_value(60.0);
        assert_eq!(widget.values(), &[50.0, 50.0]);
    }

    #[test]
    fn test_rapid_add_remove() {
        let mut widget = CommaListWidget::new();
        for i in 0..100 {
            widget.add_value(i as f64);
        }
        for i in (0..100).rev() {
            widget.remove_at(i);
        }
        assert!(widget.values().is_empty());
    }

    #[test]
    fn test_update_all_items() {
        let mut widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        for i in 0..widget.len() {
            widget.update_at(i, (i + 10) as f64);
        }
        assert_eq!(widget.values(), &[10.0, 11.0, 12.0]);
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_editing_workflow() {
        let mut widget = CommaListWidget::new()
            .with_min(0.0)
            .with_max(100.0)
            .set_require_sorted(true)
            .set_focused(true);

        // Add values
        widget.add_value(30.0);
        widget.add_value(10.0);
        widget.add_value(20.0);
        assert_eq!(widget.values(), &[10.0, 20.0, 30.0]);

        // Edit value
        widget.selected = Some(1);
        widget.update_at(1, 25.0);
        assert_eq!(widget.values(), &[10.0, 25.0, 30.0]);

        // Remove value
        widget.remove_at(0);
        assert_eq!(widget.values(), &[25.0, 30.0]);
    }

    #[test]
    fn test_parse_and_validate_workflow() {
        let mut widget = CommaListWidget::new()
            .with_min(0.0)
            .with_max(100.0)
            .set_allow_duplicates(false)
            .set_require_sorted(true);

        widget.set_values_from_str("50,20,30,10");
        assert_eq!(widget.values(), &[10.0, 20.0, 30.0, 50.0]); // Should be sorted
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_preset_workflow() {
        let mut widget = CommaListWidget::new()
            .add_preset("small", vec![1.0, 2.0, 3.0])
            .add_preset("large", vec![100.0, 200.0, 300.0]);

        widget.apply_preset(0);
        assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);

        widget.apply_preset(1);
        assert_eq!(widget.values(), &[100.0, 200.0, 300.0]);
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = CommaListWidget::new()
            .with_values(vec![1.0, 2.0, 3.0])
            .set_allow_duplicates(false)
            .set_require_sorted(true)
            .with_min(0.0)
            .with_max(100.0)
            .set_focused(true);

        let widget2 = widget1.clone();
        assert_eq!(widget1.values(), widget2.values());
        assert_eq!(widget1.allow_duplicates, widget2.allow_duplicates);
        assert_eq!(widget1.require_sorted, widget2.require_sorted);
        assert_eq!(widget1.min, widget2.min);
        assert_eq!(widget1.max, widget2.max);
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = CommaListWidget::new().with_values(vec![1.0, 2.0]);
        let mut widget2 = widget1.clone();

        widget1.add_value(3.0);
        widget2.add_value(4.0);

        assert_eq!(widget1.values(), &[1.0, 2.0, 3.0]);
        assert_eq!(widget2.values(), &[1.0, 2.0, 4.0]);
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1 = CommaListWidget::default();
        let widget2 = CommaListWidget::new();
        assert_eq!(widget1.values(), widget2.values());
        assert_eq!(widget1.allow_duplicates, widget2.allow_duplicates);
        assert_eq!(widget1.require_sorted, widget2.require_sorted);
    }

    // ========================================================================
    // Helper Method Tests
    // ========================================================================

    #[test]
    fn test_len() {
        let widget = CommaListWidget::new().with_values(vec![1.0, 2.0, 3.0]);
        assert_eq!(widget.len(), 3);
    }

    #[test]
    fn test_is_empty() {
        let widget1 = CommaListWidget::new();
        assert!(widget1.is_empty());

        let widget2 = CommaListWidget::new().with_values(vec![1.0]);
        assert!(!widget2.is_empty());
    }

    #[test]
    fn test_format_value() {
        let widget = CommaListWidget::new().with_decimals(2);
        assert_eq!(widget.format_value(1.234), "1.23");
    }

    #[test]
    fn test_format_value_zero() {
        let widget = CommaListWidget::new().with_decimals(2);
        assert_eq!(widget.format_value(0.0), "0.00");
    }

    #[test]
    fn test_format_value_negative() {
        let widget = CommaListWidget::new().with_decimals(1);
        assert_eq!(widget.format_value(-42.5), "-42.5");
    }
}
