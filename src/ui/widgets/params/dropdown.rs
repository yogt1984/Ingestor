//! Dropdown Widget
//!
//! A reusable generic dropdown widget for TUI parameter configuration.
//! Supports expandable list, keyboard navigation, type-to-search filter,
//! and custom rendering for any type T.

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fmt::Display;

// ============================================================================
// DropdownWidget
// ============================================================================

/// Generic dropdown widget for TUI parameter configuration
pub struct DropdownWidget<T> {
    /// Available options
    options: Vec<T>,
    /// Currently selected index (None = no selection)
    selected: Option<usize>,
    /// Whether dropdown is expanded
    expanded: bool,
    /// Filter text for type-to-search
    filter: String,
    /// Placeholder text shown when no selection
    placeholder: Option<String>,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
    /// List state for rendering
    list_state: ListState,
    /// Maximum visible items when expanded
    max_visible_items: usize,
}

impl<T: Clone> Clone for DropdownWidget<T> {
    fn clone(&self) -> Self {
        Self {
            options: self.options.clone(),
            selected: self.selected,
            expanded: false, // Always start collapsed
            filter: String::new(), // Don't clone filter
            placeholder: self.placeholder.clone(),
            focused: self.focused,
            read_only: self.read_only,
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
            list_state: ListState::default(),
            max_visible_items: self.max_visible_items,
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for DropdownWidget<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DropdownWidget")
            .field("options", &self.options)
            .field("selected", &self.selected)
            .field("expanded", &self.expanded)
            .field("filter", &self.filter)
            .field("focused", &self.focused)
            .field("read_only", &self.read_only)
            .field("validation_state", &self.validation_state)
            .finish()
    }
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

impl<T> Default for DropdownWidget<T>
where
    T: Display + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DropdownWidget<T>
where
    T: Display + Clone,
{
    /// Create a new dropdown widget
    pub fn new() -> Self {
        Self {
            options: Vec::new(),
            selected: None,
            expanded: false,
            filter: String::new(),
            placeholder: None,
            focused: false,
            read_only: false,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
            list_state: ListState::default(),
            max_visible_items: 10,
        }
    }

    /// Set available options
    pub fn with_options(mut self, options: Vec<T>) -> Self {
        self.options = options;
        self.validate();
        self
    }

    /// Set placeholder text
    pub fn with_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Set initial selection by index
    pub fn with_selected(mut self, index: usize) -> Self {
        if index < self.options.len() {
            self.selected = Some(index);
        }
        self.validate();
        self
    }

    /// Set initial selection by value (requires PartialEq)
    pub fn with_selected_value(mut self, value: &T) -> Self
    where
        T: PartialEq,
    {
        if let Some(index) = self.options.iter().position(|opt| opt == value) {
            self.selected = Some(index);
        }
        self.validate();
        self
    }

    /// Set maximum visible items when expanded
    pub fn with_max_visible_items(mut self, max: usize) -> Self {
        self.max_visible_items = max;
        self
    }

    /// Set whether the widget is focused
    pub fn set_focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        if !focused {
            self.expanded = false;
            self.filter.clear();
        }
        self
    }

    /// Set whether the input is read-only
    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        if read_only {
            self.expanded = false;
            self.filter.clear();
        }
        self
    }

    /// Set whether to show validation errors
    pub fn set_show_validation(mut self, show: bool) -> Self {
        self.show_validation = show;
        self
    }

    /// Get available options
    pub fn options(&self) -> &[T] {
        &self.options
    }

    /// Get filtered options based on current filter
    pub fn filtered_options(&self) -> Vec<&T> {
        if self.filter.is_empty() {
            return self.options.iter().collect();
        }

        let filter_lower = self.filter.to_lowercase();
        self.options
            .iter()
            .filter(|opt| {
                let opt_str = format!("{}", opt).to_lowercase();
                opt_str.contains(&filter_lower)
            })
            .collect()
    }

    /// Get currently selected option
    pub fn selected_option(&self) -> Option<&T> {
        self.selected.and_then(|idx| self.options.get(idx))
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
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

    /// Get filter string
    pub fn filter(&self) -> &str {
        &self.filter
    }

    /// Set filter string (for testing)
    pub fn set_filter(&mut self, filter: impl Into<String>) {
        self.filter = filter.into();
    }

    /// Get list state reference
    pub fn list_state(&self) -> &ListState {
        &self.list_state
    }

    /// Get mutable list state reference
    pub fn list_state_mut(&mut self) -> &mut ListState {
        &mut self.list_state
    }

    /// Check if dropdown is expanded
    pub fn is_expanded(&self) -> bool {
        self.expanded
    }

    /// Check if the widget is focused
    pub fn is_focused(&self) -> bool {
        self.focused
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

    /// Expand the dropdown
    pub fn expand(&mut self) {
        // Allow expansion even in read-only mode for viewing/navigation
        self.expanded = true;
        self.filter.clear();
    }

    /// Collapse the dropdown
    pub fn collapse(&mut self) {
        self.expanded = false;
        self.filter.clear();
    }

    /// Toggle expanded state
    pub fn toggle(&mut self) {
        // Allow toggle even in read-only mode for viewing/navigation
        if self.expanded {
            self.collapse();
        } else {
            self.expand();
        }
    }

    /// Select option by index
    pub fn select(&mut self, index: usize) {
        if self.read_only {
            return;
        }

        let filtered = self.filtered_options();
        if index < filtered.len() {
            // Find the original index in options
            let selected_option = filtered[index];
            if let Some(orig_idx) = self.options.iter().position(|opt| {
                std::ptr::eq(opt as *const T, selected_option as *const T)
            }) {
                self.selected = Some(orig_idx);
                self.collapse();
                self.validate();
            }
        }
    }

    /// Clear selection
    pub fn clear_selection(&mut self) {
        if self.read_only {
            return;
        }
        self.selected = None;
        self.validate();
    }

    /// Validate the current selection
    pub fn validate(&mut self) {
        if let Some(idx) = self.selected {
            if idx >= self.options.len() {
                self.validation_state = ValidationState::Invalid("Invalid selection".to_string());
            } else {
                self.validation_state = ValidationState::Valid;
            }
        } else {
            self.validation_state = ValidationState::Unvalidated;
        }
    }

    /// Handle a key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.read_only && !matches!(key.code, KeyCode::Up | KeyCode::Down | KeyCode::Enter | KeyCode::Esc) {
            return false;
        }

        match key.code {
            KeyCode::Enter | KeyCode::Char(' ') if !self.expanded => {
                // Expand dropdown
                self.expand();
                true
            }
            KeyCode::Enter if self.expanded => {
                // Select current highlighted option
                if let Some(highlighted) = self.list_state.selected() {
                    self.select(highlighted);
                }
                true
            }
            KeyCode::Esc if self.expanded => {
                // Collapse dropdown
                self.collapse();
                true
            }
            KeyCode::Up if self.expanded => {
                // Navigate up in filtered list
                let filtered_len = self.filtered_options().len();
                if filtered_len > 0 {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_index = if current == 0 {
                        filtered_len - 1
                    } else {
                        current - 1
                    };
                    self.list_state.select(Some(new_index));
                }
                true
            }
            KeyCode::Down if self.expanded => {
                // Navigate down in filtered list
                let filtered_len = self.filtered_options().len();
                if filtered_len > 0 {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_index = (current + 1) % filtered_len;
                    self.list_state.select(Some(new_index));
                }
                true
            }
            KeyCode::Char(c) if self.expanded => {
                // Type-to-search
                self.filter.push(c);
                // Reset selection to first match
                self.list_state.select(Some(0));
                true
            }
            KeyCode::Backspace if self.expanded && !self.filter.is_empty() => {
                // Remove last character from filter
                self.filter.pop();
                // Reset selection to first match
                self.list_state.select(Some(0));
                true
            }
            _ => false,
        }
    }

    /// Get display text for selected option
    fn display_text(&self) -> String {
        if let Some(selected) = self.selected_option() {
            format!("{}", selected)
        } else {
            self.placeholder.as_deref().unwrap_or("(none)").to_string()
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

        // Display text
        let display_text = if self.expanded && !self.filter.is_empty() {
            format!("{} [filter: {}]", self.display_text(), self.filter)
        } else {
            self.display_text()
        };

        // Create paragraph with text
        let paragraph = Paragraph::new(display_text)
            .style(text_style)
            .block(block);

        f.render_widget(paragraph, area);

        // Render dropdown list if expanded
        if self.expanded {
            self.render_dropdown_list(f, area);
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

    /// Render dropdown list
    fn render_dropdown_list(&self, f: &mut Frame, main_area: Rect) {
        let filtered = self.filtered_options();
        if filtered.is_empty() {
            return;
        }

        // Calculate dropdown area (below main widget)
        let dropdown_height = (filtered.len().min(self.max_visible_items) + 2) as u16; // +2 for borders
        let available_height = f.area().height.saturating_sub(main_area.y + main_area.height);
        let actual_height = dropdown_height.min(available_height);

        if actual_height < 3 {
            return; // Not enough space
        }

        let dropdown_area = Rect {
            x: main_area.x,
            y: main_area.y + main_area.height,
            width: main_area.width,
            height: actual_height,
        };

        // Create list items
        let items: Vec<ListItem> = filtered
            .iter()
            .take(self.max_visible_items)
            .enumerate()
            .map(|(i, opt)| {
                let prefix = if Some(i) == self.list_state.selected() {
                    "> "
                } else {
                    "  "
                };
                ListItem::new(format!("{}{}", prefix, opt))
            })
            .collect();

        // Create list
        let list = List::new(items)
            .style(Style::default().fg(Color::White));

        // Update list state
        let mut list_state = self.list_state.clone();
        if list_state.selected().is_none() && !filtered.is_empty() {
            list_state.select(Some(0));
        }

        f.render_stateful_widget(list, dropdown_area, &mut list_state);
    }

    /// Render validation error message
    fn render_validation_error(&self, f: &mut Frame, area: Rect, error: &str) {
        let error_y = if self.expanded {
            area.y + area.height + (self.max_visible_items.min(self.filtered_options().len()) as u16) + 2
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

impl<T> Widget for DropdownWidget<T>
where
    T: Display + Clone,
{
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
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
        let widget: DropdownWidget<String> = DropdownWidget::new();
        assert!(widget.options().is_empty());
        assert!(widget.selected_index().is_none());
        assert!(!widget.is_expanded());
        assert!(!widget.focused);
        assert!(!widget.read_only);
    }

    #[test]
    fn test_with_options() {
        let widget = DropdownWidget::new()
            .with_options(vec!["Option1".to_string(), "Option2".to_string()]);
        assert_eq!(widget.options().len(), 2);
    }

    #[test]
    fn test_with_placeholder() {
        let widget: DropdownWidget<String> = DropdownWidget::new()
            .with_placeholder("Select option...");
        assert_eq!(widget.placeholder, Some("Select option...".to_string()));
    }

    #[test]
    fn test_with_selected() {
        let widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
            .with_selected(1);
        assert_eq!(widget.selected_index(), Some(1));
        assert_eq!(widget.selected_option(), Some(&"B".to_string()));
    }

    #[test]
    fn test_with_selected_invalid_index() {
        let widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_selected(10); // Invalid index
        assert!(widget.selected_index().is_none());
    }

    #[test]
    fn test_with_selected_value() {
        let widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
            .with_selected_value(&"B".to_string());
        assert_eq!(widget.selected_index(), Some(1));
    }

    #[test]
    fn test_with_selected_value_not_found() {
        let widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_selected_value(&"Z".to_string()); // Not in options
        assert!(widget.selected_index().is_none());
    }

    #[test]
    fn test_with_max_visible_items() {
        let widget: DropdownWidget<String> = DropdownWidget::new().with_max_visible_items(5);
        assert_eq!(widget.max_visible_items, 5);
    }

    #[test]
    fn test_chained_builders() {
        let widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_placeholder("Select...")
            .with_selected(0)
            .set_focused(true);

        assert_eq!(widget.options().len(), 2);
        assert_eq!(widget.placeholder, Some("Select...".to_string()));
        assert_eq!(widget.selected_index(), Some(0));
        assert!(widget.focused);
    }

    // ========================================================================
    // Selection Tests
    // ========================================================================

    #[test]
    fn test_select() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()]);
        widget.select(1);
        assert_eq!(widget.selected_index(), Some(1));
    }

    #[test]
    fn test_select_invalid_index() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.select(10); // Invalid
        assert!(widget.selected_index().is_none());
    }

    #[test]
    fn test_select_with_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()]);
        widget.expand();
        widget.filter = "Ap".to_string();
        widget.select(0); // Select first filtered option (Apple)
        // Should select Apple (index 0)
        assert_eq!(widget.selected_index(), Some(0));
    }

    #[test]
    fn test_clear_selection() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_selected(0);
        widget.clear_selection();
        assert!(widget.selected_index().is_none());
    }

    #[test]
    fn test_clear_selection_read_only() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_selected(0)
            .set_read_only(true);
        widget.clear_selection();
        assert_eq!(widget.selected_index(), Some(0)); // Should not clear
    }

    // ========================================================================
    // Expand/Collapse Tests
    // ========================================================================

    #[test]
    fn test_expand() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.expand();
        assert!(widget.is_expanded());
    }

    #[test]
    fn test_collapse() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.expand();
        widget.collapse();
        assert!(!widget.is_expanded());
    }

    #[test]
    fn test_toggle() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.toggle();
        assert!(widget.is_expanded());
        widget.toggle();
        assert!(!widget.is_expanded());
    }

    #[test]
    fn test_toggle_read_only() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_read_only(true);
        widget.toggle();
        // Read-only mode still allows viewing/navigation, just not selection
        assert!(widget.is_expanded()); // Should expand for viewing
    }

    #[test]
    fn test_expand_clears_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.filter = "test".to_string();
        widget.expand();
        assert!(widget.filter.is_empty());
    }

    #[test]
    fn test_collapse_clears_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.expand();
        widget.filter = "test".to_string();
        widget.collapse();
        assert!(widget.filter.is_empty());
    }

    #[test]
    fn test_set_focused_false_collapses() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.expand();
        widget = widget.set_focused(false);
        assert!(!widget.is_expanded());
    }

    // ========================================================================
    // Filter Tests
    // ========================================================================

    #[test]
    fn test_filtered_options_empty_filter() {
        let widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string()]);
        let filtered = widget.filtered_options();
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_filtered_options_with_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()]);
        widget.filter = "Ap".to_string();
        let filtered = widget.filtered_options();
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|&s| s == "Apple"));
        assert!(filtered.iter().any(|&s| s == "Apricot"));
    }

    #[test]
    fn test_filtered_options_case_insensitive() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "banana".to_string(), "APRICOT".to_string()]);
        widget.filter = "a".to_string();
        let filtered = widget.filtered_options();
        assert_eq!(filtered.len(), 3); // All match (case insensitive)
    }

    #[test]
    fn test_filtered_options_no_matches() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string()]);
        widget.filter = "XYZ".to_string();
        let filtered = widget.filtered_options();
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filtered_options_partial_match() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Pineapple".to_string(), "Grape".to_string()]);
        widget.filter = "app".to_string();
        let filtered = widget.filtered_options();
        assert_eq!(filtered.len(), 2);
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_enter_expands() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.is_expanded());
    }

    #[test]
    fn test_handle_key_space_expands() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert!(widget.is_expanded());
    }

    #[test]
    fn test_handle_key_enter_selects() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        widget.expand();
        widget.list_state.select(Some(1));
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.selected_index(), Some(1));
        assert!(!widget.is_expanded());
    }

    #[test]
    fn test_handle_key_esc_collapses() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        widget.expand();
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::empty());
        widget.handle_key(key);
        assert!(!widget.is_expanded());
    }

    #[test]
    fn test_handle_key_up() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
            .set_focused(true);
        widget.expand();
        widget.list_state.select(Some(1));
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.list_state.selected(), Some(0));
    }

    #[test]
    fn test_handle_key_up_wraps() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        widget.expand();
        widget.list_state.select(Some(0));
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.list_state.selected(), Some(1)); // Wraps to last
    }

    #[test]
    fn test_handle_key_down() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
            .set_focused(true);
        widget.expand();
        widget.list_state.select(Some(0));
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.list_state.selected(), Some(1));
    }

    #[test]
    fn test_handle_key_down_wraps() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        widget.expand();
        widget.list_state.select(Some(1));
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.list_state.selected(), Some(0)); // Wraps to first
    }

    #[test]
    fn test_handle_key_char_filters() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string()])
            .set_focused(true);
        widget.expand();
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.filter, "a");
    }

    #[test]
    fn test_handle_key_backspace_filters() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        widget.expand();
        widget.filter = "test".to_string();
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.filter, "tes");
    }

    #[test]
    fn test_handle_key_backspace_empty_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_focused(true);
        widget.expand();
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.filter, "");
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_read_only(true)
            .set_focused(true);
        let key = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
    }

    #[test]
    fn test_handle_key_read_only_allows_navigation() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .set_read_only(true)
            .set_focused(true);
        widget.expand();
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(widget.handle_key(key));
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_no_selection() {
        let mut widget: DropdownWidget<String> = DropdownWidget::new();
        widget.validate();
        assert!(widget.validation_error().is_none());
    }

    #[test]
    fn test_validate_valid_selection() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_selected(0);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_invalid_selection() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        widget.selected = Some(10); // Invalid index
        widget.validate();
        assert!(!widget.is_valid());
    }

    // ========================================================================
    // Display Text Tests
    // ========================================================================

    #[test]
    fn test_display_text_with_selection() {
        let widget = DropdownWidget::new()
            .with_options(vec!["Option1".to_string(), "Option2".to_string()])
            .with_selected(0);
        assert_eq!(widget.display_text(), "Option1");
    }

    #[test]
    fn test_display_text_no_selection() {
        let widget: DropdownWidget<String> = DropdownWidget::new();
        assert_eq!(widget.display_text(), "(none)");
    }

    #[test]
    fn test_display_text_with_placeholder() {
        let widget: DropdownWidget<String> = DropdownWidget::new()
            .with_placeholder("Select option...");
        assert_eq!(widget.display_text(), "Select option...");
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_empty_options() {
        let widget: DropdownWidget<String> = DropdownWidget::new();
        assert!(widget.options().is_empty());
        assert!(widget.selected_option().is_none());
    }

    #[test]
    fn test_single_option() {
        let widget = DropdownWidget::new()
            .with_options(vec!["Only".to_string()])
            .with_selected(0);
        assert_eq!(widget.selected_option(), Some(&"Only".to_string()));
    }

    #[test]
    fn test_very_many_options() {
        let options: Vec<String> = (0..1000).map(|i| format!("Option{}", i)).collect();
        let widget = DropdownWidget::new().with_options(options);
        assert_eq!(widget.options().len(), 1000);
    }

    #[test]
    fn test_unicode_options() {
        let widget = DropdownWidget::new()
            .with_options(vec!["选项1".to_string(), "选项2".to_string(), "🚀选项".to_string()]);
        assert_eq!(widget.options().len(), 3);
    }

    #[test]
    fn test_long_option_names() {
        let long_name = "A".repeat(1000);
        let widget = DropdownWidget::new()
            .with_options(vec![long_name.clone()])
            .with_selected(0);
        assert_eq!(widget.selected_option(), Some(&long_name));
    }

    #[test]
    fn test_rapid_toggle() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        for _ in 0..100 {
            widget.toggle();
        }
        assert!(!widget.is_expanded()); // Even number of toggles
    }

    #[test]
    fn test_filter_special_characters() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Option-1".to_string(), "Option_2".to_string(), "Option.3".to_string()]);
        widget.filter = "-".to_string();
        let filtered = widget.filtered_options();
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_filter_numbers() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Option1".to_string(), "Option2".to_string(), "Other".to_string()]);
        widget.filter = "1".to_string();
        let filtered = widget.filtered_options();
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_select_after_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()]);
        widget.expand();
        widget.filter = "Ap".to_string();
        widget.select(1); // Select second filtered option (Apricot)
        assert_eq!(widget.selected_index(), Some(2)); // Apricot is at index 2
    }

    #[test]
    fn test_multiple_char_filter() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Apricot".to_string(), "Application".to_string(), "Banana".to_string()]);
        widget.filter = "App".to_string();
        let filtered = widget.filtered_options();
        // "App" (lowercase "app") should match:
        // - "Apple" -> "apple" contains "app" ✓
        // - "Apricot" -> "apricot" does NOT contain "app" (has "ap" then "pr") ✗
        // - "Application" -> "application" contains "app" ✓
        // - "Banana" -> "banana" does NOT contain "app" ✗
        // So we expect 2 matches: Apple and Application
        let filtered_names: Vec<String> = filtered.iter().map(|s| s.to_string()).collect();
        assert_eq!(filtered.len(), 2, "Expected 2 matches for 'App' filter, got: {:?}", filtered_names);
        assert!(filtered_names.contains(&"Apple".to_string()));
        assert!(filtered_names.contains(&"Application".to_string()));
    }

    #[test]
    fn test_filter_resets_selection() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()]);
        widget.expand();
        widget.list_state.select(Some(2));
        let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        widget.handle_key(key);
        assert_eq!(widget.list_state.selected(), Some(0)); // Resets to first match
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_selection_workflow() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Option1".to_string(), "Option2".to_string(), "Option3".to_string()])
            .set_focused(true);

        // Expand
        widget.expand();
        assert!(widget.is_expanded());

        // Navigate down
        widget.list_state.select(Some(0));
        let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        widget.handle_key(key_down);
        assert_eq!(widget.list_state.selected(), Some(1));

        // Select
        let key_enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key_enter);
        assert_eq!(widget.selected_index(), Some(1));
        assert!(!widget.is_expanded());
    }

    #[test]
    fn test_filter_and_select_workflow() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()])
            .set_focused(true);

        widget.expand();

        // Type filter
        let key_a = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        let key_p = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::empty());
        widget.handle_key(key_a);
        widget.handle_key(key_p);
        assert_eq!(widget.filter, "ap");

        // Select first filtered option
        let key_enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
        widget.handle_key(key_enter);
        assert_eq!(widget.selected_index(), Some(0)); // Apple
    }

    #[test]
    fn test_navigation_workflow() {
        let mut widget = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
            .set_focused(true);

        widget.expand();
        widget.list_state.select(Some(0));

        // Navigate down twice
        widget.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::empty()));
        widget.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::empty()));
        assert_eq!(widget.list_state.selected(), Some(2));

        // Navigate up
        widget.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::empty()));
        assert_eq!(widget.list_state.selected(), Some(1));
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()])
            .with_selected(0)
            .with_placeholder("Select...")
            .set_focused(true);

        let widget2 = widget1.clone();
        assert_eq!(widget1.options(), widget2.options());
        assert_eq!(widget1.selected_index(), widget2.selected_index());
        assert_eq!(widget1.placeholder, widget2.placeholder);
        assert_eq!(widget1.focused, widget2.focused);
        // Expanded and filter are reset on clone
        assert!(!widget2.is_expanded());
        assert!(widget2.filter.is_empty());
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = DropdownWidget::new()
            .with_options(vec!["A".to_string(), "B".to_string()]);
        let mut widget2 = widget1.clone();

        widget1.select(0);
        widget2.select(1);

        assert_eq!(widget1.selected_index(), Some(0));
        assert_eq!(widget2.selected_index(), Some(1));
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1: DropdownWidget<String> = DropdownWidget::default();
        let widget2: DropdownWidget<String> = DropdownWidget::new();
        assert_eq!(widget1.options().len(), widget2.options().len());
        assert_eq!(widget1.selected_index(), widget2.selected_index());
    }

    // ========================================================================
    // Generic Type Tests
    // ========================================================================

    #[test]
    fn test_with_integer_options() {
        let widget = DropdownWidget::new()
            .with_options(vec![1, 2, 3])
            .with_selected(1);
        assert_eq!(widget.selected_option(), Some(&2));
    }

    #[test]
    fn test_with_float_options() {
        let widget = DropdownWidget::new()
            .with_options(vec![1.5, 2.7, 3.9])
            .with_selected(0);
        assert_eq!(widget.selected_option(), Some(&1.5));
    }

    #[test]
    fn test_with_custom_type() {
        #[derive(Clone, PartialEq, Debug)]
        struct CustomOption {
            name: String,
            value: i32,
        }

        impl Display for CustomOption {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} ({})", self.name, self.value)
            }
        }

        let widget = DropdownWidget::new()
            .with_options(vec![
                CustomOption { name: "A".to_string(), value: 1 },
                CustomOption { name: "B".to_string(), value: 2 },
            ])
            .with_selected(0);

        assert!(widget.selected_option().is_some());
    }
}
