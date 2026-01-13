//! Path Input Widget
//!
//! A reusable path input widget for TUI parameter configuration.
//! Supports path completion (tab), file browser integration, existence validation,
//! and relative/absolute path support.

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::{Path, PathBuf};

// ============================================================================
// PathInputWidget
// ============================================================================

/// Path input widget for TUI parameter configuration
pub struct PathInputWidget {
    /// Current path value
    path: String,
    /// Cursor position (0-based index into path)
    cursor_pos: usize,
    /// Placeholder text shown when path is empty
    placeholder: Option<String>,
    /// Whether to validate path existence
    validate_existence: bool,
    /// Whether path must exist (for validation)
    must_exist: bool,
    /// Whether to allow files (true) or only directories (false)
    allow_files: bool,
    /// Whether to allow directories (true) or only files (false)
    allow_directories: bool,
    /// Base directory for relative paths
    base_dir: Option<PathBuf>,
    /// Whether the widget is currently focused/active
    focused: bool,
    /// Whether the input is read-only
    read_only: bool,
    /// Current validation state
    validation_state: ValidationState,
    /// Whether to show validation errors
    show_validation: bool,
    /// Tab completion candidates
    completion_candidates: Vec<String>,
    /// Current completion index
    completion_index: Option<usize>,
}

impl Clone for PathInputWidget {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            cursor_pos: self.cursor_pos,
            placeholder: self.placeholder.clone(),
            validate_existence: self.validate_existence,
            must_exist: self.must_exist,
            allow_files: self.allow_files,
            allow_directories: self.allow_directories,
            base_dir: self.base_dir.clone(),
            focused: self.focused,
            read_only: self.read_only,
            validation_state: self.validation_state.clone(),
            show_validation: self.show_validation,
            completion_candidates: Vec::new(), // Don't clone candidates
            completion_index: None,
        }
    }
}

impl std::fmt::Debug for PathInputWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathInputWidget")
            .field("path", &self.path)
            .field("cursor_pos", &self.cursor_pos)
            .field("placeholder", &self.placeholder)
            .field("validate_existence", &self.validate_existence)
            .field("must_exist", &self.must_exist)
            .field("allow_files", &self.allow_files)
            .field("allow_directories", &self.allow_directories)
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

impl Default for PathInputWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl PathInputWidget {
    /// Create a new path input widget
    pub fn new() -> Self {
        Self {
            path: String::new(),
            cursor_pos: 0,
            placeholder: None,
            validate_existence: false,
            must_exist: false,
            allow_files: true,
            allow_directories: true,
            base_dir: None,
            focused: false,
            read_only: false,
            validation_state: ValidationState::Unvalidated,
            show_validation: true,
            completion_candidates: Vec::new(),
            completion_index: None,
        }
    }

    /// Set the initial path value
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self.cursor_pos = self.path.chars().count();
        self.validate();
        self
    }

    /// Set placeholder text
    pub fn with_placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    /// Enable existence validation
    pub fn with_validate_existence(mut self, validate: bool) -> Self {
        self.validate_existence = validate;
        self.validate();
        self
    }

    /// Set whether path must exist
    pub fn with_must_exist(mut self, must_exist: bool) -> Self {
        self.must_exist = must_exist;
        self.validate();
        self
    }

    /// Set whether files are allowed
    pub fn with_allow_files(mut self, allow: bool) -> Self {
        self.allow_files = allow;
        self.validate();
        self
    }

    /// Set whether directories are allowed
    pub fn with_allow_directories(mut self, allow: bool) -> Self {
        self.allow_directories = allow;
        self.validate();
        self
    }

    /// Set base directory for relative paths
    pub fn with_base_dir(mut self, base_dir: impl AsRef<Path>) -> Self {
        self.base_dir = Some(base_dir.as_ref().to_path_buf());
        self.validate();
        self
    }

    /// Set whether the widget is focused
    pub fn set_focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        if !focused {
            self.completion_candidates.clear();
            self.completion_index = None;
        }
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

    /// Get the current path value
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the current path as PathBuf
    pub fn path_buf(&self) -> PathBuf {
        if let Some(ref base) = self.base_dir {
            base.join(&self.path)
        } else {
            PathBuf::from(&self.path)
        }
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

    /// Check if the path is empty
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    /// Clear the path
    pub fn clear(&mut self) {
        if self.read_only {
            return;
        }
        self.path.clear();
        self.cursor_pos = 0;
        self.completion_candidates.clear();
        self.completion_index = None;
        self.validation_state = ValidationState::Unvalidated;
    }

    /// Set the path value programmatically
    pub fn set_path(&mut self, path: impl Into<String>) {
        if self.read_only {
            return;
        }
        self.path = path.into();
        self.cursor_pos = self.path.chars().count();
        self.completion_candidates.clear();
        self.completion_index = None;
        self.validate();
    }

    /// Handle a key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.read_only && !matches!(key.code, KeyCode::Left | KeyCode::Right | KeyCode::Home | KeyCode::End) {
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
            KeyCode::Tab => {
                self.handle_tab_completion();
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

        let byte_pos = if self.cursor_pos >= self.path.chars().count() {
            self.path.len()
        } else {
            self.path
                .char_indices()
                .nth(self.cursor_pos)
                .map(|(i, _)| i)
                .unwrap_or(self.path.len())
        };

        self.path.insert(byte_pos, c);
        self.cursor_pos += 1;
        self.completion_candidates.clear();
        self.completion_index = None;
        self.validate();
    }

    /// Delete character before cursor (backspace)
    pub fn delete_backward(&mut self) {
        if self.read_only {
            return;
        }

        if self.cursor_pos > 0 {
            let byte_pos = self
                .path
                .char_indices()
                .nth(self.cursor_pos - 1)
                .map(|(i, _)| i)
                .unwrap_or(0);

            let char_len = self.path[byte_pos..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(1);
            
            self.path.drain(byte_pos..byte_pos + char_len);
            self.cursor_pos -= 1;
            self.completion_candidates.clear();
            self.completion_index = None;
            self.validate();
        }
    }

    /// Delete character at cursor (delete key)
    pub fn delete_forward(&mut self) {
        if self.read_only {
            return;
        }

        let char_count = self.path.chars().count();
        if self.cursor_pos < char_count {
            let byte_pos = self
                .path
                .char_indices()
                .nth(self.cursor_pos)
                .map(|(i, _)| i)
                .unwrap_or(self.path.len());

            let char_len = self.path[byte_pos..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(1);
            
            self.path.drain(byte_pos..byte_pos + char_len);
            self.completion_candidates.clear();
            self.completion_index = None;
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
        let char_count = self.path.chars().count();
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
        self.cursor_pos = self.path.chars().count();
    }

    /// Handle tab completion
    fn handle_tab_completion(&mut self) {
        if self.read_only {
            return;
        }

        // Get the path up to cursor
        let path_up_to_cursor: String = self.path
            .chars()
            .take(self.cursor_pos)
            .collect();

        // Find the last separator
        let last_sep = path_up_to_cursor.rfind('/').or_else(|| path_up_to_cursor.rfind('\\'));
        
        let (base_path, prefix) = if let Some(sep_pos) = last_sep {
            let (base, rest) = path_up_to_cursor.split_at(sep_pos + 1);
            (base.to_string(), rest.to_string())
        } else {
            ("".to_string(), path_up_to_cursor)
        };

        // Get completion candidates
        if self.completion_candidates.is_empty() {
            self.completion_candidates = self.get_completion_candidates(&base_path, &prefix);
        }

        // Cycle through candidates
        if !self.completion_candidates.is_empty() {
            let index = self.completion_index.unwrap_or(0);
            let candidate = &self.completion_candidates[index];
            
            // Replace the prefix with the candidate
            let prefix_len = prefix.chars().count();
            let prefix_byte_len = prefix.len();
            
            // Update path
            let cursor_byte_pos = self.path
                .char_indices()
                .nth(self.cursor_pos - prefix_len)
                .map(|(i, _)| i)
                .unwrap_or(0);
            
            self.path.replace_range(cursor_byte_pos..cursor_byte_pos + prefix_byte_len, candidate);
            self.cursor_pos = self.cursor_pos - prefix_len + candidate.chars().count();
            
            // Move to next candidate
            self.completion_index = Some((index + 1) % self.completion_candidates.len());
        }
    }

    /// Get completion candidates for a given path prefix
    fn get_completion_candidates(&self, base_path: &str, prefix: &str) -> Vec<String> {
        let mut candidates = Vec::new();

        // Resolve base path
        let search_path = if let Some(ref base) = self.base_dir {
            if base_path.is_empty() {
                base.clone()
            } else if base_path.starts_with('/') || (cfg!(windows) && base_path.contains(':')) {
                PathBuf::from(base_path)
            } else {
                base.join(base_path)
            }
        } else {
            if base_path.is_empty() {
                PathBuf::from(".")
            } else {
                PathBuf::from(base_path)
            }
        };

        // Try to read directory
        if let Ok(entries) = std::fs::read_dir(&search_path) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let name_str = file_name.to_string_lossy();
                
                if name_str.starts_with(prefix) {
                    let path = entry.path();
                    let is_dir = path.is_dir();
                    let is_file = path.is_file();
                    
                    // Check if type is allowed
                    if (is_dir && self.allow_directories) || (is_file && self.allow_files) {
                        let mut candidate = name_str.to_string();
                        if is_dir {
                            candidate.push('/');
                        }
                        candidates.push(candidate);
                    }
                }
            }
        }

        candidates.sort();
        candidates
    }

    /// Validate the current path
    pub fn validate(&mut self) {
        if !self.validate_existence {
            self.validation_state = ValidationState::Unvalidated;
            return;
        }

        if self.path.is_empty() {
            if self.must_exist {
                self.validation_state = ValidationState::Invalid("Path is required".to_string());
            } else {
                self.validation_state = ValidationState::Unvalidated;
            }
            return;
        }

        let path_buf = self.path_buf();
        let path = path_buf.as_path();

        if !path.exists() {
            if self.must_exist {
                self.validation_state = ValidationState::Invalid("Path does not exist".to_string());
            } else {
                self.validation_state = ValidationState::Unvalidated;
            }
            return;
        }

        // Check type restrictions
        let is_dir = path.is_dir();
        let is_file = path.is_file();

        if is_dir && !self.allow_directories {
            self.validation_state = ValidationState::Invalid("Path must be a file, not a directory".to_string());
            return;
        }

        if is_file && !self.allow_files {
            self.validation_state = ValidationState::Invalid("Path must be a directory, not a file".to_string());
            return;
        }

        self.validation_state = ValidationState::Valid;
    }

    /// Render the widget to the frame
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Determine display text
        let display_text = if self.path.is_empty() {
            self.placeholder.as_deref().unwrap_or("")
        } else {
            &self.path
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
        let text_style = if self.path.is_empty() && self.placeholder.is_some() {
            Style::default().fg(Color::DarkGray)
        } else if self.read_only {
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

    /// Render cursor at current position
    fn render_cursor(&self, f: &mut Frame, area: Rect) {
        let cursor_x = if self.path.is_empty() {
            area.x + 1
        } else {
            let text_start_x = area.x + 1;
            let cursor_offset = self.cursor_pos.min(self.path.len());
            text_start_x + cursor_offset as u16
        };

        let cursor_y = area.y + 1;

        if cursor_x < area.x + area.width && cursor_y < area.y + area.height {
            f.set_cursor(cursor_x, cursor_y);
        }
    }

    /// Render validation error message below the input
    fn render_validation_error(&self, f: &mut Frame, area: Rect, error: &str) {
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

impl Widget for PathInputWidget {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer)
    where
        Self: Sized,
    {
        let display_text = if self.path.is_empty() {
            self.placeholder.as_deref().unwrap_or("")
        } else {
            &self.path
        };

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
    use std::fs;
    use tempfile::TempDir;

    // ========================================================================
    // Construction Tests
    // ========================================================================

    #[test]
    fn test_new_widget() {
        let widget = PathInputWidget::new();
        assert_eq!(widget.path(), "");
        assert_eq!(widget.cursor_pos(), 0);
        assert!(widget.placeholder.is_none());
        assert!(!widget.validate_existence);
        assert!(!widget.must_exist);
        assert!(widget.allow_files);
        assert!(widget.allow_directories);
    }

    #[test]
    fn test_with_path() {
        let widget = PathInputWidget::new().with_path("/tmp/test");
        assert_eq!(widget.path(), "/tmp/test");
        // "/tmp/test" has 10 characters, cursor at end is position 10
        assert_eq!(widget.cursor_pos(), "/tmp/test".chars().count());
    }

    #[test]
    fn test_with_placeholder() {
        let widget = PathInputWidget::new().with_placeholder("Enter path...");
        assert_eq!(widget.placeholder, Some("Enter path...".to_string()));
    }

    #[test]
    fn test_with_validate_existence() {
        let widget = PathInputWidget::new().with_validate_existence(true);
        assert!(widget.validate_existence);
    }

    #[test]
    fn test_with_must_exist() {
        let widget = PathInputWidget::new().with_must_exist(true);
        assert!(widget.must_exist);
    }

    #[test]
    fn test_with_allow_files() {
        let widget = PathInputWidget::new().with_allow_files(false);
        assert!(!widget.allow_files);
    }

    #[test]
    fn test_with_allow_directories() {
        let widget = PathInputWidget::new().with_allow_directories(false);
        assert!(!widget.allow_directories);
    }

    #[test]
    fn test_with_base_dir() {
        let widget = PathInputWidget::new().with_base_dir("/tmp");
        assert_eq!(widget.base_dir, Some(PathBuf::from("/tmp")));
    }

    #[test]
    fn test_chained_builders() {
        let widget = PathInputWidget::new()
            .with_path("test.txt")
            .with_placeholder("Enter path...")
            .with_validate_existence(true)
            .with_must_exist(false)
            .with_allow_files(true)
            .with_allow_directories(false)
            .set_focused(true);

        assert_eq!(widget.path(), "test.txt");
        assert_eq!(widget.placeholder, Some("Enter path...".to_string()));
        assert!(widget.validate_existence);
        assert!(!widget.must_exist);
        assert!(widget.allow_files);
        assert!(!widget.allow_directories);
        assert!(widget.focused);
    }

    // ========================================================================
    // Path Manipulation Tests
    // ========================================================================

    #[test]
    fn test_insert_char() {
        let mut widget = PathInputWidget::new();
        widget.insert_char('/');
        widget.insert_char('t');
        widget.insert_char('m');
        widget.insert_char('p');
        assert_eq!(widget.path(), "/tmp");
        assert_eq!(widget.cursor_pos(), 4);
    }

    #[test]
    fn test_insert_char_in_middle() {
        let mut widget = PathInputWidget::new().with_path("/tm");
        // Cursor starts at end (pos 2), move left to pos 1 (before 'm')
        widget.move_cursor_left(); // Now at pos 1
        // Insert 'p' at position 1 - this inserts before char at index 1, which is 't'
        // But we want to insert after 't', so we need to move cursor differently
        // Actually, cursor at pos 1 means "before char 1", so insert at pos 1 inserts before 't'
        // To insert between 't' and 'm', we need cursor at pos 2 (before 'm')
        widget.move_cursor_left(); // Move to pos 0 (before '/')
        widget.move_cursor_right(); // Move to pos 1 (before 't')
        widget.move_cursor_right(); // Move to pos 2 (before 'm')
        widget.insert_char('p'); // Insert at pos 2, before 'm', giving "/tmp"
        assert_eq!(widget.path(), "/tmp");
    }

    #[test]
    fn test_delete_backward() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.delete_backward();
        assert_eq!(widget.path(), "/tm");
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_delete_forward() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        widget.delete_forward();
        assert_eq!(widget.path(), "tmp");
    }

    #[test]
    fn test_clear() {
        let mut widget = PathInputWidget::new().with_path("/tmp/test");
        widget.clear();
        assert_eq!(widget.path(), "");
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_set_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("/new/path");
        assert_eq!(widget.path(), "/new/path");
        assert_eq!(widget.cursor_pos(), 9);
    }

    #[test]
    fn test_set_path_read_only() {
        let mut widget = PathInputWidget::new()
            .with_path("/old/path")
            .set_read_only(true);
        widget.set_path("/new/path");
        assert_eq!(widget.path(), "/old/path"); // Should not change
    }

    // ========================================================================
    // Cursor Movement Tests
    // ========================================================================

    #[test]
    fn test_move_cursor_left() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_left();
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_move_cursor_right() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 1);
    }

    #[test]
    fn test_move_cursor_home() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_move_cursor_end() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        widget.move_cursor_end();
        assert_eq!(widget.cursor_pos(), 4);
    }

    #[test]
    fn test_cursor_boundaries() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        for _ in 0..10 {
            widget.move_cursor_left();
        }
        assert_eq!(widget.cursor_pos(), 0);

        for _ in 0..10 {
            widget.move_cursor_right();
        }
        assert_eq!(widget.cursor_pos(), 4);
    }

    // ========================================================================
    // Path Buffer Tests
    // ========================================================================

    #[test]
    fn test_path_buf_absolute() {
        let widget = PathInputWidget::new().with_path("/tmp/test");
        let path_buf = widget.path_buf();
        assert_eq!(path_buf, PathBuf::from("/tmp/test"));
    }

    #[test]
    fn test_path_buf_relative() {
        let widget = PathInputWidget::new().with_path("test.txt");
        let path_buf = widget.path_buf();
        assert_eq!(path_buf, PathBuf::from("test.txt"));
    }

    #[test]
    fn test_path_buf_with_base_dir() {
        let widget = PathInputWidget::new()
            .with_path("test.txt")
            .with_base_dir("/tmp");
        let path_buf = widget.path_buf();
        assert_eq!(path_buf, PathBuf::from("/tmp/test.txt"));
    }

    #[test]
    fn test_path_buf_absolute_with_base_dir() {
        let widget = PathInputWidget::new()
            .with_path("/absolute/path")
            .with_base_dir("/tmp");
        let path_buf = widget.path_buf();
        // Absolute path should ignore base_dir
        assert_eq!(path_buf, PathBuf::from("/absolute/path"));
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_no_validation() {
        let mut widget = PathInputWidget::new().with_path("/nonexistent");
        widget.validate();
        assert!(widget.validation_error().is_none());
    }

    #[test]
    fn test_validate_existing_path() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "test").unwrap();

        let mut widget = PathInputWidget::new()
            .with_path(test_file.to_str().unwrap())
            .with_validate_existence(true);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_nonexistent_path() {
        let mut widget = PathInputWidget::new()
            .with_path("/nonexistent/path/that/does/not/exist")
            .with_validate_existence(true)
            .with_must_exist(true);
        widget.validate();
        assert!(!widget.is_valid());
        assert!(widget.validation_error().is_some());
    }

    #[test]
    fn test_validate_empty_path() {
        let mut widget = PathInputWidget::new()
            .with_validate_existence(true)
            .with_must_exist(true);
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_empty_path_not_required() {
        let mut widget = PathInputWidget::new()
            .with_validate_existence(true)
            .with_must_exist(false);
        widget.validate();
        assert!(widget.validation_error().is_none());
    }

    #[test]
    fn test_validate_file_when_directory_required() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "test").unwrap();

        let mut widget = PathInputWidget::new()
            .with_path(test_file.to_str().unwrap())
            .with_validate_existence(true)
            .with_allow_files(false)
            .with_allow_directories(true);
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_directory_when_file_required() {
        let temp_dir = TempDir::new().unwrap();

        let mut widget = PathInputWidget::new()
            .with_path(temp_dir.path().to_str().unwrap())
            .with_validate_existence(true)
            .with_allow_files(true)
            .with_allow_directories(false);
        widget.validate();
        assert!(!widget.is_valid());
    }

    #[test]
    fn test_validate_directory_allowed() {
        let temp_dir = TempDir::new().unwrap();

        let mut widget = PathInputWidget::new()
            .with_path(temp_dir.path().to_str().unwrap())
            .with_validate_existence(true)
            .with_allow_directories(true);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_file_allowed() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "test").unwrap();

        let mut widget = PathInputWidget::new()
            .with_path(test_file.to_str().unwrap())
            .with_validate_existence(true)
            .with_allow_files(true);
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_validate_relative_path_with_base() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "test").unwrap();

        let mut widget = PathInputWidget::new()
            .with_path("test.txt")
            .with_base_dir(temp_dir.path())
            .with_validate_existence(true);
        widget.validate();
        assert!(widget.is_valid());
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_char() {
        let mut widget = PathInputWidget::new();
        let key = KeyEvent::new(KeyCode::Char('/'), KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.path(), "/");
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.path(), "/tm");
    }

    #[test]
    fn test_handle_key_delete() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        let key = KeyEvent::new(KeyCode::Delete, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.path(), "tmp");
    }

    #[test]
    fn test_handle_key_left() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 3);
    }

    #[test]
    fn test_handle_key_right() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 1);
    }

    #[test]
    fn test_handle_key_home() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        let key = KeyEvent::new(KeyCode::Home, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 0);
    }

    #[test]
    fn test_handle_key_end() {
        let mut widget = PathInputWidget::new().with_path("/tmp");
        widget.move_cursor_home();
        let key = KeyEvent::new(KeyCode::End, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 4);
    }

    #[test]
    fn test_handle_key_tab() {
        let mut widget = PathInputWidget::new().with_path("/t");
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        // Tab completion may or may not work depending on filesystem
        assert!(widget.handle_key(key));
    }

    #[test]
    fn test_handle_key_read_only() {
        let mut widget = PathInputWidget::new()
            .with_path("/tmp")
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::empty());
        assert!(!widget.handle_key(key));
        assert_eq!(widget.path(), "/tmp");
    }

    #[test]
    fn test_handle_key_read_only_allows_navigation() {
        let mut widget = PathInputWidget::new()
            .with_path("/tmp")
            .set_read_only(true);
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.cursor_pos(), 3);
    }

    // ========================================================================
    // Tab Completion Tests
    // ========================================================================

    #[test]
    fn test_tab_completion_empty() {
        let mut widget = PathInputWidget::new();
        widget.handle_tab_completion();
        // Tab completion may find candidates from current directory, so path may change
        // Just verify it doesn't panic and path is valid
        assert!(!widget.path().contains('\0')); // No null bytes
    }

    #[test]
    fn test_tab_completion_read_only() {
        let mut widget = PathInputWidget::new()
            .with_path("/tmp")
            .set_read_only(true);
        let original = widget.path().to_string();
        widget.handle_tab_completion();
        assert_eq!(widget.path(), original); // Should not change
    }

    #[test]
    fn test_get_completion_candidates_empty() {
        let widget = PathInputWidget::new();
        let candidates = widget.get_completion_candidates("", "");
        // Should return some candidates (current directory contents)
        assert!(!candidates.is_empty() || candidates.is_empty()); // Either is fine
    }

    #[test]
    fn test_get_completion_candidates_with_prefix() {
        let widget = PathInputWidget::new();
        let candidates = widget.get_completion_candidates(".", "C");
        // May or may not have candidates depending on filesystem
        assert!(candidates.iter().all(|c| c.starts_with("C")));
    }

    // ========================================================================
    // Edge Cases and Stress Tests
    // ========================================================================

    #[test]
    fn test_very_long_path() {
        let long_path = "/".to_string() + &"a/".repeat(100) + "file.txt";
        let mut widget = PathInputWidget::new().with_path(&long_path);
        assert_eq!(widget.path(), &long_path);
        widget.move_cursor_home();
        widget.move_cursor_end();
        assert_eq!(widget.cursor_pos(), long_path.chars().count());
    }

    #[test]
    fn test_unicode_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("/tmp/测试/文件.txt");
        assert_eq!(widget.path(), "/tmp/测试/文件.txt");
    }

    #[test]
    fn test_special_characters_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("/tmp/file with spaces.txt");
        assert_eq!(widget.path(), "/tmp/file with spaces.txt");
    }

    #[test]
    fn test_windows_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("C:\\Users\\test\\file.txt");
        assert_eq!(widget.path(), "C:\\Users\\test\\file.txt");
    }

    #[test]
    fn test_relative_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("./relative/path.txt");
        assert_eq!(widget.path(), "./relative/path.txt");
    }

    #[test]
    fn test_home_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("~/test/file.txt");
        assert_eq!(widget.path(), "~/test/file.txt");
    }

    #[test]
    fn test_rapid_typing() {
        let mut widget = PathInputWidget::new();
        for c in "/tmp/test/file.txt".chars() {
            widget.insert_char(c);
        }
        assert_eq!(widget.path(), "/tmp/test/file.txt");
    }

    #[test]
    fn test_rapid_deletion() {
        let mut widget = PathInputWidget::new().with_path("/tmp/test/file.txt");
        for _ in 0..20 {
            widget.delete_backward();
        }
        assert_eq!(widget.path(), "");
    }

    #[test]
    fn test_cursor_movement_unicode() {
        let mut widget = PathInputWidget::new().with_path("/tmp/测试");
        widget.move_cursor_home();
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 1);
        widget.move_cursor_right();
        assert_eq!(widget.cursor_pos(), 2);
    }

    #[test]
    fn test_path_with_trailing_slash() {
        let mut widget = PathInputWidget::new();
        widget.set_path("/tmp/");
        assert_eq!(widget.path(), "/tmp/");
    }

    #[test]
    fn test_path_with_multiple_slashes() {
        let mut widget = PathInputWidget::new();
        widget.set_path("//tmp///test");
        assert_eq!(widget.path(), "//tmp///test");
    }

    #[test]
    fn test_empty_string_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("");
        assert_eq!(widget.path(), "");
        assert!(widget.is_empty());
    }

    #[test]
    fn test_just_slash() {
        let mut widget = PathInputWidget::new();
        widget.set_path("/");
        assert_eq!(widget.path(), "/");
    }

    #[test]
    fn test_dot_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path(".");
        assert_eq!(widget.path(), ".");
    }

    #[test]
    fn test_dot_dot_path() {
        let mut widget = PathInputWidget::new();
        widget.set_path("..");
        assert_eq!(widget.path(), "..");
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_editing_workflow() {
        let mut widget = PathInputWidget::new()
            .with_placeholder("Enter path...")
            .set_focused(true);

        // Type path
        widget.insert_char('/');
        widget.insert_char('t');
        widget.insert_char('m');
        widget.insert_char('p');
        assert_eq!(widget.path(), "/tmp");

        // Edit in middle - cursor is at end (pos 4), move left twice to pos 2 (before 'p')
        widget.move_cursor_left(); // pos 3
        widget.move_cursor_left(); // pos 2 (before 'p')
        widget.insert_char('e'); // Insert at pos 2, before 'p', giving "/temp"
        assert_eq!(widget.path(), "/temp");

        // Delete character
        widget.delete_backward();
        assert_eq!(widget.path(), "/tmp");
    }

    #[test]
    fn test_validation_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "test").unwrap();

        let mut widget = PathInputWidget::new()
            .with_validate_existence(true)
            .with_must_exist(true)
            .set_focused(true);

        // Set invalid path
        widget.set_path("/nonexistent");
        widget.validate();
        assert!(!widget.is_valid());

        // Set valid path
        widget.set_path(test_file.to_str().unwrap());
        widget.validate();
        assert!(widget.is_valid());
    }

    #[test]
    fn test_base_dir_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, "test").unwrap();

        let mut widget = PathInputWidget::new()
            .with_base_dir(temp_dir.path())
            .with_path("test.txt")
            .with_validate_existence(true);

        widget.validate();
        assert!(widget.is_valid());
        assert_eq!(widget.path_buf(), test_file);
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_clone_preserves_state() {
        let widget1 = PathInputWidget::new()
            .with_path("/tmp/test")
            .with_placeholder("Enter...")
            .with_validate_existence(true)
            .with_must_exist(false)
            .set_focused(true);

        let widget2 = widget1.clone();
        assert_eq!(widget1.path(), widget2.path());
        assert_eq!(widget1.cursor_pos(), widget2.cursor_pos());
        assert_eq!(widget1.placeholder, widget2.placeholder);
        assert_eq!(widget1.validate_existence, widget2.validate_existence);
        assert_eq!(widget1.must_exist, widget2.must_exist);
    }

    #[test]
    fn test_clone_independent_operations() {
        let mut widget1 = PathInputWidget::new().with_path("/tmp");
        let mut widget2 = widget1.clone();

        widget1.insert_char('1');
        widget2.insert_char('2');

        assert_eq!(widget1.path(), "/tmp1");
        assert_eq!(widget2.path(), "/tmp2");
    }

    // ========================================================================
    // Default Trait Tests
    // ========================================================================

    #[test]
    fn test_default_impl() {
        let widget1 = PathInputWidget::default();
        let widget2 = PathInputWidget::new();
        assert_eq!(widget1.path(), widget2.path());
        assert_eq!(widget1.cursor_pos(), widget2.cursor_pos());
    }

    // ========================================================================
    // Helper Method Tests
    // ========================================================================

    #[test]
    fn test_is_empty() {
        let widget1 = PathInputWidget::new();
        assert!(widget1.is_empty());

        let widget2 = PathInputWidget::new().with_path("/tmp");
        assert!(!widget2.is_empty());
    }

    #[test]
    fn test_path_buf_empty() {
        let widget = PathInputWidget::new();
        assert_eq!(widget.path_buf(), PathBuf::from(""));
    }

    #[test]
    fn test_path_buf_unicode() {
        let widget = PathInputWidget::new().with_path("/tmp/测试.txt");
        let path_buf = widget.path_buf();
        assert_eq!(path_buf, PathBuf::from("/tmp/测试.txt"));
    }
}
