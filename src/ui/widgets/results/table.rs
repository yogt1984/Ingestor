//! Table Widget (T-3.2)
//!
//! A reusable sortable, scrollable table widget for displaying tabular data in the TUI.
//! Supports column sorting, row scrolling, row selection, and optional column resizing.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Row, Table, TableState, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::cmp::Ordering;
use std::fmt::Display;

// ============================================================================
// Types
// ============================================================================

/// Table widget for displaying sortable, scrollable tabular data
pub struct TableWidget {
    /// Column headers
    headers: Vec<TableHeader>,
    /// Table rows (each row is a vector of cells)
    rows: Vec<TableRow>,
    /// Current sort column index (None = no sorting)
    sort_column: Option<usize>,
    /// Current sort direction
    sort_direction: SortDirection,
    /// Table state for rendering and selection
    state: TableState,
    /// Currently selected row index (None = no selection)
    selected_row: Option<usize>,
    /// Scroll offset (number of rows scrolled)
    scroll_offset: usize,
    /// Whether the widget is focused
    focused: bool,
    /// Block style (optional title, borders)
    block: Option<Block<'static>>,
    /// Column widths (None = auto-calculate)
    column_widths: Option<Vec<u16>>,
    /// Minimum column width
    min_column_width: u16,
    /// Maximum visible rows
    max_visible_rows: usize,
}

impl Clone for TableWidget {
    fn clone(&self) -> Self {
        Self {
            headers: self.headers.clone(),
            rows: self.rows.clone(),
            sort_column: self.sort_column,
            sort_direction: self.sort_direction.clone(),
            state: TableState::default(),
            selected_row: self.selected_row,
            scroll_offset: 0, // Reset scroll on clone
            focused: self.focused,
            block: self.block.clone(),
            column_widths: self.column_widths.clone(),
            min_column_width: self.min_column_width,
            max_visible_rows: self.max_visible_rows,
        }
    }
}

impl std::fmt::Debug for TableWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableWidget")
            .field("headers", &self.headers)
            .field("rows_count", &self.rows.len())
            .field("sort_column", &self.sort_column)
            .field("sort_direction", &self.sort_direction)
            .field("selected_row", &self.selected_row)
            .field("scroll_offset", &self.scroll_offset)
            .field("focused", &self.focused)
            .finish()
    }
}

/// Table column header
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableHeader {
    /// Column name/title
    pub name: String,
    /// Column width (None = auto)
    pub width: Option<u16>,
    /// Text alignment
    pub align: Alignment,
    /// Whether column is sortable
    pub sortable: bool,
}

impl TableHeader {
    /// Create a new header
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            width: None,
            align: Alignment::Left,
            sortable: true,
        }
    }

    /// Set column width
    pub fn with_width(mut self, width: u16) -> Self {
        self.width = Some(width);
        self
    }

    /// Set text alignment
    pub fn with_align(mut self, align: Alignment) -> Self {
        self.align = align;
        self
    }

    /// Set whether column is sortable
    pub fn with_sortable(mut self, sortable: bool) -> Self {
        self.sortable = sortable;
        self
    }

    /// Get whether column is sortable (getter for tests)
    pub fn sortable(&self) -> bool {
        self.sortable
    }

    /// Get column width (getter for tests)
    pub fn width(&self) -> Option<u16> {
        self.width
    }
}

/// Table row (vector of cell strings)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    /// Cell values (as strings for display)
    pub cells: Vec<String>,
    /// Optional row data for sorting/comparison
    pub data: Option<serde_json::Value>,
}

impl TableRow {
    /// Create a new row from cell strings
    pub fn new(cells: Vec<impl Into<String>>) -> Self {
        Self {
            cells: cells.into_iter().map(|c| c.into()).collect(),
            data: None,
        }
    }

    /// Create a new row with data
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Get cell value at index
    pub fn get_cell(&self, index: usize) -> Option<&str> {
        self.cells.get(index).map(|s| s.as_str())
    }

    /// Get number of cells
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Check if row is empty
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Get all cells (for backwards compatibility with tests)
    pub fn cells(&self) -> &[String] {
        &self.cells
    }
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending (A-Z, 0-9)
    Ascending,
    /// Descending (Z-A, 9-0)
    Descending,
}

impl SortDirection {
    /// Toggle sort direction
    pub fn toggle(self) -> Self {
        match self {
            Self::Ascending => Self::Descending,
            Self::Descending => Self::Ascending,
        }
    }
}

// ============================================================================
// TableWidget Implementation
// ============================================================================

impl Default for TableWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl TableWidget {
    /// Create a new empty table widget
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            rows: Vec::new(),
            sort_column: None,
            sort_direction: SortDirection::Ascending,
            state: TableState::default(),
            selected_row: None,
            scroll_offset: 0,
            focused: false,
            block: None,
            column_widths: None,
            min_column_width: 5,
            max_visible_rows: 20,
        }
    }

    /// Set column headers
    pub fn with_headers(mut self, headers: Vec<TableHeader>) -> Self {
        self.headers = headers;
        self.validate_headers();
        self
    }

    /// Set table rows
    pub fn with_rows(mut self, rows: Vec<TableRow>) -> Self {
        self.rows = rows;
        self.validate_rows();
        self.apply_sorting();
        self
    }

    /// Add a row
    pub fn add_row(&mut self, row: TableRow) {
        if self.validate_row(&row) {
            self.rows.push(row);
            self.apply_sorting();
        }
    }

    /// Clear all rows
    pub fn clear_rows(&mut self) {
        self.rows.clear();
        self.selected_row = None;
        self.scroll_offset = 0;
    }

    /// Set block (title, borders)
    pub fn with_block(mut self, block: Block<'static>) -> Self {
        self.block = Some(block);
        self
    }

    /// Set column widths
    pub fn with_column_widths(mut self, widths: Vec<u16>) -> Self {
        if widths.len() == self.headers.len() {
            self.column_widths = Some(widths);
        }
        self
    }

    /// Set minimum column width
    pub fn with_min_column_width(mut self, width: u16) -> Self {
        self.min_column_width = width;
        self
    }

    /// Set maximum visible rows
    pub fn with_max_visible_rows(mut self, max: usize) -> Self {
        self.max_visible_rows = max;
        self
    }

    /// Set focus state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Get focus state
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Get number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get number of columns
    pub fn column_count(&self) -> usize {
        self.headers.len()
    }

    /// Get currently selected row index
    pub fn selected_row(&self) -> Option<usize> {
        self.selected_row
    }

    /// Get currently selected row
    pub fn get_selected_row(&self) -> Option<&TableRow> {
        self.selected_row.and_then(|idx| self.rows.get(idx))
    }

    /// Get sort column
    pub fn sort_column(&self) -> Option<usize> {
        self.sort_column
    }

    /// Get sort direction
    pub fn sort_direction(&self) -> SortDirection {
        self.sort_direction
    }

    /// Handle keyboard input
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !self.focused {
            return false;
        }

        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_selection_up();
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_selection_down();
                true
            }
            KeyCode::PageUp => {
                self.page_up();
                true
            }
            KeyCode::PageDown => {
                self.page_down();
                true
            }
            KeyCode::Home => {
                self.move_to_first();
                true
            }
            KeyCode::End => {
                self.move_to_last();
                true
            }
            KeyCode::Tab => {
                // Sort by next column
                self.sort_next_column();
                true
            }
            KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Toggle sort direction
                self.toggle_sort_direction();
                true
            }
            _ => false,
        }
    }

    /// Sort by column index (click header)
    pub fn sort_by_column(&mut self, column_index: usize) {
        if column_index >= self.headers.len() {
            return;
        }

        if !self.headers[column_index].sortable {
            return;
        }

        // Toggle direction if already sorting this column
        if self.sort_column == Some(column_index) {
            self.sort_direction = self.sort_direction.toggle();
        } else {
            self.sort_column = Some(column_index);
            self.sort_direction = SortDirection::Ascending;
        }

        self.apply_sorting();
    }

    /// Sort by next column
    fn sort_next_column(&mut self) {
        if self.headers.is_empty() {
            return;
        }

        let next = match self.sort_column {
            Some(current) => {
                // Find next sortable column
                let mut next = (current + 1) % self.headers.len();
                let mut attempts = 0;
                while next != current && !self.headers[next].sortable && attempts < self.headers.len() {
                    next = (next + 1) % self.headers.len();
                    attempts += 1;
                }
                if !self.headers[next].sortable {
                    // If no sortable column found, use first
                    self.headers
                        .iter()
                        .position(|h| h.sortable)
                        .unwrap_or(0)
                } else {
                    next
                }
            }
            None => {
                // Find first sortable column
                self.headers
                    .iter()
                    .position(|h| h.sortable)
                    .unwrap_or(0)
            }
        };

        self.sort_by_column(next);
    }

    /// Toggle sort direction
    fn toggle_sort_direction(&mut self) {
        if self.sort_column.is_some() {
            self.sort_direction = self.sort_direction.toggle();
            self.apply_sorting();
        }
    }

    /// Apply current sorting to rows
    fn apply_sorting(&mut self) {
        if self.sort_column.is_none() || self.rows.is_empty() {
            return;
        }

        let column = self.sort_column.unwrap();
        let direction = self.sort_direction;

        self.rows.sort_by(|a, b| {
            let a_val = a.get_cell(column).unwrap_or("");
            let b_val = b.get_cell(column).unwrap_or("");

            let cmp = Self::compare_cell_values(a_val, b_val);
            match direction {
                SortDirection::Ascending => cmp,
                SortDirection::Descending => cmp.reverse(),
            }
        });

        // Adjust selected row after sorting
        if let Some(selected) = self.selected_row {
            if selected >= self.rows.len() {
                self.selected_row = Some(self.rows.len().saturating_sub(1));
            }
        }
    }

    /// Compare two cell values (smart comparison: numbers vs strings)
    fn compare_cell_values(a: &str, b: &str) -> Ordering {
        // Try to parse as numbers
        if let (Ok(a_num), Ok(b_num)) = (a.parse::<f64>(), b.parse::<f64>()) {
            return a_num.partial_cmp(&b_num).unwrap_or(Ordering::Equal);
        }

        // Try to parse as integers
        if let (Ok(a_int), Ok(b_int)) = (a.parse::<i64>(), b.parse::<i64>()) {
            return a_int.cmp(&b_int);
        }

        // String comparison
        a.cmp(b)
    }

    /// Move selection up
    fn move_selection_up(&mut self) {
        match self.selected_row {
            Some(current) if current > 0 => {
                self.selected_row = Some(current - 1);
                self.update_scroll();
            }
            Some(_) => {
                // Already at top
            }
            None if !self.rows.is_empty() => {
                self.selected_row = Some(0);
            }
            None => {}
        }
    }

    /// Move selection down
    fn move_selection_down(&mut self) {
        match self.selected_row {
            Some(current) => {
                if current < self.rows.len().saturating_sub(1) {
                    self.selected_row = Some(current + 1);
                    self.update_scroll();
                } else if current >= self.rows.len() {
                    // Clamp to last row if out of bounds
                    if !self.rows.is_empty() {
                        self.selected_row = Some(self.rows.len() - 1);
                        self.update_scroll();
                    }
                }
                // Already at bottom, do nothing
            }
            None if !self.rows.is_empty() => {
                self.selected_row = Some(0);
            }
            None => {}
        }
    }

    /// Page up
    fn page_up(&mut self) {
        let page_size = self.max_visible_rows;
        match self.selected_row {
            Some(current) => {
                let new = current.saturating_sub(page_size);
                self.selected_row = Some(new);
                self.update_scroll();
            }
            None if !self.rows.is_empty() => {
                self.selected_row = Some(0);
            }
            None => {}
        }
    }

    /// Page down
    fn page_down(&mut self) {
        let page_size = self.max_visible_rows;
        match self.selected_row {
            Some(current) => {
                let new = (current + page_size).min(self.rows.len().saturating_sub(1));
                self.selected_row = Some(new);
                self.update_scroll();
            }
            None if !self.rows.is_empty() => {
                self.selected_row = Some(0);
            }
            None => {}
        }
    }

    /// Move to first row
    fn move_to_first(&mut self) {
        if !self.rows.is_empty() {
            self.selected_row = Some(0);
            self.update_scroll();
        }
    }

    /// Move to last row
    fn move_to_last(&mut self) {
        if !self.rows.is_empty() {
            self.selected_row = Some(self.rows.len() - 1);
            self.update_scroll();
        }
    }

    /// Update scroll offset to keep selected row visible
    fn update_scroll(&mut self) {
        if let Some(selected) = self.selected_row {
            let visible_height = self.max_visible_rows;
            let current_scroll = self.scroll_offset;

            // If selected row is above visible area, scroll up
            if selected < current_scroll {
                self.scroll_offset = selected;
            }
            // If selected row is below visible area, scroll down
            else if selected >= current_scroll + visible_height {
                self.scroll_offset = selected.saturating_sub(visible_height - 1);
            }
        }
    }

    /// Calculate column widths
    fn calculate_column_widths(&self, available_width: u16) -> Vec<u16> {
        let num_columns = self.headers.len();
        if num_columns == 0 {
            return Vec::new();
        }

        // If custom widths provided, use them
        if let Some(ref widths) = self.column_widths {
            if widths.len() == num_columns {
                return widths.clone();
            }
        }

        // Calculate based on header names and content
        let mut widths = vec![0u16; num_columns];

        // Start with header widths
        for (i, header) in self.headers.iter().enumerate() {
            widths[i] = header.name.len() as u16;
            if let Some(w) = header.width {
                widths[i] = w;
            }
        }

        // Expand based on content
        for row in &self.rows {
            for (i, cell) in row.cells.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.len() as u16);
                }
            }
        }

        // Apply minimum width
        for width in &mut widths {
            *width = (*width).max(self.min_column_width);
        }

        // Distribute remaining space if total exceeds available
        let total: u16 = widths.iter().sum();
        if total > available_width {
            // Proportionally reduce
            let ratio = available_width as f64 / total as f64;
            for width in &mut widths {
                *width = (*width as f64 * ratio) as u16;
                *width = (*width).max(self.min_column_width);
            }
        }

        widths
    }

    /// Validate headers
    fn validate_headers(&self) {
        // Headers should have unique names (warning only, not enforced)
        let names: Vec<&str> = self.headers.iter().map(|h| h.name.as_str()).collect();
        let unique_names: std::collections::HashSet<&str> = names.iter().copied().collect();
        if unique_names.len() != names.len() {
            eprintln!("Warning: Table has duplicate header names");
        }
    }

    /// Validate rows
    fn validate_rows(&self) {
        let expected_cols = self.headers.len();
        for (i, row) in self.rows.iter().enumerate() {
            if row.cells.len() != expected_cols {
                eprintln!(
                    "Warning: Row {} has {} cells, expected {}",
                    i,
                    row.cells.len(),
                    expected_cols
                );
            }
        }
    }

    /// Validate a single row
    fn validate_row(&self, row: &TableRow) -> bool {
        row.cells.len() == self.headers.len()
    }

    /// Export to CSV format
    pub fn to_csv(&self) -> String {
        let mut csv = String::new();

        // Headers
        let header_line: Vec<String> = self.headers.iter().map(|h| escape_csv(&h.name)).collect();
        csv.push_str(&header_line.join(","));
        csv.push('\n');

        // Rows
        for row in &self.rows {
            let row_line: Vec<String> = row.cells.iter().map(|c| escape_csv(c)).collect();
            csv.push_str(&row_line.join(","));
            csv.push('\n');
        }

        csv
    }

    /// Render the table widget
    pub fn render(&mut self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if self.headers.is_empty() {
            return;
        }

        // Calculate column widths
        let available_width = area.width.saturating_sub(2); // Account for borders
        let column_widths = self.calculate_column_widths(available_width);

        // Create constraints
        let constraints: Vec<Constraint> = column_widths
            .iter()
            .map(|&w| Constraint::Length(w))
            .collect();

        // Build header cells with sort indicators
        let header_cells: Vec<Cell> = self
            .headers
            .iter()
            .enumerate()
            .map(|(i, header)| {
                let mut text = header.name.clone();
                if header.sortable {
                    if self.sort_column == Some(i) {
                        let indicator = match self.sort_direction {
                            SortDirection::Ascending => " ▲",
                            SortDirection::Descending => " ▼",
                        };
                        text.push_str(indicator);
                    }
                }

                let style = if self.focused && self.sort_column == Some(i) {
                    Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
                } else {
                    Style::default()
                };

                Cell::from(text).style(style)
            })
            .collect();

        let header = Row::new(header_cells).height(1);

        // Build data rows
        let visible_start = self.scroll_offset;
        let visible_end = (visible_start + self.max_visible_rows).min(self.rows.len());
        let visible_rows: Vec<Row> = self.rows[visible_start..visible_end]
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let row_index = visible_start + i;
                let is_selected = self.selected_row == Some(row_index);

                let style = if is_selected && self.focused {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::White)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                let cells: Vec<Cell> = row
                    .cells
                    .iter()
                    .map(|cell| Cell::from(cell.as_str()).style(style))
                    .collect();

                Row::new(cells).height(1)
            })
            .collect();

        // Create table
        let mut table = Table::new(visible_rows, &constraints)
            .header(header)
            .column_spacing(1);

        // Add block if provided
        if let Some(ref block) = self.block {
            table = table.block(block.clone());
        } else {
            table = table.block(Block::default().borders(Borders::ALL));
        }

        // Update table state for selection
        if let Some(selected) = self.selected_row {
            if selected >= visible_start && selected < visible_end {
                self.state.select(Some(selected - visible_start));
            } else {
                self.state.select(None);
            }
        } else {
            self.state.select(None);
        }

        // Render
        Widget::render(table, area, buf);
    }
}

/// Escape CSV field
fn escape_csv(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn create_test_table() -> TableWidget {
        let headers = vec![
            TableHeader::new("Name").with_sortable(true),
            TableHeader::new("Age").with_sortable(true),
            TableHeader::new("Score").with_sortable(true),
        ];

        let rows = vec![
            TableRow::new(vec!["Alice", "25", "95.5"]),
            TableRow::new(vec!["Bob", "30", "87.0"]),
            TableRow::new(vec!["Charlie", "22", "92.3"]),
        ];

        TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
    }

    #[test]
    fn test_table_creation() {
        let table = TableWidget::new();
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.column_count(), 0);
        assert_eq!(table.selected_row(), None);
    }

    #[test]
    fn test_table_with_headers_and_rows() {
        let table = create_test_table();
        assert_eq!(table.column_count(), 3);
        assert_eq!(table.row_count(), 3);
    }

    #[test]
    fn test_table_header_creation() {
        let header = TableHeader::new("Test");
        assert_eq!(header.name, "Test");
        assert_eq!(header.sortable, true);
        assert_eq!(header.align, Alignment::Left);

        let header2 = TableHeader::new("Test2")
            .with_width(20)
            .with_align(Alignment::Right)
            .with_sortable(false);
        assert_eq!(header2.width, Some(20));
        assert_eq!(header2.align, Alignment::Right);
        assert_eq!(header2.sortable, false);
    }

    #[test]
    fn test_table_row_creation() {
        let row = TableRow::new(vec!["A", "B", "C"]);
        assert_eq!(row.len(), 3);
        assert_eq!(row.get_cell(0), Some("A"));
        assert_eq!(row.get_cell(1), Some("B"));
        assert_eq!(row.get_cell(2), Some("C"));
        assert_eq!(row.get_cell(3), None);
    }

    #[test]
    fn test_table_row_with_data() {
        let data = serde_json::json!({"id": 1, "name": "test"});
        let row = TableRow::new(vec!["test"]).with_data(data.clone());
        assert_eq!(row.data, Some(data));
    }

    #[test]
    fn test_sort_by_column() {
        let mut table = create_test_table();
        
        // Sort by name (ascending)
        table.sort_by_column(0);
        assert_eq!(table.sort_column(), Some(0));
        assert_eq!(table.sort_direction(), SortDirection::Ascending);
        
        let first_row = table.rows.first().unwrap();
        assert_eq!(first_row.get_cell(0), Some("Alice"));
    }

    #[test]
    fn test_sort_toggle_direction() {
        let mut table = create_test_table();
        
        // Sort by name ascending
        table.sort_by_column(0);
        assert_eq!(table.sort_direction(), SortDirection::Ascending);
        
        // Toggle to descending
        table.sort_by_column(0);
        assert_eq!(table.sort_direction(), SortDirection::Descending);
        
        let first_row = table.rows.first().unwrap();
        assert_eq!(first_row.get_cell(0), Some("Charlie"));
    }

    #[test]
    fn test_sort_numeric_column() {
        let mut table = create_test_table();
        
        // Sort by age (numeric)
        table.sort_by_column(1);
        assert_eq!(table.sort_column(), Some(1));
        
        let first_row = table.rows.first().unwrap();
        assert_eq!(first_row.get_cell(1), Some("22")); // Charlie, 22
    }

    #[test]
    fn test_sort_float_column() {
        let mut table = create_test_table();
        
        // Sort by score (float)
        table.sort_by_column(2);
        assert_eq!(table.sort_column(), Some(2));
        
        let first_row = table.rows.first().unwrap();
        assert_eq!(first_row.get_cell(2), Some("87.0")); // Bob, 87.0
    }

    #[test]
    fn test_sort_unsortable_column() {
        let mut table = create_test_table();
        let original_order: Vec<String> = table.rows.iter().map(|r| r.get_cell(0).unwrap().to_string()).collect();
        
        table.headers[0].sortable = false;
        table.sort_by_column(0);
        
        // Should not sort
        let new_order: Vec<String> = table.rows.iter().map(|r| r.get_cell(0).unwrap().to_string()).collect();
        assert_eq!(original_order, new_order);
    }

    #[test]
    fn test_move_selection_up() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        // Start at row 1
        table.selected_row = Some(1);
        table.move_selection_up();
        assert_eq!(table.selected_row(), Some(0));
        
        // At top, should stay
        table.move_selection_up();
        assert_eq!(table.selected_row(), Some(0));
    }

    #[test]
    fn test_move_selection_down() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        // Start at row 0
        table.selected_row = Some(0);
        table.move_selection_down();
        assert_eq!(table.selected_row(), Some(1));
        
        // At bottom, should stay
        table.selected_row = Some(2);
        table.move_selection_down();
        assert_eq!(table.selected_row(), Some(2));
    }

    #[test]
    fn test_move_selection_from_none() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        // No selection, move down should select first
        table.move_selection_down();
        assert_eq!(table.selected_row(), Some(0));
        
        // Reset and move up should select first
        table.selected_row = None;
        table.move_selection_up();
        assert_eq!(table.selected_row(), Some(0));
    }

    #[test]
    fn test_page_up() {
        let mut table = create_test_table().with_max_visible_rows(2);
        table.set_focused(true);
        
        table.selected_row = Some(2);
        table.page_up();
        assert_eq!(table.selected_row(), Some(0));
    }

    #[test]
    fn test_page_down() {
        let mut table = create_test_table().with_max_visible_rows(2);
        table.set_focused(true);
        
        table.selected_row = Some(0);
        table.page_down();
        assert_eq!(table.selected_row(), Some(2));
    }

    #[test]
    fn test_move_to_first() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        table.selected_row = Some(2);
        table.move_to_first();
        assert_eq!(table.selected_row(), Some(0));
    }

    #[test]
    fn test_move_to_last() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        table.selected_row = Some(0);
        table.move_to_last();
        assert_eq!(table.selected_row(), Some(2));
    }

    #[test]
    fn test_scroll_offset_update() {
        let mut table = create_test_table();
        table.max_visible_rows = 2;
        table.set_focused(true);
        
        // Select row 2, should scroll
        table.selected_row = Some(2);
        table.update_scroll();
        assert_eq!(table.scroll_offset, 1); // Should show rows 1-2
    }

    #[test]
    fn test_handle_key_up() {
        let mut table = create_test_table();
        table.set_focused(true);
        table.selected_row = Some(1);
        
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(table.handle_key(key));
        assert_eq!(table.selected_row(), Some(0));
    }

    #[test]
    fn test_handle_key_down() {
        let mut table = create_test_table();
        table.set_focused(true);
        table.selected_row = Some(0);
        
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(table.handle_key(key));
        assert_eq!(table.selected_row(), Some(1));
    }

    #[test]
    fn test_handle_key_j_k() {
        let mut table = create_test_table();
        table.set_focused(true);
        table.selected_row = Some(1);
        
        // 'j' should move down
        let key_j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::empty());
        assert!(table.handle_key(key_j));
        assert_eq!(table.selected_row(), Some(2));
        
        // 'k' should move up
        let key_k = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::empty());
        assert!(table.handle_key(key_k));
        assert_eq!(table.selected_row(), Some(1));
    }

    #[test]
    fn test_handle_key_not_focused() {
        let mut table = create_test_table();
        table.set_focused(false);
        
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(!table.handle_key(key));
    }

    #[test]
    fn test_add_row() {
        let mut table = create_test_table();
        let initial_count = table.row_count();
        
        let new_row = TableRow::new(vec!["David", "28", "88.0"]);
        table.add_row(new_row);
        
        assert_eq!(table.row_count(), initial_count + 1);
    }

    #[test]
    fn test_add_invalid_row() {
        let mut table = create_test_table();
        let initial_count = table.row_count();
        
        // Row with wrong number of cells
        let invalid_row = TableRow::new(vec!["David"]);
        table.add_row(invalid_row);
        
        // Should not add
        assert_eq!(table.row_count(), initial_count);
    }

    #[test]
    fn test_clear_rows() {
        let mut table = create_test_table();
        table.selected_row = Some(1);
        
        table.clear_rows();
        
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.selected_row(), None);
        assert_eq!(table.scroll_offset, 0);
    }

    #[test]
    fn test_get_selected_row() {
        let mut table = create_test_table();
        table.selected_row = Some(1);
        
        let row = table.get_selected_row();
        assert!(row.is_some());
        assert_eq!(row.unwrap().get_cell(0), Some("Bob"));
    }

    #[test]
    fn test_get_selected_row_none() {
        let table = create_test_table();
        assert_eq!(table.get_selected_row(), None);
    }

    #[test]
    fn test_calculate_column_widths() {
        let mut table = create_test_table();
        let widths = table.calculate_column_widths(50);
        
        assert_eq!(widths.len(), 3);
        assert!(widths[0] >= 5); // Minimum width
    }

    #[test]
    fn test_calculate_column_widths_custom() {
        let table = create_test_table().with_column_widths(vec![10, 15, 20]);
        let mut table = table;
        let widths = table.calculate_column_widths(50);
        assert_eq!(widths, vec![10, 15, 20]);
    }

    #[test]
    fn test_to_csv() {
        let table = create_test_table();
        let csv = table.to_csv();
        
        assert!(csv.contains("Name,Age,Score"));
        assert!(csv.contains("Alice,25,95.5"));
        assert!(csv.contains("Bob,30,87.0"));
    }

    #[test]
    fn test_to_csv_with_quotes() {
        let mut table = create_test_table();
        let row = TableRow::new(vec!["Test,Name", "25", "95.5"]);
        table.add_row(row);
        
        let csv = table.to_csv();
        assert!(csv.contains("\"Test,Name\""));
    }

    #[test]
    fn test_sort_next_column() {
        let mut table = create_test_table();
        
        // Sort by first column
        table.sort_by_column(0);
        assert_eq!(table.sort_column(), Some(0));
        
        // Sort next column
        table.sort_next_column();
        assert_eq!(table.sort_column(), Some(1));
    }

    #[test]
    fn test_sort_next_column_wraps() {
        let mut table = create_test_table();
        
        // Sort by last column
        table.sort_by_column(2);
        
        // Sort next should wrap to first
        table.sort_next_column();
        assert_eq!(table.sort_column(), Some(0));
    }

    #[test]
    fn test_toggle_sort_direction() {
        let mut table = create_test_table();
        
        table.sort_by_column(0);
        assert_eq!(table.sort_direction(), SortDirection::Ascending);
        
        table.toggle_sort_direction();
        assert_eq!(table.sort_direction(), SortDirection::Descending);
    }

    #[test]
    fn test_toggle_sort_direction_no_column() {
        let mut table = create_test_table();
        
        // No column selected, should not change
        let original = table.sort_direction();
        table.toggle_sort_direction();
        assert_eq!(table.sort_direction(), original);
    }

    #[test]
    fn test_compare_cell_values_numbers() {
        assert_eq!(
            TableWidget::compare_cell_values("10", "5"),
            Ordering::Greater
        );
        assert_eq!(
            TableWidget::compare_cell_values("5", "10"),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_cell_values_floats() {
        assert_eq!(
            TableWidget::compare_cell_values("10.5", "10.2"),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_cell_values_strings() {
        assert_eq!(
            TableWidget::compare_cell_values("Alice", "Bob"),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_cell_values_mixed() {
        // Numbers should be compared numerically
        assert_eq!(
            TableWidget::compare_cell_values("10", "2"),
            Ordering::Greater
        );
    }

    #[test]
    fn test_render_empty_table() {
        let mut table = TableWidget::new();
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = Buffer::empty(area);
        
        // Should not panic
        table.render(area, &mut buf);
    }

    #[test]
    fn test_render_table_with_rows() {
        let mut table = create_test_table();
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = Buffer::empty(area);
        
        // Should not panic
        table.render(area, &mut buf);
    }

    #[test]
    fn test_table_with_block() {
        let mut table = create_test_table()
            .with_block(Block::default().title("Test Table").borders(Borders::ALL));
        
        assert!(table.block.is_some());
    }

    #[test]
    fn test_table_focus() {
        let mut table = create_test_table();
        
        assert!(!table.is_focused());
        table.set_focused(true);
        assert!(table.is_focused());
    }

    #[test]
    fn test_table_clone() {
        let mut table = create_test_table();
        table.selected_row = Some(1);
        table.sort_by_column(0);
        
        let cloned = table.clone();
        
        assert_eq!(cloned.row_count(), table.row_count());
        assert_eq!(cloned.column_count(), table.column_count());
        assert_eq!(cloned.sort_column(), table.sort_column());
        // Scroll and state reset on clone
        assert_eq!(cloned.scroll_offset, 0);
    }

    #[test]
    fn test_table_row_is_empty() {
        let empty_row: TableRow = TableRow::new(vec![] as Vec<&str>);
        assert!(empty_row.is_empty());
        
        let non_empty = TableRow::new(vec!["A"]);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_table_with_min_column_width() {
        let mut table = create_test_table().with_min_column_width(10);
        let widths = table.calculate_column_widths(50);
        
        for width in widths {
            assert!(width >= 10);
        }
    }

    #[test]
    fn test_table_with_max_visible_rows() {
        let table = create_test_table().with_max_visible_rows(5);
        assert_eq!(table.max_visible_rows, 5);
    }

    #[test]
    fn test_table_selection_bounds() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        // Select beyond bounds
        table.selected_row = Some(100);
        table.move_selection_down();
        
        // Should clamp to last row
        assert_eq!(table.selected_row(), Some(2));
    }

    #[test]
    fn test_table_empty_selection_handling() {
        let mut table = TableWidget::new();
        table.set_focused(true);
        
        // Operations on empty table should not panic
        table.move_selection_down();
        table.move_selection_up();
        table.page_up();
        table.page_down();
    }

    #[test]
    fn test_table_sort_preserves_selection() {
        let mut table = create_test_table();
        table.selected_row = Some(1);
        
        table.sort_by_column(0);
        
        // Selection should still be valid
        assert!(table.selected_row().is_some());
        assert!(table.selected_row().unwrap() < table.row_count());
    }

    #[test]
    fn test_table_multiple_sorts() {
        let mut table = create_test_table();
        
        // Sort by column 0
        table.sort_by_column(0);
        let first_order: Vec<String> = table.rows.iter().map(|r| r.get_cell(0).unwrap().to_string()).collect();
        
        // Sort by column 1
        table.sort_by_column(1);
        let second_order: Vec<String> = table.rows.iter().map(|r| r.get_cell(0).unwrap().to_string()).collect();
        
        // Orders should be different
        assert_ne!(first_order, second_order);
    }

    #[test]
    fn test_table_header_validation() {
        let mut table = create_test_table();
        
        // Add duplicate header names (should warn but not fail)
        table.headers.push(TableHeader::new("Name"));
        table.validate_headers();
        
        // Should still work
        assert_eq!(table.column_count(), 4);
    }

    #[test]
    fn test_table_row_validation() {
        let mut table = create_test_table();
        
        // Add row with wrong number of cells
        let invalid_row = TableRow::new(vec!["Only", "Two"]);
        table.add_row(invalid_row);
        
        // Should not be added
        assert_eq!(table.row_count(), 3);
    }

    #[test]
    fn test_escape_csv() {
        assert_eq!(escape_csv("normal"), "normal");
        assert_eq!(escape_csv("with,comma"), "\"with,comma\"");
        assert_eq!(escape_csv("with\"quote"), "\"with\"\"quote\"");
    }

    #[test]
    fn test_table_handle_ctrl_s() {
        let mut table = create_test_table();
        table.set_focused(true);
        table.sort_by_column(0);
        
        let key = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL);
        assert!(table.handle_key(key));
        
        // Should toggle sort direction
        assert_eq!(table.sort_direction(), SortDirection::Descending);
    }

    #[test]
    fn test_table_handle_tab() {
        let mut table = create_test_table();
        table.set_focused(true);
        
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(table.handle_key(key));
        
        // Should sort by first sortable column (0) if none selected
        assert_eq!(table.sort_column(), Some(0));
        
        // Tab again should move to next column
        let key2 = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(table.handle_key(key2));
        assert_eq!(table.sort_column(), Some(1));
    }

    #[test]
    fn test_table_handle_home_end() {
        let mut table = create_test_table();
        table.set_focused(true);
        table.selected_row = Some(1);
        
        let home_key = KeyEvent::new(KeyCode::Home, KeyModifiers::empty());
        assert!(table.handle_key(home_key));
        assert_eq!(table.selected_row(), Some(0));
        
        let end_key = KeyEvent::new(KeyCode::End, KeyModifiers::empty());
        assert!(table.handle_key(end_key));
        assert_eq!(table.selected_row(), Some(2));
    }

    #[test]
    fn test_table_handle_page_up_down() {
        let mut table = create_test_table().with_max_visible_rows(1);
        table.set_focused(true);
        table.selected_row = Some(2);
        
        let page_up = KeyEvent::new(KeyCode::PageUp, KeyModifiers::empty());
        assert!(table.handle_key(page_up));
        assert_eq!(table.selected_row(), Some(1));
        
        let page_down = KeyEvent::new(KeyCode::PageDown, KeyModifiers::empty());
        assert!(table.handle_key(page_down));
        assert_eq!(table.selected_row(), Some(2));
    }
}
