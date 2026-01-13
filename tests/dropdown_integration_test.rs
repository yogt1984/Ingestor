//! Integration tests for DropdownWidget
//!
//! These tests verify the widget's behavior within a ratatui::Terminal environment,
//! ensuring proper rendering and interaction.

use ratatui::{
    backend::TestBackend,
    Terminal,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ingestor::ui::widgets::DropdownWidget;

// ============================================================================
// Rendering Tests
// ============================================================================

#[test]
fn test_widget_renders_basic() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = DropdownWidget::new()
        .with_options(vec!["Option1".to_string(), "Option2".to_string()])
        .with_placeholder("Select option...");

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render without panicking
}

#[test]
fn test_widget_renders_with_selection() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = DropdownWidget::new()
        .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Cherry".to_string()])
        .with_selected(1);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render with selection
}

#[test]
fn test_widget_renders_expanded() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
        .set_focused(true);
    widget.expand();

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render expanded dropdown
}

#[test]
fn test_widget_renders_with_filter() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let mut widget = DropdownWidget::new()
        .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()])
        .set_focused(true);
    widget.expand();
    widget.filter = "Ap".to_string();

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render with filtered options
}

#[test]
fn test_widget_renders_read_only() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .with_selected(0)
        .set_read_only(true);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render in read-only state
}

// ============================================================================
// Key Event Tests
// ============================================================================

#[test]
fn test_handle_key_expand() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
    widget.handle_key(key);

    assert!(widget.is_expanded());
}

#[test]
fn test_handle_key_collapse() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(true);
    widget.expand();

    let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::empty());
    widget.handle_key(key);

    assert!(!widget.is_expanded());
}

#[test]
fn test_handle_key_navigation() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
        .set_focused(true);
    widget.expand();
    widget.list_state.select(Some(0));

    // Navigate down
    let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
    widget.handle_key(key_down);
    assert_eq!(widget.list_state.selected(), Some(1));

    // Navigate up
    let key_up = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
    widget.handle_key(key_up);
    assert_eq!(widget.list_state.selected(), Some(0));
}

#[test]
fn test_handle_key_select() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string(), "C".to_string()])
        .set_focused(true);
    widget.expand();
    widget.list_state.select(Some(1));

    let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.selected_index(), Some(1));
    assert!(!widget.is_expanded());
}

#[test]
fn test_handle_key_filter() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()])
        .set_focused(true);
    widget.expand();

    // Type 'A'
    let key_a = KeyEvent::new(KeyCode::Char('A'), KeyModifiers::empty());
    widget.handle_key(key_a);
    assert_eq!(widget.filter, "A");

    // Type 'p'
    let key_p = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::empty());
    widget.handle_key(key_p);
    assert_eq!(widget.filter, "Ap");
}

#[test]
fn test_handle_key_backspace_filter() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(true);
    widget.expand();
    widget.filter = "test".to_string();

    let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.filter, "tes");
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_validation_valid_selection() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .with_selected(0);
    widget.validate();

    assert!(widget.is_valid());
}

#[test]
fn test_validation_invalid_selection() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()]);
    widget.selected = Some(10); // Invalid index
    widget.validate();

    assert!(!widget.is_valid());
    assert!(widget.validation_error().is_some());
}

// ============================================================================
// Filter Tests
// ============================================================================

#[test]
fn test_filter_case_insensitive() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["Apple".to_string(), "banana".to_string(), "APRICOT".to_string()]);
    widget.filter = "a".to_string();
    let filtered = widget.filtered_options();

    assert_eq!(filtered.len(), 3); // All match (case insensitive)
}

#[test]
fn test_filter_no_matches() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["Apple".to_string(), "Banana".to_string()]);
    widget.filter = "XYZ".to_string();
    let filtered = widget.filtered_options();

    assert!(filtered.is_empty());
}

// ============================================================================
// Full Workflow Tests
// ============================================================================

#[test]
fn test_full_selection_workflow() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["Option1".to_string(), "Option2".to_string(), "Option3".to_string()])
        .set_focused(true);

    // Expand
    widget.expand();
    assert!(widget.is_expanded());

    // Navigate
    widget.list_state.select(Some(1));

    // Select
    let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.selected_index(), Some(1));
    assert!(!widget.is_expanded());
}

#[test]
fn test_filter_and_select_workflow() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["Apple".to_string(), "Banana".to_string(), "Apricot".to_string()])
        .set_focused(true);

    widget.expand();

    // Filter
    widget.handle_key(KeyEvent::new(KeyCode::Char('A'), KeyModifiers::empty()));
    widget.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::empty()));

    // Select first filtered option
    widget.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::empty()));

    assert_eq!(widget.selected_index(), Some(0)); // Apple
}

// ============================================================================
// Focus States Tests
// ============================================================================

#[test]
fn test_widget_focus_states() {
    let widget_unfocused = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(false);

    let widget_focused = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(true);

    // Focus state should be different
    assert_ne!(widget_unfocused.is_focused(), widget_focused.is_focused());
}

#[test]
fn test_focus_collapses_dropdown() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(true);
    widget.expand();
    assert!(widget.is_expanded());

    widget = widget.set_focused(false);
    assert!(!widget.is_expanded());
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

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
fn test_unicode_options() {
    let widget = DropdownWidget::new()
        .with_options(vec!["选项1".to_string(), "选项2".to_string(), "🚀选项".to_string()]);

    assert_eq!(widget.options().len(), 3);
}

#[test]
fn test_navigation_wraps() {
    let mut widget = DropdownWidget::new()
        .with_options(vec!["A".to_string(), "B".to_string()])
        .set_focused(true);
    widget.expand();
    widget.list_state.select(Some(1));

    // Navigate down (should wrap to first)
    widget.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::empty()));
    assert_eq!(widget.list_state.selected(), Some(0));

    // Navigate up (should wrap to last)
    widget.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::empty()));
    assert_eq!(widget.list_state.selected(), Some(1));
}
