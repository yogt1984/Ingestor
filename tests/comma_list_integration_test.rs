//! Integration Tests for Comma List Widget
//!
//! These tests verify that the comma list widget works correctly
//! in a TUI context with ratatui rendering.

use ingestor::ui::widgets::params::comma_list::CommaListWidget;
use ratatui::{
    backend::TestBackend,
    buffer::Buffer,
    layout::Rect,
    Terminal,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[test]
fn test_widget_renders_correctly() {
    let widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(20, 10)).unwrap();
    terminal.backend_mut().resize(20, 10);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    // Widget should render values
    assert!(buffer.content().iter().any(|cell| cell.symbol() == "1" || cell.symbol() == "2"));
}

#[test]
fn test_widget_renders_placeholder() {
    let widget = CommaListWidget::new()
        .with_placeholder("Enter values...")
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(30, 10)).unwrap();
    terminal.backend_mut().resize(30, 10);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    // Placeholder should be visible when list is empty
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(content.contains("empty") || content.contains("Enter"));
}

#[test]
fn test_widget_handles_key_events() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.selected(), Some(0));
}

#[test]
fn test_widget_navigation() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_focused(true);

    let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
    widget.handle_key(key_down);
    assert_eq!(widget.selected(), Some(0));

    let key_up = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
    widget.handle_key(key_up);
    assert_eq!(widget.selected(), Some(2)); // Wraps to last
}

#[test]
fn test_widget_add_value() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0])
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
    widget.handle_key(key);

    assert!(widget.is_editing());
}

#[test]
fn test_widget_delete_value() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_focused(true);

    widget.set_selected(Some(1));
    let key = KeyEvent::new(KeyCode::Delete, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.values(), &[1.0, 3.0]);
}

#[test]
fn test_widget_edit_value() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_focused(true);

    widget.set_selected(Some(1));
    let key_enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
    widget.handle_key(key_enter);

    assert!(widget.is_editing());
    assert_eq!(widget.text_buffer(), "2.00");
}

#[test]
fn test_widget_validation_integration() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![10.0, 20.0, 30.0])
        .with_min(0.0)
        .with_max(100.0)
        .set_focused(true);

    widget.validate();
    assert!(widget.is_valid());

    widget.set_values_from_str("50,150,30");
    widget.validate();
    // Value gets clamped, so should be valid
    assert!(widget.is_valid());
}

#[test]
fn test_widget_parse_string() {
    let mut widget = CommaListWidget::new().set_focused(true);

    widget.set_values_from_str("1,2,3");
    assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
}

#[test]
fn test_widget_parse_string_with_spaces() {
    let mut widget = CommaListWidget::new().set_focused(true);

    widget.set_values_from_str("1, 2, 3");
    assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
}

#[test]
fn test_widget_no_duplicates() {
    let mut widget = CommaListWidget::new()
        .set_allow_duplicates(false)
        .set_focused(true);

    widget.add_value(1.0);
    widget.add_value(1.0);
    assert_eq!(widget.values(), &[1.0]); // Duplicate should be rejected
}

#[test]
fn test_widget_sorted_mode() {
    let mut widget = CommaListWidget::new()
        .set_require_sorted(true)
        .set_focused(true);

    widget.add_value(3.0);
    widget.add_value(1.0);
    widget.add_value(2.0);
    assert_eq!(widget.values(), &[1.0, 2.0, 3.0]);
}

#[test]
fn test_widget_read_only() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_read_only(true)
        .set_focused(true);

    let initial_values = widget.values().to_vec();
    let key = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.values(), &initial_values);
}

#[test]
fn test_widget_preset() {
    let mut widget = CommaListWidget::new()
        .add_preset("test", vec![10.0, 20.0, 30.0])
        .set_focused(true);

    widget.apply_preset(0);
    assert_eq!(widget.values(), &[10.0, 20.0, 30.0]);
}

#[test]
fn test_widget_values_str() {
    let widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .with_decimals(0)
        .set_focused(true);

    assert_eq!(widget.values_str(), "1,2,3");
}

#[test]
fn test_widget_full_workflow() {
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
    widget.set_selected(Some(1));
    widget.update_at(1, 25.0);
    assert_eq!(widget.values(), &[10.0, 25.0, 30.0]);

    // Remove value
    widget.remove_at(0);
    assert_eq!(widget.values(), &[25.0, 30.0]);
}

#[test]
fn test_widget_clear() {
    let mut widget = CommaListWidget::new()
        .with_values(vec![1.0, 2.0, 3.0])
        .set_focused(true);

    widget.clear();
    assert!(widget.values().is_empty());
}
