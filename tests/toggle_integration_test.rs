//! Integration Tests for Toggle Widget
//!
//! These tests verify that the toggle widget works correctly
//! in a TUI context with ratatui rendering.

use ingestor::ui::widgets::params::toggle::{ToggleWidget, ToggleStyle};
use ratatui::{
    backend::TestBackend,
    buffer::Buffer,
    layout::Rect,
    Terminal,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[test]
fn test_widget_renders_correctly() {
    let widget = ToggleWidget::new()
        .with_value(true)
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(20, 5)).unwrap();
    terminal.backend_mut().resize(20, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    // Widget should render
    assert!(buffer.content().iter().any(|cell| cell.symbol() == "[" || cell.symbol() == "X"));
}

#[test]
fn test_widget_renders_checkbox() {
    let widget = ToggleWidget::new()
        .with_value(false)
        .with_style(ToggleStyle::Checkbox)
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(20, 5)).unwrap();
    terminal.backend_mut().resize(20, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(content.contains("[ ]"));
}

#[test]
fn test_widget_renders_switch() {
    let widget = ToggleWidget::new()
        .with_value(true)
        .with_style(ToggleStyle::Switch)
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(20, 5)).unwrap();
    terminal.backend_mut().resize(20, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(content.contains("ON"));
}

#[test]
fn test_widget_renders_with_label() {
    let widget = ToggleWidget::new()
        .with_value(true)
        .with_label("Enable feature")
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(30, 5)).unwrap();
    terminal.backend_mut().resize(30, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(content.contains("Enable") || content.contains("feature"));
}

#[test]
fn test_widget_handles_key_events() {
    let mut widget = ToggleWidget::new()
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
    widget.handle_key(key);

    assert!(widget.value());
}

#[test]
fn test_widget_toggle_space() {
    let mut widget = ToggleWidget::new()
        .with_value(false)
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
    widget.handle_key(key);
    assert!(widget.value());

    widget.handle_key(key);
    assert!(!widget.value());
}

#[test]
fn test_widget_toggle_enter() {
    let mut widget = ToggleWidget::new()
        .with_value(false)
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
    widget.handle_key(key);
    assert!(widget.value());
}

#[test]
fn test_widget_set_true_with_y() {
    let mut widget = ToggleWidget::new()
        .with_value(false)
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Char('y'), KeyModifiers::empty());
    widget.handle_key(key);
    assert!(widget.value());
}

#[test]
fn test_widget_set_false_with_n() {
    let mut widget = ToggleWidget::new()
        .with_value(true)
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::empty());
    widget.handle_key(key);
    assert!(!widget.value());
}

#[test]
fn test_widget_toggle_with_t() {
    let mut widget = ToggleWidget::new()
        .with_value(false)
        .set_focused(true);

    let key = KeyEvent::new(KeyCode::Char('t'), KeyModifiers::empty());
    widget.handle_key(key);
    assert!(widget.value());
}

#[test]
fn test_widget_read_only() {
    let mut widget = ToggleWidget::new()
        .with_value(false)
        .set_read_only(true)
        .set_focused(true);

    let initial_value = widget.value();
    let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), initial_value);
}

#[test]
fn test_widget_validation_integration() {
    let mut widget = ToggleWidget::new()
        .set_focused(true);

    widget.validate();
    assert!(widget.is_valid());

    widget.toggle();
    widget.validate();
    assert!(widget.is_valid());
}

#[test]
fn test_widget_full_workflow() {
    let mut widget = ToggleWidget::new()
        .with_style(ToggleStyle::Checkbox)
        .with_label("Enable feature")
        .set_focused(true);

    // Start false
    assert!(!widget.value());

    // Toggle to true
    widget.toggle();
    assert!(widget.value());

    // Toggle back to false
    widget.toggle();
    assert!(!widget.value());
}

#[test]
fn test_widget_keyboard_workflow() {
    let mut widget = ToggleWidget::new()
        .with_style(ToggleStyle::Switch)
        .set_focused(true);

    // Space to toggle
    let key1 = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
    widget.handle_key(key1);
    assert!(widget.value());

    // Enter to toggle back
    let key2 = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
    widget.handle_key(key2);
    assert!(!widget.value());
}

#[test]
fn test_widget_style_switching() {
    let mut widget = ToggleWidget::new()
        .with_value(true)
        .with_style(ToggleStyle::Checkbox);
    assert!(widget.value());

    widget = widget.with_style(ToggleStyle::Switch);
    assert!(widget.value());
}

#[test]
fn test_widget_read_only_workflow() {
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
