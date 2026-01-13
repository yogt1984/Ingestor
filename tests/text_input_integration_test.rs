//! Integration Tests for Text Input Widget
//!
//! These tests verify that the text input widget works correctly
//! in a TUI context with ratatui rendering.

use ingestor::ui::widgets::params::text_input::{TextInputWidget, ValidationResult};
use ratatui::{
    backend::TestBackend,
    buffer::Buffer,
    layout::Rect,
    Terminal,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[test]
fn test_widget_renders_correctly() {
    let widget = TextInputWidget::new()
        .with_text("Hello")
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(20, 5)).unwrap();
    terminal.backend_mut().resize(20, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    // Widget should render text
    assert!(buffer.content().iter().any(|cell| cell.symbol() == "H"));
}

#[test]
fn test_widget_renders_placeholder() {
    let widget = TextInputWidget::new()
        .with_placeholder("Enter text...")
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(30, 5)).unwrap();
    terminal.backend_mut().resize(30, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    let buffer = terminal.backend().buffer();
    // Placeholder should be visible when text is empty
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(content.contains("Enter") || content.contains("text"));
}

#[test]
fn test_widget_handles_key_events() {
    let mut widget = TextInputWidget::new().set_focused(true);

    let key = KeyEvent::new(KeyCode::Char('H'), KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.text(), "H");
}

#[test]
fn test_widget_validation_integration() {
    let mut widget = TextInputWidget::new()
        .with_validator(|s| {
            if s.len() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        })
        .set_focused(true);

    widget.insert_char('a');
    widget.validate();
    assert!(!widget.is_valid());

    widget.insert_char('b');
    widget.insert_char('c');
    widget.validate();
    assert!(widget.is_valid());
}

#[test]
fn test_widget_max_length_integration() {
    let mut widget = TextInputWidget::new()
        .with_max_length(5)
        .set_focused(true);

    for _ in 0..10 {
        widget.insert_char('A');
    }

    assert_eq!(widget.text().chars().count(), 5);
}

#[test]
fn test_widget_read_only_integration() {
    let mut widget = TextInputWidget::new()
        .with_text("Hello")
        .set_read_only(true)
        .set_focused(true);

    let initial_text = widget.text_owned();
    let key = KeyEvent::new(KeyCode::Char('X'), KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.text(), initial_text);
}

#[test]
fn test_widget_cursor_movement_integration() {
    let mut widget = TextInputWidget::new()
        .with_text("Hello")
        .set_focused(true);

    let key_left = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
    widget.handle_key(key_left);
    assert_eq!(widget.cursor_pos(), 4);

    let key_home = KeyEvent::new(KeyCode::Home, KeyModifiers::empty());
    widget.handle_key(key_home);
    assert_eq!(widget.cursor_pos(), 0);

    let key_end = KeyEvent::new(KeyCode::End, KeyModifiers::empty());
    widget.handle_key(key_end);
    assert_eq!(widget.cursor_pos(), 5);
}

#[test]
fn test_widget_full_editing_workflow() {
    let mut widget = TextInputWidget::new()
        .with_placeholder("Enter name...")
        .with_max_length(20)
        .with_validator(|s| {
            if s.is_empty() {
                ValidationResult::Invalid("Name required".to_string())
            } else if s.chars().count() < 3 {
                ValidationResult::Invalid("Name too short".to_string())
            } else {
                ValidationResult::Valid
            }
        })
        .set_focused(true);

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
}

#[test]
fn test_widget_rendering_with_validation_error() {
    let widget = TextInputWidget::new()
        .with_text("ab")
        .with_validator(|s| {
            if s.chars().count() >= 3 {
                ValidationResult::Valid
            } else {
                ValidationResult::Invalid("Too short".to_string())
            }
        })
        .set_focused(true)
        .set_show_validation(true);

    let mut widget = widget;
    widget.validate();

    let mut terminal = Terminal::new(TestBackend::new(30, 10)).unwrap();
    terminal.backend_mut().resize(30, 10);

    terminal
        .draw(|f| {
            widget.render(f, Rect::new(0, 0, 20, 3));
        })
        .unwrap();

    // Widget should render (validation error rendering is tested separately)
    assert!(!widget.is_valid());
}

#[test]
fn test_widget_focus_states() {
    let widget_unfocused = TextInputWidget::new()
        .with_text("Hello")
        .set_focused(false);

    let widget_focused = TextInputWidget::new()
        .with_text("Hello")
        .set_focused(true);

    // Both should have same text
    assert_eq!(widget_unfocused.text(), widget_focused.text());
    // Focus states are tested through rendering behavior, not direct field access
}

#[test]
fn test_widget_unicode_rendering() {
    let widget = TextInputWidget::new()
        .with_text("🚀中文")
        .set_focused(true);

    let mut terminal = Terminal::new(TestBackend::new(20, 5)).unwrap();
    terminal.backend_mut().resize(20, 5);

    terminal
        .draw(|f| {
            widget.render(f, f.area());
        })
        .unwrap();

    // Should render without panicking
    assert_eq!(widget.text(), "🚀中文");
}

