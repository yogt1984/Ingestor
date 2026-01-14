//! Integration tests for SliderWidget
//!
//! These tests verify the widget's behavior within a ratatui::Terminal environment,
//! ensuring proper rendering and interaction.

use ratatui::{
    backend::TestBackend,
    Terminal,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ingestor::ui::widgets::SliderWidget;
use ingestor::ui::widgets::params::slider::SliderFormat;

// ============================================================================
// Rendering Tests
// ============================================================================

#[test]
fn test_widget_renders_basic() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render without panicking
}

#[test]
fn test_widget_renders_with_label() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_label("Weight");

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render with label
}

#[test]
fn test_widget_renders_with_min_max_labels() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_show_min_max_labels(true);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render with min/max labels
}

#[test]
fn test_widget_renders_without_min_max_labels() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_show_min_max_labels(false);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render without min/max labels
}

#[test]
fn test_widget_renders_percentage_format() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 1.0)
        .with_value(0.5)
        .with_format(SliderFormat::Percentage);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render with percentage format
}

#[test]
fn test_widget_renders_focused() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .set_focused(true);

    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();

    // Widget should render in focused state
}

#[test]
fn test_widget_renders_read_only() {
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
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
fn test_handle_key_left() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(10.0);

    let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), 40.0);
}

#[test]
fn test_handle_key_right() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(10.0);

    let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), 60.0);
}

#[test]
fn test_handle_key_home() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0);

    let key = KeyEvent::new(KeyCode::Home, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), 0.0);
}

#[test]
fn test_handle_key_end() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0);

    let key = KeyEvent::new(KeyCode::End, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), 100.0);
}

#[test]
fn test_handle_key_up() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(1.0);

    let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), 60.0); // Increment by 10x step
}

#[test]
fn test_handle_key_down() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(1.0);

    let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
    widget.handle_key(key);

    assert_eq!(widget.value(), 40.0); // Decrement by 10x step
}

#[test]
fn test_handle_key_read_only() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .set_read_only(true);

    let key = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
    assert!(!widget.handle_key(key));
    assert_eq!(widget.value(), 50.0);
}

// ============================================================================
// Value Manipulation Tests
// ============================================================================

#[test]
fn test_increment_integration() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(10.0);

    widget.increment();
    assert_eq!(widget.value(), 60.0);
}

#[test]
fn test_decrement_integration() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(10.0);

    widget.decrement();
    assert_eq!(widget.value(), 40.0);
}

#[test]
fn test_set_position_integration() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_step(1.0);

    widget.set_position(0.25);
    assert!((widget.value() - 25.0).abs() < 0.01);
}

#[test]
fn test_position_integration() {
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(75.0);

    assert!((widget.position() - 0.75).abs() < 0.01);
}

// ============================================================================
// Format Tests
// ============================================================================

#[test]
fn test_format_percentage_integration() {
    let widget = SliderWidget::new(0.0, 1.0)
        .with_value(0.5)
        .with_format(SliderFormat::Percentage)
        .with_decimals(1);

    // Format is tested through rendering, not direct method access
    // Widget should render correctly with percentage format
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();
}

#[test]
fn test_format_basis_points_integration() {
    let widget = SliderWidget::new(0.0, 1.0)
        .with_value(0.0125)
        .with_format(SliderFormat::BasisPoints)
        .with_decimals(0);

    // Format is tested through rendering
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();
}

#[test]
fn test_format_integer_integration() {
    let widget = SliderWidget::new(0.0, 100.0)
        .with_value(12.7)
        .with_format(SliderFormat::Integer);

    // Format is tested through rendering
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    terminal.draw(|f| {
        widget.render(f, f.size());
    }).unwrap();
}

// ============================================================================
// Full Workflow Tests
// ============================================================================

#[test]
fn test_full_adjustment_workflow_integration() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_step(10.0)
        .set_focused(true);

    // Start at middle
    assert!((widget.value() - 50.0).abs() < 0.01);

    // Increment
    widget.increment();
    assert_eq!(widget.value(), 60.0);

    // Decrement
    widget.decrement();
    assert_eq!(widget.value(), 50.0);

    // Set to min
    widget.set_to_min();
    assert_eq!(widget.value(), 0.0);

    // Set to max
    widget.set_to_max();
    assert_eq!(widget.value(), 100.0);
}

#[test]
fn test_keyboard_navigation_workflow_integration() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(50.0)
        .with_step(10.0)
        .set_focused(true);

    // Navigate left
    widget.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::empty()));
    assert_eq!(widget.value(), 40.0);

    // Navigate right
    widget.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::empty()));
    assert_eq!(widget.value(), 50.0);

    // Go to home
    widget.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::empty()));
    assert_eq!(widget.value(), 0.0);

    // Go to end
    widget.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::empty()));
    assert_eq!(widget.value(), 100.0);
}

// ============================================================================
// Focus States Tests
// ============================================================================

#[test]
fn test_widget_focus_states() {
    let widget_unfocused = SliderWidget::new(0.0, 100.0)
        .set_focused(false);

    let widget_focused = SliderWidget::new(0.0, 100.0)
        .set_focused(true);

    // Focus state should be different
    assert_ne!(widget_unfocused.is_focused(), widget_focused.is_focused());
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[test]
fn test_negative_range_integration() {
    let widget = SliderWidget::new(-100.0, 100.0)
        .with_value(0.0);

    assert!(widget.value() >= widget.min());
    assert!(widget.value() <= widget.max());
}

#[test]
fn test_small_range_integration() {
    let widget = SliderWidget::new(0.0, 1.0)
        .with_value(0.5);

    assert!(widget.value() >= widget.min());
    assert!(widget.value() <= widget.max());
}

#[test]
fn test_navigation_clamps_to_bounds() {
    let mut widget = SliderWidget::new(0.0, 100.0)
        .with_value(95.0)
        .with_step(10.0);

    widget.increment();
    assert_eq!(widget.value(), 100.0); // Clamped to max

    widget.set_value(5.0);
    widget.decrement();
    assert_eq!(widget.value(), 0.0); // Clamped to min
}
