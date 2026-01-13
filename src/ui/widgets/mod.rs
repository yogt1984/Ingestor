//! UI Widget Modules
//!
//! This module contains reusable UI widgets for the TUI interface:
//! - StatusBar (Task TUI-6.0): Persistent status bar at bottom of screens
//! - params: Parameter input widgets (Task T-2.1+)
//!
//! Widgets are self-contained components that can be rendered in any screen.

pub mod status_bar;
pub mod params;

// StatusBar (TUI-6.0)
pub use status_bar::{StatusBar, draw_status_bar};

// Parameter widgets (T-2.1+)
pub use params::text_input::TextInputWidget;
pub use params::number_input::NumberInputWidget;
pub use params::comma_list::CommaListWidget;