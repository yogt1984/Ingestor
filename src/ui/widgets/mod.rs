//! UI Widget Modules
//!
//! This module contains reusable UI widgets for the TUI interface:
//! - StatusBar (Task TUI-6.0): Persistent status bar at bottom of screens
//!
//! Widgets are self-contained components that can be rendered in any screen.

pub mod status_bar;

// StatusBar (TUI-6.0)
pub use status_bar::{StatusBar, draw_status_bar};
