//! User interface module
//!
//! Contains terminal UI (TUI) components for real-time monitoring.
//!
//! # Modules
//!
//! - `tui`: Main TUI application with menu system
//! - `screens`: Specialized dashboard screens (Task 4.x)
//!   - `research`: Research dashboard (Task 4.1)

pub mod tui;
pub mod screens;

pub use tui::run_tui;
pub use screens::{
    ResearchScreen,
    ResearchScreenState,
    ResearchEngineStatus,
    draw_research_screen,
};
