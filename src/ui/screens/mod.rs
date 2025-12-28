//! TUI Screen Modules
//!
//! This module contains specialized screens for the TUI interface:
//! - Main Menu (Task 4.0): Restructured 6-item main menu
//! - Research Dashboard (Task 4.1): Current research state display
//! - Research Menu (TUI-1.0): Research submenu implementation
//! - Validation Dashboard (Task 4.2): Validation pipeline results
//! - Algorithm Dashboard (Task 4.3): Active algorithms display
//!
//! Each screen follows the pattern:
//! - State struct for screen-specific data
//! - Render function for drawing
//! - Handle input for screen-specific key bindings
//! - Tests for all display logic

pub mod main_menu;
pub mod research;
pub mod research_menu;
pub mod algorithms_menu;

// Main Menu (Task 4.0)
pub use main_menu::{
    MainMenuItem,
    MainMenuState,
    draw_main_menu,
};

// Research Dashboard (Task 4.1)
pub use research::{
    ResearchScreen,
    ResearchScreenState,
    ResearchEngineStatus,
    draw_research_screen,
};

// Research Menu (TUI-1.0)
pub use research_menu::{
    ResearchMenu,
    draw_research_menu,
};

// Algorithms Menu (TUI-2.0)
pub use algorithms_menu::{
    AlgorithmsMenu,
    StrategyFilter,
    draw_algorithms_menu,
};
