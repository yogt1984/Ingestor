//! User interface module
//!
//! Contains terminal UI (TUI) components for real-time monitoring.
//!
//! # Modules
//!
//! - `tui`: Main TUI application with menu system
//! - `screens`: Specialized dashboard screens (Task 4.x)
//!   - `main_menu`: Main menu (Task 4.0)
//!   - `research`: Research dashboard (Task 4.1)
//! - `widgets`: Reusable UI widgets (TUI-6.0+)
//!   - `status_bar`: Persistent status bar (TUI-6.0)
//! - `state`: Global state management (TUI-0.0)
//! - `submenu`: SubMenu trait and navigation framework (TUI-0.2)
//! - `tui_integration`: Menu integration and navigation (TUI-7.0)
//! - `algorithm_persistence`: Algorithm selection persistence (TUI-8.0)

pub mod tui;
pub mod screens;
pub mod widgets;
pub mod state;
pub mod submenu;
pub mod tui_integration;
pub mod algorithm_persistence;
pub mod presets;

pub use tui::run_tui;

// Main Menu (Task 4.0)
pub use screens::{
    MainMenuItem,
    MainMenuState,
    draw_main_menu,
};

// Research Dashboard (Task 4.1)
pub use screens::{
    ResearchScreen,
    ResearchScreenState,
    ResearchEngineStatus,
    draw_research_screen,
};

// Global State (TUI-0.0)
pub use state::{
    GlobalState,
    AlgorithmConfigSummary,
    ValidationStatus,
    StageStatus,
    TradingMode,
    ResearchStatus,
    DataStats,
    ValidationStage,
};

// SubMenu Framework (TUI-0.2)
pub use submenu::{
    SubMenu,
    SubMenuAction,
    SubMenuItem,
    NavigationTarget,
    CliCommand,
    draw_submenu_frame,
    draw_submenu_title,
    draw_submenu_items,
    draw_submenu_footer,
    draw_message_dialog,
    key_to_char,
    is_back_key,
};

// Widgets (TUI-6.0+)
pub use widgets::{StatusBar, draw_status_bar};

// TUI Integration (TUI-7.0)
pub use tui_integration::{
    CurrentSubMenu,
    MenuIntegration,
    ActionResult,
    process_action,
    draw_menu_with_status,
    draw_current_menu,
};

// Algorithm Persistence (TUI-8.0)
pub use algorithm_persistence::{
    AlgorithmPersistence,
    AlgorithmPersistenceConfig,
    PersistedSelection,
    load_selected_algorithm,
    save_selected_algorithm,
    clear_selected_algorithm,
};

// Preset Management (T-2.12)
pub use presets::{
    PresetManager,
    Preset,
    PresetMetadata,
    PresetType,
};
