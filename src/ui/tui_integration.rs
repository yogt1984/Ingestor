//! TUI Integration Module (TUI-7.0)
//!
//! Provides integration between the main TUI event loop and the submenu framework.
//! This module handles:
//! - Navigation state management (which submenu is active)
//! - Submenu action processing (converting SubMenuAction to AppMode changes)
//! - Status bar state synchronization
//! - Message dialog handling

use crate::ui::state::GlobalState;
use crate::ui::submenu::{NavigationTarget, SubMenuAction, SubMenu};
use crate::ui::screens::{
    ResearchMenu, AlgorithmsMenu, ValidateMenu, TradeMenu, DataMenu,
    draw_research_menu, draw_algorithms_menu, draw_validate_menu,
    draw_trade_menu, draw_data_menu,
};
use crate::ui::widgets::draw_status_bar;

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    Frame,
};

// ============================================================================
// SubMenuState - Tracks which submenu is currently active
// ============================================================================

/// Tracks the current submenu state within the new menu system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CurrentSubMenu {
    /// Main menu is active (no submenu)
    #[default]
    None,
    /// Research submenu is active
    Research,
    /// Algorithms submenu is active
    Algorithms,
    /// Validate submenu is active
    Validate,
    /// Trade submenu is active
    Trade,
    /// Data submenu is active
    Data,
}

impl CurrentSubMenu {
    /// Check if we're at the main menu level
    pub fn is_main_menu(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Check if a submenu is active
    pub fn is_submenu_active(&self) -> bool {
        !self.is_main_menu()
    }

    /// Get the display name of the current submenu
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::None => "Main Menu",
            Self::Research => "Research",
            Self::Algorithms => "Algorithms",
            Self::Validate => "Validate",
            Self::Trade => "Trade",
            Self::Data => "Data",
        }
    }

    /// Create from a NavigationTarget
    pub fn from_navigation_target(target: &NavigationTarget) -> Option<Self> {
        match target {
            NavigationTarget::MainMenu => Some(Self::None),
            NavigationTarget::ResearchMenu => Some(Self::Research),
            NavigationTarget::AlgorithmsMenu => Some(Self::Algorithms),
            NavigationTarget::ValidateMenu => Some(Self::Validate),
            NavigationTarget::TradeMenu => Some(Self::Trade),
            NavigationTarget::DataMenu => Some(Self::Data),
            _ => None, // Other navigation targets are AppMode changes
        }
    }
}

// ============================================================================
// MenuIntegration - Holds all submenu instances and state
// ============================================================================

/// Holds all submenu instances and provides unified navigation
#[derive(Debug, Default)]
pub struct MenuIntegration {
    /// Current active submenu
    pub current: CurrentSubMenu,
    /// Research submenu instance
    pub research_menu: ResearchMenu,
    /// Algorithms submenu instance
    pub algorithms_menu: AlgorithmsMenu,
    /// Validate submenu instance
    pub validate_menu: ValidateMenu,
    /// Trade submenu instance
    pub trade_menu: TradeMenu,
    /// Data submenu instance
    pub data_menu: DataMenu,
    /// Message to display (if any)
    pub message: Option<String>,
}

impl MenuIntegration {
    /// Create a new MenuIntegration
    pub fn new() -> Self {
        Self::default()
    }

    /// Navigate to a submenu
    pub fn navigate_to(&mut self, submenu: CurrentSubMenu) {
        self.current = submenu;
        self.message = None; // Clear any pending message
    }

    /// Go back to main menu
    pub fn go_back(&mut self) {
        self.current = CurrentSubMenu::None;
        self.message = None;
    }

    /// Set a message to display
    pub fn set_message(&mut self, message: String) {
        self.message = Some(message);
    }

    /// Clear the message
    pub fn clear_message(&mut self) {
        self.message = None;
    }

    /// Check if a message is pending
    pub fn has_message(&self) -> bool {
        self.message.is_some()
    }
}

// ============================================================================
// ActionResult - Result of processing a SubMenuAction
// ============================================================================

/// Result of processing a submenu action in the TUI event loop
#[derive(Debug, Clone, PartialEq)]
pub enum ActionResult {
    /// No action needed
    None,
    /// Stay in current menu/submenu
    Stay,
    /// Navigate to a different submenu
    NavigateToSubMenu(CurrentSubMenu),
    /// Navigate to an operational mode (requires AppMode change)
    NavigateToMode(NavigationTarget),
    /// Navigate to a config screen (T-4.4)
    NavigateToConfigScreen(NavigationTarget),
    /// Navigate to a results screen (T-4.4)
    NavigateToResultsScreen(NavigationTarget),
    /// Show a message dialog
    ShowMessage(String),
    /// Execute a CLI command (the tui.rs will handle execution)
    ExecuteCommand(crate::ui::submenu::CliCommand),
    /// Quit the application
    Quit,
}

/// Process a SubMenuAction and return what the TUI should do
pub fn process_action(action: SubMenuAction) -> ActionResult {
    match action {
        SubMenuAction::None => ActionResult::None,
        SubMenuAction::Back => ActionResult::NavigateToSubMenu(CurrentSubMenu::None),
        SubMenuAction::Navigate(target) => {
            // Check if it's a config screen (T-4.4)
            if is_config_screen_target(&target) {
                ActionResult::NavigateToConfigScreen(target)
            }
            // Check if it's a results screen (T-4.4)
            else if is_results_screen_target(&target) {
                ActionResult::NavigateToResultsScreen(target)
            }
            // Check if it's a submenu navigation
            else if let Some(submenu) = CurrentSubMenu::from_navigation_target(&target) {
                ActionResult::NavigateToSubMenu(submenu)
            } else {
                ActionResult::NavigateToMode(target)
            }
        }
        SubMenuAction::ExecuteCommand(cmd) => ActionResult::ExecuteCommand(cmd),
        SubMenuAction::ShowMessage(msg) => ActionResult::ShowMessage(msg),
        SubMenuAction::Refresh => ActionResult::Stay,
    }
}

/// Check if a NavigationTarget is a config screen (T-4.4)
pub fn is_config_screen_target(target: &NavigationTarget) -> bool {
    matches!(target,
        NavigationTarget::BacktestEvaluateConfig
        | NavigationTarget::BacktestTuneConfig
        | NavigationTarget::BacktestRegimeSearchConfig
        | NavigationTarget::BacktestMultiObjectiveConfig
        | NavigationTarget::BacktestRegimeOptimizeConfig
        | NavigationTarget::BacktestTrainConfig
        | NavigationTarget::BacktestWalkForwardMLConfig
        | NavigationTarget::BacktestSweepConfig
        | NavigationTarget::BacktestWalkForwardConfig
        | NavigationTarget::BacktestOOSValidateConfig
        | NavigationTarget::BacktestSimulateConfig
        | NavigationTarget::BacktestGridConfig
        | NavigationTarget::BacktestCampaignConfig
        | NavigationTarget::BacktestPaperConfig
        | NavigationTarget::ResearchRunConfig
        | NavigationTarget::ResearchStatusConfig
        | NavigationTarget::ValidateRunConfig
        | NavigationTarget::ValidateShowConfig
        | NavigationTarget::ValidateStatusConfig
        | NavigationTarget::AlgorithmListConfig
        | NavigationTarget::AlgorithmShowConfig
    )
}

/// Check if a NavigationTarget is a results screen (T-4.4)
pub fn is_results_screen_target(target: &NavigationTarget) -> bool {
    matches!(target,
        NavigationTarget::BacktestEvaluateResults
        | NavigationTarget::BacktestTuneResults
        | NavigationTarget::BacktestRegimeSearchResults
        | NavigationTarget::BacktestMultiObjectiveResults
        | NavigationTarget::BacktestRegimeOptimizeResults
        | NavigationTarget::BacktestTrainResults
        | NavigationTarget::BacktestWalkForwardMLResults
        | NavigationTarget::BacktestSweepResults
        | NavigationTarget::BacktestWalkForwardResults
        | NavigationTarget::BacktestOOSValidateResults
        | NavigationTarget::BacktestSimulateResults
        | NavigationTarget::BacktestGridResults
        | NavigationTarget::BacktestCampaignResults
        | NavigationTarget::BacktestPaperResults
        | NavigationTarget::ResearchRunResults
        | NavigationTarget::ValidateRunResults
        | NavigationTarget::AlgorithmCreateResults
    )
}

// ============================================================================
// Drawing Functions
// ============================================================================

/// Draw the current submenu with status bar
pub fn draw_menu_with_status(
    f: &mut Frame,
    integration: &MenuIntegration,
    state: &GlobalState,
) {
    let size = f.area();

    // Create layout with status bar at bottom
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(10),    // Menu content
            Constraint::Length(1),  // Status bar
        ])
        .split(size);

    // Draw the current menu/submenu
    draw_current_menu(f, chunks[0], integration, state);

    // Draw status bar
    draw_status_bar(f, chunks[1], state);
}

/// Draw the current menu or submenu
pub fn draw_current_menu(
    f: &mut Frame,
    area: Rect,
    integration: &MenuIntegration,
    state: &GlobalState,
) {
    match integration.current {
        CurrentSubMenu::None => {
            // Main menu is drawn by draw_main_menu in tui.rs
            // This function is called when we're in a submenu
        }
        CurrentSubMenu::Research => {
            draw_research_menu(f, area, &integration.research_menu, state);
        }
        CurrentSubMenu::Algorithms => {
            draw_algorithms_menu(f, area, &integration.algorithms_menu, state);
        }
        CurrentSubMenu::Validate => {
            draw_validate_menu(f, area, &integration.validate_menu, state);
        }
        CurrentSubMenu::Trade => {
            draw_trade_menu(f, area, &integration.trade_menu, state);
        }
        CurrentSubMenu::Data => {
            draw_data_menu(f, area, &integration.data_menu, state);
        }
    }

    // Draw message dialog if present
    if let Some(ref msg) = integration.message {
        crate::ui::submenu::draw_message_dialog(f, area, msg);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::submenu::CliCommand;

    // -------------------------------------------------------------------------
    // CurrentSubMenu Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_current_submenu_default() {
        let submenu: CurrentSubMenu = Default::default();
        assert_eq!(submenu, CurrentSubMenu::None);
    }

    #[test]
    fn test_current_submenu_is_main_menu() {
        assert!(CurrentSubMenu::None.is_main_menu());
        assert!(!CurrentSubMenu::Research.is_main_menu());
        assert!(!CurrentSubMenu::Algorithms.is_main_menu());
        assert!(!CurrentSubMenu::Validate.is_main_menu());
        assert!(!CurrentSubMenu::Trade.is_main_menu());
        assert!(!CurrentSubMenu::Data.is_main_menu());
    }

    #[test]
    fn test_current_submenu_is_submenu_active() {
        assert!(!CurrentSubMenu::None.is_submenu_active());
        assert!(CurrentSubMenu::Research.is_submenu_active());
        assert!(CurrentSubMenu::Algorithms.is_submenu_active());
        assert!(CurrentSubMenu::Validate.is_submenu_active());
        assert!(CurrentSubMenu::Trade.is_submenu_active());
        assert!(CurrentSubMenu::Data.is_submenu_active());
    }

    #[test]
    fn test_current_submenu_display_name() {
        assert_eq!(CurrentSubMenu::None.display_name(), "Main Menu");
        assert_eq!(CurrentSubMenu::Research.display_name(), "Research");
        assert_eq!(CurrentSubMenu::Algorithms.display_name(), "Algorithms");
        assert_eq!(CurrentSubMenu::Validate.display_name(), "Validate");
        assert_eq!(CurrentSubMenu::Trade.display_name(), "Trade");
        assert_eq!(CurrentSubMenu::Data.display_name(), "Data");
    }

    #[test]
    fn test_current_submenu_from_navigation_target_submenus() {
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::MainMenu),
            Some(CurrentSubMenu::None)
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::ResearchMenu),
            Some(CurrentSubMenu::Research)
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::AlgorithmsMenu),
            Some(CurrentSubMenu::Algorithms)
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::ValidateMenu),
            Some(CurrentSubMenu::Validate)
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::TradeMenu),
            Some(CurrentSubMenu::Trade)
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::DataMenu),
            Some(CurrentSubMenu::Data)
        );
    }

    #[test]
    fn test_current_submenu_from_navigation_target_modes() {
        // Operational modes should return None (not submenus)
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::Live),
            None
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::Backtest),
            None
        );
        assert_eq!(
            CurrentSubMenu::from_navigation_target(&NavigationTarget::Research),
            None
        );
    }

    // -------------------------------------------------------------------------
    // MenuIntegration Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_menu_integration_default() {
        let integration = MenuIntegration::default();
        assert_eq!(integration.current, CurrentSubMenu::None);
        assert!(integration.message.is_none());
    }

    #[test]
    fn test_menu_integration_new() {
        let integration = MenuIntegration::new();
        assert_eq!(integration.current, CurrentSubMenu::None);
    }

    #[test]
    fn test_menu_integration_navigate_to() {
        let mut integration = MenuIntegration::new();

        integration.navigate_to(CurrentSubMenu::Research);
        assert_eq!(integration.current, CurrentSubMenu::Research);

        integration.navigate_to(CurrentSubMenu::Algorithms);
        assert_eq!(integration.current, CurrentSubMenu::Algorithms);

        integration.navigate_to(CurrentSubMenu::None);
        assert_eq!(integration.current, CurrentSubMenu::None);
    }

    #[test]
    fn test_menu_integration_navigate_clears_message() {
        let mut integration = MenuIntegration::new();
        integration.set_message("Test message".to_string());
        assert!(integration.has_message());

        integration.navigate_to(CurrentSubMenu::Research);
        assert!(!integration.has_message());
    }

    #[test]
    fn test_menu_integration_go_back() {
        let mut integration = MenuIntegration::new();
        integration.navigate_to(CurrentSubMenu::Research);
        assert_eq!(integration.current, CurrentSubMenu::Research);

        integration.go_back();
        assert_eq!(integration.current, CurrentSubMenu::None);
    }

    #[test]
    fn test_menu_integration_go_back_clears_message() {
        let mut integration = MenuIntegration::new();
        integration.navigate_to(CurrentSubMenu::Research);
        integration.set_message("Test".to_string());

        integration.go_back();
        assert!(!integration.has_message());
    }

    #[test]
    fn test_menu_integration_set_message() {
        let mut integration = MenuIntegration::new();

        integration.set_message("Hello".to_string());
        assert!(integration.has_message());
        assert_eq!(integration.message, Some("Hello".to_string()));
    }

    #[test]
    fn test_menu_integration_clear_message() {
        let mut integration = MenuIntegration::new();
        integration.set_message("Test".to_string());

        integration.clear_message();
        assert!(!integration.has_message());
        assert_eq!(integration.message, None);
    }

    #[test]
    fn test_menu_integration_has_message() {
        let mut integration = MenuIntegration::new();
        assert!(!integration.has_message());

        integration.set_message("Test".to_string());
        assert!(integration.has_message());

        integration.clear_message();
        assert!(!integration.has_message());
    }

    // -------------------------------------------------------------------------
    // ActionResult Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_action_result_none() {
        let result = ActionResult::None;
        assert_eq!(result, ActionResult::None);
    }

    #[test]
    fn test_action_result_stay() {
        let result = ActionResult::Stay;
        assert_eq!(result, ActionResult::Stay);
    }

    #[test]
    fn test_action_result_navigate_to_submenu() {
        let result = ActionResult::NavigateToSubMenu(CurrentSubMenu::Research);
        assert_eq!(result, ActionResult::NavigateToSubMenu(CurrentSubMenu::Research));
    }

    #[test]
    fn test_action_result_navigate_to_mode() {
        let result = ActionResult::NavigateToMode(NavigationTarget::Backtest);
        assert_eq!(result, ActionResult::NavigateToMode(NavigationTarget::Backtest));
    }

    #[test]
    fn test_action_result_show_message() {
        let result = ActionResult::ShowMessage("Test".to_string());
        assert_eq!(result, ActionResult::ShowMessage("Test".to_string()));
    }

    #[test]
    fn test_action_result_execute_command() {
        let cmd = CliCommand::research("run");
        let result = ActionResult::ExecuteCommand(cmd.clone());
        assert_eq!(result, ActionResult::ExecuteCommand(cmd));
    }

    #[test]
    fn test_action_result_quit() {
        let result = ActionResult::Quit;
        assert_eq!(result, ActionResult::Quit);
    }

    // -------------------------------------------------------------------------
    // process_action Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_process_action_none() {
        let result = process_action(SubMenuAction::None);
        assert_eq!(result, ActionResult::None);
    }

    #[test]
    fn test_process_action_back() {
        let result = process_action(SubMenuAction::Back);
        assert_eq!(result, ActionResult::NavigateToSubMenu(CurrentSubMenu::None));
    }

    #[test]
    fn test_process_action_navigate_submenu() {
        let result = process_action(SubMenuAction::Navigate(NavigationTarget::ResearchMenu));
        assert_eq!(result, ActionResult::NavigateToSubMenu(CurrentSubMenu::Research));

        let result = process_action(SubMenuAction::Navigate(NavigationTarget::AlgorithmsMenu));
        assert_eq!(result, ActionResult::NavigateToSubMenu(CurrentSubMenu::Algorithms));
    }

    #[test]
    fn test_process_action_navigate_mode() {
        let result = process_action(SubMenuAction::Navigate(NavigationTarget::Backtest));
        assert_eq!(result, ActionResult::NavigateToMode(NavigationTarget::Backtest));

        let result = process_action(SubMenuAction::Navigate(NavigationTarget::Live));
        assert_eq!(result, ActionResult::NavigateToMode(NavigationTarget::Live));
    }

    #[test]
    fn test_process_action_execute_command() {
        let cmd = CliCommand::validate("backtest", vec![]);
        let result = process_action(SubMenuAction::ExecuteCommand(cmd.clone()));
        assert_eq!(result, ActionResult::ExecuteCommand(cmd));
    }

    #[test]
    fn test_process_action_show_message() {
        let result = process_action(SubMenuAction::ShowMessage("Test message".to_string()));
        assert_eq!(result, ActionResult::ShowMessage("Test message".to_string()));
    }

    #[test]
    fn test_process_action_refresh() {
        let result = process_action(SubMenuAction::Refresh);
        assert_eq!(result, ActionResult::Stay);
    }

    #[test]
    fn test_process_action_navigate_to_config_screen() {
        let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateConfig);
        let result = process_action(action);
        assert_eq!(
            result,
            ActionResult::NavigateToConfigScreen(NavigationTarget::BacktestEvaluateConfig)
        );
    }

    #[test]
    fn test_process_action_navigate_to_results_screen() {
        let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateResults);
        let result = process_action(action);
        assert_eq!(
            result,
            ActionResult::NavigateToResultsScreen(NavigationTarget::BacktestEvaluateResults)
        );
    }

    #[test]
    fn test_is_config_screen_target() {
        assert!(is_config_screen_target(&NavigationTarget::BacktestEvaluateConfig));
        assert!(is_config_screen_target(&NavigationTarget::BacktestTuneConfig));
        assert!(is_config_screen_target(&NavigationTarget::ResearchRunConfig));
        assert!(is_config_screen_target(&NavigationTarget::ValidateRunConfig));
        assert!(is_config_screen_target(&NavigationTarget::AlgorithmListConfig));
        assert!(!is_config_screen_target(&NavigationTarget::MainMenu));
        assert!(!is_config_screen_target(&NavigationTarget::BacktestEvaluateResults));
    }

    #[test]
    fn test_is_results_screen_target() {
        assert!(is_results_screen_target(&NavigationTarget::BacktestEvaluateResults));
        assert!(is_results_screen_target(&NavigationTarget::BacktestTuneResults));
        assert!(is_results_screen_target(&NavigationTarget::ResearchRunResults));
        assert!(is_results_screen_target(&NavigationTarget::ValidateRunResults));
        assert!(!is_results_screen_target(&NavigationTarget::MainMenu));
        assert!(!is_results_screen_target(&NavigationTarget::BacktestEvaluateConfig));
    }

    // -------------------------------------------------------------------------
    // Navigation Flow Tests (T-4.5)
    // -------------------------------------------------------------------------

    #[test]
    fn test_navigation_flow_menu_to_config() {
        // Test that menu navigation to config screen works
        let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateConfig);
        let result = process_action(action);
        assert_eq!(
            result,
            ActionResult::NavigateToConfigScreen(NavigationTarget::BacktestEvaluateConfig)
        );
    }

    #[test]
    fn test_navigation_flow_config_to_results() {
        // Test that config screen can navigate to results screen
        let action = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateResults);
        let result = process_action(action);
        assert_eq!(
            result,
            ActionResult::NavigateToResultsScreen(NavigationTarget::BacktestEvaluateResults)
        );
    }

    #[test]
    fn test_navigation_flow_back_from_config() {
        // Test that back navigation from config screen works
        let action = SubMenuAction::Back;
        let result = process_action(action);
        assert_eq!(result, ActionResult::NavigateToSubMenu(CurrentSubMenu::None));
    }

    #[test]
    fn test_all_backtest_config_screens_are_detected() {
        // Test that all backtest config screens are properly detected
        let config_targets = vec![
            NavigationTarget::BacktestEvaluateConfig,
            NavigationTarget::BacktestTuneConfig,
            NavigationTarget::BacktestRegimeSearchConfig,
            NavigationTarget::BacktestMultiObjectiveConfig,
            NavigationTarget::BacktestRegimeOptimizeConfig,
            NavigationTarget::BacktestTrainConfig,
            NavigationTarget::BacktestWalkForwardMLConfig,
            NavigationTarget::BacktestSweepConfig,
            NavigationTarget::BacktestWalkForwardConfig,
            NavigationTarget::BacktestOOSValidateConfig,
            NavigationTarget::BacktestSimulateConfig,
            NavigationTarget::BacktestGridConfig,
            NavigationTarget::BacktestCampaignConfig,
            NavigationTarget::BacktestPaperConfig,
        ];

        for target in config_targets {
            assert!(
                is_config_screen_target(&target),
                "Config screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_all_backtest_results_screens_are_detected() {
        // Test that all backtest results screens are properly detected
        let results_targets = vec![
            NavigationTarget::BacktestEvaluateResults,
            NavigationTarget::BacktestTuneResults,
            NavigationTarget::BacktestRegimeSearchResults,
            NavigationTarget::BacktestMultiObjectiveResults,
            NavigationTarget::BacktestRegimeOptimizeResults,
            NavigationTarget::BacktestTrainResults,
            NavigationTarget::BacktestWalkForwardMLResults,
            NavigationTarget::BacktestSweepResults,
            NavigationTarget::BacktestWalkForwardResults,
            NavigationTarget::BacktestOOSValidateResults,
            NavigationTarget::BacktestSimulateResults,
            NavigationTarget::BacktestGridResults,
            NavigationTarget::BacktestCampaignResults,
            NavigationTarget::BacktestPaperResults,
        ];

        for target in results_targets {
            assert!(
                is_results_screen_target(&target),
                "Results screen target {:?} should be detected",
                target
            );
        }
    }

    #[test]
    fn test_navigation_flow_complete_cycle() {
        // Test a complete navigation cycle: menu -> config -> results -> menu
        // Menu to config
        let action1 = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateConfig);
        let result1 = process_action(action1);
        assert_eq!(
            result1,
            ActionResult::NavigateToConfigScreen(NavigationTarget::BacktestEvaluateConfig)
        );

        // Config to results (simulated - would happen after command execution)
        let action2 = SubMenuAction::Navigate(NavigationTarget::BacktestEvaluateResults);
        let result2 = process_action(action2);
        assert_eq!(
            result2,
            ActionResult::NavigateToResultsScreen(NavigationTarget::BacktestEvaluateResults)
        );

        // Results back to menu
        let action3 = SubMenuAction::Back;
        let result3 = process_action(action3);
        assert_eq!(result3, ActionResult::NavigateToSubMenu(CurrentSubMenu::None));
    }

    // -------------------------------------------------------------------------
    // Navigation Flow Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_navigation_flow_main_to_submenu_and_back() {
        let mut integration = MenuIntegration::new();

        // Start at main menu
        assert!(integration.current.is_main_menu());

        // Navigate to Research
        integration.navigate_to(CurrentSubMenu::Research);
        assert_eq!(integration.current, CurrentSubMenu::Research);
        assert!(integration.current.is_submenu_active());

        // Go back
        integration.go_back();
        assert!(integration.current.is_main_menu());
    }

    #[test]
    fn test_navigation_flow_between_submenus() {
        let mut integration = MenuIntegration::new();

        // Navigate through all submenus
        for submenu in [
            CurrentSubMenu::Research,
            CurrentSubMenu::Algorithms,
            CurrentSubMenu::Validate,
            CurrentSubMenu::Trade,
            CurrentSubMenu::Data,
        ] {
            integration.navigate_to(submenu);
            assert_eq!(integration.current, submenu);
        }
    }

    #[test]
    fn test_message_dialog_flow() {
        let mut integration = MenuIntegration::new();

        // Set message
        integration.set_message("Research must be complete".to_string());
        assert!(integration.has_message());

        // Navigating should clear message
        integration.navigate_to(CurrentSubMenu::Research);
        assert!(!integration.has_message());

        // Set another message
        integration.set_message("Test".to_string());
        assert!(integration.has_message());

        // Clear explicitly
        integration.clear_message();
        assert!(!integration.has_message());
    }

    // -------------------------------------------------------------------------
    // Edge Cases
    // -------------------------------------------------------------------------

    #[test]
    fn test_multiple_back_from_main_menu() {
        let mut integration = MenuIntegration::new();

        // Already at main menu
        integration.go_back();
        assert!(integration.current.is_main_menu());

        // Multiple backs should be safe
        integration.go_back();
        integration.go_back();
        assert!(integration.current.is_main_menu());
    }

    #[test]
    fn test_navigate_to_same_submenu() {
        let mut integration = MenuIntegration::new();

        integration.navigate_to(CurrentSubMenu::Research);
        integration.set_message("Test".to_string());

        // Navigating to same submenu should still clear message
        integration.navigate_to(CurrentSubMenu::Research);
        assert!(!integration.has_message());
        assert_eq!(integration.current, CurrentSubMenu::Research);
    }

    #[test]
    fn test_empty_message() {
        let mut integration = MenuIntegration::new();

        integration.set_message("".to_string());
        assert!(integration.has_message()); // Empty string is still Some

        integration.clear_message();
        assert!(!integration.has_message());
    }

    #[test]
    fn test_long_message() {
        let mut integration = MenuIntegration::new();

        let long_msg = "A".repeat(1000);
        integration.set_message(long_msg.clone());
        assert_eq!(integration.message, Some(long_msg));
    }

    // -------------------------------------------------------------------------
    // All SubMenu Variants Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_all_submenu_variants_clone() {
        let submenus = vec![
            CurrentSubMenu::None,
            CurrentSubMenu::Research,
            CurrentSubMenu::Algorithms,
            CurrentSubMenu::Validate,
            CurrentSubMenu::Trade,
            CurrentSubMenu::Data,
        ];

        for submenu in submenus {
            let cloned = submenu.clone();
            assert_eq!(submenu, cloned);
        }
    }

    #[test]
    fn test_all_submenu_variants_copy() {
        let submenu = CurrentSubMenu::Research;
        let copied = submenu; // Copy
        assert_eq!(submenu, copied); // Original still valid
    }

    #[test]
    fn test_all_submenu_variants_debug() {
        // Ensure Debug is implemented
        let submenu = CurrentSubMenu::Research;
        let debug_str = format!("{:?}", submenu);
        assert!(!debug_str.is_empty());
    }
}
