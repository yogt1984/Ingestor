//! Research Menu implementation (TUI-1.0)
//!
//! Provides a submenu for research-related operations:
//! - [R] Run Research: Analyze historical data for edges
//! - [S] Status: View current research state
//! - [C] Create Config: Generate algorithm config from findings
//!
//! The "Create Config" option is disabled until research is complete.

use crossterm::event::KeyCode;

use crate::ui::state::{GlobalState, ResearchStatus};
use crate::ui::submenu::{
    SubMenu, SubMenuAction, SubMenuItem, NavigationTarget, CliCommand,
    key_to_char, is_back_key,
};

// ============================================================================
// ResearchMenu
// ============================================================================

/// Research submenu implementing the SubMenu trait
#[derive(Debug, Clone, Default)]
pub struct ResearchMenu;

impl ResearchMenu {
    /// Create a new ResearchMenu
    pub fn new() -> Self {
        Self
    }

    /// Check if research is complete (tradeable edge found)
    fn is_research_complete(state: &GlobalState) -> bool {
        matches!(state.research_status, ResearchStatus::Complete { tradeable: true })
    }

    /// Get status indicator based on research state
    fn get_status_indicator(state: &GlobalState) -> Option<String> {
        match &state.research_status {
            ResearchStatus::Idle => Some("○ Not run".to_string()),
            ResearchStatus::Running { samples_processed } => {
                Some(format!("◐ Running ({} samples)", samples_processed))
            }
            ResearchStatus::Complete { tradeable: true } => Some("✓ Tradeable".to_string()),
            ResearchStatus::Complete { tradeable: false } => Some("✗ No edge".to_string()),
        }
    }
}

impl SubMenu for ResearchMenu {
    fn title(&self) -> &str {
        "RESEARCH - Edge Detection"
    }

    fn items(&self, state: &GlobalState) -> Vec<SubMenuItem> {
        let research_complete = Self::is_research_complete(state);
        let status = Self::get_status_indicator(state);

        vec![
            SubMenuItem::new('R', "Run Research", "Analyze historical data for edges")
                .with_status(status.clone().unwrap_or_default()),
            SubMenuItem::new('S', "Status", "View current research state"),
            SubMenuItem::new('C', "Create Config", "Generate algorithm from findings")
                .with_enabled(research_complete)
                .with_status(if research_complete {
                    "Ready".to_string()
                } else {
                    "Requires research".to_string()
                }),
        ]
    }

    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction {
        if is_back_key(key) {
            return SubMenuAction::Back;
        }

        if let Some(c) = key_to_char(key) {
            match c.to_ascii_lowercase() {
                'r' => {
                    // Run research - execute CLI command
                    SubMenuAction::ExecuteCommand(CliCommand::research("run"))
                }
                's' => {
                    // Show status - navigate to research dashboard
                    SubMenuAction::Navigate(NavigationTarget::Research)
                }
                'c' => {
                    // Create config - only if research complete
                    if Self::is_research_complete(state) {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::algorithm("create", vec!["--from-research"])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "Research must be complete before creating config".to_string()
                        )
                    }
                }
                _ => SubMenuAction::None,
            }
        } else {
            SubMenuAction::None
        }
    }

    fn footer(&self, state: &GlobalState) -> Option<String> {
        match &state.research_status {
            ResearchStatus::Idle => None,
            ResearchStatus::Running { samples_processed } => {
                Some(format!("Research in progress: {} samples processed", samples_processed))
            }
            ResearchStatus::Complete { tradeable } => {
                if *tradeable {
                    Some("Research complete - tradeable edge detected".to_string())
                } else {
                    Some("Research complete - no tradeable edge found".to_string())
                }
            }
        }
    }

    fn can_enter(&self, _state: &GlobalState) -> bool {
        // Research menu is always accessible
        true
    }

    fn blocked_message(&self, _state: &GlobalState) -> Option<String> {
        // Never blocked
        None
    }
}

// ============================================================================
// Drawing function
// ============================================================================

/// Draw the research menu
pub fn draw_research_menu(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    menu: &ResearchMenu,
    state: &GlobalState,
) {
    crate::ui::submenu::draw_submenu_frame(f, area, menu, state);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Helper for creating test state
    // -------------------------------------------------------------------------

    fn create_test_state(research_status: ResearchStatus) -> GlobalState {
        GlobalState {
            symbol: "BTCUSDT".to_string(),
            active_algorithm: None,
            validation_status: crate::ui::state::ValidationStatus::default(),
            trading_mode: crate::ui::state::TradingMode::Idle,
            research_status,
            data_stats: crate::ui::state::DataStats::default(),
        }
    }

    // -------------------------------------------------------------------------
    // Construction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_research_menu_new() {
        let menu = ResearchMenu::new();
        assert_eq!(menu.title(), "RESEARCH - Edge Detection");
    }

    #[test]
    fn test_research_menu_default() {
        let menu = ResearchMenu::default();
        assert_eq!(menu.title(), "RESEARCH - Edge Detection");
    }

    // -------------------------------------------------------------------------
    // Title tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_title() {
        let menu = ResearchMenu::new();
        assert_eq!(menu.title(), "RESEARCH - Edge Detection");
    }

    // -------------------------------------------------------------------------
    // Items tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_items_idle_state() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);
        let items = menu.items(&state);

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].key, 'R');
        assert_eq!(items[0].label, "Run Research");
        assert!(items[0].enabled);

        assert_eq!(items[1].key, 'S');
        assert_eq!(items[1].label, "Status");
        assert!(items[1].enabled);

        assert_eq!(items[2].key, 'C');
        assert_eq!(items[2].label, "Create Config");
        assert!(!items[2].enabled); // Disabled when research not complete
    }

    #[test]
    fn test_items_running_state() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Running { samples_processed: 1000 });
        let items = menu.items(&state);

        assert!(items[0].status.as_ref().unwrap().contains("Running"));
        assert!(items[0].status.as_ref().unwrap().contains("1000"));
    }

    #[test]
    fn test_items_complete_tradeable() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: true });
        let items = menu.items(&state);

        assert!(items[0].status.as_ref().unwrap().contains("Tradeable"));
        assert!(items[2].enabled); // Create Config enabled
        assert_eq!(items[2].status.as_ref().unwrap(), "Ready");
    }

    #[test]
    fn test_items_complete_not_tradeable() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: false });
        let items = menu.items(&state);

        assert!(items[0].status.as_ref().unwrap().contains("No edge"));
        assert!(!items[2].enabled); // Create Config still disabled
    }

    // -------------------------------------------------------------------------
    // Key handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_handle_key_escape() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Esc, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Backspace, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_r_run_research() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Char('r'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "research");
            assert_eq!(cmd.args, vec!["run"]);
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_r_uppercase() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Char('R'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "research");
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_s_status() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Char('s'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::Research));
    }

    #[test]
    fn test_handle_key_c_create_config_disabled() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Char('c'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("complete"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_c_create_config_enabled() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: true });

        let action = menu.handle_key(KeyCode::Char('c'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "algorithm");
            assert!(cmd.args.contains(&"create".to_string()));
            assert!(cmd.args.contains(&"--from-research".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_c_create_config_not_tradeable() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: false });

        let action = menu.handle_key(KeyCode::Char('c'), &state);
        // Even if complete, must be tradeable
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("complete"));
        } else {
            panic!("Expected ShowMessage action for non-tradeable");
        }
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Char('x'), &state);
        assert_eq!(action, SubMenuAction::None);
    }

    #[test]
    fn test_handle_key_enter() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        let action = menu.handle_key(KeyCode::Enter, &state);
        assert_eq!(action, SubMenuAction::None);
    }

    // -------------------------------------------------------------------------
    // Footer tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_footer_idle() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);

        assert!(menu.footer(&state).is_none());
    }

    #[test]
    fn test_footer_running() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Running { samples_processed: 500 });

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        assert!(footer.unwrap().contains("500"));
    }

    #[test]
    fn test_footer_complete_tradeable() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: true });

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        assert!(footer.unwrap().contains("tradeable"));
    }

    #[test]
    fn test_footer_complete_not_tradeable() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: false });

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        assert!(footer.unwrap().contains("no tradeable"));
    }

    // -------------------------------------------------------------------------
    // Access control tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_can_enter_always_true() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);
        assert!(menu.can_enter(&state));
    }

    #[test]
    fn test_blocked_message_always_none() {
        let menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Idle);
        assert!(menu.blocked_message(&state).is_none());
    }

    // -------------------------------------------------------------------------
    // Helper function tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_is_research_complete_idle() {
        let state = create_test_state(ResearchStatus::Idle);
        assert!(!ResearchMenu::is_research_complete(&state));
    }

    #[test]
    fn test_is_research_complete_running() {
        let state = create_test_state(ResearchStatus::Running { samples_processed: 100 });
        assert!(!ResearchMenu::is_research_complete(&state));
    }

    #[test]
    fn test_is_research_complete_complete_tradeable() {
        let state = create_test_state(ResearchStatus::Complete { tradeable: true });
        assert!(ResearchMenu::is_research_complete(&state));
    }

    #[test]
    fn test_is_research_complete_complete_not_tradeable() {
        let state = create_test_state(ResearchStatus::Complete { tradeable: false });
        assert!(!ResearchMenu::is_research_complete(&state));
    }

    #[test]
    fn test_get_status_indicator_idle() {
        let state = create_test_state(ResearchStatus::Idle);
        let status = ResearchMenu::get_status_indicator(&state);
        assert!(status.is_some());
        assert!(status.unwrap().contains("Not run"));
    }

    #[test]
    fn test_get_status_indicator_running() {
        let state = create_test_state(ResearchStatus::Running { samples_processed: 2000 });
        let status = ResearchMenu::get_status_indicator(&state);
        assert!(status.is_some());
        let s = status.unwrap();
        assert!(s.contains("Running"));
        assert!(s.contains("2000"));
    }

    #[test]
    fn test_get_status_indicator_complete_tradeable() {
        let state = create_test_state(ResearchStatus::Complete { tradeable: true });
        let status = ResearchMenu::get_status_indicator(&state);
        assert!(status.is_some());
        assert!(status.unwrap().contains("Tradeable"));
    }

    #[test]
    fn test_get_status_indicator_complete_not_tradeable() {
        let state = create_test_state(ResearchStatus::Complete { tradeable: false });
        let status = ResearchMenu::get_status_indicator(&state);
        assert!(status.is_some());
        assert!(status.unwrap().contains("No edge"));
    }

    // -------------------------------------------------------------------------
    // Clone and Debug tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_research_menu_clone() {
        let menu = ResearchMenu::new();
        let cloned = menu.clone();
        assert_eq!(menu.title(), cloned.title());
    }

    #[test]
    fn test_research_menu_debug() {
        let menu = ResearchMenu::new();
        let debug_str = format!("{:?}", menu);
        assert!(debug_str.contains("ResearchMenu"));
    }

    // -------------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_workflow_research_then_create() {
        let mut menu = ResearchMenu::new();

        // Initially idle - can't create config
        let state_idle = create_test_state(ResearchStatus::Idle);
        let action = menu.handle_key(KeyCode::Char('c'), &state_idle);
        assert!(matches!(action, SubMenuAction::ShowMessage(_)));

        // After research completes - can create config
        let state_complete = create_test_state(ResearchStatus::Complete { tradeable: true });
        let action = menu.handle_key(KeyCode::Char('c'), &state_complete);
        assert!(matches!(action, SubMenuAction::ExecuteCommand(_)));
    }

    #[test]
    fn test_all_keys_case_insensitive() {
        let mut menu = ResearchMenu::new();
        let state = create_test_state(ResearchStatus::Complete { tradeable: true });

        // Test lowercase
        let action_r = menu.handle_key(KeyCode::Char('r'), &state);
        let action_s = menu.handle_key(KeyCode::Char('s'), &state);
        let action_c = menu.handle_key(KeyCode::Char('c'), &state);

        // Test uppercase
        let action_R = menu.handle_key(KeyCode::Char('R'), &state);
        let action_S = menu.handle_key(KeyCode::Char('S'), &state);
        let action_C = menu.handle_key(KeyCode::Char('C'), &state);

        // Same actions for both cases
        assert!(matches!(action_r, SubMenuAction::ExecuteCommand(_)));
        assert!(matches!(action_R, SubMenuAction::ExecuteCommand(_)));
        assert!(matches!(action_s, SubMenuAction::Navigate(_)));
        assert!(matches!(action_S, SubMenuAction::Navigate(_)));
        assert!(matches!(action_c, SubMenuAction::ExecuteCommand(_)));
        assert!(matches!(action_C, SubMenuAction::ExecuteCommand(_)));
    }
}
