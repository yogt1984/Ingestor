//! Algorithms Menu implementation (TUI-2.0)
//!
//! Provides a submenu for algorithm configuration operations:
//! - [L] List: Browse saved configurations
//! - [S] Select: Activate an algorithm config
//! - [V] View: Show active config details
//! - [N] New: Create config manually
//! - [1] Momentum: Filter by Momentum strategy
//! - [2] Market Making: Filter by Market Making strategy
//! - [3] Hybrid: Filter by Hybrid strategy
//!
//! The "View" option is disabled when no algorithm is selected.

use crossterm::event::KeyCode;

use crate::ui::state::{GlobalState, AlgorithmConfigSummary};
use crate::ui::submenu::{
    SubMenu, SubMenuAction, SubMenuItem, NavigationTarget, CliCommand,
    key_to_char, is_back_key,
};

// ============================================================================
// Strategy Filter
// ============================================================================

/// Strategy type for filtering algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyFilter {
    All,
    Momentum,
    MarketMaking,
    Hybrid,
}

impl StrategyFilter {
    /// Get CLI argument for this filter
    pub fn as_cli_arg(&self) -> Option<&'static str> {
        match self {
            StrategyFilter::All => None,
            StrategyFilter::Momentum => Some("momentum"),
            StrategyFilter::MarketMaking => Some("market-making"),
            StrategyFilter::Hybrid => Some("hybrid"),
        }
    }

    /// Get display label
    pub fn label(&self) -> &'static str {
        match self {
            StrategyFilter::All => "All",
            StrategyFilter::Momentum => "Momentum",
            StrategyFilter::MarketMaking => "Market Making",
            StrategyFilter::Hybrid => "Hybrid",
        }
    }
}

impl Default for StrategyFilter {
    fn default() -> Self {
        StrategyFilter::All
    }
}

// ============================================================================
// AlgorithmsMenu
// ============================================================================

/// Algorithms submenu implementing the SubMenu trait
#[derive(Debug, Clone, Default)]
pub struct AlgorithmsMenu {
    /// Current strategy filter
    pub filter: StrategyFilter,
}

impl AlgorithmsMenu {
    /// Create a new AlgorithmsMenu
    pub fn new() -> Self {
        Self {
            filter: StrategyFilter::All,
        }
    }

    /// Check if an algorithm is currently active
    fn has_active_algorithm(state: &GlobalState) -> bool {
        state.active_algorithm.is_some()
    }

    /// Get active algorithm summary for display
    fn get_active_summary(state: &GlobalState) -> Option<String> {
        state.active_algorithm.as_ref().map(|algo| {
            format!("{} ({:?})", algo.name, algo.strategy_type)
        })
    }

    /// Build CLI command for listing algorithms
    fn build_list_command(&self) -> CliCommand {
        let args: Vec<&str> = if let Some(strategy) = self.filter.as_cli_arg() {
            vec!["--strategy", strategy]
        } else {
            vec![]
        };
        CliCommand::algorithm("list", args)
    }
}

impl SubMenu for AlgorithmsMenu {
    fn title(&self) -> &str {
        "ALGORITHMS - Strategy Configuration"
    }

    fn items(&self, state: &GlobalState) -> Vec<SubMenuItem> {
        let has_active = Self::has_active_algorithm(state);
        let active_status = Self::get_active_summary(state)
            .unwrap_or_else(|| "None selected".to_string());

        vec![
            // Management actions
            SubMenuItem::new('L', "List", "Browse saved configurations")
                .with_status(format!("Filter: {}", self.filter.label())),
            SubMenuItem::new('S', "Select", "Activate an algorithm config"),
            SubMenuItem::new('V', "View", "Show active config details")
                .with_enabled(has_active)
                .with_status(if has_active {
                    active_status.clone()
                } else {
                    "No algorithm selected".to_string()
                }),
            SubMenuItem::new('N', "New", "Create config manually"),

            // Strategy type filters
            SubMenuItem::new('1', "Momentum", "Trend-following (MIDC, persistence)")
                .with_status(if self.filter == StrategyFilter::Momentum {
                    "Active".to_string()
                } else {
                    String::new()
                }),
            SubMenuItem::new('2', "Market Making", "Spread capture (A-S, ML-spreads)")
                .with_status(if self.filter == StrategyFilter::MarketMaking {
                    "Active".to_string()
                } else {
                    String::new()
                }),
            SubMenuItem::new('3', "Hybrid", "Adaptive switching")
                .with_status(if self.filter == StrategyFilter::Hybrid {
                    "Active".to_string()
                } else {
                    String::new()
                }),
        ]
    }

    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction {
        if is_back_key(key) {
            return SubMenuAction::Back;
        }

        if let Some(c) = key_to_char(key) {
            match c.to_ascii_lowercase() {
                'l' => {
                    // List algorithms with current filter
                    SubMenuAction::ExecuteCommand(self.build_list_command())
                }
                's' => {
                    // Select/activate algorithm - interactive
                    SubMenuAction::Navigate(NavigationTarget::AlgorithmSelect)
                }
                'v' => {
                    // View active config
                    if let Some(algo) = &state.active_algorithm {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::algorithm("show", vec![algo.id.as_str()])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Use [S] Select first.".to_string()
                        )
                    }
                }
                'n' => {
                    // Create new config - launch wizard
                    SubMenuAction::Navigate(NavigationTarget::AlgorithmCreate)
                }
                '1' => {
                    // Filter: Momentum
                    self.filter = StrategyFilter::Momentum;
                    SubMenuAction::ExecuteCommand(self.build_list_command())
                }
                '2' => {
                    // Filter: Market Making
                    self.filter = StrategyFilter::MarketMaking;
                    SubMenuAction::ExecuteCommand(self.build_list_command())
                }
                '3' => {
                    // Filter: Hybrid
                    self.filter = StrategyFilter::Hybrid;
                    SubMenuAction::ExecuteCommand(self.build_list_command())
                }
                _ => SubMenuAction::None,
            }
        } else {
            SubMenuAction::None
        }
    }

    fn footer(&self, state: &GlobalState) -> Option<String> {
        if let Some(algo) = &state.active_algorithm {
            Some(format!(
                "Active: {} ({:?}) | Created: {}",
                algo.name,
                algo.strategy_type,
                algo.created_at.format("%Y-%m-%d")
            ))
        } else {
            Some("No algorithm selected - use [S] to select one".to_string())
        }
    }

    fn can_enter(&self, _state: &GlobalState) -> bool {
        // Algorithms menu is always accessible
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

/// Draw the algorithms menu
pub fn draw_algorithms_menu(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    menu: &AlgorithmsMenu,
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
    use crate::ui::state::{ResearchStatus, TradingMode, ValidationStatus, DataStats};
    use crate::core::algorithm_config::StrategyType;
    use chrono::Utc;

    // -------------------------------------------------------------------------
    // Helper for creating test state
    // -------------------------------------------------------------------------

    fn create_test_state(active_algorithm: Option<AlgorithmConfigSummary>) -> GlobalState {
        GlobalState {
            symbol: "BTCUSDT".to_string(),
            active_algorithm,
            validation_status: ValidationStatus::default(),
            trading_mode: TradingMode::Idle,
            research_status: ResearchStatus::Idle,
            data_stats: DataStats::default(),
        }
    }

    fn create_algorithm_summary(name: &str, strategy: StrategyType) -> AlgorithmConfigSummary {
        AlgorithmConfigSummary {
            id: format!("{}_{}", name.to_lowercase(), "20251228"),
            name: name.to_string(),
            strategy_type: strategy,
            created_at: Utc::now(),
        }
    }

    // -------------------------------------------------------------------------
    // Construction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_algorithms_menu_new() {
        let menu = AlgorithmsMenu::new();
        assert_eq!(menu.title(), "ALGORITHMS - Strategy Configuration");
        assert_eq!(menu.filter, StrategyFilter::All);
    }

    #[test]
    fn test_algorithms_menu_default() {
        let menu = AlgorithmsMenu::default();
        assert_eq!(menu.filter, StrategyFilter::All);
    }

    // -------------------------------------------------------------------------
    // Title tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_title() {
        let menu = AlgorithmsMenu::new();
        assert_eq!(menu.title(), "ALGORITHMS - Strategy Configuration");
    }

    // -------------------------------------------------------------------------
    // Strategy filter tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_strategy_filter_default() {
        let filter = StrategyFilter::default();
        assert_eq!(filter, StrategyFilter::All);
    }

    #[test]
    fn test_strategy_filter_cli_arg_all() {
        assert!(StrategyFilter::All.as_cli_arg().is_none());
    }

    #[test]
    fn test_strategy_filter_cli_arg_momentum() {
        assert_eq!(StrategyFilter::Momentum.as_cli_arg(), Some("momentum"));
    }

    #[test]
    fn test_strategy_filter_cli_arg_market_making() {
        assert_eq!(StrategyFilter::MarketMaking.as_cli_arg(), Some("market-making"));
    }

    #[test]
    fn test_strategy_filter_cli_arg_hybrid() {
        assert_eq!(StrategyFilter::Hybrid.as_cli_arg(), Some("hybrid"));
    }

    #[test]
    fn test_strategy_filter_label_all() {
        assert_eq!(StrategyFilter::All.label(), "All");
    }

    #[test]
    fn test_strategy_filter_label_momentum() {
        assert_eq!(StrategyFilter::Momentum.label(), "Momentum");
    }

    #[test]
    fn test_strategy_filter_label_market_making() {
        assert_eq!(StrategyFilter::MarketMaking.label(), "Market Making");
    }

    #[test]
    fn test_strategy_filter_label_hybrid() {
        assert_eq!(StrategyFilter::Hybrid.label(), "Hybrid");
    }

    // -------------------------------------------------------------------------
    // Items tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_items_no_active_algorithm() {
        let menu = AlgorithmsMenu::new();
        let state = create_test_state(None);
        let items = menu.items(&state);

        assert_eq!(items.len(), 7);

        // List always enabled
        assert_eq!(items[0].key, 'L');
        assert!(items[0].enabled);

        // Select always enabled
        assert_eq!(items[1].key, 'S');
        assert!(items[1].enabled);

        // View disabled when no algorithm
        assert_eq!(items[2].key, 'V');
        assert!(!items[2].enabled);
        assert!(items[2].status.as_ref().unwrap().contains("No algorithm"));

        // New always enabled
        assert_eq!(items[3].key, 'N');
        assert!(items[3].enabled);

        // Strategy filters
        assert_eq!(items[4].key, '1');
        assert_eq!(items[5].key, '2');
        assert_eq!(items[6].key, '3');
    }

    #[test]
    fn test_items_with_active_algorithm() {
        let menu = AlgorithmsMenu::new();
        let algo = create_algorithm_summary("momentum_btc_v3", StrategyType::Momentum);
        let state = create_test_state(Some(algo));
        let items = menu.items(&state);

        // View enabled when algorithm is active
        assert_eq!(items[2].key, 'V');
        assert!(items[2].enabled);
        assert!(items[2].status.as_ref().unwrap().contains("momentum_btc_v3"));
    }

    #[test]
    fn test_items_filter_shows_active_status() {
        let mut menu = AlgorithmsMenu::new();
        menu.filter = StrategyFilter::Momentum;
        let state = create_test_state(None);
        let items = menu.items(&state);

        // Momentum filter should show "Active"
        assert_eq!(items[4].key, '1');
        assert_eq!(items[4].status.as_ref().unwrap(), "Active");

        // Others should not show "Active"
        assert!(items[5].status.as_ref().unwrap().is_empty());
        assert!(items[6].status.as_ref().unwrap().is_empty());
    }

    // -------------------------------------------------------------------------
    // Key handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_handle_key_escape() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Esc, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Backspace, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_l_list() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('l'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "algorithm");
            assert!(cmd.args.contains(&"list".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_l_uppercase() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('L'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "algorithm");
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_s_select() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('s'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::AlgorithmSelect));
    }

    #[test]
    fn test_handle_key_v_view_no_algorithm() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('v'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("No algorithm"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_v_view_with_algorithm() {
        let mut menu = AlgorithmsMenu::new();
        let algo = create_algorithm_summary("test_algo", StrategyType::Momentum);
        let state = create_test_state(Some(algo));

        let action = menu.handle_key(KeyCode::Char('v'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "algorithm");
            assert!(cmd.args.contains(&"show".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_n_new() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('n'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::AlgorithmCreate));
    }

    #[test]
    fn test_handle_key_1_momentum_filter() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('1'), &state);
        assert_eq!(menu.filter, StrategyFilter::Momentum);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert!(cmd.args.contains(&"--strategy".to_string()));
            assert!(cmd.args.contains(&"momentum".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_2_market_making_filter() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('2'), &state);
        assert_eq!(menu.filter, StrategyFilter::MarketMaking);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert!(cmd.args.contains(&"market-making".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_3_hybrid_filter() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('3'), &state);
        assert_eq!(menu.filter, StrategyFilter::Hybrid);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert!(cmd.args.contains(&"hybrid".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let action = menu.handle_key(KeyCode::Char('x'), &state);
        assert_eq!(action, SubMenuAction::None);
    }

    // -------------------------------------------------------------------------
    // Footer tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_footer_no_algorithm() {
        let menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        assert!(footer.unwrap().contains("No algorithm selected"));
    }

    #[test]
    fn test_footer_with_algorithm() {
        let menu = AlgorithmsMenu::new();
        let algo = create_algorithm_summary("momentum_btc_v3", StrategyType::Momentum);
        let state = create_test_state(Some(algo));

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("momentum_btc_v3"));
        assert!(f.contains("Momentum"));
    }

    // -------------------------------------------------------------------------
    // Access control tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_can_enter_always_true() {
        let menu = AlgorithmsMenu::new();
        let state = create_test_state(None);
        assert!(menu.can_enter(&state));
    }

    #[test]
    fn test_blocked_message_always_none() {
        let menu = AlgorithmsMenu::new();
        let state = create_test_state(None);
        assert!(menu.blocked_message(&state).is_none());
    }

    // -------------------------------------------------------------------------
    // Helper function tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_has_active_algorithm_false() {
        let state = create_test_state(None);
        assert!(!AlgorithmsMenu::has_active_algorithm(&state));
    }

    #[test]
    fn test_has_active_algorithm_true() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo));
        assert!(AlgorithmsMenu::has_active_algorithm(&state));
    }

    #[test]
    fn test_get_active_summary_none() {
        let state = create_test_state(None);
        assert!(AlgorithmsMenu::get_active_summary(&state).is_none());
    }

    #[test]
    fn test_get_active_summary_some() {
        let algo = create_algorithm_summary("my_algo", StrategyType::MarketMaking);
        let state = create_test_state(Some(algo));
        let summary = AlgorithmsMenu::get_active_summary(&state);
        assert!(summary.is_some());
        assert!(summary.unwrap().contains("my_algo"));
    }

    #[test]
    fn test_build_list_command_no_filter() {
        let menu = AlgorithmsMenu::new();
        let cmd = menu.build_list_command();
        assert_eq!(cmd.binary, "algorithm");
        assert_eq!(cmd.args, vec!["list".to_string()]);
    }

    #[test]
    fn test_build_list_command_with_momentum_filter() {
        let mut menu = AlgorithmsMenu::new();
        menu.filter = StrategyFilter::Momentum;
        let cmd = menu.build_list_command();
        assert_eq!(cmd.binary, "algorithm");
        assert!(cmd.args.contains(&"list".to_string()));
        assert!(cmd.args.contains(&"--strategy".to_string()));
        assert!(cmd.args.contains(&"momentum".to_string()));
    }

    // -------------------------------------------------------------------------
    // Clone and Debug tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_algorithms_menu_clone() {
        let mut menu = AlgorithmsMenu::new();
        menu.filter = StrategyFilter::Hybrid;
        let cloned = menu.clone();
        assert_eq!(menu.filter, cloned.filter);
    }

    #[test]
    fn test_algorithms_menu_debug() {
        let menu = AlgorithmsMenu::new();
        let debug_str = format!("{:?}", menu);
        assert!(debug_str.contains("AlgorithmsMenu"));
    }

    #[test]
    fn test_strategy_filter_clone() {
        let filter = StrategyFilter::Momentum;
        let cloned = filter;
        assert_eq!(filter, cloned);
    }

    #[test]
    fn test_strategy_filter_debug() {
        let filter = StrategyFilter::Momentum;
        let debug_str = format!("{:?}", filter);
        assert!(debug_str.contains("Momentum"));
    }

    // -------------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_filter_cycle() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        assert_eq!(menu.filter, StrategyFilter::All);

        menu.handle_key(KeyCode::Char('1'), &state);
        assert_eq!(menu.filter, StrategyFilter::Momentum);

        menu.handle_key(KeyCode::Char('2'), &state);
        assert_eq!(menu.filter, StrategyFilter::MarketMaking);

        menu.handle_key(KeyCode::Char('3'), &state);
        assert_eq!(menu.filter, StrategyFilter::Hybrid);
    }

    #[test]
    fn test_all_keys_case_insensitive() {
        let mut menu = AlgorithmsMenu::new();
        let state = create_test_state(None);

        // Test lowercase
        let action_l = menu.handle_key(KeyCode::Char('l'), &state);
        let action_s = menu.handle_key(KeyCode::Char('s'), &state);
        let action_n = menu.handle_key(KeyCode::Char('n'), &state);

        // Test uppercase
        menu.filter = StrategyFilter::All; // Reset
        let action_L = menu.handle_key(KeyCode::Char('L'), &state);
        let action_S = menu.handle_key(KeyCode::Char('S'), &state);
        let action_N = menu.handle_key(KeyCode::Char('N'), &state);

        // Same action types for both cases
        assert!(matches!(action_l, SubMenuAction::ExecuteCommand(_)));
        assert!(matches!(action_L, SubMenuAction::ExecuteCommand(_)));
        assert!(matches!(action_s, SubMenuAction::Navigate(_)));
        assert!(matches!(action_S, SubMenuAction::Navigate(_)));
        assert!(matches!(action_n, SubMenuAction::Navigate(_)));
        assert!(matches!(action_N, SubMenuAction::Navigate(_)));
    }

    #[test]
    fn test_workflow_select_then_view() {
        let mut menu = AlgorithmsMenu::new();

        // Initially no algorithm
        let state_no_algo = create_test_state(None);
        let action = menu.handle_key(KeyCode::Char('v'), &state_no_algo);
        assert!(matches!(action, SubMenuAction::ShowMessage(_)));

        // After selecting algorithm
        let algo = create_algorithm_summary("selected_algo", StrategyType::Momentum);
        let state_with_algo = create_test_state(Some(algo));
        let action = menu.handle_key(KeyCode::Char('v'), &state_with_algo);
        assert!(matches!(action, SubMenuAction::ExecuteCommand(_)));
    }
}
