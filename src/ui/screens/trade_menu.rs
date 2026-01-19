//! Trade Menu implementation (TUI-4.0)
//!
//! Provides a submenu for trading operations:
//! - [P] Paper Trade: Simulated execution on live data
//! - [C] Campaign: 4-week validation campaign
//! - [L] Live: Real execution (requires validation)
//! - [S] Sessions: View past trading sessions
//! - [V] Validate Session: Compare session vs backtest
//!
//! Live trading is DISABLED unless all prior stages passed:
//! Backtest ✓, Forward ✓, OOS ✓, Paper ✓

use crossterm::event::KeyCode;

use crate::ui::state::{GlobalState, StageStatus, TradingMode};
use crate::ui::submenu::{
    SubMenu, SubMenuAction, SubMenuItem, NavigationTarget, CliCommand,
    key_to_char, is_back_key,
};

// ============================================================================
// TradeMenu
// ============================================================================

/// Trade submenu implementing the SubMenu trait
#[derive(Debug, Clone, Default)]
pub struct TradeMenu;

impl TradeMenu {
    /// Create a new TradeMenu
    pub fn new() -> Self {
        Self
    }

    /// Check if an algorithm is currently active
    fn has_active_algorithm(state: &GlobalState) -> bool {
        state.active_algorithm.is_some()
    }

    /// Get algorithm name for display
    fn get_algorithm_name(state: &GlobalState) -> String {
        state.active_algorithm
            .as_ref()
            .map(|algo| algo.name.clone())
            .unwrap_or_else(|| "None selected".to_string())
    }

    /// Check if all validation stages passed (can trade live)
    fn can_trade_live(state: &GlobalState) -> bool {
        state.can_trade_live()
    }

    /// Get validation status indicator
    fn validation_indicator(state: &GlobalState) -> &'static str {
        if Self::can_trade_live(state) {
            "✓"
        } else {
            "✗"
        }
    }

    /// Get the number of passed stages
    fn passed_stage_count(state: &GlobalState) -> usize {
        state.passed_stages()
    }

    /// Get stage indicator
    fn stage_indicator(status: &StageStatus) -> &'static str {
        match status {
            StageStatus::NotRun => "○",
            StageStatus::Passed { .. } => "✓",
            StageStatus::Failed { .. } => "✗",
            StageStatus::Running { .. } => "◐",
        }
    }

    /// Get validation requirements string
    fn validation_requirements(state: &GlobalState) -> String {
        let v = &state.validation_status;
        format!(
            "Backtest {} WalkFwd {} OOS {} Paper {}",
            Self::stage_indicator(&v.backtest),
            Self::stage_indicator(&v.forward),
            Self::stage_indicator(&v.oos),
            Self::stage_indicator(&v.paper),
        )
    }

    /// Get current trading mode string
    fn trading_mode_string(state: &GlobalState) -> String {
        match &state.trading_mode {
            TradingMode::Idle => "Idle".to_string(),
            TradingMode::Paper { pnl, .. } => format!("Paper ({:+.2})", pnl),
            TradingMode::Live { pnl, .. } => format!("Live ({:+.2})", pnl),
        }
    }

    /// Check if currently in paper trading mode
    fn is_paper_trading(state: &GlobalState) -> bool {
        matches!(state.trading_mode, TradingMode::Paper { .. })
    }

    /// Check if currently in live trading mode
    fn is_live_trading(state: &GlobalState) -> bool {
        matches!(state.trading_mode, TradingMode::Live { .. })
    }
}

impl SubMenu for TradeMenu {
    fn title(&self) -> &str {
        "TRADE - Execution"
    }

    fn items(&self, state: &GlobalState) -> Vec<SubMenuItem> {
        let has_algo = Self::has_active_algorithm(state);
        let can_live = Self::can_trade_live(state);

        vec![
            // Paper Trading section
            SubMenuItem::new('P', "Paper Trade", "Simulated execution on live data")
                .with_enabled(has_algo)
                .with_status(if Self::is_paper_trading(state) { "ACTIVE" } else { "" }),
            SubMenuItem::new('C', "Campaign", "4-week validation campaign")
                .with_enabled(has_algo),

            // Live Trading section
            SubMenuItem::new('L', "Live", "Real execution (requires validation)")
                .with_enabled(has_algo && can_live)
                .with_status(if Self::is_live_trading(state) {
                    "ACTIVE"
                } else if !can_live && has_algo {
                    "LOCKED"
                } else {
                    ""
                }),

            // Sessions section
            SubMenuItem::new('S', "Sessions", "View past trading sessions")
                .with_enabled(has_algo),
            SubMenuItem::new('V', "Validate Session", "Compare session vs backtest")
                .with_enabled(has_algo),
        ]
    }

    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction {
        if is_back_key(key) {
            return SubMenuAction::Back;
        }

        let has_algo = Self::has_active_algorithm(state);
        let can_live = Self::can_trade_live(state);

        if let Some(c) = key_to_char(key) {
            match c.to_ascii_lowercase() {
                'p' => {
                    // Paper trading - navigate to config screen
                    if has_algo {
                        SubMenuAction::Navigate(NavigationTarget::BacktestPaperConfig)
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'c' => {
                    // Campaign simulation - navigate to config screen
                    if has_algo {
                        SubMenuAction::Navigate(NavigationTarget::BacktestCampaignConfig)
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'l' => {
                    // Live trading (gated)
                    if !has_algo {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    } else if !can_live {
                        let reqs = Self::validation_requirements(state);
                        SubMenuAction::ShowMessage(
                            format!("Live trading requires all stages passed: {}", reqs)
                        )
                    } else {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::validate("run", vec!["--stages", "live"])
                        )
                    }
                }
                's' => {
                    // View sessions
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::backtest("simulate-session", vec![])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'v' => {
                    // Validate session
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::backtest("validate-session", vec![])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
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
        if Self::has_active_algorithm(state) {
            let algo_name = Self::get_algorithm_name(state);
            let validation = Self::validation_indicator(state);
            let mode = Self::trading_mode_string(state);
            let passed = Self::passed_stage_count(state);
            Some(format!(
                "Algorithm: {} | Validated: {} ({}/4) | Mode: {}",
                algo_name, validation, passed, mode
            ))
        } else {
            Some("No algorithm selected - use Algorithms menu first".to_string())
        }
    }

    fn can_enter(&self, _state: &GlobalState) -> bool {
        // Always allow entry, but items will be disabled without algorithm
        true
    }

    fn blocked_message(&self, _state: &GlobalState) -> Option<String> {
        // Never blocked at menu level
        None
    }
}

// ============================================================================
// Drawing function
// ============================================================================

/// Draw the trade menu
pub fn draw_trade_menu(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    menu: &TradeMenu,
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
    use crate::ui::state::{ResearchStatus, DataStats, AlgorithmConfigSummary, ValidationStatus};
    use crate::core::algorithm_config::StrategyType;
    use chrono::Utc;

    // -------------------------------------------------------------------------
    // Helper for creating test state
    // -------------------------------------------------------------------------

    fn create_test_state(
        active_algorithm: Option<AlgorithmConfigSummary>,
        validation_status: ValidationStatus,
        trading_mode: TradingMode,
    ) -> GlobalState {
        GlobalState {
            symbol: "BTCUSDT".to_string(),
            active_algorithm,
            validation_status,
            trading_mode,
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

    fn create_validation_all_not_run() -> ValidationStatus {
        ValidationStatus::default()
    }

    fn create_validation_partial() -> ValidationStatus {
        ValidationStatus {
            backtest: StageStatus::Passed { sharpe: 1.2, timestamp: Utc::now() },
            forward: StageStatus::Passed { sharpe: 0.9, timestamp: Utc::now() },
            oos: StageStatus::Failed { reason: "Sharpe 0.3 < 0.5".to_string(), timestamp: Utc::now() },
            paper: StageStatus::NotRun,
            live: StageStatus::NotRun,
        }
    }

    fn create_validation_all_passed() -> ValidationStatus {
        ValidationStatus {
            backtest: StageStatus::Passed { sharpe: 1.5, timestamp: Utc::now() },
            forward: StageStatus::Passed { sharpe: 1.2, timestamp: Utc::now() },
            oos: StageStatus::Passed { sharpe: 0.8, timestamp: Utc::now() },
            paper: StageStatus::Passed { sharpe: 0.6, timestamp: Utc::now() },
            live: StageStatus::NotRun,
        }
    }

    // -------------------------------------------------------------------------
    // Construction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_trade_menu_new() {
        let menu = TradeMenu::new();
        assert_eq!(menu.title(), "TRADE - Execution");
    }

    #[test]
    fn test_trade_menu_default() {
        let menu = TradeMenu::default();
        assert_eq!(menu.title(), "TRADE - Execution");
    }

    // -------------------------------------------------------------------------
    // Title tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_title() {
        let menu = TradeMenu::new();
        assert_eq!(menu.title(), "TRADE - Execution");
    }

    // -------------------------------------------------------------------------
    // Helper function tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_has_active_algorithm_false() {
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert!(!TradeMenu::has_active_algorithm(&state));
    }

    #[test]
    fn test_has_active_algorithm_true() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);
        assert!(TradeMenu::has_active_algorithm(&state));
    }

    #[test]
    fn test_get_algorithm_name_none() {
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert_eq!(TradeMenu::get_algorithm_name(&state), "None selected");
    }

    #[test]
    fn test_get_algorithm_name_some() {
        let algo = create_algorithm_summary("my_algo", StrategyType::MarketMaking);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);
        assert_eq!(TradeMenu::get_algorithm_name(&state), "my_algo");
    }

    #[test]
    fn test_can_trade_live_false_no_validation() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_not_run(), TradingMode::Idle);
        assert!(!TradeMenu::can_trade_live(&state));
    }

    #[test]
    fn test_can_trade_live_false_partial_validation() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_partial(), TradingMode::Idle);
        assert!(!TradeMenu::can_trade_live(&state));
    }

    #[test]
    fn test_can_trade_live_true() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_passed(), TradingMode::Idle);
        assert!(TradeMenu::can_trade_live(&state));
    }

    #[test]
    fn test_validation_indicator_passed() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_passed(), TradingMode::Idle);
        assert_eq!(TradeMenu::validation_indicator(&state), "✓");
    }

    #[test]
    fn test_validation_indicator_not_passed() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_partial(), TradingMode::Idle);
        assert_eq!(TradeMenu::validation_indicator(&state), "✗");
    }

    #[test]
    fn test_passed_stage_count_zero() {
        let state = create_test_state(None, create_validation_all_not_run(), TradingMode::Idle);
        assert_eq!(TradeMenu::passed_stage_count(&state), 0);
    }

    #[test]
    fn test_passed_stage_count_partial() {
        let state = create_test_state(None, create_validation_partial(), TradingMode::Idle);
        assert_eq!(TradeMenu::passed_stage_count(&state), 2); // backtest and forward
    }

    #[test]
    fn test_passed_stage_count_all() {
        let state = create_test_state(None, create_validation_all_passed(), TradingMode::Idle);
        assert_eq!(TradeMenu::passed_stage_count(&state), 4); // backtest, forward, oos, paper
    }

    #[test]
    fn test_stage_indicator_not_run() {
        assert_eq!(TradeMenu::stage_indicator(&StageStatus::NotRun), "○");
    }

    #[test]
    fn test_stage_indicator_passed() {
        let status = StageStatus::Passed { sharpe: 1.0, timestamp: Utc::now() };
        assert_eq!(TradeMenu::stage_indicator(&status), "✓");
    }

    #[test]
    fn test_stage_indicator_failed() {
        let status = StageStatus::Failed { reason: "test".to_string(), timestamp: Utc::now() };
        assert_eq!(TradeMenu::stage_indicator(&status), "✗");
    }

    #[test]
    fn test_stage_indicator_running() {
        let status = StageStatus::Running { progress: 0.5 };
        assert_eq!(TradeMenu::stage_indicator(&status), "◐");
    }

    #[test]
    fn test_validation_requirements() {
        let state = create_test_state(None, create_validation_partial(), TradingMode::Idle);
        let reqs = TradeMenu::validation_requirements(&state);
        assert!(reqs.contains("✓")); // backtest passed
        assert!(reqs.contains("✗")); // oos failed
        assert!(reqs.contains("○")); // paper not run
    }

    #[test]
    fn test_trading_mode_string_idle() {
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert_eq!(TradeMenu::trading_mode_string(&state), "Idle");
    }

    #[test]
    fn test_trading_mode_string_paper() {
        let state = create_test_state(
            None,
            ValidationStatus::default(),
            TradingMode::Paper { started: Utc::now(), pnl: 123.45 }
        );
        let mode_str = TradeMenu::trading_mode_string(&state);
        assert!(mode_str.contains("Paper"));
        assert!(mode_str.contains("123.45"));
    }

    #[test]
    fn test_trading_mode_string_live() {
        let state = create_test_state(
            None,
            ValidationStatus::default(),
            TradingMode::Live { started: Utc::now(), pnl: -50.00 }
        );
        let mode_str = TradeMenu::trading_mode_string(&state);
        assert!(mode_str.contains("Live"));
        assert!(mode_str.contains("-50.00"));
    }

    #[test]
    fn test_is_paper_trading_true() {
        let state = create_test_state(
            None,
            ValidationStatus::default(),
            TradingMode::Paper { started: Utc::now(), pnl: 0.0 }
        );
        assert!(TradeMenu::is_paper_trading(&state));
    }

    #[test]
    fn test_is_paper_trading_false() {
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert!(!TradeMenu::is_paper_trading(&state));
    }

    #[test]
    fn test_is_live_trading_true() {
        let state = create_test_state(
            None,
            ValidationStatus::default(),
            TradingMode::Live { started: Utc::now(), pnl: 0.0 }
        );
        assert!(TradeMenu::is_live_trading(&state));
    }

    #[test]
    fn test_is_live_trading_false() {
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert!(!TradeMenu::is_live_trading(&state));
    }

    // -------------------------------------------------------------------------
    // Items tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_items_no_algorithm() {
        let menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        let items = menu.items(&state);

        assert_eq!(items.len(), 5);

        // All items should be disabled
        for item in &items {
            assert!(!item.enabled, "Item {} should be disabled", item.key);
        }
    }

    #[test]
    fn test_items_with_algorithm_no_validation() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_not_run(), TradingMode::Idle);
        let items = menu.items(&state);

        assert_eq!(items.len(), 5);

        // Paper, Campaign, Sessions, Validate Session should be enabled
        assert_eq!(items[0].key, 'P');
        assert!(items[0].enabled); // Paper

        assert_eq!(items[1].key, 'C');
        assert!(items[1].enabled); // Campaign

        // Live should be disabled (no validation)
        assert_eq!(items[2].key, 'L');
        assert!(!items[2].enabled); // Live LOCKED

        assert_eq!(items[3].key, 'S');
        assert!(items[3].enabled); // Sessions

        assert_eq!(items[4].key, 'V');
        assert!(items[4].enabled); // Validate Session
    }

    #[test]
    fn test_items_with_algorithm_full_validation() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_passed(), TradingMode::Idle);
        let items = menu.items(&state);

        // All items should be enabled including Live
        for item in &items {
            assert!(item.enabled, "Item {} should be enabled", item.key);
        }
    }

    #[test]
    fn test_items_live_shows_locked_status() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_partial(), TradingMode::Idle);
        let items = menu.items(&state);

        // Live item should show LOCKED status
        let live_item = &items[2];
        assert_eq!(live_item.key, 'L');
        assert!(!live_item.enabled);
        assert_eq!(live_item.status, Some("LOCKED".to_string()));
    }

    #[test]
    fn test_items_paper_shows_active_status() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(
            Some(algo),
            ValidationStatus::default(),
            TradingMode::Paper { started: Utc::now(), pnl: 100.0 }
        );
        let items = menu.items(&state);

        // Paper item should show ACTIVE status
        let paper_item = &items[0];
        assert_eq!(paper_item.key, 'P');
        assert_eq!(paper_item.status, Some("ACTIVE".to_string()));
    }

    #[test]
    fn test_items_live_shows_active_status() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(
            Some(algo),
            create_validation_all_passed(),
            TradingMode::Live { started: Utc::now(), pnl: 200.0 }
        );
        let items = menu.items(&state);

        // Live item should show ACTIVE status
        let live_item = &items[2];
        assert_eq!(live_item.key, 'L');
        assert_eq!(live_item.status, Some("ACTIVE".to_string()));
    }

    // -------------------------------------------------------------------------
    // Key handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_handle_key_escape() {
        let mut menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Esc, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Backspace, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_p_no_algorithm() {
        let mut menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('p'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("No algorithm"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_p_with_algorithm() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('p'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "validate");
            assert!(cmd.args.contains(&"paper".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_c_campaign() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('c'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "backtest");
            assert!(cmd.args.contains(&"simulate-campaign".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_l_no_algorithm() {
        let mut menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('l'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("No algorithm"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_l_no_validation() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_partial(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('l'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("requires all stages passed"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_l_with_validation() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_passed(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('l'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "validate");
            assert!(cmd.args.contains(&"live".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_s_sessions() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('s'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "backtest");
            assert!(cmd.args.contains(&"simulate-session".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_v_validate_session() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('v'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "backtest");
            assert!(cmd.args.contains(&"validate-session".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('x'), &state);
        assert_eq!(action, SubMenuAction::None);
    }

    #[test]
    fn test_handle_key_case_insensitive() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);

        // Lowercase
        let action_lower = menu.handle_key(KeyCode::Char('p'), &state);
        // Uppercase
        let action_upper = menu.handle_key(KeyCode::Char('P'), &state);

        // Both should result in ExecuteCommand
        assert!(matches!(action_lower, SubMenuAction::ExecuteCommand(_)));
        assert!(matches!(action_upper, SubMenuAction::ExecuteCommand(_)));
    }

    // -------------------------------------------------------------------------
    // Footer tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_footer_no_algorithm() {
        let menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        assert!(footer.unwrap().contains("No algorithm selected"));
    }

    #[test]
    fn test_footer_with_algorithm() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("momentum_btc_v3", StrategyType::Momentum);
        let validation = create_validation_partial();
        let state = create_test_state(Some(algo), validation, TradingMode::Idle);

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("momentum_btc_v3"));
        assert!(f.contains("Validated: ✗")); // Not all passed
        assert!(f.contains("2/4")); // 2 passed out of 4 required
    }

    #[test]
    fn test_footer_with_full_validation() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("validated_algo", StrategyType::Momentum);
        let validation = create_validation_all_passed();
        let state = create_test_state(Some(algo), validation, TradingMode::Idle);

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("Validated: ✓"));
        assert!(f.contains("4/4"));
    }

    #[test]
    fn test_footer_shows_trading_mode() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(
            Some(algo),
            ValidationStatus::default(),
            TradingMode::Paper { started: Utc::now(), pnl: 50.0 }
        );

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("Paper"));
    }

    // -------------------------------------------------------------------------
    // Access control tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_can_enter_always_true() {
        let menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert!(menu.can_enter(&state));
    }

    #[test]
    fn test_blocked_message_always_none() {
        let menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        assert!(menu.blocked_message(&state).is_none());
    }

    // -------------------------------------------------------------------------
    // Clone and Debug tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_trade_menu_clone() {
        let menu = TradeMenu::new();
        let cloned = menu.clone();
        assert_eq!(menu.title(), cloned.title());
    }

    #[test]
    fn test_trade_menu_debug() {
        let menu = TradeMenu::new();
        let debug_str = format!("{:?}", menu);
        assert!(debug_str.contains("TradeMenu"));
    }

    // -------------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_full_workflow_no_algorithm() {
        let mut menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);

        // All trade actions should show message about no algorithm
        for key in ['p', 'c', 'l', 's', 'v'] {
            let action = menu.handle_key(KeyCode::Char(key), &state);
            assert!(
                matches!(action, SubMenuAction::ShowMessage(_)),
                "Key '{}' should show message when no algorithm", key
            );
        }
    }

    #[test]
    fn test_full_workflow_with_algorithm_no_validation() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_not_run(), TradingMode::Idle);

        // Paper, Campaign, Sessions, Validate should execute
        for key in ['p', 'c', 's', 'v'] {
            let action = menu.handle_key(KeyCode::Char(key), &state);
            assert!(
                matches!(action, SubMenuAction::ExecuteCommand(_)),
                "Key '{}' should execute command when algorithm selected", key
            );
        }

        // Live should show message (not validated)
        let action = menu.handle_key(KeyCode::Char('l'), &state);
        assert!(matches!(action, SubMenuAction::ShowMessage(_)));
    }

    #[test]
    fn test_full_workflow_with_algorithm_full_validation() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_all_passed(), TradingMode::Idle);

        // All trade actions should execute
        for key in ['p', 'c', 'l', 's', 'v'] {
            let action = menu.handle_key(KeyCode::Char(key), &state);
            assert!(
                matches!(action, SubMenuAction::ExecuteCommand(_)),
                "Key '{}' should execute command when fully validated", key
            );
        }
    }

    #[test]
    fn test_live_trading_gate_message_includes_requirements() {
        let mut menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), create_validation_partial(), TradingMode::Idle);

        let action = menu.handle_key(KeyCode::Char('l'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            // Should include the validation requirements
            assert!(msg.contains("Backtest"));
            assert!(msg.contains("WalkFwd"));
            assert!(msg.contains("OOS"));
            assert!(msg.contains("Paper"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_items_count() {
        let menu = TradeMenu::new();
        let state = create_test_state(None, ValidationStatus::default(), TradingMode::Idle);
        let items = menu.items(&state);

        // Should have exactly 5 items: Paper, Campaign, Live, Sessions, Validate Session
        assert_eq!(items.len(), 5);
    }

    #[test]
    fn test_items_keys() {
        let menu = TradeMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default(), TradingMode::Idle);
        let items = menu.items(&state);

        // Check all expected keys are present
        let keys: Vec<char> = items.iter().map(|i| i.key).collect();
        assert_eq!(keys, vec!['P', 'C', 'L', 'S', 'V']);
    }
}
