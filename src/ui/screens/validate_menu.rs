//! Validate Menu implementation (TUI-3.0)
//!
//! Provides a submenu for validation pipeline operations:
//! - [1] Backtest: Run historical backtest
//! - [2] Walk-Forward: Time-series cross-validation
//! - [3] OOS: Out-of-sample validation
//! - [A] All Stages: Run full pipeline
//! - [G] Grid Search: Parameter optimization
//! - [W] Sweep: Sensitivity analysis
//! - [H] History: Past validation runs
//! - [P] Presets: View/select pipeline presets
//!
//! The menu is disabled when no algorithm is selected.
//! Shows stage status indicators with Sharpe ratios.

use crossterm::event::KeyCode;

use crate::ui::state::{GlobalState, StageStatus, ValidationStatus};
use crate::ui::submenu::{
    SubMenu, SubMenuAction, SubMenuItem, NavigationTarget, CliCommand,
    key_to_char, is_back_key,
};

// ============================================================================
// ValidateMenu
// ============================================================================

/// Validate submenu implementing the SubMenu trait
#[derive(Debug, Clone, Default)]
pub struct ValidateMenu;

impl ValidateMenu {
    /// Create a new ValidateMenu
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

    /// Get status indicator for a stage
    fn stage_indicator(status: &StageStatus) -> &'static str {
        match status {
            StageStatus::NotRun => "○",
            StageStatus::Passed { .. } => "✓",
            StageStatus::Failed { .. } => "✗",
            StageStatus::Running { .. } => "◐",
        }
    }

    /// Get status string with sharpe for a stage
    fn stage_status_string(status: &StageStatus) -> String {
        match status {
            StageStatus::NotRun => "○".to_string(),
            StageStatus::Passed { sharpe, .. } => format!("✓ {:.2}", sharpe),
            StageStatus::Failed { reason, .. } => format!("✗ {}", truncate_reason(reason, 15)),
            StageStatus::Running { progress } => format!("◐ {:.0}%", progress * 100.0),
        }
    }

    /// Count passed stages
    fn count_passed_stages(validation: &ValidationStatus) -> usize {
        let stages = [
            &validation.backtest,
            &validation.forward,
            &validation.oos,
            &validation.paper,
            &validation.live,
        ];
        stages.iter().filter(|s| matches!(s, StageStatus::Passed { .. })).count()
    }

    /// Get validation summary string
    fn validation_summary(validation: &ValidationStatus) -> String {
        let passed = Self::count_passed_stages(validation);
        let indicators = format!(
            "BT:{} FW:{} OOS:{} PP:{} LV:{}",
            Self::stage_indicator(&validation.backtest),
            Self::stage_indicator(&validation.forward),
            Self::stage_indicator(&validation.oos),
            Self::stage_indicator(&validation.paper),
            Self::stage_indicator(&validation.live),
        );
        format!("{}/5 passed | {}", passed, indicators)
    }
}

/// Truncate a reason string to max length
fn truncate_reason(reason: &str, max_len: usize) -> String {
    if reason.len() <= max_len {
        reason.to_string()
    } else {
        format!("{}...", &reason[..max_len.saturating_sub(3)])
    }
}

impl SubMenu for ValidateMenu {
    fn title(&self) -> &str {
        "VALIDATE - Test Before Trading"
    }

    fn items(&self, state: &GlobalState) -> Vec<SubMenuItem> {
        let has_algo = Self::has_active_algorithm(state);
        let validation = &state.validation_status;

        vec![
            // Run Stages section
            SubMenuItem::new('1', "Backtest", "Historical replay")
                .with_enabled(has_algo)
                .with_status(Self::stage_status_string(&validation.backtest)),
            SubMenuItem::new('2', "Walk-Forward", "Time-series cross-validation")
                .with_enabled(has_algo)
                .with_status(Self::stage_status_string(&validation.forward)),
            SubMenuItem::new('3', "Out-of-Sample", "Holdout test (20%)")
                .with_enabled(has_algo)
                .with_status(Self::stage_status_string(&validation.oos)),
            SubMenuItem::new('A', "All Stages", "Run full pipeline")
                .with_enabled(has_algo),

            // Optimization section
            SubMenuItem::new('G', "Grid Search", "Parameter optimization")
                .with_enabled(has_algo),
            SubMenuItem::new('W', "Sweep", "Sensitivity analysis")
                .with_enabled(has_algo),

            // Results section
            SubMenuItem::new('H', "History", "Past validation runs")
                .with_enabled(has_algo),
            SubMenuItem::new('P', "Presets", "View/select pipeline presets"),
        ]
    }

    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction {
        if is_back_key(key) {
            return SubMenuAction::Back;
        }

        let has_algo = Self::has_active_algorithm(state);

        if let Some(c) = key_to_char(key) {
            match c.to_ascii_lowercase() {
                '1' => {
                    // Run backtest
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::validate("run", vec!["--stages", "backtest"])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                '2' => {
                    // Run walk-forward
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::validate("run", vec!["--stages", "forward"])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                '3' => {
                    // Run OOS
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::validate("run", vec!["--stages", "oos"])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'a' => {
                    // Run all stages
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::validate("run", vec![])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'g' => {
                    // Grid search
                    if has_algo {
                        SubMenuAction::Navigate(NavigationTarget::GridSearch)
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'w' => {
                    // Sweep
                    if has_algo {
                        SubMenuAction::Navigate(NavigationTarget::Sweep)
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'h' => {
                    // History
                    if has_algo {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::validate("status", vec![])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No algorithm selected. Select one in Algorithms menu.".to_string()
                        )
                    }
                }
                'p' => {
                    // Presets - always available
                    SubMenuAction::ExecuteCommand(
                        CliCommand::validate("presets", vec![])
                    )
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
            let summary = Self::validation_summary(&state.validation_status);
            Some(format!("Algorithm: {} | {}", algo_name, summary))
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

/// Draw the validate menu
pub fn draw_validate_menu(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    menu: &ValidateMenu,
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
    use crate::ui::state::{ResearchStatus, TradingMode, DataStats, AlgorithmConfigSummary};
    use crate::core::algorithm_config::StrategyType;
    use chrono::Utc;

    // -------------------------------------------------------------------------
    // Helper for creating test state
    // -------------------------------------------------------------------------

    fn create_test_state(
        active_algorithm: Option<AlgorithmConfigSummary>,
        validation_status: ValidationStatus,
    ) -> GlobalState {
        GlobalState {
            symbol: "BTCUSDT".to_string(),
            active_algorithm,
            validation_status,
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

    fn create_validation_all_not_run() -> ValidationStatus {
        ValidationStatus::default()
    }

    fn create_validation_mixed() -> ValidationStatus {
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
            live: StageStatus::Passed { sharpe: 0.5, timestamp: Utc::now() },
        }
    }

    // -------------------------------------------------------------------------
    // Construction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_menu_new() {
        let menu = ValidateMenu::new();
        assert_eq!(menu.title(), "VALIDATE - Test Before Trading");
    }

    #[test]
    fn test_validate_menu_default() {
        let menu = ValidateMenu::default();
        assert_eq!(menu.title(), "VALIDATE - Test Before Trading");
    }

    // -------------------------------------------------------------------------
    // Title tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_title() {
        let menu = ValidateMenu::new();
        assert_eq!(menu.title(), "VALIDATE - Test Before Trading");
    }

    // -------------------------------------------------------------------------
    // Stage indicator tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_stage_indicator_not_run() {
        assert_eq!(ValidateMenu::stage_indicator(&StageStatus::NotRun), "○");
    }

    #[test]
    fn test_stage_indicator_passed() {
        let status = StageStatus::Passed { sharpe: 1.0, timestamp: Utc::now() };
        assert_eq!(ValidateMenu::stage_indicator(&status), "✓");
    }

    #[test]
    fn test_stage_indicator_failed() {
        let status = StageStatus::Failed { reason: "test".to_string(), timestamp: Utc::now() };
        assert_eq!(ValidateMenu::stage_indicator(&status), "✗");
    }

    #[test]
    fn test_stage_indicator_running() {
        let status = StageStatus::Running { progress: 0.5 };
        assert_eq!(ValidateMenu::stage_indicator(&status), "◐");
    }

    // -------------------------------------------------------------------------
    // Stage status string tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_stage_status_string_not_run() {
        assert_eq!(ValidateMenu::stage_status_string(&StageStatus::NotRun), "○");
    }

    #[test]
    fn test_stage_status_string_passed() {
        let status = StageStatus::Passed { sharpe: 1.25, timestamp: Utc::now() };
        assert_eq!(ValidateMenu::stage_status_string(&status), "✓ 1.25");
    }

    #[test]
    fn test_stage_status_string_failed() {
        let status = StageStatus::Failed { reason: "Low Sharpe".to_string(), timestamp: Utc::now() };
        let result = ValidateMenu::stage_status_string(&status);
        assert!(result.starts_with("✗"));
        assert!(result.contains("Low Sharpe"));
    }

    #[test]
    fn test_stage_status_string_running() {
        let status = StageStatus::Running { progress: 0.75 };
        assert_eq!(ValidateMenu::stage_status_string(&status), "◐ 75%");
    }

    // -------------------------------------------------------------------------
    // Count passed stages tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_count_passed_stages_none() {
        let validation = create_validation_all_not_run();
        assert_eq!(ValidateMenu::count_passed_stages(&validation), 0);
    }

    #[test]
    fn test_count_passed_stages_some() {
        let validation = create_validation_mixed();
        assert_eq!(ValidateMenu::count_passed_stages(&validation), 2);
    }

    #[test]
    fn test_count_passed_stages_all() {
        let validation = create_validation_all_passed();
        assert_eq!(ValidateMenu::count_passed_stages(&validation), 5);
    }

    // -------------------------------------------------------------------------
    // Validation summary tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_validation_summary_none_passed() {
        let validation = create_validation_all_not_run();
        let summary = ValidateMenu::validation_summary(&validation);
        assert!(summary.contains("0/5 passed"));
    }

    #[test]
    fn test_validation_summary_mixed() {
        let validation = create_validation_mixed();
        let summary = ValidateMenu::validation_summary(&validation);
        assert!(summary.contains("2/5 passed"));
        assert!(summary.contains("BT:✓"));
        assert!(summary.contains("FW:✓"));
        assert!(summary.contains("OOS:✗"));
    }

    #[test]
    fn test_validation_summary_all_passed() {
        let validation = create_validation_all_passed();
        let summary = ValidateMenu::validation_summary(&validation);
        assert!(summary.contains("5/5 passed"));
    }

    // -------------------------------------------------------------------------
    // Items tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_items_no_algorithm() {
        let menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());
        let items = menu.items(&state);

        assert_eq!(items.len(), 8);

        // All items except presets should be disabled
        assert_eq!(items[0].key, '1');
        assert!(!items[0].enabled); // Backtest disabled

        assert_eq!(items[7].key, 'P');
        assert!(items[7].enabled); // Presets always enabled
    }

    #[test]
    fn test_items_with_algorithm() {
        let menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test_algo", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());
        let items = menu.items(&state);

        assert_eq!(items.len(), 8);

        // All items should be enabled
        for item in &items {
            assert!(item.enabled, "Item {} should be enabled", item.key);
        }
    }

    #[test]
    fn test_items_show_status() {
        let menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test_algo", StrategyType::Momentum);
        let validation = create_validation_mixed();
        let state = create_test_state(Some(algo), validation);
        let items = menu.items(&state);

        // Backtest should show passed status
        assert!(items[0].status.as_ref().unwrap().contains("✓ 1.20"));

        // Forward should show passed status
        assert!(items[1].status.as_ref().unwrap().contains("✓ 0.90"));

        // OOS should show failed status
        assert!(items[2].status.as_ref().unwrap().contains("✗"));
    }

    // -------------------------------------------------------------------------
    // Key handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_handle_key_escape() {
        let mut menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Esc, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Backspace, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_1_no_algorithm() {
        let mut menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('1'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("No algorithm"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_1_with_algorithm() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('1'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "validate");
            assert!(cmd.args.contains(&"backtest".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_2_walk_forward() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('2'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert!(cmd.args.contains(&"forward".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_3_oos() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('3'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert!(cmd.args.contains(&"oos".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_a_all_stages() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('a'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "validate");
            assert!(cmd.args.contains(&"run".to_string()));
            // Should not have specific stage
            assert!(!cmd.args.contains(&"--stages".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_g_grid_search() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('g'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::GridSearch));
    }

    #[test]
    fn test_handle_key_w_sweep() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('w'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::Sweep));
    }

    #[test]
    fn test_handle_key_h_history() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('h'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "validate");
            assert!(cmd.args.contains(&"status".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_p_presets_no_algorithm() {
        let mut menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        // Presets should work even without algorithm
        let action = menu.handle_key(KeyCode::Char('p'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "validate");
            assert!(cmd.args.contains(&"presets".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        let action = menu.handle_key(KeyCode::Char('x'), &state);
        assert_eq!(action, SubMenuAction::None);
    }

    #[test]
    fn test_handle_key_case_insensitive() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        // Lowercase
        let action_lower = menu.handle_key(KeyCode::Char('g'), &state);
        // Uppercase
        let action_upper = menu.handle_key(KeyCode::Char('G'), &state);

        assert_eq!(action_lower, action_upper);
    }

    // -------------------------------------------------------------------------
    // Footer tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_footer_no_algorithm() {
        let menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        assert!(footer.unwrap().contains("No algorithm selected"));
    }

    #[test]
    fn test_footer_with_algorithm() {
        let menu = ValidateMenu::new();
        let algo = create_algorithm_summary("momentum_btc_v3", StrategyType::Momentum);
        let validation = create_validation_mixed();
        let state = create_test_state(Some(algo), validation);

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("momentum_btc_v3"));
        assert!(f.contains("2/5 passed"));
    }

    // -------------------------------------------------------------------------
    // Access control tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_can_enter_always_true() {
        let menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());
        assert!(menu.can_enter(&state));
    }

    #[test]
    fn test_blocked_message_always_none() {
        let menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());
        assert!(menu.blocked_message(&state).is_none());
    }

    // -------------------------------------------------------------------------
    // Helper function tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_has_active_algorithm_false() {
        let state = create_test_state(None, ValidationStatus::default());
        assert!(!ValidateMenu::has_active_algorithm(&state));
    }

    #[test]
    fn test_has_active_algorithm_true() {
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());
        assert!(ValidateMenu::has_active_algorithm(&state));
    }

    #[test]
    fn test_get_algorithm_name_none() {
        let state = create_test_state(None, ValidationStatus::default());
        assert_eq!(ValidateMenu::get_algorithm_name(&state), "None selected");
    }

    #[test]
    fn test_get_algorithm_name_some() {
        let algo = create_algorithm_summary("my_algo", StrategyType::MarketMaking);
        let state = create_test_state(Some(algo), ValidationStatus::default());
        assert_eq!(ValidateMenu::get_algorithm_name(&state), "my_algo");
    }

    #[test]
    fn test_truncate_reason_short() {
        assert_eq!(truncate_reason("Short", 10), "Short");
    }

    #[test]
    fn test_truncate_reason_exact() {
        assert_eq!(truncate_reason("1234567890", 10), "1234567890");
    }

    #[test]
    fn test_truncate_reason_long() {
        let result = truncate_reason("This is a very long reason", 10);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 10);
    }

    // -------------------------------------------------------------------------
    // Clone and Debug tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_menu_clone() {
        let menu = ValidateMenu::new();
        let cloned = menu.clone();
        assert_eq!(menu.title(), cloned.title());
    }

    #[test]
    fn test_validate_menu_debug() {
        let menu = ValidateMenu::new();
        let debug_str = format!("{:?}", menu);
        assert!(debug_str.contains("ValidateMenu"));
    }

    // -------------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_full_workflow_no_algorithm() {
        let mut menu = ValidateMenu::new();
        let state = create_test_state(None, ValidationStatus::default());

        // Try all validation actions
        for key in ['1', '2', '3', 'a', 'g', 'w', 'h'] {
            let action = menu.handle_key(KeyCode::Char(key), &state);
            assert!(
                matches!(action, SubMenuAction::ShowMessage(_)),
                "Key '{}' should show message when no algorithm", key
            );
        }

        // Presets should still work
        let action = menu.handle_key(KeyCode::Char('p'), &state);
        assert!(matches!(action, SubMenuAction::ExecuteCommand(_)));
    }

    #[test]
    fn test_full_workflow_with_algorithm() {
        let mut menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let state = create_test_state(Some(algo), ValidationStatus::default());

        // Validation commands should execute
        for key in ['1', '2', '3', 'a', 'h', 'p'] {
            let action = menu.handle_key(KeyCode::Char(key), &state);
            assert!(
                matches!(action, SubMenuAction::ExecuteCommand(_)),
                "Key '{}' should execute command when algorithm selected", key
            );
        }

        // Navigation commands
        for key in ['g', 'w'] {
            let action = menu.handle_key(KeyCode::Char(key), &state);
            assert!(
                matches!(action, SubMenuAction::Navigate(_)),
                "Key '{}' should navigate when algorithm selected", key
            );
        }
    }

    #[test]
    fn test_items_status_running() {
        let menu = ValidateMenu::new();
        let algo = create_algorithm_summary("test", StrategyType::Momentum);
        let validation = ValidationStatus {
            backtest: StageStatus::Running { progress: 0.5 },
            forward: StageStatus::NotRun,
            oos: StageStatus::NotRun,
            paper: StageStatus::NotRun,
            live: StageStatus::NotRun,
        };
        let state = create_test_state(Some(algo), validation);
        let items = menu.items(&state);

        // Backtest should show running status
        assert!(items[0].status.as_ref().unwrap().contains("◐"));
        assert!(items[0].status.as_ref().unwrap().contains("50%"));
    }

    #[test]
    fn test_validation_summary_with_running() {
        let validation = ValidationStatus {
            backtest: StageStatus::Passed { sharpe: 1.0, timestamp: Utc::now() },
            forward: StageStatus::Running { progress: 0.3 },
            oos: StageStatus::NotRun,
            paper: StageStatus::NotRun,
            live: StageStatus::NotRun,
        };
        let summary = ValidateMenu::validation_summary(&validation);
        // Only backtest passed, forward is running
        assert!(summary.contains("1/5 passed"));
        assert!(summary.contains("BT:✓"));
        assert!(summary.contains("FW:◐"));
    }
}
