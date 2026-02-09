//! Data Menu implementation (TUI-5.0)
//!
//! Provides a submenu for data management and monitoring:
//! - [L] Live Stream: Real-time feature visualization + recording
//! - [F] Features: Detailed feature inspection
//! - [I] Info: Dataset statistics
//! - [Q] Quality: Data validation checks
//! - [P] Persist: Toggle data persistence to disk
//! - [S] Storage: Cycle max storage (1/5/10/50/100/∞ GB)
//!
//! Footer shows data statistics: file count, days, date range, event count

use crossterm::event::KeyCode;

use crate::ui::state::{GlobalState, DataStats};
use crate::ui::submenu::{
    SubMenu, SubMenuAction, SubMenuItem, NavigationTarget, CliCommand,
    SettingUpdate, key_to_char, is_back_key,
};

// ============================================================================
// DataMenu
// ============================================================================

/// Data submenu implementing the SubMenu trait
#[derive(Debug, Clone, Default)]
pub struct DataMenu;

impl DataMenu {
    /// Create a new DataMenu
    pub fn new() -> Self {
        Self
    }

    /// Format file count for display
    fn format_file_count(stats: &DataStats) -> String {
        format!("{} files", stats.file_count)
    }

    /// Format date range for display
    fn format_date_range(stats: &DataStats) -> String {
        match &stats.date_range {
            Some((start, end)) => {
                let days = (*end - *start).num_days() + 1;
                format!("{} days | {} - {}", days, start.format("%b %d"), end.format("%b %d, %Y"))
            }
            None => "No data".to_string(),
        }
    }

    /// Format event count for display
    fn format_event_count(stats: &DataStats) -> String {
        if stats.total_events >= 1000 {
            format!("{}k events", stats.total_events / 1000)
        } else {
            format!("{} events", stats.total_events)
        }
    }

    /// Format size in MB for display
    fn format_size(stats: &DataStats) -> String {
        if stats.size_mb >= 1.0 {
            format!("{:.1} MB", stats.size_mb)
        } else {
            format!("{:.0} KB", stats.size_mb * 1024.0)
        }
    }

    /// Check if data is available
    fn has_data(state: &GlobalState) -> bool {
        state.data_stats.file_count > 0
    }

    /// Get data summary string
    fn data_summary(state: &GlobalState) -> String {
        let stats = &state.data_stats;
        if stats.file_count == 0 {
            "No data available".to_string()
        } else {
            format!(
                "{} | {} | {}",
                Self::format_file_count(stats),
                Self::format_date_range(stats),
                Self::format_event_count(stats),
            )
        }
    }
}

impl SubMenu for DataMenu {
    fn title(&self) -> &str {
        "DATA - Market Data & Quality"
    }

    fn items(&self, state: &GlobalState) -> Vec<SubMenuItem> {
        let has_data = Self::has_data(state);

        // Format persist status
        let persist_label = if state.persist_features {
            "Persist: ON"
        } else {
            "Persist: OFF"
        };

        // Format storage status
        let storage_label = if state.max_storage_gb <= 0.0 {
            "Storage: Unlimited".to_string()
        } else {
            format!("Storage: {} GB", state.max_storage_gb as i32)
        };

        vec![
            SubMenuItem::new('L', "Live Stream", "Real-time feature visualization + recording")
                .with_enabled(true), // Live stream always available (connects to exchange)
            SubMenuItem::new('F', "Features", "Detailed feature inspection")
                .with_enabled(true), // Features mode always available
            SubMenuItem::new('I', "Info", "Dataset statistics")
                .with_enabled(has_data),
            SubMenuItem::new('Q', "Quality", "Data validation checks")
                .with_enabled(has_data),
            SubMenuItem::new('P', persist_label, "Toggle data persistence to disk")
                .with_enabled(true),
            SubMenuItem::new('S', &storage_label, "Cycle max storage (1/5/10/50/100/∞ GB)")
                .with_enabled(true),
        ]
    }

    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction {
        if is_back_key(key) {
            return SubMenuAction::Back;
        }

        let has_data = Self::has_data(state);

        if let Some(c) = key_to_char(key) {
            match c.to_ascii_lowercase() {
                'l' => {
                    // Live data stream - navigate to TUI Live mode
                    SubMenuAction::Navigate(NavigationTarget::Live)
                }
                'f' => {
                    // Features inspection - navigate to TUI Features mode
                    SubMenuAction::Navigate(NavigationTarget::Features)
                }
                'i' => {
                    // Data info
                    if has_data {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::backtest("info", vec![])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No data available. Start Live Stream to collect data.".to_string()
                        )
                    }
                }
                'q' => {
                    // Quality check
                    if has_data {
                        SubMenuAction::ExecuteCommand(
                            CliCommand::backtest("validate-data", vec![])
                        )
                    } else {
                        SubMenuAction::ShowMessage(
                            "No data available. Start Live Stream to collect data.".to_string()
                        )
                    }
                }
                'p' => {
                    // Toggle persist to disk
                    SubMenuAction::UpdateSetting(SettingUpdate::TogglePersist)
                }
                's' => {
                    // Cycle max storage
                    SubMenuAction::UpdateSetting(SettingUpdate::CycleMaxStorage)
                }
                _ => SubMenuAction::None,
            }
        } else {
            SubMenuAction::None
        }
    }

    fn footer(&self, state: &GlobalState) -> Option<String> {
        Some(format!("Data: {}", Self::data_summary(state)))
    }

    fn can_enter(&self, _state: &GlobalState) -> bool {
        // Always allow entry to data menu
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

/// Draw the data menu
pub fn draw_data_menu(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    menu: &DataMenu,
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
    use chrono::NaiveDate;

    // -------------------------------------------------------------------------
    // Helper for creating test state
    // -------------------------------------------------------------------------

    fn create_test_state(data_stats: DataStats) -> GlobalState {
        GlobalState {
            symbol: "BTCUSDT".to_string(),
            data_stats,
            ..Default::default()
        }
    }

    fn create_data_stats_empty() -> DataStats {
        DataStats::default()
    }

    fn create_data_stats_with_data() -> DataStats {
        DataStats {
            file_count: 97,
            total_events: 73000,
            date_range: Some((
                NaiveDate::from_ymd_opt(2025, 10, 16).unwrap(),
                NaiveDate::from_ymd_opt(2025, 12, 2).unwrap(),
            )),
            size_mb: 45.5,
        }
    }

    fn create_data_stats_small() -> DataStats {
        DataStats {
            file_count: 3,
            total_events: 500,
            date_range: Some((
                NaiveDate::from_ymd_opt(2025, 12, 1).unwrap(),
                NaiveDate::from_ymd_opt(2025, 12, 1).unwrap(),
            )),
            size_mb: 0.5,
        }
    }

    // -------------------------------------------------------------------------
    // Construction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_data_menu_new() {
        let menu = DataMenu::new();
        assert_eq!(menu.title(), "DATA - Market Data & Quality");
    }

    #[test]
    fn test_data_menu_default() {
        let menu = DataMenu::default();
        assert_eq!(menu.title(), "DATA - Market Data & Quality");
    }

    // -------------------------------------------------------------------------
    // Title tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_title() {
        let menu = DataMenu::new();
        assert_eq!(menu.title(), "DATA - Market Data & Quality");
    }

    // -------------------------------------------------------------------------
    // Helper function tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_format_file_count() {
        let stats = create_data_stats_with_data();
        let result = DataMenu::format_file_count(&stats);
        assert_eq!(result, "97 files");
    }

    #[test]
    fn test_format_file_count_zero() {
        let stats = create_data_stats_empty();
        let result = DataMenu::format_file_count(&stats);
        assert_eq!(result, "0 files");
    }

    #[test]
    fn test_format_date_range_with_data() {
        let stats = create_data_stats_with_data();
        let result = DataMenu::format_date_range(&stats);
        assert!(result.contains("48 days")); // Oct 16 to Dec 2 = 48 days
        assert!(result.contains("Oct 16"));
        assert!(result.contains("Dec 02"));
    }

    #[test]
    fn test_format_date_range_single_day() {
        let stats = create_data_stats_small();
        let result = DataMenu::format_date_range(&stats);
        assert!(result.contains("1 days"));
    }

    #[test]
    fn test_format_date_range_no_data() {
        let stats = create_data_stats_empty();
        let result = DataMenu::format_date_range(&stats);
        assert_eq!(result, "No data");
    }

    #[test]
    fn test_format_event_count_thousands() {
        let stats = create_data_stats_with_data();
        let result = DataMenu::format_event_count(&stats);
        assert_eq!(result, "73k events");
    }

    #[test]
    fn test_format_event_count_small() {
        let stats = create_data_stats_small();
        let result = DataMenu::format_event_count(&stats);
        assert_eq!(result, "500 events");
    }

    #[test]
    fn test_format_event_count_zero() {
        let stats = create_data_stats_empty();
        let result = DataMenu::format_event_count(&stats);
        assert_eq!(result, "0 events");
    }

    #[test]
    fn test_format_size_large() {
        let stats = create_data_stats_with_data();
        let result = DataMenu::format_size(&stats);
        assert_eq!(result, "45.5 MB");
    }

    #[test]
    fn test_format_size_small() {
        let stats = create_data_stats_small();
        let result = DataMenu::format_size(&stats);
        assert_eq!(result, "512 KB");
    }

    #[test]
    fn test_format_size_zero() {
        let stats = create_data_stats_empty();
        let result = DataMenu::format_size(&stats);
        assert_eq!(result, "0 KB");
    }

    #[test]
    fn test_has_data_true() {
        let state = create_test_state(create_data_stats_with_data());
        assert!(DataMenu::has_data(&state));
    }

    #[test]
    fn test_has_data_false() {
        let state = create_test_state(create_data_stats_empty());
        assert!(!DataMenu::has_data(&state));
    }

    #[test]
    fn test_data_summary_with_data() {
        let state = create_test_state(create_data_stats_with_data());
        let summary = DataMenu::data_summary(&state);
        assert!(summary.contains("97 files"));
        assert!(summary.contains("73k events"));
    }

    #[test]
    fn test_data_summary_no_data() {
        let state = create_test_state(create_data_stats_empty());
        let summary = DataMenu::data_summary(&state);
        assert_eq!(summary, "No data available");
    }

    // -------------------------------------------------------------------------
    // Items tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_items_count() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());
        let items = menu.items(&state);
        assert_eq!(items.len(), 6); // L, F, I, Q, P, S
    }

    #[test]
    fn test_items_keys() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());
        let items = menu.items(&state);

        let keys: Vec<char> = items.iter().map(|i| i.key).collect();
        assert_eq!(keys, vec!['L', 'F', 'I', 'Q', 'P', 'S']);
    }

    #[test]
    fn test_items_labels() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());
        let items = menu.items(&state);

        assert_eq!(items[0].label, "Live Stream");
        assert_eq!(items[1].label, "Features");
        assert_eq!(items[2].label, "Info");
        assert_eq!(items[3].label, "Quality");
        // P and S labels are dynamic based on state
    }

    #[test]
    fn test_items_no_data_live_features_enabled() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());
        let items = menu.items(&state);

        // Live and Features should always be enabled
        assert!(items[0].enabled); // L - Live Stream
        assert!(items[1].enabled); // F - Features
    }

    #[test]
    fn test_items_no_data_info_quality_disabled() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());
        let items = menu.items(&state);

        // Info and Quality should be disabled without data
        assert!(!items[2].enabled); // I - Info
        assert!(!items[3].enabled); // Q - Quality
    }

    #[test]
    fn test_items_with_data_all_enabled() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());
        let items = menu.items(&state);

        // All items should be enabled with data
        for item in &items {
            assert!(item.enabled, "Item {} should be enabled", item.key);
        }
    }

    // -------------------------------------------------------------------------
    // Key handling tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_handle_key_escape() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Esc, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_backspace() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Backspace, &state);
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_handle_key_l_live_stream() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('l'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::Live));
    }

    #[test]
    fn test_handle_key_f_features() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('f'), &state);
        assert_eq!(action, SubMenuAction::Navigate(NavigationTarget::Features));
    }

    #[test]
    fn test_handle_key_i_info_with_data() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());

        let action = menu.handle_key(KeyCode::Char('i'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "backtest");
            assert!(cmd.args.contains(&"info".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_i_info_no_data() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('i'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("No data"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_q_quality_with_data() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());

        let action = menu.handle_key(KeyCode::Char('q'), &state);
        if let SubMenuAction::ExecuteCommand(cmd) = action {
            assert_eq!(cmd.binary, "backtest");
            assert!(cmd.args.contains(&"validate-data".to_string()));
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    #[test]
    fn test_handle_key_q_quality_no_data() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('q'), &state);
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("No data"));
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_handle_key_unknown() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('x'), &state);
        assert_eq!(action, SubMenuAction::None);
    }

    #[test]
    fn test_handle_key_case_insensitive() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());

        // Lowercase
        let action_lower = menu.handle_key(KeyCode::Char('l'), &state);
        // Uppercase
        let action_upper = menu.handle_key(KeyCode::Char('L'), &state);

        // Both should result in Navigate
        assert!(matches!(action_lower, SubMenuAction::Navigate(_)));
        assert!(matches!(action_upper, SubMenuAction::Navigate(_)));
    }

    // -------------------------------------------------------------------------
    // Footer tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_footer_with_data() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("Data:"));
        assert!(f.contains("97 files"));
        assert!(f.contains("73k events"));
    }

    #[test]
    fn test_footer_no_data() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let footer = menu.footer(&state);
        assert!(footer.is_some());
        let f = footer.unwrap();
        assert!(f.contains("No data available"));
    }

    // -------------------------------------------------------------------------
    // Access control tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_can_enter_always_true() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());
        assert!(menu.can_enter(&state));
    }

    #[test]
    fn test_blocked_message_always_none() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());
        assert!(menu.blocked_message(&state).is_none());
    }

    // -------------------------------------------------------------------------
    // Clone and Debug tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_data_menu_clone() {
        let menu = DataMenu::new();
        let cloned = menu.clone();
        assert_eq!(menu.title(), cloned.title());
    }

    #[test]
    fn test_data_menu_debug() {
        let menu = DataMenu::new();
        let debug_str = format!("{:?}", menu);
        assert!(debug_str.contains("DataMenu"));
    }

    // -------------------------------------------------------------------------
    // Integration tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_full_workflow_no_data() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        // Live and Features should navigate
        let action_l = menu.handle_key(KeyCode::Char('l'), &state);
        assert!(matches!(action_l, SubMenuAction::Navigate(_)));

        let action_f = menu.handle_key(KeyCode::Char('f'), &state);
        assert!(matches!(action_f, SubMenuAction::Navigate(_)));

        // Info and Quality should show message
        let action_i = menu.handle_key(KeyCode::Char('i'), &state);
        assert!(matches!(action_i, SubMenuAction::ShowMessage(_)));

        let action_q = menu.handle_key(KeyCode::Char('q'), &state);
        assert!(matches!(action_q, SubMenuAction::ShowMessage(_)));
    }

    #[test]
    fn test_full_workflow_with_data() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_with_data());

        // All actions should work
        let action_l = menu.handle_key(KeyCode::Char('l'), &state);
        assert!(matches!(action_l, SubMenuAction::Navigate(_)));

        let action_f = menu.handle_key(KeyCode::Char('f'), &state);
        assert!(matches!(action_f, SubMenuAction::Navigate(_)));

        let action_i = menu.handle_key(KeyCode::Char('i'), &state);
        assert!(matches!(action_i, SubMenuAction::ExecuteCommand(_)));

        let action_q = menu.handle_key(KeyCode::Char('q'), &state);
        assert!(matches!(action_q, SubMenuAction::ExecuteCommand(_)));
    }

    #[test]
    fn test_navigation_targets() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        // Check specific navigation targets
        let action_l = menu.handle_key(KeyCode::Char('l'), &state);
        assert_eq!(action_l, SubMenuAction::Navigate(NavigationTarget::Live));

        let action_f = menu.handle_key(KeyCode::Char('f'), &state);
        assert_eq!(action_f, SubMenuAction::Navigate(NavigationTarget::Features));
    }

    #[test]
    fn test_data_stats_display_formatting() {
        // Test various data size scenarios
        let small_stats = DataStats {
            file_count: 5,
            total_events: 100,
            date_range: Some((
                NaiveDate::from_ymd_opt(2025, 12, 1).unwrap(),
                NaiveDate::from_ymd_opt(2025, 12, 5).unwrap(),
            )),
            size_mb: 0.1,
        };
        let state = create_test_state(small_stats);
        let summary = DataMenu::data_summary(&state);
        assert!(summary.contains("5 files"));
        assert!(summary.contains("100 events")); // Not "0k events"
    }

    #[test]
    fn test_boundary_event_count() {
        // Test boundary at 1000 events
        let stats_999 = DataStats {
            file_count: 1,
            total_events: 999,
            date_range: None,
            size_mb: 0.1,
        };
        let result = DataMenu::format_event_count(&stats_999);
        assert_eq!(result, "999 events");

        let stats_1000 = DataStats {
            file_count: 1,
            total_events: 1000,
            date_range: None,
            size_mb: 0.1,
        };
        let result = DataMenu::format_event_count(&stats_1000);
        assert_eq!(result, "1k events");
    }

    #[test]
    fn test_boundary_size_mb() {
        // Test boundary at 1.0 MB
        let stats_small = DataStats {
            file_count: 1,
            total_events: 100,
            date_range: None,
            size_mb: 0.999,
        };
        let result = DataMenu::format_size(&stats_small);
        assert!(result.contains("KB"));

        let stats_large = DataStats {
            file_count: 1,
            total_events: 100,
            date_range: None,
            size_mb: 1.0,
        };
        let result = DataMenu::format_size(&stats_large);
        assert!(result.contains("MB"));
    }

    // -------------------------------------------------------------------------
    // Settings tests (P and S keys)
    // -------------------------------------------------------------------------

    #[test]
    fn test_handle_key_p_toggle_persist() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('p'), &state);
        assert_eq!(action, SubMenuAction::UpdateSetting(SettingUpdate::TogglePersist));
    }

    #[test]
    fn test_handle_key_s_cycle_storage() {
        let mut menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let action = menu.handle_key(KeyCode::Char('s'), &state);
        assert_eq!(action, SubMenuAction::UpdateSetting(SettingUpdate::CycleMaxStorage));
    }

    #[test]
    fn test_persist_label_on() {
        let menu = DataMenu::new();
        let mut state = create_test_state(create_data_stats_empty());
        state.persist_features = true;

        let items = menu.items(&state);
        let persist_item = items.iter().find(|i| i.key == 'P').unwrap();
        assert_eq!(persist_item.label, "Persist: ON");
    }

    #[test]
    fn test_persist_label_off() {
        let menu = DataMenu::new();
        let mut state = create_test_state(create_data_stats_empty());
        state.persist_features = false;

        let items = menu.items(&state);
        let persist_item = items.iter().find(|i| i.key == 'P').unwrap();
        assert_eq!(persist_item.label, "Persist: OFF");
    }

    #[test]
    fn test_storage_label_limited() {
        let menu = DataMenu::new();
        let mut state = create_test_state(create_data_stats_empty());
        state.max_storage_gb = 10.0;

        let items = menu.items(&state);
        let storage_item = items.iter().find(|i| i.key == 'S').unwrap();
        assert_eq!(storage_item.label, "Storage: 10 GB");
    }

    #[test]
    fn test_storage_label_unlimited() {
        let menu = DataMenu::new();
        let mut state = create_test_state(create_data_stats_empty());
        state.max_storage_gb = 0.0;

        let items = menu.items(&state);
        let storage_item = items.iter().find(|i| i.key == 'S').unwrap();
        assert_eq!(storage_item.label, "Storage: Unlimited");
    }

    #[test]
    fn test_settings_items_always_enabled() {
        let menu = DataMenu::new();
        let state = create_test_state(create_data_stats_empty());

        let items = menu.items(&state);
        let persist_item = items.iter().find(|i| i.key == 'P').unwrap();
        let storage_item = items.iter().find(|i| i.key == 'S').unwrap();

        assert!(persist_item.enabled);
        assert!(storage_item.enabled);
    }
}
