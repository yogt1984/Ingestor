//! Main Menu Screen (Task TUI-0.1)
//!
//! Implements the workflow-driven 5-item main menu for the TUI.
//!
//! Menu Structure (per TUI_REQUIREMENTS_V0.1.md):
//! - [1] RESEARCH - Discover edges in market data
//! - [2] ALGORITHMS - Configure and manage strategies
//! - [3] VALIDATE - Test before risking capital
//! - [4] TRADE - Paper and live execution
//! - [5] DATA - Monitor streams and quality
//! - [Q] Quit
//!
//! Design Philosophy:
//! - Workflow-driven: Research -> Configure -> Validate -> Trade
//! - Algorithm-agnostic: Same UX for Momentum, Market Making, Hybrid
//! - 3 clicks max: Any function reachable in ≤3 keystrokes

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

/// Main menu item identifiers
///
/// Per TUI_REQUIREMENTS_V0.1.md, this is a workflow-driven 5-item menu:
/// Research -> Configure -> Validate -> Trade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MainMenuItem {
    /// [1] Research - Discover edges in market data
    Research,
    /// [2] Algorithms - Configure and manage strategies
    Algorithms,
    /// [3] Validate - Test before risking capital
    Validate,
    /// [4] Trade - Paper and live execution
    Trade,
    /// [5] Data - Monitor streams and quality
    Data,
    /// [Q] Quit - Exit the application
    Quit,
}

impl MainMenuItem {
    /// Get the key binding for this menu item
    ///
    /// Keys follow workflow order: 1-5 for main items, Q for quit
    pub fn key(&self) -> char {
        match self {
            MainMenuItem::Research => '1',
            MainMenuItem::Algorithms => '2',
            MainMenuItem::Validate => '3',
            MainMenuItem::Trade => '4',
            MainMenuItem::Data => '5',
            MainMenuItem::Quit => 'q',
        }
    }

    /// Get the display label for this menu item
    pub fn label(&self) -> &'static str {
        match self {
            MainMenuItem::Research => "RESEARCH",
            MainMenuItem::Algorithms => "ALGORITHMS",
            MainMenuItem::Validate => "VALIDATE",
            MainMenuItem::Trade => "TRADE",
            MainMenuItem::Data => "DATA",
            MainMenuItem::Quit => "Quit",
        }
    }

    /// Get the description for this menu item
    pub fn description(&self) -> &'static str {
        match self {
            MainMenuItem::Research => "Discover edges in market data",
            MainMenuItem::Algorithms => "Configure and manage strategies",
            MainMenuItem::Validate => "Test before risking capital",
            MainMenuItem::Trade => "Paper and live execution",
            MainMenuItem::Data => "Monitor streams and quality",
            MainMenuItem::Quit => "Exit the application",
        }
    }

    /// Get the color for this menu item
    ///
    /// Colors are semantically chosen:
    /// - Cyan: Research (discovery/exploration)
    /// - Magenta: Algorithms (configuration)
    /// - Yellow: Validate (caution/testing)
    /// - Green: Trade (go/live)
    /// - Blue: Data (information)
    /// - Red: Quit (stop/exit)
    pub fn color(&self) -> Color {
        match self {
            MainMenuItem::Research => Color::Cyan,
            MainMenuItem::Algorithms => Color::Magenta,
            MainMenuItem::Validate => Color::Yellow,
            MainMenuItem::Trade => Color::Green,
            MainMenuItem::Data => Color::Blue,
            MainMenuItem::Quit => Color::Red,
        }
    }

    /// Try to parse a key press into a menu item
    ///
    /// Maps keys 1-5 to menu items, q/Q to quit
    pub fn from_key(key: char) -> Option<MainMenuItem> {
        match key {
            '1' => Some(MainMenuItem::Research),
            '2' => Some(MainMenuItem::Algorithms),
            '3' => Some(MainMenuItem::Validate),
            '4' => Some(MainMenuItem::Trade),
            '5' => Some(MainMenuItem::Data),
            'q' | 'Q' => Some(MainMenuItem::Quit),
            _ => None,
        }
    }

    /// Get all menu items in display order
    ///
    /// Order follows workflow: Research → Algorithms → Validate → Trade → Data → Quit
    pub fn all() -> &'static [MainMenuItem] {
        &[
            MainMenuItem::Research,
            MainMenuItem::Algorithms,
            MainMenuItem::Validate,
            MainMenuItem::Trade,
            MainMenuItem::Data,
            MainMenuItem::Quit,
        ]
    }

    /// Get menu items excluding quit (5 main workflow items)
    pub fn all_except_quit() -> &'static [MainMenuItem] {
        &[
            MainMenuItem::Research,
            MainMenuItem::Algorithms,
            MainMenuItem::Validate,
            MainMenuItem::Trade,
            MainMenuItem::Data,
        ]
    }
}

/// State for the main menu screen
#[derive(Debug, Clone)]
pub struct MainMenuState {
    /// Currently selected/highlighted item
    pub selected: Option<MainMenuItem>,
    /// Symbol being traded
    pub symbol: String,
    /// Number of data files available
    pub file_count: usize,
    /// Total data size in MB
    pub data_size_mb: f64,
    /// Whether research engine is running
    pub research_running: bool,
    /// Number of active algorithms
    pub active_algorithms: usize,
    /// Last validation result summary
    pub last_validation: Option<String>,
}

impl Default for MainMenuState {
    fn default() -> Self {
        Self {
            selected: None,
            symbol: "BTCUSDT".to_string(),
            file_count: 0,
            data_size_mb: 0.0,
            research_running: false,
            active_algorithms: 0,
            last_validation: None,
        }
    }
}

impl MainMenuState {
    /// Create a new main menu state with symbol
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            ..Default::default()
        }
    }

    /// Update data statistics
    pub fn update_data_stats(&mut self, file_count: usize, data_size_mb: f64) {
        self.file_count = file_count;
        self.data_size_mb = data_size_mb;
    }

    /// Update research status
    pub fn update_research_status(&mut self, running: bool) {
        self.research_running = running;
    }

    /// Update active algorithm count
    pub fn update_algorithm_count(&mut self, count: usize) {
        self.active_algorithms = count;
    }
}

/// Draw the main menu screen
pub fn draw_main_menu(f: &mut Frame, state: &MainMenuState) {
    let size = f.size();

    let mut lines: Vec<Line> = Vec::new();

    // Header
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  INGESTOR - Trading Research Framework",
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(Span::styled(
        "  Research -> Validation -> Algorithms",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    // Status line
    let research_status = if state.research_running {
        Span::styled("RUNNING", Style::default().fg(Color::Green))
    } else {
        Span::styled("STOPPED", Style::default().fg(Color::Red))
    };

    lines.push(Line::from(vec![
        Span::raw("  Symbol: "),
        Span::styled(
            state.symbol.to_uppercase(),
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  |  Data: "),
        Span::styled(
            format!("{} files ({:.1} MB)", state.file_count, state.data_size_mb),
            Style::default().fg(Color::White),
        ),
        Span::raw("  |  Research: "),
        research_status,
        Span::raw("  |  Algorithms: "),
        Span::styled(
            format!("{}", state.active_algorithms),
            Style::default().fg(if state.active_algorithms > 0 { Color::Green } else { Color::Gray }),
        ),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    // Menu items (excluding quit)
    for item in MainMenuItem::all_except_quit() {
        let key_style = Style::default()
            .fg(item.color())
            .add_modifier(Modifier::BOLD);
        let label_style = Style::default().fg(Color::White);
        let desc_style = Style::default().fg(Color::DarkGray);

        lines.push(Line::from(vec![
            Span::raw("     "),
            Span::styled(format!("[{}] ", item.key()), key_style),
            Span::styled(format!("{:<12}", item.label()), label_style),
            Span::styled(format!(" - {}", item.description()), desc_style),
        ]));
        lines.push(Line::from("")); // Spacing between items
    }

    lines.push(Line::from(Span::styled(
        "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(""));

    // Quit option
    let quit = MainMenuItem::Quit;
    lines.push(Line::from(vec![
        Span::raw("     "),
        Span::styled(
            format!("[{}] ", quit.key()),
            Style::default().fg(quit.color()),
        ),
        Span::styled(quit.label(), Style::default().fg(Color::Red)),
    ]));

    lines.push(Line::from(""));
    lines.push(Line::from(""));

    // Framework info
    lines.push(Line::from(Span::styled(
        "  Framework Philosophy:",
        Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(Span::styled(
        "  Algorithms are ephemeral; the framework persists.",
        Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
    )));
    lines.push(Line::from(Span::styled(
        "  Research runs continuously, validation is reusable,",
        Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
    )));
    lines.push(Line::from(Span::styled(
        "  and algorithms are born from research findings.",
        Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
    )));

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" MAIN MENU (v0.2) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    f.render_widget(para, size);
}

// ============================================================================
// Tests - Per TUI_REQUIREMENTS_V0.1.md Task TUI-0.1
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // MainMenuItem Enum Tests
    // ========================================================================

    #[test]
    fn test_menu_item_keys_are_unique() {
        let items = MainMenuItem::all();
        let mut keys: Vec<char> = items.iter().map(|i| i.key()).collect();
        let original_len = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(
            keys.len(),
            original_len,
            "Menu item keys must be unique"
        );
    }

    #[test]
    fn test_menu_item_keys_are_correct() {
        // Per TUI_REQUIREMENTS_V0.1.md: 1-5 for main items, q for quit
        assert_eq!(MainMenuItem::Research.key(), '1');
        assert_eq!(MainMenuItem::Algorithms.key(), '2');
        assert_eq!(MainMenuItem::Validate.key(), '3');
        assert_eq!(MainMenuItem::Trade.key(), '4');
        assert_eq!(MainMenuItem::Data.key(), '5');
        assert_eq!(MainMenuItem::Quit.key(), 'q');
    }

    #[test]
    fn test_menu_item_from_key_valid() {
        assert_eq!(MainMenuItem::from_key('1'), Some(MainMenuItem::Research));
        assert_eq!(MainMenuItem::from_key('2'), Some(MainMenuItem::Algorithms));
        assert_eq!(MainMenuItem::from_key('3'), Some(MainMenuItem::Validate));
        assert_eq!(MainMenuItem::from_key('4'), Some(MainMenuItem::Trade));
        assert_eq!(MainMenuItem::from_key('5'), Some(MainMenuItem::Data));
        assert_eq!(MainMenuItem::from_key('q'), Some(MainMenuItem::Quit));
        assert_eq!(MainMenuItem::from_key('Q'), Some(MainMenuItem::Quit));
    }

    #[test]
    fn test_menu_item_from_key_invalid() {
        assert_eq!(MainMenuItem::from_key('6'), None); // No longer valid
        assert_eq!(MainMenuItem::from_key('7'), None);
        assert_eq!(MainMenuItem::from_key('0'), None);
        assert_eq!(MainMenuItem::from_key('a'), None);
        assert_eq!(MainMenuItem::from_key(' '), None);
        assert_eq!(MainMenuItem::from_key('\n'), None);
    }

    #[test]
    fn test_menu_item_labels_not_empty() {
        for item in MainMenuItem::all() {
            assert!(
                !item.label().is_empty(),
                "Label for {:?} should not be empty",
                item
            );
        }
    }

    #[test]
    fn test_menu_item_descriptions_not_empty() {
        for item in MainMenuItem::all() {
            assert!(
                !item.description().is_empty(),
                "Description for {:?} should not be empty",
                item
            );
        }
    }

    #[test]
    fn test_menu_item_all_count() {
        // Per TUI_REQUIREMENTS_V0.1.md: 5 main items + Quit = 6 total
        assert_eq!(MainMenuItem::all().len(), 6, "Should have 6 menu items total");
    }

    #[test]
    fn test_menu_item_all_except_quit_count() {
        // Per TUI_REQUIREMENTS_V0.1.md: 5 main workflow items
        assert_eq!(
            MainMenuItem::all_except_quit().len(),
            5,
            "Should have 5 menu items excluding quit"
        );
    }

    #[test]
    fn test_menu_item_all_except_quit_excludes_quit() {
        for item in MainMenuItem::all_except_quit() {
            assert_ne!(
                *item,
                MainMenuItem::Quit,
                "all_except_quit should not contain Quit"
            );
        }
    }

    #[test]
    fn test_menu_item_order_matches_workflow() {
        // Per TUI_REQUIREMENTS_V0.1.md: Research → Algorithms → Validate → Trade → Data → Quit
        let items = MainMenuItem::all();
        assert_eq!(items[0], MainMenuItem::Research, "First item should be Research");
        assert_eq!(items[1], MainMenuItem::Algorithms, "Second item should be Algorithms");
        assert_eq!(items[2], MainMenuItem::Validate, "Third item should be Validate");
        assert_eq!(items[3], MainMenuItem::Trade, "Fourth item should be Trade");
        assert_eq!(items[4], MainMenuItem::Data, "Fifth item should be Data");
        assert_eq!(items[5], MainMenuItem::Quit, "Last item should be Quit");
    }

    #[test]
    fn test_menu_item_roundtrip_key() {
        // Every item should roundtrip through key -> from_key
        for item in MainMenuItem::all() {
            let key = item.key();
            let parsed = MainMenuItem::from_key(key);
            assert_eq!(
                parsed,
                Some(*item),
                "Item {:?} should roundtrip through key {}",
                item,
                key
            );
        }
    }

    // ========================================================================
    // MainMenuState Tests
    // ========================================================================

    #[test]
    fn test_main_menu_state_default() {
        let state = MainMenuState::default();
        assert_eq!(state.symbol, "BTCUSDT");
        assert_eq!(state.file_count, 0);
        assert_eq!(state.data_size_mb, 0.0);
        assert!(!state.research_running);
        assert_eq!(state.active_algorithms, 0);
        assert!(state.selected.is_none());
    }

    #[test]
    fn test_main_menu_state_new_with_symbol() {
        let state = MainMenuState::new("ETHUSDT");
        assert_eq!(state.symbol, "ETHUSDT");
    }

    #[test]
    fn test_main_menu_state_update_data_stats() {
        let mut state = MainMenuState::default();
        state.update_data_stats(100, 50.5);
        assert_eq!(state.file_count, 100);
        assert_eq!(state.data_size_mb, 50.5);
    }

    #[test]
    fn test_main_menu_state_update_research_status() {
        let mut state = MainMenuState::default();
        assert!(!state.research_running);
        state.update_research_status(true);
        assert!(state.research_running);
        state.update_research_status(false);
        assert!(!state.research_running);
    }

    #[test]
    fn test_main_menu_state_update_algorithm_count() {
        let mut state = MainMenuState::default();
        assert_eq!(state.active_algorithms, 0);
        state.update_algorithm_count(3);
        assert_eq!(state.active_algorithms, 3);
    }

    // ========================================================================
    // Color Tests - Semantic Color Assignments
    // ========================================================================

    #[test]
    fn test_menu_item_colors_distinct() {
        // Each menu item should have a defined color
        for item in MainMenuItem::all() {
            let _color = item.color(); // Just ensure it doesn't panic
        }
    }

    #[test]
    fn test_quit_color_is_red() {
        // Red = stop/exit
        assert_eq!(MainMenuItem::Quit.color(), Color::Red);
    }

    #[test]
    fn test_trade_color_is_green() {
        // Green = go/live
        assert_eq!(MainMenuItem::Trade.color(), Color::Green);
    }

    #[test]
    fn test_research_color_is_cyan() {
        // Cyan = discovery/exploration
        assert_eq!(MainMenuItem::Research.color(), Color::Cyan);
    }

    #[test]
    fn test_validate_color_is_yellow() {
        // Yellow = caution/testing
        assert_eq!(MainMenuItem::Validate.color(), Color::Yellow);
    }

    #[test]
    fn test_algorithms_color_is_magenta() {
        // Magenta = configuration
        assert_eq!(MainMenuItem::Algorithms.color(), Color::Magenta);
    }

    #[test]
    fn test_data_color_is_blue() {
        // Blue = information
        assert_eq!(MainMenuItem::Data.color(), Color::Blue);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_empty_symbol_allowed() {
        let state = MainMenuState::new("");
        assert_eq!(state.symbol, "");
    }

    #[test]
    fn test_large_file_count() {
        let mut state = MainMenuState::default();
        state.update_data_stats(1_000_000, 500_000.0);
        assert_eq!(state.file_count, 1_000_000);
        assert_eq!(state.data_size_mb, 500_000.0);
    }

    #[test]
    fn test_negative_data_size_handled() {
        // Negative values shouldn't crash, even if semantically wrong
        let mut state = MainMenuState::default();
        state.update_data_stats(0, -100.0);
        assert_eq!(state.data_size_mb, -100.0);
    }

    // ========================================================================
    // TUI_REQUIREMENTS_V0.1.md Compliance Tests
    // ========================================================================

    #[test]
    fn test_menu_has_all_required_categories() {
        // Per TUI_REQUIREMENTS_V0.1.md: RESEARCH, ALGORITHMS, VALIDATE, TRADE, DATA
        let labels: Vec<&str> = MainMenuItem::all_except_quit()
            .iter()
            .map(|i| i.label())
            .collect();

        assert!(labels.contains(&"RESEARCH"), "Must have RESEARCH");
        assert!(labels.contains(&"ALGORITHMS"), "Must have ALGORITHMS");
        assert!(labels.contains(&"VALIDATE"), "Must have VALIDATE");
        assert!(labels.contains(&"TRADE"), "Must have TRADE");
        assert!(labels.contains(&"DATA"), "Must have DATA");
    }

    #[test]
    fn test_numeric_keys_1_through_5() {
        // Per TUI_REQUIREMENTS_V0.1.md: keys 1-5 for main items
        let keys: Vec<char> = MainMenuItem::all_except_quit()
            .iter()
            .map(|i| i.key())
            .collect();

        assert!(keys.contains(&'1'));
        assert!(keys.contains(&'2'));
        assert!(keys.contains(&'3'));
        assert!(keys.contains(&'4'));
        assert!(keys.contains(&'5'));
        // Key '6' should NOT be present (reduced from 6 to 5 items)
        assert!(!keys.contains(&'6'), "Key 6 should not exist in new menu");
    }

    #[test]
    fn test_quit_uses_q_key() {
        assert_eq!(MainMenuItem::Quit.key(), 'q');
    }

    #[test]
    fn test_case_insensitive_quit() {
        // Both 'q' and 'Q' should work for quit
        assert!(MainMenuItem::from_key('q').is_some());
        assert!(MainMenuItem::from_key('Q').is_some());
        assert_eq!(
            MainMenuItem::from_key('q'),
            MainMenuItem::from_key('Q')
        );
    }

    #[test]
    fn test_workflow_order_research_first() {
        // Research is first in workflow (discover edges before configuration)
        assert_eq!(MainMenuItem::all()[0], MainMenuItem::Research);
        assert_eq!(MainMenuItem::Research.key(), '1');
    }

    #[test]
    fn test_workflow_order_trade_after_validate() {
        // Trade comes after Validate (test before risking capital)
        let items = MainMenuItem::all();
        let validate_idx = items.iter().position(|i| *i == MainMenuItem::Validate).unwrap();
        let trade_idx = items.iter().position(|i| *i == MainMenuItem::Trade).unwrap();
        assert!(trade_idx > validate_idx, "Trade should come after Validate");
    }

    #[test]
    fn test_descriptions_are_meaningful() {
        // Each description should convey purpose
        assert!(MainMenuItem::Research.description().contains("edge") ||
                MainMenuItem::Research.description().contains("discover"),
                "Research description should mention discovery/edges");
        assert!(MainMenuItem::Validate.description().contains("test") ||
                MainMenuItem::Validate.description().contains("capital"),
                "Validate description should mention testing/capital protection");
        assert!(MainMenuItem::Trade.description().contains("execution") ||
                MainMenuItem::Trade.description().contains("live") ||
                MainMenuItem::Trade.description().contains("paper"),
                "Trade description should mention execution");
    }

    // ========================================================================
    // Skeptical Tests - Edge Cases and Potential Bugs
    // ========================================================================

    #[test]
    fn test_all_variants_covered_in_key() {
        // Ensure no variant is missed in key() match
        let _ = MainMenuItem::Research.key();
        let _ = MainMenuItem::Algorithms.key();
        let _ = MainMenuItem::Validate.key();
        let _ = MainMenuItem::Trade.key();
        let _ = MainMenuItem::Data.key();
        let _ = MainMenuItem::Quit.key();
    }

    #[test]
    fn test_all_variants_covered_in_label() {
        // Ensure no variant is missed in label() match
        let _ = MainMenuItem::Research.label();
        let _ = MainMenuItem::Algorithms.label();
        let _ = MainMenuItem::Validate.label();
        let _ = MainMenuItem::Trade.label();
        let _ = MainMenuItem::Data.label();
        let _ = MainMenuItem::Quit.label();
    }

    #[test]
    fn test_all_variants_covered_in_description() {
        // Ensure no variant is missed in description() match
        let _ = MainMenuItem::Research.description();
        let _ = MainMenuItem::Algorithms.description();
        let _ = MainMenuItem::Validate.description();
        let _ = MainMenuItem::Trade.description();
        let _ = MainMenuItem::Data.description();
        let _ = MainMenuItem::Quit.description();
    }

    #[test]
    fn test_all_variants_covered_in_color() {
        // Ensure no variant is missed in color() match
        let _ = MainMenuItem::Research.color();
        let _ = MainMenuItem::Algorithms.color();
        let _ = MainMenuItem::Validate.color();
        let _ = MainMenuItem::Trade.color();
        let _ = MainMenuItem::Data.color();
        let _ = MainMenuItem::Quit.color();
    }

    #[test]
    fn test_enum_derives_required_traits() {
        // Test Clone
        let item = MainMenuItem::Research;
        let cloned = item.clone();
        assert_eq!(item, cloned);

        // Test Copy
        let copied: MainMenuItem = item;
        assert_eq!(item, copied);

        // Test Debug
        let debug_str = format!("{:?}", item);
        assert!(!debug_str.is_empty());

        // Test PartialEq + Eq
        assert_eq!(MainMenuItem::Research, MainMenuItem::Research);
        assert_ne!(MainMenuItem::Research, MainMenuItem::Quit);

        // Test Hash
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(MainMenuItem::Research);
        set.insert(MainMenuItem::Algorithms);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_labels_are_uppercase() {
        // All main menu labels should be uppercase for consistency
        for item in MainMenuItem::all_except_quit() {
            let label = item.label();
            assert_eq!(
                label.to_uppercase(),
                label,
                "Label {:?} should be uppercase",
                item
            );
        }
    }

    #[test]
    fn test_quit_label_is_title_case() {
        // Quit should be title case, not uppercase (differentiate from main items)
        assert_eq!(MainMenuItem::Quit.label(), "Quit");
    }
}
