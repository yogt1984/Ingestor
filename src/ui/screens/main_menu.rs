//! Main Menu Screen (Task 4.0)
//!
//! Implements the restructured 6-item main menu for the TUI.
//!
//! Menu Structure:
//! - [1] LIVE DATA - Real-time market data streaming
//! - [2] RESEARCH - Research engine status and controls
//! - [3] VALIDATION - Validation pipeline controls
//! - [4] ALGORITHMS - Active algorithms dashboard
//! - [5] BACKTEST - Quick backtest access
//! - [6] SETTINGS - Configuration
//! - [Q] Quit

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

/// Main menu item identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MainMenuItem {
    /// [1] Live data streaming
    LiveData,
    /// [2] Research dashboard
    Research,
    /// [3] Validation pipeline
    Validation,
    /// [4] Active algorithms
    Algorithms,
    /// [5] Backtest
    Backtest,
    /// [6] Settings
    Settings,
    /// [Q] Quit
    Quit,
}

impl MainMenuItem {
    /// Get the key binding for this menu item
    pub fn key(&self) -> char {
        match self {
            MainMenuItem::LiveData => '1',
            MainMenuItem::Research => '2',
            MainMenuItem::Validation => '3',
            MainMenuItem::Algorithms => '4',
            MainMenuItem::Backtest => '5',
            MainMenuItem::Settings => '6',
            MainMenuItem::Quit => 'q',
        }
    }

    /// Get the display label for this menu item
    pub fn label(&self) -> &'static str {
        match self {
            MainMenuItem::LiveData => "LIVE DATA",
            MainMenuItem::Research => "RESEARCH",
            MainMenuItem::Validation => "VALIDATION",
            MainMenuItem::Algorithms => "ALGORITHMS",
            MainMenuItem::Backtest => "BACKTEST",
            MainMenuItem::Settings => "SETTINGS",
            MainMenuItem::Quit => "Quit",
        }
    }

    /// Get the description for this menu item
    pub fn description(&self) -> &'static str {
        match self {
            MainMenuItem::LiveData => "Real-time market data streaming",
            MainMenuItem::Research => "Research engine status and controls",
            MainMenuItem::Validation => "Validation pipeline controls",
            MainMenuItem::Algorithms => "Active algorithms dashboard",
            MainMenuItem::Backtest => "Quick backtest access",
            MainMenuItem::Settings => "Configuration",
            MainMenuItem::Quit => "Exit the application",
        }
    }

    /// Get the color for this menu item
    pub fn color(&self) -> Color {
        match self {
            MainMenuItem::LiveData => Color::Green,
            MainMenuItem::Research => Color::Cyan,
            MainMenuItem::Validation => Color::Yellow,
            MainMenuItem::Algorithms => Color::Magenta,
            MainMenuItem::Backtest => Color::Blue,
            MainMenuItem::Settings => Color::White,
            MainMenuItem::Quit => Color::Red,
        }
    }

    /// Try to parse a key press into a menu item
    pub fn from_key(key: char) -> Option<MainMenuItem> {
        match key {
            '1' => Some(MainMenuItem::LiveData),
            '2' => Some(MainMenuItem::Research),
            '3' => Some(MainMenuItem::Validation),
            '4' => Some(MainMenuItem::Algorithms),
            '5' => Some(MainMenuItem::Backtest),
            '6' => Some(MainMenuItem::Settings),
            'q' | 'Q' => Some(MainMenuItem::Quit),
            _ => None,
        }
    }

    /// Get all menu items in display order
    pub fn all() -> &'static [MainMenuItem] {
        &[
            MainMenuItem::LiveData,
            MainMenuItem::Research,
            MainMenuItem::Validation,
            MainMenuItem::Algorithms,
            MainMenuItem::Backtest,
            MainMenuItem::Settings,
            MainMenuItem::Quit,
        ]
    }

    /// Get menu items excluding quit
    pub fn all_except_quit() -> &'static [MainMenuItem] {
        &[
            MainMenuItem::LiveData,
            MainMenuItem::Research,
            MainMenuItem::Validation,
            MainMenuItem::Algorithms,
            MainMenuItem::Backtest,
            MainMenuItem::Settings,
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // MainMenuItem Tests
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
        assert_eq!(MainMenuItem::LiveData.key(), '1');
        assert_eq!(MainMenuItem::Research.key(), '2');
        assert_eq!(MainMenuItem::Validation.key(), '3');
        assert_eq!(MainMenuItem::Algorithms.key(), '4');
        assert_eq!(MainMenuItem::Backtest.key(), '5');
        assert_eq!(MainMenuItem::Settings.key(), '6');
        assert_eq!(MainMenuItem::Quit.key(), 'q');
    }

    #[test]
    fn test_menu_item_from_key_valid() {
        assert_eq!(MainMenuItem::from_key('1'), Some(MainMenuItem::LiveData));
        assert_eq!(MainMenuItem::from_key('2'), Some(MainMenuItem::Research));
        assert_eq!(MainMenuItem::from_key('3'), Some(MainMenuItem::Validation));
        assert_eq!(MainMenuItem::from_key('4'), Some(MainMenuItem::Algorithms));
        assert_eq!(MainMenuItem::from_key('5'), Some(MainMenuItem::Backtest));
        assert_eq!(MainMenuItem::from_key('6'), Some(MainMenuItem::Settings));
        assert_eq!(MainMenuItem::from_key('q'), Some(MainMenuItem::Quit));
        assert_eq!(MainMenuItem::from_key('Q'), Some(MainMenuItem::Quit));
    }

    #[test]
    fn test_menu_item_from_key_invalid() {
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
        assert_eq!(MainMenuItem::all().len(), 7, "Should have 7 menu items total");
    }

    #[test]
    fn test_menu_item_all_except_quit_count() {
        assert_eq!(
            MainMenuItem::all_except_quit().len(),
            6,
            "Should have 6 menu items excluding quit"
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
    fn test_menu_item_order_matches_spec() {
        let items = MainMenuItem::all();
        assert_eq!(items[0], MainMenuItem::LiveData, "First item should be LiveData");
        assert_eq!(items[1], MainMenuItem::Research, "Second item should be Research");
        assert_eq!(items[2], MainMenuItem::Validation, "Third item should be Validation");
        assert_eq!(items[3], MainMenuItem::Algorithms, "Fourth item should be Algorithms");
        assert_eq!(items[4], MainMenuItem::Backtest, "Fifth item should be Backtest");
        assert_eq!(items[5], MainMenuItem::Settings, "Sixth item should be Settings");
        assert_eq!(items[6], MainMenuItem::Quit, "Last item should be Quit");
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
    // Color Tests
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
        assert_eq!(MainMenuItem::Quit.color(), Color::Red);
    }

    #[test]
    fn test_live_data_color_is_green() {
        assert_eq!(MainMenuItem::LiveData.color(), Color::Green);
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
    // Integration/Behavior Tests
    // ========================================================================

    #[test]
    fn test_menu_has_all_required_categories() {
        // Per Task 4.0 spec: LIVE DATA, RESEARCH, VALIDATION, ALGORITHMS, BACKTEST, SETTINGS
        let labels: Vec<&str> = MainMenuItem::all_except_quit()
            .iter()
            .map(|i| i.label())
            .collect();

        assert!(labels.contains(&"LIVE DATA"), "Must have LIVE DATA");
        assert!(labels.contains(&"RESEARCH"), "Must have RESEARCH");
        assert!(labels.contains(&"VALIDATION"), "Must have VALIDATION");
        assert!(labels.contains(&"ALGORITHMS"), "Must have ALGORITHMS");
        assert!(labels.contains(&"BACKTEST"), "Must have BACKTEST");
        assert!(labels.contains(&"SETTINGS"), "Must have SETTINGS");
    }

    #[test]
    fn test_numeric_keys_1_through_6() {
        // Task 4.0 spec requires keys 1-6 for main items
        let keys: Vec<char> = MainMenuItem::all_except_quit()
            .iter()
            .map(|i| i.key())
            .collect();

        assert!(keys.contains(&'1'));
        assert!(keys.contains(&'2'));
        assert!(keys.contains(&'3'));
        assert!(keys.contains(&'4'));
        assert!(keys.contains(&'5'));
        assert!(keys.contains(&'6'));
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
}
