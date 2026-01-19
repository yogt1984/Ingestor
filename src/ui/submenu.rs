//! Submenu framework for TUI navigation (TUI-0.2)
//!
//! Provides a trait-based abstraction for all submenus, enabling consistent
//! navigation and rendering across Research, Algorithms, Validate, Trade, and Data menus.

use crossterm::event::KeyCode;
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::ui::state::GlobalState;

// ============================================================================
// SubMenuAction - Result of handling a key press
// ============================================================================

/// Result of handling a key press in a submenu
#[derive(Debug, Clone, PartialEq)]
pub enum SubMenuAction {
    /// Stay in current submenu, no action taken
    None,
    /// Go back to parent menu
    Back,
    /// Navigate to a TUI mode/screen by name
    /// The string corresponds to AppMode variant names
    Navigate(NavigationTarget),
    /// Execute a CLI command (blocking)
    ExecuteCommand(CliCommand),
    /// Show a message/dialog to the user
    ShowMessage(String),
    /// Refresh/update the current submenu state
    Refresh,
}

impl Default for SubMenuAction {
    fn default() -> Self {
        Self::None
    }
}

// ============================================================================
// NavigationTarget - Where to navigate
// ============================================================================

/// Target for navigation actions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NavigationTarget {
    // Main menu
    MainMenu,

    // Submenus (TUI-1.0 through TUI-5.0)
    ResearchMenu,
    AlgorithmsMenu,
    ValidateMenu,
    TradeMenu,
    DataMenu,

    // Existing operational modes
    Live,
    LiveMM,
    Features,
    Backtest,
    WalkForward,
    DataQuality,
    CampaignSimulation,
    DataInfo,
    GridSearch,
    Sweep,
    OOSValidation,
    Research,
    PresetSelect,
    PaperTradePreset,

    // Algorithm management modes (TUI-2.0)
    AlgorithmSelect,
    AlgorithmCreate,

    // Config screens (T-4.4)
    // Backtest config screens
    BacktestEvaluateConfig,
    BacktestTuneConfig,
    BacktestRegimeSearchConfig,
    BacktestMultiObjectiveConfig,
    BacktestRegimeOptimizeConfig,
    BacktestTrainConfig,
    BacktestWalkForwardMLConfig,
    BacktestSweepConfig,
    BacktestWalkForwardConfig,
    BacktestOOSValidateConfig,
    BacktestSimulateConfig,
    BacktestGridConfig,
    BacktestCampaignConfig,
    BacktestPaperConfig,

    // Research config screens
    ResearchRunConfig,
    ResearchStatusConfig,

    // Validate config screens
    ValidateRunConfig,
    ValidateShowConfig,
    ValidateStatusConfig,

    // Algorithm config screens
    AlgorithmListConfig,
    AlgorithmShowConfig,

    // Results screens (T-4.4)
    // Backtest results screens
    BacktestEvaluateResults,
    BacktestTuneResults,
    BacktestRegimeSearchResults,
    BacktestMultiObjectiveResults,
    BacktestRegimeOptimizeResults,
    BacktestTrainResults,
    BacktestWalkForwardMLResults,
    BacktestSweepResults,
    BacktestWalkForwardResults,
    BacktestOOSValidateResults,
    BacktestSimulateResults,
    BacktestGridResults,
    BacktestCampaignResults,
    BacktestPaperResults,

    // Research results screens
    ResearchRunResults,
    ValidateRunResults,
    AlgorithmCreateResults,
}

impl NavigationTarget {
    /// Get the display name for this target
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::MainMenu => "Main Menu",
            Self::ResearchMenu => "Research",
            Self::AlgorithmsMenu => "Algorithms",
            Self::ValidateMenu => "Validate",
            Self::TradeMenu => "Trade",
            Self::DataMenu => "Data",
            Self::Live => "Live Data",
            Self::LiveMM => "Live Market Making",
            Self::Features => "Features",
            Self::Backtest => "Backtest",
            Self::WalkForward => "Walk-Forward",
            Self::DataQuality => "Data Quality",
            Self::CampaignSimulation => "Campaign Simulation",
            Self::DataInfo => "Data Info",
            Self::GridSearch => "Grid Search",
            Self::Sweep => "Parameter Sweep",
            Self::OOSValidation => "OOS Validation",
            Self::Research => "Research Dashboard",
            Self::PresetSelect => "Preset Selection",
            Self::PaperTradePreset => "Paper Trading",
            Self::AlgorithmSelect => "Algorithm Selection",
            Self::AlgorithmCreate => "Create Algorithm",
            // Config screens
            Self::BacktestEvaluateConfig => "Backtest Evaluate Config",
            Self::BacktestTuneConfig => "Backtest Tune Config",
            Self::BacktestRegimeSearchConfig => "Backtest Regime Search Config",
            Self::BacktestMultiObjectiveConfig => "Backtest Multi-Objective Config",
            Self::BacktestRegimeOptimizeConfig => "Backtest Regime Optimize Config",
            Self::BacktestTrainConfig => "Backtest Train Config",
            Self::BacktestWalkForwardMLConfig => "Backtest Walk-Forward ML Config",
            Self::BacktestSweepConfig => "Backtest Sweep Config",
            Self::BacktestWalkForwardConfig => "Backtest Walk-Forward Config",
            Self::BacktestOOSValidateConfig => "Backtest OOS Validate Config",
            Self::BacktestSimulateConfig => "Backtest Simulate Config",
            Self::BacktestGridConfig => "Backtest Grid Config",
            Self::BacktestCampaignConfig => "Backtest Campaign Config",
            Self::BacktestPaperConfig => "Backtest Paper Config",
            Self::ResearchRunConfig => "Research Run Config",
            Self::ResearchStatusConfig => "Research Status Config",
            Self::ValidateRunConfig => "Validate Run Config",
            Self::ValidateShowConfig => "Validate Show Config",
            Self::ValidateStatusConfig => "Validate Status Config",
            Self::AlgorithmListConfig => "Algorithm List Config",
            Self::AlgorithmShowConfig => "Algorithm Show Config",
            // Results screens
            Self::BacktestEvaluateResults => "Backtest Evaluate Results",
            Self::BacktestTuneResults => "Backtest Tune Results",
            Self::BacktestRegimeSearchResults => "Backtest Regime Search Results",
            Self::BacktestMultiObjectiveResults => "Backtest Multi-Objective Results",
            Self::BacktestRegimeOptimizeResults => "Backtest Regime Optimize Results",
            Self::BacktestTrainResults => "Backtest Train Results",
            Self::BacktestWalkForwardMLResults => "Backtest Walk-Forward ML Results",
            Self::BacktestSweepResults => "Backtest Sweep Results",
            Self::BacktestWalkForwardResults => "Backtest Walk-Forward Results",
            Self::BacktestOOSValidateResults => "Backtest OOS Validate Results",
            Self::BacktestSimulateResults => "Backtest Simulate Results",
            Self::BacktestGridResults => "Backtest Grid Results",
            Self::BacktestCampaignResults => "Backtest Campaign Results",
            Self::BacktestPaperResults => "Backtest Paper Results",
            Self::ResearchRunResults => "Research Run Results",
            Self::ValidateRunResults => "Validate Run Results",
            Self::AlgorithmCreateResults => "Algorithm Create Results",
        }
    }
}

// ============================================================================
// CliCommand - Command to execute
// ============================================================================

/// CLI command to execute (blocking operation)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliCommand {
    /// Binary to run (e.g., "research", "validate", "algorithm", "backtest")
    pub binary: String,
    /// Command-line arguments
    pub args: Vec<String>,
    /// Description for display
    pub description: String,
}

impl CliCommand {
    /// Create a new CLI command
    pub fn new(binary: impl Into<String>, args: Vec<String>, description: impl Into<String>) -> Self {
        Self {
            binary: binary.into(),
            args,
            description: description.into(),
        }
    }

    /// Create a research command
    pub fn research(subcommand: &str) -> Self {
        Self::new(
            "research",
            vec![subcommand.to_string()],
            format!("Running research {}", subcommand),
        )
    }

    /// Create an algorithm command
    pub fn algorithm(subcommand: &str, extra_args: Vec<&str>) -> Self {
        let mut args = vec![subcommand.to_string()];
        args.extend(extra_args.into_iter().map(String::from));
        Self::new(
            "algorithm",
            args,
            format!("Running algorithm {}", subcommand),
        )
    }

    /// Create a validate command
    pub fn validate(subcommand: &str, extra_args: Vec<&str>) -> Self {
        let mut args = vec![subcommand.to_string()];
        args.extend(extra_args.into_iter().map(String::from));
        Self::new(
            "validate",
            args,
            format!("Running validation {}", subcommand),
        )
    }

    /// Create a backtest command
    pub fn backtest(subcommand: &str, extra_args: Vec<&str>) -> Self {
        let mut args = vec![subcommand.to_string()];
        args.extend(extra_args.into_iter().map(String::from));
        Self::new(
            "backtest",
            args,
            format!("Running backtest {}", subcommand),
        )
    }

    /// Get the full command string for display
    pub fn command_string(&self) -> String {
        if self.args.is_empty() {
            format!("cargo run --bin {}", self.binary)
        } else {
            format!("cargo run --bin {} -- {}", self.binary, self.args.join(" "))
        }
    }
}

// ============================================================================
// SubMenuItem - Menu item for display
// ============================================================================

/// A single menu item for rendering
#[derive(Debug, Clone)]
pub struct SubMenuItem {
    /// Key to press (e.g., 'R', '1', 'A')
    pub key: char,
    /// Label shown in menu
    pub label: String,
    /// Description/help text
    pub description: String,
    /// Whether the item is enabled (grayed out if false)
    pub enabled: bool,
    /// Optional status indicator (e.g., "✓", "✗", "○")
    pub status: Option<String>,
}

impl SubMenuItem {
    /// Create a new enabled menu item
    pub fn new(key: char, label: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            key,
            label: label.into(),
            description: description.into(),
            enabled: true,
            status: None,
        }
    }

    /// Create a new disabled menu item
    pub fn disabled(key: char, label: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            key,
            label: label.into(),
            description: description.into(),
            enabled: false,
            status: None,
        }
    }

    /// Add a status indicator
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = Some(status.into());
        self
    }

    /// Set enabled state
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Check if this item matches a key press
    pub fn matches_key(&self, key: char) -> bool {
        self.enabled && self.key.to_ascii_lowercase() == key.to_ascii_lowercase()
    }
}

// ============================================================================
// SubMenu Trait
// ============================================================================

/// Trait for all submenus
///
/// Implementing this trait enables consistent navigation and rendering
/// across all submenu screens (Research, Algorithms, Validate, Trade, Data).
pub trait SubMenu {
    /// Get the menu title
    fn title(&self) -> &str;

    /// Get menu items for display
    fn items(&self, state: &GlobalState) -> Vec<SubMenuItem>;

    /// Handle key press, return action
    fn handle_key(&mut self, key: KeyCode, state: &GlobalState) -> SubMenuAction;

    /// Optional: Get footer text (e.g., status information)
    fn footer(&self, _state: &GlobalState) -> Option<String> {
        None
    }

    /// Optional: Check if the submenu can be entered
    fn can_enter(&self, _state: &GlobalState) -> bool {
        true
    }

    /// Optional: Get a message explaining why entry is blocked
    fn blocked_message(&self, _state: &GlobalState) -> Option<String> {
        None
    }
}

// ============================================================================
// Helper Functions for Drawing
// ============================================================================

/// Draw a submenu frame with consistent styling
///
/// This provides a standard layout for all submenus:
/// - Title bar at top
/// - Menu items in center
/// - Footer at bottom (optional)
/// - ESC hint for back navigation
pub fn draw_submenu_frame<S: SubMenu>(
    f: &mut Frame,
    area: Rect,
    submenu: &S,
    state: &GlobalState,
) {
    // Create layout: title, content, footer
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Title
            Constraint::Min(5),     // Content
            Constraint::Length(3),  // Footer
        ])
        .split(area);

    // Draw title
    draw_submenu_title(f, chunks[0], submenu.title());

    // Draw menu items
    draw_submenu_items(f, chunks[1], &submenu.items(state));

    // Draw footer
    draw_submenu_footer(f, chunks[2], submenu.footer(state));
}

/// Draw the title bar
pub fn draw_submenu_title(f: &mut Frame, area: Rect, title: &str) {
    let title_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title_alignment(Alignment::Center);

    let title_text = Paragraph::new(title)
        .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD))
        .alignment(Alignment::Center)
        .block(title_block);

    f.render_widget(title_text, area);
}

/// Draw the menu items
pub fn draw_submenu_items(f: &mut Frame, area: Rect, items: &[SubMenuItem]) {
    let block = Block::default()
        .borders(Borders::LEFT | Borders::RIGHT)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Build lines for each item
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from("")); // Top padding

    for item in items {
        let key_style = if item.enabled {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let label_style = if item.enabled {
            Style::default().fg(Color::White)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let desc_style = if item.enabled {
            Style::default().fg(Color::Gray)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let mut spans = vec![
            Span::raw("   ["),
            Span::styled(item.key.to_string(), key_style),
            Span::raw("] "),
            Span::styled(format!("{:<16}", item.label), label_style),
            Span::styled(&item.description, desc_style),
        ];

        // Add status if present
        if let Some(ref status) = item.status {
            let status_style = if status.contains('✓') {
                Style::default().fg(Color::Green)
            } else if status.contains('✗') {
                Style::default().fg(Color::Red)
            } else {
                Style::default().fg(Color::Yellow)
            };
            spans.push(Span::raw("  "));
            spans.push(Span::styled(status, status_style));
        }

        lines.push(Line::from(spans));
    }

    let items_paragraph = Paragraph::new(lines);
    f.render_widget(items_paragraph, inner);
}

/// Draw the footer with ESC hint and optional status
pub fn draw_submenu_footer(f: &mut Frame, area: Rect, footer_text: Option<String>) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block.clone(), area);

    let footer_content = if let Some(text) = footer_text {
        format!("[ESC] Back    {}", text)
    } else {
        "[ESC] Back".to_string()
    };

    let footer = Paragraph::new(footer_content)
        .style(Style::default().fg(Color::DarkGray))
        .alignment(Alignment::Left);

    f.render_widget(footer, inner);
}

/// Draw a message dialog (for ShowMessage action)
pub fn draw_message_dialog(f: &mut Frame, area: Rect, message: &str) {
    // Center the dialog
    let dialog_width = 60.min(area.width.saturating_sub(4));
    let dialog_height = 7.min(area.height.saturating_sub(4));

    let dialog_area = Rect {
        x: area.x + (area.width - dialog_width) / 2,
        y: area.y + (area.height - dialog_height) / 2,
        width: dialog_width,
        height: dialog_height,
    };

    let block = Block::default()
        .title(" Message ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .style(Style::default().bg(Color::Black));

    let inner = block.inner(dialog_area);
    f.render_widget(block, dialog_area);

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(message, Style::default().fg(Color::White))),
        Line::from(""),
        Line::from(Span::styled(
            "Press any key to continue...",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let paragraph = Paragraph::new(lines).alignment(Alignment::Center);
    f.render_widget(paragraph, inner);
}

/// Get a key character from KeyCode, if applicable
pub fn key_to_char(key: KeyCode) -> Option<char> {
    match key {
        KeyCode::Char(c) => Some(c),
        _ => None,
    }
}

/// Check if a key is the back/escape key
pub fn is_back_key(key: KeyCode) -> bool {
    matches!(key, KeyCode::Esc | KeyCode::Backspace)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // SubMenuAction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_submenu_action_default() {
        let action: SubMenuAction = Default::default();
        assert_eq!(action, SubMenuAction::None);
    }

    #[test]
    fn test_submenu_action_none() {
        let action = SubMenuAction::None;
        assert_eq!(action, SubMenuAction::None);
    }

    #[test]
    fn test_submenu_action_back() {
        let action = SubMenuAction::Back;
        assert_eq!(action, SubMenuAction::Back);
    }

    #[test]
    fn test_submenu_action_navigate() {
        let action = SubMenuAction::Navigate(NavigationTarget::ResearchMenu);
        if let SubMenuAction::Navigate(target) = action {
            assert_eq!(target, NavigationTarget::ResearchMenu);
        } else {
            panic!("Expected Navigate action");
        }
    }

    #[test]
    fn test_submenu_action_show_message() {
        let action = SubMenuAction::ShowMessage("Test message".to_string());
        if let SubMenuAction::ShowMessage(msg) = action {
            assert_eq!(msg, "Test message");
        } else {
            panic!("Expected ShowMessage action");
        }
    }

    #[test]
    fn test_submenu_action_refresh() {
        let action = SubMenuAction::Refresh;
        assert_eq!(action, SubMenuAction::Refresh);
    }

    #[test]
    fn test_submenu_action_clone() {
        let action = SubMenuAction::ShowMessage("test".to_string());
        let cloned = action.clone();
        assert_eq!(action, cloned);
    }

    // -------------------------------------------------------------------------
    // NavigationTarget tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_navigation_target_display_name_main_menu() {
        assert_eq!(NavigationTarget::MainMenu.display_name(), "Main Menu");
    }

    #[test]
    fn test_navigation_target_display_name_submenus() {
        assert_eq!(NavigationTarget::ResearchMenu.display_name(), "Research");
        assert_eq!(NavigationTarget::AlgorithmsMenu.display_name(), "Algorithms");
        assert_eq!(NavigationTarget::ValidateMenu.display_name(), "Validate");
        assert_eq!(NavigationTarget::TradeMenu.display_name(), "Trade");
        assert_eq!(NavigationTarget::DataMenu.display_name(), "Data");
    }

    #[test]
    fn test_navigation_target_display_name_operational() {
        assert_eq!(NavigationTarget::Live.display_name(), "Live Data");
        assert_eq!(NavigationTarget::Backtest.display_name(), "Backtest");
        assert_eq!(NavigationTarget::GridSearch.display_name(), "Grid Search");
    }

    #[test]
    fn test_navigation_target_equality() {
        assert_eq!(NavigationTarget::Live, NavigationTarget::Live);
        assert_ne!(NavigationTarget::Live, NavigationTarget::Backtest);
    }

    #[test]
    fn test_navigation_target_clone() {
        let target = NavigationTarget::ValidateMenu;
        let cloned = target.clone();
        assert_eq!(target, cloned);
    }

    // -------------------------------------------------------------------------
    // CliCommand tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_cli_command_new() {
        let cmd = CliCommand::new("test", vec!["arg1".to_string()], "description");
        assert_eq!(cmd.binary, "test");
        assert_eq!(cmd.args, vec!["arg1"]);
        assert_eq!(cmd.description, "description");
    }

    #[test]
    fn test_cli_command_research() {
        let cmd = CliCommand::research("run");
        assert_eq!(cmd.binary, "research");
        assert_eq!(cmd.args, vec!["run"]);
        assert!(cmd.description.contains("research"));
    }

    #[test]
    fn test_cli_command_algorithm() {
        let cmd = CliCommand::algorithm("list", vec!["--strategy", "momentum"]);
        assert_eq!(cmd.binary, "algorithm");
        assert_eq!(cmd.args, vec!["list", "--strategy", "momentum"]);
    }

    #[test]
    fn test_cli_command_validate() {
        let cmd = CliCommand::validate("run", vec!["--stages", "backtest"]);
        assert_eq!(cmd.binary, "validate");
        assert_eq!(cmd.args, vec!["run", "--stages", "backtest"]);
    }

    #[test]
    fn test_cli_command_backtest() {
        let cmd = CliCommand::backtest("info", vec![]);
        assert_eq!(cmd.binary, "backtest");
        assert_eq!(cmd.args, vec!["info"]);
    }

    #[test]
    fn test_cli_command_string_no_args() {
        let cmd = CliCommand::new("test", vec![], "desc");
        assert_eq!(cmd.command_string(), "cargo run --bin test");
    }

    #[test]
    fn test_cli_command_string_with_args() {
        let cmd = CliCommand::new("test", vec!["arg1".to_string(), "arg2".to_string()], "desc");
        assert_eq!(cmd.command_string(), "cargo run --bin test -- arg1 arg2");
    }

    #[test]
    fn test_cli_command_equality() {
        let cmd1 = CliCommand::new("test", vec!["arg".to_string()], "desc");
        let cmd2 = CliCommand::new("test", vec!["arg".to_string()], "desc");
        assert_eq!(cmd1, cmd2);
    }

    #[test]
    fn test_cli_command_clone() {
        let cmd = CliCommand::research("status");
        let cloned = cmd.clone();
        assert_eq!(cmd, cloned);
    }

    // -------------------------------------------------------------------------
    // SubMenuItem tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_submenu_item_new() {
        let item = SubMenuItem::new('R', "Run", "Run the research");
        assert_eq!(item.key, 'R');
        assert_eq!(item.label, "Run");
        assert_eq!(item.description, "Run the research");
        assert!(item.enabled);
        assert!(item.status.is_none());
    }

    #[test]
    fn test_submenu_item_disabled() {
        let item = SubMenuItem::disabled('L', "Live", "Live trading");
        assert_eq!(item.key, 'L');
        assert!(!item.enabled);
    }

    #[test]
    fn test_submenu_item_with_status() {
        let item = SubMenuItem::new('1', "Backtest", "Run backtest")
            .with_status("✓ 1.2");
        assert_eq!(item.status, Some("✓ 1.2".to_string()));
    }

    #[test]
    fn test_submenu_item_with_enabled() {
        let item = SubMenuItem::new('X', "Test", "Test item")
            .with_enabled(false);
        assert!(!item.enabled);
    }

    #[test]
    fn test_submenu_item_matches_key_enabled() {
        let item = SubMenuItem::new('R', "Run", "Run");
        assert!(item.matches_key('R'));
        assert!(item.matches_key('r')); // Case insensitive
    }

    #[test]
    fn test_submenu_item_matches_key_disabled() {
        let item = SubMenuItem::disabled('R', "Run", "Run");
        assert!(!item.matches_key('R'));
        assert!(!item.matches_key('r'));
    }

    #[test]
    fn test_submenu_item_matches_key_wrong_key() {
        let item = SubMenuItem::new('R', "Run", "Run");
        assert!(!item.matches_key('X'));
    }

    #[test]
    fn test_submenu_item_clone() {
        let item = SubMenuItem::new('T', "Test", "Test")
            .with_status("✓");
        let cloned = item.clone();
        assert_eq!(item.key, cloned.key);
        assert_eq!(item.label, cloned.label);
        assert_eq!(item.status, cloned.status);
    }

    // -------------------------------------------------------------------------
    // Helper function tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_key_to_char_letter() {
        assert_eq!(key_to_char(KeyCode::Char('a')), Some('a'));
        assert_eq!(key_to_char(KeyCode::Char('Z')), Some('Z'));
    }

    #[test]
    fn test_key_to_char_number() {
        assert_eq!(key_to_char(KeyCode::Char('1')), Some('1'));
        assert_eq!(key_to_char(KeyCode::Char('9')), Some('9'));
    }

    #[test]
    fn test_key_to_char_non_char() {
        assert_eq!(key_to_char(KeyCode::Esc), None);
        assert_eq!(key_to_char(KeyCode::Enter), None);
        assert_eq!(key_to_char(KeyCode::Backspace), None);
    }

    #[test]
    fn test_is_back_key_esc() {
        assert!(is_back_key(KeyCode::Esc));
    }

    #[test]
    fn test_is_back_key_backspace() {
        assert!(is_back_key(KeyCode::Backspace));
    }

    #[test]
    fn test_is_back_key_other() {
        assert!(!is_back_key(KeyCode::Enter));
        assert!(!is_back_key(KeyCode::Char('q')));
        assert!(!is_back_key(KeyCode::Tab));
    }

    // -------------------------------------------------------------------------
    // SubMenuAction with CliCommand tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_submenu_action_execute_command() {
        let cmd = CliCommand::research("run");
        let action = SubMenuAction::ExecuteCommand(cmd.clone());
        if let SubMenuAction::ExecuteCommand(c) = action {
            assert_eq!(c, cmd);
        } else {
            panic!("Expected ExecuteCommand action");
        }
    }

    // -------------------------------------------------------------------------
    // Integration-style tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_menu_item_workflow() {
        // Create items like a real menu would
        let items = vec![
            SubMenuItem::new('R', "Run Research", "Analyze historical data")
                .with_status("○"),
            SubMenuItem::new('S', "Status", "View research state"),
            SubMenuItem::disabled('C', "Create Config", "Generate algorithm config")
                .with_status("requires research"),
        ];

        assert_eq!(items.len(), 3);
        assert!(items[0].matches_key('r'));
        assert!(items[1].matches_key('S'));
        assert!(!items[2].matches_key('C')); // Disabled
    }

    #[test]
    fn test_cli_command_workflow() {
        // Simulate building commands from menu selections
        let commands = vec![
            CliCommand::research("run"),
            CliCommand::validate("run", vec!["--stages", "backtest"]),
            CliCommand::backtest("info", vec![]),
        ];

        assert_eq!(commands[0].command_string(), "cargo run --bin research -- run");
        assert_eq!(commands[1].command_string(), "cargo run --bin validate -- run --stages backtest");
        assert_eq!(commands[2].command_string(), "cargo run --bin backtest -- info");
    }

    #[test]
    fn test_navigation_workflow() {
        // Simulate navigation flow
        let flow = vec![
            NavigationTarget::MainMenu,
            NavigationTarget::ResearchMenu,
            NavigationTarget::Research,
            NavigationTarget::ResearchMenu,
            NavigationTarget::MainMenu,
        ];

        for (i, target) in flow.iter().enumerate() {
            // Each target should have a display name
            assert!(!target.display_name().is_empty(), "Target {} has empty name", i);
        }
    }

    // -------------------------------------------------------------------------
    // Edge case tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_submenu_item_empty_strings() {
        let item = SubMenuItem::new('X', "", "");
        assert_eq!(item.label, "");
        assert_eq!(item.description, "");
        assert!(item.matches_key('x'));
    }

    #[test]
    fn test_cli_command_empty_args() {
        let cmd = CliCommand::new("test", vec![], "");
        assert!(cmd.args.is_empty());
        assert_eq!(cmd.command_string(), "cargo run --bin test");
    }

    #[test]
    fn test_submenu_item_special_chars_in_status() {
        let item = SubMenuItem::new('1', "Test", "Test")
            .with_status("✓ ✗ ○ ◐");
        assert!(item.status.is_some());
    }

    #[test]
    fn test_action_message_with_special_chars() {
        let action = SubMenuAction::ShowMessage("Error: ✗ Something went wrong!".to_string());
        if let SubMenuAction::ShowMessage(msg) = action {
            assert!(msg.contains("✗"));
        }
    }
}
