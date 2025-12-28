//! Status Bar Widget (Task TUI-6.0)
//!
//! Persistent status bar displayed at bottom of all screens.
//! Shows: Symbol | Algorithm | Validation Status | Trading Mode
//!
//! Display Format:
//! ```text
//! BTCUSDT | momentum_v3 (MOM) | Val: 4/5 | Paper (+$123.45)
//! ```
//!
//! Color Coding:
//! - Symbol: White
//! - Algorithm: Cyan (or Yellow if none)
//! - Validation: Green if all pass, Yellow if partial, Red if any fail
//! - Trading Mode: Green for Idle, Blue for Paper, Red for Live

use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::core::algorithm_config::StrategyType;
use crate::ui::state::{GlobalState, StageStatus, TradingMode, ResearchStatus};

// ============================================================================
// StatusBar Widget
// ============================================================================

/// Persistent status bar widget displayed at bottom of all screens
#[derive(Debug, Clone, Default)]
pub struct StatusBar;

impl StatusBar {
    /// Create a new StatusBar widget
    pub fn new() -> Self {
        Self
    }

    /// Draw the status bar in the given area
    ///
    /// Format: Symbol | Algorithm | Validation | Trading Mode
    pub fn draw(f: &mut Frame, area: Rect, state: &GlobalState) {
        let spans = Self::build_spans(state, area.width as usize);
        let line = Line::from(spans);
        let paragraph = Paragraph::new(line);
        f.render_widget(paragraph, area);
    }

    /// Build the spans for the status bar
    ///
    /// Constructs colored spans for each section of the status bar.
    fn build_spans(state: &GlobalState, max_width: usize) -> Vec<Span<'static>> {
        let mut spans = Vec::new();
        let mut current_len = 0;

        // Section 1: Symbol (White)
        let symbol_text = state.symbol.clone();
        let symbol_len = symbol_text.len();
        if current_len + symbol_len <= max_width {
            spans.push(Span::styled(symbol_text, Style::default().fg(Color::White)));
            current_len += symbol_len;
        }

        // Separator
        let sep = " | ";
        if current_len + sep.len() <= max_width {
            spans.push(Span::raw(sep.to_string()));
            current_len += sep.len();
        }

        // Section 2: Algorithm (Cyan or Yellow if none)
        let algo_text = Self::format_algorithm(state);
        let algo_color = Self::algorithm_color(state);
        let algo_len = algo_text.len();
        if current_len + algo_len <= max_width {
            spans.push(Span::styled(algo_text, Style::default().fg(algo_color)));
            current_len += algo_len;
        }

        // Separator
        if current_len + sep.len() <= max_width {
            spans.push(Span::raw(sep.to_string()));
            current_len += sep.len();
        }

        // Section 3: Validation Status
        let val_text = Self::format_validation(state);
        let val_color = Self::validation_color(state);
        let val_len = val_text.len();
        if current_len + val_len <= max_width {
            spans.push(Span::styled(val_text, Style::default().fg(val_color)));
            current_len += val_len;
        }

        // Separator
        if current_len + sep.len() <= max_width {
            spans.push(Span::raw(sep.to_string()));
            current_len += sep.len();
        }

        // Section 4: Trading Mode
        let mode_text = Self::format_trading_mode(state);
        let mode_color = Self::trading_mode_color(state);
        let mode_len = mode_text.len();
        if current_len + mode_len <= max_width {
            spans.push(Span::styled(mode_text, Style::default().fg(mode_color)));
        }

        spans
    }

    /// Format the algorithm section
    ///
    /// Returns: "algo_name (TYPE)" or "[None]" if no algorithm selected
    pub fn format_algorithm(state: &GlobalState) -> String {
        match &state.active_algorithm {
            Some(algo) => {
                let type_abbrev = Self::strategy_type_abbrev(&algo.strategy_type);
                format!("{} ({})", algo.name, type_abbrev)
            }
            None => "[None]".to_string(),
        }
    }

    /// Get the strategy type abbreviation
    pub fn strategy_type_abbrev(strategy_type: &StrategyType) -> &'static str {
        match strategy_type {
            StrategyType::Momentum => "MOM",
            StrategyType::MarketMaking => "MM",
            StrategyType::Hybrid => "HYB",
        }
    }

    /// Get the color for the algorithm section
    pub fn algorithm_color(state: &GlobalState) -> Color {
        match &state.active_algorithm {
            Some(_) => Color::Cyan,
            None => Color::Yellow,
        }
    }

    /// Format the validation section
    ///
    /// Returns: "Val: X/5" with appropriate indicator
    pub fn format_validation(state: &GlobalState) -> String {
        let passed = state.passed_stages();
        let total = GlobalState::total_stages();
        let indicator = Self::validation_indicator(state);
        format!("Val: {}/{}{}", passed, total, indicator)
    }

    /// Get the validation status indicator
    ///
    /// Returns appropriate symbol based on validation state
    pub fn validation_indicator(state: &GlobalState) -> &'static str {
        let vs = &state.validation_status;

        // Check for any failures
        if vs.backtest.is_failed()
            || vs.forward.is_failed()
            || vs.oos.is_failed()
            || vs.paper.is_failed()
            || vs.live.is_failed()
        {
            return " X";
        }

        // Check for any running
        if vs.backtest.is_running()
            || vs.forward.is_running()
            || vs.oos.is_running()
            || vs.paper.is_running()
            || vs.live.is_running()
        {
            return " ...";
        }

        // Check if all passed
        let passed = state.passed_stages();
        if passed == GlobalState::total_stages() {
            return " OK";
        }

        // Partial progress
        ""
    }

    /// Get the color for the validation section
    ///
    /// - Green: All stages passed
    /// - Yellow: Some stages passed/not run
    /// - Red: Any stage failed
    pub fn validation_color(state: &GlobalState) -> Color {
        let vs = &state.validation_status;

        // Any failures -> Red
        if vs.backtest.is_failed()
            || vs.forward.is_failed()
            || vs.oos.is_failed()
            || vs.paper.is_failed()
            || vs.live.is_failed()
        {
            return Color::Red;
        }

        // All passed -> Green
        let passed = state.passed_stages();
        if passed == GlobalState::total_stages() {
            return Color::Green;
        }

        // Partial -> Yellow
        Color::Yellow
    }

    /// Format the trading mode section
    ///
    /// Returns: "Idle", "Paper (+$X.XX)", or "Live (+$X.XX)"
    pub fn format_trading_mode(state: &GlobalState) -> String {
        match &state.trading_mode {
            TradingMode::Idle => "Idle".to_string(),
            TradingMode::Paper { pnl, .. } => {
                let sign = if *pnl >= 0.0 { "+" } else { "-" };
                format!("Paper ({}${:.2})", sign, pnl.abs())
            }
            TradingMode::Live { pnl, .. } => {
                let sign = if *pnl >= 0.0 { "+" } else { "-" };
                format!("LIVE ({}${:.2})", sign, pnl.abs())
            }
        }
    }

    /// Get the color for the trading mode section
    ///
    /// - Green: Idle
    /// - Blue: Paper trading
    /// - Red: Live trading (danger!)
    pub fn trading_mode_color(state: &GlobalState) -> Color {
        match &state.trading_mode {
            TradingMode::Idle => Color::Green,
            TradingMode::Paper { .. } => Color::Blue,
            TradingMode::Live { .. } => Color::Red,
        }
    }

    /// Format a compact status string (for narrow terminals)
    ///
    /// Returns: "SYM|ALG|X/5|MODE"
    pub fn format_compact(state: &GlobalState) -> String {
        let algo = match &state.active_algorithm {
            Some(a) => Self::strategy_type_abbrev(&a.strategy_type),
            None => "---",
        };
        let mode = match &state.trading_mode {
            TradingMode::Idle => "IDL",
            TradingMode::Paper { .. } => "PPR",
            TradingMode::Live { .. } => "LIV",
        };
        format!(
            "{}|{}|{}/{}|{}",
            state.symbol,
            algo,
            state.passed_stages(),
            GlobalState::total_stages(),
            mode
        )
    }

    /// Get the full status text (for testing)
    pub fn format_full(state: &GlobalState) -> String {
        format!(
            "{} | {} | {} | {}",
            state.symbol,
            Self::format_algorithm(state),
            Self::format_validation(state),
            Self::format_trading_mode(state)
        )
    }

    /// Calculate the minimum width needed for the status bar
    pub fn min_width(state: &GlobalState) -> usize {
        Self::format_compact(state).len()
    }

    /// Calculate the full width needed for the status bar
    pub fn full_width(state: &GlobalState) -> usize {
        Self::format_full(state).len()
    }

    /// Get the research status string (auxiliary display)
    pub fn format_research(state: &GlobalState) -> String {
        match &state.research_status {
            ResearchStatus::Idle => "Research: Idle".to_string(),
            ResearchStatus::Running { samples_processed } => {
                format!("Research: Running ({} samples)", samples_processed)
            }
            ResearchStatus::Complete { tradeable } => {
                if *tradeable {
                    "Research: Complete (tradeable)".to_string()
                } else {
                    "Research: Complete (not tradeable)".to_string()
                }
            }
        }
    }

    /// Get the validation stages as a visual string
    ///
    /// Returns something like: "BT:OK FW:OK OOS:X PP:- LV:-"
    pub fn format_stages_visual(state: &GlobalState) -> String {
        let stages = [
            ("BT", &state.validation_status.backtest),
            ("FW", &state.validation_status.forward),
            ("OOS", &state.validation_status.oos),
            ("PP", &state.validation_status.paper),
            ("LV", &state.validation_status.live),
        ];

        stages
            .iter()
            .map(|(name, status)| {
                let indicator = match status {
                    StageStatus::NotRun => "-",
                    StageStatus::Passed { .. } => "OK",
                    StageStatus::Failed { .. } => "X",
                    StageStatus::Running { .. } => "...",
                };
                format!("{}:{}", name, indicator)
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

// ============================================================================
// Draw Helper Function (for external use)
// ============================================================================

/// Draw the status bar (convenience function)
pub fn draw_status_bar(f: &mut Frame, area: Rect, state: &GlobalState) {
    StatusBar::draw(f, area, state);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::ui::state::{AlgorithmConfigSummary, ValidationStatus, DataStats};

    // ========================================================================
    // Helper Functions
    // ========================================================================

    fn create_default_state() -> GlobalState {
        GlobalState::default()
    }

    fn create_state_with_algorithm() -> GlobalState {
        let mut state = GlobalState::default();
        state.active_algorithm = Some(AlgorithmConfigSummary {
            id: "mom_v1".to_string(),
            name: "Momentum v1".to_string(),
            strategy_type: StrategyType::Momentum,
            created_at: Utc::now(),
        });
        state
    }

    fn create_state_with_validation() -> GlobalState {
        let mut state = create_state_with_algorithm();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::passed(1.2);
        state.validation_status.oos = StageStatus::passed(0.8);
        state
    }

    fn create_state_all_passed() -> GlobalState {
        let mut state = create_state_with_algorithm();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::passed(1.2);
        state.validation_status.oos = StageStatus::passed(0.8);
        state.validation_status.paper = StageStatus::passed(0.9);
        state.validation_status.live = StageStatus::passed(0.7);
        state
    }

    fn create_state_with_failure() -> GlobalState {
        let mut state = create_state_with_algorithm();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::failed("Low Sharpe");
        state
    }

    fn create_state_paper_trading() -> GlobalState {
        let mut state = create_state_with_validation();
        state.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: 123.45,
        };
        state
    }

    fn create_state_live_trading() -> GlobalState {
        let mut state = create_state_all_passed();
        state.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: -50.00,
        };
        state
    }

    // ========================================================================
    // Construction Tests
    // ========================================================================

    #[test]
    fn test_status_bar_new() {
        let _bar = StatusBar::new();
    }

    #[test]
    fn test_status_bar_default() {
        let _bar = StatusBar::default();
    }

    #[test]
    fn test_status_bar_clone() {
        let bar = StatusBar::new();
        let _cloned = bar.clone();
    }

    #[test]
    fn test_status_bar_debug() {
        let bar = StatusBar::new();
        let debug = format!("{:?}", bar);
        assert!(debug.contains("StatusBar"));
    }

    // ========================================================================
    // Algorithm Formatting Tests
    // ========================================================================

    #[test]
    fn test_format_algorithm_none() {
        let state = create_default_state();
        let result = StatusBar::format_algorithm(&state);
        assert_eq!(result, "[None]");
    }

    #[test]
    fn test_format_algorithm_momentum() {
        let state = create_state_with_algorithm();
        let result = StatusBar::format_algorithm(&state);
        assert_eq!(result, "Momentum v1 (MOM)");
    }

    #[test]
    fn test_format_algorithm_market_making() {
        let mut state = create_default_state();
        state.active_algorithm = Some(AlgorithmConfigSummary {
            id: "mm_v1".to_string(),
            name: "MM Strategy".to_string(),
            strategy_type: StrategyType::MarketMaking,
            created_at: Utc::now(),
        });
        let result = StatusBar::format_algorithm(&state);
        assert_eq!(result, "MM Strategy (MM)");
    }

    #[test]
    fn test_format_algorithm_hybrid() {
        let mut state = create_default_state();
        state.active_algorithm = Some(AlgorithmConfigSummary {
            id: "hyb_v1".to_string(),
            name: "Hybrid Algo".to_string(),
            strategy_type: StrategyType::Hybrid,
            created_at: Utc::now(),
        });
        let result = StatusBar::format_algorithm(&state);
        assert_eq!(result, "Hybrid Algo (HYB)");
    }

    #[test]
    fn test_strategy_type_abbrev_momentum() {
        assert_eq!(StatusBar::strategy_type_abbrev(&StrategyType::Momentum), "MOM");
    }

    #[test]
    fn test_strategy_type_abbrev_market_making() {
        assert_eq!(StatusBar::strategy_type_abbrev(&StrategyType::MarketMaking), "MM");
    }

    #[test]
    fn test_strategy_type_abbrev_hybrid() {
        assert_eq!(StatusBar::strategy_type_abbrev(&StrategyType::Hybrid), "HYB");
    }

    // ========================================================================
    // Algorithm Color Tests
    // ========================================================================

    #[test]
    fn test_algorithm_color_none() {
        let state = create_default_state();
        assert_eq!(StatusBar::algorithm_color(&state), Color::Yellow);
    }

    #[test]
    fn test_algorithm_color_with_algorithm() {
        let state = create_state_with_algorithm();
        assert_eq!(StatusBar::algorithm_color(&state), Color::Cyan);
    }

    // ========================================================================
    // Validation Formatting Tests
    // ========================================================================

    #[test]
    fn test_format_validation_none_passed() {
        let state = create_default_state();
        let result = StatusBar::format_validation(&state);
        assert_eq!(result, "Val: 0/5");
    }

    #[test]
    fn test_format_validation_some_passed() {
        let state = create_state_with_validation();
        let result = StatusBar::format_validation(&state);
        assert_eq!(result, "Val: 3/5");
    }

    #[test]
    fn test_format_validation_all_passed() {
        let state = create_state_all_passed();
        let result = StatusBar::format_validation(&state);
        assert_eq!(result, "Val: 5/5 OK");
    }

    #[test]
    fn test_format_validation_with_failure() {
        let state = create_state_with_failure();
        let result = StatusBar::format_validation(&state);
        assert!(result.contains("X"));
    }

    #[test]
    fn test_format_validation_running() {
        let mut state = create_default_state();
        state.validation_status.backtest = StageStatus::running(0.5);
        let result = StatusBar::format_validation(&state);
        assert!(result.contains("..."));
    }

    // ========================================================================
    // Validation Indicator Tests
    // ========================================================================

    #[test]
    fn test_validation_indicator_empty() {
        let state = create_default_state();
        assert_eq!(StatusBar::validation_indicator(&state), "");
    }

    #[test]
    fn test_validation_indicator_all_passed() {
        let state = create_state_all_passed();
        assert_eq!(StatusBar::validation_indicator(&state), " OK");
    }

    #[test]
    fn test_validation_indicator_failed() {
        let state = create_state_with_failure();
        assert_eq!(StatusBar::validation_indicator(&state), " X");
    }

    #[test]
    fn test_validation_indicator_running() {
        let mut state = create_default_state();
        state.validation_status.forward = StageStatus::running(0.3);
        assert_eq!(StatusBar::validation_indicator(&state), " ...");
    }

    // ========================================================================
    // Validation Color Tests
    // ========================================================================

    #[test]
    fn test_validation_color_partial() {
        let state = create_default_state();
        assert_eq!(StatusBar::validation_color(&state), Color::Yellow);
    }

    #[test]
    fn test_validation_color_all_passed() {
        let state = create_state_all_passed();
        assert_eq!(StatusBar::validation_color(&state), Color::Green);
    }

    #[test]
    fn test_validation_color_failed() {
        let state = create_state_with_failure();
        assert_eq!(StatusBar::validation_color(&state), Color::Red);
    }

    // ========================================================================
    // Trading Mode Formatting Tests
    // ========================================================================

    #[test]
    fn test_format_trading_mode_idle() {
        let state = create_default_state();
        let result = StatusBar::format_trading_mode(&state);
        assert_eq!(result, "Idle");
    }

    #[test]
    fn test_format_trading_mode_paper_positive() {
        let state = create_state_paper_trading();
        let result = StatusBar::format_trading_mode(&state);
        assert_eq!(result, "Paper (+$123.45)");
    }

    #[test]
    fn test_format_trading_mode_paper_negative() {
        let mut state = create_state_with_validation();
        state.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: -50.25,
        };
        let result = StatusBar::format_trading_mode(&state);
        assert_eq!(result, "Paper (-$50.25)");
    }

    #[test]
    fn test_format_trading_mode_live_positive() {
        let mut state = create_state_all_passed();
        state.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: 500.00,
        };
        let result = StatusBar::format_trading_mode(&state);
        assert_eq!(result, "LIVE (+$500.00)");
    }

    #[test]
    fn test_format_trading_mode_live_negative() {
        let state = create_state_live_trading();
        let result = StatusBar::format_trading_mode(&state);
        assert_eq!(result, "LIVE (-$50.00)");
    }

    #[test]
    fn test_format_trading_mode_zero_pnl() {
        let mut state = create_state_with_validation();
        state.trading_mode = TradingMode::Paper {
            started: Utc::now(),
            pnl: 0.0,
        };
        let result = StatusBar::format_trading_mode(&state);
        assert_eq!(result, "Paper (+$0.00)");
    }

    // ========================================================================
    // Trading Mode Color Tests
    // ========================================================================

    #[test]
    fn test_trading_mode_color_idle() {
        let state = create_default_state();
        assert_eq!(StatusBar::trading_mode_color(&state), Color::Green);
    }

    #[test]
    fn test_trading_mode_color_paper() {
        let state = create_state_paper_trading();
        assert_eq!(StatusBar::trading_mode_color(&state), Color::Blue);
    }

    #[test]
    fn test_trading_mode_color_live() {
        let state = create_state_live_trading();
        assert_eq!(StatusBar::trading_mode_color(&state), Color::Red);
    }

    // ========================================================================
    // Compact Format Tests
    // ========================================================================

    #[test]
    fn test_format_compact_default() {
        let state = create_default_state();
        let result = StatusBar::format_compact(&state);
        assert_eq!(result, "BTCUSDT|---|0/5|IDL");
    }

    #[test]
    fn test_format_compact_with_algorithm() {
        let state = create_state_with_algorithm();
        let result = StatusBar::format_compact(&state);
        assert_eq!(result, "BTCUSDT|MOM|0/5|IDL");
    }

    #[test]
    fn test_format_compact_paper_trading() {
        let state = create_state_paper_trading();
        let result = StatusBar::format_compact(&state);
        assert!(result.contains("PPR"));
    }

    #[test]
    fn test_format_compact_live_trading() {
        let state = create_state_live_trading();
        let result = StatusBar::format_compact(&state);
        assert!(result.contains("LIV"));
    }

    // ========================================================================
    // Full Format Tests
    // ========================================================================

    #[test]
    fn test_format_full_default() {
        let state = create_default_state();
        let result = StatusBar::format_full(&state);
        assert!(result.contains("BTCUSDT"));
        assert!(result.contains("[None]"));
        assert!(result.contains("Val: 0/5"));
        assert!(result.contains("Idle"));
    }

    #[test]
    fn test_format_full_with_data() {
        let state = create_state_paper_trading();
        let result = StatusBar::format_full(&state);
        assert!(result.contains("BTCUSDT"));
        assert!(result.contains("Momentum v1"));
        assert!(result.contains("Paper"));
    }

    // ========================================================================
    // Width Calculation Tests
    // ========================================================================

    #[test]
    fn test_min_width() {
        let state = create_default_state();
        let width = StatusBar::min_width(&state);
        assert!(width > 0);
        assert!(width < 50); // Compact should be reasonably small
    }

    #[test]
    fn test_full_width() {
        let state = create_default_state();
        let width = StatusBar::full_width(&state);
        assert!(width > 0);
        assert!(width > StatusBar::min_width(&state)); // Full should be larger
    }

    #[test]
    fn test_full_width_grows_with_algorithm() {
        let state1 = create_default_state();
        let state2 = create_state_with_algorithm();
        let width1 = StatusBar::full_width(&state1);
        let width2 = StatusBar::full_width(&state2);
        // Algorithm name adds more width
        assert!(width2 > width1);
    }

    // ========================================================================
    // Research Format Tests
    // ========================================================================

    #[test]
    fn test_format_research_idle() {
        let state = create_default_state();
        let result = StatusBar::format_research(&state);
        assert_eq!(result, "Research: Idle");
    }

    #[test]
    fn test_format_research_running() {
        let mut state = create_default_state();
        state.research_status = ResearchStatus::Running { samples_processed: 1000 };
        let result = StatusBar::format_research(&state);
        assert_eq!(result, "Research: Running (1000 samples)");
    }

    #[test]
    fn test_format_research_complete_tradeable() {
        let mut state = create_default_state();
        state.research_status = ResearchStatus::Complete { tradeable: true };
        let result = StatusBar::format_research(&state);
        assert_eq!(result, "Research: Complete (tradeable)");
    }

    #[test]
    fn test_format_research_complete_not_tradeable() {
        let mut state = create_default_state();
        state.research_status = ResearchStatus::Complete { tradeable: false };
        let result = StatusBar::format_research(&state);
        assert_eq!(result, "Research: Complete (not tradeable)");
    }

    // ========================================================================
    // Stages Visual Tests
    // ========================================================================

    #[test]
    fn test_format_stages_visual_default() {
        let state = create_default_state();
        let result = StatusBar::format_stages_visual(&state);
        assert_eq!(result, "BT:- FW:- OOS:- PP:- LV:-");
    }

    #[test]
    fn test_format_stages_visual_some_passed() {
        let state = create_state_with_validation();
        let result = StatusBar::format_stages_visual(&state);
        assert!(result.contains("BT:OK"));
        assert!(result.contains("FW:OK"));
        assert!(result.contains("OOS:OK"));
        assert!(result.contains("PP:-"));
        assert!(result.contains("LV:-"));
    }

    #[test]
    fn test_format_stages_visual_with_failure() {
        let state = create_state_with_failure();
        let result = StatusBar::format_stages_visual(&state);
        assert!(result.contains("BT:OK"));
        assert!(result.contains("FW:X"));
    }

    #[test]
    fn test_format_stages_visual_running() {
        let mut state = create_default_state();
        state.validation_status.backtest = StageStatus::running(0.5);
        let result = StatusBar::format_stages_visual(&state);
        assert!(result.contains("BT:..."));
    }

    // ========================================================================
    // Build Spans Tests
    // ========================================================================

    #[test]
    fn test_build_spans_not_empty() {
        let state = create_default_state();
        let spans = StatusBar::build_spans(&state, 100);
        assert!(!spans.is_empty());
    }

    #[test]
    fn test_build_spans_truncates_for_small_width() {
        let state = create_default_state();
        let spans_full = StatusBar::build_spans(&state, 100);
        let spans_small = StatusBar::build_spans(&state, 10);
        // Small width should have fewer spans due to truncation
        assert!(spans_small.len() <= spans_full.len());
    }

    #[test]
    fn test_build_spans_contains_separators() {
        let state = create_default_state();
        let spans = StatusBar::build_spans(&state, 100);
        let has_separator = spans.iter().any(|s| s.content.contains("|"));
        assert!(has_separator);
    }

    // ========================================================================
    // Edge Cases
    // ========================================================================

    #[test]
    fn test_empty_symbol() {
        let mut state = GlobalState::new("");
        state.active_algorithm = None;
        let result = StatusBar::format_full(&state);
        assert!(result.contains("[None]"));
    }

    #[test]
    fn test_very_long_algorithm_name() {
        let mut state = create_default_state();
        state.active_algorithm = Some(AlgorithmConfigSummary {
            id: "test".to_string(),
            name: "Very Long Algorithm Name That Might Cause Issues".to_string(),
            strategy_type: StrategyType::Momentum,
            created_at: Utc::now(),
        });
        let result = StatusBar::format_algorithm(&state);
        assert!(result.contains("Very Long Algorithm"));
    }

    #[test]
    fn test_large_pnl_values() {
        let mut state = create_state_all_passed();
        state.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: 1_000_000.00,
        };
        let result = StatusBar::format_trading_mode(&state);
        assert!(result.contains("1000000.00"));
    }

    #[test]
    fn test_negative_large_pnl() {
        let mut state = create_state_all_passed();
        state.trading_mode = TradingMode::Live {
            started: Utc::now(),
            pnl: -999_999.99,
        };
        let result = StatusBar::format_trading_mode(&state);
        assert!(result.contains("-$999999.99"));
    }

    #[test]
    fn test_all_stages_failed() {
        let mut state = create_default_state();
        state.validation_status.backtest = StageStatus::failed("fail1");
        state.validation_status.forward = StageStatus::failed("fail2");
        state.validation_status.oos = StageStatus::failed("fail3");
        state.validation_status.paper = StageStatus::failed("fail4");
        state.validation_status.live = StageStatus::failed("fail5");

        assert_eq!(StatusBar::validation_color(&state), Color::Red);
        assert_eq!(state.passed_stages(), 0);
    }

    #[test]
    fn test_mixed_validation_states() {
        let mut state = create_default_state();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::running(0.5);
        state.validation_status.oos = StageStatus::NotRun;
        state.validation_status.paper = StageStatus::failed("test");
        state.validation_status.live = StageStatus::NotRun;

        // Failed takes precedence for color
        assert_eq!(StatusBar::validation_color(&state), Color::Red);
        assert_eq!(state.passed_stages(), 1);
    }

    #[test]
    fn test_running_takes_precedence_for_indicator() {
        let mut state = create_default_state();
        state.validation_status.backtest = StageStatus::passed(1.5);
        state.validation_status.forward = StageStatus::running(0.5);
        // No failures, but running - should show "..."
        assert_eq!(StatusBar::validation_indicator(&state), " ...");
    }
}
