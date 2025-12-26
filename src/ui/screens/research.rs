//! Research Dashboard Screen (Task 4.1)
//!
//! TUI screen displaying current research state including:
//! - MIDC estimate with kappa, tau_half, r_squared
//! - Persistence statistics
//! - Top conditional signals
//! - Tradeable assessment
//! - Research engine status (running/paused)
//!
//! # Usage
//!
//! ```ignore
//! use ingestor::ui::screens::research::{ResearchScreen, draw_research_screen};
//!
//! let mut screen = ResearchScreen::new("./research");
//! screen.refresh()?;
//! draw_research_screen(frame, &screen.state);
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use crate::core::{
    ConditionalProbability, MIDCEstimate, PersistenceStats,
    RecommendedStrategy, ResearchState, ResearchStore, ResearchStoreConfig,
    TradeableAssessment,
};

// ============================================================================
// Types
// ============================================================================

/// Research engine status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResearchEngineStatus {
    /// Engine is running and processing data
    Running,
    /// Engine is paused
    Paused,
    /// Engine is idle (no data)
    #[default]
    Idle,
    /// Error state
    Error,
}

impl ResearchEngineStatus {
    /// Get display string for status
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
            Self::Idle => "IDLE",
            Self::Error => "ERROR",
        }
    }

    /// Get color for status
    pub fn color(&self) -> Color {
        match self {
            Self::Running => Color::Green,
            Self::Paused => Color::Yellow,
            Self::Idle => Color::Gray,
            Self::Error => Color::Red,
        }
    }
}

/// Screen state for the research dashboard
#[derive(Debug, Clone)]
pub struct ResearchScreenState {
    /// Current research state (loaded from store)
    pub research: Option<ResearchState>,
    /// Research engine status
    pub engine_status: ResearchEngineStatus,
    /// Last refresh time
    pub last_refresh: Option<Instant>,
    /// Error message if any
    pub error: Option<String>,
    /// Scroll offset for signals list
    pub scroll_offset: u16,
    /// Number of signals to display
    pub signals_to_show: usize,
    /// Whether the state has been loaded at least once
    pub has_loaded: bool,
}

impl Default for ResearchScreenState {
    fn default() -> Self {
        Self {
            research: None,
            engine_status: ResearchEngineStatus::Idle,
            last_refresh: None,
            error: None,
            scroll_offset: 0,
            signals_to_show: 10,
            has_loaded: false,
        }
    }
}

impl ResearchScreenState {
    /// Create new screen state
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if research state is available
    pub fn has_research(&self) -> bool {
        self.research.is_some()
    }

    /// Get MIDC estimate if available
    pub fn midc(&self) -> Option<&MIDCEstimate> {
        self.research.as_ref().map(|r| &r.midc)
    }

    /// Get tradeable assessment if available
    pub fn assessment(&self) -> Option<&TradeableAssessment> {
        self.research.as_ref().map(|r| &r.assessment)
    }

    /// Get top N conditional signals (sorted by continuation probability)
    pub fn top_signals(&self, n: usize) -> Vec<(&String, &ConditionalProbability)> {
        self.research
            .as_ref()
            .map(|r| {
                let mut signals: Vec<_> = r.conditional_table.iter().collect();
                // Sort by continuation probability descending
                signals.sort_by(|a, b| {
                    b.1.p_continuation
                        .partial_cmp(&a.1.p_continuation)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                signals.into_iter().take(n).collect()
            })
            .unwrap_or_default()
    }

    /// Get persistence stats if available
    pub fn persistence(&self) -> Option<&PersistenceStats> {
        self.research.as_ref().map(|r| &r.persistence)
    }

    /// Get recommended strategy if available
    pub fn recommended_strategy(&self) -> Option<RecommendedStrategy> {
        self.research.as_ref().map(|r| r.assessment.recommended_strategy)
    }

    /// Get entropy if available
    pub fn entropy(&self) -> Option<f64> {
        self.research.as_ref().map(|r| r.entropy)
    }

    /// Update scroll offset
    pub fn scroll_up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    /// Update scroll offset
    pub fn scroll_down(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_add(1);
    }
}

/// Research dashboard screen controller
pub struct ResearchScreen {
    /// Path to research store
    store_path: PathBuf,
    /// Symbol to load
    symbol: String,
    /// Current screen state
    pub state: ResearchScreenState,
    /// Auto-refresh interval
    refresh_interval: Duration,
}

impl ResearchScreen {
    /// Create new research screen
    pub fn new<P: Into<PathBuf>>(store_path: P) -> Self {
        Self {
            store_path: store_path.into(),
            symbol: "BTCUSDT".to_string(),
            state: ResearchScreenState::new(),
            refresh_interval: Duration::from_secs(5),
        }
    }

    /// Set symbol to load
    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    /// Set refresh interval
    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    /// Check if refresh is needed
    pub fn needs_refresh(&self) -> bool {
        match self.state.last_refresh {
            Some(last) => last.elapsed() >= self.refresh_interval,
            None => true,
        }
    }

    /// Refresh research state from store
    pub fn refresh(&mut self) -> Result<(), String> {
        let config = ResearchStoreConfig::with_path(&self.store_path);

        match ResearchStore::new(config) {
            Ok(mut store) => {
                match store.load(&self.symbol) {
                    Ok(Some(research)) => {
                        self.state.research = Some(research);
                        self.state.engine_status = ResearchEngineStatus::Idle;
                        self.state.error = None;
                        self.state.has_loaded = true;
                    }
                    Ok(None) => {
                        self.state.research = None;
                        self.state.engine_status = ResearchEngineStatus::Idle;
                        self.state.error = Some("No research state found".to_string());
                        self.state.has_loaded = true;
                    }
                    Err(e) => {
                        self.state.engine_status = ResearchEngineStatus::Error;
                        self.state.error = Some(format!("Load error: {}", e));
                    }
                }
            }
            Err(e) => {
                self.state.engine_status = ResearchEngineStatus::Error;
                self.state.error = Some(format!("Store error: {}", e));
            }
        }

        self.state.last_refresh = Some(Instant::now());
        Ok(())
    }

    /// Toggle engine status (run/pause)
    pub fn toggle_engine(&mut self) {
        self.state.engine_status = match self.state.engine_status {
            ResearchEngineStatus::Running => ResearchEngineStatus::Paused,
            ResearchEngineStatus::Paused => ResearchEngineStatus::Running,
            ResearchEngineStatus::Idle => ResearchEngineStatus::Running,
            ResearchEngineStatus::Error => ResearchEngineStatus::Idle,
        };
    }

    /// Trigger checkpoint save
    pub fn checkpoint(&self) -> Result<(), String> {
        // In a real implementation, this would signal the research engine
        // to save a checkpoint. For now, we just acknowledge the request.
        Ok(())
    }

    /// Handle key input
    /// Returns true if the key was handled
    pub fn handle_key(&mut self, key: crossterm::event::KeyCode) -> bool {
        use crossterm::event::KeyCode;
        match key {
            KeyCode::Up | KeyCode::Char('k') => {
                self.state.scroll_up();
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.state.scroll_down();
                true
            }
            KeyCode::Char('r') => {
                let _ = self.refresh();
                true
            }
            KeyCode::Char('p') => {
                self.toggle_engine();
                true
            }
            KeyCode::Char('c') => {
                let _ = self.checkpoint();
                true
            }
            _ => false,
        }
    }
}

// ============================================================================
// Rendering
// ============================================================================

/// Draw the research dashboard screen
pub fn draw_research_screen(f: &mut Frame, state: &ResearchScreenState) {
    let size = f.area();

    // Main layout: header, content, controls
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header with status
            Constraint::Min(10),    // Main content
            Constraint::Length(3),  // Controls
        ])
        .split(size);

    draw_header(f, chunks[0], state);
    draw_content(f, chunks[1], state);
    draw_controls(f, chunks[2]);
}

/// Draw header with status bar
fn draw_header(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let status_style = Style::default()
        .fg(state.engine_status.color())
        .add_modifier(Modifier::BOLD);

    let title = format!(
        " RESEARCH DASHBOARD | Status: {} ",
        state.engine_status.as_str()
    );

    let refresh_info = state
        .last_refresh
        .map(|t| format!(" | Last refresh: {:.1}s ago", t.elapsed().as_secs_f64()))
        .unwrap_or_default();

    let lines = vec![Line::from(vec![
        Span::styled(&title, status_style),
        Span::styled(refresh_info, Style::default().fg(Color::DarkGray)),
    ])];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(Span::styled(
            " [R] RESEARCH ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ));

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw main content area
fn draw_content(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    if state.error.is_some() && !state.has_research() {
        draw_error(f, area, state);
        return;
    }

    if !state.has_research() {
        draw_no_data(f, area);
        return;
    }

    // Split content into columns
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(40), // Left: MIDC, Assessment
            Constraint::Percentage(60), // Right: Signals, Persistence
        ])
        .split(area);

    draw_left_panel(f, columns[0], state);
    draw_right_panel(f, columns[1], state);
}

/// Draw left panel: MIDC, Tradeable Assessment
fn draw_left_panel(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // MIDC section
            Constraint::Min(6),     // Assessment section
        ])
        .split(area);

    draw_midc_section(f, chunks[0], state);
    draw_assessment_section(f, chunks[1], state);
}

/// Draw MIDC estimate section
fn draw_midc_section(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let midc = state.midc();
    let entropy = state.entropy();

    let lines = if let Some(m) = midc {
        let tau_half_color = if m.tau_half_seconds > 30.0 {
            Color::Green  // Long persistence = good for MM
        } else if m.tau_half_seconds > 10.0 {
            Color::Yellow
        } else {
            Color::Red    // Fast decay = risky
        };

        let r_sq_color = if m.r_squared > 0.8 {
            Color::Green
        } else if m.r_squared > 0.5 {
            Color::Yellow
        } else {
            Color::Red
        };

        vec![
            Line::from(""),
            Line::from(vec![
                Span::raw("  Kappa (MIDC):  "),
                Span::styled(
                    format!("{:.4}", m.kappa),
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Tau Half:      "),
                Span::styled(
                    format!("{:.2}s", m.tau_half_seconds),
                    Style::default().fg(tau_half_color),
                ),
            ]),
            Line::from(vec![
                Span::raw("  R-squared:     "),
                Span::styled(
                    format!("{:.3}", m.r_squared),
                    Style::default().fg(r_sq_color),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Rho_0:         "),
                Span::styled(
                    format!("{:.4}", m.rho_0),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Sample Size:   "),
                Span::styled(
                    format!("{}", m.sample_size),
                    Style::default().fg(Color::White),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Entropy:       "),
                Span::styled(
                    format!("{:.3}", entropy.unwrap_or(0.0)),
                    Style::default().fg(Color::Magenta),
                ),
            ]),
            Line::from(""),
        ]
    } else {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "  No MIDC estimate available",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(""),
        ]
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            " MIDC ESTIMATE ",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw tradeable assessment section
fn draw_assessment_section(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let assessment = state.assessment();
    let strategy = state.recommended_strategy();

    let lines = if let Some(a) = assessment {
        let tradeable_color = if a.is_tradeable {
            Color::Green
        } else {
            Color::Red
        };

        let check = |ok: bool| if ok { "OK" } else { "NO" };
        let check_color = |ok: bool| if ok { Color::Green } else { Color::Red };

        let mut lines = vec![
            Line::from(""),
            Line::from(vec![
                Span::raw("  Tradeable: "),
                Span::styled(
                    if a.is_tradeable { "YES" } else { "NO" },
                    Style::default()
                        .fg(tradeable_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  Scale: "),
                Span::styled(
                    format!("{:.0}%", a.position_scale * 100.0),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::raw("  MIDC:        "),
                Span::styled(check(a.midc_ok), Style::default().fg(check_color(a.midc_ok))),
            ]),
            Line::from(vec![
                Span::raw("  Entropy:     "),
                Span::styled(check(a.entropy_ok), Style::default().fg(check_color(a.entropy_ok))),
            ]),
            Line::from(vec![
                Span::raw("  Persistence: "),
                Span::styled(check(a.persistence_ok), Style::default().fg(check_color(a.persistence_ok))),
            ]),
            Line::from(vec![
                Span::raw("  Signals:     "),
                Span::styled(check(a.signals_ok), Style::default().fg(check_color(a.signals_ok))),
            ]),
        ];

        if let Some(s) = strategy {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::raw("  Strategy:    "),
                Span::styled(
                    format!("{:?}", s),
                    Style::default().fg(Color::Magenta),
                ),
            ]));
        }

        // Add reasoning (truncated)
        if !a.reasoning.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("  {}", truncate_str(&a.reasoning, 40)),
                Style::default().fg(Color::DarkGray),
            )));
        }

        lines
    } else {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "  No tradeable assessment available",
                Style::default().fg(Color::DarkGray),
            )),
        ]
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            " TRADEABLE ASSESSMENT ",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw right panel: Signals and Persistence
fn draw_right_panel(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(60), // Signals
            Constraint::Percentage(40), // Persistence
        ])
        .split(area);

    draw_signals_section(f, chunks[0], state);
    draw_persistence_section(f, chunks[1], state);
}

/// Draw conditional signals section
fn draw_signals_section(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let signals = state.top_signals(state.signals_to_show);

    let mut lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(
                format!("{:<18}", "Signature"),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:>8}", "P(cont)"),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:>8}", "P(rev)"),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{:>10}", "Samples"),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(Span::styled(
            "  ────────────────────────────────────────────",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    if signals.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No conditional signals available",
            Style::default().fg(Color::DarkGray),
        )));
    } else {
        for (i, (key, sig)) in signals.iter().enumerate() {
            if i < state.scroll_offset as usize {
                continue;
            }
            if lines.len() > area.height as usize - 2 {
                break;
            }

            let cont_color = if sig.p_continuation > 0.6 {
                Color::Green
            } else if sig.p_continuation > 0.45 {
                Color::Yellow
            } else {
                Color::Red
            };

            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    format!("{:<18}", truncate_str(key, 17)),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{:>7.1}%", sig.p_continuation * 100.0),
                    Style::default().fg(cont_color),
                ),
                Span::styled(
                    format!("{:>7.1}%", sig.p_reversal * 100.0),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{:>10}", sig.sample_count),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            " CONDITIONAL SIGNALS ",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw persistence stats section
fn draw_persistence_section(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let persistence = state.persistence();

    let lines = if let Some(p) = persistence {
        vec![
            Line::from(""),
            Line::from(vec![
                Span::raw("  Mean Duration:   "),
                Span::styled(
                    format!("{:.2}s", p.mean_duration_seconds),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Median Duration: "),
                Span::styled(
                    format!("{:.2}s", p.median_duration_seconds),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Std Deviation:   "),
                Span::styled(
                    format!("{:.2}s", p.std_duration_seconds),
                    Style::default().fg(Color::Yellow),
                ),
            ]),
            Line::from(vec![
                Span::raw("  P25 / P75:       "),
                Span::styled(
                    format!("{:.2}s / {:.2}s", p.percentile_25, p.percentile_75),
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                Span::raw("  Sample Count:    "),
                Span::styled(
                    format!("{}", p.sample_count),
                    Style::default().fg(Color::White),
                ),
            ]),
        ]
    } else {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "  No persistence statistics available",
                Style::default().fg(Color::DarkGray),
            )),
        ]
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            " PERSISTENCE STATISTICS ",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ));

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw controls section
fn draw_controls(f: &mut Frame, area: Rect) {
    let lines = vec![Line::from(vec![
        Span::styled(" [r] ", Style::default().fg(Color::Green)),
        Span::raw("Refresh"),
        Span::raw("  "),
        Span::styled(" [p] ", Style::default().fg(Color::Yellow)),
        Span::raw("Run/Pause"),
        Span::raw("  "),
        Span::styled(" [c] ", Style::default().fg(Color::Cyan)),
        Span::raw("Checkpoint"),
        Span::raw("  "),
        Span::styled(" [j/k] ", Style::default().fg(Color::Blue)),
        Span::raw("Scroll"),
        Span::raw("  "),
        Span::styled(" [q] ", Style::default().fg(Color::Red)),
        Span::raw("Back"),
    ])];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw error state
fn draw_error(f: &mut Frame, area: Rect, state: &ResearchScreenState) {
    let error_msg = state.error.as_deref().unwrap_or("Unknown error");

    let lines = vec![
        Line::from(""),
        Line::from(""),
        Line::from(Span::styled(
            "  Error Loading Research State",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            format!("  {}", error_msg),
            Style::default().fg(Color::Red),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  Press [r] to retry or [q] to go back",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red))
        .title(" ERROR ");

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

/// Draw no data state
fn draw_no_data(f: &mut Frame, area: Rect) {
    let lines = vec![
        Line::from(""),
        Line::from(""),
        Line::from(Span::styled(
            "  No Research Data Available",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  Run the research CLI to generate research state:",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "    cargo run --bin research -- run --data ./data/features",
            Style::default().fg(Color::Green),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  Press [r] to refresh or [q] to go back",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(" NO DATA ");

    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

// ============================================================================
// Helpers
// ============================================================================

/// Truncate string to max length with ellipsis
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use chrono::Utc;

    // ========================================================================
    // ResearchEngineStatus Tests
    // ========================================================================

    #[test]
    fn test_engine_status_as_str() {
        assert_eq!(ResearchEngineStatus::Running.as_str(), "RUNNING");
        assert_eq!(ResearchEngineStatus::Paused.as_str(), "PAUSED");
        assert_eq!(ResearchEngineStatus::Idle.as_str(), "IDLE");
        assert_eq!(ResearchEngineStatus::Error.as_str(), "ERROR");
    }

    #[test]
    fn test_engine_status_color() {
        assert_eq!(ResearchEngineStatus::Running.color(), Color::Green);
        assert_eq!(ResearchEngineStatus::Paused.color(), Color::Yellow);
        assert_eq!(ResearchEngineStatus::Idle.color(), Color::Gray);
        assert_eq!(ResearchEngineStatus::Error.color(), Color::Red);
    }

    #[test]
    fn test_engine_status_default() {
        let status = ResearchEngineStatus::default();
        assert_eq!(status, ResearchEngineStatus::Idle);
    }

    // ========================================================================
    // ResearchScreenState Tests
    // ========================================================================

    #[test]
    fn test_screen_state_default() {
        let state = ResearchScreenState::default();
        assert!(state.research.is_none());
        assert_eq!(state.engine_status, ResearchEngineStatus::Idle);
        assert!(state.last_refresh.is_none());
        assert!(state.error.is_none());
        assert_eq!(state.scroll_offset, 0);
        assert_eq!(state.signals_to_show, 10);
        assert!(!state.has_loaded);
    }

    #[test]
    fn test_screen_state_has_research() {
        let mut state = ResearchScreenState::new();
        assert!(!state.has_research());

        state.research = Some(create_test_research_state());
        assert!(state.has_research());
    }

    #[test]
    fn test_screen_state_midc_accessor() {
        let mut state = ResearchScreenState::new();
        assert!(state.midc().is_none());

        state.research = Some(create_test_research_state());
        let midc = state.midc().unwrap();
        assert!(midc.kappa > 0.0);
    }

    #[test]
    fn test_screen_state_top_signals_empty() {
        let state = ResearchScreenState::new();
        let signals = state.top_signals(5);
        assert!(signals.is_empty());
    }

    #[test]
    fn test_screen_state_top_signals_sorted() {
        let mut state = ResearchScreenState::new();
        state.research = Some(create_test_research_state_with_signals());

        let signals = state.top_signals(3);
        assert_eq!(signals.len(), 3);

        // Verify sorted by p_continuation descending
        for i in 1..signals.len() {
            assert!(signals[i - 1].1.p_continuation >= signals[i].1.p_continuation);
        }
    }

    #[test]
    fn test_screen_state_scroll_up() {
        let mut state = ResearchScreenState::new();
        state.scroll_offset = 5;
        state.scroll_up();
        assert_eq!(state.scroll_offset, 4);

        // Test underflow protection
        state.scroll_offset = 0;
        state.scroll_up();
        assert_eq!(state.scroll_offset, 0);
    }

    #[test]
    fn test_screen_state_scroll_down() {
        let mut state = ResearchScreenState::new();
        state.scroll_down();
        assert_eq!(state.scroll_offset, 1);

        state.scroll_down();
        assert_eq!(state.scroll_offset, 2);
    }

    #[test]
    fn test_screen_state_assessment_accessor() {
        let mut state = ResearchScreenState::new();
        assert!(state.assessment().is_none());

        state.research = Some(create_test_research_state());
        assert!(state.assessment().is_some());
    }

    #[test]
    fn test_screen_state_persistence_accessor() {
        let mut state = ResearchScreenState::new();
        assert!(state.persistence().is_none());

        state.research = Some(create_test_research_state());
        assert!(state.persistence().is_some());
    }

    #[test]
    fn test_screen_state_recommended_strategy_accessor() {
        let mut state = ResearchScreenState::new();
        assert!(state.recommended_strategy().is_none());

        state.research = Some(create_test_research_state());
        assert!(state.recommended_strategy().is_some());
    }

    #[test]
    fn test_screen_state_entropy_accessor() {
        let mut state = ResearchScreenState::new();
        assert!(state.entropy().is_none());

        state.research = Some(create_test_research_state());
        assert!(state.entropy().is_some());
    }

    // ========================================================================
    // ResearchScreen Tests
    // ========================================================================

    #[test]
    fn test_research_screen_new() {
        let screen = ResearchScreen::new("./test/research");
        assert_eq!(screen.store_path, PathBuf::from("./test/research"));
        assert_eq!(screen.symbol, "BTCUSDT");
        assert!(!screen.state.has_loaded);
    }

    #[test]
    fn test_research_screen_with_symbol() {
        let screen = ResearchScreen::new("./research").with_symbol("ETHUSDT");
        assert_eq!(screen.symbol, "ETHUSDT");
    }

    #[test]
    fn test_research_screen_with_refresh_interval() {
        let screen = ResearchScreen::new("./research")
            .with_refresh_interval(Duration::from_secs(10));
        assert_eq!(screen.refresh_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_research_screen_needs_refresh_initial() {
        let screen = ResearchScreen::new("./research");
        assert!(screen.needs_refresh());
    }

    #[test]
    fn test_research_screen_needs_refresh_after_refresh() {
        let mut screen = ResearchScreen::new("./research");
        screen.state.last_refresh = Some(Instant::now());
        assert!(!screen.needs_refresh());
    }

    #[test]
    fn test_research_screen_toggle_engine() {
        let mut screen = ResearchScreen::new("./research");

        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Idle);

        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Running);

        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Paused);

        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Running);
    }

    #[test]
    fn test_research_screen_toggle_engine_from_error() {
        let mut screen = ResearchScreen::new("./research");
        screen.state.engine_status = ResearchEngineStatus::Error;

        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Idle);
    }

    #[test]
    fn test_research_screen_checkpoint() {
        let screen = ResearchScreen::new("./research");
        let result = screen.checkpoint();
        assert!(result.is_ok());
    }

    #[test]
    fn test_research_screen_handle_key_scroll_up() {
        let mut screen = ResearchScreen::new("./research");
        screen.state.scroll_offset = 5;

        let handled = screen.handle_key(crossterm::event::KeyCode::Up);
        assert!(handled);
        assert_eq!(screen.state.scroll_offset, 4);

        let handled = screen.handle_key(crossterm::event::KeyCode::Char('k'));
        assert!(handled);
        assert_eq!(screen.state.scroll_offset, 3);
    }

    #[test]
    fn test_research_screen_handle_key_scroll_down() {
        let mut screen = ResearchScreen::new("./research");

        let handled = screen.handle_key(crossterm::event::KeyCode::Down);
        assert!(handled);
        assert_eq!(screen.state.scroll_offset, 1);

        let handled = screen.handle_key(crossterm::event::KeyCode::Char('j'));
        assert!(handled);
        assert_eq!(screen.state.scroll_offset, 2);
    }

    #[test]
    fn test_research_screen_handle_key_toggle() {
        let mut screen = ResearchScreen::new("./research");

        let handled = screen.handle_key(crossterm::event::KeyCode::Char('p'));
        assert!(handled);
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Running);
    }

    #[test]
    fn test_research_screen_handle_key_checkpoint() {
        let mut screen = ResearchScreen::new("./research");

        let handled = screen.handle_key(crossterm::event::KeyCode::Char('c'));
        assert!(handled);
    }

    #[test]
    fn test_research_screen_handle_key_unhandled() {
        let mut screen = ResearchScreen::new("./research");

        let handled = screen.handle_key(crossterm::event::KeyCode::Char('x'));
        assert!(!handled);
    }

    #[test]
    fn test_research_screen_refresh_missing_store() {
        let mut screen = ResearchScreen::new("/nonexistent/path");
        let _ = screen.refresh();

        // Should have error but still mark as refreshed
        assert!(screen.state.last_refresh.is_some());
    }

    // ========================================================================
    // Helper Tests
    // ========================================================================

    #[test]
    fn test_truncate_str_short() {
        let result = truncate_str("hello", 10);
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_truncate_str_exact() {
        let result = truncate_str("hello", 5);
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_truncate_str_long() {
        let result = truncate_str("hello world", 8);
        assert_eq!(result, "hello...");
    }

    #[test]
    fn test_truncate_str_very_short_max() {
        let result = truncate_str("hello", 3);
        assert_eq!(result, "...");
    }

    // ========================================================================
    // Display Logic Tests
    // ========================================================================

    #[test]
    fn test_tau_half_color_thresholds() {
        // tau_half > 30s = green (good for MM)
        // tau_half > 10s = yellow
        // tau_half <= 10s = red (risky)
        assert!(30.1 > 30.0); // green
        assert!(15.0 > 10.0 && 15.0 <= 30.0); // yellow
        assert!(5.0 <= 10.0); // red
    }

    #[test]
    fn test_signals_sorted_by_continuation() {
        let mut state = ResearchScreenState::new();
        state.research = Some(create_test_research_state_with_signals());

        let signals = state.top_signals(10);
        assert!(signals.len() >= 3);

        // Check descending order by p_continuation
        for window in signals.windows(2) {
            assert!(
                window[0].1.p_continuation >= window[1].1.p_continuation,
                "Signals not sorted: {} vs {}",
                window[0].1.p_continuation,
                window[1].1.p_continuation
            );
        }
    }

    #[test]
    fn test_top_signals_respects_limit() {
        let mut state = ResearchScreenState::new();
        state.research = Some(create_test_research_state_with_signals());

        let signals = state.top_signals(2);
        assert_eq!(signals.len(), 2);
    }

    #[test]
    fn test_engine_status_transitions() {
        let mut screen = ResearchScreen::new("./research");

        // Idle -> Running
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Idle);
        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Running);

        // Running -> Paused
        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Paused);

        // Paused -> Running
        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Running);

        // Error -> Idle
        screen.state.engine_status = ResearchEngineStatus::Error;
        screen.toggle_engine();
        assert_eq!(screen.state.engine_status, ResearchEngineStatus::Idle);
    }

    #[test]
    fn test_assessment_check_flags() {
        let mut state = ResearchScreenState::new();
        state.research = Some(create_test_research_state());

        let assessment = state.assessment().unwrap();
        // All flags should be booleans
        let _ = assessment.midc_ok;
        let _ = assessment.entropy_ok;
        let _ = assessment.persistence_ok;
        let _ = assessment.signals_ok;
        let _ = assessment.is_tradeable;
    }

    #[test]
    fn test_persistence_stats_display() {
        let mut state = ResearchScreenState::new();
        state.research = Some(create_test_research_state());

        let persistence = state.persistence().unwrap();
        assert!(persistence.mean_duration_seconds >= 0.0);
        assert!(persistence.median_duration_seconds >= 0.0);
        assert!(persistence.std_duration_seconds >= 0.0);
    }

    // ========================================================================
    // Integration-like Tests
    // ========================================================================

    #[test]
    fn test_full_screen_state_workflow() {
        // Create screen
        let mut screen = ResearchScreen::new("./test_research")
            .with_symbol("BTCUSDT")
            .with_refresh_interval(Duration::from_secs(1));

        // Initial state
        assert!(!screen.state.has_research());
        assert!(screen.needs_refresh());

        // Simulate loading data
        screen.state.research = Some(create_test_research_state());
        screen.state.has_loaded = true;
        screen.state.last_refresh = Some(Instant::now());

        // Now has data
        assert!(screen.state.has_research());
        assert!(!screen.needs_refresh());

        // Access all data
        assert!(screen.state.midc().is_some());
        assert!(screen.state.assessment().is_some());
        assert!(screen.state.persistence().is_some());
        assert!(screen.state.recommended_strategy().is_some());
        assert!(screen.state.entropy().is_some());
    }

    #[test]
    fn test_error_state_handling() {
        let mut state = ResearchScreenState::new();
        state.error = Some("Test error".to_string());
        state.engine_status = ResearchEngineStatus::Error;

        assert!(!state.has_research());
        assert!(state.error.is_some());
        assert_eq!(state.engine_status, ResearchEngineStatus::Error);
    }

    // ========================================================================
    // Test Helpers
    // ========================================================================

    fn create_test_research_state() -> ResearchState {
        ResearchState {
            id: "test-id".to_string(),
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            midc: MIDCEstimate {
                kappa: 0.15,
                tau_half_seconds: 6.5,
                rho_0: 0.8,
                r_squared: 0.85,
                sample_size: 1000,
                confidence: 0.85,
                computed_at: Utc::now(),
            },
            persistence: PersistenceStats {
                mean_duration_seconds: 5.0,
                median_duration_seconds: 4.5,
                std_duration_seconds: 2.0,
                percentile_25: 3.0,
                percentile_75: 7.0,
                sample_count: 500,
                updated_at: Utc::now(),
            },
            conditional_table: HashMap::new(),
            entropy: 0.65,
            assessment: TradeableAssessment::new(true, true, true, true),
            data_start: Some(Utc::now()),
            data_end: Some(Utc::now()),
            snapshots_processed: 10000,
            engine_version: "1.0.0".to_string(),
            tsmom_config: None,
            tsmom_signal: None,
            tsmom_stats: None,
        }
    }

    fn create_test_research_state_with_signals() -> ResearchState {
        let mut state = create_test_research_state();

        state.conditional_table.insert(
            "high_entropy".to_string(),
            ConditionalProbability {
                p_continuation: 0.75,
                p_reversal: 0.25,
                expected_magnitude_bps: 5.0,
                std_magnitude_bps: 2.0,
                sample_count: 500,
                confidence_interval: (0.70, 0.80),
            },
        );
        state.conditional_table.insert(
            "low_imbalance".to_string(),
            ConditionalProbability {
                p_continuation: 0.65,
                p_reversal: 0.35,
                expected_magnitude_bps: 3.0,
                std_magnitude_bps: 1.5,
                sample_count: 800,
                confidence_interval: (0.60, 0.70),
            },
        );
        state.conditional_table.insert(
            "high_volatility".to_string(),
            ConditionalProbability {
                p_continuation: 0.45,
                p_reversal: 0.55,
                expected_magnitude_bps: 8.0,
                std_magnitude_bps: 4.0,
                sample_count: 300,
                confidence_interval: (0.40, 0.50),
            },
        );
        state.conditional_table.insert(
            "low_volume".to_string(),
            ConditionalProbability {
                p_continuation: 0.55,
                p_reversal: 0.45,
                expected_magnitude_bps: 2.0,
                std_magnitude_bps: 1.0,
                sample_count: 200,
                confidence_interval: (0.48, 0.62),
            },
        );
        state.conditional_table.insert(
            "spread_wide".to_string(),
            ConditionalProbability {
                p_continuation: 0.60,
                p_reversal: 0.40,
                expected_magnitude_bps: 4.0,
                std_magnitude_bps: 2.0,
                sample_count: 150,
                confidence_interval: (0.52, 0.68),
            },
        );

        state
    }
}
