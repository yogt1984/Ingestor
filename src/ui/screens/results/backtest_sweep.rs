//! Backtest Sweep Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest sweep command results.
//! Supports multiple view modes: TopResults, FullTable, Heatmap.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{SweepResult, SweepResultItem};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for sweep results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepViewMode {
    /// Top 10 results table
    TopResults,
    /// Full table with all results (sortable)
    FullTable,
    /// Heatmap visualization (spread vs skew)
    Heatmap,
}

impl SweepViewMode {
    /// Get all view modes
    pub fn all() -> Vec<SweepViewMode> {
        vec![
            SweepViewMode::TopResults,
            SweepViewMode::FullTable,
            SweepViewMode::Heatmap,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            SweepViewMode::TopResults => "Top Results",
            SweepViewMode::FullTable => "Full Table",
            SweepViewMode::Heatmap => "Heatmap",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> SweepViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> SweepViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let prev_idx = if current_idx == 0 {
            all.len() - 1
        } else {
            current_idx - 1
        };
        all[prev_idx]
    }
}

/// Backtest sweep results screen
pub struct BacktestSweepResultsScreen {
    /// Sweep result data
    result: SweepResult,
    /// Current view mode
    view_mode: SweepViewMode,
    /// Selected result index (for FullTable view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestSweepResultsScreen {
    /// Create a new results screen from SweepResult
    pub fn new(result: SweepResult) -> Self {
        Self {
            result,
            view_mode: SweepViewMode::TopResults,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &SweepResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> SweepViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: SweepViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.result.all_results.len() {
                self.selected_index = Some(idx);
            } else {
                self.selected_index = None;
            }
        } else {
            self.selected_index = None;
        }
    }

    /// Get selected result
    pub fn selected_result(&self) -> Option<&SweepResultItem> {
        self.selected_index
            .and_then(|idx| self.result.all_results.get(idx))
    }

    /// Get best result
    pub fn best_result(&self) -> Option<&SweepResultItem> {
        self.result.best.as_ref()
    }

    /// Check if focused
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Set focused state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Get top N results
    pub fn top_results(&self, n: usize) -> Vec<&SweepResultItem> {
        self.result.all_results.iter().take(n).collect()
    }

    /// Handle key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !self.focused {
            return false;
        }

        match key.code {
            KeyCode::Tab => {
                self.view_mode = self.view_mode.next();
                true
            }
            KeyCode::BackTab => {
                self.view_mode = self.view_mode.previous();
                true
            }
            KeyCode::Char('e') => {
                self.export_path = Some("export.json".to_string());
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.view_mode == SweepViewMode::FullTable {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.all_results.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == SweepViewMode::FullTable {
                    if let Some(idx) = self.selected_index {
                        if idx > 0 {
                            self.set_selected_index(Some(idx - 1));
                        }
                    }
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Create results table
    fn create_results_table(&self, results: &[&SweepResultItem]) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Rank".to_string()).with_width(6).with_sortable(true),
            TableHeader::new("Spread".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Skew".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Sharpe".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Return".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Drawdown".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Trades".to_string()).with_width(8).with_sortable(true),
            TableHeader::new("Win Rate".to_string()).with_width(10).with_sortable(true),
        ];

        let rows: Vec<TableRow> = results
            .iter()
            .enumerate()
            .map(|(rank, item)| {
                TableRow::new(vec![
                    format!("{}", rank + 1),
                    format!("{:.2}", item.spread),
                    format!("{:.2}", item.skew),
                    format!("{:.4}", item.sharpe),
                    format!("{:.2}%", item.total_return * 100.0),
                    format!("{:.2}%", item.max_drawdown * 100.0),
                    format!("{}", item.num_trades),
                    format!("{:.2}%", item.win_rate * 100.0),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create heatmap series
    fn create_heatmap_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.result.all_results
            .iter()
            .map(|item| {
                let x = item.spread;
                let y = item.skew;
                let label = format!("{:.4}", item.sharpe);
                DataPoint::new(x, y).with_label(Some(label))
            })
            .collect();

        DataSeries::new("Sweep Results".to_string())
            .with_points(points)
            .with_color(Some(Color::Green))
            .with_symbol(None)
    }

    /// Export to JSON
    pub fn export_to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(&self.result)?)
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Tabs
                Constraint::Min(0),    // Content
            ])
            .split(area);

        // Render tabs
        let tab_titles: Vec<Line> = SweepViewMode::all()
            .iter()
            .map(|mode| {
                let style = if *mode == self.view_mode {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(mode.name(), style))
            })
            .collect();

        let tabs = Tabs::new(tab_titles)
            .block(Block::default().borders(Borders::ALL).title("View Mode"))
            .select(SweepViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            SweepViewMode::TopResults => {
                self.render_top_results(frame, chunks[1]);
            }
            SweepViewMode::FullTable => {
                self.render_full_table(frame, chunks[1]);
            }
            SweepViewMode::Heatmap => {
                self.render_heatmap(frame, chunks[1]);
            }
        }
    }

    /// Render top results view
    fn render_top_results(&self, f: &mut Frame, area: Rect) {
        let top_10 = self.top_results(10);
        let (headers, rows) = self.create_results_table(&top_10);

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .set_focused(self.focused);

        table.render(area, f.buffer_mut());
    }

    /// Render full table view
    fn render_full_table(&self, f: &mut Frame, area: Rect) {
        let all: Vec<&SweepResultItem> = self.result.all_results.iter().collect();
        let (headers, rows) = self.create_results_table(&all);

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .set_focused(self.focused);

        if let Some(selected_idx) = self.selected_index {
        }

        table.render(area, f.buffer_mut());
    }

    /// Render heatmap view
    fn render_heatmap(&self, f: &mut Frame, area: Rect) {
        let series = self.create_heatmap_series();
        let chart = ChartWidget::new()
            .with_chart_type(ChartType::Scatter)
            .with_series(vec![series])
            .with_x_axis(AxisConfig::default().with_label("Spread (bps)"))
            .with_y_axis(AxisConfig::default().with_label("Skew"));

        chart.render(area, f.buffer_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_sweep_result() -> SweepResult {
        let mut all_results = Vec::new();
        for i in 0..20 {
            all_results.push(SweepResultItem {
                spread: 1.0 + i as f64 * 0.1,
                skew: 0.5 + i as f64 * 0.05,
                sharpe: 2.0 - i as f64 * 0.1,
                total_return: 0.15 - i as f64 * 0.01,
                max_drawdown: -0.05 - i as f64 * 0.005,
                num_trades: 100 - i * 5,
                win_rate: 0.55 - i as f64 * 0.01,
            });
        }

        SweepResult {
            algorithm: "AvellanedaStoikov".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results,
            best: None,
            total_combinations: 20,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = SweepViewMode::all();
        assert_eq!(modes.len(), 3);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(SweepViewMode::TopResults.name(), "Top Results");
        assert_eq!(SweepViewMode::FullTable.name(), "Full Table");
        assert_eq!(SweepViewMode::Heatmap.name(), "Heatmap");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(SweepViewMode::TopResults.next(), SweepViewMode::FullTable);
        assert_eq!(SweepViewMode::FullTable.next(), SweepViewMode::Heatmap);
        assert_eq!(SweepViewMode::Heatmap.next(), SweepViewMode::TopResults);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_sweep_result();
        let screen = BacktestSweepResultsScreen::new(result);
        assert_eq!(screen.view_mode(), SweepViewMode::TopResults);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_sweep_result();
        let mut screen = BacktestSweepResultsScreen::new(result);
        screen.set_view_mode(SweepViewMode::FullTable);
        assert_eq!(screen.view_mode(), SweepViewMode::FullTable);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_sweep_result();
        let mut screen = BacktestSweepResultsScreen::new(result);
        screen.set_selected_index(Some(5));
        assert_eq!(screen.selected_index(), Some(5));
    }

    #[test]
    fn test_selected_result() {
        let result = create_test_sweep_result();
        let mut screen = BacktestSweepResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_result().is_some());
    }

    #[test]
    fn test_top_results() {
        let result = create_test_sweep_result();
        let screen = BacktestSweepResultsScreen::new(result);
        let top_10 = screen.top_results(10);
        assert_eq!(top_10.len(), 10);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_sweep_result();
        let screen = BacktestSweepResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_sweep_result();
        let mut screen = BacktestSweepResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), SweepViewMode::FullTable);
    }

    #[test]
    fn test_create_results_table() {
        let result = create_test_sweep_result();
        let screen = BacktestSweepResultsScreen::new(result);
        let all: Vec<&SweepResultItem> = result.all_results.iter().collect();
        let (headers, rows) = screen.create_results_table(&all);
        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn test_create_heatmap_series() {
        let result = create_test_sweep_result();
        let screen = BacktestSweepResultsScreen::new(result);
        let series = screen.create_heatmap_series();
        assert_eq!(series.points.len(), 20);
    }

    #[test]
    fn test_best_result() {
        let mut result = create_test_sweep_result();
        result.best = Some(result.all_results[0].clone());
        let screen = BacktestSweepResultsScreen::new(result);
        assert!(screen.best_result().is_some());
    }

    #[test]
    fn test_screen_with_empty_results() {
        let result = SweepResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: Vec::new(),
            best: None,
            total_combinations: 0,
        };
        let screen = BacktestSweepResultsScreen::new(result);
        assert_eq!(screen.top_results(10).len(), 0);
    }

    #[test]
    fn test_selected_index_bounds() {
        let result = create_test_sweep_result();
        let mut screen = BacktestSweepResultsScreen::new(result);
        screen.set_selected_index(Some(100));
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = SweepViewMode::TopResults;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 3);
    }
}
