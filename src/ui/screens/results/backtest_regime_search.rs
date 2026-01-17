//! Backtest Regime Search Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest regime_search command results.
//! Supports multiple view modes: TopResults, FullTable, Heatmap, Comparison.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{RegimeSearchResult, RegimeSearchResultItem};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
};

// ============================================================================
// Types
// ============================================================================

/// View mode for regime search results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegimeSearchViewMode {
    /// Top 10 results table
    TopResults,
    /// Full table with all results (sortable)
    FullTable,
    /// Heatmap visualization (spread vs skew)
    Heatmap,
    /// Comparison view (with vs without quote)
    Comparison,
}

impl RegimeSearchViewMode {
    /// Get all view modes
    pub fn all() -> Vec<RegimeSearchViewMode> {
        vec![
            RegimeSearchViewMode::TopResults,
            RegimeSearchViewMode::FullTable,
            RegimeSearchViewMode::Heatmap,
            RegimeSearchViewMode::Comparison,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            RegimeSearchViewMode::TopResults => "Top Results",
            RegimeSearchViewMode::FullTable => "Full Table",
            RegimeSearchViewMode::Heatmap => "Heatmap",
            RegimeSearchViewMode::Comparison => "Comparison",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> RegimeSearchViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> RegimeSearchViewMode {
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

/// Backtest regime search results screen
pub struct BacktestRegimeSearchResultsScreen {
    /// Regime search result data
    result: RegimeSearchResult,
    /// Current view mode
    view_mode: RegimeSearchViewMode,
    /// Selected result index (for FullTable view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestRegimeSearchResultsScreen {
    /// Create a new results screen from RegimeSearchResult
    pub fn new(result: RegimeSearchResult) -> Self {
        Self {
            result,
            view_mode: RegimeSearchViewMode::TopResults,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &RegimeSearchResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> RegimeSearchViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: RegimeSearchViewMode) {
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
    pub fn selected_result(&self) -> Option<&RegimeSearchResultItem> {
        self.selected_index
            .and_then(|idx| self.result.all_results.get(idx))
    }

    /// Get best result
    pub fn best_result(&self) -> Option<&RegimeSearchResultItem> {
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
    pub fn top_results(&self, n: usize) -> Vec<&RegimeSearchResultItem> {
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
                if self.view_mode == RegimeSearchViewMode::FullTable {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.all_results.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == RegimeSearchViewMode::FullTable {
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
    fn create_results_table(&self, results: &[&RegimeSearchResultItem]) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Rank".to_string()).with_width(6).with_sortable(true),
            TableHeader::new("High Spread".to_string()).with_width(12).with_sortable(true),
            TableHeader::new("High Skew".to_string()).with_width(12).with_sortable(true),
            TableHeader::new("Med Spread".to_string()).with_width(12).with_sortable(true),
            TableHeader::new("Med Skew".to_string()).with_width(12).with_sortable(true),
            TableHeader::new("Low Spread".to_string()).with_width(12).with_sortable(true),
            TableHeader::new("Low Skew".to_string()).with_width(12).with_sortable(true),
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
                    format!("{:.2}", item.high_spread),
                    format!("{:.2}", item.high_skew),
                    format!("{:.2}", item.med_spread),
                    format!("{:.2}", item.med_skew),
                    item.low_spread.map(|s| format!("{:.2}", s)).unwrap_or_else(|| "N/A".to_string()),
                    format!("{:.2}", item.low_skew),
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
                let x = item.high_spread;
                let y = item.high_skew;
                let label = format!("{:.4}", item.sharpe);
                DataPoint::new(x, y).with_label(Some(label))
            })
            .collect();

        DataSeries::new("Regime Search Results".to_string())
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
        let tab_titles: Vec<Line> = RegimeSearchViewMode::all()
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
            .select(RegimeSearchViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            RegimeSearchViewMode::TopResults => {
                self.render_top_results(frame, chunks[1]);
            }
            RegimeSearchViewMode::FullTable => {
                self.render_full_table(f, chunks[1]);
            }
            RegimeSearchViewMode::Heatmap => {
                self.render_heatmap(f, chunks[1]);
            }
            RegimeSearchViewMode::Comparison => {
                self.render_comparison(f, chunks[1]);
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
        let all: Vec<&RegimeSearchResultItem> = self.result.all_results.iter().collect();
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
            .with_x_axis(AxisConfig::default().with_label("High Spread (bps)"))
            .with_y_axis(AxisConfig::default().with_label("High Skew"));

        chart.render(area, f.buffer_mut());
    }

    /// Render comparison view
    fn render_comparison(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(0),
            ])
            .split(area);

        // Summary metrics
        let metrics = vec![
            Metric::new("Total Combinations".to_string(), MetricValue::Number(self.result.total_combinations as f64), MetricFormat::Integer),
            Metric::new("Best Sharpe".to_string(), MetricValue::Number(self.result.best.as_ref().map(|b| b.sharpe).unwrap_or(0.0)), MetricFormat::Decimal(4)),
            Metric::new("Avg Sharpe (With Quote)".to_string(), MetricValue::Number(self.result.avg_sharpe_with_quote.unwrap_or(0.0)), MetricFormat::Decimal(4)),
            Metric::new("Avg Sharpe (No Quote)".to_string(), MetricValue::Number(self.result.avg_sharpe_without_quote.unwrap_or(0.0)), MetricFormat::Decimal(4)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(frame, chunks[0]);

        // Best result details
        if let Some(best) = &self.result.best {
            let details = vec![
                format!("High Spread: {:.2} bps", best.high_spread),
                format!("High Skew: {:.2}", best.high_skew),
                format!("Med Spread: {:.2} bps", best.med_spread),
                format!("Med Skew: {:.2}", best.med_skew),
                format!("Low Spread: {}", best.low_spread.map(|s| format!("{:.2} bps", s)).unwrap_or_else(|| "N/A".to_string())),
                format!("Low Skew: {:.2}", best.low_skew),
                format!("Sharpe: {:.4}", best.sharpe),
                format!("Return: {:.2}%", best.total_return * 100.0),
                format!("Drawdown: {:.2}%", best.max_drawdown * 100.0),
                format!("Trades: {}", best.num_trades),
                format!("Win Rate: {:.2}%", best.win_rate * 100.0),
            ];

            let text: Vec<Line> = details.iter().map(|s| Line::from(s.as_str())).collect();
            let paragraph = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Best Result"))
                .alignment(Alignment::Left);

            f.render_widget(paragraph, chunks[1]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_regime_search_result() -> RegimeSearchResult {
        let mut all_results = Vec::new();
        for i in 0..20 {
            all_results.push(RegimeSearchResultItem {
                high_spread: 1.0 + i as f64 * 0.1,
                high_skew: 0.5 + i as f64 * 0.05,
                med_spread: 0.8 + i as f64 * 0.08,
                med_skew: 0.4 + i as f64 * 0.04,
                low_spread: Some(0.5 + i as f64 * 0.05),
                low_skew: 0.3 + i as f64 * 0.03,
                fill_prob: 0.8,
                sharpe: 2.0 - i as f64 * 0.1,
                total_return: 0.15 - i as f64 * 0.01,
                max_drawdown: -0.05 - i as f64 * 0.005,
                num_trades: 100 - i * 5,
                win_rate: 0.55 - i as f64 * 0.01,
            });
        }

        RegimeSearchResult {
            algorithm: "AvellanedaStoikov".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results,
            best: None,
            total_combinations: 20,
            avg_sharpe_with_quote: Some(1.5),
            avg_sharpe_without_quote: Some(1.2),
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = RegimeSearchViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(RegimeSearchViewMode::TopResults.name(), "Top Results");
        assert_eq!(RegimeSearchViewMode::FullTable.name(), "Full Table");
        assert_eq!(RegimeSearchViewMode::Heatmap.name(), "Heatmap");
        assert_eq!(RegimeSearchViewMode::Comparison.name(), "Comparison");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(RegimeSearchViewMode::TopResults.next(), RegimeSearchViewMode::FullTable);
        assert_eq!(RegimeSearchViewMode::FullTable.next(), RegimeSearchViewMode::Heatmap);
        assert_eq!(RegimeSearchViewMode::Heatmap.next(), RegimeSearchViewMode::Comparison);
        assert_eq!(RegimeSearchViewMode::Comparison.next(), RegimeSearchViewMode::TopResults);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_regime_search_result();
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        assert_eq!(screen.view_mode(), RegimeSearchViewMode::TopResults);
        assert!(screen.is_focused());
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_regime_search_result();
        let mut screen = BacktestRegimeSearchResultsScreen::new(result);
        screen.set_view_mode(RegimeSearchViewMode::FullTable);
        assert_eq!(screen.view_mode(), RegimeSearchViewMode::FullTable);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_regime_search_result();
        let mut screen = BacktestRegimeSearchResultsScreen::new(result);
        screen.set_selected_index(Some(5));
        assert_eq!(screen.selected_index(), Some(5));
    }

    #[test]
    fn test_selected_result() {
        let result = create_test_regime_search_result();
        let mut screen = BacktestRegimeSearchResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_result().is_some());
    }

    #[test]
    fn test_top_results() {
        let result = create_test_regime_search_result();
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        let top_10 = screen.top_results(10);
        assert_eq!(top_10.len(), 10);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_regime_search_result();
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_regime_search_result();
        let mut screen = BacktestRegimeSearchResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), RegimeSearchViewMode::FullTable);
    }

    #[test]
    fn test_handle_key_arrow_keys() {
        let result = create_test_regime_search_result();
        let mut screen = BacktestRegimeSearchResultsScreen::new(result);
        screen.set_view_mode(RegimeSearchViewMode::FullTable);
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.selected_index(), Some(0));
    }

    #[test]
    fn test_create_results_table() {
        let result = create_test_regime_search_result();
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        let all: Vec<&RegimeSearchResultItem> = result.all_results.iter().collect();
        let (headers, rows) = screen.create_results_table(&all);
        assert_eq!(headers.len(), 12);
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn test_create_heatmap_series() {
        let result = create_test_regime_search_result();
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        let series = screen.create_heatmap_series();
        assert_eq!(series.points.len(), 20);
    }

    #[test]
    fn test_best_result() {
        let mut result = create_test_regime_search_result();
        result.best = Some(result.all_results[0].clone());
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        assert!(screen.best_result().is_some());
    }

    #[test]
    fn test_screen_with_empty_results() {
        let result = RegimeSearchResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: Vec::new(),
            best: None,
            total_combinations: 0,
            avg_sharpe_with_quote: None,
            avg_sharpe_without_quote: None,
        };
        let screen = BacktestRegimeSearchResultsScreen::new(result);
        assert_eq!(screen.top_results(10).len(), 0);
    }

    #[test]
    fn test_selected_index_bounds() {
        let result = create_test_regime_search_result();
        let mut screen = BacktestRegimeSearchResultsScreen::new(result);
        screen.set_selected_index(Some(100));
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = RegimeSearchViewMode::TopResults;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }
}
