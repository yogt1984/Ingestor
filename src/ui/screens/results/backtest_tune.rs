//! Backtest Tune Results Screen (T-3.7)
//!
//! TUI screen for displaying backtest tune command results (grid search).
//! Supports multiple view modes: TopResults, FullTable, Heatmap, Pareto.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{TuneResult, TuneResultItem};
use crate::ui::widgets::{
    MetricsDashboardWidget, Metric, MetricValue, MetricFormat,
    TableWidget, TableHeader, TableRow,
    ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig,
    ParetoFrontierWidget, ParetoSolution,
};
use serde_json;

// ============================================================================
// Types
// ============================================================================

/// View mode for tune results display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuneViewMode {
    /// Top 10 results table
    TopResults,
    /// Full table with all results (sortable)
    FullTable,
    /// Heatmap visualization (spread vs skew)
    Heatmap,
    /// Pareto frontier view (if multi-objective)
    Pareto,
}

impl TuneViewMode {
    /// Get all view modes
    pub fn all() -> Vec<TuneViewMode> {
        vec![
            TuneViewMode::TopResults,
            TuneViewMode::FullTable,
            TuneViewMode::Heatmap,
            TuneViewMode::Pareto,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            TuneViewMode::TopResults => "Top Results",
            TuneViewMode::FullTable => "Full Table",
            TuneViewMode::Heatmap => "Heatmap",
            TuneViewMode::Pareto => "Pareto",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> TuneViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> TuneViewMode {
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

/// Backtest tune results screen
pub struct BacktestTuneResultsScreen {
    /// Tune result data
    tune_result: TuneResult,
    /// Current view mode
    view_mode: TuneViewMode,
    /// Selected result index (for FullTable view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestTuneResultsScreen {
    /// Create a new results screen from TuneResult
    pub fn new(tune_result: TuneResult) -> Self {
        Self {
            tune_result,
            view_mode: TuneViewMode::TopResults,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: TuneViewMode) {
        self.view_mode = mode;
    }

    /// Get current view mode
    pub fn view_mode(&self) -> TuneViewMode {
        self.view_mode
    }

    /// Set selected result index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.tune_result.all_results.len() {
                self.selected_index = Some(idx);
            }
        } else {
            self.selected_index = None;
        }
    }

    /// Get selected result index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Get selected result
    pub fn selected_result(&self) -> Option<&TuneResultItem> {
        self.selected_index.and_then(|idx| self.tune_result.all_results.get(idx))
    }

    /// Set focus state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Get focus state
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Handle keyboard input
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !self.focused {
            return false;
        }

        match key.code {
            KeyCode::Tab | KeyCode::Char('n') => {
                self.view_mode = self.view_mode.next();
                true
            }
            KeyCode::BackTab | KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.view_mode = self.view_mode.previous();
                true
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == TuneViewMode::FullTable {
                    if let Some(idx) = self.selected_index {
                        if idx > 0 {
                            self.selected_index = Some(idx - 1);
                        }
                    } else if !self.tune_result.all_results.is_empty() {
                        self.selected_index = Some(0);
                    }
                }
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.view_mode == TuneViewMode::FullTable {
                    if let Some(idx) = self.selected_index {
                        if idx + 1 < self.tune_result.all_results.len() {
                            self.selected_index = Some(idx + 1);
                        }
                    } else if !self.tune_result.all_results.is_empty() {
                        self.selected_index = Some(0);
                    }
                }
                true
            }
            KeyCode::Char('e') => {
                // Export functionality (placeholder)
                self.export_path = Some("export.json".to_string());
                true
            }
            _ => false,
        }
    }

    /// Render the screen
    pub fn render(&self, f: &mut Frame, area: Rect) {
        // Title bar
        let title = format!("Tune Results: {} ({} combinations)", 
            self.tune_result.algorithm_name,
            self.tune_result.total_combinations);

        let title_block = Block::default()
            .borders(Borders::ALL)
            .title(title.as_str())
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

        let inner = title_block.inner(area);
        title_block.render(area, f.buffer_mut());

        if inner.width < 2 || inner.height < 2 {
            return;
        }

        // View mode tabs
        let tabs_height = 3;
        let tabs_area = Rect {
            x: inner.x,
            y: inner.y,
            width: inner.width,
            height: tabs_height,
        };

        let content_area = Rect {
            x: inner.x,
            y: inner.y + tabs_height,
            width: inner.width,
            height: inner.height.saturating_sub(tabs_height),
        };

        self.render_tabs(f, tabs_area);
        self.render_content(f, content_area);
    }

    /// Render view mode tabs
    fn render_tabs(&self, f: &mut Frame, area: Rect) {
        let tabs: Vec<&str> = TuneViewMode::all()
            .iter()
            .map(|m| m.name())
            .collect();

        let selected = TuneViewMode::all()
            .iter()
            .position(|m| *m == self.view_mode)
            .unwrap_or(0);

        let tabs_widget = Tabs::new(tabs)
            .block(Block::default().borders(Borders::BOTTOM))
            .select(selected)
            .style(Style::default().fg(Color::White))
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            );

        tabs_widget.render(area, f.buffer_mut());
    }

    /// Render content based on view mode
    fn render_content(&self, f: &mut Frame, area: Rect) {
        match self.view_mode {
            TuneViewMode::TopResults => self.render_top_results(f, area),
            TuneViewMode::FullTable => self.render_full_table(f, area),
            TuneViewMode::Heatmap => self.render_heatmap(f, area),
            TuneViewMode::Pareto => self.render_pareto(f, area),
        }
    }

    /// Render top results view
    fn render_top_results(&self, f: &mut Frame, area: Rect) {
        let top_10: Vec<&TuneResultItem> = self.tune_result.all_results
            .iter()
            .take(10)
            .collect();

        if top_10.is_empty() {
            let paragraph = Paragraph::new("No results available")
                .style(Style::default().fg(Color::Red))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
            return;
        }

        let (headers, rows) = self.create_results_table(&top_10);
        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title("Top 10 Results"));
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render full table view
    fn render_full_table(&self, f: &mut Frame, area: Rect) {
        if self.tune_result.all_results.is_empty() {
            let paragraph = Paragraph::new("No results available")
                .style(Style::default().fg(Color::Red))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
            return;
        }

        let all: Vec<&TuneResultItem> = self.tune_result.all_results.iter().collect();
        let (headers, rows) = self.create_results_table(&all);
        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows)
            .with_block(Block::default().borders(Borders::ALL).title("All Results"));
        table.set_focused(self.focused);
        
        // Note: TableWidget handles its own selection via handle_key
        // The selected_index is tracked separately for this screen
        table.render(area, f.buffer_mut());
    }

    /// Render heatmap view
    fn render_heatmap(&self, f: &mut Frame, area: Rect) {
        if self.tune_result.all_results.is_empty() {
            let paragraph = Paragraph::new("No results available")
                .style(Style::default().fg(Color::Red))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
            return;
        }

        let series = self.create_heatmap_series();
        if !series.points.is_empty() {
            let mut chart = ChartWidget::new();
            chart.add_series(series);
            chart = chart
                .with_chart_type(ChartType::Heatmap)
                .with_block(Block::default().borders(Borders::ALL).title("Spread vs Skew Heatmap (Sharpe Ratio)"))
                .with_x_axis(AxisConfig {
                    label: Some("Spread".to_string()),
                    min: None,
                    max: None,
                    show_grid: true,
                    ticks: 5,
                })
                .with_y_axis(AxisConfig {
                    label: Some("Skew".to_string()),
                    min: None,
                    max: None,
                    show_grid: true,
                    ticks: 5,
                });

            chart.render(area, f.buffer_mut());
        } else {
            let paragraph = Paragraph::new("No data for heatmap")
                .style(Style::default().fg(Color::Yellow))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
        }
    }

    /// Render Pareto view
    fn render_pareto(&self, f: &mut Frame, area: Rect) {
        if self.tune_result.all_results.is_empty() {
            let paragraph = Paragraph::new("No results available")
                .style(Style::default().fg(Color::Red))
                .alignment(Alignment::Center);
            paragraph.render(area, f.buffer_mut());
            return;
        }

        let mut widget = self.create_pareto_widget();
        widget.set_focused(self.focused);
        widget.render(area, f.buffer_mut());
    }

    /// Create results table (headers and rows)
    pub fn create_results_table(&self, results: &[&TuneResultItem]) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Rank".to_string())
                .with_width(6)
                .with_sortable(true),
            TableHeader::new("Spread".to_string())
                .with_width(10)
                .with_sortable(true),
            TableHeader::new("Skew".to_string())
                .with_width(10)
                .with_sortable(true),
            TableHeader::new("Sharpe".to_string())
                .with_width(10)
                .with_sortable(true),
            TableHeader::new("Return".to_string())
                .with_width(10)
                .with_sortable(true),
            TableHeader::new("Drawdown".to_string())
                .with_width(12)
                .with_sortable(true),
            TableHeader::new("Trades".to_string())
                .with_width(8)
                .with_sortable(true),
            TableHeader::new("Win Rate".to_string())
                .with_width(10)
                .with_sortable(true),
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
    pub fn create_heatmap_series(&self) -> DataSeries {
        let points: Vec<DataPoint> = self.tune_result.all_results
            .iter()
            .map(|item| DataPoint {
                x: item.spread,
                y: item.skew,
                label: Some(format!("{:.4}", item.sharpe)),
            })
            .collect();

        DataSeries {
            name: "Sharpe Ratio".to_string(),
            points,
            color: Some(Color::Green),
            symbol: None,
        }
    }

    /// Create Pareto frontier widget
    pub fn create_pareto_widget(&self) -> ParetoFrontierWidget {
        let mut widget = ParetoFrontierWidget::new()
            .with_objective_names(vec!["Sharpe Ratio".to_string(), "Total Return".to_string(), "Max Drawdown".to_string()])
            .with_show_frontier(true)
            .with_show_all_solutions(true);

        for (idx, item) in self.tune_result.all_results.iter().enumerate() {
            let solution = ParetoSolution::new(
                format!("R{}", idx + 1),
                vec![
                    item.sharpe,
                    item.total_return,
                    -item.max_drawdown, // Negate drawdown so higher is better
                ],
            )
            .with_metadata(serde_json::json!({
                "spread": item.spread,
                "skew": item.skew,
                "sharpe": item.sharpe
            }));

            widget.add_solution(solution);
        }

        // Frontier is automatically updated when solutions are added via add_solution()
        widget
    }

    /// Get top N results
    pub fn top_results(&self, n: usize) -> Vec<&TuneResultItem> {
        self.tune_result.all_results
            .iter()
            .take(n)
            .collect()
    }

    /// Get best result
    pub fn best_result(&self) -> Option<&TuneResultItem> {
        self.tune_result.best.as_ref()
            .or_else(|| self.tune_result.all_results.first())
    }

    /// Export results to JSON
    pub fn export_to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string_pretty(&self.tune_result)
            .map_err(|e| anyhow::anyhow!("Failed to serialize results: {}", e))
    }

    /// Get tune result
    pub fn tune_result(&self) -> &TuneResult {
        &self.tune_result
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_tune_result() -> TuneResult {
        let mut all_results = Vec::new();
        for i in 0..20 {
            all_results.push(TuneResultItem {
                spread: 1.0 + i as f64 * 0.1,
                skew: 0.5 + i as f64 * 0.05,
                high_entropy_threshold: 0.7,
                fill_prob: 0.8,
                sharpe: 2.0 - i as f64 * 0.1, // Decreasing Sharpe
                total_return: 0.15 - i as f64 * 0.01,
                max_drawdown: -0.05 - i as f64 * 0.005,
                num_trades: 100 - i * 5,
                win_rate: 0.55 - i as f64 * 0.01,
                avg_trade_pnl: 0.001 - i as f64 * 0.0001,
            });
        }

        TuneResult {
            algorithm: "AvellanedaStoikov".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_results,
            best: None,
            total_combinations: 20,
        }
    }

    #[test]
    fn test_tune_view_mode_all() {
        let modes = TuneViewMode::all();
        assert_eq!(modes.len(), 4);
        assert!(modes.contains(&TuneViewMode::TopResults));
        assert!(modes.contains(&TuneViewMode::FullTable));
        assert!(modes.contains(&TuneViewMode::Heatmap));
        assert!(modes.contains(&TuneViewMode::Pareto));
    }

    #[test]
    fn test_tune_view_mode_name() {
        assert_eq!(TuneViewMode::TopResults.name(), "Top Results");
        assert_eq!(TuneViewMode::FullTable.name(), "Full Table");
        assert_eq!(TuneViewMode::Heatmap.name(), "Heatmap");
        assert_eq!(TuneViewMode::Pareto.name(), "Pareto");
    }

    #[test]
    fn test_tune_view_mode_next() {
        assert_eq!(TuneViewMode::TopResults.next(), TuneViewMode::FullTable);
        assert_eq!(TuneViewMode::FullTable.next(), TuneViewMode::Heatmap);
        assert_eq!(TuneViewMode::Heatmap.next(), TuneViewMode::Pareto);
        assert_eq!(TuneViewMode::Pareto.next(), TuneViewMode::TopResults);
    }

    #[test]
    fn test_tune_view_mode_previous() {
        assert_eq!(TuneViewMode::TopResults.previous(), TuneViewMode::Pareto);
        assert_eq!(TuneViewMode::FullTable.previous(), TuneViewMode::TopResults);
        assert_eq!(TuneViewMode::Heatmap.previous(), TuneViewMode::FullTable);
        assert_eq!(TuneViewMode::Pareto.previous(), TuneViewMode::Heatmap);
    }

    #[test]
    fn test_screen_creation() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);
        assert!(screen.is_focused());
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_set_view_mode() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);

        screen.set_view_mode(TuneViewMode::FullTable);
        assert_eq!(screen.view_mode(), TuneViewMode::FullTable);

        screen.set_view_mode(TuneViewMode::Heatmap);
        assert_eq!(screen.view_mode(), TuneViewMode::Heatmap);
    }

    #[test]
    fn test_set_selected_index() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        assert_eq!(screen.selected_index(), None);

        screen.set_selected_index(Some(5));
        assert_eq!(screen.selected_index(), Some(5));

        screen.set_selected_index(Some(100)); // Out of bounds
        assert_eq!(screen.selected_index(), None); // Should be clamped or None

        screen.set_selected_index(None);
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_selected_result() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        assert!(screen.selected_result().is_none());
        
        screen.set_selected_index(Some(0));
        assert!(screen.selected_result().is_some());
        assert_eq!(screen.selected_result().unwrap().spread, 1.0);
    }

    #[test]
    fn test_set_focused() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        assert!(screen.is_focused());

        screen.set_focused(false);
        assert!(!screen.is_focused());

        screen.set_focused(true);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_handle_key_tab() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);

        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), TuneViewMode::FullTable);
    }

    #[test]
    fn test_handle_key_n() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);

        let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), TuneViewMode::FullTable);
    }

    #[test]
    fn test_handle_key_shift_tab() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);

        let key = KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT);
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);
    }

    #[test]
    fn test_handle_key_not_focused() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_focused(false);

        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(!screen.handle_key(key));
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);
    }

    #[test]
    fn test_handle_key_up_down() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);

        // Down key
        let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(screen.handle_key(key_down));
        assert_eq!(screen.selected_index(), Some(0));

        // Down again
        assert!(screen.handle_key(key_down));
        assert_eq!(screen.selected_index(), Some(1));

        // Up key
        let key_up = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(screen.handle_key(key_up));
        assert_eq!(screen.selected_index(), Some(0));

        // Up at top
        assert!(screen.handle_key(key_up));
        assert_eq!(screen.selected_index(), Some(0)); // Should stay at 0
    }

    #[test]
    fn test_handle_key_j_k() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);

        let key_j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::empty());
        assert!(screen.handle_key(key_j));
        assert_eq!(screen.selected_index(), Some(0));

        let key_k = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::empty());
        assert!(screen.handle_key(key_k));
        assert_eq!(screen.selected_index(), Some(0));
    }

    #[test]
    fn test_handle_key_export() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);

        let key = KeyEvent::new(KeyCode::Char('e'), KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert!(screen.export_path.is_some());
    }

    #[test]
    fn test_create_results_table() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let top_5: Vec<&TuneResultItem> = screen.top_results(5);
        let (headers, rows) = screen.create_results_table(&top_5);

        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 5);
        assert_eq!(headers[0].name, "Rank");
        assert_eq!(headers[1].name, "Spread");
        assert_eq!(rows[0].cells()[0], "1");
    }

    #[test]
    fn test_create_heatmap_series() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();

        assert_eq!(series.name, "Sharpe Ratio");
        assert_eq!(series.points.len(), 20);
        assert_eq!(series.points[0].x, 1.0);
        assert_eq!(series.points[0].y, 0.5);
    }

    #[test]
    fn test_create_pareto_widget() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();

        assert_eq!(widget.solution_count(), 20);
        assert!(widget.objective_names.len() >= 3);
    }

    #[test]
    fn test_top_results() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let top_10 = screen.top_results(10);

        assert_eq!(top_10.len(), 10);
        assert_eq!(top_10[0].spread, 1.0);
    }

    #[test]
    fn test_top_results_more_than_available() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let top_100 = screen.top_results(100);

        assert_eq!(top_100.len(), 20); // Only 20 available
    }

    #[test]
    fn test_best_result() {
        let mut tune_result = create_test_tune_result();
        tune_result.best = Some(tune_result.all_results[0].clone());
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let best = screen.best_result();

        assert!(best.is_some());
        assert_eq!(best.unwrap().spread, 1.0);
    }

    #[test]
    fn test_best_result_no_best() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let best = screen.best_result();

        assert!(best.is_some()); // Should return first result if no best set
        assert_eq!(best.unwrap().spread, 1.0);
    }

    #[test]
    fn test_best_result_empty() {
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: Vec::new(),
            best: None,
            total_combinations: 0,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let best = screen.best_result();

        assert!(best.is_none());
    }

    #[test]
    fn test_export_to_json() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json();

        assert!(json.is_ok());
        let json_str = json.unwrap();
        assert!(json_str.contains("Avellaneda-Stoikov"));
        assert!(json_str.contains("all_results"));
        assert!(json_str.contains("sharpe"));
    }

    #[test]
    fn test_tune_result_accessor() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let result = screen.tune_result();

        assert_eq!(result.algorithm, "AvellanedaStoikov");
        assert_eq!(result.total_combinations, 20);
    }

    #[test]
    fn test_view_mode_equality() {
        assert_eq!(TuneViewMode::TopResults, TuneViewMode::TopResults);
        assert_ne!(TuneViewMode::TopResults, TuneViewMode::FullTable);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = TuneViewMode::TopResults;
        for _ in 0..8 {
            mode = mode.next();
        }
        // After 8 cycles (2 full cycles), should be back to starting point
        assert_eq!(mode, TuneViewMode::TopResults);
    }

    #[test]
    fn test_view_mode_reverse_cycle() {
        let mut mode = TuneViewMode::TopResults;
        for _ in 0..8 {
            mode = mode.previous();
        }
        // After 8 cycles (2 full cycles), should be back to starting point
        assert_eq!(mode, TuneViewMode::TopResults);
    }

    #[test]
    fn test_selected_index_bounds() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Set to valid index
        screen.set_selected_index(Some(10));
        assert_eq!(screen.selected_index(), Some(10));
        
        // Set to out of bounds
        screen.set_selected_index(Some(100));
        // Should be None or clamped
        assert!(screen.selected_index().is_none() || screen.selected_index().unwrap() < 20);
    }

    #[test]
    fn test_handle_key_up_down_not_full_table() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::TopResults);

        let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(screen.handle_key(key_down));
        // Should not set selected_index in TopResults mode
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_create_results_table_empty() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let empty: Vec<&TuneResultItem> = Vec::new();
        let (headers, rows) = screen.create_results_table(&empty);

        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_create_heatmap_series_empty() {
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: Vec::new(),
            best: None,
            total_combinations: 0,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();

        assert_eq!(series.points.len(), 0);
    }

    #[test]
    fn test_create_pareto_widget_empty() {
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: Vec::new(),
            best: None,
            total_combinations: 0,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();

        assert_eq!(widget.solution_count(), 0);
    }

    // ============================================================================
    // Additional Comprehensive Tests
    // ============================================================================

    #[test]
    fn test_screen_with_best_result() {
        let mut tune_result = create_test_tune_result();
        tune_result.best = Some(tune_result.all_results[0].clone());
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let best = screen.best_result();
        assert!(best.is_some());
        assert_eq!(best.unwrap().spread, 1.0);
    }

    #[test]
    fn test_screen_with_large_dataset() {
        let mut all_results = Vec::new();
        for i in 0..1000 {
            all_results.push(TuneResultItem {
                spread: 1.0 + i as f64 * 0.01,
                skew: 0.5 + i as f64 * 0.005,
                high_entropy_threshold: 0.7,
                fill_prob: 0.8,
                sharpe: 2.0 - i as f64 * 0.001,
                total_return: 0.15 - i as f64 * 0.0001,
                max_drawdown: -0.05 - i as f64 * 0.00005,
                num_trades: 100 - i / 10,
                win_rate: 0.55 - i as f64 * 0.0001,
                avg_trade_pnl: 0.001 - i as f64 * 0.000001,
            });
        }
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1000,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        assert_eq!(screen.tune_result().total_combinations, 1000);
        let top_10 = screen.top_results(10);
        assert_eq!(top_10.len(), 10);
    }

    #[test]
    fn test_create_results_table_single_item() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let single: Vec<&TuneResultItem> = vec![&screen.tune_result().all_results[0]];
        let (headers, rows) = screen.create_results_table(&single);
        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cells()[0], "1"); // Rank
    }

    #[test]
    fn test_create_results_table_all_columns() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (headers, rows) = screen.create_results_table(&all);
        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 20);
        // Verify all columns have data
        for row in &rows {
            assert_eq!(row.cells().len(), 8);
        }
    }

    #[test]
    fn test_heatmap_series_data_correctness() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        assert_eq!(series.points.len(), 20);
        // Verify first point
        assert_eq!(series.points[0].x, 1.0);
        assert_eq!(series.points[0].y, 0.5);
        assert!(series.points[0].label.is_some());
        // Verify last point
        assert_eq!(series.points[19].x, 2.9);
        assert_eq!(series.points[19].y, 1.45);
    }

    #[test]
    fn test_pareto_widget_objectives() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        assert_eq!(widget.objective_names.len(), 3);
        assert_eq!(widget.objective_names[0], "Sharpe Ratio");
        assert_eq!(widget.objective_names[1], "Total Return");
        assert_eq!(widget.objective_names[2], "Max Drawdown");
    }

    #[test]
    fn test_pareto_widget_solutions_count() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        assert_eq!(widget.solution_count(), 20);
        // Verify solutions have correct objective count
        for i in 0..widget.solution_count() {
            let solution = widget.solutions.get(i).unwrap();
            assert_eq!(solution.objective_count(), 3);
        }
    }

    #[test]
    fn test_pareto_widget_solution_metadata() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        let first_solution = widget.solutions.get(0).unwrap();
        assert!(first_solution.metadata.is_some());
        let metadata = first_solution.metadata.as_ref().unwrap();
        assert!(metadata.contains("Spread="));
        assert!(metadata.contains("Skew="));
        assert!(metadata.contains("Sharpe="));
    }

    #[test]
    fn test_handle_key_all_modes() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        for mode in TuneViewMode::all() {
            screen.set_view_mode(mode);
            let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
            assert!(screen.handle_key(key));
        }
    }

    #[test]
    fn test_handle_key_arrow_keys_in_full_table() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);
        
        // Down arrow
        let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(screen.handle_key(key_down));
        assert_eq!(screen.selected_index(), Some(0));
        
        // Down again
        assert!(screen.handle_key(key_down));
        assert_eq!(screen.selected_index(), Some(1));
        
        // Up arrow
        let key_up = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(screen.handle_key(key_up));
        assert_eq!(screen.selected_index(), Some(0));
    }

    #[test]
    fn test_handle_key_arrow_keys_not_in_full_table() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::TopResults);
        
        let key_down = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(screen.handle_key(key_down));
        // Should not set selection in TopResults mode
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_handle_key_j_k_in_full_table() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);
        
        let key_j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::empty());
        assert!(screen.handle_key(key_j));
        assert_eq!(screen.selected_index(), Some(0));
        
        let key_k = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::empty());
        assert!(screen.handle_key(key_k));
        assert_eq!(screen.selected_index(), Some(0)); // At top, stays at 0
    }

    #[test]
    fn test_selected_index_bounds_checking() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Valid index
        screen.set_selected_index(Some(10));
        assert_eq!(screen.selected_index(), Some(10));
        
        // Exactly at boundary
        screen.set_selected_index(Some(19));
        assert_eq!(screen.selected_index(), Some(19));
        
        // Out of bounds
        screen.set_selected_index(Some(20));
        assert_eq!(screen.selected_index(), None);
        
        // Way out of bounds
        screen.set_selected_index(Some(1000));
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_selected_result_bounds() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Valid selection
        screen.set_selected_index(Some(5));
        let result = screen.selected_result();
        assert!(result.is_some());
        assert_eq!(result.unwrap().spread, 1.5);
        
        // Last item
        screen.set_selected_index(Some(19));
        let result = screen.selected_result();
        assert!(result.is_some());
        
        // Invalid selection
        screen.set_selected_index(Some(100));
        let result = screen.selected_result();
        assert!(result.is_none());
    }

    #[test]
    fn test_top_results_various_n() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        
        assert_eq!(screen.top_results(0).len(), 0);
        assert_eq!(screen.top_results(1).len(), 1);
        assert_eq!(screen.top_results(5).len(), 5);
        assert_eq!(screen.top_results(10).len(), 10);
        assert_eq!(screen.top_results(20).len(), 20);
        assert_eq!(screen.top_results(100).len(), 20); // Only 20 available
    }

    #[test]
    fn test_top_results_ordering() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let top_5 = screen.top_results(5);
        
        // Results should be sorted by Sharpe (descending)
        for i in 0..top_5.len().saturating_sub(1) {
            assert!(top_5[i].sharpe >= top_5[i + 1].sharpe);
        }
    }

    #[test]
    fn test_export_json_structure() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json().unwrap();
        
        // Verify JSON contains expected fields
        assert!(json.contains("\"algorithm\""));
        assert!(json.contains("\"algorithm_name\""));
        assert!(json.contains("\"all_results\""));
        assert!(json.contains("\"total_combinations\""));
        assert!(json.contains("Avellaneda-Stoikov"));
    }

    #[test]
    fn test_export_json_all_results() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json().unwrap();
        
        // Verify all results are in JSON
        assert!(json.contains("\"spread\""));
        assert!(json.contains("\"skew\""));
        assert!(json.contains("\"sharpe\""));
        assert!(json.contains("\"total_return\""));
    }

    #[test]
    fn test_create_results_table_formatting() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // Verify formatting
        let first_row = &rows[0];
        let cells = first_row.cells();
        assert_eq!(cells[0], "1"); // Rank
        assert!(cells[1].contains(".")); // Spread (formatted)
        assert!(cells[2].contains(".")); // Skew (formatted)
        assert!(cells[3].contains(".")); // Sharpe (formatted)
        assert!(cells[4].contains("%")); // Return (percentage)
        assert!(cells[5].contains("%")); // Drawdown (percentage)
        assert!(!cells[6].is_empty()); // Trades
        assert!(cells[7].contains("%")); // Win rate (percentage)
    }

    #[test]
    fn test_heatmap_series_labels() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // All points should have labels
        for point in &series.points {
            assert!(point.label.is_some());
            let label = point.label.as_ref().unwrap();
            assert!(label.contains(".")); // Should contain decimal
        }
    }

    #[test]
    fn test_pareto_widget_frontier_calculation() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Widget should have calculated frontier
        let frontier = widget.get_frontier_solutions_sorted();
        assert!(!frontier.is_empty());
    }

    #[test]
    fn test_pareto_widget_solution_ids() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Verify solution IDs
        for i in 0..widget.solution_count() {
            let solution = widget.solutions.get(i).unwrap();
            assert_eq!(solution.id, format!("R{}", i + 1));
        }
    }

    #[test]
    fn test_pareto_widget_objective_values() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Verify first solution objectives
        let first_solution = widget.solutions.get(0).unwrap();
        assert_eq!(first_solution.get_objective(0), Some(2.0)); // Sharpe
        assert_eq!(first_solution.get_objective(1), Some(0.15)); // Return
        assert_eq!(first_solution.get_objective(2), Some(0.05)); // Negated drawdown
    }

    #[test]
    fn test_view_mode_cycle_completeness() {
        let mut mode = TuneViewMode::TopResults;
        let mut visited = std::collections::HashSet::new();
        
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        
        // Should have visited all modes
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_view_mode_reverse_cycle_completeness() {
        let mut mode = TuneViewMode::TopResults;
        let mut visited = std::collections::HashSet::new();
        
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.previous();
        }
        
        // Should have visited all modes
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_screen_tune_result_immutability() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let result1 = screen.tune_result();
        let result2 = screen.tune_result();
        
        // Should return same reference
        assert_eq!(result1.algorithm, result2.algorithm);
        assert_eq!(result1.total_combinations, result2.total_combinations);
    }

    #[test]
    fn test_create_results_table_rank_numbering() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // Verify ranks are sequential
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.cells()[0], format!("{}", i + 1));
        }
    }

    #[test]
    fn test_handle_key_invalid_keys() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        let invalid_keys = vec![
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::empty()),
            KeyEvent::new(KeyCode::Char('z'), KeyModifiers::empty()),
            KeyEvent::new(KeyCode::F(1), KeyModifiers::empty()),
            KeyEvent::new(KeyCode::Esc, KeyModifiers::empty()),
        ];
        
        for key in invalid_keys {
            let original_mode = screen.view_mode();
            assert!(!screen.handle_key(key));
            assert_eq!(screen.view_mode(), original_mode);
        }
    }

    #[test]
    fn test_handle_key_selection_at_bounds() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);
        
        // Move to last item
        for _ in 0..20 {
            let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
            screen.handle_key(key);
        }
        assert_eq!(screen.selected_index(), Some(19));
        
        // Try to go beyond
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.selected_index(), Some(19)); // Should stay at last
    }

    #[test]
    fn test_create_heatmap_series_unique_points() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // All points should be unique (different spread/skew combinations)
        let mut points_set = std::collections::HashSet::new();
        for point in &series.points {
            let key = (point.x, point.y);
            assert!(points_set.insert(key), "Duplicate point found: {:?}", key);
        }
    }

    #[test]
    fn test_pareto_widget_update_frontier() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Verify frontier is updated
        let frontier_count = widget.get_frontier_solutions_sorted().len();
        assert!(frontier_count > 0);
        assert!(frontier_count <= widget.solution_count());
    }

    #[test]
    fn test_export_path_setting() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        let key = KeyEvent::new(KeyCode::Char('e'), KeyModifiers::empty());
        screen.handle_key(key);
        
        assert!(screen.export_path.is_some());
        assert_eq!(screen.export_path.as_ref().unwrap(), "export.json");
    }

    #[test]
    fn test_screen_with_empty_results() {
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: Vec::new(),
            best: None,
            total_combinations: 0,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        
        assert_eq!(screen.top_results(10).len(), 0);
        assert!(screen.best_result().is_none());
        assert_eq!(screen.create_results_table(&[]).1.len(), 0);
        assert_eq!(screen.create_heatmap_series().points.len(), 0);
        assert_eq!(screen.create_pareto_widget().solution_count(), 0);
    }

    #[test]
    fn test_screen_with_single_result() {
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: vec![TuneResultItem {
                spread: 1.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.8,
                sharpe: 2.0,
                total_return: 0.15,
                max_drawdown: -0.05,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
            }],
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        
        assert_eq!(screen.top_results(10).len(), 1);
        assert!(screen.best_result().is_some());
        assert_eq!(screen.create_results_table(&screen.tune_result().all_results.iter().collect::<Vec<_>>()).1.len(), 1);
    }

    #[test]
    fn test_create_results_table_negative_values() {
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: -1.0,
            skew: -0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: -2.0,
            total_return: -0.15,
            max_drawdown: -0.05,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: -0.001,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        assert_eq!(rows.len(), 1);
        // Should handle negative values correctly
        assert!(rows[0].cells()[1].contains("-")); // Negative spread
    }

    #[test]
    fn test_create_results_table_zero_values() {
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: 0.0,
            skew: 0.0,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        assert_eq!(rows.len(), 1);
        // Should handle zero values correctly
        assert_eq!(rows[0].cells()[6], "0"); // Zero trades
    }

    #[test]
    fn test_create_results_table_very_large_values() {
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: 1000.0,
            skew: 500.0,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: 100.0,
            total_return: 10.0,
            max_drawdown: -5.0,
            num_trades: 1000000,
            win_rate: 1.0,
            avg_trade_pnl: 1000.0,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        assert_eq!(rows.len(), 1);
        // Should format large values correctly
        assert_eq!(rows[0].cells()[6], "1000000"); // Large number of trades
    }

    #[test]
    fn test_heatmap_series_extreme_values() {
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: f64::MAX,
            skew: f64::MIN,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: 0.0,
            total_return: 0.0,
            max_drawdown: 0.0,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // Should handle extreme values without panicking
        assert_eq!(series.points.len(), 1);
    }

    #[test]
    fn test_pareto_widget_with_extreme_objectives() {
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: 1.0,
            skew: 0.5,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: f64::INFINITY,
            total_return: f64::NEG_INFINITY,
            max_drawdown: f64::NAN,
            num_trades: 0,
            win_rate: 0.0,
            avg_trade_pnl: 0.0,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Should handle extreme values without panicking
        assert_eq!(widget.solution_count(), 1);
    }

    #[test]
    fn test_view_mode_switching_preserves_state() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_selected_index(Some(5));
        
        // Switch modes
        screen.set_view_mode(TuneViewMode::Heatmap);
        assert_eq!(screen.view_mode(), TuneViewMode::Heatmap);
        // Selected index should be preserved
        assert_eq!(screen.selected_index(), Some(5));
        
        screen.set_view_mode(TuneViewMode::Pareto);
        assert_eq!(screen.view_mode(), TuneViewMode::Pareto);
        assert_eq!(screen.selected_index(), Some(5));
    }

    #[test]
    fn test_handle_key_multiple_rapid_presses() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Rapidly press Tab multiple times
        for _ in 0..10 {
            let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
            screen.handle_key(key);
        }
        
        // Should cycle through all modes
        let modes_visited = std::collections::HashSet::new();
        // After 10 presses (2.5 cycles), should be at a valid mode
        assert!(TuneViewMode::all().contains(&screen.view_mode()));
    }

    #[test]
    fn test_create_results_table_unicode_safety() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // All cell values should be valid strings
        for row in &rows {
            for cell in row.cells() {
                // Should not panic when converting to string
                let _ = cell.to_string();
            }
        }
    }

    #[test]
    fn test_export_json_valid_json() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json().unwrap();
        
        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_object());
        assert!(parsed.get("algorithm").is_some());
        assert!(parsed.get("all_results").is_some());
    }

    #[test]
    fn test_export_json_round_trip() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json().unwrap();
        
        // Should be able to deserialize back
        let deserialized: TuneResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.algorithm, tune_result.algorithm);
        assert_eq!(deserialized.all_results.len(), tune_result.all_results.len());
    }

    #[test]
    fn test_tune_result_accessor_consistency() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result.clone());
        
        let result1 = screen.tune_result();
        let result2 = screen.tune_result();
        
        // Should return consistent data
        assert_eq!(result1.algorithm, result2.algorithm);
        assert_eq!(result1.total_combinations, result2.total_combinations);
        assert_eq!(result1.all_results.len(), result2.all_results.len());
    }

    #[test]
    fn test_create_results_table_header_sortability() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (headers, _) = screen.create_results_table(&all);
        
        // All headers should be sortable
        for header in &headers {
            assert!(header.sortable());
        }
    }

    #[test]
    fn test_create_results_table_header_widths() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (headers, _) = screen.create_results_table(&all);
        
        // All headers should have widths set
        for header in &headers {
            assert!(header.width().is_some());
        }
    }

    #[test]
    fn test_heatmap_series_color() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // Series should have color set
        assert!(series.color.is_some());
        assert_eq!(series.color.unwrap(), Color::Green);
    }

    #[test]
    fn test_pareto_widget_show_settings() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Widget should have show settings configured
        assert!(widget.show_frontier);
        assert!(widget.show_all_solutions);
    }

    #[test]
    fn test_screen_focus_state_persistence() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        screen.set_focused(false);
        assert!(!screen.is_focused());
        
        // Change view mode
        screen.set_view_mode(TuneViewMode::FullTable);
        assert!(!screen.is_focused()); // Should persist
        
        screen.set_focused(true);
        assert!(screen.is_focused());
    }

    #[test]
    fn test_handle_key_focus_requirement() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_focused(false);
        
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        let handled = screen.handle_key(key);
        
        assert!(!handled);
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults); // Unchanged
    }

    #[test]
    fn test_create_results_table_performance_large() {
        let mut all_results = Vec::new();
        for i in 0..10000 {
            all_results.push(TuneResultItem {
                spread: 1.0 + i as f64 * 0.001,
                skew: 0.5 + i as f64 * 0.0005,
                high_entropy_threshold: 0.7,
                fill_prob: 0.8,
                sharpe: 2.0 - i as f64 * 0.0001,
                total_return: 0.15 - i as f64 * 0.00001,
                max_drawdown: -0.05 - i as f64 * 0.000005,
                num_trades: 100 - i / 100,
                win_rate: 0.55 - i as f64 * 0.00001,
                avg_trade_pnl: 0.001 - i as f64 * 0.0000001,
            });
        }
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 10000,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (headers, rows) = screen.create_results_table(&all);
        
        // Should handle large datasets
        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 10000);
    }

    #[test]
    fn test_pareto_widget_large_dataset() {
        let mut all_results = Vec::new();
        for i in 0..500 {
            all_results.push(TuneResultItem {
                spread: 1.0 + i as f64 * 0.01,
                skew: 0.5 + i as f64 * 0.005,
                high_entropy_threshold: 0.7,
                fill_prob: 0.8,
                sharpe: 2.0 - i as f64 * 0.001,
                total_return: 0.15 - i as f64 * 0.0001,
                max_drawdown: -0.05 - i as f64 * 0.00005,
                num_trades: 100 - i / 5,
                win_rate: 0.55 - i as f64 * 0.0001,
                avg_trade_pnl: 0.001 - i as f64 * 0.000001,
            });
        }
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 500,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Should handle large datasets
        assert_eq!(widget.solution_count(), 500);
    }

    #[test]
    fn test_create_heatmap_series_all_points_have_labels() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // Every point should have a label
        for point in &series.points {
            assert!(point.label.is_some());
            let label = point.label.as_ref().unwrap();
            // Label should be a valid number string
            assert!(!label.is_empty());
        }
    }

    #[test]
    fn test_pareto_widget_solution_objectives_match() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Verify solution objectives match original data
        for (i, solution) in widget.solutions.iter().enumerate() {
            let original = &screen.tune_result().all_results[i];
            assert_eq!(solution.get_objective(0), Some(original.sharpe));
            assert_eq!(solution.get_objective(1), Some(original.total_return));
            assert_eq!(solution.get_objective(2), Some(-original.max_drawdown));
        }
    }

    #[test]
    fn test_view_mode_equality_and_hashing() {
        let mode1 = TuneViewMode::TopResults;
        let mode2 = TuneViewMode::TopResults;
        let mode3 = TuneViewMode::FullTable;
        
        assert_eq!(mode1, mode2);
        assert_ne!(mode1, mode3);
    }

    #[test]
    fn test_screen_clone_behavior() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Access tune_result multiple times
        let result1 = screen.tune_result();
        let result2 = screen.tune_result();
        
        // Should return same data
        assert_eq!(result1.algorithm, result2.algorithm);
    }

    #[test]
    fn test_create_results_table_empty_headers() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let empty: Vec<&TuneResultItem> = Vec::new();
        let (headers, rows) = screen.create_results_table(&empty);
        
        // Headers should still be present even with no data
        assert_eq!(headers.len(), 8);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_handle_key_selection_wraparound() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);
        screen.set_selected_index(Some(19)); // Last item
        
        // Try to go down from last
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.selected_index(), Some(19)); // Should stay at last
        
        // Go to first
        screen.set_selected_index(Some(0));
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.selected_index(), Some(0)); // Should stay at first
    }

    #[test]
    fn test_export_json_pretty_formatting() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json().unwrap();
        
        // Pretty JSON should have newlines
        assert!(json.contains('\n'));
        // Should have proper indentation
        assert!(json.contains("  ")); // Indentation spaces
    }

    #[test]
    fn test_create_results_table_numeric_precision() {
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: 1.123456789,
            skew: 0.987654321,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: 1.23456789,
            total_return: 0.123456789,
            max_drawdown: -0.0987654321,
            num_trades: 12345,
            win_rate: 0.654321,
            avg_trade_pnl: 0.00123456789,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // Should format with appropriate precision
        assert_eq!(rows.len(), 1);
        let cells = rows[0].cells();
        assert!(cells[1].contains(".")); // Spread formatted
        assert!(cells[2].contains(".")); // Skew formatted
    }

    #[test]
    fn test_pareto_widget_axis_selection() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Default axis selection should be valid
        assert!(widget.x_axis_objective() < widget.objective_names.len());
        assert!(widget.y_axis_objective() < widget.objective_names.len());
        assert_ne!(widget.x_axis_objective(), widget.y_axis_objective());
    }

    #[test]
    fn test_create_heatmap_series_data_integrity() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // Verify data integrity: each point should correspond to a result
        assert_eq!(series.points.len(), screen.tune_result().all_results.len());
        
        for (i, point) in series.points.iter().enumerate() {
            let result = &screen.tune_result().all_results[i];
            assert_eq!(point.x, result.spread);
            assert_eq!(point.y, result.skew);
        }
    }

    #[test]
    fn test_best_result_priority() {
        let mut tune_result = create_test_tune_result();
        // Set best to a different item than first
        tune_result.best = Some(tune_result.all_results[5].clone());
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let best = screen.best_result();
        
        assert!(best.is_some());
        // Should return the explicitly set best, not the first
        assert_eq!(best.unwrap().spread, 1.5); // Item at index 5
    }

    #[test]
    fn test_top_results_maintains_order() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let top_10 = screen.top_results(10);
        
        // Results should maintain the order from all_results (sorted by Sharpe)
        for i in 0..top_10.len().saturating_sub(1) {
            assert!(top_10[i].sharpe >= top_10[i + 1].sharpe);
        }
    }

    #[test]
    fn test_handle_key_shift_tab_variations() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        screen.set_view_mode(TuneViewMode::FullTable);
        
        // Shift+Tab should go to previous mode
        let key = KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT);
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);
    }

    #[test]
    fn test_create_results_table_percentage_formatting() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // Percentage columns should have % sign
        for row in &rows {
            let cells = row.cells();
            assert!(cells[4].contains("%")); // Return
            assert!(cells[5].contains("%")); // Drawdown
            assert!(cells[7].contains("%")); // Win rate
        }
    }

    #[test]
    fn test_pareto_widget_solution_metadata_formatting() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        // Check metadata formatting
        let first_solution = widget.solutions.get(0).unwrap();
        let metadata = first_solution.metadata.as_ref().unwrap();
        assert!(metadata.contains("Spread=1.00"));
        assert!(metadata.contains("Skew=0.50"));
        assert!(metadata.contains("Sharpe=2.0000"));
    }

    #[test]
    fn test_screen_state_consistency() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Change multiple properties
        screen.set_view_mode(TuneViewMode::FullTable);
        screen.set_selected_index(Some(5));
        screen.set_focused(false);
        
        // Verify all changes persisted
        assert_eq!(screen.view_mode(), TuneViewMode::FullTable);
        assert_eq!(screen.selected_index(), Some(5));
        assert!(!screen.is_focused());
    }

    #[test]
    fn test_create_heatmap_series_label_formatting() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let series = screen.create_heatmap_series();
        
        // Labels should be formatted Sharpe values
        for (i, point) in series.points.iter().enumerate() {
            let label = point.label.as_ref().unwrap();
            let expected_sharpe = screen.tune_result().all_results[i].sharpe;
            let expected_label = format!("{:.4}", expected_sharpe);
            assert_eq!(label, &expected_label);
        }
    }

    #[test]
    fn test_pareto_widget_frontier_calculation_accuracy() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        let frontier = widget.get_frontier_solutions_sorted();
        // Frontier should be non-empty for diverse results
        assert!(!frontier.is_empty());
        
        // All frontier solutions should be marked as frontier
        for &idx in &frontier {
            let solution = widget.solutions.get(idx).unwrap();
            assert!(solution.is_frontier);
        }
    }

    #[test]
    fn test_export_json_completeness() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let json = screen.export_to_json().unwrap();
        
        // Verify all key fields are present
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("algorithm").is_some());
        assert!(parsed.get("algorithm_name").is_some());
        assert!(parsed.get("all_results").is_some());
        assert!(parsed.get("total_combinations").is_some());
        
        // Verify all_results is an array
        let all_results = parsed.get("all_results").unwrap().as_array().unwrap();
        assert_eq!(all_results.len(), 20);
    }

    #[test]
    fn test_create_results_table_row_consistency() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // All rows should have same number of cells
        let cell_count = rows[0].cells().len();
        for row in &rows {
            assert_eq!(row.cells().len(), cell_count);
        }
    }

    #[test]
    fn test_view_mode_all_contains_all() {
        let all_modes = TuneViewMode::all();
        let expected_modes = vec![
            TuneViewMode::TopResults,
            TuneViewMode::FullTable,
            TuneViewMode::Heatmap,
            TuneViewMode::Pareto,
        ];
        
        for expected in &expected_modes {
            assert!(all_modes.contains(expected));
        }
    }

    #[test]
    fn test_screen_initial_state() {
        let tune_result = create_test_tune_result();
        let screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Verify initial state
        assert_eq!(screen.view_mode(), TuneViewMode::TopResults);
        assert!(screen.is_focused());
        assert_eq!(screen.selected_index(), None);
        assert!(screen.export_path.is_none());
    }

    #[test]
    fn test_handle_key_export_path_setting() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        assert!(screen.export_path.is_none());
        
        let key = KeyEvent::new(KeyCode::Char('e'), KeyModifiers::empty());
        screen.handle_key(key);
        
        assert!(screen.export_path.is_some());
        assert_eq!(screen.export_path.as_ref().unwrap(), "export.json");
    }

    #[test]
    fn test_create_results_table_with_best_result() {
        let mut tune_result = create_test_tune_result();
        tune_result.best = Some(tune_result.all_results[0].clone());
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // Should work the same regardless of best being set
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn test_pareto_widget_with_single_solution() {
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results: vec![TuneResultItem {
                spread: 1.0,
                skew: 0.5,
                high_entropy_threshold: 0.7,
                fill_prob: 0.8,
                sharpe: 2.0,
                total_return: 0.15,
                max_drawdown: -0.05,
                num_trades: 100,
                win_rate: 0.55,
                avg_trade_pnl: 0.001,
            }],
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let widget = screen.create_pareto_widget();
        
        assert_eq!(widget.solution_count(), 1);
        // Single solution should be on frontier
        assert!(widget.solutions[0].is_frontier);
    }

    #[test]
    fn test_handle_key_navigation_all_modes() {
        let tune_result = create_test_tune_result();
        let mut screen = BacktestTuneResultsScreen::new(tune_result);
        
        // Test navigation in each mode
        for mode in TuneViewMode::all() {
            screen.set_view_mode(mode);
            
            // Tab should work in all modes
            let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
            assert!(screen.handle_key(key));
        }
    }

    #[test]
    fn test_create_results_table_special_characters() {
        // Test with results that might have special formatting needs
        let mut all_results = Vec::new();
        all_results.push(TuneResultItem {
            spread: 0.0001,
            skew: 0.00001,
            high_entropy_threshold: 0.7,
            fill_prob: 0.8,
            sharpe: 0.00001,
            total_return: 0.000001,
            max_drawdown: -0.0000001,
            num_trades: 1,
            win_rate: 0.0001,
            avg_trade_pnl: 0.0000001,
        });
        let tune_result = TuneResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_results,
            best: None,
            total_combinations: 1,
        };
        let screen = BacktestTuneResultsScreen::new(tune_result);
        let all: Vec<&TuneResultItem> = screen.tune_result().all_results.iter().collect();
        let (_, rows) = screen.create_results_table(&all);
        
        // Should handle very small values
        assert_eq!(rows.len(), 1);
    }
}
