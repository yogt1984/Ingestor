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

        let (headers, rows) = self.create_results_table(&self.tune_result.all_results);
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
            .with_metadata(format!(
                "Spread={:.2}, Skew={:.2}, Sharpe={:.4}",
                item.spread, item.skew, item.sharpe
            ));

            widget.add_solution(solution);
        }

        widget.update_frontier();
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
}
