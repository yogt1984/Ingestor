//! Backtest Multi-Objective Results Screen (T-3.8)
//!
//! TUI screen for displaying backtest multi_objective command results.
//! Supports multiple view modes: TopResults, FullTable, Pareto, Comparison.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::backtest::{MultiObjectiveResult, MultiObjectiveSolution};
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

/// View mode for multi-objective results display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiObjectiveViewMode {
    /// Top 10 results table
    TopResults,
    /// Full table with all results (sortable)
    FullTable,
    /// Pareto frontier visualization
    Pareto,
    /// Comparison view (frontier vs all)
    Comparison,
}

impl MultiObjectiveViewMode {
    /// Get all view modes
    pub fn all() -> Vec<MultiObjectiveViewMode> {
        vec![
            MultiObjectiveViewMode::TopResults,
            MultiObjectiveViewMode::FullTable,
            MultiObjectiveViewMode::Pareto,
            MultiObjectiveViewMode::Comparison,
        ]
    }

    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            MultiObjectiveViewMode::TopResults => "Top Results",
            MultiObjectiveViewMode::FullTable => "Full Table",
            MultiObjectiveViewMode::Pareto => "Pareto",
            MultiObjectiveViewMode::Comparison => "Comparison",
        }
    }

    /// Get next view mode
    pub fn next(&self) -> MultiObjectiveViewMode {
        let all = Self::all();
        let current_idx = all.iter().position(|v| v == self).unwrap_or(0);
        let next_idx = (current_idx + 1) % all.len();
        all[next_idx]
    }

    /// Get previous view mode
    pub fn previous(&self) -> MultiObjectiveViewMode {
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

/// Backtest multi-objective results screen
pub struct BacktestMultiObjectiveResultsScreen {
    /// Multi-objective result data
    result: MultiObjectiveResult,
    /// Current view mode
    view_mode: MultiObjectiveViewMode,
    /// Selected result index (for FullTable view)
    selected_index: Option<usize>,
    /// Whether the screen is focused
    focused: bool,
    /// Export path (if exporting)
    export_path: Option<String>,
}

impl BacktestMultiObjectiveResultsScreen {
    /// Create a new results screen from MultiObjectiveResult
    pub fn new(result: MultiObjectiveResult) -> Self {
        Self {
            result,
            view_mode: MultiObjectiveViewMode::TopResults,
            selected_index: None,
            focused: true,
            export_path: None,
        }
    }

    /// Get the result data
    pub fn result(&self) -> &MultiObjectiveResult {
        &self.result
    }

    /// Get current view mode
    pub fn view_mode(&self) -> MultiObjectiveViewMode {
        self.view_mode
    }

    /// Set view mode
    pub fn set_view_mode(&mut self, mode: MultiObjectiveViewMode) {
        self.view_mode = mode;
    }

    /// Get selected index
    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    /// Set selected index
    pub fn set_selected_index(&mut self, index: Option<usize>) {
        if let Some(idx) = index {
            if idx < self.result.all_solutions.len() {
                self.selected_index = Some(idx);
            } else {
                self.selected_index = None;
            }
        } else {
            self.selected_index = None;
        }
    }

    /// Get selected solution
    pub fn selected_solution(&self) -> Option<&MultiObjectiveSolution> {
        self.selected_index
            .and_then(|idx| self.result.all_solutions.get(idx))
    }

    /// Get best weighted solution
    pub fn best_weighted(&self) -> Option<&MultiObjectiveSolution> {
        self.result.best_weighted.as_ref()
    }

    /// Get Pareto frontier solutions
    pub fn pareto_frontier(&self) -> &[MultiObjectiveSolution] {
        &self.result.pareto_frontier
    }

    /// Check if focused
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Set focused state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Get top N solutions
    pub fn top_solutions(&self, n: usize) -> Vec<&MultiObjectiveSolution> {
        self.result.all_solutions.iter().take(n).collect()
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
                if self.view_mode == MultiObjectiveViewMode::FullTable {
                    let new_idx = self.selected_index.map(|i| i + 1).unwrap_or(0);
                    self.set_selected_index(Some(new_idx.min(self.result.all_solutions.len().saturating_sub(1))));
                    true
                } else {
                    false
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.view_mode == MultiObjectiveViewMode::FullTable {
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
    fn create_results_table(&self, solutions: &[&MultiObjectiveSolution]) -> (Vec<TableHeader>, Vec<TableRow>) {
        let headers = vec![
            TableHeader::new("Rank".to_string()).with_width(6).with_sortable(true),
            TableHeader::new("Pareto Rank".to_string()).with_width(12).with_sortable(true),
            TableHeader::new("Spread".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Skew".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Sharpe".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Drawdown".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Return".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Fill Rate".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Turnover".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Win Rate".to_string()).with_width(10).with_sortable(true),
            TableHeader::new("Trades".to_string()).with_width(8).with_sortable(true),
            TableHeader::new("Crowding".to_string()).with_width(10).with_sortable(true),
        ];

        let rows: Vec<TableRow> = solutions
            .iter()
            .enumerate()
            .map(|(rank, solution)| {
                TableRow::new(vec![
                    format!("{}", rank + 1),
                    format!("{}", solution.pareto_rank),
                    format!("{:.2}", solution.spread_bps),
                    format!("{:.2}", solution.skew_factor),
                    format!("{:.4}", solution.sharpe),
                    format!("{:.2}%", solution.drawdown * 100.0),
                    format!("{:.2}%", solution.total_return * 100.0),
                    format!("{:.2}%", solution.fill_rate * 100.0),
                    format!("{:.4}", solution.turnover),
                    format!("{:.2}%", solution.win_rate * 100.0),
                    format!("{}", solution.num_trades),
                    format!("{:.4}", solution.crowding_distance),
                ])
            })
            .collect();

        (headers, rows)
    }

    /// Create Pareto frontier widget
    fn create_pareto_widget(&self) -> ParetoFrontierWidget {
        let mut widget = ParetoFrontierWidget::new()
            .with_objective_names(vec![
                "Sharpe Ratio".to_string(),
                "Total Return".to_string(),
                "Max Drawdown".to_string(),
                "Fill Rate".to_string(),
                "Turnover".to_string(),
            ])
            .with_show_frontier(true)
            .with_show_all_solutions(true);

        for (idx, solution) in self.result.all_solutions.iter().enumerate() {
            let pareto_solution = ParetoSolution::new(
                format!("S{}", idx + 1),
                vec![
                    solution.sharpe,
                    solution.total_return,
                    -solution.drawdown, // Negate drawdown so higher is better
                    solution.fill_rate,
                    solution.turnover,
                ],
            )
            .with_metadata(serde_json::json!({
                "spread": solution.spread_bps,
                "skew": solution.skew_factor,
                "fill_prob": solution.fill_probability,
                "high_entropy_threshold": solution.high_entropy_threshold,
                "pareto_rank": solution.pareto_rank,
                "crowding_distance": solution.crowding_distance
            }));

            widget.add_solution(pareto_solution);
        }

        widget
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
        let tab_titles: Vec<Line> = MultiObjectiveViewMode::all()
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
            .select(MultiObjectiveViewMode::all().iter().position(|m| *m == self.view_mode).unwrap_or(0))
            .divider("|");

        f.render_widget(tabs, chunks[0]);

        // Render content based on view mode
        match self.view_mode {
            MultiObjectiveViewMode::TopResults => {
                self.render_top_results(f, chunks[1]);
            }
            MultiObjectiveViewMode::FullTable => {
                self.render_full_table(f, chunks[1]);
            }
            MultiObjectiveViewMode::Pareto => {
                self.render_pareto(f, chunks[1]);
            }
            MultiObjectiveViewMode::Comparison => {
                self.render_comparison(f, chunks[1]);
            }
        }
    }

    /// Render top results view
    fn render_top_results(&self, f: &mut Frame, area: Rect) {
        let top_10 = self.top_solutions(10);
        let (headers, rows) = self.create_results_table(&top_10);

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render full table view
    fn render_full_table(&self, f: &mut Frame, area: Rect) {
        let all: Vec<&MultiObjectiveSolution> = self.result.all_solutions.iter().collect();
        let (headers, rows) = self.create_results_table(&all);

        let mut table = TableWidget::new()
            .with_headers(headers)
            .with_rows(rows);
        table.set_focused(self.focused);
        table.render(area, f.buffer_mut());
    }

    /// Render Pareto view
    fn render_pareto(&self, f: &mut Frame, area: Rect) {
        let mut widget = self.create_pareto_widget();
        widget.render(area, f.buffer_mut());
    }

    /// Render comparison view
    fn render_comparison(&self, f: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(0),
            ])
            .split(area);

        // Summary metrics
        let metrics = vec![
            Metric::new("Total Combinations".to_string(), MetricValue::Number(self.result.total_combinations as f64)),
            Metric::new("Pareto Frontier Size".to_string(), MetricValue::Number(self.result.pareto_frontier.len() as f64)),
            Metric::new("Time Span (hours)".to_string(), MetricValue::Number(self.result.time_span_hours)),
            Metric::new("Events Processed".to_string(), MetricValue::Number(self.result.num_events as f64)),
        ];

        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics);
        dashboard.render(chunks[0], f.buffer_mut());

        // Best weighted solution details
        if let Some(best) = &self.result.best_weighted {
            let details = vec![
                format!("Spread: {:.2} bps", best.spread_bps),
                format!("Skew: {:.2}", best.skew_factor),
                format!("Fill Prob: {:.2}", best.fill_probability),
                format!("High Entropy Threshold: {:.2}", best.high_entropy_threshold),
                format!("Sharpe: {:.4}", best.sharpe),
                format!("Drawdown: {:.2}%", best.drawdown * 100.0),
                format!("Return: {:.2}%", best.total_return * 100.0),
                format!("Fill Rate: {:.2}%", best.fill_rate * 100.0),
                format!("Turnover: {:.4}", best.turnover),
                format!("Win Rate: {:.2}%", best.win_rate * 100.0),
                format!("Trades: {}", best.num_trades),
                format!("Pareto Rank: {}", best.pareto_rank),
                format!("Crowding Distance: {:.4}", best.crowding_distance),
            ];

            let text: Vec<Line> = details.iter().map(|s| Line::from(s.as_str())).collect();
            let paragraph = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Best Weighted Solution"))
                .alignment(Alignment::Left);

            f.render_widget(paragraph, chunks[1]);
        } else {
            let paragraph = Paragraph::new("No best weighted solution available")
                .block(Block::default().borders(Borders::ALL).title("Best Weighted Solution"))
                .alignment(Alignment::Center);
            f.render_widget(paragraph, chunks[1]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_test_multi_objective_result() -> MultiObjectiveResult {
        let mut all_solutions = Vec::new();
        for i in 0..20 {
            all_solutions.push(MultiObjectiveSolution {
                spread_bps: 1.0 + i as f64 * 0.1,
                skew_factor: 0.5 + i as f64 * 0.05,
                fill_probability: 0.8,
                high_entropy_threshold: 0.7,
                sharpe: 2.0 - i as f64 * 0.1,
                drawdown: 0.05 + i as f64 * 0.005,
                fill_rate: 0.9 - i as f64 * 0.01,
                turnover: 1.0 + i as f64 * 0.1,
                total_return: 0.15 - i as f64 * 0.01,
                win_rate: 0.55 - i as f64 * 0.01,
                num_trades: 100 - i * 5,
                pareto_rank: if i < 5 { 1 } else { 2 },
                crowding_distance: 1.0 - i as f64 * 0.05,
            });
        }

        let pareto_frontier: Vec<MultiObjectiveSolution> = all_solutions.iter()
            .filter(|s| s.pareto_rank == 1)
            .cloned()
            .collect();

        MultiObjectiveResult {
            algorithm: "AvellanedaStoikov".to_string(),
            algorithm_name: "Avellaneda-Stoikov".to_string(),
            all_solutions,
            pareto_frontier,
            best_weighted: None,
            total_combinations: 20,
            time_span_hours: 720.0,
            num_events: 10000,
        }
    }

    #[test]
    fn test_view_mode_all() {
        let modes = MultiObjectiveViewMode::all();
        assert_eq!(modes.len(), 4);
    }

    #[test]
    fn test_view_mode_name() {
        assert_eq!(MultiObjectiveViewMode::TopResults.name(), "Top Results");
        assert_eq!(MultiObjectiveViewMode::FullTable.name(), "Full Table");
        assert_eq!(MultiObjectiveViewMode::Pareto.name(), "Pareto");
        assert_eq!(MultiObjectiveViewMode::Comparison.name(), "Comparison");
    }

    #[test]
    fn test_view_mode_next() {
        assert_eq!(MultiObjectiveViewMode::TopResults.next(), MultiObjectiveViewMode::FullTable);
        assert_eq!(MultiObjectiveViewMode::FullTable.next(), MultiObjectiveViewMode::Pareto);
        assert_eq!(MultiObjectiveViewMode::Pareto.next(), MultiObjectiveViewMode::Comparison);
        assert_eq!(MultiObjectiveViewMode::Comparison.next(), MultiObjectiveViewMode::TopResults);
    }

    #[test]
    fn test_screen_creation() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        assert_eq!(screen.view_mode(), MultiObjectiveViewMode::TopResults);
        assert!(screen.is_focused());
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_set_view_mode() {
        let result = create_test_multi_objective_result();
        let mut screen = BacktestMultiObjectiveResultsScreen::new(result);
        screen.set_view_mode(MultiObjectiveViewMode::Pareto);
        assert_eq!(screen.view_mode(), MultiObjectiveViewMode::Pareto);
    }

    #[test]
    fn test_set_selected_index() {
        let result = create_test_multi_objective_result();
        let mut screen = BacktestMultiObjectiveResultsScreen::new(result);
        screen.set_selected_index(Some(5));
        assert_eq!(screen.selected_index(), Some(5));
    }

    #[test]
    fn test_selected_solution() {
        let result = create_test_multi_objective_result();
        let mut screen = BacktestMultiObjectiveResultsScreen::new(result);
        screen.set_selected_index(Some(0));
        assert!(screen.selected_solution().is_some());
    }

    #[test]
    fn test_pareto_frontier() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        assert_eq!(screen.pareto_frontier().len(), 5);
    }

    #[test]
    fn test_best_weighted() {
        let mut result = create_test_multi_objective_result();
        result.best_weighted = Some(result.all_solutions[0].clone());
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        assert!(screen.best_weighted().is_some());
    }

    #[test]
    fn test_top_solutions() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        let top_10 = screen.top_solutions(10);
        assert_eq!(top_10.len(), 10);
    }

    #[test]
    fn test_export_json() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        let json = screen.export_to_json().unwrap();
        assert!(json.contains("\"algorithm\""));
    }

    #[test]
    fn test_handle_key_tab() {
        let result = create_test_multi_objective_result();
        let mut screen = BacktestMultiObjectiveResultsScreen::new(result);
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        assert!(screen.handle_key(key));
        assert_eq!(screen.view_mode(), MultiObjectiveViewMode::FullTable);
    }

    #[test]
    fn test_create_results_table() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result.clone());
        let all: Vec<&MultiObjectiveSolution> = result.all_solutions.iter().collect();
        let (headers, rows) = screen.create_results_table(&all);
        assert_eq!(headers.len(), 12);
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn test_create_pareto_widget() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        let widget = screen.create_pareto_widget();
        assert_eq!(widget.solution_count(), 20);
    }

    #[test]
    fn test_screen_with_empty_results() {
        let result = MultiObjectiveResult {
            algorithm: "Test".to_string(),
            algorithm_name: "Test".to_string(),
            all_solutions: Vec::new(),
            pareto_frontier: Vec::new(),
            best_weighted: None,
            total_combinations: 0,
            time_span_hours: 0.0,
            num_events: 0,
        };
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        assert_eq!(screen.top_solutions(10).len(), 0);
        assert_eq!(screen.pareto_frontier().len(), 0);
    }

    #[test]
    fn test_selected_index_bounds() {
        let result = create_test_multi_objective_result();
        let mut screen = BacktestMultiObjectiveResultsScreen::new(result);
        screen.set_selected_index(Some(100));
        assert_eq!(screen.selected_index(), None);
    }

    #[test]
    fn test_view_mode_cycle() {
        let mut mode = MultiObjectiveViewMode::TopResults;
        let mut visited = HashSet::new();
        for _ in 0..10 {
            visited.insert(mode);
            mode = mode.next();
        }
        assert_eq!(visited.len(), 4);
    }

    #[test]
    fn test_pareto_frontier_filtering() {
        let result = create_test_multi_objective_result();
        let screen = BacktestMultiObjectiveResultsScreen::new(result);
        let frontier = screen.pareto_frontier();
        // All frontier solutions should have rank 1
        for solution in frontier {
            assert_eq!(solution.pareto_rank, 1);
        }
    }
}
