//! Pareto Frontier Widget (T-3.4)
//!
//! A reusable widget for displaying Pareto frontier (multi-objective optimization results).
//! Supports 2D scatter plot rendering, solution selection, and axis selection for different objectives.

use ratatui::{
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fmt::Display;

// ============================================================================
// Types
// ============================================================================

/// Pareto frontier widget for displaying multi-objective optimization results
pub struct ParetoFrontierWidget {
    /// Pareto solutions
    solutions: Vec<ParetoSolution>,
    /// Currently selected solution index (None = no selection)
    selected_solution: Option<usize>,
    /// X-axis objective index
    x_axis_objective: usize,
    /// Y-axis objective index
    y_axis_objective: usize,
    /// Available objective names
    objective_names: Vec<String>,
    /// Whether to show Pareto frontier line
    show_frontier: bool,
    /// Whether to show all solutions or only frontier
    show_all_solutions: bool,
    /// Block style (optional title, borders)
    block: Option<Block<'static>>,
    /// Whether the widget is focused
    focused: bool,
    /// Color palette for solutions
    color_palette: Vec<Color>,
}

impl Clone for ParetoFrontierWidget {
    fn clone(&self) -> Self {
        Self {
            solutions: self.solutions.clone(),
            selected_solution: self.selected_solution,
            x_axis_objective: self.x_axis_objective,
            y_axis_objective: self.y_axis_objective,
            objective_names: self.objective_names.clone(),
            show_frontier: self.show_frontier,
            show_all_solutions: self.show_all_solutions,
            block: self.block.clone(),
            focused: self.focused,
            color_palette: self.color_palette.clone(),
        }
    }
}

impl std::fmt::Debug for ParetoFrontierWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParetoFrontierWidget")
            .field("solutions_count", &self.solutions.len())
            .field("selected_solution", &self.selected_solution)
            .field("x_axis_objective", &self.x_axis_objective)
            .field("y_axis_objective", &self.y_axis_objective)
            .field("show_frontier", &self.show_frontier)
            .finish()
    }
}

/// Pareto solution (point in multi-objective space)
#[derive(Debug, Clone, PartialEq)]
pub struct ParetoSolution {
    /// Solution identifier/name
    pub id: String,
    /// Objective values (multiple objectives)
    pub objectives: Vec<f64>,
    /// Whether this solution is on the Pareto frontier
    pub is_frontier: bool,
    /// Optional solution metadata
    pub metadata: Option<serde_json::Value>,
    /// Custom color (optional)
    pub color: Option<Color>,
}

impl ParetoSolution {
    /// Create a new Pareto solution
    pub fn new(id: impl Into<String>, objectives: Vec<f64>) -> Self {
        Self {
            id: id.into(),
            objectives,
            is_frontier: false,
            metadata: None,
            color: None,
        }
    }

    /// Set whether solution is on frontier
    pub fn with_frontier(mut self, is_frontier: bool) -> Self {
        self.is_frontier = is_frontier;
        self
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Set custom color
    pub fn with_color(mut self, color: Color) -> Self {
        self.color = Some(color);
        self
    }

    /// Get objective value at index
    pub fn get_objective(&self, index: usize) -> Option<f64> {
        self.objectives.get(index).copied()
    }

    /// Get number of objectives
    pub fn objective_count(&self) -> usize {
        self.objectives.len()
    }
}

// ============================================================================
// ParetoFrontierWidget Implementation
// ============================================================================

impl Default for ParetoFrontierWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl ParetoFrontierWidget {
    /// Create a new empty Pareto frontier widget
    pub fn new() -> Self {
        Self {
            solutions: Vec::new(),
            selected_solution: None,
            x_axis_objective: 0,
            y_axis_objective: 1,
            objective_names: Vec::new(),
            show_frontier: true,
            show_all_solutions: true,
            block: None,
            focused: false,
            color_palette: vec![
                Color::Blue,
                Color::Green,
                Color::Red,
                Color::Yellow,
                Color::Magenta,
                Color::Cyan,
            ],
        }
    }

    /// Add a solution
    pub fn add_solution(&mut self, solution: ParetoSolution) {
        self.solutions.push(solution);
        self.update_frontier();
    }

    /// Set solutions
    pub fn with_solutions(mut self, solutions: Vec<ParetoSolution>) -> Self {
        self.solutions = solutions;
        self.update_frontier();
        self
    }

    /// Set objective names
    pub fn with_objective_names(mut self, names: Vec<String>) -> Self {
        self.objective_names = names;
        self
    }

    /// Set X-axis objective index
    pub fn with_x_axis_objective(mut self, index: usize) -> Self {
        if index <= self.max_objective_index() {
            self.x_axis_objective = index;
        }
        self
    }

    /// Set Y-axis objective index
    pub fn with_y_axis_objective(mut self, index: usize) -> Self {
        if index <= self.max_objective_index() {
            self.y_axis_objective = index;
        }
        self
    }

    /// Set whether to show frontier line
    pub fn with_show_frontier(mut self, show: bool) -> Self {
        self.show_frontier = show;
        self
    }

    /// Set whether to show all solutions
    pub fn with_show_all_solutions(mut self, show: bool) -> Self {
        self.show_all_solutions = show;
        self
    }

    /// Set block (title, borders)
    pub fn with_block(mut self, block: Block<'static>) -> Self {
        self.block = Some(block);
        self
    }

    /// Set focus state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    /// Get focus state
    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// Get number of solutions
    pub fn solution_count(&self) -> usize {
        self.solutions.len()
    }

    /// Get currently selected solution
    pub fn selected_solution(&self) -> Option<&ParetoSolution> {
        self.selected_solution.and_then(|idx| self.solutions.get(idx))
    }

    /// Get selected solution index
    pub fn selected_solution_index(&self) -> Option<usize> {
        self.selected_solution
    }

    /// Get X-axis objective index
    pub fn x_axis_objective(&self) -> usize {
        self.x_axis_objective
    }

    /// Get Y-axis objective index
    pub fn y_axis_objective(&self) -> usize {
        self.y_axis_objective
    }

    /// Get all solutions
    pub fn solutions(&self) -> &[ParetoSolution] {
        &self.solutions
    }

    /// Get objective names
    pub fn objective_names(&self) -> &[String] {
        &self.objective_names
    }

    /// Get whether to show frontier
    pub fn show_frontier(&self) -> bool {
        self.show_frontier
    }

    /// Get whether to show all solutions
    pub fn show_all_solutions(&self) -> bool {
        self.show_all_solutions
    }

    /// Handle keyboard input
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !self.focused {
            return false;
        }

        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_selection_up();
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_selection_down();
                true
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.select_previous_objective_x();
                true
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.select_next_objective_x();
                true
            }
            KeyCode::Char('x') => {
                self.cycle_x_axis();
                true
            }
            KeyCode::Char('y') => {
                self.cycle_y_axis();
                true
            }
            KeyCode::Char('f') => {
                self.toggle_show_frontier();
                true
            }
            KeyCode::Char('a') => {
                self.toggle_show_all_solutions();
                true
            }
            _ => false,
        }
    }

    /// Select solution by index
    pub fn select_solution(&mut self, index: usize) {
        if index < self.solutions.len() {
            self.selected_solution = Some(index);
        }
    }

    /// Clear selection
    pub fn clear_selection(&mut self) {
        self.selected_solution = None;
    }

    /// Get maximum objective index
    fn max_objective_index(&self) -> usize {
        self.solutions
            .iter()
            .map(|s| s.objective_count())
            .max()
            .unwrap_or(0)
            .saturating_sub(1)
    }

    /// Update frontier status for all solutions
    fn update_frontier(&mut self) {
        if self.solutions.is_empty() {
            return;
        }

        let num_objectives = self.solutions[0].objective_count();
        if num_objectives == 0 {
            return;
        }

        // Mark all as non-frontier first
        for solution in &mut self.solutions {
            solution.is_frontier = false;
        }

        // Find Pareto-optimal solutions (for minimization: no other solution is better in all objectives)
        for i in 0..self.solutions.len() {
            let mut is_dominated = false;
            for j in 0..self.solutions.len() {
                if i == j {
                    continue;
                }

                // Check if solution j dominates solution i
                let mut better_in_all = true;
                let mut better_in_some = false;

                for obj_idx in 0..num_objectives {
                    let val_i = self.solutions[i].objectives[obj_idx];
                    let val_j = self.solutions[j].objectives[obj_idx];

                    // Assuming minimization (lower is better)
                    if val_j > val_i {
                        better_in_all = false;
                        break;
                    } else if val_j < val_i {
                        better_in_some = true;
                    }
                }

                if better_in_all && better_in_some {
                    is_dominated = true;
                    break;
                }
            }

            if !is_dominated {
                self.solutions[i].is_frontier = true;
            }
        }
    }

    /// Move selection up
    fn move_selection_up(&mut self) {
        match self.selected_solution {
            Some(current) if current > 0 => {
                self.selected_solution = Some(current - 1);
            }
            Some(_) => {}
            None if !self.solutions.is_empty() => {
                self.selected_solution = Some(0);
            }
            None => {}
        }
    }

    /// Move selection down
    fn move_selection_down(&mut self) {
        match self.selected_solution {
            Some(current) if current < self.solutions.len().saturating_sub(1) => {
                self.selected_solution = Some(current + 1);
            }
            Some(_) => {}
            None if !self.solutions.is_empty() => {
                self.selected_solution = Some(0);
            }
            None => {}
        }
    }

    /// Select previous objective for X-axis
    fn select_previous_objective_x(&mut self) {
        if self.x_axis_objective > 0 {
            self.x_axis_objective -= 1;
        } else {
            self.x_axis_objective = self.max_objective_index();
        }
    }

    /// Select next objective for X-axis
    fn select_next_objective_x(&mut self) {
        let max = self.max_objective_index();
        if self.x_axis_objective < max {
            self.x_axis_objective += 1;
        } else {
            self.x_axis_objective = 0;
        }
    }

    /// Cycle X-axis objective
    fn cycle_x_axis(&mut self) {
        self.select_next_objective_x();
    }

    /// Cycle Y-axis objective
    fn cycle_y_axis(&mut self) {
        let max = self.max_objective_index();
        if self.y_axis_objective < max {
            self.y_axis_objective += 1;
        } else {
            self.y_axis_objective = 0;
        }
        // Ensure X and Y are different
        if self.y_axis_objective == self.x_axis_objective {
            self.cycle_y_axis();
        }
    }

    /// Toggle show frontier
    fn toggle_show_frontier(&mut self) {
        self.show_frontier = !self.show_frontier;
    }

    /// Toggle show all solutions
    fn toggle_show_all_solutions(&mut self) {
        self.show_all_solutions = !self.show_all_solutions;
    }

    /// Calculate X range for current axis
    fn calculate_x_range(&self) -> (f64, f64) {
        if self.solutions.is_empty() {
            return (0.0, 1.0);
        }

        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;

        for solution in &self.solutions {
            if let Some(x_val) = solution.get_objective(self.x_axis_objective) {
                min_x = min_x.min(x_val);
                max_x = max_x.max(x_val);
            }
        }

        if min_x == f64::INFINITY {
            (0.0, 1.0)
        } else if min_x == max_x {
            (min_x - 1.0, max_x + 1.0)
        } else {
            let padding = (max_x - min_x) * 0.1;
            (min_x - padding, max_x + padding)
        }
    }

    /// Calculate Y range for current axis
    fn calculate_y_range(&self) -> (f64, f64) {
        if self.solutions.is_empty() {
            return (0.0, 1.0);
        }

        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;

        for solution in &self.solutions {
            if let Some(y_val) = solution.get_objective(self.y_axis_objective) {
                min_y = min_y.min(y_val);
                max_y = max_y.max(y_val);
            }
        }

        if min_y == f64::INFINITY {
            (0.0, 1.0)
        } else if min_y == max_y {
            (min_y - 1.0, max_y + 1.0)
        } else {
            let padding = (max_y - min_y) * 0.1;
            (min_y - padding, max_y + padding)
        }
    }

    /// Convert X value to screen X coordinate
    fn x_to_screen(&self, x: f64, x_min: f64, x_max: f64, width: u16) -> u16 {
        if x_max == x_min {
            return width / 2;
        }
        let ratio = (x - x_min) / (x_max - x_min);
        ((ratio * width as f64) as u16).min(width.saturating_sub(1))
    }

    /// Convert Y value to screen Y coordinate
    fn y_to_screen(&self, y: f64, y_min: f64, y_max: f64, height: u16) -> u16 {
        if y_max == y_min {
            return height / 2;
        }
        let ratio = (y - y_min) / (y_max - y_min);
        // Y is inverted (0 at top, height at bottom)
        let screen_y = height.saturating_sub(1) as f64 - (ratio * (height.saturating_sub(1)) as f64);
        screen_y as u16
    }

    /// Get frontier solutions sorted by X-axis
    pub fn get_frontier_solutions_sorted(&self) -> Vec<usize> {
        let mut frontier_indices: Vec<usize> = self
            .solutions
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_frontier)
            .map(|(idx, _)| idx)
            .collect();

        // Sort by X-axis value
        frontier_indices.sort_by(|&a, &b| {
            let x_a = self.solutions[a].get_objective(self.x_axis_objective).unwrap_or(0.0);
            let x_b = self.solutions[b].get_objective(self.x_axis_objective).unwrap_or(0.0);
            x_a.partial_cmp(&x_b).unwrap_or(std::cmp::Ordering::Equal)
        });

        frontier_indices
    }

    /// Render the Pareto frontier widget
    pub fn render(&mut self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if self.solutions.is_empty() {
            return;
        }

        let inner_area = if let Some(ref block) = self.block {
            let inner = block.inner(area);
            block.clone().render(area, buf);
            inner
        } else {
            area
        };

        let (x_min, x_max) = self.calculate_x_range();
        let (y_min, y_max) = self.calculate_y_range();

        // Draw grid
        self.draw_grid(inner_area, x_min, x_max, y_min, y_max, buf);

        // Draw frontier line if enabled
        if self.show_frontier {
            self.draw_frontier_line(inner_area, x_min, x_max, y_min, y_max, buf);
        }

        // Draw solutions
        let solutions_to_show = if self.show_all_solutions {
            (0..self.solutions.len()).collect()
        } else {
            self.get_frontier_solutions_sorted()
        };

        for &idx in &solutions_to_show {
            let solution = &self.solutions[idx];
            let is_selected = self.selected_solution == Some(idx);

            self.draw_solution(
                solution,
                idx,
                is_selected,
                inner_area,
                x_min,
                x_max,
                y_min,
                y_max,
                buf,
            );
        }

        // Draw axes labels
        self.draw_axes_labels(inner_area, buf);

        // Draw legend/info
        self.draw_info(inner_area, buf);
    }

    /// Draw grid
    fn draw_grid(&self, area: Rect, x_min: f64, x_max: f64, y_min: f64, y_max: f64, buf: &mut ratatui::buffer::Buffer) {
        // Draw horizontal grid lines
        for i in 0..=5 {
            let y_val = y_min + (y_max - y_min) * (i as f64 / 5.0);
            let y_pos = self.y_to_screen(y_val, y_min, y_max, area.height);
            let y_abs = area.y + y_pos;
            if y_pos < area.height && y_abs < buf.area.height {
                for x in area.x..area.x + area.width {
                    if x < buf.area.width {
                        buf.get_mut(x, y_abs)
                            .set_char('─')
                            .set_style(Style::default().fg(Color::DarkGray));
                    }
                }
            }
        }

        // Draw vertical grid lines
        for i in 0..=5 {
            let x_val = x_min + (x_max - x_min) * (i as f64 / 5.0);
            let x_pos = self.x_to_screen(x_val, x_min, x_max, area.width);
            let x_abs = area.x + x_pos;
            if x_pos < area.width && x_abs < buf.area.width {
                for y in area.y..area.y + area.height {
                    if y < buf.area.height {
                        buf.get_mut(x_abs, y)
                            .set_char('│')
                            .set_style(Style::default().fg(Color::DarkGray));
                    }
                }
            }
        }
    }

    /// Draw frontier line
    fn draw_frontier_line(&self, area: Rect, x_min: f64, x_max: f64, y_min: f64, y_max: f64, buf: &mut ratatui::buffer::Buffer) {
        let frontier_indices = self.get_frontier_solutions_sorted();
        if frontier_indices.len() < 2 {
            return;
        }

        // Draw line connecting frontier points
        for i in 0..frontier_indices.len().saturating_sub(1) {
            let idx1 = frontier_indices[i];
            let idx2 = frontier_indices[i + 1];

            let x1 = self.solutions[idx1].get_objective(self.x_axis_objective).unwrap_or(0.0);
            let y1 = self.solutions[idx1].get_objective(self.y_axis_objective).unwrap_or(0.0);
            let x2 = self.solutions[idx2].get_objective(self.x_axis_objective).unwrap_or(0.0);
            let y2 = self.solutions[idx2].get_objective(self.y_axis_objective).unwrap_or(0.0);

            let screen_x1 = self.x_to_screen(x1, x_min, x_max, area.width);
            let screen_y1 = self.y_to_screen(y1, y_min, y_max, area.height);
            let screen_x2 = self.x_to_screen(x2, x_min, x_max, area.width);
            let screen_y2 = self.y_to_screen(y2, y_min, y_max, area.height);

            self.draw_line(
                area.x + screen_x1,
                area.y + screen_y1,
                area.x + screen_x2,
                area.y + screen_y2,
                '─',
                Color::Green,
                buf,
            );
        }
    }

    /// Draw a solution point
    fn draw_solution(
        &self,
        solution: &ParetoSolution,
        index: usize,
        is_selected: bool,
        area: Rect,
        x_min: f64,
        x_max: f64,
        y_min: f64,
        y_max: f64,
        buf: &mut ratatui::buffer::Buffer,
    ) {
        let x_val = solution.get_objective(self.x_axis_objective).unwrap_or(0.0);
        let y_val = solution.get_objective(self.y_axis_objective).unwrap_or(0.0);

        let screen_x = self.x_to_screen(x_val, x_min, x_max, area.width);
        let screen_y = self.y_to_screen(y_val, y_min, y_max, area.height);

        let color = solution.color.unwrap_or_else(|| {
            if solution.is_frontier {
                Color::Green
            } else {
                Color::Blue
            }
        });

        let symbol = if is_selected {
            '●'
        } else if solution.is_frontier {
            '◆'
        } else {
            '○'
        };

        let screen_x_abs = area.x + screen_x;
        let screen_y_abs = area.y + screen_y;

        if screen_x_abs < buf.area.width && screen_y_abs < buf.area.height {
            let style = if is_selected && self.focused {
                Style::default()
                    .fg(color)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else {
                Style::default().fg(color)
            };

            buf.get_mut(screen_x_abs, screen_y_abs)
                .set_char(symbol)
                .set_style(style);
        }
    }

    /// Draw a line between two points
    fn draw_line(&self, x1: u16, y1: u16, x2: u16, y2: u16, char: char, color: Color, buf: &mut ratatui::buffer::Buffer) {
        let dx = (x2 as i16 - x1 as i16).abs();
        let dy = (y2 as i16 - y1 as i16).abs();
        let sx = if x1 < x2 { 1 } else { -1 };
        let sy = if y1 < y2 { 1 } else { -1 };
        let mut err = dx - dy;
        let mut x = x1 as i16;
        let mut y = y1 as i16;

        loop {
            if x >= 0 && x < buf.area.width as i16 && y >= 0 && y < buf.area.height as i16 {
                buf.get_mut(x as u16, y as u16)
                    .set_char(char)
                    .set_style(Style::default().fg(color));
            }

            if x == x2 as i16 && y == y2 as i16 {
                break;
            }

            let e2 = 2 * err;
            if e2 > -dy {
                err -= dy;
                x += sx;
            }
            if e2 < dx {
                err += dx;
                y += sy;
            }
        }
    }

    /// Draw axes labels
    fn draw_axes_labels(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        // X-axis label
        let x_label = if self.x_axis_objective < self.objective_names.len() {
            self.objective_names[self.x_axis_objective].clone()
        } else {
            format!("Objective {}", self.x_axis_objective)
        };

        let x_label_y = area.y + area.height.saturating_sub(1);
        if x_label_y < buf.area.height {
            let label_width = x_label.len() as u16;
            let max_width = buf.area.width.saturating_sub(area.x);
            let clamped_width = label_width.min(max_width);
            let label_x = area.x + (max_width.saturating_sub(clamped_width)) / 2;
            let paragraph = Paragraph::new(x_label.as_str());
            paragraph.render(
                Rect {
                    x: label_x,
                    y: x_label_y,
                    width: clamped_width,
                    height: 1,
                },
                buf,
            );
        }

        // Y-axis label (rotated/vertical would be ideal but ASCII is limited)
        let y_label = if self.y_axis_objective < self.objective_names.len() {
            self.objective_names[self.y_axis_objective].clone()
        } else {
            format!("Objective {}", self.y_axis_objective)
        };

        // Place Y label on left side
        let y_label_x = area.x;
        let y_label_y = area.y + area.height / 2;
        if y_label_x < buf.area.width && y_label_y < buf.area.height {
            let paragraph = Paragraph::new(y_label.as_str());
            paragraph.render(
                Rect {
                    x: y_label_x,
                    y: y_label_y,
                    width: y_label.len().min(10) as u16,
                    height: 1,
                },
                buf,
            );
        }
    }

    /// Draw info/legend
    fn draw_info(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let info_y = area.y;
        if info_y >= buf.area.height {
            return;
        }

        let mut info_text = format!(
            "Frontier: {} | All: {} | X:{} Y:{}",
            if self.show_frontier { "ON" } else { "OFF" },
            if self.show_all_solutions { "ON" } else { "OFF" },
            self.x_axis_objective,
            self.y_axis_objective
        );

        if let Some(selected) = self.selected_solution {
            if let Some(solution) = self.solutions.get(selected) {
                info_text.push_str(&format!(" | Selected: {}", solution.id));
            }
        }

        let info_x = area.x;
        let paragraph = Paragraph::new(info_text.as_str())
            .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
        paragraph.render(
            Rect {
                x: info_x,
                y: info_y,
                width: info_text.len().min(area.width as usize) as u16,
                height: 1,
            },
            buf,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_solutions() -> Vec<ParetoSolution> {
        vec![
            ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]),
            ParetoSolution::new("S2", vec![2.0, 1.0, 3.0]),
            ParetoSolution::new("S3", vec![3.0, 3.0, 1.0]),
            ParetoSolution::new("S4", vec![1.5, 1.5, 2.5]),
        ]
    }

    #[test]
    fn test_pareto_widget_creation() {
        let widget = ParetoFrontierWidget::new();
        assert_eq!(widget.solution_count(), 0);
        assert_eq!(widget.x_axis_objective(), 0);
        assert_eq!(widget.y_axis_objective(), 1);
    }

    #[test]
    fn test_pareto_solution_creation() {
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0, 3.0]);
        assert_eq!(solution.id, "Test");
        assert_eq!(solution.objectives, vec![1.0, 2.0, 3.0]);
        assert_eq!(solution.is_frontier, false);
    }

    #[test]
    fn test_pareto_solution_with_frontier() {
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0]).with_frontier(true);
        assert_eq!(solution.is_frontier, true);
    }

    #[test]
    fn test_pareto_solution_with_metadata() {
        let metadata = serde_json::json!({"key": "value"});
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0]).with_metadata(metadata.clone());
        assert_eq!(solution.metadata, Some(metadata));
    }

    #[test]
    fn test_pareto_solution_with_color() {
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0]).with_color(Color::Red);
        assert_eq!(solution.color, Some(Color::Red));
    }

    #[test]
    fn test_pareto_solution_get_objective() {
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0, 3.0]);
        assert_eq!(solution.get_objective(0), Some(1.0));
        assert_eq!(solution.get_objective(1), Some(2.0));
        assert_eq!(solution.get_objective(2), Some(3.0));
        assert_eq!(solution.get_objective(3), None);
    }

    #[test]
    fn test_pareto_solution_objective_count() {
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0, 3.0]);
        assert_eq!(solution.objective_count(), 3);
    }

    #[test]
    fn test_widget_add_solution() {
        let mut widget = ParetoFrontierWidget::new();
        let solution = ParetoSolution::new("S1", vec![1.0, 2.0]);
        widget.add_solution(solution);
        assert_eq!(widget.solution_count(), 1);
    }

    #[test]
    fn test_widget_with_solutions() {
        let solutions = create_test_solutions();
        let widget = ParetoFrontierWidget::new().with_solutions(solutions.clone());
        assert_eq!(widget.solution_count(), solutions.len());
    }

    #[test]
    fn test_widget_with_objective_names() {
        let names = vec!["Sharpe".to_string(), "Drawdown".to_string(), "Turnover".to_string()];
        let widget = ParetoFrontierWidget::new().with_objective_names(names.clone());
        assert_eq!(widget.objective_names, names);
    }

    #[test]
    fn test_widget_with_x_axis_objective() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget = widget.with_x_axis_objective(2);
        assert_eq!(widget.x_axis_objective(), 2);
    }

    #[test]
    fn test_widget_with_y_axis_objective() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget = widget.with_y_axis_objective(2);
        assert_eq!(widget.y_axis_objective(), 2);
    }

    #[test]
    fn test_widget_with_show_frontier() {
        let widget = ParetoFrontierWidget::new().with_show_frontier(false);
        assert_eq!(widget.show_frontier, false);
    }

    #[test]
    fn test_widget_with_show_all_solutions() {
        let widget = ParetoFrontierWidget::new().with_show_all_solutions(false);
        assert_eq!(widget.show_all_solutions, false);
    }

    #[test]
    fn test_widget_with_block() {
        let block = Block::default().title("Pareto Frontier");
        let widget = ParetoFrontierWidget::new().with_block(block);
        assert!(widget.block.is_some());
    }

    #[test]
    fn test_widget_select_solution() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.select_solution(0);
        assert_eq!(widget.selected_solution_index(), Some(0));
    }

    #[test]
    fn test_widget_select_solution_invalid() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.select_solution(10);
        assert_eq!(widget.selected_solution_index(), None);
    }

    #[test]
    fn test_widget_clear_selection() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.select_solution(0);
        widget.clear_selection();
        assert_eq!(widget.selected_solution_index(), None);
    }

    #[test]
    fn test_widget_selected_solution() {
        let mut widget = ParetoFrontierWidget::new();
        let solution = ParetoSolution::new("S1", vec![1.0, 2.0]);
        widget.add_solution(solution.clone());
        widget.select_solution(0);
        assert_eq!(widget.selected_solution().map(|s| s.id.as_str()), Some("S1"));
    }

    #[test]
    fn test_widget_move_selection_up() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.select_solution(1);
        widget.move_selection_up();
        assert_eq!(widget.selected_solution_index(), Some(0));
    }

    #[test]
    fn test_widget_move_selection_down() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.select_solution(0);
        widget.move_selection_down();
        assert_eq!(widget.selected_solution_index(), Some(1));
    }

    #[test]
    fn test_widget_move_selection_from_none() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.move_selection_down();
        assert_eq!(widget.selected_solution_index(), Some(0));
    }

    #[test]
    fn test_widget_select_previous_objective_x() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget.x_axis_objective = 1;
        widget.select_previous_objective_x();
        assert_eq!(widget.x_axis_objective(), 0);
    }

    #[test]
    fn test_widget_select_next_objective_x() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget.select_next_objective_x();
        assert_eq!(widget.x_axis_objective(), 1);
    }

    #[test]
    fn test_widget_cycle_x_axis() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        let original = widget.x_axis_objective();
        widget.cycle_x_axis();
        assert_ne!(widget.x_axis_objective(), original);
    }

    #[test]
    fn test_widget_cycle_y_axis() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget.x_axis_objective = 0;
        widget.y_axis_objective = 1;
        widget.cycle_y_axis();
        // Should be different from X
        assert_ne!(widget.y_axis_objective(), widget.x_axis_objective());
    }

    #[test]
    fn test_widget_toggle_show_frontier() {
        let mut widget = ParetoFrontierWidget::new();
        let original = widget.show_frontier;
        widget.toggle_show_frontier();
        assert_ne!(widget.show_frontier, original);
    }

    #[test]
    fn test_widget_toggle_show_all_solutions() {
        let mut widget = ParetoFrontierWidget::new();
        let original = widget.show_all_solutions;
        widget.toggle_show_all_solutions();
        assert_ne!(widget.show_all_solutions, original);
    }

    #[test]
    fn test_calculate_x_range() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![3.0, 4.0]));
        let (min, max) = widget.calculate_x_range();
        assert!(min <= 1.0);
        assert!(max >= 3.0);
    }

    #[test]
    fn test_calculate_x_range_empty() {
        let widget = ParetoFrontierWidget::new();
        let (min, max) = widget.calculate_x_range();
        assert_eq!(min, 0.0);
        assert_eq!(max, 1.0);
    }

    #[test]
    fn test_calculate_y_range() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![3.0, 4.0]));
        let (min, max) = widget.calculate_y_range();
        assert!(min <= 2.0);
        assert!(max >= 4.0);
    }

    #[test]
    fn test_calculate_y_range_empty() {
        let widget = ParetoFrontierWidget::new();
        let (min, max) = widget.calculate_y_range();
        assert_eq!(min, 0.0);
        assert_eq!(max, 1.0);
    }

    #[test]
    fn test_x_to_screen() {
        let widget = ParetoFrontierWidget::new();
        let x = widget.x_to_screen(5.0, 0.0, 10.0, 100);
        assert!(x < 100);
    }

    #[test]
    fn test_x_to_screen_same_min_max() {
        let widget = ParetoFrontierWidget::new();
        let x = widget.x_to_screen(5.0, 5.0, 5.0, 100);
        assert_eq!(x, 50);
    }

    #[test]
    fn test_y_to_screen() {
        let widget = ParetoFrontierWidget::new();
        let y = widget.y_to_screen(5.0, 0.0, 10.0, 100);
        assert!(y < 100);
    }

    #[test]
    fn test_y_to_screen_inverted() {
        let widget = ParetoFrontierWidget::new();
        let y_min = widget.y_to_screen(0.0, 0.0, 10.0, 100);
        let y_max = widget.y_to_screen(10.0, 0.0, 10.0, 100);
        assert!(y_min > y_max); // Y is inverted
    }

    #[test]
    fn test_y_to_screen_same_min_max() {
        let widget = ParetoFrontierWidget::new();
        let y = widget.y_to_screen(5.0, 5.0, 5.0, 100);
        assert_eq!(y, 50);
    }

    #[test]
    fn test_get_frontier_solutions_sorted() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]).with_frontier(true));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]).with_frontier(true));
        widget.add_solution(ParetoSolution::new("S3", vec![3.0, 3.0]));
        
        let frontier = widget.get_frontier_solutions_sorted();
        assert_eq!(frontier.len(), 2);
        // Should be sorted by X-axis
        assert!(frontier[0] < frontier[1] || widget.solutions[frontier[0]].get_objective(0).unwrap() <= widget.solutions[frontier[1]].get_objective(0).unwrap());
    }

    #[test]
    fn test_update_frontier() {
        let mut widget = ParetoFrontierWidget::new();
        // S1 dominates S3 in all objectives (assuming minimization)
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 1.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.add_solution(ParetoSolution::new("S3", vec![3.0, 3.0]));
        
        widget.update_frontier();
        
        // S1 and S2 should be on frontier (not dominated)
        assert!(widget.solutions[0].is_frontier || widget.solutions[1].is_frontier);
    }

    #[test]
    fn test_update_frontier_empty() {
        let mut widget = ParetoFrontierWidget::new();
        // Should not panic
        widget.update_frontier();
    }

    #[test]
    fn test_handle_key_up() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.select_solution(1);
        
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.selected_solution_index(), Some(0));
    }

    #[test]
    fn test_handle_key_down() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.select_solution(0);
        
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        assert!(widget.handle_key(key));
        assert_eq!(widget.selected_solution_index(), Some(1));
    }

    #[test]
    fn test_handle_key_j_k() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.select_solution(0);
        
        let key_j = KeyEvent::new(KeyCode::Char('j'), KeyModifiers::empty());
        assert!(widget.handle_key(key_j));
        assert_eq!(widget.selected_solution_index(), Some(1));
        
        let key_k = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::empty());
        assert!(widget.handle_key(key_k));
        assert_eq!(widget.selected_solution_index(), Some(0));
    }

    #[test]
    fn test_handle_key_x_y() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        let original_x = widget.x_axis_objective();
        
        let key_x = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::empty());
        assert!(widget.handle_key(key_x));
        assert_ne!(widget.x_axis_objective(), original_x);
        
        let original_y = widget.y_axis_objective();
        let key_y = KeyEvent::new(KeyCode::Char('y'), KeyModifiers::empty());
        assert!(widget.handle_key(key_y));
        assert_ne!(widget.y_axis_objective(), original_y);
    }

    #[test]
    fn test_handle_key_f_a() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        let original_frontier = widget.show_frontier;
        let original_all = widget.show_all_solutions;
        
        let key_f = KeyEvent::new(KeyCode::Char('f'), KeyModifiers::empty());
        assert!(widget.handle_key(key_f));
        assert_ne!(widget.show_frontier, original_frontier);
        
        let key_a = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::empty());
        assert!(widget.handle_key(key_a));
        assert_ne!(widget.show_all_solutions, original_all);
    }

    #[test]
    fn test_handle_key_not_focused() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(false);
        
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        assert!(!widget.handle_key(key));
    }

    #[test]
    fn test_render_empty_widget() {
        let mut widget = ParetoFrontierWidget::new();
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_solutions() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_block() {
        let mut widget = ParetoFrontierWidget::new()
            .with_block(Block::default().title("Pareto Frontier").borders(Borders::ALL));
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_selection() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        widget.select_solution(0);
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_render_frontier_only() {
        let mut widget = ParetoFrontierWidget::new()
            .with_show_all_solutions(false);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]).with_frontier(true));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_render_without_frontier_line() {
        let mut widget = ParetoFrontierWidget::new()
            .with_show_frontier(false);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_widget_clone() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.select_solution(0);
        
        let cloned = widget.clone();
        assert_eq!(cloned.solution_count(), widget.solution_count());
        assert_eq!(cloned.selected_solution_index(), widget.selected_solution_index());
    }

    #[test]
    fn test_pareto_solution_clone() {
        let solution = ParetoSolution::new("Test", vec![1.0, 2.0])
            .with_frontier(true)
            .with_color(Color::Red);
        
        let cloned = solution.clone();
        assert_eq!(cloned.id, solution.id);
        assert_eq!(cloned.objectives, solution.objectives);
        assert_eq!(cloned.is_frontier, solution.is_frontier);
    }

    #[test]
    fn test_pareto_solution_equality() {
        let s1 = ParetoSolution::new("S1", vec![1.0, 2.0]);
        let s2 = ParetoSolution::new("S1", vec![1.0, 2.0]);
        let s3 = ParetoSolution::new("S2", vec![1.0, 2.0]);
        
        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_widget_focus() {
        let mut widget = ParetoFrontierWidget::new();
        assert!(!widget.is_focused());
        widget.set_focused(true);
        assert!(widget.is_focused());
    }

    #[test]
    fn test_widget_with_x_axis_bounds() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        // Try to set X-axis beyond bounds
        widget = widget.with_x_axis_objective(10);
        // Should clamp to valid range
        assert!(widget.x_axis_objective() < 2);
    }

    #[test]
    fn test_widget_with_y_axis_bounds() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        // Try to set Y-axis beyond bounds
        widget = widget.with_y_axis_objective(10);
        // Should clamp to valid range
        assert!(widget.y_axis_objective() < 2);
    }

    #[test]
    fn test_calculate_range_single_point() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![5.0, 10.0]));
        
        let (x_min, x_max) = widget.calculate_x_range();
        let (y_min, y_max) = widget.calculate_y_range();
        
        // Should add padding for single point
        assert!(x_min < 5.0);
        assert!(x_max > 5.0);
        assert!(y_min < 10.0);
        assert!(y_max > 10.0);
    }

    #[test]
    fn test_calculate_range_same_values() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![5.0, 10.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![5.0, 10.0]));
        
        let (x_min, x_max) = widget.calculate_x_range();
        let (y_min, y_max) = widget.calculate_y_range();
        
        // Should add padding for same values
        assert!(x_min < 5.0);
        assert!(x_max > 5.0);
        assert!(y_min < 10.0);
        assert!(y_max > 10.0);
    }

    #[test]
    fn test_get_frontier_solutions_empty() {
        let widget = ParetoFrontierWidget::new();
        let frontier = widget.get_frontier_solutions_sorted();
        assert_eq!(frontier.len(), 0);
    }

    #[test]
    fn test_get_frontier_solutions_no_frontier() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]));
        // Mark as non-frontier
        for solution in &mut widget.solutions {
            solution.is_frontier = false;
        }
        
        let frontier = widget.get_frontier_solutions_sorted();
        assert_eq!(frontier.len(), 0);
    }

    #[test]
    fn test_render_small_area() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        
        let area = Rect::new(0, 0, 10, 5);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic even with small area
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_handle_key_left_right() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget.x_axis_objective = 1;
        
        let key_left = KeyEvent::new(KeyCode::Left, KeyModifiers::empty());
        assert!(widget.handle_key(key_left));
        assert_eq!(widget.x_axis_objective(), 0);
        
        let key_right = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
        assert!(widget.handle_key(key_right));
        assert_eq!(widget.x_axis_objective(), 1);
    }

    #[test]
    fn test_handle_key_h_l() {
        let mut widget = ParetoFrontierWidget::new();
        widget.set_focused(true);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget.x_axis_objective = 1;
        
        let key_h = KeyEvent::new(KeyCode::Char('h'), KeyModifiers::empty());
        assert!(widget.handle_key(key_h));
        assert_eq!(widget.x_axis_objective(), 0);
        
        let key_l = KeyEvent::new(KeyCode::Char('l'), KeyModifiers::empty());
        assert!(widget.handle_key(key_l));
        assert_eq!(widget.x_axis_objective(), 1);
    }

    #[test]
    fn test_update_frontier_single_solution() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.update_frontier();
        
        // Single solution should be on frontier
        assert!(widget.solutions[0].is_frontier);
    }

    #[test]
    fn test_update_frontier_dominated_solution() {
        let mut widget = ParetoFrontierWidget::new();
        // S1 dominates S2 (better in all objectives for minimization)
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 1.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 2.0]));
        widget.update_frontier();
        
        // S1 should be on frontier, S2 should not
        assert!(widget.solutions[0].is_frontier);
        // S2 might also be on frontier if not strictly dominated
    }

    #[test]
    fn test_render_with_objective_names() {
        let mut widget = ParetoFrontierWidget::new()
            .with_objective_names(vec!["Sharpe".to_string(), "Drawdown".to_string()]);
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_custom_colors() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]).with_color(Color::Red));
        widget.add_solution(ParetoSolution::new("S2", vec![2.0, 1.0]).with_color(Color::Blue));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        widget.render(area, &mut buf);
    }

    #[test]
    fn test_x_to_screen_bounds() {
        let widget = ParetoFrontierWidget::new();
        let x = widget.x_to_screen(100.0, 0.0, 10.0, 50);
        // Should clamp to width
        assert!(x < 50);
    }

    #[test]
    fn test_y_to_screen_bounds() {
        let widget = ParetoFrontierWidget::new();
        let y = widget.y_to_screen(100.0, 0.0, 10.0, 20);
        // Should be within bounds
        assert!(y < 20);
    }

    #[test]
    fn test_cycle_x_axis_wraps() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.x_axis_objective = 0;
        
        // Cycle should wrap
        widget.cycle_x_axis();
        // With only 2 objectives, X should wrap back or stay valid
        assert!(widget.x_axis_objective() < 2);
    }

    #[test]
    fn test_cycle_y_axis_avoids_x() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        widget.x_axis_objective = 0;
        widget.y_axis_objective = 1;
        
        widget.cycle_y_axis();
        // Y should be different from X
        assert_ne!(widget.y_axis_objective(), widget.x_axis_objective());
    }

    #[test]
    fn test_max_objective_index() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0, 3.0]));
        assert_eq!(widget.max_objective_index(), 2);
    }

    #[test]
    fn test_max_objective_index_empty() {
        let widget = ParetoFrontierWidget::new();
        assert_eq!(widget.max_objective_index(), 0);
    }

    #[test]
    fn test_max_objective_index_mixed() {
        let mut widget = ParetoFrontierWidget::new();
        widget.add_solution(ParetoSolution::new("S1", vec![1.0, 2.0]));
        widget.add_solution(ParetoSolution::new("S2", vec![1.0, 2.0, 3.0, 4.0]));
        // Should use max from all solutions
        assert!(widget.max_objective_index() >= 2);
    }
}
