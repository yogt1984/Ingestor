//! Chart Widget (T-3.3)
//!
//! A reusable ASCII/Unicode chart widget for displaying data visualizations in the TUI.
//! Supports multiple chart types (Line, Bar, Scatter, Heatmap), multiple series,
//! auto-scaling, axis labels, and legends.

use ratatui::{
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
};

// ============================================================================
// Types
// ============================================================================

/// Chart widget for displaying data visualizations
pub struct ChartWidget {
    /// Chart type
    chart_type: ChartType,
    /// Data series
    series: Vec<DataSeries>,
    /// X-axis configuration
    x_axis: AxisConfig,
    /// Y-axis configuration
    y_axis: AxisConfig,
    /// Whether to show legend
    show_legend: bool,
    /// Legend position
    legend_position: LegendPosition,
    /// Block style (optional title, borders)
    block: Option<Block<'static>>,
    /// Chart width (for rendering)
    width: u16,
    /// Chart height (for rendering)
    height: u16,
    /// Color palette for series
    color_palette: Vec<Color>,
}

impl Clone for ChartWidget {
    fn clone(&self) -> Self {
        Self {
            chart_type: self.chart_type.clone(),
            series: self.series.clone(),
            x_axis: self.x_axis.clone(),
            y_axis: self.y_axis.clone(),
            show_legend: self.show_legend,
            legend_position: self.legend_position.clone(),
            block: self.block.clone(),
            width: self.width,
            height: self.height,
            color_palette: self.color_palette.clone(),
        }
    }
}

impl std::fmt::Debug for ChartWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChartWidget")
            .field("chart_type", &self.chart_type)
            .field("series_count", &self.series.len())
            .field("show_legend", &self.show_legend)
            .finish()
    }
}

/// Chart type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartType {
    /// Line chart
    Line,
    /// Bar chart
    Bar,
    /// Scatter plot
    Scatter,
    /// Heatmap
    Heatmap,
}

/// Data point in a series
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    /// X coordinate
    pub x: f64,
    /// Y coordinate
    pub y: f64,
    /// Optional label
    pub label: Option<String>,
}

impl DataPoint {
    /// Create a new data point
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            label: None,
        }
    }

    /// Create a data point with label
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

/// Data series (collection of data points)
#[derive(Debug, Clone)]
pub struct DataSeries {
    /// Series name
    pub name: String,
    /// Data points
    pub points: Vec<DataPoint>,
    /// Series color (optional, uses palette if None)
    pub color: Option<Color>,
    /// Series symbol/character for rendering
    pub symbol: Option<char>,
}

impl DataSeries {
    /// Create a new data series
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            points: Vec::new(),
            color: None,
            symbol: None,
        }
    }

    /// Add a data point
    pub fn add_point(mut self, point: DataPoint) -> Self {
        self.points.push(point);
        self
    }

    /// Add multiple data points
    pub fn with_points(mut self, points: Vec<DataPoint>) -> Self {
        self.points = points;
        self
    }

    /// Set series color
    pub fn with_color(mut self, color: Color) -> Self {
        self.color = Some(color);
        self
    }

    /// Set series symbol
    pub fn with_symbol(mut self, symbol: char) -> Self {
        self.symbol = Some(symbol);
        self
    }

    /// Get min/max X values
    pub fn x_range(&self) -> Option<(f64, f64)> {
        if self.points.is_empty() {
            return None;
        }
        let min_x = self.points.iter().map(|p| p.x).fold(f64::INFINITY, f64::min);
        let max_x = self.points.iter().map(|p| p.x).fold(f64::NEG_INFINITY, f64::max);
        Some((min_x, max_x))
    }

    /// Get min/max Y values
    pub fn y_range(&self) -> Option<(f64, f64)> {
        if self.points.is_empty() {
            return None;
        }
        let min_y = self.points.iter().map(|p| p.y).fold(f64::INFINITY, f64::min);
        let max_y = self.points.iter().map(|p| p.y).fold(f64::NEG_INFINITY, f64::max);
        Some((min_y, max_y))
    }
}

/// Axis configuration
#[derive(Debug, Clone)]
pub struct AxisConfig {
    /// Axis label
    pub label: Option<String>,
    /// Minimum value (None = auto)
    pub min: Option<f64>,
    /// Maximum value (None = auto)
    pub max: Option<f64>,
    /// Whether to show grid lines
    pub show_grid: bool,
    /// Number of tick marks
    pub ticks: usize,
}

impl Default for AxisConfig {
    fn default() -> Self {
        Self {
            label: None,
            min: None,
            max: None,
            show_grid: true,
            ticks: 5,
        }
    }
}

impl AxisConfig {
    /// Create a new axis config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set axis label
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set min value
    pub fn with_min(mut self, min: f64) -> Self {
        self.min = Some(min);
        self
    }

    /// Set max value
    pub fn with_max(mut self, max: f64) -> Self {
        self.max = Some(max);
        self
    }

    /// Set show grid
    pub fn with_show_grid(mut self, show: bool) -> Self {
        self.show_grid = show;
        self
    }

    /// Set number of ticks
    pub fn with_ticks(mut self, ticks: usize) -> Self {
        self.ticks = ticks;
        self
    }
}

/// Legend position
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegendPosition {
    /// Top
    Top,
    /// Bottom
    Bottom,
    /// Left
    Left,
    /// Right
    Right,
}

// ============================================================================
// ChartWidget Implementation
// ============================================================================

impl Default for ChartWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl ChartWidget {
    /// Create a new empty chart widget
    pub fn new() -> Self {
        Self {
            chart_type: ChartType::Line,
            series: Vec::new(),
            x_axis: AxisConfig::default(),
            y_axis: AxisConfig::default(),
            show_legend: true,
            legend_position: LegendPosition::Bottom,
            block: None,
            width: 0,
            height: 0,
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

    /// Set chart type
    pub fn with_chart_type(mut self, chart_type: ChartType) -> Self {
        self.chart_type = chart_type;
        self
    }

    /// Add a data series
    pub fn add_series(&mut self, series: DataSeries) {
        self.series.push(series);
    }

    /// Set data series
    pub fn with_series(mut self, series: Vec<DataSeries>) -> Self {
        self.series = series;
        self
    }

    /// Set X-axis configuration
    pub fn with_x_axis(mut self, axis: AxisConfig) -> Self {
        self.x_axis = axis;
        self
    }

    /// Set Y-axis configuration
    pub fn with_y_axis(mut self, axis: AxisConfig) -> Self {
        self.y_axis = axis;
        self
    }

    /// Set whether to show legend
    pub fn with_show_legend(mut self, show: bool) -> Self {
        self.show_legend = show;
        self
    }

    /// Set legend position
    pub fn with_legend_position(mut self, position: LegendPosition) -> Self {
        self.legend_position = position;
        self
    }

    /// Set block (title, borders)
    pub fn with_block(mut self, block: Block<'static>) -> Self {
        self.block = Some(block);
        self
    }

    /// Get number of series
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Clear all series
    pub fn clear(&mut self) {
        self.series.clear();
    }

    /// Calculate auto-scaling for X axis
    fn calculate_x_range(&self) -> (f64, f64) {
        if let Some(min) = self.x_axis.min {
            if let Some(max) = self.x_axis.max {
                return (min, max);
            }
        }

        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;

        for series in &self.series {
            if let Some((s_min, s_max)) = series.x_range() {
                min_x = min_x.min(s_min);
                max_x = max_x.max(s_max);
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

    /// Calculate auto-scaling for Y axis
    fn calculate_y_range(&self) -> (f64, f64) {
        if let Some(min) = self.y_axis.min {
            if let Some(max) = self.y_axis.max {
                return (min, max);
            }
        }

        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;

        for series in &self.series {
            if let Some((s_min, s_max)) = series.y_range() {
                min_y = min_y.min(s_min);
                max_y = max_y.max(s_max);
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

    /// Render the chart widget
    pub fn render(&mut self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if self.series.is_empty() {
            return;
        }

        self.width = area.width;
        self.height = area.height;

        let inner_area = if let Some(ref block) = self.block {
            let inner = block.inner(area);
            block.clone().render(area, buf);
            inner
        } else {
            area
        };

        match self.chart_type {
            ChartType::Line => self.render_line_chart(inner_area, buf),
            ChartType::Bar => self.render_bar_chart(inner_area, buf),
            ChartType::Scatter => self.render_scatter_plot(inner_area, buf),
            ChartType::Heatmap => self.render_heatmap(inner_area, buf),
        }
    }

    /// Render line chart
    fn render_line_chart(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let (x_min, x_max) = self.calculate_x_range();
        let (y_min, y_max) = self.calculate_y_range();

        let chart_area = if self.show_legend && self.legend_position == LegendPosition::Bottom {
            Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: area.height.saturating_sub(2),
            }
        } else {
            area
        };

        // Draw grid if enabled
        if self.y_axis.show_grid {
            for i in 0..=self.y_axis.ticks {
                let y_val = y_min + (y_max - y_min) * (i as f64 / self.y_axis.ticks as f64);
                let y_pos = self.y_to_screen(y_val, y_min, y_max, chart_area.height);
                if y_pos < chart_area.height {
                    for x in chart_area.x..chart_area.x + chart_area.width {
                        if x < buf.area.width && y_pos < buf.area.height {
                            buf.get_mut(x, chart_area.y + y_pos)
                                .set_char('─')
                                .set_style(Style::default().fg(Color::DarkGray));
                        }
                    }
                }
            }
        }

        // Draw series
        for (series_idx, series) in self.series.iter().enumerate() {
            if series.points.is_empty() {
                continue;
            }

            let color = series.color.unwrap_or_else(|| {
                self.color_palette[series_idx % self.color_palette.len()]
            });
            let symbol = series.symbol.unwrap_or('●');

            // Sort points by X
            let mut sorted_points = series.points.clone();
            sorted_points.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap_or(std::cmp::Ordering::Equal));

            // Draw line connecting points
            for i in 0..sorted_points.len().saturating_sub(1) {
                let p1 = &sorted_points[i];
                let p2 = &sorted_points[i + 1];

                let x1 = self.x_to_screen(p1.x, x_min, x_max, chart_area.width);
                let y1 = self.y_to_screen(p1.y, y_min, y_max, chart_area.height);
                let x2 = self.x_to_screen(p2.x, x_min, x_max, chart_area.width);
                let y2 = self.y_to_screen(p2.y, y_min, y_max, chart_area.height);

                // Draw line between points
                self.draw_line(
                    chart_area.x + x1,
                    chart_area.y + y1,
                    chart_area.x + x2,
                    chart_area.y + y2,
                    symbol,
                    color,
                    buf,
                );
            }

            // Draw points
            for point in &sorted_points {
                let x = self.x_to_screen(point.x, x_min, x_max, chart_area.width);
                let y = self.y_to_screen(point.y, y_min, y_max, chart_area.height);
                let screen_x = chart_area.x + x;
                let screen_y = chart_area.y + y;

                if screen_x < buf.area.width && screen_y < buf.area.height {
                    buf.get_mut(screen_x, screen_y)
                        .set_char(symbol)
                        .set_style(Style::default().fg(color));
                }
            }
        }

        // Draw axes labels
        self.draw_axes_labels(chart_area, x_min, x_max, y_min, y_max, buf);

        // Draw legend
        if self.show_legend {
            self.draw_legend(area, buf);
        }
    }

    /// Render bar chart
    fn render_bar_chart(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let (x_min, x_max) = self.calculate_x_range();
        let (y_min, y_max) = self.calculate_y_range();

        let chart_area = if self.show_legend && self.legend_position == LegendPosition::Bottom {
            Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: area.height.saturating_sub(2),
            }
        } else {
            area
        };

        // Draw grid
        if self.y_axis.show_grid {
            for i in 0..=self.y_axis.ticks {
                let y_val = y_min + (y_max - y_min) * (i as f64 / self.y_axis.ticks as f64);
                let y_pos = self.y_to_screen(y_val, y_min, y_max, chart_area.height);
                if y_pos < chart_area.height {
                    for x in chart_area.x..chart_area.x + chart_area.width {
                        if x < buf.area.width && y_pos < buf.area.height {
                            buf.get_mut(x, chart_area.y + y_pos)
                                .set_char('─')
                                .set_style(Style::default().fg(Color::DarkGray));
                        }
                    }
                }
            }
        }

        // Draw bars for each series
        for (series_idx, series) in self.series.iter().enumerate() {
            if series.points.is_empty() {
                continue;
            }

            let color = series.color.unwrap_or_else(|| {
                self.color_palette[series_idx % self.color_palette.len()]
            });

            let bar_width = (chart_area.width as f64 / series.points.len() as f64 / self.series.len() as f64) as u16;
            let bar_spacing = 1;

            for (point_idx, point) in series.points.iter().enumerate() {
                let x = self.x_to_screen(point.x, x_min, x_max, chart_area.width);
                let y = self.y_to_screen(point.y, y_min, y_max, chart_area.height);
                let zero_y = self.y_to_screen(0.0, y_min, y_max, chart_area.height);

                let bar_x = chart_area.x + x + (series_idx as u16 * (bar_width + bar_spacing));
                let bar_top = chart_area.y + y;
                let bar_bottom = chart_area.y + zero_y;

                // Draw bar
                for bar_y in bar_top.min(bar_bottom)..=bar_top.max(bar_bottom) {
                    for bar_x_offset in 0..bar_width {
                        let screen_x = bar_x + bar_x_offset;
                        if screen_x < buf.area.width && bar_y < buf.area.height {
                            buf.get_mut(screen_x, bar_y)
                                .set_char('█')
                                .set_style(Style::default().fg(color));
                        }
                    }
                }
            }
        }

        // Draw axes labels
        self.draw_axes_labels(chart_area, x_min, x_max, y_min, y_max, buf);

        // Draw legend
        if self.show_legend {
            self.draw_legend(area, buf);
        }
    }

    /// Render scatter plot
    fn render_scatter_plot(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let (x_min, x_max) = self.calculate_x_range();
        let (y_min, y_max) = self.calculate_y_range();

        let chart_area = if self.show_legend && self.legend_position == LegendPosition::Bottom {
            Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: area.height.saturating_sub(2),
            }
        } else {
            area
        };

        // Draw grid
        if self.y_axis.show_grid {
            for i in 0..=self.y_axis.ticks {
                let y_val = y_min + (y_max - y_min) * (i as f64 / self.y_axis.ticks as f64);
                let y_pos = self.y_to_screen(y_val, y_min, y_max, chart_area.height);
                if y_pos < chart_area.height {
                    for x in chart_area.x..chart_area.x + chart_area.width {
                        if x < buf.area.width && y_pos < buf.area.height {
                            buf.get_mut(x, chart_area.y + y_pos)
                                .set_char('─')
                                .set_style(Style::default().fg(Color::DarkGray));
                        }
                    }
                }
            }
        }

        // Draw points for each series
        for (series_idx, series) in self.series.iter().enumerate() {
            if series.points.is_empty() {
                continue;
            }

            let color = series.color.unwrap_or_else(|| {
                self.color_palette[series_idx % self.color_palette.len()]
            });
            let symbol = series.symbol.unwrap_or('●');

            for point in &series.points {
                let x = self.x_to_screen(point.x, x_min, x_max, chart_area.width);
                let y = self.y_to_screen(point.y, y_min, y_max, chart_area.height);
                let screen_x = chart_area.x + x;
                let screen_y = chart_area.y + y;

                if screen_x < buf.area.width && screen_y < buf.area.height {
                    buf.get_mut(screen_x, screen_y)
                        .set_char(symbol)
                        .set_style(Style::default().fg(color));
                }
            }
        }

        // Draw axes labels
        self.draw_axes_labels(chart_area, x_min, x_max, y_min, y_max, buf);

        // Draw legend
        if self.show_legend {
            self.draw_legend(area, buf);
        }
    }

    /// Render heatmap
    fn render_heatmap(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        // Heatmap requires 2D data, simplified implementation
        if self.series.is_empty() || self.series[0].points.is_empty() {
            return;
        }

        let (x_min, x_max) = self.calculate_x_range();
        let (y_min, y_max) = self.calculate_y_range();

        // Use first series as heatmap data
        let series = &self.series[0];
        let heatmap_chars = [' ', '░', '▒', '▓', '█'];

        for point in &series.points {
            let x = self.x_to_screen(point.x, x_min, x_max, area.width);
            let y = self.y_to_screen(point.y, y_min, y_max, area.height);
            let screen_x = area.x + x;
            let screen_y = area.y + y;

            // Normalize value to 0-1 range
            let normalized = (point.y - y_min) / (y_max - y_min);
            let char_idx = ((normalized * (heatmap_chars.len() - 1) as f64) as usize)
                .min(heatmap_chars.len() - 1);

            if screen_x < buf.area.width && screen_y < buf.area.height {
                buf.get_mut(screen_x, screen_y)
                    .set_char(heatmap_chars[char_idx])
                    .set_style(Style::default().fg(Color::Yellow));
            }
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
    fn draw_axes_labels(&self, area: Rect, x_min: f64, x_max: f64, y_min: f64, y_max: f64, buf: &mut ratatui::buffer::Buffer) {
        // X-axis label
        if let Some(ref label) = self.x_axis.label {
            let label_y = area.y + area.height.saturating_sub(1);
            if label_y < buf.area.height {
                let label_text = format!("{}", label);
                let label_x = area.x + area.width.saturating_sub(label_text.len() as u16) / 2;
                let paragraph = Paragraph::new(label_text.as_str());
                paragraph.render(
                    Rect {
                        x: label_x,
                        y: label_y,
                        width: label_text.len() as u16,
                        height: 1,
                    },
                    buf,
                );
            }
        }

        // Y-axis label
        if let Some(ref label) = self.y_axis.label {
            // Y-axis labels are harder in ASCII, skip for now
        }
    }

    /// Draw legend
    fn draw_legend(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if self.series.is_empty() {
            return;
        }

        let legend_y = match self.legend_position {
            LegendPosition::Bottom => area.y + area.height.saturating_sub(1),
            LegendPosition::Top => area.y,
            _ => area.y + area.height.saturating_sub(1), // Default to bottom
        };

        if legend_y >= buf.area.height {
            return;
        }

        let mut x_offset = area.x;
        for (idx, series) in self.series.iter().enumerate() {
            let color = series.color.unwrap_or_else(|| {
                self.color_palette[idx % self.color_palette.len()]
            });
            let symbol = series.symbol.unwrap_or('●');
            let legend_text = format!("{} {} ", symbol, series.name);

            if x_offset + legend_text.len() as u16 > area.x + area.width {
                break;
            }

            let spans = vec![
                Span::styled(
                    format!("{} ", symbol),
                    Style::default().fg(color),
                ),
                Span::raw(series.name.as_str()),
            ];

            let line = Line::from(spans);
            let paragraph = Paragraph::new(line);
            paragraph.render(
                Rect {
                    x: x_offset,
                    y: legend_y,
                    width: legend_text.len() as u16,
                    height: 1,
                },
                buf,
            );

            x_offset += legend_text.len() as u16;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_series() -> DataSeries {
        DataSeries::new("Test Series")
            .with_points(vec![
                DataPoint::new(0.0, 1.0),
                DataPoint::new(1.0, 2.0),
                DataPoint::new(2.0, 1.5),
                DataPoint::new(3.0, 3.0),
            ])
    }

    #[test]
    fn test_chart_widget_creation() {
        let chart = ChartWidget::new();
        assert_eq!(chart.series_count(), 0);
        assert_eq!(chart.chart_type, ChartType::Line);
    }

    #[test]
    fn test_data_point_creation() {
        let point = DataPoint::new(1.0, 2.0);
        assert_eq!(point.x, 1.0);
        assert_eq!(point.y, 2.0);
        assert_eq!(point.label, None);
    }

    #[test]
    fn test_data_point_with_label() {
        let point = DataPoint::new(1.0, 2.0).with_label("Test");
        assert_eq!(point.label, Some("Test".to_string()));
    }

    #[test]
    fn test_data_series_creation() {
        let series = DataSeries::new("Test");
        assert_eq!(series.name, "Test");
        assert_eq!(series.points.len(), 0);
    }

    #[test]
    fn test_data_series_add_point() {
        let series = DataSeries::new("Test")
            .add_point(DataPoint::new(1.0, 2.0));
        assert_eq!(series.points.len(), 1);
    }

    #[test]
    fn test_data_series_with_points() {
        let points = vec![
            DataPoint::new(1.0, 2.0),
            DataPoint::new(2.0, 3.0),
        ];
        let series = DataSeries::new("Test").with_points(points.clone());
        assert_eq!(series.points.len(), 2);
    }

    #[test]
    fn test_data_series_with_color() {
        let series = DataSeries::new("Test").with_color(Color::Red);
        assert_eq!(series.color, Some(Color::Red));
    }

    #[test]
    fn test_data_series_with_symbol() {
        let series = DataSeries::new("Test").with_symbol('*');
        assert_eq!(series.symbol, Some('*'));
    }

    #[test]
    fn test_data_series_x_range() {
        let series = create_test_series();
        let range = series.x_range();
        assert!(range.is_some());
        let (min, max) = range.unwrap();
        assert_eq!(min, 0.0);
        assert_eq!(max, 3.0);
    }

    #[test]
    fn test_data_series_x_range_empty() {
        let series = DataSeries::new("Test");
        assert_eq!(series.x_range(), None);
    }

    #[test]
    fn test_data_series_y_range() {
        let series = create_test_series();
        let range = series.y_range();
        assert!(range.is_some());
        let (min, max) = range.unwrap();
        assert_eq!(min, 1.0);
        assert_eq!(max, 3.0);
    }

    #[test]
    fn test_data_series_y_range_empty() {
        let series = DataSeries::new("Test");
        assert_eq!(series.y_range(), None);
    }

    #[test]
    fn test_axis_config_default() {
        let axis = AxisConfig::new();
        assert_eq!(axis.label, None);
        assert_eq!(axis.min, None);
        assert_eq!(axis.max, None);
        assert_eq!(axis.show_grid, true);
        assert_eq!(axis.ticks, 5);
    }

    #[test]
    fn test_axis_config_with_label() {
        let axis = AxisConfig::new().with_label("X Axis");
        assert_eq!(axis.label, Some("X Axis".to_string()));
    }

    #[test]
    fn test_axis_config_with_min_max() {
        let axis = AxisConfig::new()
            .with_min(0.0)
            .with_max(100.0);
        assert_eq!(axis.min, Some(0.0));
        assert_eq!(axis.max, Some(100.0));
    }

    #[test]
    fn test_axis_config_with_show_grid() {
        let axis = AxisConfig::new().with_show_grid(false);
        assert_eq!(axis.show_grid, false);
    }

    #[test]
    fn test_axis_config_with_ticks() {
        let axis = AxisConfig::new().with_ticks(10);
        assert_eq!(axis.ticks, 10);
    }

    #[test]
    fn test_chart_widget_with_chart_type() {
        let chart = ChartWidget::new().with_chart_type(ChartType::Bar);
        assert_eq!(chart.chart_type, ChartType::Bar);
    }

    #[test]
    fn test_chart_widget_add_series() {
        let mut chart = ChartWidget::new();
        chart.add_series(create_test_series());
        assert_eq!(chart.series_count(), 1);
    }

    #[test]
    fn test_chart_widget_with_series() {
        let series = vec![create_test_series()];
        let chart = ChartWidget::new().with_series(series.clone());
        assert_eq!(chart.series_count(), 1);
    }

    #[test]
    fn test_chart_widget_with_x_axis() {
        let axis = AxisConfig::new().with_label("X");
        let chart = ChartWidget::new().with_x_axis(axis);
        assert_eq!(chart.x_axis.label, Some("X".to_string()));
    }

    #[test]
    fn test_chart_widget_with_y_axis() {
        let axis = AxisConfig::new().with_label("Y");
        let chart = ChartWidget::new().with_y_axis(axis);
        assert_eq!(chart.y_axis.label, Some("Y".to_string()));
    }

    #[test]
    fn test_chart_widget_with_show_legend() {
        let chart = ChartWidget::new().with_show_legend(false);
        assert_eq!(chart.show_legend, false);
    }

    #[test]
    fn test_chart_widget_with_legend_position() {
        let chart = ChartWidget::new().with_legend_position(LegendPosition::Top);
        assert_eq!(chart.legend_position, LegendPosition::Top);
    }

    #[test]
    fn test_chart_widget_with_block() {
        let block = Block::default().title("Chart");
        let chart = ChartWidget::new().with_block(block);
        assert!(chart.block.is_some());
    }

    #[test]
    fn test_chart_widget_clear() {
        let mut chart = ChartWidget::new();
        chart.add_series(create_test_series());
        assert_eq!(chart.series_count(), 1);
        chart.clear();
        assert_eq!(chart.series_count(), 0);
    }

    #[test]
    fn test_calculate_x_range() {
        let mut chart = ChartWidget::new();
        chart.add_series(create_test_series());
        let (min, max) = chart.calculate_x_range();
        assert!(min <= 0.0);
        assert!(max >= 3.0);
    }

    #[test]
    fn test_calculate_x_range_with_axis_config() {
        let mut chart = ChartWidget::new();
        chart.x_axis.min = Some(0.0);
        chart.x_axis.max = Some(10.0);
        chart.add_series(create_test_series());
        let (min, max) = chart.calculate_x_range();
        assert_eq!(min, 0.0);
        assert_eq!(max, 10.0);
    }

    #[test]
    fn test_calculate_y_range() {
        let mut chart = ChartWidget::new();
        chart.add_series(create_test_series());
        let (min, max) = chart.calculate_y_range();
        assert!(min <= 1.0);
        assert!(max >= 3.0);
    }

    #[test]
    fn test_calculate_y_range_with_axis_config() {
        let mut chart = ChartWidget::new();
        chart.y_axis.min = Some(0.0);
        chart.y_axis.max = Some(5.0);
        chart.add_series(create_test_series());
        let (min, max) = chart.calculate_y_range();
        assert_eq!(min, 0.0);
        assert_eq!(max, 5.0);
    }

    #[test]
    fn test_calculate_x_range_empty() {
        let chart = ChartWidget::new();
        let (min, max) = chart.calculate_x_range();
        assert_eq!(min, 0.0);
        assert_eq!(max, 1.0);
    }

    #[test]
    fn test_calculate_y_range_empty() {
        let chart = ChartWidget::new();
        let (min, max) = chart.calculate_y_range();
        assert_eq!(min, 0.0);
        assert_eq!(max, 1.0);
    }

    #[test]
    fn test_x_to_screen() {
        let chart = ChartWidget::new();
        let x = chart.x_to_screen(5.0, 0.0, 10.0, 100);
        assert!(x < 100);
    }

    #[test]
    fn test_x_to_screen_edge_cases() {
        let chart = ChartWidget::new();
        let x_min = chart.x_to_screen(0.0, 0.0, 10.0, 100);
        let x_max = chart.x_to_screen(10.0, 0.0, 10.0, 100);
        assert!(x_min < x_max);
    }

    #[test]
    fn test_x_to_screen_same_min_max() {
        let chart = ChartWidget::new();
        let x = chart.x_to_screen(5.0, 5.0, 5.0, 100);
        assert_eq!(x, 50);
    }

    #[test]
    fn test_y_to_screen() {
        let chart = ChartWidget::new();
        let y = chart.y_to_screen(5.0, 0.0, 10.0, 100);
        assert!(y < 100);
    }

    #[test]
    fn test_y_to_screen_inverted() {
        let chart = ChartWidget::new();
        let y_min = chart.y_to_screen(0.0, 0.0, 10.0, 100);
        let y_max = chart.y_to_screen(10.0, 0.0, 10.0, 100);
        assert!(y_min > y_max); // Y is inverted
    }

    #[test]
    fn test_y_to_screen_same_min_max() {
        let chart = ChartWidget::new();
        let y = chart.y_to_screen(5.0, 5.0, 5.0, 100);
        // When min == max, should return height / 2 (middle)
        assert_eq!(y, 50); // height / 2
    }

    #[test]
    fn test_render_empty_chart() {
        let mut chart = ChartWidget::new();
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_line_chart() {
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Line);
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_bar_chart() {
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Bar);
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_scatter_plot() {
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Scatter);
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_heatmap() {
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Heatmap);
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_block() {
        let mut chart = ChartWidget::new()
            .with_block(Block::default().title("Chart").borders(Borders::ALL));
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_legend() {
        let mut chart = ChartWidget::new()
            .with_show_legend(true);
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_without_legend() {
        let mut chart = ChartWidget::new()
            .with_show_legend(false);
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_multiple_series() {
        let mut chart = ChartWidget::new();
        chart.add_series(create_test_series());
        chart.add_series(DataSeries::new("Series 2")
            .with_points(vec![
                DataPoint::new(0.0, 2.0),
                DataPoint::new(1.0, 3.0),
            ]));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_axis_labels() {
        let mut chart = ChartWidget::new()
            .with_x_axis(AxisConfig::new().with_label("X Axis"))
            .with_y_axis(AxisConfig::new().with_label("Y Axis"));
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_grid() {
        let mut chart = ChartWidget::new()
            .with_y_axis(AxisConfig::new().with_show_grid(true));
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_render_without_grid() {
        let mut chart = ChartWidget::new()
            .with_y_axis(AxisConfig::new().with_show_grid(false));
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_chart_type_equality() {
        assert_eq!(ChartType::Line, ChartType::Line);
        assert_ne!(ChartType::Line, ChartType::Bar);
    }

    #[test]
    fn test_legend_position_equality() {
        assert_eq!(LegendPosition::Bottom, LegendPosition::Bottom);
        assert_ne!(LegendPosition::Bottom, LegendPosition::Top);
    }

    #[test]
    fn test_data_point_equality() {
        let p1 = DataPoint::new(1.0, 2.0);
        let p2 = DataPoint::new(1.0, 2.0);
        let p3 = DataPoint::new(1.0, 3.0);
        
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_chart_widget_clone() {
        let mut chart = ChartWidget::new()
            .with_chart_type(ChartType::Bar);
        chart.add_series(create_test_series());
        
        let cloned = chart.clone();
        assert_eq!(cloned.chart_type, chart.chart_type);
        assert_eq!(cloned.series_count(), chart.series_count());
    }

    #[test]
    fn test_data_series_clone() {
        let series = create_test_series();
        let cloned = series.clone();
        assert_eq!(cloned.name, series.name);
        assert_eq!(cloned.points.len(), series.points.len());
    }

    #[test]
    fn test_render_small_area() {
        let mut chart = ChartWidget::new();
        chart.add_series(create_test_series());
        
        let area = Rect::new(0, 0, 10, 5);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic even with small area
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_calculate_range_with_single_point() {
        let mut chart = ChartWidget::new();
        chart.add_series(DataSeries::new("Test")
            .with_points(vec![DataPoint::new(5.0, 10.0)]));
        
        let (x_min, x_max) = chart.calculate_x_range();
        let (y_min, y_max) = chart.calculate_y_range();
        
        // Should add padding for single point
        assert!(x_min < 5.0);
        assert!(x_max > 5.0);
        assert!(y_min < 10.0);
        assert!(y_max > 10.0);
    }

    #[test]
    fn test_calculate_range_with_same_values() {
        let mut chart = ChartWidget::new();
        chart.add_series(DataSeries::new("Test")
            .with_points(vec![
                DataPoint::new(5.0, 10.0),
                DataPoint::new(5.0, 10.0),
            ]));
        
        let (x_min, x_max) = chart.calculate_x_range();
        let (y_min, y_max) = chart.calculate_y_range();
        
        // Should add padding for same values
        assert!(x_min < 5.0);
        assert!(x_max > 5.0);
        assert!(y_min < 10.0);
        assert!(y_max > 10.0);
    }

    #[test]
    fn test_data_series_with_custom_color() {
        let series = DataSeries::new("Test")
            .with_color(Color::Cyan)
            .with_points(vec![DataPoint::new(1.0, 2.0)]);
        assert_eq!(series.color, Some(Color::Cyan));
    }

    #[test]
    fn test_data_series_with_custom_symbol() {
        let series = DataSeries::new("Test")
            .with_symbol('*')
            .with_points(vec![DataPoint::new(1.0, 2.0)]);
        assert_eq!(series.symbol, Some('*'));
    }

    #[test]
    fn test_all_chart_types() {
        for chart_type in [ChartType::Line, ChartType::Bar, ChartType::Scatter, ChartType::Heatmap] {
            let mut chart = ChartWidget::new().with_chart_type(chart_type);
            chart.add_series(create_test_series());
            
            let area = Rect::new(0, 0, 50, 20);
            let mut buf = ratatui::buffer::Buffer::empty(area);
            chart.render(area, &mut buf);
        }
    }

    #[test]
    fn test_all_legend_positions() {
        for position in [LegendPosition::Top, LegendPosition::Bottom, LegendPosition::Left, LegendPosition::Right] {
            let mut chart = ChartWidget::new().with_legend_position(position);
            chart.add_series(create_test_series());
            
            let area = Rect::new(0, 0, 50, 20);
            let mut buf = ratatui::buffer::Buffer::empty(area);
            chart.render(area, &mut buf);
        }
    }

    #[test]
    fn test_x_to_screen_bounds() {
        let chart = ChartWidget::new();
        let x = chart.x_to_screen(100.0, 0.0, 10.0, 50);
        // Should clamp to width
        assert!(x < 50);
    }

    #[test]
    fn test_y_to_screen_bounds() {
        let chart = ChartWidget::new();
        let y = chart.y_to_screen(100.0, 0.0, 10.0, 20);
        // Should be within bounds
        assert!(y < 20);
    }

    #[test]
    fn test_render_with_empty_series() {
        let mut chart = ChartWidget::new();
        chart.add_series(DataSeries::new("Empty"));
        
        let area = Rect::new(0, 0, 50, 20);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        chart.render(area, &mut buf);
    }

    #[test]
    fn test_axis_config_clone() {
        let axis = AxisConfig::new()
            .with_label("Test")
            .with_min(0.0)
            .with_max(100.0);
        let cloned = axis.clone();
        assert_eq!(cloned.label, axis.label);
        assert_eq!(cloned.min, axis.min);
        assert_eq!(cloned.max, axis.max);
    }
}
