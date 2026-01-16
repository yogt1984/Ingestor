//! Metrics Dashboard Widget (T-3.1)
//!
//! A reusable widget for displaying key metrics in various layouts (Grid, List, Cards).
//! Supports different metric value types, color coding for trends, and visual indicators.

use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Widget},
    Frame,
};
use std::fmt::Display;

// ============================================================================
// Types
// ============================================================================

/// Metrics dashboard widget for displaying key metrics
pub struct MetricsDashboardWidget {
    /// List of metrics to display
    metrics: Vec<Metric>,
    /// Layout style
    layout: LayoutStyle,
    /// Number of columns for grid layout
    grid_columns: usize,
    /// Whether to show trend indicators
    show_trends: bool,
    /// Block style (optional title, borders)
    block: Option<Block<'static>>,
    /// Maximum width for card layout
    card_max_width: u16,
}

impl Clone for MetricsDashboardWidget {
    fn clone(&self) -> Self {
        Self {
            metrics: self.metrics.clone(),
            layout: self.layout.clone(),
            grid_columns: self.grid_columns,
            show_trends: self.show_trends,
            block: self.block.clone(),
            card_max_width: self.card_max_width,
        }
    }
}

impl std::fmt::Debug for MetricsDashboardWidget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsDashboardWidget")
            .field("metrics_count", &self.metrics.len())
            .field("layout", &self.layout)
            .field("grid_columns", &self.grid_columns)
            .field("show_trends", &self.show_trends)
            .finish()
    }
}

/// Individual metric to display
#[derive(Debug, Clone)]
pub struct Metric {
    /// Metric name/label
    pub name: String,
    /// Metric value
    pub value: MetricValue,
    /// Format style
    pub format: MetricFormat,
    /// Trend indicator (if applicable)
    pub trend: Option<Trend>,
    /// Custom color (overrides trend color)
    pub color: Option<Color>,
    /// Optional description/tooltip
    pub description: Option<String>,
}

impl Metric {
    /// Create a new metric
    pub fn new(name: impl Into<String>, value: MetricValue) -> Self {
        Self {
            name: name.into(),
            value,
            format: MetricFormat::default(),
            trend: None,
            color: None,
            description: None,
        }
    }

    /// Set format style
    pub fn with_format(mut self, format: MetricFormat) -> Self {
        self.format = format;
        self
    }

    /// Set trend indicator
    pub fn with_trend(mut self, trend: Trend) -> Self {
        self.trend = Some(trend);
        self
    }

    /// Set custom color
    pub fn with_color(mut self, color: Color) -> Self {
        self.color = Some(color);
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Get display color (trend color or custom color)
    pub fn display_color(&self) -> Option<Color> {
        self.color.or_else(|| self.trend.map(|t| t.color()))
    }

    /// Format value as string
    pub fn format_value(&self) -> String {
        self.format.format(&self.value)
    }
}

/// Metric value types
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// Numeric value (f64)
    Number(f64),
    /// Percentage value (0.0-100.0)
    Percentage(f64),
    /// Integer value
    Integer(i64),
    /// String value
    String(String),
    /// Boolean value
    Boolean(bool),
}

impl Display for MetricValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{}", n),
            Self::Percentage(p) => write!(f, "{:.2}%", p),
            Self::Integer(i) => write!(f, "{}", i),
            Self::String(s) => write!(f, "{}", s),
            Self::Boolean(b) => write!(f, "{}", if *b { "Yes" } else { "No" }),
        }
    }
}

/// Format style for metric values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricFormat {
    /// Default format
    Default,
    /// Currency format
    Currency,
    /// Scientific notation
    Scientific,
    /// Compact format (K, M, B suffixes)
    Compact,
    /// Custom decimal places
    Decimal(usize),
}

impl Default for MetricFormat {
    fn default() -> Self {
        Self::Default
    }
}

impl MetricFormat {
    /// Format a metric value according to this format
    pub fn format(&self, value: &MetricValue) -> String {
        match (self, value) {
            (Self::Default, _) => value.to_string(),
            (Self::Currency, MetricValue::Number(n)) => format!("${:.2}", n),
            (Self::Currency, MetricValue::Integer(i)) => format!("${}", i),
            (Self::Currency, _) => value.to_string(),
            (Self::Scientific, MetricValue::Number(n)) => format!("{:.2e}", n),
            (Self::Scientific, MetricValue::Integer(i)) => format!("{:.2e}", *i as f64),
            (Self::Scientific, _) => value.to_string(),
            (Self::Compact, MetricValue::Number(n)) => Self::format_compact(*n),
            (Self::Compact, MetricValue::Integer(i)) => Self::format_compact(*i as f64),
            (Self::Compact, _) => value.to_string(),
            (Self::Decimal(places), MetricValue::Number(n)) => {
                format!("{:.1$}", n, places)
            }
            (Self::Decimal(places), MetricValue::Percentage(p)) => {
                format!("{:.1$}%", p, places)
            }
            (Self::Decimal(_), _) => value.to_string(),
        }
    }

    fn format_compact(value: f64) -> String {
        let abs = value.abs();
        let sign = if value < 0.0 { "-" } else { "" };
        
        if abs >= 1_000_000_000.0 {
            format!("{}{:.2}B", sign, abs / 1_000_000_000.0)
        } else if abs >= 1_000_000.0 {
            format!("{}{:.2}M", sign, abs / 1_000_000.0)
        } else if abs >= 1_000.0 {
            format!("{}{:.2}K", sign, abs / 1_000.0)
        } else {
            format!("{:.2}", value)
        }
    }
}

/// Trend indicator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trend {
    /// Upward trend (positive)
    Up,
    /// Downward trend (negative)
    Down,
    /// Neutral/stable trend
    Neutral,
}

impl Trend {
    /// Get color for trend
    pub fn color(self) -> Color {
        match self {
            Self::Up => Color::Green,
            Self::Down => Color::Red,
            Self::Neutral => Color::Yellow,
        }
    }

    /// Get symbol for trend
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Up => "↑",
            Self::Down => "↓",
            Self::Neutral => "→",
        }
    }
}

/// Layout style for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutStyle {
    /// Grid layout (multiple columns)
    Grid,
    /// List layout (single column, vertical)
    List,
    /// Card layout (boxed metrics)
    Cards,
}

// ============================================================================
// MetricsDashboardWidget Implementation
// ============================================================================

impl Default for MetricsDashboardWidget {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsDashboardWidget {
    /// Create a new empty metrics dashboard
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
            layout: LayoutStyle::Grid,
            grid_columns: 3,
            show_trends: true,
            block: None,
            card_max_width: 20,
        }
    }

    /// Add a metric
    pub fn add_metric(&mut self, metric: Metric) {
        self.metrics.push(metric);
    }

    /// Set metrics
    pub fn with_metrics(mut self, metrics: Vec<Metric>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Set layout style
    pub fn with_layout(mut self, layout: LayoutStyle) -> Self {
        self.layout = layout;
        self
    }

    /// Set number of grid columns
    pub fn with_grid_columns(mut self, columns: usize) -> Self {
        self.grid_columns = columns.max(1);
        self
    }

    /// Set whether to show trends
    pub fn with_show_trends(mut self, show: bool) -> Self {
        self.show_trends = show;
        self
    }

    /// Set block (title, borders)
    pub fn with_block(mut self, block: Block<'static>) -> Self {
        self.block = Some(block);
        self
    }

    /// Set card max width
    pub fn with_card_max_width(mut self, width: u16) -> Self {
        self.card_max_width = width;
        self
    }

    /// Get number of metrics
    pub fn metric_count(&self) -> usize {
        self.metrics.len()
    }

    /// Clear all metrics
    pub fn clear(&mut self) {
        self.metrics.clear();
    }

    /// Render the metrics dashboard
    pub fn render(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if self.metrics.is_empty() {
            return;
        }

        let inner_area = if let Some(ref block) = self.block {
            let inner = block.inner(area);
            block.clone().render(area, buf);
            inner
        } else {
            area
        };

        match self.layout {
            LayoutStyle::Grid => self.render_grid(inner_area, buf),
            LayoutStyle::List => self.render_list(inner_area, buf),
            LayoutStyle::Cards => self.render_cards(inner_area, buf),
        }
    }

    /// Render grid layout
    fn render_grid(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let rows = (self.metrics.len() + self.grid_columns - 1) / self.grid_columns;
        let constraints: Vec<Constraint> = (0..self.grid_columns)
            .map(|_| Constraint::Percentage(100 / self.grid_columns as u16))
            .collect();

        for row_idx in 0..rows {
            let row_height = if area.height > row_idx as u16 {
                1
            } else {
                0
            };

            if row_height == 0 {
                break;
            }

            let row_area = Rect {
                x: area.x,
                y: area.y + row_idx as u16,
                width: area.width,
                height: row_height,
            };

            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints(&constraints)
                .split(row_area);

            for (col_idx, chunk) in chunks.iter().enumerate() {
                let metric_idx = row_idx * self.grid_columns + col_idx;
                if metric_idx < self.metrics.len() {
                    self.render_metric(&self.metrics[metric_idx], *chunk, buf);
                }
            }
        }
    }

    /// Render list layout
    fn render_list(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let constraints: Vec<Constraint> = self
            .metrics
            .iter()
            .take(area.height as usize)
            .map(|_| Constraint::Length(1))
            .collect();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(&constraints)
            .split(area);

        for (idx, chunk) in chunks.iter().enumerate() {
            if idx < self.metrics.len() {
                self.render_metric(&self.metrics[idx], *chunk, buf);
            }
        }
    }

    /// Render cards layout
    fn render_cards(&self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let card_width = self.card_max_width.min(area.width);
        let cards_per_row = (area.width / card_width).max(1) as usize;
        let rows = (self.metrics.len() + cards_per_row - 1) / cards_per_row;

        for row_idx in 0..rows {
            let row_y = area.y + (row_idx * 3) as u16; // 3 lines per card
            if row_y >= area.y + area.height {
                break;
            }

            for col_idx in 0..cards_per_row {
                let metric_idx = row_idx * cards_per_row + col_idx;
                if metric_idx >= self.metrics.len() {
                    break;
                }

                let card_x = area.x + (col_idx as u16 * card_width);
                let card_area = Rect {
                    x: card_x,
                    y: row_y,
                    width: card_width,
                    height: 3.min(area.height.saturating_sub(row_y - area.y)),
                };

                self.render_metric_card(&self.metrics[metric_idx], card_area, buf);
            }
        }
    }

    /// Render a single metric
    fn render_metric(&self, metric: &Metric, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        let color = metric.display_color();
        let value_text = metric.format_value();
        let trend_symbol = if self.show_trends {
            metric.trend.map(|t| t.symbol()).unwrap_or("")
        } else {
            ""
        };

        let mut spans = vec![
            Span::styled(
                format!("{}: ", metric.name),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ];

        if !trend_symbol.is_empty() {
            spans.push(Span::styled(
                trend_symbol,
                Style::default().fg(color.unwrap_or(Color::White)),
            ));
            spans.push(Span::raw(" "));
        }

        spans.push(Span::styled(
            value_text,
            Style::default().fg(color.unwrap_or(Color::White)),
        ));

        let line = Line::from(spans);
        let paragraph = Paragraph::new(line);
        paragraph.render(area, buf);
    }

    /// Render a metric as a card
    fn render_metric_card(&self, metric: &Metric, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        if area.height < 3 {
            return;
        }

        let color = metric.display_color();
        let value_text = metric.format_value();
        let trend_symbol = if self.show_trends {
            metric.trend.map(|t| t.symbol()).unwrap_or("")
        } else {
            ""
        };

        // Card border
        let block = Block::default()
            .borders(Borders::ALL)
            .title(metric.name.as_str());
        let inner = block.inner(area);
        block.render(area, buf);

        // Value line
        let value_line = if !trend_symbol.is_empty() {
            Line::from(vec![
                Span::styled(
                    format!("{} ", trend_symbol),
                    Style::default().fg(color.unwrap_or(Color::White)),
                ),
                Span::styled(
                    value_text,
                    Style::default()
                        .fg(color.unwrap_or(Color::White))
                        .add_modifier(Modifier::BOLD),
                ),
            ])
        } else {
            Line::from(Span::styled(
                value_text,
                Style::default()
                    .fg(color.unwrap_or(Color::White))
                    .add_modifier(Modifier::BOLD),
            ))
        };

        let value_paragraph = Paragraph::new(value_line).alignment(Alignment::Center);
        let value_area = Rect {
            x: inner.x,
            y: inner.y + 1,
            width: inner.width,
            height: 1,
        };
        value_paragraph.render(value_area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metrics() -> Vec<Metric> {
        vec![
            Metric::new("Sharpe Ratio", MetricValue::Number(1.85))
                .with_format(MetricFormat::Decimal(2))
                .with_trend(Trend::Up),
            Metric::new("Max Drawdown", MetricValue::Percentage(15.5))
                .with_trend(Trend::Down),
            Metric::new("Total Trades", MetricValue::Integer(1234)),
            Metric::new("Win Rate", MetricValue::Percentage(62.3))
                .with_trend(Trend::Up),
            Metric::new("Status", MetricValue::String("Active".to_string())),
            Metric::new("Enabled", MetricValue::Boolean(true)),
        ]
    }

    #[test]
    fn test_metrics_dashboard_creation() {
        let dashboard = MetricsDashboardWidget::new();
        assert_eq!(dashboard.metric_count(), 0);
        assert_eq!(dashboard.layout, LayoutStyle::Grid);
    }

    #[test]
    fn test_metric_creation() {
        let metric = Metric::new("Test", MetricValue::Number(42.0));
        assert_eq!(metric.name, "Test");
        assert_eq!(metric.value, MetricValue::Number(42.0));
    }

    #[test]
    fn test_metric_with_format() {
        let metric = Metric::new("Price", MetricValue::Number(1234.56))
            .with_format(MetricFormat::Currency);
        assert_eq!(metric.format_value(), "$1234.56");
    }

    #[test]
    fn test_metric_with_trend() {
        let metric = Metric::new("Value", MetricValue::Number(100.0))
            .with_trend(Trend::Up);
        assert_eq!(metric.trend, Some(Trend::Up));
        assert_eq!(metric.display_color(), Some(Color::Green));
    }

    #[test]
    fn test_metric_with_color() {
        let metric = Metric::new("Value", MetricValue::Number(100.0))
            .with_color(Color::Blue);
        assert_eq!(metric.display_color(), Some(Color::Blue));
    }

    #[test]
    fn test_metric_color_override() {
        let metric = Metric::new("Value", MetricValue::Number(100.0))
            .with_trend(Trend::Up)
            .with_color(Color::Blue);
        // Custom color should override trend color
        assert_eq!(metric.display_color(), Some(Color::Blue));
    }

    #[test]
    fn test_metric_value_number() {
        let value = MetricValue::Number(42.5);
        assert_eq!(value.to_string(), "42.5");
    }

    #[test]
    fn test_metric_value_percentage() {
        let value = MetricValue::Percentage(75.5);
        assert_eq!(value.to_string(), "75.50%");
    }

    #[test]
    fn test_metric_value_integer() {
        let value = MetricValue::Integer(123);
        assert_eq!(value.to_string(), "123");
    }

    #[test]
    fn test_metric_value_string() {
        let value = MetricValue::String("Test".to_string());
        assert_eq!(value.to_string(), "Test");
    }

    #[test]
    fn test_metric_value_boolean() {
        let value_true = MetricValue::Boolean(true);
        assert_eq!(value_true.to_string(), "Yes");
        
        let value_false = MetricValue::Boolean(false);
        assert_eq!(value_false.to_string(), "No");
    }

    #[test]
    fn test_metric_format_default() {
        let format = MetricFormat::Default;
        assert_eq!(format.format(&MetricValue::Number(42.5)), "42.5");
    }

    #[test]
    fn test_metric_format_currency() {
        let format = MetricFormat::Currency;
        assert_eq!(format.format(&MetricValue::Number(1234.56)), "$1234.56");
        assert_eq!(format.format(&MetricValue::Integer(1000)), "$1000");
    }

    #[test]
    fn test_metric_format_scientific() {
        let format = MetricFormat::Scientific;
        let result = format.format(&MetricValue::Number(1234.56));
        assert!(result.contains('e'));
    }

    #[test]
    fn test_metric_format_compact() {
        let format = MetricFormat::Compact;
        assert_eq!(format.format(&MetricValue::Number(1500.0)), "1.50K");
        assert_eq!(format.format(&MetricValue::Number(1_500_000.0)), "1.50M");
        assert_eq!(format.format(&MetricValue::Number(1_500_000_000.0)), "1.50B");
    }

    #[test]
    fn test_metric_format_compact_negative() {
        let format = MetricFormat::Compact;
        assert_eq!(format.format(&MetricValue::Number(-1500.0)), "-1.50K");
    }

    #[test]
    fn test_metric_format_decimal() {
        let format = MetricFormat::Decimal(3);
        assert_eq!(format.format(&MetricValue::Number(42.5)), "42.500");
        assert_eq!(format.format(&MetricValue::Percentage(75.5)), "75.500%");
    }

    #[test]
    fn test_trend_color() {
        assert_eq!(Trend::Up.color(), Color::Green);
        assert_eq!(Trend::Down.color(), Color::Red);
        assert_eq!(Trend::Neutral.color(), Color::Yellow);
    }

    #[test]
    fn test_trend_symbol() {
        assert_eq!(Trend::Up.symbol(), "↑");
        assert_eq!(Trend::Down.symbol(), "↓");
        assert_eq!(Trend::Neutral.symbol(), "→");
    }

    #[test]
    fn test_dashboard_add_metric() {
        let mut dashboard = MetricsDashboardWidget::new();
        let metric = Metric::new("Test", MetricValue::Number(42.0));
        dashboard.add_metric(metric);
        assert_eq!(dashboard.metric_count(), 1);
    }

    #[test]
    fn test_dashboard_with_metrics() {
        let metrics = create_test_metrics();
        let dashboard = MetricsDashboardWidget::new().with_metrics(metrics.clone());
        assert_eq!(dashboard.metric_count(), metrics.len());
    }

    #[test]
    fn test_dashboard_with_layout() {
        let dashboard = MetricsDashboardWidget::new().with_layout(LayoutStyle::List);
        assert_eq!(dashboard.layout, LayoutStyle::List);
    }

    #[test]
    fn test_dashboard_with_grid_columns() {
        let dashboard = MetricsDashboardWidget::new().with_grid_columns(4);
        assert_eq!(dashboard.grid_columns, 4);
    }

    #[test]
    fn test_dashboard_with_grid_columns_min() {
        let dashboard = MetricsDashboardWidget::new().with_grid_columns(0);
        assert_eq!(dashboard.grid_columns, 1); // Should be at least 1
    }

    #[test]
    fn test_dashboard_with_show_trends() {
        let dashboard = MetricsDashboardWidget::new().with_show_trends(false);
        assert_eq!(dashboard.show_trends, false);
    }

    #[test]
    fn test_dashboard_with_block() {
        let block = Block::default().title("Metrics");
        let dashboard = MetricsDashboardWidget::new().with_block(block);
        assert!(dashboard.block.is_some());
    }

    #[test]
    fn test_dashboard_with_card_max_width() {
        let dashboard = MetricsDashboardWidget::new().with_card_max_width(30);
        assert_eq!(dashboard.card_max_width, 30);
    }

    #[test]
    fn test_dashboard_clear() {
        let mut dashboard = MetricsDashboardWidget::new();
        dashboard.add_metric(Metric::new("Test", MetricValue::Number(42.0)));
        assert_eq!(dashboard.metric_count(), 1);
        
        dashboard.clear();
        assert_eq!(dashboard.metric_count(), 0);
    }

    #[test]
    fn test_metric_format_value() {
        let metric = Metric::new("Value", MetricValue::Number(42.5))
            .with_format(MetricFormat::Decimal(2));
        assert_eq!(metric.format_value(), "42.50");
    }

    #[test]
    fn test_metric_with_description() {
        let metric = Metric::new("Test", MetricValue::Number(42.0))
            .with_description("Test description");
        assert_eq!(metric.description, Some("Test description".to_string()));
    }

    #[test]
    fn test_dashboard_clone() {
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(create_test_metrics())
            .with_layout(LayoutStyle::List);
        
        let cloned = dashboard.clone();
        assert_eq!(cloned.metric_count(), dashboard.metric_count());
        assert_eq!(cloned.layout, dashboard.layout);
    }

    #[test]
    fn test_metric_clone() {
        let metric = Metric::new("Test", MetricValue::Number(42.0))
            .with_trend(Trend::Up)
            .with_color(Color::Blue);
        
        let cloned = metric.clone();
        assert_eq!(cloned.name, metric.name);
        assert_eq!(cloned.value, metric.value);
        assert_eq!(cloned.trend, metric.trend);
    }

    #[test]
    fn test_render_empty_dashboard() {
        let dashboard = MetricsDashboardWidget::new();
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        dashboard.render(area, &mut buf);
    }

    #[test]
    fn test_render_grid_layout() {
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(create_test_metrics())
            .with_layout(LayoutStyle::Grid)
            .with_grid_columns(3);
        
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        dashboard.render(area, &mut buf);
    }

    #[test]
    fn test_render_list_layout() {
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(create_test_metrics())
            .with_layout(LayoutStyle::List);
        
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        dashboard.render(area, &mut buf);
    }

    #[test]
    fn test_render_cards_layout() {
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(create_test_metrics())
            .with_layout(LayoutStyle::Cards)
            .with_card_max_width(20);
        
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        dashboard.render(area, &mut buf);
    }

    #[test]
    fn test_render_with_block() {
        let block = Block::default().title("Metrics Dashboard").borders(Borders::ALL);
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(create_test_metrics())
            .with_block(block);
        
        let area = Rect::new(0, 0, 50, 10);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic
        dashboard.render(area, &mut buf);
    }

    #[test]
    fn test_metric_format_compact_small() {
        let format = MetricFormat::Compact;
        let result = format.format(&MetricValue::Number(42.5));
        assert_eq!(result, "42.50");
    }

    #[test]
    fn test_metric_format_compact_thousands() {
        let format = MetricFormat::Compact;
        assert_eq!(format.format(&MetricValue::Number(1500.0)), "1.50K");
        assert_eq!(format.format(&MetricValue::Number(999.0)), "999.00");
    }

    #[test]
    fn test_metric_format_compact_millions() {
        let format = MetricFormat::Compact;
        assert_eq!(format.format(&MetricValue::Number(1_500_000.0)), "1.50M");
        // 999_999.0 / 1000.0 = 999.999, which rounds to 1000.00K
        assert_eq!(format.format(&MetricValue::Number(999_999.0)), "1000.00K");
    }

    #[test]
    fn test_metric_format_compact_billions() {
        let format = MetricFormat::Compact;
        assert_eq!(format.format(&MetricValue::Number(1_500_000_000.0)), "1.50B");
        // 999_999_999.0 / 1_000_000.0 = 999.999999, which rounds to 1000.00M
        assert_eq!(format.format(&MetricValue::Number(999_999_999.0)), "1000.00M");
    }

    #[test]
    fn test_all_layout_styles() {
        for layout in [LayoutStyle::Grid, LayoutStyle::List, LayoutStyle::Cards] {
            let dashboard = MetricsDashboardWidget::new()
                .with_metrics(create_test_metrics())
                .with_layout(layout);
            
            let area = Rect::new(0, 0, 50, 10);
            let mut buf = ratatui::buffer::Buffer::empty(area);
            dashboard.render(area, &mut buf);
        }
    }

    #[test]
    fn test_metric_display_color_no_trend_no_color() {
        let metric = Metric::new("Test", MetricValue::Number(42.0));
        assert_eq!(metric.display_color(), None);
    }

    #[test]
    fn test_metric_display_color_trend_only() {
        let metric = Metric::new("Test", MetricValue::Number(42.0))
            .with_trend(Trend::Up);
        assert_eq!(metric.display_color(), Some(Color::Green));
    }

    #[test]
    fn test_metric_display_color_custom_only() {
        let metric = Metric::new("Test", MetricValue::Number(42.0))
            .with_color(Color::Cyan);
        assert_eq!(metric.display_color(), Some(Color::Cyan));
    }

    #[test]
    fn test_metric_format_percentage() {
        let metric = Metric::new("Rate", MetricValue::Percentage(75.5));
        assert_eq!(metric.format_value(), "75.50%");
    }

    #[test]
    fn test_metric_format_integer() {
        let metric = Metric::new("Count", MetricValue::Integer(123));
        assert_eq!(metric.format_value(), "123");
    }

    #[test]
    fn test_metric_format_string() {
        let metric = Metric::new("Status", MetricValue::String("Active".to_string()));
        assert_eq!(metric.format_value(), "Active");
    }

    #[test]
    fn test_metric_format_boolean() {
        let metric = Metric::new("Flag", MetricValue::Boolean(true));
        assert_eq!(metric.format_value(), "Yes");
        
        let metric2 = Metric::new("Flag", MetricValue::Boolean(false));
        assert_eq!(metric2.format_value(), "No");
    }

    #[test]
    fn test_dashboard_multiple_metrics() {
        let mut dashboard = MetricsDashboardWidget::new();
        for i in 0..10 {
            dashboard.add_metric(Metric::new(
                format!("Metric {}", i),
                MetricValue::Number(i as f64),
            ));
        }
        assert_eq!(dashboard.metric_count(), 10);
    }

    #[test]
    fn test_render_small_area() {
        let dashboard = MetricsDashboardWidget::new()
            .with_metrics(create_test_metrics());
        
        let area = Rect::new(0, 0, 10, 5);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        
        // Should not panic even with small area
        dashboard.render(area, &mut buf);
    }

    #[test]
    fn test_metric_value_equality() {
        let v1 = MetricValue::Number(42.0);
        let v2 = MetricValue::Number(42.0);
        let v3 = MetricValue::Number(43.0);
        
        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_metric_format_equality() {
        assert_eq!(MetricFormat::Default, MetricFormat::Default);
        assert_eq!(MetricFormat::Decimal(2), MetricFormat::Decimal(2));
        assert_ne!(MetricFormat::Decimal(2), MetricFormat::Decimal(3));
    }

    #[test]
    fn test_trend_equality() {
        assert_eq!(Trend::Up, Trend::Up);
        assert_ne!(Trend::Up, Trend::Down);
    }

    #[test]
    fn test_layout_style_equality() {
        assert_eq!(LayoutStyle::Grid, LayoutStyle::Grid);
        assert_ne!(LayoutStyle::Grid, LayoutStyle::List);
    }
}
