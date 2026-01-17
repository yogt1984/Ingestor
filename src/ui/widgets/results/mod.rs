//! Results Display Widgets (Phase 3)
//!
//! Widgets for displaying backtest and analysis results:
//! - Metrics Dashboard Widget (T-3.1): Display key metrics with trends
//! - Table Widget (T-3.2): Sortable, scrollable table for tabular data
//! - Chart Widget (T-3.3): ASCII/Unicode charts (Line, Bar, Scatter, Heatmap)
//! - Pareto Frontier Widget (T-3.4): Multi-objective optimization results

pub mod metrics_dashboard;
pub mod table;
pub mod chart;
pub mod pareto;

pub use metrics_dashboard::{MetricsDashboardWidget, Metric, MetricValue, MetricFormat, Trend, LayoutStyle};
pub use table::{TableWidget, TableHeader, TableRow, SortDirection};
pub use chart::{ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig, LegendPosition};
pub use pareto::{ParetoFrontierWidget, ParetoSolution};