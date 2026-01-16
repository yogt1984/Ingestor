//! Results Display Widgets (Phase 3)
//!
//! Widgets for displaying backtest and analysis results:
//! - Metrics Dashboard Widget (T-3.1): Display key metrics with trends
//! - Table Widget (T-3.2): Sortable, scrollable table for tabular data

pub mod metrics_dashboard;
pub mod table;

pub use metrics_dashboard::{MetricsDashboardWidget, Metric, MetricValue, MetricFormat, Trend, LayoutStyle};
pub use table::{TableWidget, TableHeader, TableRow, SortDirection};
