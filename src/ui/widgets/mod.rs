//! UI Widget Modules
//!
//! This module contains reusable UI widgets for the TUI interface:
//! - StatusBar (Task TUI-6.0): Persistent status bar at bottom of screens
//! - params: Parameter input widgets (Task T-2.1+)
//! - results: Results display widgets (Phase 3)
//!
//! Widgets are self-contained components that can be rendered in any screen.

pub mod status_bar;
pub mod params;
pub mod results;

// StatusBar (TUI-6.0)
pub use status_bar::{StatusBar, draw_status_bar};

// Parameter widgets (T-2.1+)
pub use params::text_input::TextInputWidget;
pub use params::number_input::NumberInputWidget;
pub use params::comma_list::CommaListWidget;
pub use params::toggle::ToggleWidget;
pub use params::path_input::PathInputWidget;
pub use params::dropdown::DropdownWidget;
pub use params::slider::SliderWidget;

// Results widgets (Phase 3)
pub use results::metrics_dashboard::{MetricsDashboardWidget, Metric, MetricValue, MetricFormat, Trend, LayoutStyle};
pub use results::table::{TableWidget, TableHeader, TableRow, SortDirection};
pub use results::chart::{ChartWidget, ChartType, DataPoint, DataSeries, AxisConfig, LegendPosition};
pub use results::pareto::{ParetoFrontierWidget, ParetoSolution};
pub use results::progress::ProgressWidget;