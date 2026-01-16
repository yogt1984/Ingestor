//! Results Display Widgets (Phase 3)
//!
//! Widgets for displaying backtest and analysis results:
//! - Table Widget (T-3.2): Sortable, scrollable table for tabular data

pub mod table;

pub use table::{TableWidget, TableHeader, TableRow, SortDirection};
