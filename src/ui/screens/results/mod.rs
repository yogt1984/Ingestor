//! Results Display Screens (Phase 3)
//!
//! Screens for displaying command execution results:
//! - Backtest Evaluate Results Screen (T-3.6): Display backtest evaluation results
//! - Backtest Tune Results Screen (T-3.7): Display backtest tune (grid search) results
//! - Backtest Regime Search Results Screen (T-3.8): Display regime search results
//! - Backtest Sweep Results Screen (T-3.8): Display sweep results
//! - Backtest Grid Results Screen (T-3.8): Display grid search results
//! - Backtest Multi-Objective Results Screen (T-3.8): Display multi-objective optimization results

pub mod backtest_evaluate;
pub mod backtest_tune;
pub mod backtest_regime_search;
pub mod backtest_sweep;
pub mod backtest_grid;
pub mod backtest_multi_objective;

pub use backtest_evaluate::{BacktestEvaluateResultsScreen, ViewMode};
pub use backtest_tune::{BacktestTuneResultsScreen, TuneViewMode};
pub use backtest_regime_search::{BacktestRegimeSearchResultsScreen, RegimeSearchViewMode};
pub use backtest_sweep::{BacktestSweepResultsScreen, SweepViewMode};
pub use backtest_grid::{BacktestGridResultsScreen, GridViewMode};
pub use backtest_multi_objective::{BacktestMultiObjectiveResultsScreen, MultiObjectiveViewMode};
