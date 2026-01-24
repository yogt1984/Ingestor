//! Results Display Screens (Phase 3)
//!
//! Screens for displaying command execution results:
//! - Backtest Evaluate Results Screen (T-3.6): Display backtest evaluation results
//! - Backtest Tune Results Screen (T-3.7): Display backtest tune (grid search) results
//! - Backtest Regime Search Results Screen (T-3.8): Display regime search results
//! - Backtest Sweep Results Screen (T-3.8): Display sweep results
//! - Backtest Grid Results Screen (T-3.8): Display grid search results
//! - Backtest Multi-Objective Results Screen (T-3.8): Display multi-objective optimization results
//! - Backtest Regime Optimize Results Screen (T-3.8): Display regime-specific parameter optimization results
//! - Backtest Train Results Screen (T-3.8): Display ML weight training results
//! - Backtest Walk-Forward ML Results Screen (T-3.8): Display walk-forward ML training results
//! - Backtest Walk-Forward Results Screen (T-3.8): Display walk-forward validation results
//! - Backtest OOS Validate Results Screen (T-3.8): Display out-of-sample validation results
//! - Backtest Simulate Results Screen (T-3.8): Display simulation campaign results
//! - Backtest Campaign Results Screen (T-3.8): Display validation campaign results
//! - Backtest Paper Results Screen (T-3.8): Display paper trading session results
//! - Research Run Results Screen (T-3.8): Display research analysis results
//! - Validate Run Results Screen (T-3.8): Display validation pipeline results
//! - Algorithm Create Results Screen (T-3.8): Display algorithm creation results

pub mod backtest_evaluate;
pub mod backtest_tune;
pub mod backtest_regime_search;
pub mod backtest_sweep;
pub mod backtest_grid;
pub mod backtest_multi_objective;
pub mod backtest_regime_optimize;
pub mod backtest_train;
pub mod backtest_walk_forward_ml;
pub mod backtest_walk_forward;
pub mod backtest_oos_validate;
pub mod backtest_simulate;
pub mod backtest_campaign;
pub mod backtest_paper;
pub mod backtest_info;
pub mod backtest_validate_data;
pub mod backtest_compare;
pub mod backtest_head_to_head;
pub mod research_run;
pub mod validate_run;
pub mod algorithm_create;

pub use backtest_evaluate::{BacktestEvaluateResultsScreen, ViewMode};
pub use backtest_tune::{BacktestTuneResultsScreen, TuneViewMode};
pub use backtest_regime_search::{BacktestRegimeSearchResultsScreen, RegimeSearchViewMode};
pub use backtest_sweep::{BacktestSweepResultsScreen, SweepViewMode};
pub use backtest_grid::{BacktestGridResultsScreen, GridViewMode};
pub use backtest_multi_objective::{BacktestMultiObjectiveResultsScreen, MultiObjectiveViewMode};
pub use backtest_regime_optimize::{BacktestRegimeOptimizeResultsScreen, RegimeOptimizeViewMode};
pub use backtest_train::{BacktestTrainResultsScreen, TrainViewMode};
pub use backtest_walk_forward_ml::{BacktestWalkForwardMLResultsScreen, WalkForwardMLViewMode};
pub use backtest_walk_forward::{BacktestWalkForwardResultsScreen, WalkForwardViewMode};
pub use backtest_oos_validate::{BacktestOOSValidateResultsScreen, OOSValidateViewMode};
pub use backtest_simulate::{BacktestSimulateResultsScreen, SimulateViewMode};
pub use backtest_campaign::{BacktestCampaignResultsScreen, CampaignViewMode};
pub use backtest_paper::{BacktestPaperResultsScreen, PaperViewMode};
pub use backtest_info::{BacktestInfoResultsScreen, InfoViewMode};
pub use backtest_validate_data::{BacktestValidateDataResultsScreen, ValidateDataViewMode};
pub use backtest_compare::{BacktestCompareResultsScreen, CompareViewMode};
pub use backtest_head_to_head::{BacktestHeadToHeadResultsScreen, HeadToHeadViewMode};
pub use research_run::{ResearchRunResultsScreen, ResearchRunViewMode};
pub use validate_run::{ValidateRunResultsScreen, ValidateRunViewMode};
pub use algorithm_create::{AlgorithmCreateResultsScreen, AlgorithmCreateViewMode};
