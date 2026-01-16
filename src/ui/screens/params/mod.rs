//! Parameter Configuration Screens
//!
//! This module contains TUI screens for configuring command parameters:
//! - Backtest Evaluate Config (T-2.8): Configure backtest evaluate parameters
//! - Backtest Tune Config (T-2.9): Configure backtest tune parameters (MM only)
//! - Backtest Multi-Objective Config (T-2.10): Configure backtest multi-objective parameters (MM only)

pub mod backtest_evaluate;
pub mod backtest_tune;
pub mod backtest_multi_objective;

pub use backtest_evaluate::{
    BacktestEvaluateConfigScreen,
    EvaluateField,
    ParameterGroup as EvaluateParameterGroup,
    draw_backtest_evaluate_config_screen,
};

pub use backtest_tune::{
    BacktestTuneConfigScreen,
    TuneField,
    ParameterGroup as TuneParameterGroup,
    draw_backtest_tune_config_screen,
};

pub use backtest_multi_objective::{
    BacktestMultiObjectiveConfigScreen,
    MultiObjectiveField,
    ParameterGroup as MultiObjectiveParameterGroup,
    draw_backtest_multi_objective_config_screen,
};

pub mod backtest_regime_search;
pub mod backtest_regime_optimize;
pub mod backtest_train;
pub mod backtest_sweep;
pub mod backtest_walk_forward;
pub mod backtest_oos_validate;
pub mod backtest_walk_forward_ml;
pub mod backtest_grid;
pub mod backtest_simulate;
pub mod backtest_campaign;
pub mod backtest_paper;
pub mod research_run;
pub mod research_status;
pub mod validate_run;
pub mod validate_status;
pub mod validate_show;
pub mod algorithm_create;
pub mod algorithm_list;
pub mod algorithm_show;

pub use backtest_regime_search::{
    BacktestRegimeSearchConfigScreen,
    RegimeSearchField,
    ParameterGroup as RegimeSearchParameterGroup,
    draw_backtest_regime_search_config_screen,
};

pub use backtest_regime_optimize::{
    BacktestRegimeOptimizeConfigScreen,
    RegimeOptimizeField,
    ParameterGroup as RegimeOptimizeParameterGroup,
    draw_backtest_regime_optimize_config_screen,
};

pub use backtest_train::{
    BacktestTrainConfigScreen,
    TrainField,
    ParameterGroup as TrainParameterGroup,
    draw_backtest_train_config_screen,
};

pub use backtest_sweep::{
    BacktestSweepConfigScreen,
    SweepField,
    ParameterGroup as SweepParameterGroup,
    draw_backtest_sweep_config_screen,
};

pub use backtest_walk_forward::{
    BacktestWalkForwardConfigScreen,
    WalkForwardField,
    ParameterGroup as WalkForwardParameterGroup,
    draw_backtest_walk_forward_config_screen,
};

pub use backtest_oos_validate::{
    BacktestOOSValidateConfigScreen,
    OOSValidateField,
    ParameterGroup as OOSValidateParameterGroup,
    draw_backtest_oos_validate_config_screen,
};

pub use backtest_walk_forward_ml::{
    BacktestWalkForwardMLConfigScreen,
    WalkForwardMLField,
    ParameterGroup as WalkForwardMLParameterGroup,
    draw_backtest_walk_forward_ml_config_screen,
};

pub use backtest_grid::{
    BacktestGridConfigScreen,
    GridField,
    ParameterGroup as GridParameterGroup,
    draw_backtest_grid_config_screen,
};

pub use backtest_simulate::{
    BacktestSimulateConfigScreen,
    SimulateField,
    ParameterGroup as SimulateParameterGroup,
    draw_backtest_simulate_config_screen,
};

pub use backtest_campaign::{
    BacktestCampaignConfigScreen,
    CampaignField,
    ParameterGroup as CampaignParameterGroup,
    draw_backtest_campaign_config_screen,
};

pub use backtest_paper::{
    BacktestPaperConfigScreen,
    PaperField,
    ParameterGroup as PaperParameterGroup,
    draw_backtest_paper_config_screen,
};

pub use research_run::{
    ResearchRunConfigScreen,
    ResearchRunField,
    ParameterGroup as ResearchRunParameterGroup,
    draw_research_run_config_screen,
};

pub use research_status::{
    ResearchStatusScreen,
    draw_research_status_screen,
};

pub use validate_run::{
    ValidateRunConfigScreen,
    ValidateRunField,
    ParameterGroup as ValidateRunParameterGroup,
    draw_validate_run_config_screen,
};

pub use validate_status::{
    ValidateStatusScreen,
    draw_validate_status_screen,
};

pub use validate_show::{
    ValidateShowScreen,
    draw_validate_show_screen,
};

pub use algorithm_create::{
    AlgorithmCreateConfigScreen,
    AlgorithmCreateField,
    ParameterGroup as AlgorithmCreateParameterGroup,
    draw_algorithm_create_config_screen,
};

pub use algorithm_list::{
    AlgorithmListScreen,
    draw_algorithm_list_screen,
};

pub use algorithm_show::{
    AlgorithmShowScreen,
    draw_algorithm_show_screen,
};
