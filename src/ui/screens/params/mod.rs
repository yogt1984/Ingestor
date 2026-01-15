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
