//! Parameter Configuration Screens
//!
//! This module contains TUI screens for configuring command parameters:
//! - Backtest Evaluate Config (T-2.8): Configure backtest evaluate parameters

pub mod backtest_evaluate;

pub use backtest_evaluate::{
    BacktestEvaluateConfigScreen,
    EvaluateField,
    ParameterGroup,
    draw_backtest_evaluate_config_screen,
};
