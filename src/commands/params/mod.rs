//! Parameter Builders
//!
//! This module provides parameter builder structs for all commands.
//! Each command has a corresponding parameter struct and builder that
//! validates inputs and provides sensible defaults.
//!
//! # Organization
//!
//! - `backtest_params.rs` - All backtest command parameters
//! - `research_params.rs` - All research command parameters
//! - `validate_params.rs` - All validate command parameters
//! - `algorithm_params.rs` - All algorithm command parameters

pub mod backtest_params;
pub mod research_params;
pub mod validate_params;
pub mod algorithm_params;

// Re-export for convenience
pub use research_params::{RunParams, RunParamsBuilder, StatusParams, StatusParamsBuilder};
pub use validate_params::{
    RunParams as ValidateRunParams, RunParamsBuilder as ValidateRunParamsBuilder,
    PresetsParams, PresetsParamsBuilder,
    StagesParams, StagesParamsBuilder,
    StatusParams as ValidateStatusParams, StatusParamsBuilder as ValidateStatusParamsBuilder,
    ShowParams, ShowParamsBuilder,
};


