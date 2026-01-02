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

// Re-export for convenience (will be added as parameters are implemented)
// pub use backtest_params::*;
// pub use research_params::*;
// pub use validate_params::*;
// pub use algorithm_params::*;


