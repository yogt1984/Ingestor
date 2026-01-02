//! Unified Command Execution Layer
//!
//! This module provides a unified command execution layer that is shared between
//! CLI binaries and TUI. This eliminates code duplication and ensures consistent
//! behavior across both interfaces.
//!
//! # Architecture
//!
//! ```text
//! Command Execution Layer (src/commands/)
//!     - backtest.rs      - All backtest commands
//!     - research.rs      - All research commands
//!     - validate.rs      - All validate commands
//!     - algorithm.rs     - All algorithm commands
//!     - params/          - Parameter builders
//!         - backtest_params.rs
//!         - research_params.rs
//!         - validate_params.rs
//!         - algorithm_params.rs
//!     - common.rs         - Progress callbacks, shared types
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use ingestor::commands::BacktestCommands;
//! use ingestor::commands::params::EvaluateParams;
//!
//! let params = EvaluateParams::builder()
//!     .data_path("./data/features")
//!     .algorithm("as")
//!     .spread(2.0)
//!     .build()?;
//!
//! let result = BacktestCommands::evaluate(params, None).await?;
//! ```

pub mod backtest;
pub mod research;
pub mod validate;
pub mod algorithm;
pub mod params;
pub mod common;

// Re-export command structs for convenience
pub use backtest::BacktestCommands;
pub use research::ResearchCommands;
pub use validate::ValidateCommands;
pub use algorithm::AlgorithmCommands;

// Re-export common types
pub use common::{
    ProgressCallback,
    ProgressEvent,
    LogLevel,
    NoOpCallback,
};


