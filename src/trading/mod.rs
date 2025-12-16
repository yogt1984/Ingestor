//! Trading module
//!
//! Contains market making engines, paper trading simulators,
//! risk management, and preset configurations.

pub mod market_maker;
pub mod mm_simulator;
pub mod risk_manager;
pub mod presets;

pub use market_maker::{MarketMakerEngine, MMConfig, Quote};
pub use mm_simulator::{RiskManagedPaperTradingEngine, SimulatorConfig};
pub use risk_manager::{RiskManager, RiskConfig};
pub use presets::{ParameterPreset, PresetStore};
