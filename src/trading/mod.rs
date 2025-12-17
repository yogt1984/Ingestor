//! Trading module
//!
//! Contains market making engines, paper trading simulators,
//! risk management, OCO order management, position management, and preset configurations.

pub mod market_maker;
pub mod mm_simulator;
pub mod oco_manager;
pub mod position_manager;
pub mod risk_manager;
pub mod presets;

pub use market_maker::{MarketMakerEngine, MMConfig, Quote};
pub use mm_simulator::{RiskManagedPaperTradingEngine, SimulatorConfig};
pub use oco_manager::{OCOManager, OCOOrder, OCOStats, OCOTrigger, OCOError, Side, TriggerType};
pub use position_manager::{PositionManager, PositionConfig, Position, PositionSide, PositionSizeRequest, PositionSizeResult, SizingMethod, PositionError, PortfolioStats};
pub use risk_manager::{RiskManager, RiskConfig};
pub use presets::{ParameterPreset, PresetStore};
