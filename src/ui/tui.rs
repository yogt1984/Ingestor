use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use crossbeam::channel::Receiver;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

use ratatui::{
    backend::CrosstermBackend,
    Terminal,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, Paragraph, Sparkline},
    style::{Style, Color, Modifier},
    text::{Span, Line},
};

use crate::features::feature_fusion::FeaturesSnapshot;
use crate::execution::market_maker::{MarketMakerEngine, MMConfig, MarketRegime};
use crate::execution::mm_simulator::{PaperTradingEngine, PaperTradingState, RiskManagedPaperTradingEngine, RiskManagedState, SimulatorConfig};
use crate::execution::risk_manager::RiskAction;
use crate::forward_testing::{ForwardTestSession, ForwardTestConfig};
use crate::execution::presets::{PresetStore, ParameterPreset};
use crate::strategies::AlgorithmType;
use crate::ui::screens::{ResearchScreen, draw_research_screen, MainMenuState, draw_main_menu, MainMenuItem};
use crate::ui::tui_integration::{MenuIntegration, CurrentSubMenu, process_action, ActionResult, draw_menu_with_status};
use crate::ui::state::GlobalState;
use crate::ui::submenu::{SubMenu, SettingUpdate};

type Term = Terminal<CrosstermBackend<io::Stdout>>;

const MAX_HISTORY: usize = 60; // 60 seconds of history at 1Hz
const UPDATE_INTERVAL_MS: u64 = 1000; // 1Hz update rate

/// Application mode
#[derive(Clone, PartialEq)]
enum AppMode {
    NewMenu,         // Main menu (TUI v0.1)
    Live,
    LiveMM,          // Live with Market Maker
    PresetSelect,    // Preset selection for paper trading
    PaperTradePreset, // Paper trading with selected preset
    Features,
    Backtest,        // Running backtest
    WalkForward,     // Walk-forward validation
    DataQuality,     // Data quality check
    CampaignSimulation, // Simulated 4-week validation campaign
    DataInfo,        // Data statistics and info (CLI parity)
    GridSearch,      // Grid search parameter optimization (CLI parity)
    Sweep,           // Parameter sensitivity sweep (CLI parity)
    OOSValidation,   // Out-of-sample validation (CLI parity)
    Research,        // Research Dashboard (Task 4.1)
    // T-4.5: Config and results screens
    ConfigScreen(crate::ui::submenu::NavigationTarget),
    ResultsScreen(crate::ui::submenu::NavigationTarget),
    ExecutingCommand {
        config_target: crate::ui::submenu::NavigationTarget,
        command_name: String,
    },
}

/// Settings for the application
#[derive(Clone)]
pub struct TuiSettings {
    pub persist_features: bool,
    /// Maximum storage size in GB (0 = unlimited)
    pub max_storage_gb: f64,
    /// Run backtest after data collection
    pub run_backtest: bool,
}

impl Default for TuiSettings {
    fn default() -> Self {
        Self {
            persist_features: true, // Default to persisting
            max_storage_gb: 10.0,   // Default 10 GB
            run_backtest: false,
        }
    }
}

impl TuiSettings {
    /// Calculate max files based on storage limit
    /// Each parquet file is ~200KB with 1000 rows
    pub fn max_files(&self) -> usize {
        if self.max_storage_gb <= 0.0 {
            0 // Unlimited
        } else {
            // ~200KB per file, convert GB to files
            let bytes_per_file = 200_000.0;
            let max_bytes = self.max_storage_gb * 1_000_000_000.0;
            (max_bytes / bytes_per_file) as usize
        }
    }
}

/// Full feature accumulator for 1-second averaging
#[derive(Default)]
struct FeatureAccumulator {
    // Order Book
    best_bid: f64,
    best_ask: f64,
    mid_price: f64,
    microprice: f64,
    spread: f64,
    imbalance: f64,
    pwi_1: f64,
    pwi_5: f64,
    pwi_25: f64,
    pwi_50: f64,
    bid_slope: f64,
    ask_slope: f64,
    volume_imbalance_top5: f64,
    bid_depth_ratio: f64,
    ask_depth_ratio: f64,
    bid_volume_001: f64,
    ask_volume_001: f64,

    // Trades
    last_trade_price: f64,
    trade_imbalance: f64,
    vwap_total: f64,
    vwap_10: f64,
    vwap_50: f64,
    vwap_100: f64,
    vwap_1000: f64,
    price_change: f64,
    avg_trade_size: f64,
    signed_count_momentum: f64,
    trade_rate_10s: f64,
    aggr_ratio_10: f64,
    aggr_ratio_50: f64,
    aggr_ratio_100: f64,
    aggr_ratio_1000: f64,

    // Order Flow
    order_flow_imbalance: f64,
    order_flow_pressure: f64,

    // Illiquidity
    roll_spread: f64,
    amihuds_lambda: f64,
    kyles_lambda: f64,
    hasbroucks_lambda: f64,
    vpin: f64,

    // Entropy
    tick_entropy_1s: f64,
    tick_entropy_5s: f64,
    tick_entropy_10s: f64,
    tick_entropy_15s: f64,
    tick_entropy_30s: f64,
    tick_entropy_1m: f64,
    tick_entropy_15m: f64,
    volume_tick_entropy_1s: f64,
    volume_tick_entropy_5s: f64,
    volume_tick_entropy_10s: f64,
    volume_tick_entropy_15s: f64,
    volume_tick_entropy_30s: f64,
    volume_tick_entropy_1m: f64,
    volume_tick_entropy_15m: f64,

    // Volatility
    realized_volatility_100: f64,
    realized_volatility_1000: f64,
    bipower_variation_100: f64,
    jump_indicator: f64,
    vol_of_vol: f64,

    // Toxicity
    toxic_flow_ratio_micro: f64,
    toxic_flow_ratio_mid: f64,
    adverse_selection_micro: f64,
    adverse_selection_mid: f64,
    arrival_asymmetry: f64,
    size_toxicity_ratio: f64,
    toxicity_index: f64,

    count: usize,
}

impl FeatureAccumulator {
    fn add(&mut self, snap: &FeaturesSnapshot) {
        self.best_bid += dec_to_f64(snap.best_bid);
        self.best_ask += dec_to_f64(snap.best_ask);
        self.mid_price += dec_to_f64(snap.mid_price);
        self.microprice += dec_to_f64(snap.microprice);
        self.spread += dec_to_f64(snap.spread);
        self.imbalance += dec_to_f64(snap.imbalance);
        self.pwi_1 += dec_to_f64(snap.pwi_1);
        self.pwi_5 += dec_to_f64(snap.pwi_5);
        self.pwi_25 += dec_to_f64(snap.pwi_25);
        self.pwi_50 += dec_to_f64(snap.pwi_50);
        self.bid_slope += dec_to_f64(snap.bid_slope);
        self.ask_slope += dec_to_f64(snap.ask_slope);
        self.volume_imbalance_top5 += dec_to_f64(snap.volume_imbalance_top5);
        self.bid_depth_ratio += dec_to_f64(snap.bid_depth_ratio);
        self.ask_depth_ratio += dec_to_f64(snap.ask_depth_ratio);
        self.bid_volume_001 += dec_to_f64(snap.bid_volume_001);
        self.ask_volume_001 += dec_to_f64(snap.ask_volume_001);

        self.last_trade_price += dec_to_f64(snap.last_trade_price);
        self.trade_imbalance += dec_to_f64(snap.trade_imbalance);
        self.vwap_total += dec_to_f64(snap.vwap_total);
        self.vwap_10 += dec_to_f64(snap.vwap_10);
        self.vwap_50 += dec_to_f64(snap.vwap_50);
        self.vwap_100 += dec_to_f64(snap.vwap_100);
        self.vwap_1000 += dec_to_f64(snap.vwap_1000);
        self.price_change += dec_to_f64(snap.price_change);
        self.avg_trade_size += dec_to_f64(snap.avg_trade_size);
        self.signed_count_momentum += snap.signed_count_momentum as f64;
        self.trade_rate_10s += snap.trade_rate_10s.unwrap_or(0.0);
        self.aggr_ratio_10 += dec_to_f64(snap.aggr_ratio_10);
        self.aggr_ratio_50 += dec_to_f64(snap.aggr_ratio_50);
        self.aggr_ratio_100 += dec_to_f64(snap.aggr_ratio_100);
        self.aggr_ratio_1000 += dec_to_f64(snap.aggr_ratio_1000);

        self.order_flow_imbalance += dec_to_f64(snap.order_flow_imbalance);
        self.order_flow_pressure += dec_to_f64(Some(snap.order_flow_pressure));

        self.roll_spread += dec_to_f64(snap.roll_spread);
        self.amihuds_lambda += dec_to_f64(snap.amihuds_lambda);
        self.kyles_lambda += dec_to_f64(snap.kyles_lambda);
        self.hasbroucks_lambda += dec_to_f64(snap.hasbroucks_lambda);
        self.vpin += dec_to_f64(snap.vpin);

        self.tick_entropy_1s += dec_to_f64(snap.tick_entropy_1s);
        self.tick_entropy_5s += dec_to_f64(snap.tick_entropy_5s);
        self.tick_entropy_10s += dec_to_f64(snap.tick_entropy_10s);
        self.tick_entropy_15s += dec_to_f64(snap.tick_entropy_15s);
        self.tick_entropy_30s += dec_to_f64(snap.tick_entropy_30s);
        self.tick_entropy_1m += dec_to_f64(snap.tick_entropy_1m);
        self.tick_entropy_15m += dec_to_f64(snap.tick_entropy_15m);
        self.volume_tick_entropy_1s += dec_to_f64(snap.volume_tick_entropy_1s);
        self.volume_tick_entropy_5s += dec_to_f64(snap.volume_tick_entropy_5s);
        self.volume_tick_entropy_10s += dec_to_f64(snap.volume_tick_entropy_10s);
        self.volume_tick_entropy_15s += dec_to_f64(snap.volume_tick_entropy_15s);
        self.volume_tick_entropy_30s += dec_to_f64(snap.volume_tick_entropy_30s);
        self.volume_tick_entropy_1m += dec_to_f64(snap.volume_tick_entropy_1m);
        self.volume_tick_entropy_15m += dec_to_f64(snap.volume_tick_entropy_15m);

        // Volatility
        self.realized_volatility_100 += snap.realized_volatility_100.unwrap_or(0.0);
        self.realized_volatility_1000 += snap.realized_volatility_1000.unwrap_or(0.0);
        self.bipower_variation_100 += snap.bipower_variation_100.unwrap_or(0.0);
        self.jump_indicator += snap.jump_indicator.unwrap_or(0.0);
        self.vol_of_vol += snap.vol_of_vol.unwrap_or(0.0);

        // Toxicity
        self.toxic_flow_ratio_micro += dec_to_f64(snap.toxic_flow_ratio_micro);
        self.toxic_flow_ratio_mid += dec_to_f64(snap.toxic_flow_ratio_mid);
        self.adverse_selection_micro += dec_to_f64(snap.adverse_selection_micro);
        self.adverse_selection_mid += dec_to_f64(snap.adverse_selection_mid);
        self.arrival_asymmetry += dec_to_f64(snap.arrival_asymmetry);
        self.size_toxicity_ratio += dec_to_f64(snap.size_toxicity_ratio);
        self.toxicity_index += dec_to_f64(snap.toxicity_index);

        self.count += 1;
    }

    fn average(&self) -> AveragedFeatures {
        let n = if self.count == 0 { 1.0 } else { self.count as f64 };
        AveragedFeatures {
            best_bid: self.best_bid / n,
            best_ask: self.best_ask / n,
            mid_price: self.mid_price / n,
            microprice: self.microprice / n,
            spread: self.spread / n,
            imbalance: self.imbalance / n,
            pwi_1: self.pwi_1 / n,
            pwi_5: self.pwi_5 / n,
            pwi_25: self.pwi_25 / n,
            pwi_50: self.pwi_50 / n,
            bid_slope: self.bid_slope / n,
            ask_slope: self.ask_slope / n,
            volume_imbalance_top5: self.volume_imbalance_top5 / n,
            bid_depth_ratio: self.bid_depth_ratio / n,
            ask_depth_ratio: self.ask_depth_ratio / n,
            bid_volume_001: self.bid_volume_001 / n,
            ask_volume_001: self.ask_volume_001 / n,
            last_trade_price: self.last_trade_price / n,
            trade_imbalance: self.trade_imbalance / n,
            vwap_total: self.vwap_total / n,
            vwap_10: self.vwap_10 / n,
            vwap_50: self.vwap_50 / n,
            vwap_100: self.vwap_100 / n,
            vwap_1000: self.vwap_1000 / n,
            price_change: self.price_change / n,
            avg_trade_size: self.avg_trade_size / n,
            signed_count_momentum: self.signed_count_momentum / n,
            trade_rate_10s: self.trade_rate_10s / n,
            aggr_ratio_10: self.aggr_ratio_10 / n,
            aggr_ratio_50: self.aggr_ratio_50 / n,
            aggr_ratio_100: self.aggr_ratio_100 / n,
            aggr_ratio_1000: self.aggr_ratio_1000 / n,
            order_flow_imbalance: self.order_flow_imbalance / n,
            order_flow_pressure: self.order_flow_pressure / n,
            roll_spread: self.roll_spread / n,
            amihuds_lambda: self.amihuds_lambda / n,
            kyles_lambda: self.kyles_lambda / n,
            hasbroucks_lambda: self.hasbroucks_lambda / n,
            vpin: self.vpin / n,
            tick_entropy_1s: self.tick_entropy_1s / n,
            tick_entropy_5s: self.tick_entropy_5s / n,
            tick_entropy_10s: self.tick_entropy_10s / n,
            tick_entropy_15s: self.tick_entropy_15s / n,
            tick_entropy_30s: self.tick_entropy_30s / n,
            tick_entropy_1m: self.tick_entropy_1m / n,
            tick_entropy_15m: self.tick_entropy_15m / n,
            volume_tick_entropy_1s: self.volume_tick_entropy_1s / n,
            volume_tick_entropy_5s: self.volume_tick_entropy_5s / n,
            volume_tick_entropy_10s: self.volume_tick_entropy_10s / n,
            volume_tick_entropy_15s: self.volume_tick_entropy_15s / n,
            volume_tick_entropy_30s: self.volume_tick_entropy_30s / n,
            volume_tick_entropy_1m: self.volume_tick_entropy_1m / n,
            volume_tick_entropy_15m: self.volume_tick_entropy_15m / n,
            // Volatility
            realized_volatility_100: self.realized_volatility_100 / n,
            realized_volatility_1000: self.realized_volatility_1000 / n,
            bipower_variation_100: self.bipower_variation_100 / n,
            jump_indicator: self.jump_indicator / n,
            vol_of_vol: self.vol_of_vol / n,
            // Toxicity
            toxic_flow_ratio_micro: self.toxic_flow_ratio_micro / n,
            toxic_flow_ratio_mid: self.toxic_flow_ratio_mid / n,
            adverse_selection_micro: self.adverse_selection_micro / n,
            adverse_selection_mid: self.adverse_selection_mid / n,
            arrival_asymmetry: self.arrival_asymmetry / n,
            size_toxicity_ratio: self.size_toxicity_ratio / n,
            toxicity_index: self.toxicity_index / n,
            samples: self.count,
        }
    }

    fn reset(&mut self) {
        *self = Self::default();
    }

    fn has_data(&self) -> bool {
        self.count > 0
    }
}

#[derive(Default, Clone)]
struct AveragedFeatures {
    best_bid: f64,
    best_ask: f64,
    mid_price: f64,
    microprice: f64,
    spread: f64,
    imbalance: f64,
    pwi_1: f64,
    pwi_5: f64,
    pwi_25: f64,
    pwi_50: f64,
    bid_slope: f64,
    ask_slope: f64,
    volume_imbalance_top5: f64,
    bid_depth_ratio: f64,
    ask_depth_ratio: f64,
    bid_volume_001: f64,
    ask_volume_001: f64,
    last_trade_price: f64,
    trade_imbalance: f64,
    vwap_total: f64,
    vwap_10: f64,
    vwap_50: f64,
    vwap_100: f64,
    vwap_1000: f64,
    price_change: f64,
    avg_trade_size: f64,
    signed_count_momentum: f64,
    trade_rate_10s: f64,
    aggr_ratio_10: f64,
    aggr_ratio_50: f64,
    aggr_ratio_100: f64,
    aggr_ratio_1000: f64,
    order_flow_imbalance: f64,
    order_flow_pressure: f64,
    roll_spread: f64,
    amihuds_lambda: f64,
    kyles_lambda: f64,
    hasbroucks_lambda: f64,
    vpin: f64,
    tick_entropy_1s: f64,
    tick_entropy_5s: f64,
    tick_entropy_10s: f64,
    tick_entropy_15s: f64,
    tick_entropy_30s: f64,
    tick_entropy_1m: f64,
    tick_entropy_15m: f64,
    volume_tick_entropy_1s: f64,
    volume_tick_entropy_5s: f64,
    volume_tick_entropy_10s: f64,
    volume_tick_entropy_15s: f64,
    volume_tick_entropy_30s: f64,
    volume_tick_entropy_1m: f64,
    volume_tick_entropy_15m: f64,
    // Volatility
    realized_volatility_100: f64,
    realized_volatility_1000: f64,
    bipower_variation_100: f64,
    jump_indicator: f64,
    vol_of_vol: f64,
    // Toxicity
    toxic_flow_ratio_micro: f64,
    toxic_flow_ratio_mid: f64,
    adverse_selection_micro: f64,
    adverse_selection_mid: f64,
    arrival_asymmetry: f64,
    size_toxicity_ratio: f64,
    toxicity_index: f64,
    samples: usize,
}

fn dec_to_f64(d: Option<Decimal>) -> f64 {
    d.and_then(|d| d.to_f64()).unwrap_or(0.0)
}

/// Format risk action for display
fn format_risk_action(action: &RiskAction) -> &'static str {
    match action {
        RiskAction::Allow => "OK",
        RiskAction::ReduceOnly => "REDUCE",
        RiskAction::Halt { .. } => "HALT",
        RiskAction::Emergency { .. } => "EMERG",
    }
}

/// Get color for risk action
fn risk_action_color(action: &RiskAction) -> Color {
    match action {
        RiskAction::Allow => Color::Green,
        RiskAction::ReduceOnly => Color::Yellow,
        RiskAction::Halt { .. } => Color::Red,
        RiskAction::Emergency { .. } => Color::Magenta,
    }
}

/// Academic feature descriptions
fn get_feature_descriptions() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        // Order Book Features
        ("ORDER BOOK FEATURES", "", ""),
        ("Best Bid/Ask", "Top-of-book prices", "The highest bid and lowest ask prices currently available in the limit order book (Kyle, 1985)."),
        ("Mid Price", "(bid + ask) / 2", "Arithmetic mean of best bid and ask; commonly used as a fair value estimate (Harris, 2003)."),
        ("Microprice", "Volume-weighted mid", "Mid price adjusted by order book imbalance: mid + spread * (Vb-Va)/(Vb+Va). More accurate fair value (Gatheral & Oomen, 2010)."),
        ("Spread", "ask - bid", "Transaction cost measure; tighter spreads indicate higher liquidity (Roll, 1984)."),
        ("Imbalance", "(Vb-Va)/(Vb+Va)", "Order book imbalance at top level; predicts short-term price direction (Cont et al., 2014)."),
        ("PWI 1%/5%/25%/50%", "Price-Weighted Imbalance", "Cumulative imbalance at different depth percentiles; captures liquidity distribution (Cartea et al., 2015)."),
        ("Bid/Ask Slope", "dV/dP regression", "Linear regression slope of volume vs price distance; measures order book resilience (Bouchaud et al., 2002)."),
        ("Volume Imbalance Top 5", "Imbalance of top 5 levels", "Extended imbalance measure capturing deeper book dynamics."),
        ("Depth Ratio", "Top3/Top10 volume", "Concentration of liquidity near best prices; high ratio = thin book (Gould et al., 2013)."),
        ("Volume 0.01%", "Volume within 1bp", "Immediate liquidity available within 1 basis point of mid price."),

        // Trade Features
        ("TRADE FEATURES", "", ""),
        ("Last Trade Price", "Most recent execution", "Price of the last matched trade; tracks market activity."),
        ("Trade Imbalance", "Buy/Sell volume ratio", "Ratio of buyer-initiated to total volume; indicates aggression direction (Lee & Ready, 1991)."),
        ("VWAP", "Volume-Weighted Avg Price", "Average execution price weighted by volume; benchmark for execution quality (Berkowitz et al., 1988)."),
        ("Price Change", "P(t) - P(t-1)", "Tick-to-tick price movement; basis for return calculations."),
        ("Avg Trade Size", "Mean quantity", "Average trade size; larger sizes may indicate institutional activity."),
        ("Signed Momentum", "Net buy/sell count", "Cumulative count of buy vs sell trades; measures directional pressure."),
        ("Trade Rate", "Trades per second", "Trading intensity measure; high rate indicates active market."),
        ("Aggressor Ratio", "Taker/Total trades", "Proportion of aggressive (market) orders; high ratio = directional conviction (Biais et al., 1995)."),

        // Order Flow Features
        ("ORDER FLOW FEATURES", "", ""),
        ("Flow Imbalance", "Placement vs Cancel", "Net order flow from placements minus cancellations; real-time pressure indicator."),
        ("Flow Pressure", "Cumulative flow", "Integrated order flow over time window; sustained pressure predicts moves."),

        // Illiquidity Metrics
        ("ILLIQUIDITY METRICS", "", ""),
        ("Roll Spread", "2*sqrt(-cov)", "Effective spread estimator from price autocovariance (Roll, 1984)."),
        ("Amihud Lambda", "|r|/V", "Price impact per unit volume; illiquidity measure (Amihud, 2002)."),
        ("Kyle Lambda", "dP/dQ slope", "Permanent price impact coefficient from regression (Kyle, 1985)."),
        ("Hasbrouck Lambda", "Trade impact", "Effective spread from trade-by-trade analysis (Hasbrouck, 2009)."),
        ("VPIN", "Volume-sync. PIN", "Probability of informed trading; toxicity indicator (Easley et al., 2012)."),

        // Entropy Metrics
        ("ENTROPY METRICS", "", ""),
        ("Tick Entropy", "H = -Σp*log(p)", "Shannon entropy of price tick directions (up/down/unchanged). Range [0, log₂(3)]. Higher = more random (Shannon, 1948)."),
        ("Volume Entropy", "Volume-weighted H", "Entropy weighted by trade volume; accounts for trade significance."),
        ("Time Windows", "1s to 15m", "Multi-scale entropy captures regime changes at different frequencies."),

        // Volatility Metrics
        ("VOLATILITY METRICS", "", ""),
        ("Realized Volatility", "RV = √(Σr²/n)", "Sum of squared returns over window. Standard volatility estimator for quadratic variation."),
        ("RV Windows", "100 and 1000 trades", "Short (100) and long (1000) trade windows for multi-scale volatility."),
        ("Bipower Variation", "BV = (π/2)×Σ|r_t||r_{t-1}|", "Jump-robust volatility estimator using adjacent absolute returns (Barndorff-Nielsen & Shephard, 2004)."),
        ("Jump Indicator", "Z = (RV-BV)/√Var(BV)", "Statistical test for price jumps. Z > 3 indicates significant jump at 99.7% confidence."),
        ("Vol-of-Vol", "σ(σ_t)", "Volatility of volatility; measures regime instability and second-order uncertainty."),

        // Toxicity Metrics
        ("TOXICITY METRICS", "", ""),
        ("Toxic Flow Ratio", "Toxic Vol / Total Vol", "Proportion of volume that trades against fair value. Higher = more informed flow (Easley et al., 2012)."),
        ("Adverse Selection", "E[cost to informed]", "Expected loss per unit traded to informed traders. In price units."),
        ("Arrival Asymmetry", "(buys-sells)/total", "Normalized difference between buy and sell arrival rates. Directional pressure indicator."),
        ("Size Toxicity", "Large/Small toxic ratio", "Compares toxicity of large vs small trades. > 1 means large trades more informed."),
        ("Toxicity Index", "Composite score [0,1]", "Weighted combination of toxicity measures. Higher = more toxic trading environment."),

        // Fill Simulation (Backtesting)
        ("FILL SIMULATION", "", ""),
        ("Queue Position", "0=front, 1=back", "Position in limit order book queue. Front of queue gets filled first; back rarely gets filled (Moallemi & Yuan, 2017)."),
        ("Fill Probability", "P(fill | touch)", "Probability of getting filled given price touches our quote. Depends on queue position, trade intensity, and market regime."),
        ("Adverse Selection", "E[loss | filled]", "Expected loss when filled due to informed traders. Fills tend to precede unfavorable price moves (Cont et al., 2014)."),
        ("Trade Intensity", "Trades/second", "Higher trade rate increases fill probability. More market orders hitting the book = more chances to get filled."),
        ("Spread Competitiveness", "Our spread vs market", "Tighter spreads relative to market get priority fills. But too tight = adverse selection."),
    ]
}

/// Run the TUI with menu system
/// Returns the settings chosen by the user (including persistence option)
pub fn run_tui(rx: Receiver<FeaturesSnapshot>, symbol: String) -> anyhow::Result<TuiSettings> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Term::new(backend)?;
    terminal.clear()?;

    let settings = TuiSettings::default();
    let res = main_loop(&mut terminal, rx, symbol, settings);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

fn main_loop(
    terminal: &mut Term,
    rx: Receiver<FeaturesSnapshot>,
    symbol: String,
    mut settings: TuiSettings,
) -> anyhow::Result<TuiSettings> {
    let mut mode = AppMode::NewMenu;
    let mut scroll_offset: u16 = 0;
    let mut last_update = Instant::now();
    let mut accumulator = FeatureAccumulator::default();
    let mut current_features = AveragedFeatures::default();
    let mut has_data = false;
    let mut last_snapshot: Option<FeaturesSnapshot> = None;

    // History for sparklines
    let mut microprice_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut pwi50_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut entropy_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut volatility_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);

    // Market maker state - default engine for LiveMM mode
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig::default();
    let mut paper_trading = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    // Risk-managed paper trading engine for preset mode (supports multiple algorithms with risk controls)
    let mut risk_managed_paper_trading: Option<RiskManagedPaperTradingEngine> = None;

    // Forward testing session for trade logging
    let mut forward_session = ForwardTestSession::new(ForwardTestConfig::default());

    // Preset store for paper trading with optimized parameters
    let mut preset_store = PresetStore::load();
    let mut selected_preset_idx: usize = 0;
    let mut active_preset: Option<ParameterPreset> = None;

    // Research dashboard screen (Task 4.1)
    let mut research_screen = ResearchScreen::new("./data/research")
        .with_symbol(&symbol);

    // New TUI v0.1 main menu state (under development)
    let mut main_menu_state = MainMenuState::new(&symbol);

    // TUI-7.0: Menu integration with submenus
    let mut global_state = GlobalState {
        symbol: symbol.clone(),
        persist_features: settings.persist_features,
        max_storage_gb: settings.max_storage_gb,
        ..Default::default()
    };
    let mut menu_integration = MenuIntegration::new();

    loop {
        // Handle input
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match mode {
                    AppMode::Live => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::NewMenu,
                        _ => {}
                    },
                    AppMode::LiveMM => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            // End forward testing session and save
                            if forward_session.is_active() {
                                if let Ok(summary) = forward_session.end() {
                                    // Session saved to ./data/sessions/
                                    log::info!("Session {} saved with {} trades",
                                        summary.session_id, summary.trade_count);
                                }
                            }
                            mode = AppMode::NewMenu;
                        }
                        KeyCode::Char('r') => {
                            // Reset MM state and start new session
                            paper_trading.reset();
                            forward_session = ForwardTestSession::new(ForwardTestConfig::default());
                            forward_session.start();
                        }
                        _ => {}
                    },
                    AppMode::PresetSelect => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::NewMenu,
                        KeyCode::Up | KeyCode::Char('k') => {
                            if selected_preset_idx > 0 {
                                selected_preset_idx -= 1;
                            }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if selected_preset_idx + 1 < preset_store.presets.len() {
                                selected_preset_idx += 1;
                            }
                        }
                        KeyCode::Enter => {
                            // Select preset and start paper trading with risk management
                            if let Some(preset) = preset_store.get(selected_preset_idx) {
                                // Use queue position model for more realistic fills
                                let sim_config = SimulatorConfig {
                                    use_queue_model: true,
                                    queue_position_fraction: 0.5, // Middle of queue
                                    ..Default::default()
                                };

                                // Create algorithm from preset (supports A-S and ML)
                                let algorithm = preset.create_algorithm();

                                // Create risk-managed engine with default risk config
                                risk_managed_paper_trading = Some(RiskManagedPaperTradingEngine::with_default_risk(
                                    algorithm,
                                    sim_config,
                                ));

                                active_preset = Some(preset.clone());
                                forward_session = ForwardTestSession::new(ForwardTestConfig::default());
                                forward_session.start();
                                mode = AppMode::PaperTradePreset;
                            }
                        }
                        _ => {}
                    },
                    AppMode::PaperTradePreset => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            // End forward testing session and save
                            if forward_session.is_active() {
                                if let Ok(summary) = forward_session.end() {
                                    log::info!("Preset session {} saved with {} trades",
                                        summary.session_id, summary.trade_count);
                                }
                            }
                            active_preset = None;
                            risk_managed_paper_trading = None;
                            mode = AppMode::NewMenu;
                        }
                        KeyCode::Char('r') => {
                            // Reset but keep preset (including risk manager)
                            if let Some(ref mut engine) = risk_managed_paper_trading {
                                engine.reset();
                            }
                            forward_session = ForwardTestSession::new(ForwardTestConfig::default());
                            forward_session.start();
                        }
                        KeyCode::Char('h') => {
                            // Manual halt toggle - useful for emergency situations
                            if let Some(ref mut engine) = risk_managed_paper_trading {
                                let current_time = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64;
                                let state = engine.state();
                                if matches!(state.risk_action, RiskAction::Halt { .. } | RiskAction::Emergency { .. }) {
                                    engine.manual_reset(current_time);
                                } else {
                                    engine.manual_halt(current_time);
                                }
                            }
                        }
                        _ => {}
                    },
                    AppMode::Features => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::NewMenu,
                        KeyCode::Up | KeyCode::Char('k') => {
                            scroll_offset = scroll_offset.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            scroll_offset = scroll_offset.saturating_add(1);
                        }
                        KeyCode::PageUp => {
                            scroll_offset = scroll_offset.saturating_sub(10);
                        }
                        KeyCode::PageDown => {
                            scroll_offset = scroll_offset.saturating_add(10);
                        }
                        _ => {}
                    },
                    AppMode::Backtest | AppMode::WalkForward | AppMode::DataQuality | AppMode::CampaignSimulation | AppMode::DataInfo | AppMode::GridSearch | AppMode::Sweep | AppMode::OOSValidation => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::NewMenu,
                        KeyCode::Up | KeyCode::Char('k') => {
                            scroll_offset = scroll_offset.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            scroll_offset = scroll_offset.saturating_add(1);
                        }
                        _ => {}
                    },
                    AppMode::Research => {
                        // Pass key events to research screen and check if it wants to exit
                        if research_screen.handle_key(key.code) {
                            mode = AppMode::NewMenu;
                        }
                    },
                    // T-4.5: Handle config screen input
                    AppMode::ConfigScreen(ref target) => {
                        // TODO: Handle config screen input
                        // For now, just allow escape to go back
                        match key.code {
                            KeyCode::Esc | KeyCode::Char('q') => {
                                mode = AppMode::NewMenu;
                            }
                            KeyCode::Enter => {
                                // Execute command - TODO: implement
                                // For now, just show a message
                                menu_integration.set_message("Command execution not yet implemented".to_string());
                            }
                            _ => {}
                        }
                    }
                    // T-4.5: Handle results screen input
                    AppMode::ResultsScreen(ref _target) => {
                        match key.code {
                            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('b') => {
                                mode = AppMode::NewMenu;
                            }
                            _ => {}
                        }
                    }
                    // T-4.5: Handle executing command (show progress, allow cancel)
                    AppMode::ExecutingCommand { ref config_target, ref command_name } => {
                        match key.code {
                            KeyCode::Esc | KeyCode::Char('c') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                                // Cancel command execution
                                let cmd_name = command_name.clone();
                                mode = AppMode::NewMenu;
                                menu_integration.set_message(format!("Cancelled: {}", cmd_name));
                            }
                            _ => {}
                        }
                    }
                    AppMode::NewMenu => {
                        // TUI-7.0: Handle new TUI main menu with submenu navigation
                        if menu_integration.current.is_main_menu() {
                            // We're at the main menu level
                            match key.code {
                                KeyCode::Esc => {
                                    // Quit the application (NewMenu is the main menu now)
                                    return Ok(settings);
                                }
                                KeyCode::Char(c) => {
                                    // Handle menu selection via MainMenuItem
                                    if let Some(item) = MainMenuItem::from_key(c) {
                                        main_menu_state.selected = Some(item);
                                        match item {
                                            MainMenuItem::Research => {
                                                menu_integration.navigate_to(CurrentSubMenu::Research);
                                            }
                                            MainMenuItem::Algorithms => {
                                                menu_integration.navigate_to(CurrentSubMenu::Algorithms);
                                            }
                                            MainMenuItem::Validate => {
                                                menu_integration.navigate_to(CurrentSubMenu::Validate);
                                            }
                                            MainMenuItem::Trade => {
                                                menu_integration.navigate_to(CurrentSubMenu::Trade);
                                            }
                                            MainMenuItem::Data => {
                                                menu_integration.navigate_to(CurrentSubMenu::Data);
                                            }
                                            MainMenuItem::Quit => {
                                                return Ok(settings);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        } else {
                            // We're in a submenu - delegate to the appropriate submenu handler
                            let action = match menu_integration.current {
                                CurrentSubMenu::Research => {
                                    menu_integration.research_menu.handle_key(key.code, &global_state)
                                }
                                CurrentSubMenu::Algorithms => {
                                    menu_integration.algorithms_menu.handle_key(key.code, &global_state)
                                }
                                CurrentSubMenu::Validate => {
                                    menu_integration.validate_menu.handle_key(key.code, &global_state)
                                }
                                CurrentSubMenu::Trade => {
                                    menu_integration.trade_menu.handle_key(key.code, &global_state)
                                }
                                CurrentSubMenu::Data => {
                                    menu_integration.data_menu.handle_key(key.code, &global_state)
                                }
                                CurrentSubMenu::None => crate::ui::submenu::SubMenuAction::None,
                            };

                            // Process the action
                            match process_action(action) {
                                ActionResult::None | ActionResult::Stay => {}
                                ActionResult::NavigateToSubMenu(submenu) => {
                                    menu_integration.navigate_to(submenu);
                                }
                                ActionResult::NavigateToMode(target) => {
                                    // Navigate to AppMode based on target
                                    use crate::ui::submenu::NavigationTarget;
                                    match target {
                                        NavigationTarget::Live => mode = AppMode::Live,
                                        NavigationTarget::LiveMM => mode = AppMode::LiveMM,
                                        NavigationTarget::Backtest => mode = AppMode::Backtest,
                                        NavigationTarget::WalkForward => mode = AppMode::WalkForward,
                                        NavigationTarget::DataQuality => mode = AppMode::DataQuality,
                                        NavigationTarget::CampaignSimulation => mode = AppMode::CampaignSimulation,
                                        NavigationTarget::DataInfo => mode = AppMode::DataInfo,
                                        NavigationTarget::GridSearch => mode = AppMode::GridSearch,
                                        NavigationTarget::Sweep => mode = AppMode::Sweep,
                                        NavigationTarget::OOSValidation => mode = AppMode::OOSValidation,
                                        NavigationTarget::Research => mode = AppMode::Research,
                                        NavigationTarget::PresetSelect => mode = AppMode::PresetSelect,
                                        NavigationTarget::Features => mode = AppMode::Features,
                                        _ => {}
                                    }
                                    // Reset submenu state when leaving
                                    menu_integration.go_back();
                                }
                                ActionResult::NavigateToConfigScreen(target) => {
                                    // T-4.5: Navigate to config screen
                                    mode = AppMode::ConfigScreen(target.clone());
                                    menu_integration.go_back(); // Exit submenu
                                }
                                ActionResult::NavigateToResultsScreen(target) => {
                                    // T-4.4: Navigate to results screen
                                    // TODO: Implement results screen display
                                    menu_integration.set_message(format!(
                                        "Results screen for {} - Implementation in progress",
                                        target.display_name()
                                    ));
                                }
                                ActionResult::ShowMessage(msg) => {
                                    menu_integration.set_message(msg);
                                }
                                ActionResult::ExecuteCommand(_cmd) => {
                                    // CLI command execution would go here
                                    // For now, just show a message
                                    menu_integration.set_message("CLI commands not yet implemented".to_string());
                                }
                                ActionResult::UpdateSetting(update) => {
                                    // Handle settings updates
                                    match update {
                                        SettingUpdate::TogglePersist => {
                                            settings.persist_features = !settings.persist_features;
                                            global_state.persist_features = settings.persist_features;
                                            let status = if settings.persist_features { "ON" } else { "OFF" };
                                            menu_integration.set_message(format!("Persist to disk: {}", status));
                                        }
                                        SettingUpdate::CycleMaxStorage => {
                                            settings.max_storage_gb = match settings.max_storage_gb as i32 {
                                                0 => 1.0,
                                                1 => 5.0,
                                                5 => 10.0,
                                                10 => 50.0,
                                                50 => 100.0,
                                                _ => 0.0, // unlimited
                                            };
                                            global_state.max_storage_gb = settings.max_storage_gb;
                                            let label = if settings.max_storage_gb <= 0.0 {
                                                "Unlimited".to_string()
                                            } else {
                                                format!("{} GB", settings.max_storage_gb as i32)
                                            };
                                            menu_integration.set_message(format!("Max storage: {}", label));
                                        }
                                    }
                                }
                                ActionResult::Quit => {
                                    return Ok(settings);
                                }
                            }

                            // Clear message on any key if one is showing
                            if menu_integration.has_message() && !matches!(process_action(crate::ui::submenu::SubMenuAction::None), ActionResult::ShowMessage(_)) {
                                menu_integration.clear_message();
                            }
                        }
                    },
                }
            }
        }

        // Drain data from channel
        while let Ok(snap) = rx.try_recv() {
            accumulator.add(&snap);
            has_data = true;
            last_snapshot = Some(snap);
        }

        // Update averages every second
        if last_update.elapsed() >= Duration::from_millis(UPDATE_INTERVAL_MS) {
            if accumulator.has_data() {
                current_features = accumulator.average();

                microprice_hist.push_back(current_features.microprice);
                if microprice_hist.len() > MAX_HISTORY {
                    microprice_hist.pop_front();
                }

                pwi50_hist.push_back(current_features.pwi_50);
                if pwi50_hist.len() > MAX_HISTORY {
                    pwi50_hist.pop_front();
                }

                entropy_hist.push_back(current_features.tick_entropy_1m);
                if entropy_hist.len() > MAX_HISTORY {
                    entropy_hist.pop_front();
                }

                volatility_hist.push_back(current_features.realized_volatility_100);
                if volatility_hist.len() > MAX_HISTORY {
                    volatility_hist.pop_front();
                }

                // Update MM engine with latest features
                if let Some(ref snap) = last_snapshot {
                    let microprice = snap.microprice.unwrap_or(snap.mid_price.unwrap_or_default());
                    let mid_price = snap.mid_price.unwrap_or_default();
                    let volatility = snap.realized_volatility_100.unwrap_or(0.001);

                    // Compute entropy score from tick entropies using algorithms module
                    let entropy_score = crate::strategies::compute_entropy_score(
                        snap.tick_entropy_1s,
                        snap.tick_entropy_5s,
                        snap.tick_entropy_10s,
                    );

                    // Compute flow imbalance from aggressor ratios using algorithms module
                    let buy_vol = snap.aggr_ratio_100.unwrap_or(Decimal::new(5, 1)); // 0.5 default
                    let sell_vol = Decimal::ONE - buy_vol;
                    let flow_imbalance = crate::strategies::compute_flow_imbalance(buy_vol, sell_vol);

                    let timestamp_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    match mode {
                        AppMode::LiveMM => {
                            let quotes = paper_trading.on_features(
                                microprice,
                                mid_price,
                                volatility,
                                entropy_score,
                                flow_imbalance,
                                timestamp_ms,
                            );

                            // Log quotes to forward testing session
                            if forward_session.is_active() {
                                forward_session.log_quote(
                                    timestamp_ms,
                                    quotes.bid.as_ref().map(|q| q.price),
                                    quotes.bid.as_ref().map(|q| q.size),
                                    quotes.ask.as_ref().map(|q| q.price),
                                    quotes.ask.as_ref().map(|q| q.size),
                                    mid_price,
                                    paper_trading.mm.inventory(),
                                    &format!("{:?}", quotes.regime),
                                );
                            }
                        }
                        AppMode::PaperTradePreset => {
                            if let Some(ref mut engine) = risk_managed_paper_trading {
                                let quotes = engine.on_features(
                                    microprice,
                                    mid_price,
                                    volatility,
                                    entropy_score,
                                    flow_imbalance,
                                    timestamp_ms,
                                );

                                // Log quotes to forward testing session
                                if forward_session.is_active() {
                                    let state = engine.state();
                                    forward_session.log_quote(
                                        timestamp_ms,
                                        quotes.bid.as_ref().map(|q| q.price),
                                        quotes.bid.as_ref().map(|q| q.size),
                                        quotes.ask.as_ref().map(|q| q.price),
                                        quotes.ask.as_ref().map(|q| q.size),
                                        mid_price,
                                        state.trading_state.mm_state.inventory,
                                        &format!("{:?} [{}]", quotes.regime, format_risk_action(&state.risk_action)),
                                    );
                                }
                            }
                        }
                        _ => {}
                    }
                }

                accumulator.reset();
            }
            last_update = Instant::now();
        }

        // Draw based on mode
        match mode {
            AppMode::Live => {
                terminal.draw(|f| {
                    if !has_data {
                        draw_waiting(f);
                    } else {
                        draw_live(f, &symbol, &current_features, &microprice_hist, &pwi50_hist, &entropy_hist, &volatility_hist);
                    }
                })?;
            }
            AppMode::LiveMM => {
                terminal.draw(|f| {
                    if !has_data {
                        draw_waiting(f);
                    } else {
                        let state = paper_trading.state();
                        let max_inv = paper_trading.mm.config().max_inventory;
                        draw_live_mm(f, &symbol, &current_features, &state, max_inv, &forward_session, None);
                    }
                })?;
            }
            AppMode::PresetSelect => {
                terminal.draw(|f| draw_preset_select(f, &preset_store, selected_preset_idx))?;
            }
            AppMode::PaperTradePreset => {
                terminal.draw(|f| {
                    if !has_data {
                        draw_waiting(f);
                    } else if let Some(ref engine) = risk_managed_paper_trading {
                        let state = engine.state();
                        let max_inv = engine.max_inventory();
                        draw_live_mm_with_risk(f, &symbol, &current_features, &state, max_inv, &forward_session, active_preset.as_ref());
                    } else {
                        // Fallback if no engine (shouldn't happen in this mode)
                        draw_waiting(f);
                    }
                })?;
            }
            AppMode::Features => {
                terminal.draw(|f| draw_features(f, &mut scroll_offset))?;
            }
            AppMode::Backtest => {
                terminal.draw(|f| draw_backtest_screen(f, &mut scroll_offset))?;
            }
            AppMode::WalkForward => {
                terminal.draw(|f| draw_walkforward_screen(f, &mut scroll_offset))?;
            }
            AppMode::DataQuality => {
                terminal.draw(|f| draw_dataquality_screen(f, &mut scroll_offset))?;
            }
            AppMode::CampaignSimulation => {
                terminal.draw(|f| draw_campaign_screen(f, &mut scroll_offset))?;
            }
            AppMode::DataInfo => {
                terminal.draw(|f| draw_data_info_screen(f))?;
            }
            AppMode::GridSearch => {
                terminal.draw(|f| draw_grid_search_screen(f, &mut scroll_offset))?;
            }
            AppMode::Sweep => {
                terminal.draw(|f| draw_sweep_screen(f, &mut scroll_offset))?;
            }
            AppMode::OOSValidation => {
                terminal.draw(|f| draw_oos_validation_screen(f, &mut scroll_offset))?;
            }
            AppMode::Research => {
                // Auto-refresh if needed
                if research_screen.needs_refresh() {
                    let _ = research_screen.refresh();
                }
                terminal.draw(|f| draw_research_screen(f, &research_screen.state))?;
            }
            // T-4.5: Draw config screen
            AppMode::ConfigScreen(ref target) => {
                // TODO: Draw config screen based on target
                terminal.draw(|f| {
                    use ratatui::widgets::Paragraph;
                    use ratatui::layout::Alignment;
                    use ratatui::style::Style;
                    use ratatui::text::Line;
                    let area = f.size();
                    let text = vec![
                        Line::from(""),
                        Line::from(format!("Config Screen: {}", target.display_name())),
                        Line::from(""),
                        Line::from("Implementation in progress..."),
                        Line::from(""),
                        Line::from("Press ESC or 'q' to go back"),
                        Line::from("Press Enter to execute (when implemented)"),
                    ];
                    let para = Paragraph::new(text)
                        .style(Style::default().fg(ratatui::style::Color::Yellow))
                        .alignment(Alignment::Center)
                        .block(ratatui::widgets::Block::default()
                            .borders(ratatui::widgets::Borders::ALL)
                            .title("Config Screen"));
                    f.render_widget(para, area);
                })?;
            }
            // T-4.5: Draw results screen
            AppMode::ResultsScreen(ref target) => {
                // TODO: Draw results screen based on target
                terminal.draw(|f| {
                    use ratatui::widgets::Paragraph;
                    use ratatui::layout::Alignment;
                    use ratatui::style::Style;
                    use ratatui::text::Line;
                    let area = f.size();
                    let text = vec![
                        Line::from(""),
                        Line::from(format!("Results Screen: {}", target.display_name())),
                        Line::from(""),
                        Line::from("Implementation in progress..."),
                        Line::from(""),
                        Line::from("Press ESC, 'q', or 'b' to go back"),
                    ];
                    let para = Paragraph::new(text)
                        .style(Style::default().fg(ratatui::style::Color::Green))
                        .alignment(Alignment::Center)
                        .block(ratatui::widgets::Block::default()
                            .borders(ratatui::widgets::Borders::ALL)
                            .title("Results Screen"));
                    f.render_widget(para, area);
                })?;
            }
            // T-4.5: Draw executing command (progress screen)
            AppMode::ExecutingCommand { ref command_name, .. } => {
                terminal.draw(|f| {
                    use ratatui::widgets::{Paragraph, Gauge};
                    use ratatui::layout::{Alignment, Constraint, Direction, Layout};
                    use ratatui::style::Style;
                    use ratatui::text::Line;
                    let area = f.size();
                    let chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Length(3),
                            Constraint::Length(3),
                            Constraint::Min(1),
                        ])
                        .split(area);
                    let text = vec![
                        Line::from(""),
                        Line::from(format!("Executing: {}", command_name)),
                        Line::from(""),
                    ];
                    let para = Paragraph::new(text)
                        .style(Style::default().fg(ratatui::style::Color::Cyan))
                        .alignment(Alignment::Center)
                        .block(ratatui::widgets::Block::default()
                            .borders(ratatui::widgets::Borders::ALL)
                            .title("Executing Command"));
                    f.render_widget(para, chunks[0]);
                    // Progress bar (placeholder - would show actual progress)
                    let gauge = Gauge::default()
                        .block(ratatui::widgets::Block::default().borders(ratatui::widgets::Borders::ALL).title("Progress"))
                        .gauge_style(Style::default().fg(ratatui::style::Color::Yellow))
                        .percent(50); // Placeholder
                    f.render_widget(gauge, chunks[1]);
                    let help = Paragraph::new("Press Ctrl+C to cancel")
                        .style(Style::default().fg(ratatui::style::Color::DarkGray))
                        .alignment(Alignment::Center);
                    f.render_widget(help, chunks[2]);
                })?;
            }
            AppMode::NewMenu => {
                // TUI-7.0: Draw the new TUI with submenu support
                if menu_integration.current.is_main_menu() {
                    // Draw main menu
                    terminal.draw(|f| draw_main_menu(f, &main_menu_state))?;
                } else {
                    // Draw current submenu with status bar
                    terminal.draw(|f| draw_menu_with_status(f, &menu_integration, &global_state))?;
                }
            }
        }
    }
}

fn draw_waiting(f: &mut ratatui::Frame) {
    let size = f.size();
    let block = Block::default()
        .title(Span::styled(
            " Waiting for market data... [q] back ",
            Style::default().fg(Color::Gray),
        ))
        .borders(Borders::ALL);
    f.render_widget(block, size);
}

/// Draw data info screen - mirrors CLI `backtest info` command
fn draw_data_info_screen(f: &mut ratatui::Frame) {
    let size = f.size();

    // Load data info using the replay module
    let data_dir = std::path::PathBuf::from("./data/features");

    // Collect parquet file info
    let mut file_count = 0;
    let mut total_size_bytes: u64 = 0;
    let mut oldest_file: Option<std::time::SystemTime> = None;
    let mut newest_file: Option<std::time::SystemTime> = None;

    if data_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|x| x == "parquet").unwrap_or(false) {
                    file_count += 1;
                    if let Ok(metadata) = entry.metadata() {
                        total_size_bytes += metadata.len();
                        if let Ok(modified) = metadata.modified() {
                            match &oldest_file {
                                None => oldest_file = Some(modified),
                                Some(old) if modified < *old => oldest_file = Some(modified),
                                _ => {}
                            }
                            match &newest_file {
                                None => newest_file = Some(modified),
                                Some(new) if modified > *new => newest_file = Some(modified),
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }

    let total_size_mb = total_size_bytes as f64 / 1_000_000.0;

    // Try to load event count using replay
    let (event_count, time_range_str, duration_str, event_rate_str) = {
        use crate::backtest::replay::{ParquetReplay, ReplayConfig};

        let config = ReplayConfig {
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        let mut replay = ParquetReplay::new(config);
        match replay.load() {
            Ok(num_events) => {
                if let Some((start, end)) = replay.time_range() {
                    let duration_ms = end - start;
                    let duration_hours = duration_ms as f64 / (1000.0 * 60.0 * 60.0);
                    let duration_days = duration_hours / 24.0;

                    let start_dt = chrono::DateTime::from_timestamp_millis(start)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "Unknown".to_string());
                    let end_dt = chrono::DateTime::from_timestamp_millis(end)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "Unknown".to_string());

                    let event_rate = num_events as f64 / (duration_ms as f64 / 1000.0);

                    (
                        format!("{}", num_events),
                        format!("{} to {}", start_dt, end_dt),
                        format!("{:.1} hours ({:.2} days)", duration_hours, duration_days),
                        format!("{:.1} events/second", event_rate),
                    )
                } else {
                    (format!("{}", num_events), "Unknown".to_string(), "Unknown".to_string(), "Unknown".to_string())
                }
            }
            Err(_) => ("Error loading".to_string(), "Unknown".to_string(), "Unknown".to_string(), "Unknown".to_string()),
        }
    };

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  DATA INFO / STATISTICS",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Equivalent to: cargo run --bin backtest -- info",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled("  FILE STATISTICS", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::raw("  Directory:     "),
            Span::styled(format!("{:?}", data_dir), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::raw("  Parquet Files: "),
            Span::styled(format!("{}", file_count), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(vec![
            Span::raw("  Total Size:    "),
            Span::styled(format!("{:.1} MB ({:.2} GB)", total_size_mb, total_size_mb / 1000.0), Style::default().fg(Color::Cyan)),
        ]),
        Line::from(""),
        Line::from(Span::styled("  EVENT STATISTICS", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::raw("  Total Events:  "),
            Span::styled(event_count, Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(vec![
            Span::raw("  Event Rate:    "),
            Span::styled(event_rate_str, Style::default().fg(Color::Cyan)),
        ]),
        Line::from(""),
        Line::from(Span::styled("  TIME RANGE", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::raw("  Range:         "),
            Span::styled(time_range_str, Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::raw("  Duration:      "),
            Span::styled(duration_str, Style::default().fg(Color::Cyan)),
        ]),
        Line::from(""),
        Line::from(""),
        Line::from(vec![
            Span::styled("  [q] ", Style::default().fg(Color::Red)),
            Span::raw("Back to menu"),
        ]),
        Line::from(""),
    ];

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" DATA INFO ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(para, size);
}

/// Draw grid search screen - runs hyperparameter optimization
/// Mirrors CLI `backtest grid-search` command
fn draw_grid_search_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    use crate::backtest::grid_search::{GridSearchEngine, GridSearchConfig};
    use crate::backtest::replay::ReplayConfig;

    let size = f.size();
    let data_dir = std::path::PathBuf::from("./data/features");

    // Static storage for results (persists across redraws)
    use std::sync::OnceLock;
    static GRID_SEARCH_RESULTS: OnceLock<Result<crate::backtest::grid_search::GridSearchResults, String>> = OnceLock::new();

    let results = GRID_SEARCH_RESULTS.get_or_init(|| {
        let config = GridSearchConfig::default();
        let replay_config = ReplayConfig {
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        let engine = GridSearchEngine::new(config, replay_config);
        engine.run().map_err(|e| e.to_string())
    });

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  GRID SEARCH - HYPERPARAMETER OPTIMIZATION",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Equivalent to: cargo run --bin backtest -- grid-search",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    match results {
        Ok(grid_results) => {
            let config = &grid_results.config;

            // Configuration section
            lines.push(Line::from(Span::styled("  PARAMETER SPACE", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(vec![
                Span::raw("  Spreads:          "),
                Span::styled(format!("{:?}", config.spreads), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Skews:            "),
                Span::styled(format!("{:?}", config.skews), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  High Entropies:   "),
                Span::styled(format!("{:?}", config.high_entropies), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Fill Probs:       "),
                Span::styled(format!("{:?}", config.fill_probs), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Total Combinations: "),
                Span::styled(format!("{}", config.total_combinations()), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));

            // Top results
            lines.push(Line::from(Span::styled("  TOP 10 PARAMETER SETS (by Sharpe)", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(""));

            for (i, result) in grid_results.top_n(10).iter().enumerate() {
                let sharpe_color = if result.sharpe > 0.0 { Color::Green } else { Color::Red };
                let return_color = if result.total_return > 0.0 { Color::Green } else { Color::Red };

                lines.push(Line::from(vec![
                    Span::styled(format!("  {:>2}. ", i + 1), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                    Span::raw(format!("Spread={:.1} ", result.spread)),
                    Span::raw(format!("Skew={:.1} ", result.skew)),
                    Span::raw(format!("Entropy={:.1} ", result.high_entropy_threshold)),
                    Span::raw(format!("FillP={:.2}", result.fill_prob)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("      "),
                    Span::styled(format!("Sharpe={:+.2} ", result.sharpe), Style::default().fg(sharpe_color)),
                    Span::styled(format!("Return={:+.2}% ", result.total_return * 100.0), Style::default().fg(return_color)),
                    Span::raw(format!("DD={:.2}% ", result.max_drawdown * 100.0)),
                    Span::raw(format!("WR={:.1}% ", result.win_rate * 100.0)),
                    Span::raw(format!("Tr={}", result.num_trades)),
                ]));
            }

            lines.push(Line::from(""));

            // Best recommendation
            if let Some(best) = grid_results.best() {
                lines.push(Line::from(Span::styled("  RECOMMENDED PARAMETERS", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))));
                lines.push(Line::from(vec![
                    Span::raw("  base_spread_bps:            "),
                    Span::styled(format!("{}", best.spread), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  inventory_skew_factor:      "),
                    Span::styled(format!("{}", best.skew), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  high_entropy_threshold:     "),
                    Span::styled(format!("{}", best.high_entropy_threshold), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  base_fill_probability:      "),
                    Span::styled(format!("{}", best.fill_prob), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                ]));
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled("  Expected Performance:", Style::default().fg(Color::Yellow))));
                lines.push(Line::from(vec![
                    Span::raw("  Sharpe Ratio: "),
                    Span::styled(format!("{:+.2}", best.sharpe), Style::default().fg(if best.sharpe > 0.0 { Color::Green } else { Color::Red })),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  Total Return: "),
                    Span::styled(format!("{:+.2}%", best.total_return * 100.0), Style::default().fg(if best.total_return > 0.0 { Color::Green } else { Color::Red })),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  Max Drawdown: "),
                    Span::styled(format!("{:.2}%", best.max_drawdown * 100.0), Style::default().fg(Color::Yellow)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  Win Rate:     "),
                    Span::styled(format!("{:.1}%", best.win_rate * 100.0), Style::default().fg(Color::Cyan)),
                ]));
            }
        }
        Err(e) => {
            lines.push(Line::from(Span::styled(
                format!("  Error: {}", e),
                Style::default().fg(Color::Red),
            )));
            lines.push(Line::from(""));
            lines.push(Line::from("  Ensure data exists in ./data/features/"));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  [q] ", Style::default().fg(Color::Red)),
        Span::raw("Back to menu"),
        Span::styled("  [j/k] ", Style::default().fg(Color::Blue)),
        Span::raw("Scroll"),
    ]));
    lines.push(Line::from(""));

    // Handle scrolling
    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" GRID SEARCH ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

/// Draw sweep screen - runs parameter sensitivity analysis
/// Mirrors CLI `backtest sweep` command
fn draw_sweep_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    use crate::backtest::sweep::{SweepEngine, SweepConfig};
    use crate::backtest::replay::ReplayConfig;

    let size = f.size();
    let data_dir = std::path::PathBuf::from("./data/features");

    // Static storage for results (persists across redraws)
    use std::sync::OnceLock;
    static SWEEP_RESULTS: OnceLock<Result<crate::backtest::sweep::SweepResults, String>> = OnceLock::new();

    let results = SWEEP_RESULTS.get_or_init(|| {
        let config = SweepConfig::default();
        let replay_config = ReplayConfig {
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        let engine = SweepEngine::new(config, replay_config);
        engine.run().map_err(|e| e.to_string())
    });

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  SWEEP - PARAMETER SENSITIVITY ANALYSIS",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Equivalent to: cargo run --bin backtest -- sweep",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    match results {
        Ok(sweep_results) => {
            let config = &sweep_results.config;

            // Configuration section
            lines.push(Line::from(Span::styled("  PARAMETER SPACE", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(vec![
                Span::raw("  Spreads:          "),
                Span::styled(format!("{:?}", config.spreads), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Skews:            "),
                Span::styled(format!("{:?}", config.skews), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Total Combinations: "),
                Span::styled(format!("{}", config.total_combinations()), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            ]));
            lines.push(Line::from(""));

            // Top results
            lines.push(Line::from(Span::styled("  ALL RESULTS (sorted by Sharpe)", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(""));

            // Display results as a 2D heatmap-style table
            lines.push(Line::from(Span::styled("  Spread x Skew Matrix (Sharpe ratios):", Style::default().fg(Color::White))));
            lines.push(Line::from(""));

            // Create header row with skew values
            let mut header = vec![Span::raw("          ")]; // padding for spread column
            for skew in &config.skews {
                header.push(Span::styled(format!("  Sk={:.1}  ", skew), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)));
            }
            lines.push(Line::from(header));

            // Build map for quick lookup
            let mut result_map: std::collections::HashMap<(u32, u32), &crate::backtest::sweep::SweepResult> = std::collections::HashMap::new();
            for r in &sweep_results.results {
                let spread_key = (r.spread * 100.0) as u32;
                let skew_key = (r.skew * 100.0) as u32;
                result_map.insert((spread_key, skew_key), r);
            }

            // Create rows for each spread value
            for spread in &config.spreads {
                let mut row = vec![
                    Span::styled(format!("  Sp={:.1} ", spread), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                ];

                for skew in &config.skews {
                    let spread_key = (*spread * 100.0) as u32;
                    let skew_key = (*skew * 100.0) as u32;

                    if let Some(result) = result_map.get(&(spread_key, skew_key)) {
                        let sharpe_color = if result.sharpe > 0.0 { Color::Green } else if result.sharpe > -0.5 { Color::Yellow } else { Color::Red };
                        row.push(Span::styled(format!("  {:+.2}   ", result.sharpe), Style::default().fg(sharpe_color)));
                    } else {
                        row.push(Span::styled("    -    ", Style::default().fg(Color::DarkGray)));
                    }
                }

                lines.push(Line::from(row));
            }

            lines.push(Line::from(""));

            // Top 5 results detail
            lines.push(Line::from(Span::styled("  TOP 5 PARAMETER SETS (by Sharpe)", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(""));

            for (i, result) in sweep_results.top_n(5).iter().enumerate() {
                let sharpe_color = if result.sharpe > 0.0 { Color::Green } else { Color::Red };
                let return_color = if result.total_return > 0.0 { Color::Green } else { Color::Red };

                lines.push(Line::from(vec![
                    Span::styled(format!("  {:>2}. ", i + 1), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                    Span::raw(format!("Spread={:.1} ", result.spread)),
                    Span::raw(format!("Skew={:.1}", result.skew)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("      "),
                    Span::styled(format!("Sharpe={:+.2} ", result.sharpe), Style::default().fg(sharpe_color)),
                    Span::styled(format!("Return={:+.2}% ", result.total_return * 100.0), Style::default().fg(return_color)),
                    Span::raw(format!("DD={:.2}% ", result.max_drawdown * 100.0)),
                    Span::raw(format!("WR={:.1}% ", result.win_rate * 100.0)),
                    Span::raw(format!("Tr={}", result.num_trades)),
                ]));
            }

            lines.push(Line::from(""));

            // Best recommendation
            if let Some(best) = sweep_results.best() {
                lines.push(Line::from(Span::styled("  RECOMMENDED PARAMETERS", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))));
                lines.push(Line::from(vec![
                    Span::raw("  base_spread_bps:       "),
                    Span::styled(format!("{}", best.spread), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  inventory_skew_factor: "),
                    Span::styled(format!("{}", best.skew), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                ]));
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled("  Expected Performance:", Style::default().fg(Color::Yellow))));
                lines.push(Line::from(vec![
                    Span::raw("  Sharpe Ratio: "),
                    Span::styled(format!("{:+.2}", best.sharpe), Style::default().fg(if best.sharpe > 0.0 { Color::Green } else { Color::Red })),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  Total Return: "),
                    Span::styled(format!("{:+.2}%", best.total_return * 100.0), Style::default().fg(if best.total_return > 0.0 { Color::Green } else { Color::Red })),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  Max Drawdown: "),
                    Span::styled(format!("{:.2}%", best.max_drawdown * 100.0), Style::default().fg(Color::Yellow)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  Win Rate:     "),
                    Span::styled(format!("{:.1}%", best.win_rate * 100.0), Style::default().fg(Color::Cyan)),
                ]));
            }
        }
        Err(e) => {
            lines.push(Line::from(Span::styled(
                format!("  Error: {}", e),
                Style::default().fg(Color::Red),
            )));
            lines.push(Line::from(""));
            lines.push(Line::from("  Ensure data exists in ./data/features/"));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  [q] ", Style::default().fg(Color::Red)),
        Span::raw("Back to menu"),
        Span::styled("  [j/k] ", Style::default().fg(Color::Blue)),
        Span::raw("Scroll"),
    ]));
    lines.push(Line::from(""));

    // Handle scrolling
    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" SWEEP ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

fn draw_oos_validation_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    use crate::backtest::oos_validation::{OOSValidator, OOSConfig, OverfitVerdict, ValidationRecommendation};

    let size = f.size();
    let data_dir = std::path::PathBuf::from("./data/features");

    // Static storage for results (persists across redraws)
    use std::sync::OnceLock;
    static OOS_RESULTS: OnceLock<Result<Vec<crate::backtest::oos_validation::ValidationReport>, String>> = OnceLock::new();

    // Default parameters (best from grid search)
    let spreads = vec![1.0, 2.0, 3.0];
    let skews = vec![0.3, 0.5, 0.7];
    let fill_probs = vec![0.10];

    let results = OOS_RESULTS.get_or_init(|| {
        let config = OOSConfig {
            holdout_fraction: 0.20,
            embargo_hours: 1.0,
            data_dir: data_dir.clone(),
            verbose: false,
            ..Default::default()
        };

        let mut validator = OOSValidator::new(config);
        validator.load_data().map_err(|e| e.to_string())?;
        validator.validate_grid(&spreads, &skews, &fill_probs).map_err(|e| e.to_string())
    });

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  OUT-OF-SAMPLE VALIDATION",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Equivalent to: cargo run --bin backtest -- oos-validate",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    match results {
        Ok(reports) => {
            // Configuration section
            lines.push(Line::from(Span::styled("  CONFIGURATION", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(vec![
                Span::raw("  Holdout Fraction: "),
                Span::styled("20%", Style::default().fg(Color::Cyan)),
                Span::raw("   Embargo: "),
                Span::styled("1 hour", Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Spreads:          "),
                Span::styled(format!("{:?}", spreads), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Skews:            "),
                Span::styled(format!("{:?}", skews), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  Fill Probs:       "),
                Span::styled(format!("{:?}", fill_probs), Style::default().fg(Color::Cyan)),
            ]));
            lines.push(Line::from(""));

            // Summary statistics
            let robust_count = reports.iter().filter(|r| matches!(r.overfit_verdict, OverfitVerdict::Robust)).count();
            let mild_count = reports.iter().filter(|r| matches!(r.overfit_verdict, OverfitVerdict::MildOverfit)).count();
            let moderate_count = reports.iter().filter(|r| matches!(r.overfit_verdict, OverfitVerdict::ModerateOverfit)).count();
            let severe_count = reports.iter().filter(|r| matches!(r.overfit_verdict, OverfitVerdict::SevereOverfit)).count();

            lines.push(Line::from(Span::styled("  OVERFITTING SUMMARY", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(vec![
                Span::styled(format!("  Robust: {}", robust_count), Style::default().fg(Color::Green)),
                Span::raw("  "),
                Span::styled(format!("Mild: {}", mild_count), Style::default().fg(Color::Yellow)),
                Span::raw("  "),
                Span::styled(format!("Moderate: {}", moderate_count), Style::default().fg(Color::Red)),
                Span::raw("  "),
                Span::styled(format!("Severe: {}", severe_count), Style::default().fg(Color::Magenta)),
            ]));
            lines.push(Line::from(""));

            // Results table header
            lines.push(Line::from(Span::styled("  VALIDATION RESULTS (sorted by OOS Sharpe)", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "  Spread  Skew   IS Sharpe  OOS Sharpe  Degrad%   Verdict",
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(Span::styled(
                "  ------  ----   ---------  ----------  -------   -------",
                Style::default().fg(Color::DarkGray),
            )));

            // Sort by OOS Sharpe descending
            let mut sorted_reports: Vec<_> = reports.iter().collect();
            sorted_reports.sort_by(|a, b| {
                b.comparison.out_of_sample.sharpe_ratio
                    .partial_cmp(&a.comparison.out_of_sample.sharpe_ratio)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            for report in sorted_reports.iter() {
                let is_sharpe = report.comparison.in_sample.sharpe_ratio;
                let oos_sharpe = report.comparison.out_of_sample.sharpe_ratio;
                let degradation = (1.0 - report.comparison.sharpe_degradation) * 100.0;

                let oos_color = if oos_sharpe > 0.0 { Color::Green } else { Color::Red };
                let verdict_color = match report.overfit_verdict {
                    OverfitVerdict::Robust => Color::Green,
                    OverfitVerdict::MildOverfit => Color::Yellow,
                    OverfitVerdict::ModerateOverfit => Color::Red,
                    OverfitVerdict::SevereOverfit => Color::Magenta,
                    OverfitVerdict::Inconclusive => Color::DarkGray,
                };
                let verdict_str = match report.overfit_verdict {
                    OverfitVerdict::Robust => "ROBUST",
                    OverfitVerdict::MildOverfit => "MILD",
                    OverfitVerdict::ModerateOverfit => "MODERATE",
                    OverfitVerdict::SevereOverfit => "SEVERE",
                    OverfitVerdict::Inconclusive => "INCONC",
                };

                lines.push(Line::from(vec![
                    Span::raw(format!("  {:>5.1}  ", report.params_tested.spread_bps)),
                    Span::raw(format!("{:>4.1}   ", report.params_tested.skew_factor)),
                    Span::styled(format!("{:>+8.3}   ", is_sharpe), Style::default().fg(if is_sharpe > 0.0 { Color::Green } else { Color::Red })),
                    Span::styled(format!("{:>+9.3}   ", oos_sharpe), Style::default().fg(oos_color)),
                    Span::raw(format!("{:>6.0}%   ", degradation)),
                    Span::styled(verdict_str, Style::default().fg(verdict_color)),
                ]));
            }

            lines.push(Line::from(""));

            // Best recommendation
            if let Some(best) = sorted_reports.first() {
                lines.push(Line::from(Span::styled("  BEST OOS PERFORMER", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))));
                lines.push(Line::from(vec![
                    Span::raw("  Parameters: Spread="),
                    Span::styled(format!("{:.1}", best.params_tested.spread_bps), Style::default().fg(Color::Cyan)),
                    Span::raw(" Skew="),
                    Span::styled(format!("{:.1}", best.params_tested.skew_factor), Style::default().fg(Color::Cyan)),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  OOS Sharpe: "),
                    Span::styled(
                        format!("{:+.3}", best.comparison.out_of_sample.sharpe_ratio),
                        Style::default().fg(if best.comparison.out_of_sample.sharpe_ratio > 0.0 { Color::Green } else { Color::Red }),
                    ),
                    Span::raw("   OOS Return: "),
                    Span::styled(
                        format!("{:+.2}%", best.comparison.out_of_sample.total_return * 100.0),
                        Style::default().fg(if best.comparison.out_of_sample.total_return > 0.0 { Color::Green } else { Color::Red }),
                    ),
                ]));

                let rec_color = match best.recommendation {
                    ValidationRecommendation::ReadyForPaperTrading => Color::Green,
                    ValidationRecommendation::NeedsMoreData => Color::Yellow,
                    ValidationRecommendation::SimplifyStrategy => Color::Red,
                    ValidationRecommendation::ReconsiderApproach => Color::Magenta,
                    ValidationRecommendation::StatisticallyInsignificant => Color::DarkGray,
                };
                lines.push(Line::from(vec![
                    Span::raw("  Recommendation: "),
                    Span::styled(format!("{}", best.recommendation), Style::default().fg(rec_color)),
                ]));
            }
        }
        Err(e) => {
            lines.push(Line::from(Span::styled(
                format!("  Error: {}", e),
                Style::default().fg(Color::Red),
            )));
            lines.push(Line::from(""));
            lines.push(Line::from("  Ensure data exists in ./data/features/"));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  [q] ", Style::default().fg(Color::Red)),
        Span::raw("Back to menu"),
        Span::styled("  [j/k] ", Style::default().fg(Color::Blue)),
        Span::raw("Scroll"),
    ]));
    lines.push(Line::from(""));

    // Handle scrolling
    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" OOS VALIDATION ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

fn draw_live(
    f: &mut ratatui::Frame,
    symbol: &str,
    feat: &AveragedFeatures,
    microprice_hist: &VecDeque<f64>,
    pwi50_hist: &VecDeque<f64>,
    entropy_hist: &VecDeque<f64>,
    volatility_hist: &VecDeque<f64>,
) {
    let size = f.size();

    // Layout: title + 6 panels + sparklines
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(1),   // Title
            Constraint::Length(8),   // Order Book
            Constraint::Length(6),   // Trades
            Constraint::Length(4),   // Illiquidity
            Constraint::Length(4),   // Entropy
            Constraint::Length(4),   // Volatility
            Constraint::Length(4),   // Toxicity
            Constraint::Min(4),      // Sparklines
        ])
        .split(size);

    // Title
    let now = chrono::Local::now().format("%H:%M:%S");
    let title = format!(
        " {} | {} | {} samples/sec | [q] menu ",
        symbol.to_uppercase(), now, feat.samples
    );
    let title_para = Paragraph::new(Line::from(Span::styled(
        title,
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    )));
    f.render_widget(title_para, rows[0]);

    // Order Book Panel
    let ob_lines = vec![
        Line::from(vec![
            Span::styled("BID ", Style::default().fg(Color::Green)),
            Span::raw(format!("{:.2}", feat.best_bid)),
            Span::raw("  "),
            Span::styled("ASK ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.2}", feat.best_ask)),
            Span::raw("  "),
            Span::styled("SPREAD ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.2}", feat.spread)),
        ]),
        Line::from(vec![
            Span::styled("MID ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.mid_price)),
            Span::raw("  "),
            Span::styled("MICRO ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.microprice)),
            Span::raw("  "),
            Span::styled("IMB ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:+.2}%", feat.imbalance * 100.0)),
        ]),
        Line::from(vec![
            Span::styled("PWI ", Style::default().fg(Color::Blue)),
            Span::raw(format!(
                "1%={:+.2}% 5%={:+.2}% 25%={:+.2}% 50%={:+.2}%",
                feat.pwi_1 * 100.0, feat.pwi_5 * 100.0, feat.pwi_25 * 100.0, feat.pwi_50 * 100.0
            )),
        ]),
        Line::from(vec![
            Span::styled("SLOPE ", Style::default().fg(Color::Gray)),
            Span::raw(format!("B={:.4} A={:.4}", feat.bid_slope, feat.ask_slope)),
            Span::raw("  "),
            Span::styled("VOL_IMB ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:+.2}%", feat.volume_imbalance_top5 * 100.0)),
        ]),
        Line::from(vec![
            Span::styled("DEPTH ", Style::default().fg(Color::Gray)),
            Span::raw(format!("B={:.2} A={:.2}", feat.bid_depth_ratio, feat.ask_depth_ratio)),
            Span::raw("  "),
            Span::styled("VOL_001 ", Style::default().fg(Color::Gray)),
            Span::raw(format!("B={:.2} A={:.2}", feat.bid_volume_001, feat.ask_volume_001)),
        ]),
    ];
    let ob_para = Paragraph::new(ob_lines).block(
        Block::default().title(" ORDER BOOK ").borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(ob_para, rows[1]);

    // Trades Panel
    let mom_color = if feat.signed_count_momentum > 0.0 { Color::Green } else if feat.signed_count_momentum < 0.0 { Color::Red } else { Color::Gray };
    let trade_lines = vec![
        Line::from(vec![
            Span::styled("LAST ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.last_trade_price)),
            Span::raw("  "),
            Span::styled("VWAP ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.vwap_total)),
            Span::raw("  "),
            Span::styled("SIZE ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", feat.avg_trade_size)),
        ]),
        Line::from(vec![
            Span::styled("VWAP ", Style::default().fg(Color::Blue)),
            Span::raw(format!("10={:.2} 50={:.2} 100={:.2} 1000={:.2}",
                feat.vwap_10, feat.vwap_50, feat.vwap_100, feat.vwap_1000)),
        ]),
        Line::from(vec![
            Span::styled("MOM ", Style::default().fg(Color::Yellow)),
            Span::styled(format!("{:+.0}", feat.signed_count_momentum), Style::default().fg(mom_color)),
            Span::raw("  "),
            Span::styled("RATE ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.1}/s", feat.trade_rate_10s)),
            Span::raw("  "),
            Span::styled("FLOW ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("I={:+.2} P={:.1}", feat.order_flow_imbalance, feat.order_flow_pressure)),
        ]),
    ];
    let trade_para = Paragraph::new(trade_lines).block(
        Block::default().title(" TRADES & FLOW ").borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(trade_para, rows[2]);

    // Illiquidity Panel
    let illiq_lines = vec![
        Line::from(vec![
            Span::styled("ROLL ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.6}", feat.roll_spread)),
            Span::raw("  "),
            Span::styled("AMIHUD ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.2e}", feat.amihuds_lambda)),
            Span::raw("  "),
            Span::styled("KYLE ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", feat.kyles_lambda)),
            Span::raw("  "),
            Span::styled("VPIN ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.2}", feat.vpin)),
        ]),
    ];
    let illiq_para = Paragraph::new(illiq_lines).block(
        Block::default().title(" ILLIQUIDITY ").borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(illiq_para, rows[3]);

    // Entropy Panel
    let entropy_lines = vec![
        Line::from(vec![
            Span::styled("TICK ", Style::default().fg(Color::Magenta)),
            Span::raw(format!(
                "1s={:.3} 5s={:.3} 10s={:.3} 30s={:.3} 1m={:.3} 15m={:.3}",
                feat.tick_entropy_1s, feat.tick_entropy_5s, feat.tick_entropy_10s,
                feat.tick_entropy_30s, feat.tick_entropy_1m, feat.tick_entropy_15m
            )),
        ]),
    ];
    let entropy_para = Paragraph::new(entropy_lines).block(
        Block::default().title(" ENTROPY ").borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(entropy_para, rows[4]);

    // Volatility Panel
    let jump_color = if feat.jump_indicator.abs() > 3.0 { Color::Red } else if feat.jump_indicator.abs() > 2.0 { Color::Yellow } else { Color::Gray };
    let vol_lines = vec![
        Line::from(vec![
            Span::styled("RV_100 ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.6}", feat.realized_volatility_100)),
            Span::raw("  "),
            Span::styled("RV_1K ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.6}", feat.realized_volatility_1000)),
            Span::raw("  "),
            Span::styled("BV ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.6}", feat.bipower_variation_100)),
            Span::raw("  "),
            Span::styled("JUMP ", Style::default().fg(jump_color)),
            Span::raw(format!("{:.2}", feat.jump_indicator)),
            Span::raw("  "),
            Span::styled("VOV ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.6}", feat.vol_of_vol)),
        ]),
    ];
    let vol_para = Paragraph::new(vol_lines).block(
        Block::default().title(" VOLATILITY ").borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(vol_para, rows[5]);

    // Toxicity Panel
    let tox_color = if feat.toxicity_index > 0.5 { Color::Red } else if feat.toxicity_index > 0.3 { Color::Yellow } else { Color::Green };
    let tox_lines = vec![
        Line::from(vec![
            Span::styled("TOXIC ", Style::default().fg(Color::Red)),
            Span::raw(format!("M={:.2}% m={:.2}%", feat.toxic_flow_ratio_micro * 100.0, feat.toxic_flow_ratio_mid * 100.0)),
            Span::raw("  "),
            Span::styled("ADV ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", feat.adverse_selection_micro)),
            Span::raw("  "),
            Span::styled("ASYM ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:+.2}", feat.arrival_asymmetry)),
            Span::raw("  "),
            Span::styled("SIZE ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.2}", feat.size_toxicity_ratio)),
            Span::raw("  "),
            Span::styled("IDX ", Style::default().fg(tox_color)),
            Span::raw(format!("{:.2}", feat.toxicity_index)),
        ]),
    ];
    let tox_para = Paragraph::new(tox_lines).block(
        Block::default().title(" TOXICITY ").borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(tox_para, rows[6]);

    // Sparklines - 4 columns for microprice, PWI50, entropy, volatility
    let spark_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(rows[7]);

    fn normalize_spark(buf: &VecDeque<f64>) -> Vec<u64> {
        if buf.is_empty() { return vec![]; }
        let min = buf.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = buf.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let span = (max - min).max(1e-9);
        buf.iter().map(|v| (((v - min) / span) * 100.0) as u64).collect()
    }

    // Microprice sparkline
    let micro_data = normalize_spark(microprice_hist);
    let micro_spark = Sparkline::default()
        .block(Block::default()
            .title(format!(" MICRO {:.0} ", feat.microprice))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)))
        .style(Style::default().fg(Color::Cyan))
        .data(&micro_data);
    f.render_widget(micro_spark, spark_cols[0]);

    // PWI50 sparkline
    let pwi_data = normalize_spark(pwi50_hist);
    let pwi_color = if feat.pwi_50 > 0.0 { Color::Green } else if feat.pwi_50 < 0.0 { Color::Red } else { Color::Yellow };
    let pwi_spark = Sparkline::default()
        .block(Block::default()
            .title(format!(" PWI {:+.1}% ", feat.pwi_50 * 100.0))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)))
        .style(Style::default().fg(pwi_color))
        .data(&pwi_data);
    f.render_widget(pwi_spark, spark_cols[1]);

    // Entropy sparkline
    let ent_data = normalize_spark(entropy_hist);
    let ent_spark = Sparkline::default()
        .block(Block::default()
            .title(format!(" ENT {:.2} ", feat.tick_entropy_1m))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)))
        .style(Style::default().fg(Color::Magenta))
        .data(&ent_data);
    f.render_widget(ent_spark, spark_cols[2]);

    // Volatility sparkline
    let vol_data = normalize_spark(volatility_hist);
    let vol_spark = Sparkline::default()
        .block(Block::default()
            .title(format!(" VOL {:.4} ", feat.realized_volatility_100))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)))
        .style(Style::default().fg(Color::Yellow))
        .data(&vol_data);
    f.render_widget(vol_spark, spark_cols[3]);
}

fn draw_live_mm(
    f: &mut ratatui::Frame,
    symbol: &str,
    feat: &AveragedFeatures,
    state: &PaperTradingState,
    max_inventory: Decimal,
    session: &ForwardTestSession,
    active_preset: Option<&ParameterPreset>,
) {
    let size = f.size();

    // Layout: title + MM panel + PnL panel + Quotes panel + Market Data (condensed)
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(1),   // Title
            Constraint::Length(8),   // MM State & Regime
            Constraint::Length(6),   // PnL & Position
            Constraint::Length(5),   // Current Quotes
            Constraint::Length(5),   // Simulator Stats
            Constraint::Min(4),      // Market data summary
        ])
        .split(size);

    // Title with session info and preset info
    let now = chrono::Local::now().format("%H:%M:%S");
    let session_info = if session.is_active() {
        let m = session.metrics();
        format!("Quotes: {}", m.quotes_generated)
    } else {
        "inactive".to_string()
    };

    // Show algorithm type in title
    let algo_badge = match state.algorithm_type {
        AlgorithmType::AvellanedaStoikov => "[A-S]",
        AlgorithmType::MLSpreadSkew => "[ML]",
        AlgorithmType::FixedSpread => "[FS]",
    };

    let preset_info = if let Some(preset) = active_preset {
        format!("{} {} ({})", algo_badge, preset.name, preset.created_at_local())
    } else {
        format!("{} Default params", algo_badge)
    };
    let title = format!(
        " {} | {} | {} | {} | [r] reset [q] menu ",
        symbol.to_uppercase(), now, preset_info, session_info
    );
    let title_para = Paragraph::new(Line::from(Span::styled(
        title,
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
    )));
    f.render_widget(title_para, rows[0]);

    // MM State & Regime Panel
    // Get regime from last quotes
    let regime = state.last_quotes.as_ref().map(|q| q.regime).unwrap_or(MarketRegime::MediumEntropy);
    let regime_str = match regime {
        MarketRegime::HighEntropy => ("HIGH ENTROPY", Color::Green),
        MarketRegime::MediumEntropy => ("MEDIUM ENTROPY", Color::Yellow),
        MarketRegime::LowEntropy => ("LOW ENTROPY", Color::Red),
    };

    let inv_color = if state.mm_state.inventory.is_zero() {
        Color::Gray
    } else if state.mm_state.inventory.is_sign_positive() {
        Color::Green
    } else {
        Color::Red
    };

    let mm_lines = vec![
        Line::from(vec![
            Span::styled("REGIME ", Style::default().fg(Color::Yellow)),
            Span::styled(regime_str.0, Style::default().fg(regime_str.1).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled("ENTROPY ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.3}", feat.tick_entropy_5s)),
        ]),
        Line::from(vec![
            Span::styled("FAIR VALUE ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", state.last_quotes.as_ref().map(|q| q.fair_value).unwrap_or_default())),
            Span::raw("  "),
            Span::styled("HALF SPREAD ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.2}", state.last_quotes.as_ref().map(|q| q.half_spread).unwrap_or_default())),
            Span::raw("  "),
            Span::styled("SKEW ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.2}", state.last_quotes.as_ref().map(|q| q.skew).unwrap_or_default())),
        ]),
        Line::from(vec![
            Span::styled("INVENTORY ", Style::default().fg(inv_color)),
            Span::raw(format!("{:+.6}", state.mm_state.inventory)),
            Span::raw("  "),
            Span::styled("AVG ENTRY ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.2}", state.mm_state.avg_entry_price)),
            Span::raw("  "),
            Span::styled("MAX ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.4}", max_inventory)),
        ]),
        Line::from(vec![
            Span::styled("VOLATILITY ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.6}", feat.realized_volatility_100)),
            Span::raw("  "),
            Span::styled("TOXICITY ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.2}", feat.toxicity_index)),
        ]),
    ];

    let mm_para = Paragraph::new(mm_lines).block(
        Block::default()
            .title(" MARKET MAKER STATE ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(mm_para, rows[1]);

    // PnL Panel
    let pnl = &state.mm_state.pnl;
    let realized_color = if pnl.realized_pnl.is_sign_positive() { Color::Green } else { Color::Red };
    let unrealized_color = if pnl.unrealized_pnl.is_sign_positive() { Color::Green } else { Color::Red };
    let total_color = if pnl.total_pnl.is_sign_positive() { Color::Green } else { Color::Red };

    let pnl_lines = vec![
        Line::from(vec![
            Span::styled("REALIZED ", Style::default().fg(realized_color)),
            Span::raw(format!("{:+.4}", pnl.realized_pnl)),
            Span::raw("  "),
            Span::styled("UNREALIZED ", Style::default().fg(unrealized_color)),
            Span::raw(format!("{:+.4}", pnl.unrealized_pnl)),
            Span::raw("  "),
            Span::styled("TOTAL ", Style::default().fg(total_color).add_modifier(Modifier::BOLD)),
            Span::raw(format!("{:+.4}", pnl.total_pnl)),
        ]),
        Line::from(vec![
            Span::styled("FEES PAID ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.6}", pnl.fees_paid)),
            Span::raw("  "),
            Span::styled("TRADES ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", pnl.num_trades)),
            Span::raw("  "),
            Span::styled("VOLUME ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", pnl.total_volume)),
        ]),
    ];

    let pnl_para = Paragraph::new(pnl_lines).block(
        Block::default()
            .title(" P&L ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Green)),
    );
    f.render_widget(pnl_para, rows[2]);

    // Quotes Panel
    let quotes = state.last_quotes.as_ref();
    let bid_price = quotes.and_then(|q| q.bid.as_ref()).map(|b| format!("{:.2}", b.price)).unwrap_or_else(|| "---".to_string());
    let bid_size = quotes.and_then(|q| q.bid.as_ref()).map(|b| format!("{:.4}", b.size)).unwrap_or_else(|| "---".to_string());
    let ask_price = quotes.and_then(|q| q.ask.as_ref()).map(|a| format!("{:.2}", a.price)).unwrap_or_else(|| "---".to_string());
    let ask_size = quotes.and_then(|q| q.ask.as_ref()).map(|a| format!("{:.4}", a.size)).unwrap_or_else(|| "---".to_string());

    let quote_lines = vec![
        Line::from(vec![
            Span::styled("  BID ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} x {}", bid_price, bid_size)),
            Span::raw("      "),
            Span::styled("ASK ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} x {}", ask_price, ask_size)),
        ]),
        Line::from(vec![
            Span::styled("  MID ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.mid_price)),
            Span::raw("  "),
            Span::styled("MICRO ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.microprice)),
            Span::raw("  "),
            Span::styled("SPREAD ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.2}", feat.spread)),
        ]),
    ];

    let quote_para = Paragraph::new(quote_lines).block(
        Block::default()
            .title(" QUOTES ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );
    f.render_widget(quote_para, rows[3]);

    // Simulator Stats Panel
    let sim = &state.sim_stats;
    let fill_rate = sim.fill_rate() * 100.0;

    let sim_lines = vec![
        Line::from(vec![
            Span::styled("TRADES SEEN ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", sim.trades_seen)),
            Span::raw("  "),
            Span::styled("BID FILLS ", Style::default().fg(Color::Green)),
            Span::raw(format!("{}", sim.bid_fills)),
            Span::raw("  "),
            Span::styled("ASK FILLS ", Style::default().fg(Color::Red)),
            Span::raw(format!("{}", sim.ask_fills)),
            Span::raw("  "),
            Span::styled("FILL RATE ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.1}%", fill_rate)),
        ]),
        Line::from(vec![
            Span::styled("BID MISSES ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", sim.bid_misses)),
            Span::raw("  "),
            Span::styled("ASK MISSES ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", sim.ask_misses)),
            Span::raw("  "),
            Span::styled("FILL VOL ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.4}", sim.total_fill_volume)),
        ]),
    ];

    let sim_para = Paragraph::new(sim_lines).block(
        Block::default()
            .title(" SIMULATOR ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(sim_para, rows[4]);

    // Condensed Market Data
    let mkt_lines = vec![
        Line::from(vec![
            Span::styled("IMB ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:+.1}%", feat.imbalance * 100.0)),
            Span::raw("  "),
            Span::styled("PWI ", Style::default().fg(Color::Blue)),
            Span::raw(format!("{:+.1}%", feat.pwi_50 * 100.0)),
            Span::raw("  "),
            Span::styled("FLOW ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:+.2}", feat.order_flow_imbalance)),
            Span::raw("  "),
            Span::styled("ENT ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.2} / {:.2} / {:.2}",
                feat.tick_entropy_1s, feat.tick_entropy_5s, feat.tick_entropy_10s)),
            Span::raw("  "),
            Span::styled("VPIN ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.2}", feat.vpin)),
        ]),
    ];

    let mkt_para = Paragraph::new(mkt_lines).block(
        Block::default()
            .title(" MARKET ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(mkt_para, rows[5]);
}

/// Draw live MM screen with risk management status
fn draw_live_mm_with_risk(
    f: &mut ratatui::Frame,
    symbol: &str,
    feat: &AveragedFeatures,
    state: &RiskManagedState,
    max_inventory: Decimal,
    session: &ForwardTestSession,
    active_preset: Option<&ParameterPreset>,
) {
    let size = f.size();
    let trading_state = &state.trading_state;

    // Layout: title + Risk Status + MM panel + PnL panel + Quotes panel + Market Data
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(1),   // Title
            Constraint::Length(4),   // Risk Status (new)
            Constraint::Length(8),   // MM State & Regime
            Constraint::Length(6),   // PnL & Position
            Constraint::Length(5),   // Current Quotes
            Constraint::Length(5),   // Simulator Stats
            Constraint::Min(3),      // Market data summary
        ])
        .split(size);

    // Title with session info and preset info
    let now = chrono::Local::now().format("%H:%M:%S");
    let session_info = if session.is_active() {
        let m = session.metrics();
        format!("Quotes: {}", m.quotes_generated)
    } else {
        "inactive".to_string()
    };

    // Show algorithm type in title
    let algo_badge = match trading_state.algorithm_type {
        AlgorithmType::AvellanedaStoikov => "[A-S]",
        AlgorithmType::MLSpreadSkew => "[ML]",
        AlgorithmType::FixedSpread => "[FS]",
    };

    let preset_info = if let Some(preset) = active_preset {
        format!("{} {} ({})", algo_badge, preset.name, preset.created_at_local())
    } else {
        algo_badge.to_string()
    };

    let title_para = Paragraph::new(Line::from(vec![
        Span::styled(format!(" {} ", symbol), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(preset_info, Style::default().fg(Color::Yellow)),
        Span::raw("  "),
        Span::styled(format!("{} ", now), Style::default().fg(Color::DarkGray)),
        Span::styled(session_info, Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        Span::styled("[r] Reset  [h] Halt/Resume  [q] Exit", Style::default().fg(Color::DarkGray)),
    ]));
    f.render_widget(title_para, rows[0]);

    // Risk Status Panel
    let risk_color = risk_action_color(&state.risk_action);
    let risk_status = format_risk_action(&state.risk_action);
    let risk_reason = match &state.risk_action {
        RiskAction::Allow => "Trading normally".to_string(),
        RiskAction::ReduceOnly => "Reduce-only mode active".to_string(),
        RiskAction::Halt { reason } => reason.to_string(),
        RiskAction::Emergency { reason } => format!("EMERGENCY: {}", reason),
    };

    let risk_lines = vec![
        Line::from(vec![
            Span::styled("RISK STATUS ", Style::default().fg(Color::White)),
            Span::styled(risk_status, Style::default().fg(risk_color).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(&risk_reason, Style::default().fg(risk_color)),
        ]),
        Line::from(vec![
            Span::styled("BLOCKED ", Style::default().fg(Color::Gray)),
            Span::styled("Quotes: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{}", state.quotes_blocked)),
            Span::raw("  "),
            Span::styled("Fills: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{}", state.fills_blocked)),
            Span::raw("  "),
            Span::styled("Halts: ", Style::default().fg(Color::Red)),
            Span::raw(format!("{}", state.risk_stats.halt_count)),
            Span::raw("  "),
            Span::styled("ReduceOnly: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{}", state.risk_stats.reduce_only_count)),
        ]),
    ];

    let risk_border_color = match state.risk_action {
        RiskAction::Allow => Color::Green,
        RiskAction::ReduceOnly => Color::Yellow,
        RiskAction::Halt { .. } => Color::Red,
        RiskAction::Emergency { .. } => Color::Magenta,
    };

    let risk_para = Paragraph::new(risk_lines).block(
        Block::default()
            .title(" RISK MANAGER ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(risk_border_color)),
    );
    f.render_widget(risk_para, rows[1]);

    // MM State & Regime Panel
    let regime = trading_state.last_quotes.as_ref().map(|q| q.regime).unwrap_or(MarketRegime::MediumEntropy);
    let regime_str = match regime {
        MarketRegime::HighEntropy => ("HIGH ENTROPY", Color::Green),
        MarketRegime::MediumEntropy => ("MEDIUM ENTROPY", Color::Yellow),
        MarketRegime::LowEntropy => ("LOW ENTROPY", Color::Red),
    };

    let inv_color = if trading_state.mm_state.inventory.is_zero() {
        Color::Gray
    } else if trading_state.mm_state.inventory.is_sign_positive() {
        Color::Green
    } else {
        Color::Red
    };

    let mm_lines = vec![
        Line::from(vec![
            Span::styled("REGIME ", Style::default().fg(Color::Yellow)),
            Span::styled(regime_str.0, Style::default().fg(regime_str.1).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled("ENTROPY ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.3}", feat.tick_entropy_5s)),
        ]),
        Line::from(vec![
            Span::styled("FAIR VALUE ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", trading_state.last_quotes.as_ref().map(|q| q.fair_value).unwrap_or_default())),
            Span::raw("  "),
            Span::styled("HALF SPREAD ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.2}", trading_state.last_quotes.as_ref().map(|q| q.half_spread).unwrap_or_default())),
            Span::raw("  "),
            Span::styled("SKEW ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.2}", trading_state.last_quotes.as_ref().map(|q| q.skew).unwrap_or_default())),
        ]),
        Line::from(vec![
            Span::styled("INVENTORY ", Style::default().fg(inv_color)),
            Span::raw(format!("{:+.6}", trading_state.mm_state.inventory)),
            Span::raw("  "),
            Span::styled("AVG ENTRY ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.2}", trading_state.mm_state.avg_entry_price)),
            Span::raw("  "),
            Span::styled("MAX ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.4}", max_inventory)),
        ]),
        Line::from(vec![
            Span::styled("VOLATILITY ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.6}", feat.realized_volatility_100)),
            Span::raw("  "),
            Span::styled("TOXICITY ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.2}", feat.toxicity_index)),
        ]),
    ];

    let mm_para = Paragraph::new(mm_lines).block(
        Block::default()
            .title(" MARKET MAKER STATE ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(mm_para, rows[2]);

    // PnL Panel
    let pnl = &trading_state.mm_state.pnl;
    let realized_color = if pnl.realized_pnl.is_sign_positive() { Color::Green } else { Color::Red };
    let unrealized_color = if pnl.unrealized_pnl.is_sign_positive() { Color::Green } else { Color::Red };
    let total_color = if pnl.total_pnl.is_sign_positive() { Color::Green } else { Color::Red };

    let pnl_lines = vec![
        Line::from(vec![
            Span::styled("REALIZED ", Style::default().fg(realized_color)),
            Span::raw(format!("{:+.4}", pnl.realized_pnl)),
            Span::raw("  "),
            Span::styled("UNREALIZED ", Style::default().fg(unrealized_color)),
            Span::raw(format!("{:+.4}", pnl.unrealized_pnl)),
            Span::raw("  "),
            Span::styled("TOTAL ", Style::default().fg(total_color).add_modifier(Modifier::BOLD)),
            Span::raw(format!("{:+.4}", pnl.total_pnl)),
        ]),
        Line::from(vec![
            Span::styled("FEES PAID ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.6}", pnl.fees_paid)),
            Span::raw("  "),
            Span::styled("TRADES ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", pnl.num_trades)),
            Span::raw("  "),
            Span::styled("VOLUME ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", pnl.total_volume)),
        ]),
    ];

    let pnl_para = Paragraph::new(pnl_lines).block(
        Block::default()
            .title(" P&L ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Green)),
    );
    f.render_widget(pnl_para, rows[3]);

    // Quotes Panel
    let quotes = trading_state.last_quotes.as_ref();
    let bid_price = quotes.and_then(|q| q.bid.as_ref()).map(|b| format!("{:.2}", b.price)).unwrap_or_else(|| "---".to_string());
    let bid_size = quotes.and_then(|q| q.bid.as_ref()).map(|b| format!("{:.4}", b.size)).unwrap_or_else(|| "---".to_string());
    let ask_price = quotes.and_then(|q| q.ask.as_ref()).map(|a| format!("{:.2}", a.price)).unwrap_or_else(|| "---".to_string());
    let ask_size = quotes.and_then(|q| q.ask.as_ref()).map(|a| format!("{:.4}", a.size)).unwrap_or_else(|| "---".to_string());

    let quote_lines = vec![
        Line::from(vec![
            Span::styled("  BID ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} x {}", bid_price, bid_size)),
            Span::raw("      "),
            Span::styled("ASK ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::raw(format!("{} x {}", ask_price, ask_size)),
        ]),
        Line::from(vec![
            Span::styled("  MID ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.mid_price)),
            Span::raw("  "),
            Span::styled("MICRO ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.2}", feat.microprice)),
            Span::raw("  "),
            Span::styled("SPREAD ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.2}", feat.spread)),
        ]),
    ];

    let quote_para = Paragraph::new(quote_lines).block(
        Block::default()
            .title(" QUOTES ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );
    f.render_widget(quote_para, rows[4]);

    // Simulator Stats Panel
    let sim = &trading_state.sim_stats;
    let fill_rate = sim.fill_rate() * 100.0;

    let sim_lines = vec![
        Line::from(vec![
            Span::styled("TRADES SEEN ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", sim.trades_seen)),
            Span::raw("  "),
            Span::styled("BID FILLS ", Style::default().fg(Color::Green)),
            Span::raw(format!("{}", sim.bid_fills)),
            Span::raw("  "),
            Span::styled("ASK FILLS ", Style::default().fg(Color::Red)),
            Span::raw(format!("{}", sim.ask_fills)),
            Span::raw("  "),
            Span::styled("FILL RATE ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.1}%", fill_rate)),
        ]),
        Line::from(vec![
            Span::styled("BID MISSES ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", sim.bid_misses)),
            Span::raw("  "),
            Span::styled("ASK MISSES ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", sim.ask_misses)),
            Span::raw("  "),
            Span::styled("FILL VOL ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.4}", sim.total_fill_volume)),
        ]),
    ];

    let sim_para = Paragraph::new(sim_lines).block(
        Block::default()
            .title(" SIMULATOR ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(sim_para, rows[5]);

    // Condensed Market Data
    let mkt_lines = vec![
        Line::from(vec![
            Span::styled("IMB ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:+.1}%", feat.imbalance * 100.0)),
            Span::raw("  "),
            Span::styled("PWI ", Style::default().fg(Color::Blue)),
            Span::raw(format!("{:+.1}%", feat.pwi_50 * 100.0)),
            Span::raw("  "),
            Span::styled("FLOW ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:+.2}", feat.order_flow_imbalance)),
            Span::raw("  "),
            Span::styled("ENT ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.2} / {:.2} / {:.2}",
                feat.tick_entropy_1s, feat.tick_entropy_5s, feat.tick_entropy_10s)),
            Span::raw("  "),
            Span::styled("VPIN ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.2}", feat.vpin)),
        ]),
    ];

    let mkt_para = Paragraph::new(mkt_lines).block(
        Block::default()
            .title(" MARKET ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(mkt_para, rows[6]);
}

fn draw_features(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    let size = f.size();
    let descriptions = get_feature_descriptions();

    let mut lines: Vec<Line> = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  MARKET MICROSTRUCTURE FEATURES - Academic Descriptions",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];

    for (name, formula, desc) in descriptions {
        if formula.is_empty() && desc.is_empty() {
            // Section header
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("  === {} ===", name),
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(""));
        } else {
            lines.push(Line::from(vec![
                Span::styled(format!("  {:<22}", name), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::styled(format!(" {:<24}", formula), Style::default().fg(Color::Blue)),
            ]));
            // Wrap long descriptions
            let max_width = size.width.saturating_sub(6) as usize;
            let wrapped = textwrap_simple(desc, max_width);
            for line in wrapped {
                lines.push(Line::from(Span::styled(format!("    {}", line), Style::default().fg(Color::Gray))));
            }
            lines.push(Line::from(""));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  [↑/↓] Scroll  [PgUp/PgDn] Fast scroll  [q] Back to menu",
        Style::default().fg(Color::DarkGray),
    )));

    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" FEATURE DESCRIPTIONS ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

fn textwrap_simple(text: &str, max_width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current_line = String::new();

    for word in text.split_whitespace() {
        if current_line.is_empty() {
            current_line = word.to_string();
        } else if current_line.len() + 1 + word.len() <= max_width {
            current_line.push(' ');
            current_line.push_str(word);
        } else {
            lines.push(current_line);
            current_line = word.to_string();
        }
    }

    if !current_line.is_empty() {
        lines.push(current_line);
    }

    lines
}

/// Run backtest and display results
fn draw_backtest_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    use crate::backtest::{BacktestEngine, BacktestConfig, ReplayConfig};
    use std::path::PathBuf;

    let size = f.size();

    // Run backtest (cached result would be better, but this is simpler for now)
    let data_dir = PathBuf::from("./data/features");
    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  BACKTEST RESULTS",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];

    if !data_dir.exists() {
        lines.push(Line::from(Span::styled(
            "  No data found in ./data/features",
            Style::default().fg(Color::Red),
        )));
        lines.push(Line::from("  Run option [0] or [1] first to collect data."));
    } else {
        lines.push(Line::from(Span::styled(
            "  Running backtest... (this may take a moment)",
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::from(""));

        let config = BacktestConfig {
            replay: ReplayConfig {
                data_dir,
                ..Default::default()
            },
            verbose: false,
            ..Default::default()
        };

        let mut engine = BacktestEngine::new(config);
        match engine.load_data() {
            Ok(num_events) => {
                lines.push(Line::from(format!("  Loaded {} events", num_events)));

                match engine.run() {
                    Ok(results) => {
                        lines.push(Line::from(""));
                        lines.push(Line::from(Span::styled("  PERFORMANCE", Style::default().fg(Color::Yellow))));
                        lines.push(Line::from(format!("  Total Return:    {:+.2}%", results.metrics.total_return * 100.0)));
                        lines.push(Line::from(format!("  Sharpe Ratio:    {:+.2}", results.metrics.sharpe_ratio)));
                        lines.push(Line::from(format!("  Max Drawdown:    {:.2}%", results.metrics.max_drawdown * 100.0)));
                        lines.push(Line::from(format!("  Win Rate:        {:.1}%", results.metrics.win_rate * 100.0)));
                        lines.push(Line::from(""));
                        lines.push(Line::from(Span::styled("  TRADING", Style::default().fg(Color::Yellow))));
                        lines.push(Line::from(format!("  Trades:          {}", results.metrics.num_trades)));
                        lines.push(Line::from(format!("  Profit Factor:   {:.2}", results.metrics.profit_factor)));
                        lines.push(Line::from(""));
                        lines.push(Line::from(Span::styled("  FILL SIMULATION", Style::default().fg(Color::Yellow))));
                        lines.push(Line::from(format!("  Bid Fill Rate:   {:.1}%", results.fill_stats.bid_fill_rate * 100.0)));
                        lines.push(Line::from(format!("  Ask Fill Rate:   {:.1}%", results.fill_stats.ask_fill_rate * 100.0)));
                        lines.push(Line::from(format!("  Partial Fills:   {}", results.fill_stats.partial_fills)));
                    }
                    Err(e) => {
                        lines.push(Line::from(Span::styled(
                            format!("  Error: {}", e),
                            Style::default().fg(Color::Red),
                        )));
                    }
                }
            }
            Err(e) => {
                lines.push(Line::from(Span::styled(
                    format!("  Error loading data: {}", e),
                    Style::default().fg(Color::Red),
                )));
            }
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  [q] Back to menu",
        Style::default().fg(Color::DarkGray),
    )));

    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" BACKTEST ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

/// Run walk-forward validation and display results
fn draw_walkforward_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    let size = f.size();
    let data_dir = std::path::PathBuf::from("./data/features");

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  WALK-FORWARD VALIDATION",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Time-series cross-validation to detect overfitting",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    if !data_dir.exists() {
        lines.push(Line::from(Span::styled(
            "  No data found. Collect data first using [0] or [1].",
            Style::default().fg(Color::Red),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            "  Walk-forward validation takes ~1-5 minutes depending on data size.",
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from("  For faster results, use the CLI:"));
        lines.push(Line::from(Span::styled(
            "    ./target/release/backtest --data ./data/features walk-forward",
            Style::default().fg(Color::Green),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from("  Options:"));
        lines.push(Line::from("    --folds N        Number of train/test splits (default: 5)"));
        lines.push(Line::from("    --test-hours H   Hours per test fold (default: 24)"));
        lines.push(Line::from("    --rolling        Use rolling (vs expanding) window"));
        lines.push(Line::from("    -o FILE.json     Save results to JSON file"));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  [q] Back to menu",
        Style::default().fg(Color::DarkGray),
    )));

    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" WALK-FORWARD ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

/// Run data quality check and display results
fn draw_dataquality_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    use crate::backtest::DataValidator;
    let size = f.size();
    let data_dir = std::path::PathBuf::from("./data/features");

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  DATA QUALITY REPORT",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];

    if !data_dir.exists() {
        lines.push(Line::from(Span::styled(
            "  No data found in ./data/features",
            Style::default().fg(Color::Red),
        )));
    } else {
        let validator = DataValidator::new();
        match validator.validate_directory(&data_dir) {
            Ok(report) => {
                let score_color = if report.quality_score > 0.8 {
                    Color::Green
                } else if report.quality_score > 0.5 {
                    Color::Yellow
                } else {
                    Color::Red
                };

                lines.push(Line::from(vec![
                    Span::styled("  Quality Score: ", Style::default()),
                    Span::styled(
                        format!("{:.0}%", report.quality_score * 100.0),
                        Style::default().fg(score_color).add_modifier(Modifier::BOLD),
                    ),
                ]));
                lines.push(Line::from(""));
                lines.push(Line::from(format!("  Total Events:   {}", report.total_events)));
                lines.push(Line::from(format!("  Valid Events:   {} ({:.1}%)",
                    report.valid_events,
                    report.valid_events as f64 / report.total_events.max(1) as f64 * 100.0)));
                lines.push(Line::from(format!("  Invalid Events: {}", report.invalid_events)));
                lines.push(Line::from(""));

                if !report.price_anomalies.is_empty() {
                    lines.push(Line::from(Span::styled(
                        format!("  Price Anomalies: {}", report.price_anomalies.len()),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                if !report.data_gaps.is_empty() {
                    let total_gap_hours: f64 = report.data_gaps.iter().map(|g| g.duration_hours).sum();
                    lines.push(Line::from(Span::styled(
                        format!("  Data Gaps: {} ({:.1} hours total)", report.data_gaps.len(), total_gap_hours),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                if !report.timestamp_issues.is_empty() {
                    lines.push(Line::from(Span::styled(
                        format!("  Timestamp Issues: {}", report.timestamp_issues.len()),
                        Style::default().fg(Color::Yellow),
                    )));
                }

                lines.push(Line::from(""));
                if !report.recommendations.is_empty() {
                    lines.push(Line::from(Span::styled("  RECOMMENDATIONS:", Style::default().fg(Color::Yellow))));
                    for rec in &report.recommendations {
                        lines.push(Line::from(format!("    - {}", rec)));
                    }
                }
            }
            Err(e) => {
                lines.push(Line::from(Span::styled(
                    format!("  Error: {}", e),
                    Style::default().fg(Color::Red),
                )));
            }
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  [q] Back to menu",
        Style::default().fg(Color::DarkGray),
    )));

    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" DATA QUALITY ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        );
    f.render_widget(para, size);
}

/// Draw preset selection screen
fn draw_preset_select(f: &mut ratatui::Frame, preset_store: &PresetStore, selected_idx: usize) {
    let size = f.size();

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  SELECT PARAMETER PRESET FOR PAPER TRADING",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Choose an optimized configuration to validate with live data",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    if preset_store.presets.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No presets available.",
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from("  Run grid-search or Bayesian optimization to create presets:"));
        lines.push(Line::from(Span::styled(
            "    cargo run --release --bin backtest -- grid-search --test-gate",
            Style::default().fg(Color::Green),
        )));
        lines.push(Line::from(Span::styled(
            "    python3 scripts/optimize.py --trials 50",
            Style::default().fg(Color::Green),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            "  AVAILABLE PRESETS:",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(""));

        for (i, preset) in preset_store.presets.iter().enumerate() {
            let is_selected = i == selected_idx;
            let prefix = if is_selected { ">> " } else { "   " };
            let style = if is_selected {
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };

            // Algorithm type badge
            let (algo_badge, algo_color) = match preset.algorithm_type {
                AlgorithmType::AvellanedaStoikov => ("[A-S]", Color::Blue),
                AlgorithmType::MLSpreadSkew => ("[ML]", Color::Magenta),
                AlgorithmType::FixedSpread => ("[FS]", Color::Green),
            };

            // Main preset line with algorithm badge
            lines.push(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(format!("[{}] ", i + 1), Style::default().fg(Color::Green)),
                Span::styled(format!("{} ", algo_badge), Style::default().fg(algo_color).add_modifier(Modifier::BOLD)),
                Span::styled(&preset.name, style),
            ]));

            // Details line
            let details = format!(
                "       Developed: {} via {}",
                preset.created_at_local(),
                preset.optimization_method
            );
            lines.push(Line::from(Span::styled(
                details,
                if is_selected { Style::default().fg(Color::Cyan) } else { Style::default().fg(Color::DarkGray) },
            )));

            // Parameters line - different for ML vs A-S vs FS
            let params = match preset.algorithm_type {
                AlgorithmType::AvellanedaStoikov => format!(
                    "       Spread: {:.1}bps | Skew: {:.2} | Entropy: {:.2} | Fill Prob: {:.0}%",
                    preset.spread_bps,
                    preset.skew,
                    preset.high_entropy_threshold,
                    preset.fill_prob_assumption * 100.0
                ),
                AlgorithmType::MLSpreadSkew => {
                    let model_version = preset.ml_weights.as_ref()
                        .map(|w| w.version.as_str())
                        .unwrap_or("default");
                    format!(
                        "       Model: {} | Fill Prob: {:.0}% | Dynamic spread/skew",
                        model_version,
                        preset.fill_prob_assumption * 100.0
                    )
                }
                AlgorithmType::FixedSpread => format!(
                    "       Spread: {:.1}bps | Skew: {:.2} | Fill Prob: {:.0}% | Baseline",
                    preset.spread_bps,
                    preset.skew,
                    preset.fill_prob_assumption * 100.0
                ),
            };
            lines.push(Line::from(Span::styled(
                params,
                if is_selected { Style::default().fg(Color::Yellow) } else { Style::default().fg(Color::DarkGray) },
            )));

            // Expected performance line
            let expected = format!(
                "       Expected: {:+.1}% return | {:.1}% win rate | {} trades",
                preset.expected_return * 100.0,
                preset.expected_win_rate * 100.0,
                preset.expected_trades
            );
            lines.push(Line::from(Span::styled(
                expected,
                if is_selected { Style::default().fg(Color::Green) } else { Style::default().fg(Color::DarkGray) },
            )));

            // Data range
            if !preset.data_range.is_empty() {
                let data_info = format!("       Data: {}", preset.data_range);
                lines.push(Line::from(Span::styled(
                    data_info,
                    Style::default().fg(Color::DarkGray),
                )));
            }

            lines.push(Line::from(""));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  [up/down] Navigate  [Enter] Select  [q] Back to menu",
        Style::default().fg(Color::DarkGray),
    )));

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" PRESET SELECTION ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(para, size);
}

/// Draw campaign simulation screen (informational - runs via CLI)
fn draw_campaign_screen(f: &mut ratatui::Frame, scroll_offset: &mut u16) {
    let size = f.size();
    let data_dir = std::path::PathBuf::from("./data/features");

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  4-WEEK VALIDATION CAMPAIGN SIMULATION",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Simulate a complete validation campaign using historical data",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    // Check data availability
    let (file_count, total_days) = if data_dir.exists() {
        let files: Vec<_> = std::fs::read_dir(&data_dir)
            .map(|rd| rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
                .collect())
            .unwrap_or_default();
        let days = files.len() as f64 / 2.0; // Rough estimate: ~2 files per day
        (files.len(), days as usize)
    } else {
        (0, 0)
    };

    if file_count == 0 {
        lines.push(Line::from(Span::styled(
            "  No data found in ./data/features",
            Style::default().fg(Color::Red),
        )));
        lines.push(Line::from("  Run option [0] or [1] first to collect data."));
    } else {
        lines.push(Line::from(vec![
            Span::styled("  Data Available: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                format!("{} files (~{} days)", file_count, total_days),
                Style::default().fg(Color::Green),
            ),
        ]));

        let enough_data = total_days >= 28;
        if !enough_data {
            lines.push(Line::from(Span::styled(
                format!("  Warning: 4-week campaign needs ~28 days of data (have ~{})", total_days),
                Style::default().fg(Color::Yellow),
            )));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("  ABOUT CAMPAIGN SIMULATION", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
        lines.push(Line::from(""));
        lines.push(Line::from("  The simulate-campaign command replays historical data as if running"));
        lines.push(Line::from("  a real 4-week paper trading validation campaign:"));
        lines.push(Line::from(""));
        lines.push(Line::from("    - Simulates daily 8-hour trading sessions"));
        lines.push(Line::from("    - Applies weekly validation gates (Sharpe, PSR, drawdown)"));
        lines.push(Line::from("    - Tracks fill rate calibration (expected vs actual)"));
        lines.push(Line::from("    - Generates comprehensive validation report"));
        lines.push(Line::from("    - Produces GoLive/Recalibrate/Reject verdict"));
        lines.push(Line::from(""));

        lines.push(Line::from(Span::styled("  HOW TO RUN", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
        lines.push(Line::from(""));
        lines.push(Line::from("  Run from terminal (this is a long-running process):"));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "    cargo run --release --bin backtest -- simulate-campaign",
            Style::default().fg(Color::Green),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("  OPTIONS", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
        lines.push(Line::from(""));
        lines.push(Line::from("    --weeks <N>           Number of weeks (default: 4)"));
        lines.push(Line::from("    --session-hours <H>   Hours per session (default: 8.0)"));
        lines.push(Line::from("    --spread <BPS>        Spread in basis points (default: 1.0)"));
        lines.push(Line::from("    --skew <S>            Inventory skew factor (default: 0.3)"));
        lines.push(Line::from("    --min-sharpe <S>      Weekly Sharpe gate (default: -0.5)"));
        lines.push(Line::from("    --min-psr <P>         Weekly PSR gate (default: 0.3)"));
        lines.push(Line::from("    --max-drawdown <D>    Max drawdown gate (default: 0.05)"));
        lines.push(Line::from("    --fill-prob <P>       Fill probability (default: 0.10)"));
        lines.push(Line::from("    -o, --output <FILE>   Save report to JSON file"));
        lines.push(Line::from("    -v, --verbose         Show detailed progress"));
        lines.push(Line::from(""));

        lines.push(Line::from(Span::styled("  EXAMPLE", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "    cargo run --release --bin backtest -- simulate-campaign \\",
            Style::default().fg(Color::Green),
        )));
        lines.push(Line::from(Span::styled(
            "      --weeks 4 --spread 1.0 --skew 0.3 -v -o campaign_report.json",
            Style::default().fg(Color::Green),
        )));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  [q] Back to menu  [up/down] Scroll",
        Style::default().fg(Color::DarkGray),
    )));

    let max_scroll = lines.len().saturating_sub(size.height as usize) as u16;
    *scroll_offset = (*scroll_offset).min(max_scroll);

    let para = Paragraph::new(lines)
        .scroll((*scroll_offset, 0))
        .block(
            Block::default()
                .title(" CAMPAIGN SIMULATION ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Magenta)),
        );
    f.render_widget(para, size);
}
