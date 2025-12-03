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
    layout::{Constraint, Direction, Layout, Rect},
    widgets::{Block, Borders, Paragraph, Sparkline, Scrollbar, ScrollbarOrientation, ScrollbarState},
    style::{Style, Color, Modifier},
    text::{Span, Line},
};

use crate::feature_fusion::FeaturesSnapshot;
use crate::market_maker::{MarketMakerEngine, MMConfig, MarketRegime};
use crate::mm_simulator::{PaperTradingEngine, SimulatorConfig};
use ingestor::forward_testing::{ForwardTestSession, ForwardTestConfig};

type Term = Terminal<CrosstermBackend<io::Stdout>>;

const MAX_HISTORY: usize = 60; // 60 seconds of history at 1Hz
const UPDATE_INTERVAL_MS: u64 = 1000; // 1Hz update rate

/// Application mode
#[derive(Clone, Copy, PartialEq)]
enum AppMode {
    Menu,
    Live,
    LiveMM,  // Live with Market Maker
    Features,
    Backtest,       // Running backtest
    WalkForward,    // Walk-forward validation
    DataQuality,    // Data quality check
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
    let mut mode = AppMode::Menu;
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

    // Market maker state
    let mm_config = MMConfig::default();
    let sim_config = SimulatorConfig::default();
    let mut paper_trading = PaperTradingEngine::new(
        MarketMakerEngine::new(mm_config),
        sim_config,
    );

    // Forward testing session for trade logging
    let mut forward_session = ForwardTestSession::new(ForwardTestConfig::default());

    loop {
        // Handle input
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match mode {
                    AppMode::Menu => match key.code {
                        KeyCode::Char('0') => mode = AppMode::Live,
                        KeyCode::Char('1') => {
                            mode = AppMode::LiveMM;
                            // Start forward testing session
                            forward_session = ForwardTestSession::new(ForwardTestConfig::default());
                            forward_session.start();
                        }
                        KeyCode::Char('2') => {
                            mode = AppMode::Features;
                            scroll_offset = 0;
                        }
                        KeyCode::Char('3') => mode = AppMode::Backtest,
                        KeyCode::Char('4') => mode = AppMode::WalkForward,
                        KeyCode::Char('5') => mode = AppMode::DataQuality,
                        KeyCode::Char('p') => {
                            settings.persist_features = !settings.persist_features;
                        }
                        KeyCode::Char('s') => {
                            // Cycle through storage options: 1, 5, 10, 50, 100, unlimited
                            settings.max_storage_gb = match settings.max_storage_gb as i32 {
                                0 => 1.0,
                                1 => 5.0,
                                5 => 10.0,
                                10 => 50.0,
                                50 => 100.0,
                                _ => 0.0, // unlimited
                            };
                        }
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(settings),
                        _ => {}
                    },
                    AppMode::Live => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::Menu,
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
                            mode = AppMode::Menu;
                        }
                        KeyCode::Char('r') => {
                            // Reset MM state and start new session
                            paper_trading.reset();
                            forward_session = ForwardTestSession::new(ForwardTestConfig::default());
                            forward_session.start();
                        }
                        _ => {}
                    },
                    AppMode::Features => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::Menu,
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
                    AppMode::Backtest | AppMode::WalkForward | AppMode::DataQuality => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::Menu,
                        KeyCode::Up | KeyCode::Char('k') => {
                            scroll_offset = scroll_offset.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            scroll_offset = scroll_offset.saturating_add(1);
                        }
                        _ => {}
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
                if mode == AppMode::LiveMM {
                    if let Some(ref snap) = last_snapshot {
                        let microprice = snap.microprice.unwrap_or(snap.mid_price.unwrap_or_default());
                        let mid_price = snap.mid_price.unwrap_or_default();
                        let volatility = snap.realized_volatility_100.unwrap_or(0.001);

                        // Compute entropy score from tick entropies
                        let entropy_score = paper_trading.mm.compute_entropy_score(
                            snap.tick_entropy_1s,
                            snap.tick_entropy_5s,
                            snap.tick_entropy_10s,
                        );

                        // Compute flow imbalance from aggressor ratios
                        let buy_vol = snap.aggr_ratio_100.unwrap_or(Decimal::new(5, 1)); // 0.5 default
                        let sell_vol = Decimal::ONE - buy_vol;
                        let flow_imbalance = paper_trading.mm.compute_flow_imbalance(buy_vol, sell_vol);

                        let timestamp_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;

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
                }

                accumulator.reset();
            }
            last_update = Instant::now();
        }

        // Draw based on mode
        match mode {
            AppMode::Menu => {
                terminal.draw(|f| draw_menu(f, &symbol, &settings))?;
            }
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
                        draw_live_mm(f, &symbol, &current_features, &paper_trading, &forward_session);
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
        }
    }
}

fn draw_menu(f: &mut ratatui::Frame, symbol: &str, settings: &TuiSettings) {
    let size = f.size();

    let persist_status = if settings.persist_features { "ON " } else { "OFF" };
    let persist_color = if settings.persist_features { Color::Green } else { Color::Red };

    let storage_str = if settings.max_storage_gb <= 0.0 {
        "UNLIMITED".to_string()
    } else {
        format!("{:.0} GB", settings.max_storage_gb)
    };

    // Calculate current data stats
    let data_dir = std::path::Path::new("./data/features");
    let (file_count, total_size_mb) = if data_dir.exists() {
        let files: Vec<_> = std::fs::read_dir(data_dir)
            .map(|rd| rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
                .collect())
            .unwrap_or_default();
        let size: u64 = files.iter()
            .filter_map(|f| f.metadata().ok())
            .map(|m| m.len())
            .sum();
        (files.len(), size as f64 / 1_000_000.0)
    } else {
        (0, 0.0)
    };

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  INGESTOR - Real-Time Market Microstructure Features",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "  Binance WebSocket -> 60+ Features -> Parquet -> Backtest",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("  Symbol: "),
            Span::styled(symbol.to_uppercase(), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::raw(format!("    Data: {} files ({:.1} MB)", file_count, total_size_mb)),
        ]),
        Line::from(""),
        Line::from(Span::styled("  DATA COLLECTION", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::styled("  [0] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Live Dashboard"),
            Span::styled(" - stream features, save to ./data/features/*.parquet", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("  [1] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Live + Market Maker"),
            Span::styled(" - paper trade while collecting data", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(Span::styled("  BACKTESTING", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::styled("  [3] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Run Backtest"),
            Span::styled(" - test MM strategy on collected data", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("  [4] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Walk-Forward Validation"),
            Span::styled(" - cross-validate to detect overfitting", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("  [5] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Data Quality Check"),
            Span::styled(" - validate data before backtesting", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(Span::styled("  INFO", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::styled("  [2] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Feature Descriptions"),
            Span::styled(" - 60+ microstructure features explained", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(Span::styled("  SETTINGS", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(vec![
            Span::styled("  [p] ", Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)),
            Span::raw("Persist to disk: "),
            Span::styled(persist_status, Style::default().fg(persist_color).add_modifier(Modifier::BOLD)),
            Span::styled("   (saves 60+ features per tick to Parquet)", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("  [s] ", Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)),
            Span::raw("Max storage: "),
            Span::styled(&storage_str, Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled(format!("   (~{} files)", settings.max_files()), Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  [q] ", Style::default().fg(Color::Red)),
            Span::raw("Quit"),
        ]),
        Line::from(""),
        Line::from(Span::styled("  OVERNIGHT DATA COLLECTION", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD))),
        Line::from(Span::styled(
            "  To record data overnight/continuously:",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "    1. Start Live Dashboard [0] and leave running",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "    2. Use tmux/screen to keep session alive: tmux new -s ingestor",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "    3. Or run headless: cargo run --release > /dev/null 2>&1 &",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled("  AFTER DATA COLLECTION (run from terminal):", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD))),
        Line::from(Span::styled(
            "    cargo run --release --bin backtest -- grid-search --test-gate",
            Style::default().fg(Color::Green),
        )),
        Line::from(""),
    ];

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" MAIN MENU ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(para, size);
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
    paper_trading: &PaperTradingEngine,
    session: &ForwardTestSession,
) {
    let size = f.size();
    let state = paper_trading.state();

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

    // Title with session info
    let now = chrono::Local::now().format("%H:%M:%S");
    let session_info = if session.is_active() {
        let m = session.metrics();
        format!("Session: {} | Quotes: {}", session.session_id(), m.quotes_generated)
    } else {
        "Session: inactive".to_string()
    };
    let title = format!(
        " {} | {} | MARKET MAKER (paper) | {} | [r] reset [q] menu ",
        symbol.to_uppercase(), now, session_info
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

    let max_inventory = paper_trading.mm.config().max_inventory;

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
    let fill_rate = paper_trading.simulator.fill_rate() * 100.0;

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
    use ingestor::backtest::{BacktestEngine, BacktestConfig, ReplayConfig};
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
    use ingestor::backtest::DataValidator;
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
