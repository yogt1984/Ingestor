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

type Term = Terminal<CrosstermBackend<io::Stdout>>;

const MAX_HISTORY: usize = 60; // 60 seconds of history at 1Hz
const UPDATE_INTERVAL_MS: u64 = 1000; // 1Hz update rate

/// Application mode
#[derive(Clone, Copy, PartialEq)]
enum AppMode {
    Menu,
    Live,
    Features,
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
    ]
}

/// Run the TUI with menu system
pub fn run_tui(rx: Receiver<FeaturesSnapshot>, symbol: String) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Term::new(backend)?;
    terminal.clear()?;

    let res = main_loop(&mut terminal, rx, symbol);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

fn main_loop(terminal: &mut Term, rx: Receiver<FeaturesSnapshot>, symbol: String) -> anyhow::Result<()> {
    let mut mode = AppMode::Menu;
    let mut scroll_offset: u16 = 0;
    let mut last_update = Instant::now();
    let mut accumulator = FeatureAccumulator::default();
    let mut current_features = AveragedFeatures::default();
    let mut has_data = false;

    // History for sparklines
    let mut microprice_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut pwi50_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut entropy_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);

    loop {
        // Handle input
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match mode {
                    AppMode::Menu => match key.code {
                        KeyCode::Char('0') => mode = AppMode::Live,
                        KeyCode::Char('1') => {
                            mode = AppMode::Features;
                            scroll_offset = 0;
                        }
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                        _ => {}
                    },
                    AppMode::Live => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => mode = AppMode::Menu,
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
                }
            }
        }

        // Drain data from channel
        while let Ok(snap) = rx.try_recv() {
            accumulator.add(&snap);
            has_data = true;
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

                accumulator.reset();
            }
            last_update = Instant::now();
        }

        // Draw based on mode
        match mode {
            AppMode::Menu => {
                terminal.draw(|f| draw_menu(f, &symbol))?;
            }
            AppMode::Live => {
                terminal.draw(|f| {
                    if !has_data {
                        draw_waiting(f);
                    } else {
                        draw_live(f, &symbol, &current_features, &microprice_hist, &pwi50_hist, &entropy_hist);
                    }
                })?;
            }
            AppMode::Features => {
                terminal.draw(|f| draw_features(f, &mut scroll_offset))?;
            }
        }
    }
}

fn draw_menu(f: &mut ratatui::Frame, symbol: &str) {
    let size = f.size();

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  INGESTOR - Real-Time Market Microstructure Features",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(format!("  Symbol: {}", symbol.to_uppercase())),
        Line::from(""),
        Line::from(Span::styled("  Select an option:", Style::default().fg(Color::Yellow))),
        Line::from(""),
        Line::from(vec![
            Span::styled("  [0] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Run Live Dashboard"),
        ]),
        Line::from(vec![
            Span::styled("  [1] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("Show Feature Descriptions"),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  [q] ", Style::default().fg(Color::Red)),
            Span::raw("Quit"),
        ]),
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
) {
    let size = f.size();

    // Layout: title + 4 panels + sparklines
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(1),   // Title
            Constraint::Length(8),   // Order Book
            Constraint::Length(6),   // Trades
            Constraint::Length(4),   // Illiquidity
            Constraint::Length(4),   // Entropy
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

    // Sparklines - 3 columns for microprice, PWI50, entropy
    let spark_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(rows[5]);

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
            .title(format!(" MICROPRICE {:.2} ", feat.microprice))
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
            .title(format!(" PWI50 {:+.2}% ", feat.pwi_50 * 100.0))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)))
        .style(Style::default().fg(pwi_color))
        .data(&pwi_data);
    f.render_widget(pwi_spark, spark_cols[1]);

    // Entropy sparkline
    let ent_data = normalize_spark(entropy_hist);
    let ent_spark = Sparkline::default()
        .block(Block::default()
            .title(format!(" ENTROPY {:.3} ", feat.tick_entropy_1m))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)))
        .style(Style::default().fg(Color::Magenta))
        .data(&ent_data);
    f.render_widget(ent_spark, spark_cols[2]);
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
