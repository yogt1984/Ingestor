use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use crossbeam::channel::Receiver;
use rust_decimal::prelude::ToPrimitive;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

use ratatui::{
    backend::CrosstermBackend,
    Terminal,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::{Block, Borders, Paragraph, Sparkline},
    style::{Style, Color, Modifier},
    text::{Span, Line},
};

use crate::feature_fusion::FeaturesSnapshot;

type Term = Terminal<CrosstermBackend<io::Stdout>>;

const MAX_HISTORY: usize = 120; // Last 120 samples for sparklines

/// Helper to convert Decimal to f64, defaulting to 0.0
fn dec_to_f64(d: Option<rust_decimal::Decimal>) -> f64 {
    d.and_then(|d| d.to_f64()).unwrap_or(0.0)
}

/// Run the TUI dashboard
pub fn run_tui(rx: Receiver<FeaturesSnapshot>) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Term::new(backend)?;
    terminal.clear()?;

    let res = ui_loop(&mut terminal, rx);

    // Graceful shutdown
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

fn ui_loop(terminal: &mut Term, rx: Receiver<FeaturesSnapshot>) -> anyhow::Result<()> {
    let mut last_snapshot: Option<FeaturesSnapshot> = None;
    let mut last_redraw = Instant::now();

    // History buffers for sparklines
    let mut mid_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut pwi_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut entropy_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);

    loop {
        // Handle key input (non-blocking)
        if event::poll(Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    _ => {}
                }
            }
        }

        // Drain channel for latest metrics
        while let Ok(snap) = rx.try_recv() {
            let mid = dec_to_f64(snap.mid_price);
            mid_hist.push_back(mid);
            if mid_hist.len() > MAX_HISTORY {
                mid_hist.pop_front();
            }

            let pwi_50 = dec_to_f64(snap.pwi_50);
            pwi_hist.push_back(pwi_50);
            if pwi_hist.len() > MAX_HISTORY {
                pwi_hist.pop_front();
            }

            // Use 1m tick entropy as a single scalar history
            let ent_1m = dec_to_f64(snap.tick_entropy_1m);
            entropy_hist.push_back(ent_1m);
            if entropy_hist.len() > MAX_HISTORY {
                entropy_hist.pop_front();
            }

            last_snapshot = Some(snap);
        }

        if last_snapshot.is_none() {
            // No data yet: simple "waiting" screen
            terminal.draw(|f| {
                let size = f.size();
                let block = Block::default()
                    .title(Span::styled(
                        "Ingestor TUI — waiting for data… (press q to quit)",
                        Style::default().fg(Color::Gray),
                    ))
                    .borders(Borders::ALL);
                f.render_widget(block, size);
            })?;
            continue;
        }

        if last_redraw.elapsed() >= Duration::from_millis(80) {
            let snap = last_snapshot.as_ref().unwrap();
            terminal.draw(|f| {
                let size = f.size();

                // Outer layout: 3 vertical sections
                let rows = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(1)
                    .constraints([
                        Constraint::Length(7), // core + liquidity
                        Constraint::Length(7), // trades + flow
                        Constraint::Min(5),    // charts
                    ])
                    .split(size);

                draw_title_bar(f, size, snap);
                draw_core_liquidity(f, rows[0], snap);
                draw_trades_flow(f, rows[1], snap);
                draw_charts(
                    f,
                    rows[2],
                    snap,
                    &mid_hist,
                    &pwi_hist,
                    &entropy_hist,
                );
            })?;

            last_redraw = Instant::now();
        }
    }
}

// ────────────────────────────────────────────────────────────
// Drawing helpers
// ────────────────────────────────────────────────────────────

fn draw_title_bar(
    f: &mut ratatui::Frame,
    area: Rect,
    snap: &FeaturesSnapshot,
) {
    let timestamp = DateTime::parse_from_rfc3339(&snap.timestamp)
        .unwrap_or_else(|_| Utc::now().into())
        .with_timezone(&Utc)
        .format("%Y-%m-%d %H:%M:%S UTC");

    let title = format!(" Ingestor — AVAXUSDT — {} — [q] quit ", timestamp);

    let block = Block::default()
        .borders(Borders::TOP)
        .title(Span::styled(
            title,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));

    let header_area = Rect {
        x: area.x,
        y: area.y,
        width: area.width,
        height: 1,
    };
    f.render_widget(block, header_area);
}

fn draw_core_liquidity(
    f: &mut ratatui::Frame,
    area: Rect,
    snap: &FeaturesSnapshot,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // CORE block
    let mid = dec_to_f64(snap.mid_price);
    let micro = dec_to_f64(snap.microprice);
    let delta = micro - mid;
    let delta_color = if delta > 0.0 {
        Color::Green
    } else if delta < 0.0 {
        Color::Red
    } else {
        Color::Gray
    };

    let spread = dec_to_f64(snap.spread);
    let bid = dec_to_f64(snap.best_bid);
    let ask = dec_to_f64(snap.best_ask);
    let imb = dec_to_f64(snap.imbalance) * 100.0;
    let pwi_1 = dec_to_f64(snap.pwi_1) * 100.0;
    let pwi_5 = dec_to_f64(snap.pwi_5) * 100.0;
    let pwi_25 = dec_to_f64(snap.pwi_25) * 100.0;
    let pwi_50 = dec_to_f64(snap.pwi_50) * 100.0;
    let bid_slope = dec_to_f64(snap.bid_slope);
    let ask_slope = dec_to_f64(snap.ask_slope);
    let vol_imb = dec_to_f64(snap.volume_imbalance_top5) * 100.0;
    let bid_depth = dec_to_f64(snap.bid_depth_ratio);
    let ask_depth = dec_to_f64(snap.ask_depth_ratio);

    let core_lines = vec![
        Line::from(vec![
            Span::styled("MID ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.4}", mid)),
            Span::raw("  "),
            Span::styled("MICRO ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.4}", micro)),
        ]),
        Line::from(vec![
            Span::styled("ΔMICRO-MID ", Style::default().fg(Color::Yellow)),
            Span::styled(format!("{:+.4}", delta), Style::default().fg(delta_color)),
            Span::raw("  "),
            Span::styled("SPRD ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.4}", spread)),
        ]),
        Line::from(vec![
            Span::styled("BID ", Style::default().fg(Color::Green)),
            Span::raw(format!("{:.4}", bid)),
            Span::raw("  "),
            Span::styled("ASK ", Style::default().fg(Color::Red)),
            Span::raw(format!("{:.4}", ask)),
        ]),
        Line::from(vec![
            Span::styled("IMB ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.1}%", imb)),
        ]),
        Line::from(vec![
            Span::styled("PWI ", Style::default().fg(Color::Blue)),
            Span::raw(format!(
                "1%={:+.2}% 5%={:+.2}% 25%={:+.2}% 50%={:+.2}%",
                pwi_1, pwi_5, pwi_25, pwi_50
            )),
        ]),
        Line::from(vec![
            Span::styled("SLOPE ", Style::default().fg(Color::Gray)),
            Span::raw(format!("B{:.4}/A{:.4}", bid_slope, ask_slope)),
            Span::raw("  "),
            Span::styled("VOL_IMB ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.1}%", vol_imb)),
        ]),
        Line::from(vec![
            Span::styled("DEPTH ", Style::default().fg(Color::Gray)),
            Span::raw(format!("B{:.2}/A{:.2}", bid_depth, ask_depth)),
        ]),
    ];

    let core = Paragraph::new(core_lines)
        .block(
            Block::default()
                .title(" CORE ")
                .borders(Borders::ALL),
        );
    f.render_widget(core, chunks[0]);

    // LIQUIDITY block
    let roll = dec_to_f64(snap.roll_spread);
    let amihud = dec_to_f64(snap.amihuds_lambda);
    let kyle = dec_to_f64(snap.kyles_lambda);
    let hasbrouck = dec_to_f64(snap.hasbroucks_lambda);
    let vpin = dec_to_f64(snap.vpin);

    let liq_lines = vec![
        Line::from(vec![
            Span::styled("Roll ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.6}", roll)),
            Span::raw("  "),
            Span::styled("Amihud ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.3e}", amihud)),
        ]),
        Line::from(vec![
            Span::styled("Kyle ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", kyle)),
            Span::raw("  "),
            Span::styled("Hasbrouck ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:.4}", hasbrouck)),
        ]),
        Line::from(vec![
            Span::styled("VPIN ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:.2}", vpin)),
        ]),
    ];

    let liq = Paragraph::new(liq_lines)
        .block(
            Block::default()
                .title(" LIQUIDITY ")
                .borders(Borders::ALL),
        );
    f.render_widget(liq, chunks[1]);
}

fn draw_trades_flow(
    f: &mut ratatui::Frame,
    area: Rect,
    snap: &FeaturesSnapshot,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // TRADES
    let last_price = dec_to_f64(snap.last_trade_price);
    let vwap_tot = dec_to_f64(snap.vwap_total);
    let vwap_10 = dec_to_f64(snap.vwap_10);
    let vwap_50 = dec_to_f64(snap.vwap_50);
    let vwap_100 = dec_to_f64(snap.vwap_100);
    let vwap_1000 = dec_to_f64(snap.vwap_1000);
    let price_change = dec_to_f64(snap.price_change);
    let avg_trade = dec_to_f64(snap.avg_trade_size);
    let momentum = snap.signed_count_momentum as f64;
    let trade_rate = snap.trade_rate_10s.unwrap_or(0.0);

    let mom_color = if momentum > 0.0 {
        Color::Green
    } else if momentum < 0.0 {
        Color::Red
    } else {
        Color::Gray
    };

    let trade_lines = vec![
        Line::from(vec![
            Span::styled("LAST ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.4}", last_price)),
            Span::raw("  "),
            Span::styled("SIZE ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{:.4}", avg_trade)),
        ]),
        Line::from(vec![
            Span::styled("VWAP ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("TOT={:.4} 10={:.4} 50={:.4}", vwap_tot, vwap_10, vwap_50)),
        ]),
        Line::from(vec![
            Span::styled("VWAP ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("100={:.4} 1000={:.4}", vwap_100, vwap_1000)),
        ]),
        Line::from(vec![
            Span::styled("ΔPRICE ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.4}%", price_change)),
            Span::raw("  "),
            Span::styled("MOM ", Style::default().fg(Color::Yellow)),
            Span::styled(format!("{:+.0}", momentum), Style::default().fg(mom_color)),
        ]),
        Line::from(vec![
            Span::styled("RATE ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.1} /s", trade_rate)),
        ]),
    ];

    let trades = Paragraph::new(trade_lines)
        .block(
            Block::default()
                .title(" TRADES ")
                .borders(Borders::ALL),
        );
    f.render_widget(trades, chunks[0]);

    // FLOW
    let flow_imb = dec_to_f64(snap.order_flow_imbalance);
    let flow_pressure = dec_to_f64(Some(snap.order_flow_pressure));
    let flow_sig = snap.order_flow_significance;
    let aggr_10 = dec_to_f64(snap.aggr_ratio_10) * 100.0;
    let aggr_50 = dec_to_f64(snap.aggr_ratio_50) * 100.0;
    let aggr_100 = dec_to_f64(snap.aggr_ratio_100) * 100.0;
    let aggr_1000 = dec_to_f64(snap.aggr_ratio_1000) * 100.0;

    let flow_color = if flow_sig {
        Color::Yellow
    } else {
        Color::Gray
    };

    let flow_lines = vec![
        Line::from(vec![
            Span::styled("IMB ", Style::default().fg(Color::Blue)),
            Span::raw(format!("{:+.3}", flow_imb)),
        ]),
        Line::from(vec![
            Span::styled("PRES ", Style::default().fg(Color::Blue)),
            Span::raw(format!("{:.1}", flow_pressure)),
        ]),
        Line::from(vec![
            Span::styled("SIG ", Style::default().fg(flow_color)),
            Span::raw(if flow_sig { "●" } else { "○" }),
        ]),
        Line::from(vec![
            Span::styled("AGGR ", Style::default().fg(Color::Gray)),
            Span::raw(format!("10={:.1}% 50={:.1}%", aggr_10, aggr_50)),
        ]),
        Line::from(vec![
            Span::styled("AGGR ", Style::default().fg(Color::Gray)),
            Span::raw(format!("100={:.1}% 1000={:.1}%", aggr_100, aggr_1000)),
        ]),
    ];

    let flow = Paragraph::new(flow_lines)
        .block(
            Block::default()
                .title(" FLOW ")
                .borders(Borders::ALL),
        );
    f.render_widget(flow, chunks[1]);
}

fn draw_charts(
    f: &mut ratatui::Frame,
    area: Rect,
    snap: &FeaturesSnapshot,
    mid_hist: &VecDeque<f64>,
    pwi_hist: &VecDeque<f64>,
    entropy_hist: &VecDeque<f64>,
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(area);

    // Helper: convert f64 hist to u64 for sparkline
    fn as_spark_data(buf: &VecDeque<f64>) -> Vec<u64> {
        if buf.is_empty() {
            return vec![];
        }
        let min = buf.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = buf.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let span = (max - min).max(1e-9);
        buf.iter()
            .map(|v| {
                let normalized = ((v - min) / span * 100.0).max(0.0);
                normalized as u64
            })
            .collect()
    }

    // 1) MID price sparkline
    let mid_data = as_spark_data(mid_hist);
    let mid = dec_to_f64(snap.mid_price);
    let mid_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    format!(" MID {:.4} ", mid),
                    Style::default().fg(Color::Cyan),
                ))
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Green))
        .data(&mid_data);
    f.render_widget(mid_spark, cols[0]);

    // 2) PWI 50% sparkline
    let pwi_data = as_spark_data(pwi_hist);
    let pwi_50 = dec_to_f64(snap.pwi_50) * 100.0;
    let pwi_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    format!(" PWI50 {:+.2}% ", pwi_50),
                    Style::default().fg(Color::Blue),
                ))
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Yellow))
        .data(&pwi_data);
    f.render_widget(pwi_spark, cols[1]);

    // 3) Entropy sparkline
    let entropy_data = as_spark_data(entropy_hist);
    let ent_1m = dec_to_f64(snap.tick_entropy_1m);
    let ent_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    format!(" ENTROPY (1m) {:.4} ", ent_1m),
                    Style::default().fg(Color::Magenta),
                ))
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::Magenta))
        .data(&entropy_data);
    f.render_widget(ent_spark, cols[2]);
}

