use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

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

const MAX_HISTORY: usize = 60; // 60 seconds of history at 1Hz
const UPDATE_INTERVAL_MS: u64 = 1000; // 1Hz update rate

/// Accumulator for 1-second averaging
struct MetricAccumulator {
    microprice_sum: f64,
    pwi50_sum: f64,
    entropy_sum: f64,
    count: usize,
}

impl MetricAccumulator {
    fn new() -> Self {
        Self {
            microprice_sum: 0.0,
            pwi50_sum: 0.0,
            entropy_sum: 0.0,
            count: 0,
        }
    }

    fn add(&mut self, snap: &FeaturesSnapshot) {
        self.microprice_sum += dec_to_f64(snap.microprice);
        self.pwi50_sum += dec_to_f64(snap.pwi_50);
        self.entropy_sum += dec_to_f64(snap.tick_entropy_1m);
        self.count += 1;
    }

    fn average(&self) -> (f64, f64, f64) {
        if self.count == 0 {
            return (0.0, 0.0, 0.0);
        }
        let n = self.count as f64;
        (
            self.microprice_sum / n,
            self.pwi50_sum / n,
            self.entropy_sum / n,
        )
    }

    fn reset(&mut self) {
        self.microprice_sum = 0.0;
        self.pwi50_sum = 0.0;
        self.entropy_sum = 0.0;
        self.count = 0;
    }

    fn has_data(&self) -> bool {
        self.count > 0
    }
}

/// Helper to convert Decimal to f64, defaulting to 0.0
fn dec_to_f64(d: Option<rust_decimal::Decimal>) -> f64 {
    d.and_then(|d| d.to_f64()).unwrap_or(0.0)
}

/// Run the simplified TUI dashboard (1Hz, 3 metrics)
pub fn run_tui(rx: Receiver<FeaturesSnapshot>, symbol: String) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Term::new(backend)?;
    terminal.clear()?;

    let res = ui_loop(&mut terminal, rx, symbol);

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

fn ui_loop(terminal: &mut Term, rx: Receiver<FeaturesSnapshot>, symbol: String) -> anyhow::Result<()> {
    let mut last_update = Instant::now();
    let mut accumulator = MetricAccumulator::new();

    // Current averaged values for display
    let mut current_microprice: f64 = 0.0;
    let mut current_pwi50: f64 = 0.0;
    let mut current_entropy: f64 = 0.0;
    let mut samples_in_window: usize = 0;

    // History buffers for sparklines (1 point per second)
    let mut microprice_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut pwi50_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);
    let mut entropy_hist: VecDeque<f64> = VecDeque::with_capacity(MAX_HISTORY);

    let mut has_data = false;

    loop {
        // Handle key input (non-blocking, check every 50ms)
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    _ => {}
                }
            }
        }

        // Drain channel and accumulate metrics
        while let Ok(snap) = rx.try_recv() {
            accumulator.add(&snap);
            has_data = true;
        }

        // Check if 1 second has passed
        if last_update.elapsed() >= Duration::from_millis(UPDATE_INTERVAL_MS) {
            if accumulator.has_data() {
                let (micro, pwi, ent) = accumulator.average();
                current_microprice = micro;
                current_pwi50 = pwi;
                current_entropy = ent;
                samples_in_window = accumulator.count;

                // Update history
                microprice_hist.push_back(micro);
                if microprice_hist.len() > MAX_HISTORY {
                    microprice_hist.pop_front();
                }

                pwi50_hist.push_back(pwi);
                if pwi50_hist.len() > MAX_HISTORY {
                    pwi50_hist.pop_front();
                }

                entropy_hist.push_back(ent);
                if entropy_hist.len() > MAX_HISTORY {
                    entropy_hist.pop_front();
                }

                accumulator.reset();
            }
            last_update = Instant::now();

            // Draw UI at 1Hz
            terminal.draw(|f| {
                let size = f.size();

                if !has_data {
                    // Waiting screen
                    let block = Block::default()
                        .title(Span::styled(
                            " Ingestor — waiting for data... [q] quit ",
                            Style::default().fg(Color::Gray),
                        ))
                        .borders(Borders::ALL);
                    f.render_widget(block, size);
                    return;
                }

                // Main layout: title + metrics + charts
                let rows = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(1)
                    .constraints([
                        Constraint::Length(1),  // Title bar
                        Constraint::Length(5),  // Metrics panel
                        Constraint::Min(6),     // Charts
                    ])
                    .split(size);

                // Title bar
                draw_title(f, rows[0], &symbol, samples_in_window);

                // Metrics panel
                draw_metrics(f, rows[1], current_microprice, current_pwi50, current_entropy);

                // Charts
                draw_sparklines(
                    f,
                    rows[2],
                    current_microprice,
                    current_pwi50,
                    current_entropy,
                    &microprice_hist,
                    &pwi50_hist,
                    &entropy_hist,
                );
            })?;
        }
    }
}

fn draw_title(f: &mut ratatui::Frame, area: Rect, symbol: &str, samples: usize) {
    let now = chrono::Local::now().format("%H:%M:%S");
    let title = format!(
        " INGESTOR — {} — {} — {} samples/sec — [q] quit ",
        symbol.to_uppercase(),
        now,
        samples
    );

    let para = Paragraph::new(Line::from(Span::styled(
        title,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    f.render_widget(para, area);
}

fn draw_metrics(
    f: &mut ratatui::Frame,
    area: Rect,
    microprice: f64,
    pwi50: f64,
    entropy: f64,
) {
    let pwi_color = if pwi50 > 0.0 {
        Color::Green
    } else if pwi50 < 0.0 {
        Color::Red
    } else {
        Color::Gray
    };

    // Entropy interpretation: higher = more random/uncertain
    let entropy_color = if entropy > 1.0 {
        Color::Yellow  // High entropy - caution
    } else if entropy > 0.5 {
        Color::White   // Medium
    } else {
        Color::Green   // Low entropy - more predictable
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("  MICROPRICE  ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.2}", microprice),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  PWI 50%     ", Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:+.4}%", pwi50 * 100.0),
                Style::default().fg(pwi_color).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  ENTROPY 1m  ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
            Span::styled(
                format!("{:.4}", entropy),
                Style::default().fg(entropy_color).add_modifier(Modifier::BOLD),
            ),
        ]),
    ];

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" 1-SECOND AVERAGES ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(para, area);
}

fn draw_sparklines(
    f: &mut ratatui::Frame,
    area: Rect,
    microprice: f64,
    pwi50: f64,
    entropy: f64,
    microprice_hist: &VecDeque<f64>,
    pwi50_hist: &VecDeque<f64>,
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

    // Convert f64 history to u64 for sparkline (normalized 0-100)
    fn normalize_for_spark(buf: &VecDeque<f64>) -> Vec<u64> {
        if buf.is_empty() {
            return vec![];
        }
        let min = buf.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = buf.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let span = (max - min).max(1e-9);
        buf.iter()
            .map(|v| (((v - min) / span) * 100.0).max(0.0) as u64)
            .collect()
    }

    // Microprice sparkline
    let micro_data = normalize_for_spark(microprice_hist);
    let micro_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    format!(" MICROPRICE {:.2} ", microprice),
                    Style::default().fg(Color::Cyan),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .style(Style::default().fg(Color::Cyan))
        .data(&micro_data);
    f.render_widget(micro_spark, cols[0]);

    // PWI50 sparkline
    let pwi_data = normalize_for_spark(pwi50_hist);
    let pwi_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    format!(" PWI50 {:+.2}% ", pwi50 * 100.0),
                    Style::default().fg(Color::Blue),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .style(Style::default().fg(Color::Yellow))
        .data(&pwi_data);
    f.render_widget(pwi_spark, cols[1]);

    // Entropy sparkline
    let entropy_data = normalize_for_spark(entropy_hist);
    let entropy_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    format!(" ENTROPY {:.3} ", entropy),
                    Style::default().fg(Color::Magenta),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .style(Style::default().fg(Color::Magenta))
        .data(&entropy_data);
    f.render_widget(entropy_spark, cols[2]);
}
