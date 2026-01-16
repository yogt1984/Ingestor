//! Backtest Campaign Config Screen (T-2.11)
//!
//! TUI screen for configuring backtest campaign command parameters (both algorithm types).

use std::path::PathBuf;
use std::collections::HashMap;

use anyhow::Result;

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
    Frame,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::commands::params::backtest_params::{CampaignParams, CampaignParamsBuilder};
use crate::ui::widgets::{
    TextInputWidget, NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CampaignField {
    DataPath,
    Algorithm,
    WeightsFile,
    Weeks,
    SessionHours,
    MinSessionsPerWeek,
    Preset,
    Spread,
    Skew,
    ExpectedFillRate,
    ExpectedSharpe,
    ExpectedReturn,
    MinWeeklyTrades,
    MaxDrawdownPct,
    MinWinRate,
    CampaignsDir,
    MaxInventory,
    QuoteSize,
    FeeRate,
    NaiveFills,
    FillProb,
    QueuePos,
    Output,
    Quiet,
}

impl CampaignField {
    pub fn all() -> Vec<Self> {
        vec![
            Self::DataPath,
            Self::Algorithm,
            Self::WeightsFile,
            Self::Weeks,
            Self::SessionHours,
            Self::MinSessionsPerWeek,
            Self::Preset,
            Self::Spread,
            Self::Skew,
            Self::ExpectedFillRate,
            Self::ExpectedSharpe,
            Self::ExpectedReturn,
            Self::MinWeeklyTrades,
            Self::MaxDrawdownPct,
            Self::MinWinRate,
            Self::CampaignsDir,
            Self::MaxInventory,
            Self::QuoteSize,
            Self::FeeRate,
            Self::NaiveFills,
            Self::FillProb,
            Self::QueuePos,
            Self::Output,
            Self::Quiet,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::DataPath => "Data Path",
            Self::Algorithm => "Algorithm",
            Self::WeightsFile => "Weights File",
            Self::Weeks => "Weeks",
            Self::SessionHours => "Session Hours",
            Self::MinSessionsPerWeek => "Min Sessions/Week",
            Self::Preset => "Preset Name",
            Self::Spread => "Spread (bps)",
            Self::Skew => "Skew",
            Self::ExpectedFillRate => "Expected Fill Rate",
            Self::ExpectedSharpe => "Expected Sharpe",
            Self::ExpectedReturn => "Expected Return",
            Self::MinWeeklyTrades => "Min Weekly Trades",
            Self::MaxDrawdownPct => "Max Drawdown %",
            Self::MinWinRate => "Min Win Rate",
            Self::CampaignsDir => "Campaigns Directory",
            Self::MaxInventory => "Max Inventory",
            Self::QuoteSize => "Quote Size",
            Self::FeeRate => "Fee Rate",
            Self::NaiveFills => "Naive Fills",
            Self::FillProb => "Fill Probability",
            Self::QueuePos => "Queue Position",
            Self::Output => "Output File",
            Self::Quiet => "Quiet Mode",
        }
    }

    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::DataPath | Self::Algorithm | Self::WeightsFile | Self::Weeks |
            Self::SessionHours | Self::MinSessionsPerWeek | Self::Preset => ParameterGroup::Basic,
            Self::Spread | Self::Skew | Self::ExpectedFillRate | Self::ExpectedSharpe |
            Self::ExpectedReturn | Self::MinWeeklyTrades | Self::MaxDrawdownPct |
            Self::MinWinRate | Self::CampaignsDir | Self::MaxInventory | Self::QuoteSize |
            Self::FeeRate | Self::NaiveFills | Self::FillProb | Self::QueuePos |
            Self::Quiet => ParameterGroup::Parameters,
            Self::Output => ParameterGroup::Output,
        }
    }

    pub fn fields_in_group(group: ParameterGroup) -> Vec<Self> {
        Self::all().into_iter()
            .filter(|f| f.group() == group)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterGroup {
    Basic,
    Parameters,
    Output,
}

impl ParameterGroup {
    pub fn all() -> Vec<Self> {
        vec![Self::Basic, Self::Parameters, Self::Output]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Basic => "Basic",
            Self::Parameters => "Parameters",
            Self::Output => "Output",
        }
    }

    pub fn next(&self) -> Self {
        match self {
            Self::Basic => Self::Parameters,
            Self::Parameters => Self::Output,
            Self::Output => Self::Basic,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            Self::Basic => Self::Output,
            Self::Parameters => Self::Basic,
            Self::Output => Self::Parameters,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BacktestCampaignConfigScreen {
    pub current_group: ParameterGroup,
    pub selected_field_index: usize,
    pub widgets: HashMap<CampaignField, FieldWidget>,
    pub validation_errors: HashMap<CampaignField, String>,
}

impl Default for BacktestCampaignConfigScreen {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum FieldWidget {
    TextInput(TextInputWidget),
    NumberInput(NumberInputWidget),
    Toggle(ToggleWidget),
    PathInput(PathInputWidget),
    Dropdown(DropdownWidget<String>),
}

impl BacktestCampaignConfigScreen {
    pub fn new() -> Self {
        let mut screen = Self {
            current_group: ParameterGroup::Basic,
            selected_field_index: 0,
            widgets: HashMap::new(),
            validation_errors: HashMap::new(),
        };
        
        screen.initialize_widgets();
        screen
    }

    fn initialize_widgets(&mut self) {
        self.widgets.insert(CampaignField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["as".to_string(), "ml".to_string(), "fixed".to_string(), "mom".to_string()])
                .with_placeholder("Select algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::WeightsFile, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to weights file (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Weeks, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(4.0)
                .with_min(1.0)
                .with_max(52.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::SessionHours, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(8.0)
                .with_min(0.0)
                .with_decimals(1)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::MinSessionsPerWeek, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(5.0)
                .with_min(1.0)
                .with_max(7.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Preset, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Preset name (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Spread, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(1.0)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Skew, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::ExpectedFillRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::ExpectedSharpe, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(1.0)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::ExpectedReturn, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.01)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::MinWeeklyTrades, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(10.0)
                .with_min(0.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::MaxDrawdownPct, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(10.0)
                .with_min(0.0)
                .with_max(100.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::MinWinRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::CampaignsDir, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to campaigns directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::FillProb, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Quiet, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Quiet mode")
                .set_focused(false)
        ));
        
        self.widgets.insert(CampaignField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
    }

    pub fn current_field(&self) -> Option<CampaignField> {
        let fields = CampaignField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    pub fn get_widget(&self, field: CampaignField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    pub fn get_widget_mut(&mut self, field: CampaignField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    pub fn next_field(&mut self) {
        let fields = CampaignField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    pub fn prev_field(&mut self) {
        let fields = CampaignField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = if self.selected_field_index == 0 {
                fields.len() - 1
            } else {
                self.selected_field_index - 1
            };
            self.update_focus();
        }
    }

    pub fn next_group(&mut self) {
        self.current_group = self.current_group.next();
        self.selected_field_index = 0;
        self.update_focus();
    }

    pub fn prev_group(&mut self) {
        self.current_group = self.current_group.prev();
        self.selected_field_index = 0;
        self.update_focus();
    }

    fn update_focus(&mut self) {
        let current = self.current_field();
        for (field, widget) in &mut self.widgets {
            let focused = Some(*field) == current;
            match widget {
                FieldWidget::TextInput(w) => {
                    *w = w.clone().set_focused(focused);
                }
                FieldWidget::NumberInput(w) => {
                    *w = w.clone().set_focused(focused);
                }
                FieldWidget::Toggle(w) => {
                    *w = w.clone().set_focused(focused);
                }
                FieldWidget::PathInput(w) => {
                    *w = w.clone().set_focused(focused);
                }
                FieldWidget::Dropdown(w) => {
                    *w = w.clone().set_focused(focused);
                }
            }
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Tab => {
                self.next_field();
                true
            }
            KeyCode::BackTab => {
                self.prev_field();
                true
            }
            KeyCode::Up => {
                self.prev_field();
                true
            }
            KeyCode::Down => {
                self.next_field();
                true
            }
            KeyCode::Left if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.prev_group();
                true
            }
            KeyCode::Right if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.next_group();
                true
            }
            _ => {
                if let Some(field) = self.current_field() {
                    if let Some(widget) = self.get_widget_mut(field) {
                        return widget.handle_key(key);
                    }
                }
                false
            }
        }
    }

    pub fn build_params(&self) -> Result<CampaignParams> {
        let mut builder = CampaignParamsBuilder::new();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&CampaignField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&CampaignField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&CampaignField::WeightsFile) {
            if !w.path().is_empty() {
                builder = builder.weights_file(Some(PathBuf::from(w.path())));
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::Weeks) {
            builder = builder.weeks(w.value() as u8);
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::SessionHours) {
            builder = builder.session_hours(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::MinSessionsPerWeek) {
            builder = builder.min_sessions_per_week(w.value() as u8);
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&CampaignField::Preset) {
            if !w.text().is_empty() {
                builder = builder.preset(Some(w.text().to_string()));
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::Spread) {
            builder = builder.spread(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::Skew) {
            builder = builder.skew(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::ExpectedFillRate) {
            builder = builder.expected_fill_rate(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::ExpectedSharpe) {
            builder = builder.expected_sharpe(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::ExpectedReturn) {
            builder = builder.expected_return(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::MinWeeklyTrades) {
            builder = builder.min_weekly_trades(w.value() as usize);
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::MaxDrawdownPct) {
            builder = builder.max_drawdown_pct(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::MinWinRate) {
            builder = builder.min_win_rate(w.value());
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&CampaignField::CampaignsDir) {
            if !w.path().is_empty() {
                builder = builder.campaigns_dir(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&CampaignField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::FillProb) {
            builder = builder.fill_prob(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&CampaignField::Quiet) {
            builder = builder.quiet(w.value());
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&CampaignField::Output) {
            if !w.path().is_empty() {
                builder = builder.output(Some(PathBuf::from(w.path())));
            }
        }

        builder.build()
    }

    pub fn validate(&mut self) {
        self.validation_errors.clear();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&CampaignField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(CampaignField::DataPath, "Data path is required".to_string());
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&CampaignField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(CampaignField::Algorithm, "Algorithm is required".to_string());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&CampaignField::CampaignsDir) {
            if w.path().is_empty() {
                self.validation_errors.insert(CampaignField::CampaignsDir, "Campaigns directory is required".to_string());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::FillProb) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(CampaignField::FillProb, "Fill probability must be in range [0.0, 1.0]".to_string());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&CampaignField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(CampaignField::QueuePos, "Queue position must be in range [0.0, 1.0]".to_string());
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }
}

impl FieldWidget {
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match self {
            Self::TextInput(w) => w.handle_key(key),
            Self::NumberInput(w) => w.handle_key(key),
            Self::Toggle(w) => w.handle_key(key),
            Self::PathInput(w) => w.handle_key(key),
            Self::Dropdown(w) => w.handle_key(key),
        }
    }
}

pub fn draw_backtest_campaign_config_screen(
    f: &mut Frame,
    screen: &BacktestCampaignConfigScreen,
) {
    let area = f.area();
    
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(2),
        ])
        .split(area);

    let title = "Parameter Groups";
    let tabs = Tabs::new(
        ParameterGroup::all()
            .iter()
            .map(|g| g.label())
            .collect::<Vec<_>>(),
    )
    .block(Block::default().borders(Borders::ALL).title(title))
    .select(match screen.current_group {
        ParameterGroup::Basic => 0,
        ParameterGroup::Parameters => 1,
        ParameterGroup::Output => 2,
    })
    .style(Style::default().fg(Color::White))
    .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));

    f.render_widget(tabs, chunks[0]);
    draw_content_area(f, chunks[1], screen);
    draw_status(f, chunks[2]);
}

fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestCampaignConfigScreen) {
    let fields = CampaignField::fields_in_group(screen.current_group);
    if fields.is_empty() {
        return;
    }

    let field_height = 3;
    let max_visible = (area.height / field_height).max(1) as usize;
    let start_index = screen.selected_field_index.saturating_sub(max_visible / 2);
    let end_index = (start_index + max_visible).min(fields.len());

    let mut y = area.y;
    for (idx, field) in fields.iter().enumerate().skip(start_index).take(end_index - start_index) {
        if y + field_height > area.y + area.height {
            break;
        }

        let field_area = Rect {
            x: area.x,
            y,
            width: area.width,
            height: field_height,
        };

        draw_field(f, field_area, *field, screen, idx == screen.selected_field_index);
        y += field_height;
    }
}

fn draw_field(
    f: &mut Frame,
    area: Rect,
    field: CampaignField,
    screen: &BacktestCampaignConfigScreen,
    selected: bool,
) {
    let label = field.label();
    let label_width = label.len().min(25);
    
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(label_width as u16 + 2),
            Constraint::Min(10),
        ])
        .split(area);

    let label_style = if selected {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    };

    let label_paragraph = Paragraph::new(label)
        .style(label_style)
        .block(Block::default().borders(Borders::RIGHT));
    f.render_widget(label_paragraph, chunks[0]);

    if let Some(widget) = screen.get_widget(field) {
        match widget {
            FieldWidget::TextInput(w) => w.render(f, chunks[1]),
            FieldWidget::NumberInput(w) => w.render(f, chunks[1]),
            FieldWidget::Toggle(w) => w.render(f, chunks[1]),
            FieldWidget::PathInput(w) => w.render(f, chunks[1]),
            FieldWidget::Dropdown(w) => w.render(f, chunks[1]),
        }
    }

    if let Some(error) = screen.validation_errors.get(&field) {
        let error_area = Rect {
            x: chunks[1].x,
            y: chunks[1].y + chunks[1].height,
            width: chunks[1].width,
            height: 1,
        };
        let error_span = Span::styled(
            format!("⚠ {}", error),
            Style::default().fg(Color::Red),
        );
        let error_paragraph = Paragraph::new(Line::from(vec![error_span]));
        f.render_widget(error_paragraph, error_area);
    }
}

fn draw_status(f: &mut Frame, area: Rect) {
    let status_text = "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Esc: Cancel";
    let status_paragraph = Paragraph::new(status_text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status_paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_screen() {
        let screen = BacktestCampaignConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_campaign_field_all() {
        let fields = CampaignField::all();
        assert_eq!(fields.len(), 24);
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestCampaignConfigScreen::new();
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(CampaignField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(CampaignField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(CampaignField::CampaignsDir) {
            let new_w = w.clone().with_path("/tmp/campaigns");
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }
}
