//! Backtest Walk Forward ML Config Screen (T-2.11)
//!
//! TUI screen for configuring backtest walk forward ML command parameters (MM only).
//! Supports ML-specific parameter grids with walk-forward validation.

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

use crate::commands::params::backtest_params::{WalkForwardMLParams, WalkForwardMLParamsBuilder};
use crate::ui::widgets::{
    NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget, CommaListWidget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WalkForwardMLField {
    DataPath,
    Algorithm,
    Folds,
    MinTrainHours,
    TestHours,
    Rolling,
    EmbargoHours,
    SpreadIntercepts,
    SpreadEntropyWeights,
    SpreadVolWeights,
    SkewIntercepts,
    SkewInvWeights,
    MaxInventory,
    QuoteSize,
    FillProb,
    FeeRate,
    NaiveFills,
    QueuePos,
    Output,
    WeightsOutput,
}

impl WalkForwardMLField {
    pub fn all() -> Vec<Self> {
        vec![
            Self::DataPath,
            Self::Algorithm,
            Self::Folds,
            Self::MinTrainHours,
            Self::TestHours,
            Self::Rolling,
            Self::EmbargoHours,
            Self::SpreadIntercepts,
            Self::SpreadEntropyWeights,
            Self::SpreadVolWeights,
            Self::SkewIntercepts,
            Self::SkewInvWeights,
            Self::MaxInventory,
            Self::QuoteSize,
            Self::FillProb,
            Self::FeeRate,
            Self::NaiveFills,
            Self::QueuePos,
            Self::Output,
            Self::WeightsOutput,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::DataPath => "Data Path",
            Self::Algorithm => "Algorithm (MM only)",
            Self::Folds => "Folds",
            Self::MinTrainHours => "Min Train Hours",
            Self::TestHours => "Test Hours",
            Self::Rolling => "Rolling Window",
            Self::EmbargoHours => "Embargo Hours",
            Self::SpreadIntercepts => "Spread Intercepts (grid)",
            Self::SpreadEntropyWeights => "Spread Entropy Weights (grid)",
            Self::SpreadVolWeights => "Spread Vol Weights (grid)",
            Self::SkewIntercepts => "Skew Intercepts (grid)",
            Self::SkewInvWeights => "Skew Inv Weights (grid)",
            Self::MaxInventory => "Max Inventory",
            Self::QuoteSize => "Quote Size",
            Self::FillProb => "Fill Probability",
            Self::FeeRate => "Fee Rate",
            Self::NaiveFills => "Naive Fills",
            Self::QueuePos => "Queue Position",
            Self::Output => "Output File",
            Self::WeightsOutput => "Weights Output File",
        }
    }

    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::DataPath | Self::Algorithm | Self::Folds | Self::MinTrainHours |
            Self::TestHours | Self::Rolling | Self::EmbargoHours => ParameterGroup::Basic,
            Self::SpreadIntercepts | Self::SpreadEntropyWeights | Self::SpreadVolWeights |
            Self::SkewIntercepts | Self::SkewInvWeights | Self::MaxInventory |
            Self::QuoteSize | Self::FillProb | Self::FeeRate |
            Self::NaiveFills | Self::QueuePos => ParameterGroup::Parameters,
            Self::Output | Self::WeightsOutput => ParameterGroup::Output,
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
pub struct BacktestWalkForwardMLConfigScreen {
    pub current_group: ParameterGroup,
    pub selected_field_index: usize,
    pub widgets: HashMap<WalkForwardMLField, FieldWidget>,
    pub validation_errors: HashMap<WalkForwardMLField, String>,
    pub total_combinations: usize,
}

impl Default for BacktestWalkForwardMLConfigScreen {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum FieldWidget {
    NumberInput(NumberInputWidget),
    Toggle(ToggleWidget),
    PathInput(PathInputWidget),
    Dropdown(DropdownWidget<String>),
    CommaList(CommaListWidget),
}

impl BacktestWalkForwardMLConfigScreen {
    pub fn new() -> Self {
        let mut screen = Self {
            current_group: ParameterGroup::Basic,
            selected_field_index: 0,
            widgets: HashMap::new(),
            validation_errors: HashMap::new(),
            total_combinations: 0,
        };
        
        screen.initialize_widgets();
        screen.update_combinations();
        screen
    }

    fn initialize_widgets(&mut self) {
        self.widgets.insert(WalkForwardMLField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["as".to_string(), "ml".to_string(), "fixed".to_string()])
                .with_placeholder("Select MM algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::Folds, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(5.0)
                .with_min(1.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::MinTrainHours, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(720.0)
                .with_min(0.0)
                .with_decimals(1)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::TestHours, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(168.0)
                .with_min(0.0)
                .with_decimals(1)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::Rolling, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(true)
                .with_label("Use rolling window")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::EmbargoHours, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(24.0)
                .with_min(0.0)
                .with_decimals(1)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::SpreadIntercepts, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.001,0.002,0.003")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::SpreadEntropyWeights, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2,0.3")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::SpreadVolWeights, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2,0.3")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::SkewIntercepts, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.0,0.1,0.2")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::SkewInvWeights, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2,0.3")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::FillProb, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(WalkForwardMLField::WeightsOutput, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Weights output file path (optional)...")
                .set_focused(false)
        ));
    }

    pub fn current_field(&self) -> Option<WalkForwardMLField> {
        let fields = WalkForwardMLField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    pub fn get_widget(&self, field: WalkForwardMLField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    pub fn get_widget_mut(&mut self, field: WalkForwardMLField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    pub fn next_field(&mut self) {
        let fields = WalkForwardMLField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    pub fn prev_field(&mut self) {
        let fields = WalkForwardMLField::fields_in_group(self.current_group);
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
                FieldWidget::CommaList(w) => {
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
                        let handled = widget.handle_key(key);
                        if handled {
                            if matches!(field, WalkForwardMLField::SpreadIntercepts | WalkForwardMLField::SpreadEntropyWeights |
                                      WalkForwardMLField::SpreadVolWeights | WalkForwardMLField::SkewIntercepts |
                                      WalkForwardMLField::SkewInvWeights) {
                                self.update_combinations();
                            }
                        }
                        return handled;
                    }
                }
                false
            }
        }
    }

    fn update_combinations(&mut self) {
        let spread_int = self.get_grid_size(WalkForwardMLField::SpreadIntercepts);
        let spread_ent = self.get_grid_size(WalkForwardMLField::SpreadEntropyWeights);
        let spread_vol = self.get_grid_size(WalkForwardMLField::SpreadVolWeights);
        let skew_int = self.get_grid_size(WalkForwardMLField::SkewIntercepts);
        let skew_inv = self.get_grid_size(WalkForwardMLField::SkewInvWeights);
        self.total_combinations = spread_int * spread_ent * spread_vol * skew_int * skew_inv;
    }

    fn get_grid_size(&self, field: WalkForwardMLField) -> usize {
        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&field) {
            w.len().max(1)
        } else {
            1
        }
    }

    pub fn build_params(&self) -> Result<WalkForwardMLParams> {
        let mut builder = WalkForwardMLParamsBuilder::new();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&WalkForwardMLField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&WalkForwardMLField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::Folds) {
            builder = builder.folds(w.value() as usize);
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::MinTrainHours) {
            builder = builder.min_train_hours(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::TestHours) {
            builder = builder.test_hours(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&WalkForwardMLField::Rolling) {
            builder = builder.rolling(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::EmbargoHours) {
            builder = builder.embargo_hours(w.value());
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&WalkForwardMLField::SpreadIntercepts) {
            builder = builder.spread_intercepts(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&WalkForwardMLField::SpreadEntropyWeights) {
            builder = builder.spread_entropy_weights(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&WalkForwardMLField::SpreadVolWeights) {
            builder = builder.spread_vol_weights(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&WalkForwardMLField::SkewIntercepts) {
            builder = builder.skew_intercepts(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&WalkForwardMLField::SkewInvWeights) {
            builder = builder.skew_inv_weights(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::FillProb) {
            builder = builder.fill_prob(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&WalkForwardMLField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&WalkForwardMLField::Output) {
            if !w.path().is_empty() {
                builder = builder.output(Some(PathBuf::from(w.path())));
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&WalkForwardMLField::WeightsOutput) {
            if !w.path().is_empty() {
                builder = builder.weights_output(Some(PathBuf::from(w.path())));
            }
        }

        builder.build()
    }

    pub fn validate(&mut self) {
        self.validation_errors.clear();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&WalkForwardMLField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(WalkForwardMLField::DataPath, "Data path is required".to_string());
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&WalkForwardMLField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(WalkForwardMLField::Algorithm, "Algorithm is required".to_string());
            } else if let Some(alg) = w.selected_option() {
                if !["as", "ml", "fixed"].contains(&alg.as_str()) {
                    self.validation_errors.insert(WalkForwardMLField::Algorithm, "Algorithm must be MM type: as, ml, or fixed".to_string());
                }
            }
        }

        for field in [WalkForwardMLField::SpreadIntercepts, WalkForwardMLField::SpreadEntropyWeights,
                     WalkForwardMLField::SpreadVolWeights, WalkForwardMLField::SkewIntercepts,
                     WalkForwardMLField::SkewInvWeights] {
            if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&field) {
                if w.is_empty() {
                    self.validation_errors.insert(field, format!("{} must have at least one value", field.label()));
                }
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::Folds) {
            if w.value() < 1.0 {
                self.validation_errors.insert(WalkForwardMLField::Folds, "Folds must be >= 1".to_string());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::FillProb) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(WalkForwardMLField::FillProb, "Fill probability must be in range [0.0, 1.0]".to_string());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&WalkForwardMLField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(WalkForwardMLField::QueuePos, "Queue position must be in range [0.0, 1.0]".to_string());
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }

    pub fn combination_preview(&self) -> String {
        let spread_int = self.get_grid_size(WalkForwardMLField::SpreadIntercepts);
        let spread_ent = self.get_grid_size(WalkForwardMLField::SpreadEntropyWeights);
        let spread_vol = self.get_grid_size(WalkForwardMLField::SpreadVolWeights);
        let skew_int = self.get_grid_size(WalkForwardMLField::SkewIntercepts);
        let skew_inv = self.get_grid_size(WalkForwardMLField::SkewInvWeights);
        format!(
            "Spread: {}×{}×{} × Skew: {}×{} = {} combinations",
            spread_int, spread_ent, spread_vol, skew_int, skew_inv, self.total_combinations
        )
    }
}

impl FieldWidget {
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match self {
            Self::NumberInput(w) => w.handle_key(key),
            Self::Toggle(w) => w.handle_key(key),
            Self::PathInput(w) => w.handle_key(key),
            Self::Dropdown(w) => w.handle_key(key),
            Self::CommaList(w) => w.handle_key(key),
        }
    }
}

pub fn draw_backtest_walk_forward_ml_config_screen(
    f: &mut Frame,
    screen: &BacktestWalkForwardMLConfigScreen,
) {
    let area = f.area();
    
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(4),
        ])
        .split(area);

    let title = "Parameter Groups (MM Only - ML)";
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
    draw_preview_and_status(f, chunks[2], screen);
}

fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestWalkForwardMLConfigScreen) {
    let fields = WalkForwardMLField::fields_in_group(screen.current_group);
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
    field: WalkForwardMLField,
    screen: &BacktestWalkForwardMLConfigScreen,
    selected: bool,
) {
    let label = field.label();
    let label_width = label.len().min(35);
    
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
            FieldWidget::NumberInput(w) => w.render(f, chunks[1]),
            FieldWidget::Toggle(w) => w.render(f, chunks[1]),
            FieldWidget::PathInput(w) => w.render(f, chunks[1]),
            FieldWidget::Dropdown(w) => w.render(f, chunks[1]),
            FieldWidget::CommaList(w) => w.render(f, chunks[1]),
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

fn draw_preview_and_status(f: &mut Frame, area: Rect, screen: &BacktestWalkForwardMLConfigScreen) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(area);

    let preview_text = screen.combination_preview();
    let preview_paragraph = Paragraph::new(preview_text)
        .style(Style::default().fg(Color::Cyan))
        .block(Block::default().borders(Borders::ALL).title("Combination Preview"));
    f.render_widget(preview_paragraph, chunks[0]);

    let status_text = "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Esc: Cancel";
    let status_paragraph = Paragraph::new(status_text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status_paragraph, chunks[1]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_screen() {
        let screen = BacktestWalkForwardMLConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_walk_forward_ml_field_all() {
        let fields = WalkForwardMLField::all();
        assert_eq!(fields.len(), 20);
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestWalkForwardMLConfigScreen::new();
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(WalkForwardMLField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(WalkForwardMLField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        for field in [WalkForwardMLField::SpreadIntercepts, WalkForwardMLField::SpreadEntropyWeights,
                     WalkForwardMLField::SpreadVolWeights, WalkForwardMLField::SkewIntercepts,
                     WalkForwardMLField::SkewInvWeights] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }
}
