//! Backtest Train Config Screen (T-2.11)
//!
//! TUI screen for configuring backtest train command parameters (MM only - ML Spread/Skew).
//! Supports ML-specific parameter grids for spread and skew model training.

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

use crate::commands::params::backtest_params::{TrainParams, TrainParamsBuilder};
use crate::ui::widgets::{
    NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget, CommaListWidget,
};

// ============================================================================
// Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrainField {
    DataPath,
    Algorithm,
    TrainRatio,
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
}

impl TrainField {
    pub fn all() -> Vec<Self> {
        vec![
            Self::DataPath,
            Self::Algorithm,
            Self::TrainRatio,
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
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::DataPath => "Data Path",
            Self::Algorithm => "Algorithm (ML only)",
            Self::TrainRatio => "Train Ratio",
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
        }
    }

    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::DataPath | Self::Algorithm | Self::TrainRatio => ParameterGroup::Basic,
            Self::SpreadIntercepts | Self::SpreadEntropyWeights | Self::SpreadVolWeights |
            Self::SkewIntercepts | Self::SkewInvWeights | Self::MaxInventory |
            Self::QuoteSize | Self::FillProb | Self::FeeRate |
            Self::NaiveFills | Self::QueuePos => ParameterGroup::Parameters,
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
pub struct BacktestTrainConfigScreen {
    pub current_group: ParameterGroup,
    pub selected_field_index: usize,
    pub widgets: HashMap<TrainField, FieldWidget>,
    pub validation_errors: HashMap<TrainField, String>,
    pub total_combinations: usize,
}

impl Default for BacktestTrainConfigScreen {
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

impl BacktestTrainConfigScreen {
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
        self.widgets.insert(TrainField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["ml".to_string(), "ml-spread-skew".to_string()])
                .with_placeholder("Select ML algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::TrainRatio, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.8)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        // ML grids
        self.widgets.insert(TrainField::SpreadIntercepts, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.001,0.002,0.003")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::SpreadEntropyWeights, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2,0.3")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::SpreadVolWeights, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2,0.3")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::SkewIntercepts, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.0,0.1,0.2")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::SkewInvWeights, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2,0.3")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::FillProb, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(TrainField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
    }

    pub fn current_field(&self) -> Option<TrainField> {
        let fields = TrainField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    pub fn get_widget(&self, field: TrainField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    pub fn get_widget_mut(&mut self, field: TrainField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    pub fn next_field(&mut self) {
        let fields = TrainField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    pub fn prev_field(&mut self) {
        let fields = TrainField::fields_in_group(self.current_group);
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
                            if matches!(field, TrainField::SpreadIntercepts | TrainField::SpreadEntropyWeights |
                                      TrainField::SpreadVolWeights | TrainField::SkewIntercepts |
                                      TrainField::SkewInvWeights) {
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
        let spread_int = self.get_grid_size(TrainField::SpreadIntercepts);
        let spread_ent = self.get_grid_size(TrainField::SpreadEntropyWeights);
        let spread_vol = self.get_grid_size(TrainField::SpreadVolWeights);
        let skew_int = self.get_grid_size(TrainField::SkewIntercepts);
        let skew_inv = self.get_grid_size(TrainField::SkewInvWeights);
        self.total_combinations = spread_int * spread_ent * spread_vol * skew_int * skew_inv;
    }

    fn get_grid_size(&self, field: TrainField) -> usize {
        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&field) {
            w.len().max(1)
        } else {
            1
        }
    }

    pub fn build_params(&self) -> Result<TrainParams> {
        let mut builder = TrainParamsBuilder::new();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TrainField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&TrainField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::TrainRatio) {
            builder = builder.train_ratio(w.value());
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TrainField::SpreadIntercepts) {
            builder = builder.spread_intercepts(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TrainField::SpreadEntropyWeights) {
            builder = builder.spread_entropy_weights(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TrainField::SpreadVolWeights) {
            builder = builder.spread_vol_weights(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TrainField::SkewIntercepts) {
            builder = builder.skew_intercepts(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TrainField::SkewInvWeights) {
            builder = builder.skew_inv_weights(w.values().iter().map(|v| v.to_string()).collect::<Vec<_>>().join(","));
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::FillProb) {
            builder = builder.fill_prob(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&TrainField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TrainField::Output) {
            if !w.path().is_empty() {
                builder = builder.output(Some(PathBuf::from(w.path())));
            }
        }

        builder.build()
    }

    pub fn validate(&mut self) {
        self.validation_errors.clear();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TrainField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(TrainField::DataPath, "Data path is required".to_string());
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&TrainField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(TrainField::Algorithm, "Algorithm is required".to_string());
            } else if let Some(alg) = w.selected_option() {
                if !["ml", "ml-spread-skew"].contains(&alg.as_str()) {
                    self.validation_errors.insert(TrainField::Algorithm, "Algorithm must be ML type: ml or ml-spread-skew".to_string());
                }
            }
        }

        for field in [TrainField::SpreadIntercepts, TrainField::SpreadEntropyWeights,
                     TrainField::SpreadVolWeights, TrainField::SkewIntercepts,
                     TrainField::SkewInvWeights] {
            if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&field) {
                if w.is_empty() {
                    self.validation_errors.insert(field, format!("{} must have at least one value", field.label()));
                }
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::TrainRatio) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(TrainField::TrainRatio, "Train ratio must be in range [0.0, 1.0]".to_string());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::FillProb) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(TrainField::FillProb, "Fill probability must be in range [0.0, 1.0]".to_string());
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TrainField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(TrainField::QueuePos, "Queue position must be in range [0.0, 1.0]".to_string());
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }

    pub fn combination_preview(&self) -> String {
        let spread_int = self.get_grid_size(TrainField::SpreadIntercepts);
        let spread_ent = self.get_grid_size(TrainField::SpreadEntropyWeights);
        let spread_vol = self.get_grid_size(TrainField::SpreadVolWeights);
        let skew_int = self.get_grid_size(TrainField::SkewIntercepts);
        let skew_inv = self.get_grid_size(TrainField::SkewInvWeights);
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

pub fn draw_backtest_train_config_screen(
    f: &mut Frame,
    screen: &BacktestTrainConfigScreen,
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

fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestTrainConfigScreen) {
    let fields = TrainField::fields_in_group(screen.current_group);
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
    field: TrainField,
    screen: &BacktestTrainConfigScreen,
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

fn draw_preview_and_status(f: &mut Frame, area: Rect, screen: &BacktestTrainConfigScreen) {
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
        let screen = BacktestTrainConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_train_field_all() {
        let fields = TrainField::all();
        assert_eq!(fields.len(), 15);
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestTrainConfigScreen::new();
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TrainField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TrainField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        for field in [TrainField::SpreadIntercepts, TrainField::SpreadEntropyWeights,
                     TrainField::SpreadVolWeights, TrainField::SkewIntercepts,
                     TrainField::SkewInvWeights] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }
}
