//! Research Run Config Screen (T-2.11)
//!
//! TUI screen for configuring research run command parameters.

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

use crate::commands::params::research_params::{RunParams, RunParamsBuilder};
use crate::ui::widgets::{
    TextInputWidget, NumberInputWidget, ToggleWidget,
    PathInputWidget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResearchRunField {
    Data,
    Output,
    Symbol,
    Start,
    End,
    MinSamples,
    CheckpointInterval,
    Resume,
    Quiet,
    Json,
}

impl ResearchRunField {
    pub fn all() -> Vec<Self> {
        vec![
            Self::Data,
            Self::Output,
            Self::Symbol,
            Self::Start,
            Self::End,
            Self::MinSamples,
            Self::CheckpointInterval,
            Self::Resume,
            Self::Quiet,
            Self::Json,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Data => "Data Directory",
            Self::Output => "Output Directory",
            Self::Symbol => "Symbol",
            Self::Start => "Start Date (YYYY-MM-DD)",
            Self::End => "End Date (YYYY-MM-DD)",
            Self::MinSamples => "Min Samples",
            Self::CheckpointInterval => "Checkpoint Interval",
            Self::Resume => "Resume",
            Self::Quiet => "Quiet Mode",
            Self::Json => "JSON Output",
        }
    }

    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::Data | Self::Output | Self::Symbol | Self::Start | Self::End => ParameterGroup::Basic,
            Self::MinSamples | Self::CheckpointInterval | Self::Resume | Self::Quiet | Self::Json => ParameterGroup::Parameters,
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
}

impl ParameterGroup {
    pub fn all() -> Vec<Self> {
        vec![Self::Basic, Self::Parameters]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Basic => "Basic",
            Self::Parameters => "Parameters",
        }
    }

    pub fn next(&self) -> Self {
        match self {
            Self::Basic => Self::Parameters,
            Self::Parameters => Self::Basic,
        }
    }

    pub fn prev(&self) -> Self {
        match self {
            Self::Basic => Self::Parameters,
            Self::Parameters => Self::Basic,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResearchRunConfigScreen {
    pub current_group: ParameterGroup,
    pub selected_field_index: usize,
    pub widgets: HashMap<ResearchRunField, FieldWidget>,
    pub validation_errors: HashMap<ResearchRunField, String>,
}

impl Default for ResearchRunConfigScreen {
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
}

impl ResearchRunConfigScreen {
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
        self.widgets.insert(ResearchRunField::Data, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to output directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::Symbol, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Trading symbol (e.g., BTCUSDT)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::Start, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Start date YYYY-MM-DD (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::End, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("End date YYYY-MM-DD (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::MinSamples, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(1000.0)
                .with_min(1.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::CheckpointInterval, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(10000.0)
                .with_min(1.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::Resume, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Resume from previous state")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::Quiet, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Quiet mode")
                .set_focused(false)
        ));
        
        self.widgets.insert(ResearchRunField::Json, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Output as JSON")
                .set_focused(false)
        ));
    }

    pub fn current_field(&self) -> Option<ResearchRunField> {
        let fields = ResearchRunField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    pub fn get_widget(&self, field: ResearchRunField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    pub fn get_widget_mut(&mut self, field: ResearchRunField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    pub fn next_field(&mut self) {
        let fields = ResearchRunField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    pub fn prev_field(&mut self) {
        let fields = ResearchRunField::fields_in_group(self.current_group);
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

    pub fn build_params(&self) -> Result<RunParams> {
        let mut builder = RunParamsBuilder::new();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ResearchRunField::Data) {
            if !w.path().is_empty() {
                builder = builder.with_data(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ResearchRunField::Output) {
            if !w.path().is_empty() {
                builder = builder.with_output(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ResearchRunField::Symbol) {
            if !w.text().is_empty() {
                builder = builder.with_symbol(w.text().to_string());
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ResearchRunField::Start) {
            if !w.text().is_empty() {
                builder = builder.with_start(Some(w.text().to_string()));
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ResearchRunField::End) {
            if !w.text().is_empty() {
                builder = builder.with_end(Some(w.text().to_string()));
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&ResearchRunField::MinSamples) {
            builder = builder.with_min_samples(w.value() as usize);
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&ResearchRunField::CheckpointInterval) {
            builder = builder.with_checkpoint_interval(w.value() as usize);
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ResearchRunField::Resume) {
            builder = builder.with_resume(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ResearchRunField::Quiet) {
            builder = builder.with_quiet(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ResearchRunField::Json) {
            builder = builder.with_json(w.value());
        }

        builder.build()
    }

    pub fn validate(&mut self) {
        self.validation_errors.clear();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ResearchRunField::Data) {
            if w.path().is_empty() {
                self.validation_errors.insert(ResearchRunField::Data, "Data directory is required".to_string());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ResearchRunField::Output) {
            if w.path().is_empty() {
                self.validation_errors.insert(ResearchRunField::Output, "Output directory is required".to_string());
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ResearchRunField::Symbol) {
            if w.text().is_empty() {
                self.validation_errors.insert(ResearchRunField::Symbol, "Symbol is required".to_string());
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
        }
    }
}

pub fn draw_research_run_config_screen(
    f: &mut Frame,
    screen: &ResearchRunConfigScreen,
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
    })
    .style(Style::default().fg(Color::White))
    .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));

    f.render_widget(tabs, chunks[0]);
    draw_content_area(f, chunks[1], screen);
    draw_status(f, chunks[2]);
}

fn draw_content_area(f: &mut Frame, area: Rect, screen: &ResearchRunConfigScreen) {
    let fields = ResearchRunField::fields_in_group(screen.current_group);
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
    field: ResearchRunField,
    screen: &ResearchRunConfigScreen,
    selected: bool,
) {
    let label = field.label();
    let label_width = label.len().min(30);
    
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
        let screen = ResearchRunConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_research_run_field_all() {
        let fields = ResearchRunField::all();
        assert_eq!(fields.len(), 10);
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = ResearchRunConfigScreen::new();
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(ResearchRunField::Data) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(ResearchRunField::Output) {
            let new_w = w.clone().with_path("/tmp/output");
            *w = new_w;
        }
        if let Some(FieldWidget::TextInput(ref mut w)) = screen.get_widget_mut(ResearchRunField::Symbol) {
            let new_w = w.clone().with_text("BTCUSDT");
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }
}
