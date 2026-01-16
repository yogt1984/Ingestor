//! Validate Run Config Screen (T-2.11)
//!
//! TUI screen for configuring validate run command parameters.

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

use crate::commands::params::validate_params::{RunParams, RunParamsBuilder};
use crate::core::ValidationStageType;
use crate::ui::widgets::{
    TextInputWidget, NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidateRunField {
    Config,
    FromResearch,
    Stages,
    FromStage,
    Data,
    Results,
    Preset,
    Name,
    Quiet,
    Json,
    Output,
    ContinueOnFailure,
    NoPersist,
}

impl ValidateRunField {
    pub fn all() -> Vec<Self> {
        vec![
            Self::Config,
            Self::FromResearch,
            Self::Stages,
            Self::FromStage,
            Self::Data,
            Self::Results,
            Self::Preset,
            Self::Name,
            Self::Quiet,
            Self::Json,
            Self::Output,
            Self::ContinueOnFailure,
            Self::NoPersist,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Config => "Config File",
            Self::FromResearch => "From Research Path",
            Self::Stages => "Stages (comma-separated)",
            Self::FromStage => "From Stage",
            Self::Data => "Data Directory",
            Self::Results => "Results Directory",
            Self::Preset => "Preset Name",
            Self::Name => "Run Name",
            Self::Quiet => "Quiet Mode",
            Self::Json => "JSON Output",
            Self::Output => "Output File",
            Self::ContinueOnFailure => "Continue On Failure",
            Self::NoPersist => "No Persist",
        }
    }

    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::Config | Self::FromResearch | Self::Stages | Self::FromStage |
            Self::Data | Self::Results | Self::Preset | Self::Name => ParameterGroup::Basic,
            Self::Quiet | Self::Json | Self::Output | Self::ContinueOnFailure |
            Self::NoPersist => ParameterGroup::Parameters,
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
pub struct ValidateRunConfigScreen {
    pub current_group: ParameterGroup,
    pub selected_field_index: usize,
    pub widgets: HashMap<ValidateRunField, FieldWidget>,
    pub validation_errors: HashMap<ValidateRunField, String>,
}

impl Default for ValidateRunConfigScreen {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub enum FieldWidget {
    TextInput(TextInputWidget),
    Toggle(ToggleWidget),
    PathInput(PathInputWidget),
    Dropdown(DropdownWidget<String>),
}

impl ValidateRunConfigScreen {
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
        self.widgets.insert(ValidateRunField::Config, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to config file (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::FromResearch, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to research state (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Stages, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Comma-separated stages (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::FromStage, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["Backtest".to_string(), "Forward".to_string(), "OutOfSample".to_string(), "Paper".to_string(), "Live".to_string()])
                .with_placeholder("Select starting stage (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Data, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Results, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to results directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Preset, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Preset name (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Name, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Run name prefix...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Quiet, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Quiet mode")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Json, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Output as JSON")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::ContinueOnFailure, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Continue on failure")
                .set_focused(false)
        ));
        
        self.widgets.insert(ValidateRunField::NoPersist, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Disable persistence")
                .set_focused(false)
        ));
    }

    pub fn current_field(&self) -> Option<ValidateRunField> {
        let fields = ValidateRunField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    pub fn get_widget(&self, field: ValidateRunField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    pub fn get_widget_mut(&mut self, field: ValidateRunField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    pub fn next_field(&mut self) {
        let fields = ValidateRunField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    pub fn prev_field(&mut self) {
        let fields = ValidateRunField::fields_in_group(self.current_group);
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

    pub fn build_params(&self) -> Result<RunParams> {
        let mut builder = RunParamsBuilder::new();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::Config) {
            if !w.path().is_empty() {
                builder = builder.with_config(Some(PathBuf::from(w.path())));
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::FromResearch) {
            if !w.path().is_empty() {
                builder = builder.with_from_research(Some(PathBuf::from(w.path())));
            }
        }

        // Stages parsing - simplified, would need proper parsing in real implementation
        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ValidateRunField::Stages) {
            if !w.text().is_empty() {
                // Parse comma-separated stages - simplified for now
                let stages: Vec<ValidationStageType> = w.text()
                    .split(',')
                    .filter_map(|s| {
                        match s.trim() {
                            "Backtest" => Some(ValidationStageType::Backtest),
                            "Forward" => Some(ValidationStageType::Forward),
                            "OutOfSample" => Some(ValidationStageType::OutOfSample),
                            "Paper" => Some(ValidationStageType::Paper),
                            "Live" => Some(ValidationStageType::Live),
                            _ => None,
                        }
                    })
                    .collect();
                if !stages.is_empty() {
                    builder = builder.with_stages(Some(stages));
                }
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&ValidateRunField::FromStage) {
            if let Some(stage_str) = w.selected_option() {
                let stage = match stage_str.as_str() {
                    "Backtest" => Some(ValidationStageType::Backtest),
                    "Forward" => Some(ValidationStageType::Forward),
                    "OutOfSample" => Some(ValidationStageType::OutOfSample),
                    "Paper" => Some(ValidationStageType::Paper),
                    "Live" => Some(ValidationStageType::Live),
                    _ => None,
                };
                if let Some(s) = stage {
                    builder = builder.with_from_stage(Some(s));
                }
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::Data) {
            if !w.path().is_empty() {
                builder = builder.with_data(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::Results) {
            if !w.path().is_empty() {
                builder = builder.with_results(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ValidateRunField::Preset) {
            if !w.text().is_empty() {
                builder = builder.with_preset(Some(w.text().to_string()));
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ValidateRunField::Name) {
            builder = builder.with_name(w.text().to_string());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ValidateRunField::Quiet) {
            builder = builder.with_quiet(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ValidateRunField::Json) {
            builder = builder.with_json(w.value());
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::Output) {
            if !w.path().is_empty() {
                builder = builder.with_output(Some(PathBuf::from(w.path())));
            }
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ValidateRunField::ContinueOnFailure) {
            builder = builder.with_continue_on_failure(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&ValidateRunField::NoPersist) {
            builder = builder.with_no_persist(w.value());
        }

        builder.build()
    }

    pub fn validate(&mut self) {
        self.validation_errors.clear();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::Data) {
            if w.path().is_empty() {
                self.validation_errors.insert(ValidateRunField::Data, "Data directory is required".to_string());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&ValidateRunField::Results) {
            if w.path().is_empty() {
                self.validation_errors.insert(ValidateRunField::Results, "Results directory is required".to_string());
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&ValidateRunField::Name) {
            if w.text().is_empty() {
                self.validation_errors.insert(ValidateRunField::Name, "Run name is required".to_string());
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
            Self::Toggle(w) => w.handle_key(key),
            Self::PathInput(w) => w.handle_key(key),
            Self::Dropdown(w) => w.handle_key(key),
        }
    }
}

pub fn draw_validate_run_config_screen(
    f: &mut Frame,
    screen: &ValidateRunConfigScreen,
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

fn draw_content_area(f: &mut Frame, area: Rect, screen: &ValidateRunConfigScreen) {
    let fields = ValidateRunField::fields_in_group(screen.current_group);
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
    field: ValidateRunField,
    screen: &ValidateRunConfigScreen,
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
        let screen = ValidateRunConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_validate_run_field_all() {
        let fields = ValidateRunField::all();
        assert_eq!(fields.len(), 13);
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = ValidateRunConfigScreen::new();
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(ValidateRunField::Data) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(ValidateRunField::Results) {
            let new_w = w.clone().with_path("/tmp/results");
            *w = new_w;
        }
        if let Some(FieldWidget::TextInput(ref mut w)) = screen.get_widget_mut(ValidateRunField::Name) {
            let new_w = w.clone().with_text("test-run");
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }
}
