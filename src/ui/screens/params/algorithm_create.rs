//! Algorithm Create Config Screen (T-2.11)
//!
//! TUI screen for configuring algorithm create command parameters.

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

use crate::commands::params::algorithm_params::{CreateParams, CreateParamsBuilder};
use crate::core::StrategyType;
use crate::ui::widgets::{
    TextInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlgorithmCreateField {
    Research,
    Output,
    Symbol,
    Name,
    Strategy,
    Validate,
    Data,
    Stages,
    DryRun,
}

impl AlgorithmCreateField {
    pub fn all() -> Vec<Self> {
        vec![
            Self::Research,
            Self::Output,
            Self::Symbol,
            Self::Name,
            Self::Strategy,
            Self::Validate,
            Self::Data,
            Self::Stages,
            Self::DryRun,
        ]
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Research => "Research Directory",
            Self::Output => "Output Directory",
            Self::Symbol => "Symbol",
            Self::Name => "Name (optional)",
            Self::Strategy => "Strategy Type",
            Self::Validate => "Run Validation",
            Self::Data => "Data Directory",
            Self::Stages => "Validation Stages",
            Self::DryRun => "Dry Run",
        }
    }

    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::Research | Self::Output | Self::Symbol | Self::Name | Self::Strategy => ParameterGroup::Basic,
            Self::Validate | Self::Data | Self::Stages | Self::DryRun => ParameterGroup::Parameters,
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
pub struct AlgorithmCreateConfigScreen {
    pub current_group: ParameterGroup,
    pub selected_field_index: usize,
    pub widgets: HashMap<AlgorithmCreateField, FieldWidget>,
    pub validation_errors: HashMap<AlgorithmCreateField, String>,
}

impl Default for AlgorithmCreateConfigScreen {
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

impl AlgorithmCreateConfigScreen {
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
        self.widgets.insert(AlgorithmCreateField::Research, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to research directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to output directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Symbol, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Trading symbol (e.g., BTCUSDT)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Name, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Custom name (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Strategy, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["Momentum".to_string(), "MarketMaking".to_string(), "Hybrid".to_string()])
                .with_placeholder("Select strategy type (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Validate, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Run validation pipeline")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Data, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory (if validate)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::Stages, FieldWidget::TextInput(
            TextInputWidget::new()
                .with_placeholder("Comma-separated validation stages...")
                .set_focused(false)
        ));
        
        self.widgets.insert(AlgorithmCreateField::DryRun, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Dry run (show without saving)")
                .set_focused(false)
        ));
    }

    pub fn current_field(&self) -> Option<AlgorithmCreateField> {
        let fields = AlgorithmCreateField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    pub fn get_widget(&self, field: AlgorithmCreateField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    pub fn get_widget_mut(&mut self, field: AlgorithmCreateField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    pub fn next_field(&mut self) {
        let fields = AlgorithmCreateField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    pub fn prev_field(&mut self) {
        let fields = AlgorithmCreateField::fields_in_group(self.current_group);
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

    pub fn build_params(&self) -> Result<CreateParams> {
        let mut builder = CreateParamsBuilder::new();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&AlgorithmCreateField::Research) {
            if !w.path().is_empty() {
                builder = builder.with_research(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&AlgorithmCreateField::Output) {
            if !w.path().is_empty() {
                builder = builder.with_output(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&AlgorithmCreateField::Symbol) {
            if !w.text().is_empty() {
                builder = builder.with_symbol(w.text().to_string());
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&AlgorithmCreateField::Name) {
            if !w.text().is_empty() {
                builder = builder.with_name(Some(w.text().to_string()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&AlgorithmCreateField::Strategy) {
            if let Some(strategy_str) = w.selected_option() {
                let strategy = match strategy_str.as_str() {
                    "Momentum" => Some(StrategyType::Momentum),
                    "MarketMaking" => Some(StrategyType::MarketMaking),
                    "Hybrid" => Some(StrategyType::Hybrid),
                    _ => None,
                };
                if let Some(s) = strategy {
                    builder = builder.with_strategy(Some(s));
                }
            }
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&AlgorithmCreateField::Validate) {
            builder = builder.with_validate(w.value());
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&AlgorithmCreateField::Data) {
            if !w.path().is_empty() {
                builder = builder.with_data(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&AlgorithmCreateField::Stages) {
            if !w.text().is_empty() {
                builder = builder.with_stages(w.text().to_string());
            }
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&AlgorithmCreateField::DryRun) {
            builder = builder.with_dry_run(w.value());
        }

        builder.build()
    }

    pub fn validate(&mut self) {
        self.validation_errors.clear();

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&AlgorithmCreateField::Research) {
            if w.path().is_empty() {
                self.validation_errors.insert(AlgorithmCreateField::Research, "Research directory is required".to_string());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&AlgorithmCreateField::Output) {
            if w.path().is_empty() {
                self.validation_errors.insert(AlgorithmCreateField::Output, "Output directory is required".to_string());
            }
        }

        if let Some(FieldWidget::TextInput(w)) = self.widgets.get(&AlgorithmCreateField::Symbol) {
            if w.text().is_empty() {
                self.validation_errors.insert(AlgorithmCreateField::Symbol, "Symbol is required".to_string());
            }
        }

        // If validate is enabled, data is required
        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&AlgorithmCreateField::Validate) {
            if w.value() {
                if let Some(FieldWidget::PathInput(data_w)) = self.widgets.get(&AlgorithmCreateField::Data) {
                    if data_w.path().is_empty() {
                        self.validation_errors.insert(AlgorithmCreateField::Data, "Data directory is required when validation is enabled".to_string());
                    }
                }
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

pub fn draw_algorithm_create_config_screen(
    f: &mut Frame,
    screen: &AlgorithmCreateConfigScreen,
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

fn draw_content_area(f: &mut Frame, area: Rect, screen: &AlgorithmCreateConfigScreen) {
    let fields = AlgorithmCreateField::fields_in_group(screen.current_group);
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
    field: AlgorithmCreateField,
    screen: &AlgorithmCreateConfigScreen,
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
        let screen = AlgorithmCreateConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_algorithm_create_field_all() {
        let fields = AlgorithmCreateField::all();
        assert_eq!(fields.len(), 9);
    }

    #[test]
    fn test_build_params_with_required() {
        use std::fs;
        let mut screen = AlgorithmCreateConfigScreen::new();
        
        // Create temporary directories for testing
        let temp_dir = std::env::temp_dir();
        let research_dir = temp_dir.join("test_research");
        let output_dir = temp_dir.join("test_output");
        let data_dir = temp_dir.join("test_data");
        
        let _ = fs::create_dir_all(&research_dir);
        let _ = fs::create_dir_all(&output_dir);
        let _ = fs::create_dir_all(&data_dir);
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(AlgorithmCreateField::Research) {
            let new_w = w.clone().with_path(research_dir.to_string_lossy().to_string());
            *w = new_w;
        }
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(AlgorithmCreateField::Output) {
            let new_w = w.clone().with_path(output_dir.to_string_lossy().to_string());
            *w = new_w;
        }
        if let Some(FieldWidget::TextInput(ref mut w)) = screen.get_widget_mut(AlgorithmCreateField::Symbol) {
            let new_w = w.clone().with_text("BTCUSDT");
            *w = new_w;
        }
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(AlgorithmCreateField::Data) {
            let new_w = w.clone().with_path(data_dir.to_string_lossy().to_string());
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok(), "build_params failed: {:?}", result);
        
        // Cleanup
        let _ = fs::remove_dir_all(&research_dir);
        let _ = fs::remove_dir_all(&output_dir);
        let _ = fs::remove_dir_all(&data_dir);
    }
}
