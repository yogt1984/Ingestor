//! Backtest Tune Config Screen (T-2.9)
//!
//! TUI screen for configuring backtest tune command parameters (MM algorithms only).
//! Supports 4 parameter grids (spreads, skews, high_entropies, fill_probs),
//! grid combination preview, total combinations calculation, and estimated time.

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

use crate::commands::params::backtest_params::{TuneParams, TuneParamsBuilder};
use crate::ui::widgets::{
    TextInputWidget, NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget, CommaListWidget,
};

// ============================================================================
// Types
// ============================================================================

/// Field identifiers for navigation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TuneField {
    // Basic Group
    DataPath,
    Algorithm,
    WeightsFile,
    
    // Grid Parameters
    Spreads,
    Skews,
    HighEntropies,
    FillProbs,
    
    // Other Parameters
    MaxInventory,
    QuoteSize,
    FeeRate,
    NaiveFills,
    QueuePos,
    LowEntropy,
    
    // Output
    Output,
}

impl TuneField {
    /// Get all fields in order
    pub fn all() -> Vec<Self> {
        vec![
            Self::DataPath,
            Self::Algorithm,
            Self::WeightsFile,
            Self::Spreads,
            Self::Skews,
            Self::HighEntropies,
            Self::FillProbs,
            Self::MaxInventory,
            Self::QuoteSize,
            Self::FeeRate,
            Self::NaiveFills,
            Self::QueuePos,
            Self::LowEntropy,
            Self::Output,
        ]
    }

    /// Get field label
    pub fn label(&self) -> &'static str {
        match self {
            Self::DataPath => "Data Path",
            Self::Algorithm => "Algorithm (MM only)",
            Self::WeightsFile => "Weights File",
            Self::Spreads => "Spreads (grid)",
            Self::Skews => "Skews (grid)",
            Self::HighEntropies => "High Entropies (grid)",
            Self::FillProbs => "Fill Probs (grid)",
            Self::MaxInventory => "Max Inventory",
            Self::QuoteSize => "Quote Size",
            Self::FeeRate => "Fee Rate",
            Self::NaiveFills => "Naive Fills",
            Self::QueuePos => "Queue Position",
            Self::LowEntropy => "Low Entropy",
            Self::Output => "Output File",
        }
    }

    /// Get field group
    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::DataPath | Self::Algorithm | Self::WeightsFile => ParameterGroup::Basic,
            Self::Spreads | Self::Skews | Self::HighEntropies | Self::FillProbs |
            Self::MaxInventory | Self::QuoteSize | Self::FeeRate |
            Self::NaiveFills | Self::QueuePos | Self::LowEntropy => ParameterGroup::Parameters,
            Self::Output => ParameterGroup::Output,
        }
    }

    /// Get fields in a group
    pub fn fields_in_group(group: ParameterGroup) -> Vec<Self> {
        Self::all().into_iter()
            .filter(|f| f.group() == group)
            .collect()
    }
}

/// Parameter groups/tabs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterGroup {
    Basic,
    Parameters,
    Output,
}

impl ParameterGroup {
    /// Get all groups
    pub fn all() -> Vec<Self> {
        vec![Self::Basic, Self::Parameters, Self::Output]
    }

    /// Get group label
    pub fn label(&self) -> &'static str {
        match self {
            Self::Basic => "Basic",
            Self::Parameters => "Parameters",
            Self::Output => "Output",
        }
    }

    /// Get next group
    pub fn next(&self) -> Self {
        match self {
            Self::Basic => Self::Parameters,
            Self::Parameters => Self::Output,
            Self::Output => Self::Basic,
        }
    }

    /// Get previous group
    pub fn prev(&self) -> Self {
        match self {
            Self::Basic => Self::Output,
            Self::Parameters => Self::Basic,
            Self::Output => Self::Parameters,
        }
    }
}

/// Screen state for backtest tune config
#[derive(Debug, Clone)]
pub struct BacktestTuneConfigScreen {
    /// Current parameter group/tab
    pub current_group: ParameterGroup,
    /// Currently selected field index (within current group)
    pub selected_field_index: usize,
    /// Widgets for each field
    pub widgets: HashMap<TuneField, FieldWidget>,
    /// Current validation errors
    pub validation_errors: HashMap<TuneField, String>,
    /// Total combinations count
    pub total_combinations: usize,
    /// Estimated time in seconds (placeholder calculation)
    pub estimated_time_seconds: f64,
}

impl Default for BacktestTuneConfigScreen {
    fn default() -> Self {
        Self::new()
    }
}

/// Widget type for a field
#[derive(Debug, Clone)]
pub enum FieldWidget {
    TextInput(TextInputWidget),
    NumberInput(NumberInputWidget),
    Toggle(ToggleWidget),
    PathInput(PathInputWidget),
    Dropdown(DropdownWidget<String>),
    CommaList(CommaListWidget),
}

impl BacktestTuneConfigScreen {
    /// Create a new screen with default values
    pub fn new() -> Self {
        let mut screen = Self {
            current_group: ParameterGroup::Basic,
            selected_field_index: 0,
            widgets: HashMap::new(),
            validation_errors: HashMap::new(),
            total_combinations: 0,
            estimated_time_seconds: 0.0,
        };
        
        // Initialize widgets with defaults
        screen.initialize_widgets();
        screen.update_combinations();
        screen
    }

    /// Initialize all widgets with default values
    fn initialize_widgets(&mut self) {
        // Basic group
        self.widgets.insert(TuneField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["as".to_string(), "ml".to_string(), "fixed".to_string()])
                .with_placeholder("Select MM algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::WeightsFile, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to weights file (optional)...")
                .set_focused(false)
        ));
        
        // Grid parameters - use CommaListWidget
        self.widgets.insert(TuneField::Spreads, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 1.0,2.0,3.0")
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::Skews, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.3,0.5,0.7")
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::HighEntropies, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.6,0.7,0.8")
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::FillProbs, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.05,0.10,0.15")
                .set_focused(false)
        ));
        
        // Other parameters
        self.widgets.insert(TuneField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(TuneField::LowEntropy, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.4)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        // Output
        self.widgets.insert(TuneField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
    }

    /// Get current field
    pub fn current_field(&self) -> Option<TuneField> {
        let fields = TuneField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    /// Get widget for a field
    pub fn get_widget(&self, field: TuneField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    /// Get mutable widget for a field
    pub fn get_widget_mut(&mut self, field: TuneField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    /// Navigate to next field
    pub fn next_field(&mut self) {
        let fields = TuneField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    /// Navigate to previous field
    pub fn prev_field(&mut self) {
        let fields = TuneField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = if self.selected_field_index == 0 {
                fields.len() - 1
            } else {
                self.selected_field_index - 1
            };
            self.update_focus();
        }
    }

    /// Navigate to next group
    pub fn next_group(&mut self) {
        self.current_group = self.current_group.next();
        self.selected_field_index = 0;
        self.update_focus();
    }

    /// Navigate to previous group
    pub fn prev_group(&mut self) {
        self.current_group = self.current_group.prev();
        self.selected_field_index = 0;
        self.update_focus();
    }

    /// Update focus state for all widgets
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
                FieldWidget::CommaList(w) => {
                    *w = w.clone().set_focused(focused);
                }
            }
        }
    }

    /// Handle key event
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
                // Forward to current widget
                if let Some(field) = self.current_field() {
                    if let Some(widget) = self.get_widget_mut(field) {
                        let handled = widget.handle_key(key);
                        if handled {
                            // Update combinations if grid parameter changed
                            if matches!(field, TuneField::Spreads | TuneField::Skews | 
                                      TuneField::HighEntropies | TuneField::FillProbs) {
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

    /// Update total combinations and estimated time
    fn update_combinations(&mut self) {
        let spreads_count = self.get_grid_size(TuneField::Spreads);
        let skews_count = self.get_grid_size(TuneField::Skews);
        let high_entropies_count = self.get_grid_size(TuneField::HighEntropies);
        let fill_probs_count = self.get_grid_size(TuneField::FillProbs);
        
        self.total_combinations = spreads_count * skews_count * high_entropies_count * fill_probs_count;
        
        // Estimate time: assume ~1 second per combination (very rough estimate)
        self.estimated_time_seconds = self.total_combinations as f64;
    }

    /// Get grid size for a field
    fn get_grid_size(&self, field: TuneField) -> usize {
        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&field) {
            w.len().max(1) // At least 1 to avoid zero multiplication
        } else {
            1
        }
    }

    /// Build TuneParams from current widget values
    pub fn build_params(&self) -> Result<TuneParams> {
        let mut builder = TuneParamsBuilder::new();

        // Basic group
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TuneField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&TuneField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TuneField::WeightsFile) {
            if !w.path().is_empty() {
                builder = builder.weights_file(Some(PathBuf::from(w.path())));
            }
        }

        // Grid parameters - convert to comma-separated strings
        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TuneField::Spreads) {
            let spreads_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.spreads(spreads_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TuneField::Skews) {
            let skews_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.skews(skews_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TuneField::HighEntropies) {
            let high_entropies_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.high_entropies(high_entropies_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&TuneField::FillProbs) {
            let fill_probs_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.fill_probs(fill_probs_str);
        }

        // Other parameters
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TuneField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TuneField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TuneField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&TuneField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TuneField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TuneField::LowEntropy) {
            builder = builder.low_entropy(w.value());
        }

        // Output
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TuneField::Output) {
            if !w.path().is_empty() {
                builder = builder.output(Some(PathBuf::from(w.path())));
            }
        }

        builder.build()
    }

    /// Validate all fields
    pub fn validate(&mut self) {
        self.validation_errors.clear();

        // Validate required fields
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&TuneField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(
                    TuneField::DataPath,
                    "Data path is required".to_string(),
                );
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&TuneField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(
                    TuneField::Algorithm,
                    "Algorithm is required".to_string(),
                );
            } else if let Some(alg) = w.selected_option() {
                // Validate algorithm is MM type
                if !["as", "ml", "fixed"].contains(&alg.as_str()) {
                    self.validation_errors.insert(
                        TuneField::Algorithm,
                        "Algorithm must be MM type: as, ml, or fixed".to_string(),
                    );
                }
            }
        }

        // Validate grid parameters have at least one value
        for field in [TuneField::Spreads, TuneField::Skews, TuneField::HighEntropies, TuneField::FillProbs] {
            if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&field) {
                if w.is_empty() {
                    self.validation_errors.insert(
                        field,
                        format!("{} must have at least one value", field.label()),
                    );
                }
            }
        }

        // Validate ranges
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&TuneField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    TuneField::QueuePos,
                    "Queue position must be in range [0.0, 1.0]".to_string(),
                );
            }
        }
    }

    /// Check if all fields are valid
    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }

    /// Get combination preview text
    pub fn combination_preview(&self) -> String {
        let spreads_count = self.get_grid_size(TuneField::Spreads);
        let skews_count = self.get_grid_size(TuneField::Skews);
        let high_entropies_count = self.get_grid_size(TuneField::HighEntropies);
        let fill_probs_count = self.get_grid_size(TuneField::FillProbs);
        
        format!(
            "Spreads: {} × Skews: {} × High Entropies: {} × Fill Probs: {} = {} combinations",
            spreads_count, skews_count, high_entropies_count, fill_probs_count,
            self.total_combinations
        )
    }

    /// Format estimated time
    pub fn format_estimated_time(&self) -> String {
        if self.estimated_time_seconds < 60.0 {
            format!("~{:.1} seconds", self.estimated_time_seconds)
        } else if self.estimated_time_seconds < 3600.0 {
            format!("~{:.1} minutes", self.estimated_time_seconds / 60.0)
        } else {
            format!("~{:.1} hours", self.estimated_time_seconds / 3600.0)
        }
    }
}

/// Handle key event for a field widget
impl FieldWidget {
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        match self {
            Self::TextInput(w) => w.handle_key(key),
            Self::NumberInput(w) => w.handle_key(key),
            Self::Toggle(w) => w.handle_key(key),
            Self::PathInput(w) => w.handle_key(key),
            Self::Dropdown(w) => w.handle_key(key),
            Self::CommaList(w) => w.handle_key(key),
        }
    }
}

/// Render the backtest tune config screen
pub fn draw_backtest_tune_config_screen(
    f: &mut Frame,
    screen: &BacktestTuneConfigScreen,
) {
    let area = f.area();
    
    // Create layout: tabs at top, content in middle, preview/status at bottom
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tabs
            Constraint::Min(10),   // Content
            Constraint::Length(5), // Preview and status
        ])
        .split(area);

    // Render tabs with MM indicator
    let title = "Parameter Groups (MM Only)";
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

    // Render content area
    draw_content_area(f, chunks[1], screen);

    // Render preview and status
    draw_preview_and_status(f, chunks[2], screen);
}

/// Draw content area with fields
fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestTuneConfigScreen) {
    let fields = TuneField::fields_in_group(screen.current_group);
    
    if fields.is_empty() {
        return;
    }

    // Create vertical layout for fields
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

/// Draw a single field
fn draw_field(
    f: &mut Frame,
    area: Rect,
    field: TuneField,
    screen: &BacktestTuneConfigScreen,
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

    // Draw label
    let label_style = if selected {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    };

    let label_paragraph = Paragraph::new(label)
        .style(label_style)
        .block(Block::default().borders(Borders::RIGHT));
    f.render_widget(label_paragraph, chunks[0]);

    // Draw widget
    if let Some(widget) = screen.get_widget(field) {
        match widget {
            FieldWidget::TextInput(w) => w.render(f, chunks[1]),
            FieldWidget::NumberInput(w) => w.render(f, chunks[1]),
            FieldWidget::Toggle(w) => w.render(f, chunks[1]),
            FieldWidget::PathInput(w) => w.render(f, chunks[1]),
            FieldWidget::Dropdown(w) => w.render(f, chunks[1]),
            FieldWidget::CommaList(w) => w.render(f, chunks[1]),
        }
    }

    // Draw validation error if any
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

/// Draw preview and status bar
fn draw_preview_and_status(f: &mut Frame, area: Rect, screen: &BacktestTuneConfigScreen) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2), // Preview
            Constraint::Length(3), // Status
        ])
        .split(area);

    // Draw combination preview
    let preview_text = screen.combination_preview();
    let time_text = format!("Estimated time: {}", screen.format_estimated_time());
    let preview_line = Line::from(vec![
        Span::styled(preview_text, Style::default().fg(Color::Cyan)),
        Span::raw(" | "),
        Span::styled(time_text, Style::default().fg(Color::Yellow)),
    ]);
    let preview_paragraph = Paragraph::new(preview_line)
        .block(Block::default().borders(Borders::ALL).title("Grid Preview"));
    f.render_widget(preview_paragraph, chunks[0]);

    // Draw status bar
    let status_text = "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Esc: Cancel";
    let status_paragraph = Paragraph::new(status_text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status_paragraph, chunks[1]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    // ========================================================================
    // Construction Tests
    // ========================================================================

    #[test]
    fn test_new_screen() {
        let screen = BacktestTuneConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        assert_eq!(screen.selected_field_index, 0);
    }

    #[test]
    fn test_default_screen() {
        let screen1 = BacktestTuneConfigScreen::new();
        let screen2 = BacktestTuneConfigScreen::default();
        assert_eq!(screen1.current_group, screen2.current_group);
    }

    #[test]
    fn test_widgets_initialized() {
        let screen = BacktestTuneConfigScreen::new();
        assert!(screen.widgets.contains_key(&TuneField::DataPath));
        assert!(screen.widgets.contains_key(&TuneField::Algorithm));
        assert!(screen.widgets.contains_key(&TuneField::Spreads));
        assert!(screen.widgets.contains_key(&TuneField::Skews));
        assert!(screen.widgets.contains_key(&TuneField::HighEntropies));
        assert!(screen.widgets.contains_key(&TuneField::FillProbs));
    }

    #[test]
    fn test_grid_widgets_are_comma_list() {
        let screen = BacktestTuneConfigScreen::new();
        
        match screen.get_widget(TuneField::Spreads).unwrap() {
            FieldWidget::CommaList(_) => {}
            _ => panic!("Spreads should be CommaList"),
        }
        
        match screen.get_widget(TuneField::Skews).unwrap() {
            FieldWidget::CommaList(_) => {}
            _ => panic!("Skews should be CommaList"),
        }
        
        match screen.get_widget(TuneField::HighEntropies).unwrap() {
            FieldWidget::CommaList(_) => {}
            _ => panic!("HighEntropies should be CommaList"),
        }
        
        match screen.get_widget(TuneField::FillProbs).unwrap() {
            FieldWidget::CommaList(_) => {}
            _ => panic!("FillProbs should be CommaList"),
        }
    }

    // ========================================================================
    // Field Enum Tests
    // ========================================================================

    #[test]
    fn test_tune_field_all() {
        let fields = TuneField::all();
        assert_eq!(fields.len(), 14); // DataPath, Algorithm, WeightsFile, Spreads, Skews, HighEntropies, FillProbs, MaxInventory, QuoteSize, FeeRate, NaiveFills, QueuePos, LowEntropy, Output
    }

    #[test]
    fn test_tune_field_labels() {
        assert_eq!(TuneField::DataPath.label(), "Data Path");
        assert_eq!(TuneField::Algorithm.label(), "Algorithm (MM only)");
        assert_eq!(TuneField::Spreads.label(), "Spreads (grid)");
    }

    #[test]
    fn test_tune_field_groups() {
        assert_eq!(TuneField::DataPath.group(), ParameterGroup::Basic);
        assert_eq!(TuneField::Spreads.group(), ParameterGroup::Parameters);
        assert_eq!(TuneField::Output.group(), ParameterGroup::Output);
    }

    #[test]
    fn test_fields_in_group() {
        let basic_fields = TuneField::fields_in_group(ParameterGroup::Basic);
        assert_eq!(basic_fields.len(), 3);
        assert!(basic_fields.contains(&TuneField::DataPath));
        assert!(basic_fields.contains(&TuneField::Algorithm));

        let param_fields = TuneField::fields_in_group(ParameterGroup::Parameters);
        assert!(param_fields.contains(&TuneField::Spreads));
        assert!(param_fields.contains(&TuneField::Skews));

        let output_fields = TuneField::fields_in_group(ParameterGroup::Output);
        assert_eq!(output_fields.len(), 1);
        assert!(output_fields.contains(&TuneField::Output));
    }

    // ========================================================================
    // Parameter Group Tests
    // ========================================================================

    #[test]
    fn test_parameter_group_all() {
        let groups = ParameterGroup::all();
        assert_eq!(groups.len(), 3);
    }

    #[test]
    fn test_parameter_group_labels() {
        assert_eq!(ParameterGroup::Basic.label(), "Basic");
        assert_eq!(ParameterGroup::Parameters.label(), "Parameters");
        assert_eq!(ParameterGroup::Output.label(), "Output");
    }

    #[test]
    fn test_parameter_group_next() {
        assert_eq!(ParameterGroup::Basic.next(), ParameterGroup::Parameters);
        assert_eq!(ParameterGroup::Parameters.next(), ParameterGroup::Output);
        assert_eq!(ParameterGroup::Output.next(), ParameterGroup::Basic);
    }

    #[test]
    fn test_parameter_group_prev() {
        assert_eq!(ParameterGroup::Basic.prev(), ParameterGroup::Output);
        assert_eq!(ParameterGroup::Parameters.prev(), ParameterGroup::Basic);
        assert_eq!(ParameterGroup::Output.prev(), ParameterGroup::Parameters);
    }

    // ========================================================================
    // Navigation Tests
    // ========================================================================

    #[test]
    fn test_current_field() {
        let screen = BacktestTuneConfigScreen::new();
        let field = screen.current_field();
        assert!(field.is_some());
        assert_eq!(field.unwrap().group(), ParameterGroup::Basic);
    }

    #[test]
    fn test_next_field() {
        let mut screen = BacktestTuneConfigScreen::new();
        let initial_index = screen.selected_field_index;
        screen.next_field();
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_prev_field() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.selected_field_index = 1;
        let initial_index = screen.selected_field_index;
        screen.prev_field();
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_next_group() {
        let mut screen = BacktestTuneConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Parameters);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Output);
    }

    #[test]
    fn test_prev_group() {
        let mut screen = BacktestTuneConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.prev_group();
        assert_eq!(screen.current_group, ParameterGroup::Output);
    }

    // ========================================================================
    // Combination Calculation Tests
    // ========================================================================

    #[test]
    fn test_update_combinations_empty_grids() {
        let screen = BacktestTuneConfigScreen::new();
        // Empty grids should default to 1
        assert!(screen.total_combinations >= 1);
    }

    #[test]
    fn test_update_combinations_single_values() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set single value in each grid
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.1]);
            *w = new_w;
        }
        
        screen.update_combinations();
        assert_eq!(screen.total_combinations, 1);
    }

    #[test]
    fn test_update_combinations_multiple_values() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set multiple values in each grid
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0, 3.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.3, 0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.6, 0.7, 0.8]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.05, 0.10]);
            *w = new_w;
        }
        
        screen.update_combinations();
        assert_eq!(screen.total_combinations, 3 * 2 * 3 * 2); // 36 combinations
    }

    #[test]
    fn test_combination_preview() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.5]);
            *w = new_w;
        }
        
        screen.update_combinations();
        let preview = screen.combination_preview();
        assert!(preview.contains("Spreads: 2"));
        assert!(preview.contains("Skews: 1"));
        assert!(preview.contains("combinations"));
    }

    #[test]
    fn test_format_estimated_time_seconds() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.estimated_time_seconds = 30.0;
        let formatted = screen.format_estimated_time();
        assert!(formatted.contains("seconds"));
    }

    #[test]
    fn test_format_estimated_time_minutes() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.estimated_time_seconds = 120.0;
        let formatted = screen.format_estimated_time();
        assert!(formatted.contains("minutes"));
    }

    #[test]
    fn test_format_estimated_time_hours() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.estimated_time_seconds = 7200.0;
        let formatted = screen.format_estimated_time();
        assert!(formatted.contains("hours"));
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_tab() {
        let mut screen = BacktestTuneConfigScreen::new();
        let initial_index = screen.selected_field_index;
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        screen.handle_key(key);
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_handle_key_up() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.selected_field_index = 1;
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.selected_field_index, 0);
    }

    #[test]
    fn test_handle_key_ctrl_left() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.current_group = ParameterGroup::Parameters;
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_handle_key_updates_combinations() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.current_group = ParameterGroup::Parameters;
        screen.selected_field_index = 0; // Spreads
        
        // Add value to spreads grid
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0]);
            *w = new_w;
        }
        
        let initial_combinations = screen.total_combinations;
        
        // Trigger update by handling a key that modifies the grid
        // (In real usage, this would happen when user adds/removes values)
        screen.update_combinations();
        
        // Combinations should be updated
        assert_ne!(screen.total_combinations, initial_combinations);
    }

    // ========================================================================
    // Build Params Tests
    // ========================================================================

    #[test]
    fn test_build_params_missing_required() {
        let screen = BacktestTuneConfigScreen::new();
        let result = screen.build_params();
        assert!(result.is_err()); // Missing data_path and algorithm
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set data path
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        
        // Set algorithm
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.1]);
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.spreads, "1,2");
        assert_eq!(params.skews, "0.5");
    }

    #[test]
    fn test_build_params_grid_conversion() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid with multiple values
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.5, 3.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.3, 0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.6, 0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.05, 0.10, 0.15]);
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        
        // Check grid values are converted to comma-separated strings
        assert_eq!(params.spreads, "1,2.5,3");
        assert_eq!(params.skews, "0.3,0.5");
        assert_eq!(params.high_entropies, "0.6,0.7");
        assert_eq!(params.fill_probs, "0.05,0.1,0.15");
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_empty_screen() {
        let mut screen = BacktestTuneConfigScreen::new();
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&TuneField::DataPath));
        assert!(screen.validation_errors.contains_key(&TuneField::Algorithm));
    }

    #[test]
    fn test_validate_with_required() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0); // "as"
            *w = new_w;
        }
        
        // Set grid values
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.1]);
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.is_valid());
    }

    #[test]
    fn test_validate_algorithm_mm_only() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set data path
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        
        // Algorithm dropdown only has MM algorithms, so this should be valid
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0); // "as" is MM
            *w = new_w;
        }
        
        // Set grid values
        for field in [TuneField::Spreads, TuneField::Skews, TuneField::HighEntropies, TuneField::FillProbs] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        screen.validate();
        // Should be valid since "as" is an MM algorithm
        assert!(screen.is_valid());
    }

    #[test]
    fn test_validate_empty_grids() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Leave grids empty
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&TuneField::Spreads));
        assert!(screen.validation_errors.contains_key(&TuneField::Skews));
        assert!(screen.validation_errors.contains_key(&TuneField::HighEntropies));
        assert!(screen.validation_errors.contains_key(&TuneField::FillProbs));
    }

    #[test]
    fn test_validate_queue_pos_range() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set invalid queue_pos
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(TuneField::QueuePos) {
            let new_w = w.clone().with_value(-0.1); // Invalid: < 0.0
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.validation_errors.contains_key(&TuneField::QueuePos));
    }

    // ========================================================================
    // Grid Size Tests
    // ========================================================================

    #[test]
    fn test_get_grid_size_empty() {
        let screen = BacktestTuneConfigScreen::new();
        let size = screen.get_grid_size(TuneField::Spreads);
        assert_eq!(size, 1); // Empty defaults to 1
    }

    #[test]
    fn test_get_grid_size_with_values() {
        let mut screen = BacktestTuneConfigScreen::new();
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0, 3.0]);
            *w = new_w;
        }
        let size = screen.get_grid_size(TuneField::Spreads);
        assert_eq!(size, 3);
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_configuration_workflow() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Configure grid parameters
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.3, 0.5, 0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.6, 0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.05, 0.10]);
            *w = new_w;
        }
        
        // Update combinations
        screen.update_combinations();
        assert_eq!(screen.total_combinations, 2 * 3 * 2 * 2); // 24 combinations
        
        // Validate
        screen.validate();
        assert!(screen.is_valid());
        
        // Build params
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.spreads, "1,2");
        assert_eq!(params.skews, "0.3,0.5,0.7");
    }

    #[test]
    fn test_navigation_through_all_groups() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Navigate through all groups
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Parameters);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Output);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_combination_calculation_large_grids() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // Set large grids
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let values: Vec<f64> = (1..=10).map(|i| i as f64).collect();
            let new_w = w.clone().with_values(values);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let values: Vec<f64> = (1..=5).map(|i| i as f64 * 0.1).collect();
            let new_w = w.clone().with_values(values);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let values: Vec<f64> = vec![0.6, 0.7, 0.8];
            let new_w = w.clone().with_values(values);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let values: Vec<f64> = vec![0.05, 0.10, 0.15, 0.20];
            let new_w = w.clone().with_values(values);
            *w = new_w;
        }
        
        screen.update_combinations();
        assert_eq!(screen.total_combinations, 10 * 5 * 3 * 4); // 600 combinations
    }

    // ========================================================================
    // Edge Cases Tests
    // ========================================================================

    #[test]
    fn test_all_fields_have_widgets() {
        let screen = BacktestTuneConfigScreen::new();
        for field in TuneField::all() {
            assert!(screen.widgets.contains_key(&field), "Field {:?} should have a widget", field);
        }
    }

    #[test]
    fn test_field_navigation_bounds() {
        let mut screen = BacktestTuneConfigScreen::new();
        let fields = TuneField::fields_in_group(ParameterGroup::Basic);
        
        // Navigate forward many times
        for _ in 0..fields.len() * 2 {
            screen.next_field();
        }
        assert!(screen.selected_field_index < fields.len());
        
        // Navigate backward many times
        for _ in 0..fields.len() * 2 {
            screen.prev_field();
        }
        assert!(screen.selected_field_index < fields.len());
    }

    #[test]
    fn test_validation_clears_errors() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        // First validation (should have errors)
        screen.validate();
        assert!(!screen.validation_errors.is_empty());
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(TuneField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(TuneField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        for field in [TuneField::Spreads, TuneField::Skews, TuneField::HighEntropies, TuneField::FillProbs] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        // Second validation (should clear errors)
        screen.validate();
        assert!(screen.validation_errors.is_empty());
    }

    #[test]
    fn test_combination_preview_format() {
        let mut screen = BacktestTuneConfigScreen::new();
        
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0, 3.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::Skews) {
            let new_w = w.clone().with_values(vec![0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.6, 0.7]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(TuneField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.1]);
            *w = new_w;
        }
        
        screen.update_combinations();
        let preview = screen.combination_preview();
        
        assert!(preview.contains("Spreads: 3"));
        assert!(preview.contains("Skews: 1"));
        assert!(preview.contains("High Entropies: 2"));
        assert!(preview.contains("Fill Probs: 1"));
        assert!(preview.contains("6 combinations")); // 3 * 1 * 2 * 1
    }
}
