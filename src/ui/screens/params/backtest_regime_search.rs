//! Backtest Regime Search Config Screen (T-2.11)
//!
//! TUI screen for configuring backtest regime search command parameters (MM only).
//! Supports regime-based spread/skew grids for high/medium/low entropy regimes.

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

use crate::commands::params::backtest_params::{RegimeSearchParams, RegimeSearchParamsBuilder};
use crate::ui::widgets::{
    NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget, CommaListWidget,
};

// ============================================================================
// Types
// ============================================================================

/// Field identifiers for navigation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegimeSearchField {
    // Basic Group
    DataPath,
    Algorithm,
    WeightsFile,
    
    // Regime Grids
    HighSpreads,
    MedSpreads,
    LowSpreads,
    HighSkews,
    MedSkews,
    LowSkews,
    FillProbs,
    
    // Other Parameters
    MaxInventory,
    QuoteSize,
    FeeRate,
    NaiveFills,
    QueuePos,
    HighEntropy,
    LowEntropy,
    
    // Output
    Output,
}

impl RegimeSearchField {
    /// Get all fields in order
    pub fn all() -> Vec<Self> {
        vec![
            Self::DataPath,
            Self::Algorithm,
            Self::WeightsFile,
            Self::HighSpreads,
            Self::MedSpreads,
            Self::LowSpreads,
            Self::HighSkews,
            Self::MedSkews,
            Self::LowSkews,
            Self::FillProbs,
            Self::MaxInventory,
            Self::QuoteSize,
            Self::FeeRate,
            Self::NaiveFills,
            Self::QueuePos,
            Self::HighEntropy,
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
            Self::HighSpreads => "High Entropy Spreads",
            Self::MedSpreads => "Medium Entropy Spreads",
            Self::LowSpreads => "Low Entropy Spreads",
            Self::HighSkews => "High Entropy Skews",
            Self::MedSkews => "Medium Entropy Skews",
            Self::LowSkews => "Low Entropy Skews",
            Self::FillProbs => "Fill Probs (grid)",
            Self::MaxInventory => "Max Inventory",
            Self::QuoteSize => "Quote Size",
            Self::FeeRate => "Fee Rate",
            Self::NaiveFills => "Naive Fills",
            Self::QueuePos => "Queue Position",
            Self::HighEntropy => "High Entropy Threshold",
            Self::LowEntropy => "Low Entropy Threshold",
            Self::Output => "Output File",
        }
    }

    /// Get field group
    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::DataPath | Self::Algorithm | Self::WeightsFile => ParameterGroup::Basic,
            Self::HighSpreads | Self::MedSpreads | Self::LowSpreads |
            Self::HighSkews | Self::MedSkews | Self::LowSkews |
            Self::FillProbs | Self::MaxInventory | Self::QuoteSize |
            Self::FeeRate | Self::NaiveFills | Self::QueuePos |
            Self::HighEntropy | Self::LowEntropy => ParameterGroup::Parameters,
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

/// Screen state for backtest regime search config
#[derive(Debug, Clone)]
pub struct BacktestRegimeSearchConfigScreen {
    /// Current parameter group/tab
    pub current_group: ParameterGroup,
    /// Currently selected field index (within current group)
    pub selected_field_index: usize,
    /// Widgets for each field
    pub widgets: HashMap<RegimeSearchField, FieldWidget>,
    /// Current validation errors
    pub validation_errors: HashMap<RegimeSearchField, String>,
}

impl Default for BacktestRegimeSearchConfigScreen {
    fn default() -> Self {
        Self::new()
    }
}

/// Widget type for a field
#[derive(Debug, Clone)]
pub enum FieldWidget {
    NumberInput(NumberInputWidget),
    Toggle(ToggleWidget),
    PathInput(PathInputWidget),
    Dropdown(DropdownWidget<String>),
    CommaList(CommaListWidget),
}

impl BacktestRegimeSearchConfigScreen {
    /// Create a new screen with default values
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

    /// Initialize all widgets with default values
    fn initialize_widgets(&mut self) {
        // Basic group
        self.widgets.insert(RegimeSearchField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["as".to_string(), "ml".to_string(), "fixed".to_string()])
                .with_placeholder("Select MM algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::WeightsFile, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to weights file (optional)...")
                .set_focused(false)
        ));
        
        // Regime grids
        self.widgets.insert(RegimeSearchField::HighSpreads, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 1.0,2.0,3.0")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::MedSpreads, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.5,1.0,1.5")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::LowSpreads, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.1,0.2 or 'none'")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::HighSkews, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.3,0.5,0.7")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::MedSkews, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.3,0.5,0.7")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::LowSkews, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.3,0.5,0.7")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::FillProbs, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.05,0.10,0.15")
                .set_focused(false)
        ));
        
        // Other parameters
        self.widgets.insert(RegimeSearchField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::HighEntropy, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.7)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(RegimeSearchField::LowEntropy, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.4)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        // Output
        self.widgets.insert(RegimeSearchField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
    }

    /// Get current field
    pub fn current_field(&self) -> Option<RegimeSearchField> {
        let fields = RegimeSearchField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    /// Get widget for a field
    pub fn get_widget(&self, field: RegimeSearchField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    /// Get mutable widget for a field
    pub fn get_widget_mut(&mut self, field: RegimeSearchField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    /// Navigate to next field
    pub fn next_field(&mut self) {
        let fields = RegimeSearchField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    /// Navigate to previous field
    pub fn prev_field(&mut self) {
        let fields = RegimeSearchField::fields_in_group(self.current_group);
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
                        return widget.handle_key(key);
                    }
                }
                false
            }
        }
    }

    /// Build RegimeSearchParams from current widget values
    pub fn build_params(&self) -> Result<RegimeSearchParams> {
        let mut builder = RegimeSearchParamsBuilder::new();

        // Basic group
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&RegimeSearchField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&RegimeSearchField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&RegimeSearchField::WeightsFile) {
            if !w.path().is_empty() {
                builder = builder.weights_file(Some(PathBuf::from(w.path())));
            }
        }

        // Regime grids
        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::HighSpreads) {
            let spreads_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.high_spreads(spreads_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::MedSpreads) {
            let spreads_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.med_spreads(spreads_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::LowSpreads) {
            let spreads_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.low_spreads(spreads_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::HighSkews) {
            let skews_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.high_skews(skews_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::MedSkews) {
            let skews_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.med_skews(skews_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::LowSkews) {
            let skews_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.low_skews(skews_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&RegimeSearchField::FillProbs) {
            let fill_probs_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.fill_probs(fill_probs_str);
        }

        // Other parameters
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&RegimeSearchField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::HighEntropy) {
            builder = builder.high_entropy(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::LowEntropy) {
            builder = builder.low_entropy(w.value());
        }

        // Output
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&RegimeSearchField::Output) {
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
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&RegimeSearchField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(
                    RegimeSearchField::DataPath,
                    "Data path is required".to_string(),
                );
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&RegimeSearchField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(
                    RegimeSearchField::Algorithm,
                    "Algorithm is required".to_string(),
                );
            } else if let Some(alg) = w.selected_option() {
                // Validate algorithm is MM type
                if !["as", "ml", "fixed"].contains(&alg.as_str()) {
                    self.validation_errors.insert(
                        RegimeSearchField::Algorithm,
                        "Algorithm must be MM type: as, ml, or fixed".to_string(),
                    );
                }
            }
        }

        // Validate grid parameters have at least one value
        for field in [RegimeSearchField::HighSpreads, RegimeSearchField::MedSpreads,
                     RegimeSearchField::LowSpreads, RegimeSearchField::HighSkews,
                     RegimeSearchField::MedSkews, RegimeSearchField::LowSkews,
                     RegimeSearchField::FillProbs] {
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
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    RegimeSearchField::QueuePos,
                    "Queue position must be in range [0.0, 1.0]".to_string(),
                );
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::HighEntropy) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    RegimeSearchField::HighEntropy,
                    "High entropy threshold must be in range [0.0, 1.0]".to_string(),
                );
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&RegimeSearchField::LowEntropy) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    RegimeSearchField::LowEntropy,
                    "Low entropy threshold must be in range [0.0, 1.0]".to_string(),
                );
            }
        }

        // Validate high_entropy > low_entropy
        if let (Some(FieldWidget::NumberInput(high)), Some(FieldWidget::NumberInput(low))) = 
            (self.widgets.get(&RegimeSearchField::HighEntropy),
             self.widgets.get(&RegimeSearchField::LowEntropy)) {
            if high.value() <= low.value() {
                self.validation_errors.insert(
                    RegimeSearchField::HighEntropy,
                    "High entropy threshold must be greater than low entropy threshold".to_string(),
                );
            }
        }
    }

    /// Check if all fields are valid
    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }
}

/// Handle key event for a field widget
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

/// Render the backtest regime search config screen
pub fn draw_backtest_regime_search_config_screen(
    f: &mut Frame,
    screen: &BacktestRegimeSearchConfigScreen,
) {
    let area = f.area();
    
    // Create layout: tabs at top, content in middle, status at bottom
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tabs
            Constraint::Min(10),   // Content
            Constraint::Length(2), // Status
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

    // Render status bar
    draw_status_bar(f, chunks[2]);
}

/// Draw content area with fields
fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestRegimeSearchConfigScreen) {
    let fields = RegimeSearchField::fields_in_group(screen.current_group);
    
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
    field: RegimeSearchField,
    screen: &BacktestRegimeSearchConfigScreen,
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

/// Draw status bar
fn draw_status_bar(f: &mut Frame, area: Rect) {
    let status_text = "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Esc: Cancel";
    let status_paragraph = Paragraph::new(status_text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status_paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    #[test]
    fn test_new_screen() {
        let screen = BacktestRegimeSearchConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        assert_eq!(screen.selected_field_index, 0);
    }

    #[test]
    fn test_regime_search_field_all() {
        let fields = RegimeSearchField::all();
        assert_eq!(fields.len(), 18);
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestRegimeSearchConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(RegimeSearchField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(RegimeSearchField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        for field in [RegimeSearchField::HighSpreads, RegimeSearchField::MedSpreads,
                     RegimeSearchField::LowSpreads, RegimeSearchField::HighSkews,
                     RegimeSearchField::MedSkews, RegimeSearchField::LowSkews,
                     RegimeSearchField::FillProbs] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_algorithm_mm_only() {
        let mut screen = BacktestRegimeSearchConfigScreen::new();
        
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(RegimeSearchField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(RegimeSearchField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        for field in [RegimeSearchField::HighSpreads, RegimeSearchField::MedSpreads,
                     RegimeSearchField::LowSpreads, RegimeSearchField::HighSkews,
                     RegimeSearchField::MedSkews, RegimeSearchField::LowSkews,
                     RegimeSearchField::FillProbs] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        screen.validate();
        assert!(screen.is_valid());
    }

    #[test]
    fn test_validate_entropy_thresholds() {
        let mut screen = BacktestRegimeSearchConfigScreen::new();
        
        // Set high_entropy <= low_entropy
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(RegimeSearchField::HighEntropy) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(RegimeSearchField::LowEntropy) {
            let new_w = w.clone().with_value(0.5);
            *w = new_w;
        }
        
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&RegimeSearchField::HighEntropy));
    }
}
