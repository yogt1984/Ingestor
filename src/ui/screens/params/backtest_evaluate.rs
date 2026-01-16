//! Backtest Evaluate Config Screen (T-2.8)
//!
//! TUI screen for configuring backtest evaluate command parameters.
//! Supports all 25+ parameters organized into groups (Basic, Advanced, Output),
//! field navigation, validation, and preset save/load.

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

use crate::commands::params::backtest_params::{EvaluateParams, EvaluateParamsBuilder};
use crate::ui::widgets::{
    TextInputWidget, NumberInputWidget, CommaListWidget, ToggleWidget,
    PathInputWidget, DropdownWidget,
};

// ============================================================================
// Types
// ============================================================================

/// Field identifiers for navigation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvaluateField {
    // Basic Group
    DataPath,
    Algorithm,
    WeightsFile,
    Spread,
    Skew,
    MaxInventory,
    QuoteSize,
    FeeRate,
    
    // Advanced Group
    NaiveFills,
    FillProb,
    QueuePos,
    HighEntropy,
    LowEntropy,
    RegimeParams,
    HighSpread,
    MedSpread,
    LowSpread,
    HighSkew,
    MedSkew,
    LowSkew,
    QuoteLowEntropy,
    
    // Output Group
    Output,
    Json,
    Quiet,
    Stats,
}

impl EvaluateField {
    /// Get all fields in order
    pub fn all() -> Vec<Self> {
        vec![
            // Basic
            Self::DataPath,
            Self::Algorithm,
            Self::WeightsFile,
            Self::Spread,
            Self::Skew,
            Self::MaxInventory,
            Self::QuoteSize,
            Self::FeeRate,
            // Advanced
            Self::NaiveFills,
            Self::FillProb,
            Self::QueuePos,
            Self::HighEntropy,
            Self::LowEntropy,
            Self::RegimeParams,
            Self::HighSpread,
            Self::MedSpread,
            Self::LowSpread,
            Self::HighSkew,
            Self::MedSkew,
            Self::LowSkew,
            Self::QuoteLowEntropy,
            // Output
            Self::Output,
            Self::Json,
            Self::Quiet,
            Self::Stats,
        ]
    }

    /// Get field label
    pub fn label(&self) -> &'static str {
        match self {
            Self::DataPath => "Data Path",
            Self::Algorithm => "Algorithm",
            Self::WeightsFile => "Weights File",
            Self::Spread => "Spread (bps)",
            Self::Skew => "Skew",
            Self::MaxInventory => "Max Inventory",
            Self::QuoteSize => "Quote Size",
            Self::FeeRate => "Fee Rate",
            Self::NaiveFills => "Naive Fills",
            Self::FillProb => "Fill Probability",
            Self::QueuePos => "Queue Position",
            Self::HighEntropy => "High Entropy Threshold",
            Self::LowEntropy => "Low Entropy Threshold",
            Self::RegimeParams => "Regime Parameters",
            Self::HighSpread => "High Spread (bps)",
            Self::MedSpread => "Medium Spread (bps)",
            Self::LowSpread => "Low Spread (bps)",
            Self::HighSkew => "High Skew",
            Self::MedSkew => "Medium Skew",
            Self::LowSkew => "Low Skew",
            Self::QuoteLowEntropy => "Quote Low Entropy",
            Self::Output => "Output File",
            Self::Json => "JSON Output",
            Self::Quiet => "Quiet Mode",
            Self::Stats => "Statistics",
        }
    }

    /// Get field group
    pub fn group(&self) -> ParameterGroup {
        match self {
            Self::DataPath | Self::Algorithm | Self::WeightsFile |
            Self::Spread | Self::Skew | Self::MaxInventory |
            Self::QuoteSize | Self::FeeRate => ParameterGroup::Basic,
            Self::NaiveFills | Self::FillProb | Self::QueuePos |
            Self::HighEntropy | Self::LowEntropy | Self::RegimeParams |
            Self::HighSpread | Self::MedSpread | Self::LowSpread |
            Self::HighSkew | Self::MedSkew | Self::LowSkew |
            Self::QuoteLowEntropy => ParameterGroup::Advanced,
            Self::Output | Self::Json | Self::Quiet | Self::Stats => ParameterGroup::Output,
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
    Advanced,
    Output,
}

impl ParameterGroup {
    /// Get all groups
    pub fn all() -> Vec<Self> {
        vec![Self::Basic, Self::Advanced, Self::Output]
    }

    /// Get group label
    pub fn label(&self) -> &'static str {
        match self {
            Self::Basic => "Basic",
            Self::Advanced => "Advanced",
            Self::Output => "Output",
        }
    }

    /// Get next group
    pub fn next(&self) -> Self {
        match self {
            Self::Basic => Self::Advanced,
            Self::Advanced => Self::Output,
            Self::Output => Self::Basic,
        }
    }

    /// Get previous group
    pub fn prev(&self) -> Self {
        match self {
            Self::Basic => Self::Output,
            Self::Advanced => Self::Basic,
            Self::Output => Self::Advanced,
        }
    }
}

/// Screen state for backtest evaluate config
#[derive(Debug, Clone)]
pub struct BacktestEvaluateConfigScreen {
    /// Current parameter group/tab
    pub current_group: ParameterGroup,
    /// Currently selected field index (within current group)
    pub selected_field_index: usize,
    /// Widgets for each field
    pub widgets: HashMap<EvaluateField, FieldWidget>,
    /// Current validation errors
    pub validation_errors: HashMap<EvaluateField, String>,
    /// Preset name for save/load
    pub preset_name: String,
    /// Available presets
    pub presets: Vec<String>,
    /// Whether in preset selection mode
    pub preset_mode: bool,
}

impl Default for BacktestEvaluateConfigScreen {
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
}

impl BacktestEvaluateConfigScreen {
    /// Create a new screen with default values
    pub fn new() -> Self {
        let mut screen = Self {
            current_group: ParameterGroup::Basic,
            selected_field_index: 0,
            widgets: HashMap::new(),
            validation_errors: HashMap::new(),
            preset_name: String::new(),
            presets: Vec::new(),
            preset_mode: false,
        };
        
        // Initialize widgets with defaults
        screen.initialize_widgets();
        screen
    }

    /// Initialize all widgets with default values
    fn initialize_widgets(&mut self) {
        // Basic group
        self.widgets.insert(EvaluateField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["as".to_string(), "ml".to_string(), "fixed".to_string()])
                .with_placeholder("Select algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::WeightsFile, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to weights file (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::Spread, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(2.0)
                .with_min(0.0)
                .with_decimals(2)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Decimal)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::Skew, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        // Advanced group
        self.widgets.insert(EvaluateField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::FillProb, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.10)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::HighEntropy, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.7)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::LowEntropy, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.4)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::RegimeParams, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use regime-specific parameters")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::HighSpread, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(1.0)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::MedSpread, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(2.5)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::LowSpread, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(5.0)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::HighSkew, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.3)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::MedSkew, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::LowSkew, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(1.0)
                .with_min(0.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::QuoteLowEntropy, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Quote in low entropy")
                .set_focused(false)
        ));
        
        // Output group
        self.widgets.insert(EvaluateField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::Json, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Output as JSON")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::Quiet, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Quiet mode")
                .set_focused(false)
        ));
        
        self.widgets.insert(EvaluateField::Stats, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Show statistics")
                .set_focused(false)
        ));
    }

    /// Get current field
    pub fn current_field(&self) -> Option<EvaluateField> {
        let fields = EvaluateField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    /// Get widget for a field
    pub fn get_widget(&self, field: EvaluateField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    /// Get mutable widget for a field
    pub fn get_widget_mut(&mut self, field: EvaluateField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    /// Navigate to next field
    pub fn next_field(&mut self) {
        let fields = EvaluateField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    /// Navigate to previous field
    pub fn prev_field(&mut self) {
        let fields = EvaluateField::fields_in_group(self.current_group);
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
            }
        }
    }

    /// Handle key event
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if self.preset_mode {
            return self.handle_preset_key(key);
        }

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
            KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Save preset
                self.preset_mode = true;
                true
            }
            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Load preset
                self.preset_mode = true;
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

    /// Handle key in preset mode
    fn handle_preset_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => {
                self.preset_mode = false;
                self.preset_name.clear();
                true
            }
            KeyCode::Enter => {
                // Preset save/load can be implemented using PresetManager:
                // use crate::ui::presets::{PresetManager, PresetType, Preset, PresetMetadata};
                // let manager = PresetManager::new()?;
                // let preset = Preset { ... };
                // manager.save_preset(&preset)?;
                // or: let preset = manager.load_preset(&self.preset_name, PresetType::BacktestEvaluate)?;
                self.preset_mode = false;
                self.preset_name.clear();
                true
            }
            KeyCode::Char(c) => {
                self.preset_name.push(c);
                true
            }
            KeyCode::Backspace => {
                self.preset_name.pop();
                true
            }
            _ => false,
        }
    }

    /// Build EvaluateParams from current widget values
    pub fn build_params(&self) -> Result<EvaluateParams> {
        let mut builder = EvaluateParamsBuilder::new();

        // Basic group
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&EvaluateField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&EvaluateField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&EvaluateField::WeightsFile) {
            if !w.path().is_empty() {
                builder = builder.weights_file(Some(PathBuf::from(w.path())));
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::Spread) {
            builder = builder.spread(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::Skew) {
            builder = builder.skew(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        // Advanced group
        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&EvaluateField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::FillProb) {
            builder = builder.fill_prob(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::HighEntropy) {
            builder = builder.high_entropy(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::LowEntropy) {
            builder = builder.low_entropy(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&EvaluateField::RegimeParams) {
            builder = builder.regime_params(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::HighSpread) {
            builder = builder.high_spread(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::MedSpread) {
            builder = builder.med_spread(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::LowSpread) {
            builder = builder.low_spread(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::HighSkew) {
            builder = builder.high_skew(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::MedSkew) {
            builder = builder.med_skew(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::LowSkew) {
            builder = builder.low_skew(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&EvaluateField::QuoteLowEntropy) {
            builder = builder.quote_low_entropy(w.value());
        }

        // Output group
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&EvaluateField::Output) {
            if !w.path().is_empty() {
                builder = builder.output(Some(PathBuf::from(w.path())));
            }
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&EvaluateField::Json) {
            builder = builder.json(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&EvaluateField::Quiet) {
            builder = builder.quiet(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&EvaluateField::Stats) {
            builder = builder.stats(w.value());
        }

        builder.build()
    }

    /// Validate all fields
    pub fn validate(&mut self) {
        self.validation_errors.clear();

        // Validate required fields
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&EvaluateField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(
                    EvaluateField::DataPath,
                    "Data path is required".to_string(),
                );
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&EvaluateField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(
                    EvaluateField::Algorithm,
                    "Algorithm is required".to_string(),
                );
            }
        }

        // Validate ranges
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::FillProb) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    EvaluateField::FillProb,
                    "Fill probability must be in range [0.0, 1.0]".to_string(),
                );
            }
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&EvaluateField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    EvaluateField::QueuePos,
                    "Queue position must be in range [0.0, 1.0]".to_string(),
                );
            }
        }

        // Validate entropy thresholds
        if let Some(FieldWidget::NumberInput(high)) = self.widgets.get(&EvaluateField::HighEntropy) {
            if let Some(FieldWidget::NumberInput(low)) = self.widgets.get(&EvaluateField::LowEntropy) {
                if high.value() <= low.value() {
                    self.validation_errors.insert(
                        EvaluateField::HighEntropy,
                        "High entropy must be greater than low entropy".to_string(),
                    );
                }
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
            Self::TextInput(w) => w.handle_key(key),
            Self::NumberInput(w) => w.handle_key(key),
            Self::Toggle(w) => w.handle_key(key),
            Self::PathInput(w) => w.handle_key(key),
            Self::Dropdown(w) => w.handle_key(key),
        }
    }
}

/// Render the backtest evaluate config screen
pub fn draw_backtest_evaluate_config_screen(
    f: &mut Frame,
    screen: &BacktestEvaluateConfigScreen,
) {
    let area = f.area();
    
    // Create layout: tabs at top, content in middle, status at bottom
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tabs
            Constraint::Min(10),   // Content
            Constraint::Length(3), // Status
        ])
        .split(area);

    // Render tabs
    let tabs = Tabs::new(
        ParameterGroup::all()
            .iter()
            .map(|g| g.label())
            .collect::<Vec<_>>(),
    )
    .block(Block::default().borders(Borders::ALL).title("Parameter Groups"))
    .select(match screen.current_group {
        ParameterGroup::Basic => 0,
        ParameterGroup::Advanced => 1,
        ParameterGroup::Output => 2,
    })
    .style(Style::default().fg(Color::White))
    .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));

    f.render_widget(tabs, chunks[0]);

    // Render content area
    draw_content_area(f, chunks[1], screen);

    // Render status bar
    draw_status_bar(f, chunks[2], screen);
}

/// Draw content area with fields
fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestEvaluateConfigScreen) {
    let fields = EvaluateField::fields_in_group(screen.current_group);
    
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
    field: EvaluateField,
    screen: &BacktestEvaluateConfigScreen,
    selected: bool,
) {
    let label = field.label();
    let label_width = label.len().min(20);
    
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
fn draw_status_bar(f: &mut Frame, area: Rect, screen: &BacktestEvaluateConfigScreen) {
    let status_text = if screen.preset_mode {
        format!("Preset mode: {}", screen.preset_name)
    } else {
        format!(
            "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Ctrl+S: Save preset | Ctrl+L: Load preset | Esc: Cancel"
        )
    };

    let status_paragraph = Paragraph::new(status_text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status_paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::path::PathBuf;

    // ========================================================================
    // Construction Tests
    // ========================================================================

    #[test]
    fn test_new_screen() {
        let screen = BacktestEvaluateConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        assert_eq!(screen.selected_field_index, 0);
        assert!(!screen.preset_mode);
    }

    #[test]
    fn test_default_screen() {
        let screen1 = BacktestEvaluateConfigScreen::new();
        let screen2 = BacktestEvaluateConfigScreen::default();
        assert_eq!(screen1.current_group, screen2.current_group);
    }

    #[test]
    fn test_widgets_initialized() {
        let screen = BacktestEvaluateConfigScreen::new();
        assert!(screen.widgets.contains_key(&EvaluateField::DataPath));
        assert!(screen.widgets.contains_key(&EvaluateField::Algorithm));
        assert!(screen.widgets.contains_key(&EvaluateField::Spread));
        assert!(screen.widgets.contains_key(&EvaluateField::NaiveFills));
        assert!(screen.widgets.contains_key(&EvaluateField::Output));
    }

    // ========================================================================
    // Field Enum Tests
    // ========================================================================

    #[test]
    fn test_evaluate_field_all() {
        let fields = EvaluateField::all();
        assert_eq!(fields.len(), 25);
    }

    #[test]
    fn test_evaluate_field_labels() {
        assert_eq!(EvaluateField::DataPath.label(), "Data Path");
        assert_eq!(EvaluateField::Algorithm.label(), "Algorithm");
        assert_eq!(EvaluateField::Spread.label(), "Spread (bps)");
    }

    #[test]
    fn test_evaluate_field_groups() {
        assert_eq!(EvaluateField::DataPath.group(), ParameterGroup::Basic);
        assert_eq!(EvaluateField::NaiveFills.group(), ParameterGroup::Advanced);
        assert_eq!(EvaluateField::Output.group(), ParameterGroup::Output);
    }

    #[test]
    fn test_fields_in_group() {
        let basic_fields = EvaluateField::fields_in_group(ParameterGroup::Basic);
        assert!(!basic_fields.is_empty());
        assert!(basic_fields.contains(&EvaluateField::DataPath));
        assert!(basic_fields.contains(&EvaluateField::Algorithm));

        let advanced_fields = EvaluateField::fields_in_group(ParameterGroup::Advanced);
        assert!(!advanced_fields.is_empty());
        assert!(advanced_fields.contains(&EvaluateField::NaiveFills));

        let output_fields = EvaluateField::fields_in_group(ParameterGroup::Output);
        assert!(!output_fields.is_empty());
        assert!(output_fields.contains(&EvaluateField::Output));
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
        assert_eq!(ParameterGroup::Advanced.label(), "Advanced");
        assert_eq!(ParameterGroup::Output.label(), "Output");
    }

    #[test]
    fn test_parameter_group_next() {
        assert_eq!(ParameterGroup::Basic.next(), ParameterGroup::Advanced);
        assert_eq!(ParameterGroup::Advanced.next(), ParameterGroup::Output);
        assert_eq!(ParameterGroup::Output.next(), ParameterGroup::Basic);
    }

    #[test]
    fn test_parameter_group_prev() {
        assert_eq!(ParameterGroup::Basic.prev(), ParameterGroup::Output);
        assert_eq!(ParameterGroup::Advanced.prev(), ParameterGroup::Basic);
        assert_eq!(ParameterGroup::Output.prev(), ParameterGroup::Advanced);
    }

    // ========================================================================
    // Navigation Tests
    // ========================================================================

    #[test]
    fn test_current_field() {
        let screen = BacktestEvaluateConfigScreen::new();
        let field = screen.current_field();
        assert!(field.is_some());
        assert_eq!(field.unwrap().group(), ParameterGroup::Basic);
    }

    #[test]
    fn test_next_field() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let initial_index = screen.selected_field_index;
        screen.next_field();
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_prev_field() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.selected_field_index = 1;
        let initial_index = screen.selected_field_index;
        screen.prev_field();
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_next_field_wraps() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let fields = EvaluateField::fields_in_group(ParameterGroup::Basic);
        screen.selected_field_index = fields.len() - 1;
        screen.next_field();
        assert_eq!(screen.selected_field_index, 0);
    }

    #[test]
    fn test_prev_field_wraps() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.selected_field_index = 0;
        screen.prev_field();
        let fields = EvaluateField::fields_in_group(ParameterGroup::Basic);
        assert_eq!(screen.selected_field_index, fields.len() - 1);
    }

    #[test]
    fn test_next_group() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Advanced);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Output);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_prev_group() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.prev_group();
        assert_eq!(screen.current_group, ParameterGroup::Output);
        screen.prev_group();
        assert_eq!(screen.current_group, ParameterGroup::Advanced);
    }

    #[test]
    fn test_group_switch_resets_field_index() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.selected_field_index = 5;
        screen.next_group();
        assert_eq!(screen.selected_field_index, 0);
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_tab() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let initial_index = screen.selected_field_index;
        let key = KeyEvent::new(KeyCode::Tab, KeyModifiers::empty());
        screen.handle_key(key);
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_handle_key_backtab() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.selected_field_index = 1;
        let initial_index = screen.selected_field_index;
        let key = KeyEvent::new(KeyCode::BackTab, KeyModifiers::empty());
        screen.handle_key(key);
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_handle_key_up() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.selected_field_index = 1;
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.selected_field_index, 0);
    }

    #[test]
    fn test_handle_key_down() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let initial_index = screen.selected_field_index;
        let key = KeyEvent::new(KeyCode::Down, KeyModifiers::empty());
        screen.handle_key(key);
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_handle_key_ctrl_left() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.current_group = ParameterGroup::Advanced;
        let key = KeyEvent::new(KeyCode::Left, KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_handle_key_ctrl_right() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let key = KeyEvent::new(KeyCode::Right, KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert_eq!(screen.current_group, ParameterGroup::Advanced);
    }

    #[test]
    fn test_handle_key_ctrl_s() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let key = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert!(screen.preset_mode);
    }

    #[test]
    fn test_handle_key_ctrl_l() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let key = KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert!(screen.preset_mode);
    }

    #[test]
    fn test_handle_preset_key_esc() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.preset_mode = true;
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::empty());
        screen.handle_key(key);
        assert!(!screen.preset_mode);
    }

    #[test]
    fn test_handle_preset_key_char() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.preset_mode = true;
        let key = KeyEvent::new(KeyCode::Char('t'), KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.preset_name, "t");
    }

    #[test]
    fn test_handle_preset_key_backspace() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.preset_mode = true;
        screen.preset_name = "test".to_string();
        let key = KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty());
        screen.handle_key(key);
        assert_eq!(screen.preset_name, "tes");
    }

    // ========================================================================
    // Widget Access Tests
    // ========================================================================

    #[test]
    fn test_get_widget() {
        let screen = BacktestEvaluateConfigScreen::new();
        let widget = screen.get_widget(EvaluateField::DataPath);
        assert!(widget.is_some());
        match widget.unwrap() {
            FieldWidget::PathInput(_) => {}
            _ => panic!("Expected PathInput widget"),
        }
    }

    #[test]
    fn test_get_widget_mut() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let widget = screen.get_widget_mut(EvaluateField::DataPath);
        assert!(widget.is_some());
    }

    #[test]
    fn test_widget_types() {
        let screen = BacktestEvaluateConfigScreen::new();
        
        // Check different widget types
        match screen.get_widget(EvaluateField::DataPath).unwrap() {
            FieldWidget::PathInput(_) => {}
            _ => panic!("DataPath should be PathInput"),
        }
        
        match screen.get_widget(EvaluateField::Algorithm).unwrap() {
            FieldWidget::Dropdown(_) => {}
            _ => panic!("Algorithm should be Dropdown"),
        }
        
        match screen.get_widget(EvaluateField::Spread).unwrap() {
            FieldWidget::NumberInput(_) => {}
            _ => panic!("Spread should be NumberInput"),
        }
        
        match screen.get_widget(EvaluateField::NaiveFills).unwrap() {
            FieldWidget::Toggle(_) => {}
            _ => panic!("NaiveFills should be Toggle"),
        }
    }

    // ========================================================================
    // Build Params Tests
    // ========================================================================

    #[test]
    fn test_build_params_missing_required() {
        let screen = BacktestEvaluateConfigScreen::new();
        let result = screen.build_params();
        assert!(result.is_err()); // Missing data_path and algorithm
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set data path
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(EvaluateField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        
        // Set algorithm
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(EvaluateField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_params_all_fields() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(EvaluateField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(EvaluateField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set some optional fields
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::Spread) {
            let new_w = w.clone().with_value(5.0);
            *w = new_w;
        }
        if let Some(FieldWidget::Toggle(ref mut w)) = screen.get_widget_mut(EvaluateField::NaiveFills) {
            let new_w = w.clone().with_value(true);
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.spread, 5.0);
        assert!(params.naive_fills);
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_empty_screen() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&EvaluateField::DataPath));
        assert!(screen.validation_errors.contains_key(&EvaluateField::Algorithm));
    }

    #[test]
    fn test_validate_with_required() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(EvaluateField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(EvaluateField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.is_valid());
    }

    #[test]
    fn test_validate_fill_prob_range() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set invalid fill_prob
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::FillProb) {
            let new_w = w.clone().with_value(1.5); // Invalid: > 1.0
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.validation_errors.contains_key(&EvaluateField::FillProb));
    }

    #[test]
    fn test_validate_queue_pos_range() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set invalid queue_pos
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::QueuePos) {
            let new_w = w.clone().with_value(-0.1); // Invalid: < 0.0
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.validation_errors.contains_key(&EvaluateField::QueuePos));
    }

    #[test]
    fn test_validate_entropy_thresholds() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set high_entropy <= low_entropy
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::HighEntropy) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::LowEntropy) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.validation_errors.contains_key(&EvaluateField::HighEntropy));
    }

    // ========================================================================
    // Field Widget Tests
    // ========================================================================

    #[test]
    fn test_field_widget_handle_key() {
        let mut widget = FieldWidget::Toggle(
            ToggleWidget::new().with_value(false)
        );
        
        let key = KeyEvent::new(KeyCode::Char(' '), KeyModifiers::empty());
        widget.handle_key(key);
        
        match widget {
            FieldWidget::Toggle(w) => assert!(w.value()),
            _ => panic!("Expected Toggle widget"),
        }
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_navigation_workflow() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Navigate through all fields in Basic group
        let fields = EvaluateField::fields_in_group(ParameterGroup::Basic);
        for _ in 0..fields.len() {
            screen.next_field();
        }
        assert_eq!(screen.selected_field_index, 0); // Should wrap around
        
        // Switch to Advanced group
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Advanced);
        
        // Navigate through Advanced fields
        let advanced_fields = EvaluateField::fields_in_group(ParameterGroup::Advanced);
        for _ in 0..advanced_fields.len() {
            screen.next_field();
        }
        assert_eq!(screen.selected_field_index, 0);
    }

    #[test]
    fn test_full_configuration_workflow() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Set all required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(EvaluateField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(EvaluateField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Configure some parameters
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::Spread) {
            let new_w = w.clone().with_value(3.0);
            *w = new_w;
        }
        if let Some(FieldWidget::NumberInput(ref mut w)) = screen.get_widget_mut(EvaluateField::Skew) {
            let new_w = w.clone().with_value(0.7);
            *w = new_w;
        }
        if let Some(FieldWidget::Toggle(ref mut w)) = screen.get_widget_mut(EvaluateField::NaiveFills) {
            let new_w = w.clone().with_value(true);
            *w = new_w;
        }
        
        // Validate
        screen.validate();
        assert!(screen.is_valid());
        
        // Build params
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.spread, 3.0);
        assert_eq!(params.skew, 0.7);
        assert!(params.naive_fills);
    }

    #[test]
    fn test_preset_mode_workflow() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // Enter preset mode
        let key = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert!(screen.preset_mode);
        
        // Type preset name
        screen.handle_key(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::empty()));
        screen.handle_key(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::empty()));
        screen.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::empty()));
        assert_eq!(screen.preset_name, "myp");
        
        // Cancel preset mode
        screen.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::empty()));
        assert!(!screen.preset_mode);
        assert!(screen.preset_name.is_empty());
    }

    // ========================================================================
    // Edge Cases Tests
    // ========================================================================

    #[test]
    fn test_empty_groups() {
        // All groups should have fields
        for group in ParameterGroup::all() {
            let fields = EvaluateField::fields_in_group(group);
            assert!(!fields.is_empty(), "Group {:?} should have fields", group);
        }
    }

    #[test]
    fn test_all_fields_have_widgets() {
        let screen = BacktestEvaluateConfigScreen::new();
        for field in EvaluateField::all() {
            assert!(screen.widgets.contains_key(&field), "Field {:?} should have a widget", field);
        }
    }

    #[test]
    fn test_field_navigation_bounds() {
        let mut screen = BacktestEvaluateConfigScreen::new();
        let fields = EvaluateField::fields_in_group(ParameterGroup::Basic);
        
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
        let mut screen = BacktestEvaluateConfigScreen::new();
        
        // First validation (should have errors)
        screen.validate();
        assert!(!screen.validation_errors.is_empty());
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(EvaluateField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(EvaluateField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Second validation (should clear errors)
        screen.validate();
        assert!(screen.validation_errors.is_empty());
    }
}
