//! Backtest Multi-Objective Config Screen (T-2.10)
//!
//! TUI screen for configuring backtest multi-objective command parameters (MM only).
//! Supports 4 objective weights (Sharpe, Drawdown, Fill, Turnover) that must sum to 1.0,
//! with real-time validation and visual weight distribution.

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

use crate::commands::params::backtest_params::{MultiObjectiveParams, MultiObjectiveParamsBuilder};
use crate::ui::widgets::{
    NumberInputWidget, ToggleWidget,
    PathInputWidget, DropdownWidget, CommaListWidget, SliderWidget,
};
use crate::ui::widgets::params::slider::SliderFormat;

// ============================================================================
// Types
// ============================================================================

/// Field identifiers for navigation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiObjectiveField {
    // Basic Group
    DataPath,
    Algorithm,
    WeightsFile,
    
    // Grid Parameters
    Spreads,
    Skews,
    FillProbs,
    HighEntropies,
    
    // Objective Weights
    WSharpe,
    WDrawdown,
    WFill,
    WTurnover,
    
    // Other Parameters
    MinTrades,
    MaxInventory,
    QuoteSize,
    FeeRate,
    NaiveFills,
    QueuePos,
    LowEntropy,
    
    // Output
    Output,
}

impl MultiObjectiveField {
    /// Get all fields in order
    pub fn all() -> Vec<Self> {
        vec![
            Self::DataPath,
            Self::Algorithm,
            Self::WeightsFile,
            Self::Spreads,
            Self::Skews,
            Self::FillProbs,
            Self::HighEntropies,
            Self::WSharpe,
            Self::WDrawdown,
            Self::WFill,
            Self::WTurnover,
            Self::MinTrades,
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
            Self::FillProbs => "Fill Probs (grid)",
            Self::HighEntropies => "High Entropies (grid)",
            Self::WSharpe => "Weight: Sharpe Ratio",
            Self::WDrawdown => "Weight: Drawdown",
            Self::WFill => "Weight: Fill Rate",
            Self::WTurnover => "Weight: Turnover",
            Self::MinTrades => "Min Trades",
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
            Self::Spreads | Self::Skews | Self::FillProbs | Self::HighEntropies |
            Self::WSharpe | Self::WDrawdown | Self::WFill | Self::WTurnover |
            Self::MinTrades | Self::MaxInventory | Self::QuoteSize | Self::FeeRate |
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

    /// Check if field is a weight field
    pub fn is_weight_field(&self) -> bool {
        matches!(self, Self::WSharpe | Self::WDrawdown | Self::WFill | Self::WTurnover)
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

/// Screen state for backtest multi-objective config
#[derive(Debug, Clone)]
pub struct BacktestMultiObjectiveConfigScreen {
    /// Current parameter group/tab
    pub current_group: ParameterGroup,
    /// Currently selected field index (within current group)
    pub selected_field_index: usize,
    /// Widgets for each field
    pub widgets: HashMap<MultiObjectiveField, FieldWidget>,
    /// Current validation errors
    pub validation_errors: HashMap<MultiObjectiveField, String>,
    /// Weight sum (for validation)
    pub weight_sum: f64,
    /// Whether to auto-normalize weights
    pub auto_normalize: bool,
}

impl Default for BacktestMultiObjectiveConfigScreen {
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
    Slider(SliderWidget),
}

impl BacktestMultiObjectiveConfigScreen {
    /// Create a new screen with default values
    pub fn new() -> Self {
        let mut screen = Self {
            current_group: ParameterGroup::Basic,
            selected_field_index: 0,
            widgets: HashMap::new(),
            validation_errors: HashMap::new(),
            weight_sum: 0.0,
            auto_normalize: false,
        };
        
        // Initialize widgets with defaults
        screen.initialize_widgets();
        screen.update_weight_sum();
        screen
    }

    /// Initialize all widgets with default values
    fn initialize_widgets(&mut self) {
        // Basic group
        self.widgets.insert(MultiObjectiveField::DataPath, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to data directory...")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::Algorithm, FieldWidget::Dropdown(
            DropdownWidget::new()
                .with_options(vec!["as".to_string(), "ml".to_string(), "fixed".to_string()])
                .with_placeholder("Select MM algorithm...")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::WeightsFile, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Path to weights file (optional)...")
                .set_focused(false)
        ));
        
        // Grid parameters
        self.widgets.insert(MultiObjectiveField::Spreads, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 1.0,2.0,3.0")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::Skews, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.3,0.5,0.7")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::FillProbs, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.05,0.10,0.15")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::HighEntropies, FieldWidget::CommaList(
            CommaListWidget::new()
                .with_placeholder("e.g., 0.6,0.7,0.8")
                .set_focused(false)
        ));
        
        // Objective weights - use SliderWidget (0.0 to 1.0)
        self.widgets.insert(MultiObjectiveField::WSharpe, FieldWidget::Slider(
            SliderWidget::new(0.0, 1.0)
                .with_value(0.25)
                .with_step(0.01)
                .with_decimals(2)
                .with_format(SliderFormat::Percentage)
                .with_label("Sharpe")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::WDrawdown, FieldWidget::Slider(
            SliderWidget::new(0.0, 1.0)
                .with_value(0.25)
                .with_step(0.01)
                .with_decimals(2)
                .with_format(SliderFormat::Percentage)
                .with_label("Drawdown")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::WFill, FieldWidget::Slider(
            SliderWidget::new(0.0, 1.0)
                .with_value(0.25)
                .with_step(0.01)
                .with_decimals(2)
                .with_format(SliderFormat::Percentage)
                .with_label("Fill Rate")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::WTurnover, FieldWidget::Slider(
            SliderWidget::new(0.0, 1.0)
                .with_value(0.25)
                .with_step(0.01)
                .with_decimals(2)
                .with_format(SliderFormat::Percentage)
                .with_label("Turnover")
                .set_focused(false)
        ));
        
        // Other parameters
        self.widgets.insert(MultiObjectiveField::MinTrades, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(100.0)
                .with_min(0.0)
                .with_format(crate::ui::widgets::params::number_input::NumberFormat::Integer)
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::MaxInventory, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.1)
                .with_min(0.0)
                .with_decimals(3)
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::QuoteSize, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.001)
                .with_min(0.0)
                .with_decimals(4)
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::FeeRate, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.0001)
                .with_min(0.0)
                .with_decimals(6)
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::NaiveFills, FieldWidget::Toggle(
            ToggleWidget::new()
                .with_value(false)
                .with_label("Use naive fill simulation")
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::QueuePos, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.5)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        self.widgets.insert(MultiObjectiveField::LowEntropy, FieldWidget::NumberInput(
            NumberInputWidget::new()
                .with_value(0.4)
                .with_min(0.0)
                .with_max(1.0)
                .with_decimals(2)
                .set_focused(false)
        ));
        
        // Output
        self.widgets.insert(MultiObjectiveField::Output, FieldWidget::PathInput(
            PathInputWidget::new()
                .with_placeholder("Output file path (optional)...")
                .set_focused(false)
        ));
    }

    /// Get current field
    pub fn current_field(&self) -> Option<MultiObjectiveField> {
        let fields = MultiObjectiveField::fields_in_group(self.current_group);
        fields.get(self.selected_field_index).copied()
    }

    /// Get widget for a field
    pub fn get_widget(&self, field: MultiObjectiveField) -> Option<&FieldWidget> {
        self.widgets.get(&field)
    }

    /// Get mutable widget for a field
    pub fn get_widget_mut(&mut self, field: MultiObjectiveField) -> Option<&mut FieldWidget> {
        self.widgets.get_mut(&field)
    }

    /// Get weight value for a field
    fn get_weight_value(&self, field: MultiObjectiveField) -> f64 {
        if let Some(FieldWidget::Slider(w)) = self.widgets.get(&field) {
            w.value()
        } else {
            0.0
        }
    }

    /// Update weight sum
    fn update_weight_sum(&mut self) {
        self.weight_sum = self.get_weight_value(MultiObjectiveField::WSharpe)
            + self.get_weight_value(MultiObjectiveField::WDrawdown)
            + self.get_weight_value(MultiObjectiveField::WFill)
            + self.get_weight_value(MultiObjectiveField::WTurnover);
    }

    /// Normalize weights to sum to 1.0
    fn normalize_weights(&mut self) {
        self.update_weight_sum(); // Ensure we have current sum
        
        if self.weight_sum.abs() < f64::EPSILON {
            // All weights are zero, set equal weights
            let equal_weight = 0.25;
            for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                         MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
                if let Some(FieldWidget::Slider(ref mut w)) = self.get_widget_mut(field) {
                    let new_w = w.clone().with_value(equal_weight);
                    *w = new_w;
                }
            }
        } else {
            // Normalize by scaling
            let scale = 1.0 / self.weight_sum;
            for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                         MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
                if let Some(FieldWidget::Slider(ref mut w)) = self.get_widget_mut(field) {
                    let normalized = w.value() * scale;
                    let new_w = w.clone().with_value(normalized.max(0.0).min(1.0));
                    *w = new_w;
                }
            }
        }
        self.update_weight_sum(); // Update sum after normalization
    }

    /// Navigate to next field
    pub fn next_field(&mut self) {
        let fields = MultiObjectiveField::fields_in_group(self.current_group);
        if !fields.is_empty() {
            self.selected_field_index = (self.selected_field_index + 1) % fields.len();
            self.update_focus();
        }
    }

    /// Navigate to previous field
    pub fn prev_field(&mut self) {
        let fields = MultiObjectiveField::fields_in_group(self.current_group);
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
                FieldWidget::Slider(w) => {
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
            KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Toggle auto-normalize
                self.auto_normalize = !self.auto_normalize;
                if self.auto_normalize {
                    self.normalize_weights();
                }
                true
            }
            _ => {
                // Forward to current widget
                if let Some(field) = self.current_field() {
                    if let Some(widget) = self.get_widget_mut(field) {
                        let handled = widget.handle_key(key);
                        if handled && field.is_weight_field() {
                            // Update weight sum when weight changes
                            self.update_weight_sum();
                            // Auto-normalize if enabled
                            if self.auto_normalize {
                                self.normalize_weights();
                            }
                        }
                        return handled;
                    }
                }
                false
            }
        }
    }

    /// Build MultiObjectiveParams from current widget values
    pub fn build_params(&self) -> Result<MultiObjectiveParams> {
        let mut builder = MultiObjectiveParamsBuilder::new();

        // Basic group
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&MultiObjectiveField::DataPath) {
            if !w.path().is_empty() {
                builder = builder.data_path(PathBuf::from(w.path()));
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&MultiObjectiveField::Algorithm) {
            if let Some(alg) = w.selected_option() {
                builder = builder.algorithm(alg.clone());
            }
        }

        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&MultiObjectiveField::WeightsFile) {
            if !w.path().is_empty() {
                builder = builder.weights_file(Some(PathBuf::from(w.path())));
            }
        }

        // Grid parameters
        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&MultiObjectiveField::Spreads) {
            let spreads_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.spreads(spreads_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&MultiObjectiveField::Skews) {
            let skews_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.skews(skews_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&MultiObjectiveField::FillProbs) {
            let fill_probs_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.fill_probs(fill_probs_str);
        }

        if let Some(FieldWidget::CommaList(w)) = self.widgets.get(&MultiObjectiveField::HighEntropies) {
            let high_entropies_str = w.values().iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            builder = builder.high_entropies(high_entropies_str);
        }

        // Objective weights
        if let Some(FieldWidget::Slider(w)) = self.widgets.get(&MultiObjectiveField::WSharpe) {
            builder = builder.w_sharpe(w.value());
        }

        if let Some(FieldWidget::Slider(w)) = self.widgets.get(&MultiObjectiveField::WDrawdown) {
            builder = builder.w_drawdown(w.value());
        }

        if let Some(FieldWidget::Slider(w)) = self.widgets.get(&MultiObjectiveField::WFill) {
            builder = builder.w_fill(w.value());
        }

        if let Some(FieldWidget::Slider(w)) = self.widgets.get(&MultiObjectiveField::WTurnover) {
            builder = builder.w_turnover(w.value());
        }

        // Other parameters
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::MinTrades) {
            builder = builder.min_trades(w.value() as usize);
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::MaxInventory) {
            builder = builder.max_inventory(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::QuoteSize) {
            builder = builder.quote_size(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::FeeRate) {
            builder = builder.fee_rate(w.value());
        }

        if let Some(FieldWidget::Toggle(w)) = self.widgets.get(&MultiObjectiveField::NaiveFills) {
            builder = builder.naive_fills(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::QueuePos) {
            builder = builder.queue_pos(w.value());
        }

        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::LowEntropy) {
            builder = builder.low_entropy(w.value());
        }

        // Output
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&MultiObjectiveField::Output) {
            if !w.path().is_empty() {
                builder = builder.output(Some(PathBuf::from(w.path())));
            }
        }

        builder.build()
    }

    /// Validate all fields
    pub fn validate(&mut self) {
        self.validation_errors.clear();
        self.update_weight_sum();

        // Validate required fields
        if let Some(FieldWidget::PathInput(w)) = self.widgets.get(&MultiObjectiveField::DataPath) {
            if w.path().is_empty() {
                self.validation_errors.insert(
                    MultiObjectiveField::DataPath,
                    "Data path is required".to_string(),
                );
            }
        }

        if let Some(FieldWidget::Dropdown(w)) = self.widgets.get(&MultiObjectiveField::Algorithm) {
            if w.selected_option().is_none() {
                self.validation_errors.insert(
                    MultiObjectiveField::Algorithm,
                    "Algorithm is required".to_string(),
                );
            } else if let Some(alg) = w.selected_option() {
                // Validate algorithm is MM type
                if !["as", "ml", "fixed"].contains(&alg.as_str()) {
                    self.validation_errors.insert(
                        MultiObjectiveField::Algorithm,
                        "Algorithm must be MM type: as, ml, or fixed".to_string(),
                    );
                }
            }
        }

        // Validate weight sum
        let sum_diff = (self.weight_sum - 1.0).abs();
        if sum_diff > 0.01 { // Allow small floating point error
            self.validation_errors.insert(
                MultiObjectiveField::WSharpe,
                format!("Weights must sum to 1.0 (current: {:.2})", self.weight_sum),
            );
        }

        // Validate individual weights are in range
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(w)) = self.widgets.get(&field) {
                if w.value() < 0.0 || w.value() > 1.0 {
                    self.validation_errors.insert(
                        field,
                        format!("Weight must be in range [0.0, 1.0] (current: {:.2})", w.value()),
                    );
                }
            }
        }

        // Validate grid parameters have at least one value
        for field in [MultiObjectiveField::Spreads, MultiObjectiveField::Skews,
                     MultiObjectiveField::FillProbs, MultiObjectiveField::HighEntropies] {
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
        if let Some(FieldWidget::NumberInput(w)) = self.widgets.get(&MultiObjectiveField::QueuePos) {
            if w.value() < 0.0 || w.value() > 1.0 {
                self.validation_errors.insert(
                    MultiObjectiveField::QueuePos,
                    "Queue position must be in range [0.0, 1.0]".to_string(),
                );
            }
        }
    }

    /// Check if all fields are valid
    pub fn is_valid(&self) -> bool {
        self.validation_errors.is_empty()
    }

    /// Get weight distribution text for display
    pub fn weight_distribution_text(&self) -> String {
        let sharpe = self.get_weight_value(MultiObjectiveField::WSharpe);
        let drawdown = self.get_weight_value(MultiObjectiveField::WDrawdown);
        let fill = self.get_weight_value(MultiObjectiveField::WFill);
        let turnover = self.get_weight_value(MultiObjectiveField::WTurnover);
        
        format!(
            "Sharpe: {:.1}% | Drawdown: {:.1}% | Fill: {:.1}% | Turnover: {:.1}% | Sum: {:.2}",
            sharpe * 100.0, drawdown * 100.0, fill * 100.0, turnover * 100.0, self.weight_sum
        )
    }

    /// Get visual weight distribution (bar representation)
    pub fn visual_weight_distribution(&self, width: usize) -> String {
        let sharpe = self.get_weight_value(MultiObjectiveField::WSharpe);
        let drawdown = self.get_weight_value(MultiObjectiveField::WDrawdown);
        let fill = self.get_weight_value(MultiObjectiveField::WFill);
        let turnover = self.get_weight_value(MultiObjectiveField::WTurnover);
        
        let total = sharpe + drawdown + fill + turnover;
        if total.abs() < f64::EPSILON {
            return " ".repeat(width);
        }
        
        let sharpe_width = ((sharpe / total) * width as f64).round() as usize;
        let drawdown_width = ((drawdown / total) * width as f64).round() as usize;
        let fill_width = ((fill / total) * width as f64).round() as usize;
        let used_width = sharpe_width + drawdown_width + fill_width;
        let turnover_width = if used_width < width {
            width - used_width
        } else {
            0
        };
        
        // Build result ensuring exact width
        let mut result = String::with_capacity(width * 3); // Unicode chars are 3 bytes
        let mut used = 0;
        
        // Add sharpe
        let sharpe_actual = sharpe_width.min(width.saturating_sub(used));
        if sharpe_actual > 0 {
            result.push_str(&"█".repeat(sharpe_actual));
            used += sharpe_actual;
        }
        
        // Add drawdown
        let drawdown_actual = drawdown_width.min(width.saturating_sub(used));
        if drawdown_actual > 0 {
            result.push_str(&"▓".repeat(drawdown_actual));
            used += drawdown_actual;
        }
        
        // Add fill
        let fill_actual = fill_width.min(width.saturating_sub(used));
        if fill_actual > 0 {
            result.push_str(&"▒".repeat(fill_actual));
            used += fill_actual;
        }
        
        // Add turnover
        let turnover_actual = turnover_width.min(width.saturating_sub(used));
        if turnover_actual > 0 {
            result.push_str(&"░".repeat(turnover_actual));
            used += turnover_actual;
        }
        
        // Pad with spaces if needed to reach exact width
        let current_chars = result.chars().count();
        if current_chars < width {
            result.push_str(&" ".repeat(width - current_chars));
        }
        
        // Truncate to exact width (in case of rounding issues) - use char count
        result.chars().take(width).collect::<String>()
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
            Self::Slider(w) => w.handle_key(key),
        }
    }
}

/// Render the backtest multi-objective config screen
pub fn draw_backtest_multi_objective_config_screen(
    f: &mut Frame,
    screen: &BacktestMultiObjectiveConfigScreen,
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
fn draw_content_area(f: &mut Frame, area: Rect, screen: &BacktestMultiObjectiveConfigScreen) {
    let fields = MultiObjectiveField::fields_in_group(screen.current_group);
    
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
    field: MultiObjectiveField,
    screen: &BacktestMultiObjectiveConfigScreen,
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
            FieldWidget::NumberInput(w) => w.render(f, chunks[1]),
            FieldWidget::Toggle(w) => w.render(f, chunks[1]),
            FieldWidget::PathInput(w) => w.render(f, chunks[1]),
            FieldWidget::Dropdown(w) => w.render(f, chunks[1]),
            FieldWidget::CommaList(w) => w.render(f, chunks[1]),
            FieldWidget::Slider(w) => w.render(f, chunks[1]),
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
fn draw_preview_and_status(f: &mut Frame, area: Rect, screen: &BacktestMultiObjectiveConfigScreen) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Weight distribution
            Constraint::Length(2), // Status
        ])
        .split(area);

    // Draw weight distribution
    let dist_text = screen.weight_distribution_text();
    let visual_dist = screen.visual_weight_distribution((chunks[0].width.saturating_sub(4)) as usize);
    
    let dist_line = Line::from(vec![
        Span::styled(dist_text, Style::default().fg(Color::Cyan)),
    ]);
    let visual_line = Line::from(vec![
        Span::styled(visual_dist, Style::default().fg(Color::Green)),
    ]);
    
    let dist_paragraph = Paragraph::new(vec![dist_line, visual_line])
        .block(Block::default().borders(Borders::ALL).title("Weight Distribution"));
    f.render_widget(dist_paragraph, chunks[0]);

    // Draw status bar
    let status_text = if screen.auto_normalize {
        "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Ctrl+N: Toggle auto-normalize (ON) | Esc: Cancel"
    } else {
        "Tab/↑↓: Navigate | Ctrl+←→: Switch group | Ctrl+N: Toggle auto-normalize (OFF) | Esc: Cancel"
    };
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
        let screen = BacktestMultiObjectiveConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        assert_eq!(screen.selected_field_index, 0);
        assert!(!screen.auto_normalize);
    }

    #[test]
    fn test_default_screen() {
        let screen1 = BacktestMultiObjectiveConfigScreen::new();
        let screen2 = BacktestMultiObjectiveConfigScreen::default();
        assert_eq!(screen1.current_group, screen2.current_group);
    }

    #[test]
    fn test_widgets_initialized() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        assert!(screen.widgets.contains_key(&MultiObjectiveField::DataPath));
        assert!(screen.widgets.contains_key(&MultiObjectiveField::Algorithm));
        assert!(screen.widgets.contains_key(&MultiObjectiveField::WSharpe));
        assert!(screen.widgets.contains_key(&MultiObjectiveField::WDrawdown));
        assert!(screen.widgets.contains_key(&MultiObjectiveField::WFill));
        assert!(screen.widgets.contains_key(&MultiObjectiveField::WTurnover));
    }

    #[test]
    fn test_weight_widgets_are_sliders() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            match screen.get_widget(field).unwrap() {
                FieldWidget::Slider(_) => {}
                _ => panic!("{:?} should be Slider", field),
            }
        }
    }

    #[test]
    fn test_initial_weights_sum() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        // Initial weights should be 0.25 each, summing to 1.0
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
    }

    // ========================================================================
    // Field Enum Tests
    // ========================================================================

    #[test]
    fn test_multi_objective_field_all() {
        let fields = MultiObjectiveField::all();
        assert_eq!(fields.len(), 19);
    }

    #[test]
    fn test_multi_objective_field_labels() {
        assert_eq!(MultiObjectiveField::DataPath.label(), "Data Path");
        assert_eq!(MultiObjectiveField::Algorithm.label(), "Algorithm (MM only)");
        assert_eq!(MultiObjectiveField::WSharpe.label(), "Weight: Sharpe Ratio");
    }

    #[test]
    fn test_multi_objective_field_groups() {
        assert_eq!(MultiObjectiveField::DataPath.group(), ParameterGroup::Basic);
        assert_eq!(MultiObjectiveField::WSharpe.group(), ParameterGroup::Parameters);
        assert_eq!(MultiObjectiveField::Output.group(), ParameterGroup::Output);
    }

    #[test]
    fn test_is_weight_field() {
        assert!(MultiObjectiveField::WSharpe.is_weight_field());
        assert!(MultiObjectiveField::WDrawdown.is_weight_field());
        assert!(MultiObjectiveField::WFill.is_weight_field());
        assert!(MultiObjectiveField::WTurnover.is_weight_field());
        assert!(!MultiObjectiveField::DataPath.is_weight_field());
    }

    #[test]
    fn test_fields_in_group() {
        let basic_fields = MultiObjectiveField::fields_in_group(ParameterGroup::Basic);
        assert_eq!(basic_fields.len(), 3);
        
        let param_fields = MultiObjectiveField::fields_in_group(ParameterGroup::Parameters);
        assert!(param_fields.contains(&MultiObjectiveField::WSharpe));
        assert!(param_fields.contains(&MultiObjectiveField::Spreads));
        
        let output_fields = MultiObjectiveField::fields_in_group(ParameterGroup::Output);
        assert_eq!(output_fields.len(), 1);
    }

    // ========================================================================
    // Weight Sum Tests
    // ========================================================================

    #[test]
    fn test_update_weight_sum() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        screen.update_weight_sum();
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_update_weight_sum_zero() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set all weights to zero
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_value(0.0);
                *w = new_w;
            }
        }
        
        screen.update_weight_sum();
        assert!((screen.weight_sum - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_normalize_weights() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights that don't sum to 1.0
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        
        screen.normalize_weights();
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_normalize_weights_zero_sum() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set all weights to zero
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_value(0.0);
                *w = new_w;
            }
        }
        
        screen.normalize_weights();
        // Should set equal weights (0.25 each)
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
        assert!((screen.get_weight_value(MultiObjectiveField::WSharpe) - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_normalize_weights_preserves_ratios() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights with specific ratios
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        let sharpe_before = screen.get_weight_value(MultiObjectiveField::WSharpe);
        let drawdown_before = screen.get_weight_value(MultiObjectiveField::WDrawdown);
        let ratio = sharpe_before / drawdown_before;
        
        // Double all weights (sum = 2.0)
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_value(w.value() * 2.0);
                *w = new_w;
            }
        }
        
        screen.normalize_weights();
        
        // Ratio should be preserved
        let sharpe_after = screen.get_weight_value(MultiObjectiveField::WSharpe);
        let drawdown_after = screen.get_weight_value(MultiObjectiveField::WDrawdown);
        let ratio_after = sharpe_after / drawdown_after;
        assert!((ratio - ratio_after).abs() < 0.01);
    }

    // ========================================================================
    // Navigation Tests
    // ========================================================================

    #[test]
    fn test_current_field() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        let field = screen.current_field();
        assert!(field.is_some());
        assert_eq!(field.unwrap().group(), ParameterGroup::Basic);
    }

    #[test]
    fn test_next_field() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        let initial_index = screen.selected_field_index;
        screen.next_field();
        assert_ne!(screen.selected_field_index, initial_index);
    }

    #[test]
    fn test_next_group() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Parameters);
    }

    // ========================================================================
    // Key Event Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_key_ctrl_n_toggles_auto_normalize() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        assert!(!screen.auto_normalize);
        
        let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert!(screen.auto_normalize);
        
        screen.handle_key(key);
        assert!(!screen.auto_normalize);
    }

    #[test]
    fn test_handle_key_weight_change_updates_sum() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        screen.current_group = ParameterGroup::Parameters;
        screen.selected_field_index = 4; // WSharpe (assuming it's at index 4)
        
        let initial_sum = screen.weight_sum;
        
        // Navigate to WSharpe if needed
        let fields = MultiObjectiveField::fields_in_group(ParameterGroup::Parameters);
        if let Some(idx) = fields.iter().position(|&f| f == MultiObjectiveField::WSharpe) {
            screen.selected_field_index = idx;
            screen.update_focus();
            
            // Increment weight
            let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
            screen.handle_key(key);
            
            // Sum should be updated
            assert_ne!(screen.weight_sum, initial_sum);
        }
    }

    #[test]
    fn test_handle_key_auto_normalize_on_weight_change() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        screen.auto_normalize = true;
        
        // Set weights that don't sum to 1.0
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.5);
            *w = new_w;
        }
        screen.update_weight_sum();
        
        // Change a weight
        screen.current_group = ParameterGroup::Parameters;
        let fields = MultiObjectiveField::fields_in_group(ParameterGroup::Parameters);
        if let Some(idx) = fields.iter().position(|&f| f == MultiObjectiveField::WDrawdown) {
            screen.selected_field_index = idx;
            screen.update_focus();
            
            let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
            screen.handle_key(key);
            
            // Weights should be normalized
            assert!((screen.weight_sum - 1.0).abs() < 0.01);
        }
    }

    // ========================================================================
    // Build Params Tests
    // ========================================================================

    #[test]
    fn test_build_params_missing_required() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        let result = screen.build_params();
        assert!(result.is_err());
    }

    #[test]
    fn test_build_params_with_required() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        for field in [MultiObjectiveField::Spreads, MultiObjectiveField::Skews,
                     MultiObjectiveField::FillProbs, MultiObjectiveField::HighEntropies] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_params_weights() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        for field in [MultiObjectiveField::Spreads, MultiObjectiveField::Skews,
                     MultiObjectiveField::FillProbs, MultiObjectiveField::HighEntropies] {
            if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_values(vec![1.0]);
                *w = new_w;
            }
        }
        
        // Set weights
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        assert!((params.w_sharpe - 0.4).abs() < 0.01);
        assert!((params.w_drawdown - 0.3).abs() < 0.01);
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_empty_screen() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&MultiObjectiveField::DataPath));
        assert!(screen.validation_errors.contains_key(&MultiObjectiveField::Algorithm));
    }

    #[test]
    fn test_validate_weight_sum() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights that don't sum to 1.0
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.5);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.05);
            *w = new_w;
        }
        
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&MultiObjectiveField::WSharpe));
    }

    #[test]
    fn test_validate_weight_sum_valid() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights that sum to 1.0
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        screen.validate();
        // Should not have weight sum error (but may have other errors)
        assert!(!screen.validation_errors.contains_key(&MultiObjectiveField::WSharpe) ||
                !screen.validation_errors.get(&MultiObjectiveField::WSharpe)
                    .unwrap().contains("sum"));
    }

    #[test]
    fn test_validate_weight_out_of_range() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weight > 1.0
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(1.5);
            *w = new_w;
        }
        
        screen.validate();
        assert!(screen.validation_errors.contains_key(&MultiObjectiveField::WSharpe));
    }

    #[test]
    fn test_validate_algorithm_mm_only() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set data path
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        
        // Algorithm dropdown only has MM algorithms
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Algorithm) {
            let new_w = w.clone().with_selected(0); // "as" is MM
            *w = new_w;
        }
        
        // Set grid values
        for field in [MultiObjectiveField::Spreads, MultiObjectiveField::Skews,
                     MultiObjectiveField::FillProbs, MultiObjectiveField::HighEntropies] {
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
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Leave grids empty
        screen.validate();
        assert!(!screen.is_valid());
        assert!(screen.validation_errors.contains_key(&MultiObjectiveField::Spreads));
    }

    // ========================================================================
    // Weight Distribution Tests
    // ========================================================================

    #[test]
    fn test_weight_distribution_text() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        let text = screen.weight_distribution_text();
        assert!(text.contains("Sharpe"));
        assert!(text.contains("Drawdown"));
        assert!(text.contains("Fill"));
        assert!(text.contains("Turnover"));
        assert!(text.contains("Sum"));
    }

    #[test]
    fn test_visual_weight_distribution() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set equal weights
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_value(0.25);
                *w = new_w;
            }
        }
        
        let visual = screen.visual_weight_distribution(20);
        assert_eq!(visual.chars().count(), 20);
        // Should have roughly equal distribution
        assert!(visual.contains("█")); // Sharpe
        assert!(visual.contains("▓")); // Drawdown
        assert!(visual.contains("▒")); // Fill
        assert!(visual.contains("░")); // Turnover
    }

    #[test]
    fn test_visual_weight_distribution_zero_weights() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set all weights to zero
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_value(0.0);
                *w = new_w;
            }
        }
        
        let visual = screen.visual_weight_distribution(20);
        // Should return empty spaces
        assert_eq!(visual.chars().count(), 20);
    }

    #[test]
    fn test_visual_weight_distribution_single_weight() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set only Sharpe weight
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(1.0);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.0);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.0);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.0);
            *w = new_w;
        }
        
        let visual = screen.visual_weight_distribution(20);
        // Should be all Sharpe (█)
        assert!(visual.chars().all(|c| c == '█' || c == ' '));
    }

    // ========================================================================
    // Integration-style Tests
    // ========================================================================

    #[test]
    fn test_full_configuration_workflow() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Spreads) {
            let new_w = w.clone().with_values(vec![1.0, 2.0]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Skews) {
            let new_w = w.clone().with_values(vec![0.3, 0.5]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::FillProbs) {
            let new_w = w.clone().with_values(vec![0.05, 0.10]);
            *w = new_w;
        }
        if let Some(FieldWidget::CommaList(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::HighEntropies) {
            let new_w = w.clone().with_values(vec![0.6, 0.7]);
            *w = new_w;
        }
        
        // Set weights
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        // Validate
        screen.validate();
        assert!(screen.is_valid());
        
        // Build params
        let result = screen.build_params();
        assert!(result.is_ok());
        let params = result.unwrap();
        assert!((params.w_sharpe - 0.4).abs() < 0.01);
        assert!((params.w_drawdown - 0.3).abs() < 0.01);
        assert_eq!(params.spreads, "1,2");
    }

    #[test]
    fn test_auto_normalize_workflow() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Enable auto-normalize
        screen.auto_normalize = true;
        
        // Set weights that don't sum to 1.0
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.5);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.05);
            *w = new_w;
        }
        
        // Normalize
        screen.normalize_weights();
        
        // Weights should sum to 1.0
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
        
        // Ratios should be preserved
        let sharpe = screen.get_weight_value(MultiObjectiveField::WSharpe);
        let drawdown = screen.get_weight_value(MultiObjectiveField::WDrawdown);
        let expected_ratio = 0.5 / 0.3;
        let actual_ratio = sharpe / drawdown;
        assert!((expected_ratio - actual_ratio).abs() < 0.01);
    }

    #[test]
    fn test_weight_adjustment_preserves_other_weights() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set initial weights
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        let drawdown_before = screen.get_weight_value(MultiObjectiveField::WDrawdown);
        let fill_before = screen.get_weight_value(MultiObjectiveField::WFill);
        let turnover_before = screen.get_weight_value(MultiObjectiveField::WTurnover);
        
        // Change Sharpe weight
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.5);
            *w = new_w;
        }
        
        // Other weights should remain unchanged (unless auto-normalize is on)
        assert_eq!(screen.get_weight_value(MultiObjectiveField::WDrawdown), drawdown_before);
        assert_eq!(screen.get_weight_value(MultiObjectiveField::WFill), fill_before);
        assert_eq!(screen.get_weight_value(MultiObjectiveField::WTurnover), turnover_before);
    }

    // ========================================================================
    // Edge Cases Tests
    // ========================================================================

    #[test]
    fn test_all_fields_have_widgets() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        for field in MultiObjectiveField::all() {
            assert!(screen.widgets.contains_key(&field), "Field {:?} should have a widget", field);
        }
    }

    #[test]
    fn test_weight_sum_precision() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights with many decimal places
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.333333);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.333333);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.333334);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.0);
            *w = new_w;
        }
        
        screen.update_weight_sum();
        // Sum should be close to 1.0 (within floating point precision)
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_normalize_weights_very_small_values() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set very small weights
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.001);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.001);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.001);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.001);
            *w = new_w;
        }
        
        screen.normalize_weights();
        // Should normalize to equal weights
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_validation_clears_errors() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // First validation (should have errors)
        screen.validate();
        assert!(!screen.validation_errors.is_empty());
        
        // Set required fields
        if let Some(FieldWidget::PathInput(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::DataPath) {
            let new_w = w.clone().with_path("/tmp/data");
            *w = new_w;
        }
        if let Some(FieldWidget::Dropdown(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::Algorithm) {
            let new_w = w.clone().with_selected(0);
            *w = new_w;
        }
        
        // Set grid values
        for field in [MultiObjectiveField::Spreads, MultiObjectiveField::Skews,
                     MultiObjectiveField::FillProbs, MultiObjectiveField::HighEntropies] {
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
    fn test_weight_distribution_text_format() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set specific weights
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WSharpe) {
            let new_w = w.clone().with_value(0.4);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WDrawdown) {
            let new_w = w.clone().with_value(0.3);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WFill) {
            let new_w = w.clone().with_value(0.2);
            *w = new_w;
        }
        if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(MultiObjectiveField::WTurnover) {
            let new_w = w.clone().with_value(0.1);
            *w = new_w;
        }
        
        let text = screen.weight_distribution_text();
        assert!(text.contains("40.0")); // 0.4 * 100
        assert!(text.contains("30.0")); // 0.3 * 100
        assert!(text.contains("20.0")); // 0.2 * 100
        assert!(text.contains("10.0")); // 0.1 * 100
        assert!(text.contains("1.00")); // Sum
    }

    #[test]
    fn test_visual_weight_distribution_width() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        
        // With default weights (0.25 each), should distribute evenly
        let visual_10 = screen.visual_weight_distribution(10);
        assert_eq!(visual_10.chars().count(), 10, "Visual distribution should be exactly 10 chars, got: '{}' (char count: {})", visual_10, visual_10.chars().count());
        
        let visual_50 = screen.visual_weight_distribution(50);
        assert_eq!(visual_50.chars().count(), 50, "Visual distribution should be exactly 50 chars, got: '{}' (char count: {})", visual_50, visual_50.chars().count());
    }

    #[test]
    fn test_get_weight_value() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        let sharpe = screen.get_weight_value(MultiObjectiveField::WSharpe);
        assert!((sharpe - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_get_weight_value_non_weight_field() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        let value = screen.get_weight_value(MultiObjectiveField::DataPath);
        assert_eq!(value, 0.0); // Non-weight fields return 0.0
    }

    #[test]
    fn test_field_widget_handle_key() {
        let mut widget = FieldWidget::Slider(
            SliderWidget::new(0.0, 1.0).with_value(0.5)
        );
        
        let key = KeyEvent::new(KeyCode::Right, KeyModifiers::empty());
        widget.handle_key(key);
        
        match widget {
            FieldWidget::Slider(w) => assert!(w.value() > 0.5),
            _ => panic!("Expected Slider widget"),
        }
    }

    #[test]
    fn test_navigation_through_all_groups() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        assert_eq!(screen.current_group, ParameterGroup::Basic);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Parameters);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Output);
        screen.next_group();
        assert_eq!(screen.current_group, ParameterGroup::Basic);
    }

    #[test]
    fn test_weight_slider_defaults() {
        let screen = BacktestMultiObjectiveConfigScreen::new();
        
        // All weights should default to 0.25
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            let value = screen.get_weight_value(field);
            assert!((value - 0.25).abs() < 0.01, "Field {:?} should default to 0.25", field);
        }
    }

    #[test]
    fn test_weight_sum_after_normalize() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        
        // Set weights that sum to 2.0
        for field in [MultiObjectiveField::WSharpe, MultiObjectiveField::WDrawdown,
                     MultiObjectiveField::WFill, MultiObjectiveField::WTurnover] {
            if let Some(FieldWidget::Slider(ref mut w)) = screen.get_widget_mut(field) {
                let new_w = w.clone().with_value(0.5);
                *w = new_w;
            }
        }
        
        screen.normalize_weights();
        assert!((screen.weight_sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_auto_normalize_toggle() {
        let mut screen = BacktestMultiObjectiveConfigScreen::new();
        assert!(!screen.auto_normalize);
        
        let key = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL);
        screen.handle_key(key);
        assert!(screen.auto_normalize);
        
        screen.handle_key(key);
        assert!(!screen.auto_normalize);
    }
}
