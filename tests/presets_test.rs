//! Tests for the presets module

use ingestor::presets::{ParameterPreset, PresetStore};

#[test]
fn test_preset_creation() {
    let preset = ParameterPreset::new("Test", "manual", 2.0, 0.5, 0.7, 0.10);
    assert_eq!(preset.name, "Test");
    assert_eq!(preset.optimization_method, "manual");
    assert_eq!(preset.spread_bps, 2.0);
    assert_eq!(preset.skew, 0.5);
    assert_eq!(preset.high_entropy_threshold, 0.7);
    assert_eq!(preset.fill_prob_assumption, 0.10);
}

#[test]
fn test_preset_defaults() {
    let preset = ParameterPreset::new("Default", "grid-search", 1.0, 0.3, 0.7, 0.10);
    assert_eq!(preset.low_entropy_threshold, 0.4);
    assert!(!preset.entropy_gate);
    assert!(preset.data_range.is_empty());
    assert_eq!(preset.num_events, 0);
    assert_eq!(preset.expected_return, 0.0);
}

#[test]
fn test_preset_to_mm_config() {
    let preset = ParameterPreset::new("Test", "manual", 1.5, 0.4, 0.8, 0.10);
    let config = preset.to_mm_config();

    // Now using RegimeParams - high entropy spread is the base spread
    assert_eq!(config.regime_params.high_entropy.spread_bps, 1.5);
    assert_eq!(config.regime_params.high_entropy.skew_factor, 0.4);
    assert_eq!(config.regime_thresholds.high_entropy_threshold, 0.8);
    assert_eq!(config.regime_thresholds.low_entropy_threshold, 0.4);
    // Without entropy gate, should_quote is true in low entropy
    assert!(config.regime_params.low_entropy.should_quote);
}

#[test]
fn test_preset_with_entropy_gate() {
    let mut preset = ParameterPreset::new("Gated", "grid-search", 1.0, 0.3, 0.7, 0.10);
    preset.entropy_gate = true;

    let config = preset.to_mm_config();
    // With entropy gate, should_quote is false in low entropy
    assert!(!config.regime_params.low_entropy.should_quote);
}

#[test]
fn test_preset_menu_description() {
    let mut preset = ParameterPreset::new("GridSearch-Best", "grid-search", 1.0, 0.3, 0.7, 0.10);
    preset.expected_return = 0.05; // 5%

    let desc = preset.menu_description();
    assert!(desc.contains("GridSearch-Best"));
    assert!(desc.contains("spread=1.0bps"));
    assert!(desc.contains("skew=0.3"));
    assert!(desc.contains("+5.0%"));
}

#[test]
fn test_preset_store_default() {
    let store = PresetStore::default();
    assert!(store.presets.is_empty());
}

#[test]
fn test_preset_store_latest() {
    let mut store = PresetStore::default();
    assert!(store.latest().is_none());

    store.presets.push(ParameterPreset::new("First", "manual", 1.0, 0.3, 0.7, 0.10));
    store.presets.push(ParameterPreset::new("Second", "manual", 2.0, 0.5, 0.8, 0.15));

    let latest = store.latest().unwrap();
    assert_eq!(latest.name, "Second");
}

#[test]
fn test_preset_store_get() {
    let mut store = PresetStore::default();
    store.presets.push(ParameterPreset::new("First", "manual", 1.0, 0.3, 0.7, 0.10));
    store.presets.push(ParameterPreset::new("Second", "manual", 2.0, 0.5, 0.8, 0.15));

    assert_eq!(store.get(0).unwrap().name, "First");
    assert_eq!(store.get(1).unwrap().name, "Second");
    assert!(store.get(2).is_none());
}

#[test]
fn test_preset_created_at_local() {
    let preset = ParameterPreset::new("Test", "manual", 1.0, 0.3, 0.7, 0.10);
    let local_str = preset.created_at_local();

    // Should be in format YYYY-MM-DD HH:MM
    assert!(local_str.len() >= 16);
    assert!(local_str.contains('-'));
    assert!(local_str.contains(':'));
}

#[test]
fn test_preset_serialization() {
    let preset = ParameterPreset::new("Test", "optuna", 1.5, 0.4, 0.75, 0.12);

    // Serialize
    let json = serde_json::to_string(&preset).unwrap();
    assert!(json.contains("\"name\":\"Test\""));
    assert!(json.contains("\"optimization_method\":\"optuna\""));

    // Deserialize
    let deserialized: ParameterPreset = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.name, "Test");
    assert_eq!(deserialized.spread_bps, 1.5);
}

#[test]
fn test_preset_store_serialization() {
    let mut store = PresetStore::default();
    store.presets.push(ParameterPreset::new("One", "grid", 1.0, 0.3, 0.7, 0.10));
    store.presets.push(ParameterPreset::new("Two", "optuna", 2.0, 0.5, 0.8, 0.15));

    let json = serde_json::to_string(&store).unwrap();
    let deserialized: PresetStore = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.presets.len(), 2);
    assert_eq!(deserialized.presets[0].name, "One");
    assert_eq!(deserialized.presets[1].name, "Two");
}
