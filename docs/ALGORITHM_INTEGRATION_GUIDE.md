# Algorithm Integration Guide

## How to Add Algorithm Selection to Live Trading

### Current Gap

The codebase has excellent infrastructure for algorithm selection (trait-based factory pattern), but it's not wired into the live trading path:

1. **Infrastructure exists**: `src/algorithms/` module with trait + factory
2. **MLSpreadSkewAlgorithm is implemented**: Ready to use
3. **Integration missing**: main.rs and tui.rs hardcoded to use AvellanedaStoikovMM

### Integration Steps (No Changes Yet - Research Only)

---

## Step 1: Modify Main.rs to Accept Algorithm Selection

### Current State
```rust
// Line 41: Hardcoded symbol
const SYMBOL: &str = "BTCUSDT";

// Lines 59-62: Direct instantiation
let lob_manager = LobFeedManager::new(SYMBOL);
// ... feature engines...
// Line 195-204: TUI thread creation
```

### Required Changes
1. **Parse CLI arguments** (lines 37-57):
   - Add clap argument parser
   - Accept `--algorithm` flag: `avellaneda-stoikov` or `ml-spread-skew`
   - Accept `--symbol` flag to override hardcoded value
   - Accept `--ml-weights` for path to ML model file

2. **Modify TUI initialization** (lines 195-204):
   - Pass algorithm selection to TUI thread
   - Send through Arc or environment

3. **Modify tui::run_tui() signature**:
   - Accept `algorithm_type` parameter
   - Accept optional `ml_weights` path

### Example CLI Interface
```bash
cargo run -- --algorithm avellaneda-stoikov --symbol BTCUSDT
cargo run -- --algorithm ml-spread-skew --symbol ETHUSDT --ml-weights ./models/eth_weights.json
cargo run -- --symbol AVAXUSDT  # Default to A-S
```

---

## Step 2: Modify TUI to Create Algorithm by Type

### Current State (tui.rs lines 520-532)
```rust
let paper_trading = PaperTradingEngine::new(
    AvellanedaStoikovMM::new(MMConfig::default()),  // Hardcoded
    SimulatorConfig::default()
);
```

### Required Changes

**Import algorithm factories** (at top of tui.rs):
```rust
use ingestor::algorithms::{
    create_algorithm, create_ml_algorithm,
    AlgorithmType, MLModelWeights,
};
```

**Modify TUI initialization** (around line 520):
```rust
pub fn run_tui(
    rx: Receiver<FeaturesSnapshot>,
    symbol: String,
    algorithm_type: AlgorithmType,              // NEW
    ml_weights_path: Option<String>,            // NEW
) -> anyhow::Result<TuiSettings> {
    // ... existing setup ...
    
    // Create algorithm by type
    let algo = if algorithm_type == AlgorithmType::MLSpreadSkew {
        // Load ML weights from file or use defaults
        let weights = if let Some(path) = ml_weights_path {
            MLModelWeights::load_from_file(&path)
                .unwrap_or_else(|_| MLModelWeights::default())
        } else {
            MLModelWeights::default()
        };
        create_ml_algorithm(
            dec!(0.1),      // max_inventory
            dec!(0.001),    // quote_size
            weights,
        )
    } else {
        create_algorithm(
            algorithm_type,
            dec!(0.1),
            dec!(0.001),
            None,
        )
    }?;
    
    // Extract base type for PaperTradingEngine
    // NOTE: This requires PaperTradingEngine to accept trait object
    let paper_trading = PaperTradingEngine::new(
        algo,  // Box<dyn MarketMakingAlgorithm>
        SimulatorConfig::default()
    );
```

---

## Step 3: Modify PaperTradingEngine to Accept Trait Object

### Current State (mm_simulator.rs lines 186-199)
```rust
pub struct PaperTradingEngine {
    pub mm: MarketMakerEngine,  // Type alias for AvellanedaStoikovMM
    pub simulator: MMSimulator,
    last_quotes: Option<MMQuotes>,
}

impl PaperTradingEngine {
    pub fn new(mm: MarketMakerEngine, sim_config: SimulatorConfig) -> Self {
        Self {
            mm,
            simulator: MMSimulator::new(sim_config),
            last_quotes: None,
        }
    }
}
```

### Required Changes

**Option A: Keep backward compatibility (RECOMMENDED)**

Create new struct for trait-based usage:
```rust
pub struct PaperTradingEngineGeneric {
    pub mm: Box<dyn MarketMakingAlgorithm>,
    pub simulator: MMSimulator,
    last_quotes: Option<MMQuotes>,
}

impl PaperTradingEngineGeneric {
    pub fn new(
        mm: Box<dyn MarketMakingAlgorithm>,
        sim_config: SimulatorConfig,
    ) -> Self {
        Self {
            mm,
            simulator: MMSimulator::new(sim_config),
            last_quotes: None,
        }
    }
    
    // Same methods as PaperTradingEngine
    pub fn on_features(&mut self, ...) -> MMQuotes {
        let quotes = self.mm.compute_quotes(&market_input);
        // ... rest same ...
    }
}

// Keep old type for backward compatibility
pub type PaperTradingEngine = PaperTradingEngineGeneric;
```

**Option B: Conditional compilation**
- Use feature flags to switch between hardcoded and generic versions
- Minimizes changes to existing code

### Changes Required to on_features()
The method signature stays the same because it wraps the trait method calls. The trait `MarketMakingAlgorithm::compute_quotes()` takes `MarketInput` struct instead of individual parameters.

**Create MarketInput wrapper**:
```rust
pub fn on_features(
    &mut self,
    microprice: Decimal,
    mid_price: Decimal,
    volatility: f64,
    entropy_score: f64,
    flow_imbalance: f64,
    timestamp_ms: u64,
) -> MMQuotes {
    // Convert to trait interface
    let input = MarketInput {
        best_bid: /* extract from features */,
        best_ask: /* extract from features */,
        volatility,
        entropy: entropy_score,
        book_imbalance: flow_imbalance,
        timestamp_ms,
    };
    
    let quotes = self.mm.compute_quotes(&input);
    self.simulator.update_quotes(&quotes);
    self.last_quotes = Some(quotes.clone());
    self.mm.update_mark_to_market(mid_price);
    quotes
}
```

---

## Step 4: Update TUI Display for Algorithm Info

### Current Display (tui.rs lines 1299-1307)
Shows preset info but not algorithm type.

### Required Changes

**Add algorithm display** (in draw_live_mm):
```rust
let title = format!(
    " {} | {} | {} | {} | ALG: {} | [r] reset [q] menu ",
    symbol.to_uppercase(),
    now,
    preset_info,
    session_info,
    paper_trading.mm.algorithm_type().display_name(),  // NEW
);
```

**Add algorithm-specific parameter display**:
- For A-S: Show regime, spread bps, skew factor
- For ML: Show model version, training info if available

---

## Step 5: Update Preset System for Algorithm

### Current State (presets.rs)
Presets only store spread/skew parameters; they don't specify algorithm type.

### Required Changes

**Add algorithm field to ParameterPreset**:
```rust
pub struct ParameterPreset {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub optimization_method: String,
    pub data_range: String,
    pub num_events: usize,
    pub expected_return: f64,
    pub expected_sharpe: f64,
    pub expected_trades: usize,
    pub expected_win_rate: f64,
    
    // Algorithm selection
    pub algorithm: String,                      // NEW: "avellaneda_stoikov" or "ml_spread_skew"
    
    // A-S specific
    pub spread_bps: f64,
    pub skew: f64,
    pub high_entropy_threshold: f64,
    pub low_entropy_threshold: f64,
    
    // ML specific (optional)
    pub ml_weights_version: Option<String>,     // NEW
    pub ml_weights_path: Option<String>,        // NEW
    
    pub fill_prob_assumption: f64,
    pub notes: String,
}

impl ParameterPreset {
    pub fn algorithm_type(&self) -> Result<AlgorithmType, AlgorithmError> {
        AlgorithmType::from_str(&self.algorithm)
    }
    
    pub fn to_algorithm(
        &self,
    ) -> Result<Box<dyn MarketMakingAlgorithm>, AlgorithmError> {
        let algo_type = self.algorithm_type()?;
        
        match algo_type {
            AlgorithmType::AvellanedaStoikov => {
                let config = AvellanedaStoikovConfig {
                    regime_params: RegimeParams::uniform(self.spread_bps, self.skew),
                    regime_thresholds: RegimeThresholds {
                        high_entropy_threshold: self.high_entropy_threshold,
                        low_entropy_threshold: self.low_entropy_threshold,
                    },
                    ..Default::default()
                };
                Ok(Box::new(AvellanedaStoikovMM::new(config)))
            }
            AlgorithmType::MLSpreadSkew => {
                let weights = if let Some(path) = &self.ml_weights_path {
                    MLModelWeights::load_from_file(path)
                        .unwrap_or_else(|_| MLModelWeights::default())
                } else {
                    MLModelWeights::default()
                };
                Ok(create_ml_algorithm(dec!(0.1), dec!(0.001), weights)?)
            }
        }
    }
}
```

**Update default presets** (presets.rs lines 154-173):
```rust
let mut best = ParameterPreset::new(
    "GridSearch-Best",
    "grid-search",
    1.0,    // spread
    0.3,    // skew
    0.7,    // entropy threshold
    0.10,   // fill prob
);
best.algorithm = "avellaneda_stoikov".to_string();  // NEW
// ... rest of fields ...
```

---

## Step 6: Update Preset Selection UI

### Current State (tui.rs lines 1862-1900)
Shows list of presets with their parameters.

### Required Changes

**Display algorithm in preset menu**:
```rust
pub fn menu_description(&self) -> String {
    let algo_name = self.algorithm_type()
        .map(|a| a.display_name())
        .unwrap_or("unknown");
    
    format!(
        "{} ({}): {} | spread={:.1}bps, skew={:.1}, exp={:+.1}%",
        self.name,
        self.created_at_local(),
        algo_name,  // NEW
        self.spread_bps,
        self.skew,
        self.expected_return * 100.0
    )
}
```

---

## Wiring in MLSpreadSkewAlgorithm Specifically

### Key Points for ML Algorithm

1. **Model weights are the key parameter**:
   - Located in: `src/algorithms/ml_spread_skew.rs` lines 116-144
   - Default weights provided (SpreadWeights + SkewWeights)
   - Can load from JSON file or set programmatically

2. **Linear model structure**:
   ```
   spread_bps = intercept + w_entropy*entropy + w_volatility*volatility
              + w_imbalance*imbalance + w_interaction*(entropy*volatility)
   
   skew = intercept + w_entropy*entropy + w_volatility*volatility
        + w_imbalance*imbalance + w_inventory*inventory_ratio
   ```

3. **Feature requirements** (from MarketInput):
   - best_bid, best_ask (for mid price)
   - volatility (from realized_volatility_100)
   - entropy (from entropy score)
   - book_imbalance (from order flow or price-weighted imbalance)

4. **Integration in main.rs**:
   ```rust
   // At top of main
   use ingestor::algorithms::*;
   
   // Parse CLI args
   let args = parse_args();  // CLI argument parsing
   
   // Create algorithm
   let algo: Box<dyn MarketMakingAlgorithm> = if args.algorithm == "ml" {
       let weights = if let Some(path) = args.ml_weights {
           MLModelWeights::load_from_file(path)?
       } else {
           MLModelWeights::default()
       };
       create_ml_algorithm(dec!(0.1), dec!(0.001), weights)?
   } else {
       create_algorithm(
           AlgorithmType::AvellanedaStoikov,
           dec!(0.1),
           dec!(0.001),
           None,
       )?
   };
   ```

5. **Model storage**:
   - Weights can be saved to `./data/models/` directory
   - JSON format with metadata (version, training info)
   - Load at startup or switch between runs

---

## Testing Strategy for Algorithm Switch

### Unit Tests Needed
1. **Algorithm factory tests**: Verify `create_algorithm()` returns correct types
2. **MarketInput conversion**: Ensure feature data converts correctly to algorithm input
3. **Quote output consistency**: Same inputs → same outputs across algorithm versions

### Integration Tests Needed
1. **LiveMM mode with A-S**: Baseline behavior unchanged
2. **LiveMM mode with ML**: ML algorithm produces reasonable quotes
3. **Preset loading**: Presets correctly instantiate algorithms
4. **Algorithm switching**: Switching between algorithms in TUI

### Manual Testing Checklist
- [ ] Start with `--algorithm avellaneda-stoikov`: produces quotes
- [ ] Start with `--algorithm ml-spread-skew`: produces quotes
- [ ] Preset selection shows algorithm name
- [ ] Quotes differ between algorithms with same inputs
- [ ] PnL tracking works with both algorithms
- [ ] No panic on algorithm switch

---

## Backward Compatibility Considerations

### Keep Existing Users Happy
1. **Default to A-S**: If no `--algorithm` specified, use Avellaneda-Stoikov
2. **Preserve preset JSON**: Old presets without algorithm field default to A-S
3. **Existing TUI workflows**: Main menu, preset selection, display should feel same

### Migration Path
1. Version 0.2.0: Add algorithm field to presets, default to A-S
2. Version 0.3.0: ML algorithm production-ready
3. Version 0.4.0: Support hybrid/ensemble modes (future)

---

## File Changes Summary

### Files to Modify (Research-Only List)

| File | Changes | Lines | Priority |
|------|---------|-------|----------|
| src/main.rs | CLI arg parsing, pass algo type to TUI | 37-60 | HIGH |
| src/tui.rs | Accept algo type, create algorithm, display algo info | 481+, 1299+, 520+ | HIGH |
| src/mm_simulator.rs | Accept trait object instead of concrete type | 186-199 | HIGH |
| src/presets.rs | Add algorithm field to preset, conversion logic | 15-82 | MEDIUM |
| Cargo.toml | No changes (clap already included) | N/A | N/A |

### Files That Don't Need Changes
- src/market_maker.rs: Already stable, used through wrapper
- src/algorithms/*.rs: Already implemented and tested
- src/feature_fusion.rs: No changes needed
- src/orderbook.rs: No changes needed
- src/entropy.rs: No changes needed

---

## Configuration File Examples

### CLI Example
```bash
# Start with default A-S
cargo run --release

# Start with A-S explicitly
cargo run --release -- --algorithm avellaneda-stoikov

# Start with ML using default weights
cargo run --release -- --algorithm ml-spread-skew

# Start with ML using trained weights
cargo run --release -- --algorithm ml-spread-skew \
    --ml-weights ./data/models/btc_v1.json

# Start with different symbol
cargo run --release -- --symbol ETHUSDT --algorithm ml-spread-skew
```

### Preset JSON Example
```json
{
  "presets": [
    {
      "name": "ML-BTCUSDT-v1",
      "created_at": "2025-12-06T12:00:00Z",
      "algorithm": "ml_spread_skew",
      "ml_weights_version": "1.0.0",
      "ml_weights_path": "./data/models/btc_v1.json",
      "spread_bps": 2.0,
      "skew": 0.5,
      "expected_return": 0.0742,
      "expected_sharpe": -0.95,
      "expected_trades": 612,
      "expected_win_rate": 0.612,
      "notes": "Trained on Oct-Dec data"
    }
  ]
}
```

---

## Summary of Integration Points

1. **main.rs**: Entry point for algorithm selection
2. **TUI init**: Receives algorithm type and creates instance
3. **PaperTradingEngine**: Holds trait object, calls compute_quotes
4. **Preset system**: Stores and loads algorithm choice
5. **Display**: Shows which algorithm is active

The architecture is already designed for this; it just needs to be wired together.
