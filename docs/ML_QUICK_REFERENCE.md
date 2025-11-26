# ML Algorithms & Kernel Filtering - Quick Reference

## Overview

The evolutionary trading system supports multiple ML algorithms (non-deep learning) with special focus on:
- **Support Vector Machines (SVM)** with 4 kernel types
- **Kernel filtering** for time delay compensation
- **Experiment framework** for automated algorithm comparison

---

## SVM Kernels

| Kernel | Formula | Hyperparameters | Use Case |
|--------|---------|----------------|----------|
| **Linear** | `K(x,y) = x^T y` | `C` | Linearly separable, fast |
| **RBF** | `K(x,y) = exp(-γ||x-y||²)` | `C`, `gamma` | Non-linear, most common |
| **Polynomial** | `K(x,y) = (γx^T y + r)^d` | `C`, `gamma`, `degree`, `coef0` | Feature interactions |
| **Sigmoid** | `K(x,y) = tanh(γx^T y + r)` | `C`, `gamma`, `coef0` | Neural-like, less common |

**Genotype Expression:**
```json
{
  "ml_algorithm": {
    "type": "SVM",
    "kernel": {
      "type": "RBF",
      "gamma": 0.001
    },
    "c": 10.0
  }
}
```

---

## Kernel Filtering

**Purpose**: Time delay compensation and signal smoothing

| Kernel Type | Parameter | Use Case |
|-------------|-----------|----------|
| **Gaussian** | `sigma` | General smoothing, default |
| **Exponential** | `lambda` | Recent data weighted more |
| **Polynomial** | `degree`, `c` | Trend extraction |
| **RBF** | `gamma` | Similar to Gaussian |
| **Epanechnikov** | `bandwidth` | Optimal MSE |

**Genotype Expression:**
```json
{
  "kernel_filter": {
    "kernel_type": "Gaussian",
    "sigma": 0.5,
    "window_size": 10,
    "delay_compensation_ms": 50
  }
}
```

---

## Supported ML Algorithms

1. **SVM** - Linear, RBF, Polynomial, Sigmoid kernels
2. **XGBoost** - Gradient boosting
3. **LightGBM** - Fast gradient boosting
4. **Random Forest** - Ensemble trees
5. **Logistic Regression** - Linear baseline

**All algorithms** are expressed in genotype and can evolve via GA/PSO.

---

## Experiment Framework

**Workflow:**
1. Load labeled data (three-bar classification)
2. Apply kernel filtering (optional)
3. Run experiments:
   - Test all algorithms
   - Grid search hyperparameters
   - Cross-validate (5-fold)
4. Select best model
5. Store in genotype

**Example:**
```rust
let experiment = ExperimentConfig {
    algorithms: vec![
        MLAlgorithm::SVM { kernel: SVMKernel::Linear, ... },
        MLAlgorithm::SVM { kernel: SVMKernel::RBF { gamma: 0.001 }, ... },
        MLAlgorithm::XGBoost { ... },
    ],
    feature_sets: vec![...],
    cross_validation: CrossValidationConfig { folds: 5, ... },
};
```

---

## Genotype Structure

```rust
pub struct Genotype {
    // ... other fields ...
    
    // ML algorithm selection
    pub ml_algorithm: MLAlgorithm,
    pub ml_hyperparameters: HashMap<String, HyperparameterValue>,
    
    // Kernel filtering
    pub kernel_filter: Option<KernelFilterConfig>,
}
```

**Evolution:**
- **Mutation**: Change kernel type, adjust hyperparameters
- **Crossover**: Combine hyperparameters from parents
- **Selection**: Prefer better ML performance

---

## Data Flow

```
FeatureState (raw)
    ↓
Kernel Filter (time delay compensation)
    ↓
KernelFilteredState (smoothed)
    ↓
ML Model Inference (SVM/XGBoost/etc.)
    ↓
Trading Decision
```

---

## Implementation Notes

**Rust Libraries:**
- SVM: `linfa-svm` or Python bridge (scikit-learn)
- Tree models: `linfa` or Python bridge
- Kernel filtering: Custom Rust implementation

**RPC Service:**
- `TrainSVM` - Train SVM with specified kernel
- `RunExperiment` - Test multiple algorithms
- `ValidateModel` - Cross-validation

**Model Storage:**
- Format: ONNX (portable)
- Location: `models/{genotype_id}/{algorithm}.onnx`
- Metadata: Database (algorithm, hyperparameters, metrics)

---

## Best Practices

1. **Start with RBF kernel** - Most versatile
2. **Try Linear** - Fast baseline
3. **Grid search hyperparameters** - Exhaustive but slow
4. **Use cross-validation** - Prevents overfitting
5. **Compare all algorithms** - Use experiment framework
6. **Kernel filtering optional** - Measure impact first

---

## References

- **Full Guide**: `docs/ML_ALGORITHMS_GUIDE.md`
- **Architecture**: `docs/ARCHITECTURE.md` Section 4.8-4.11
- **Component Diagram**: `docs/COMPONENT_DIAGRAM.md`

