# ML Algorithms and Experiment Framework Guide

## Overview

This document details the machine learning (non-deep learning) algorithms supported in the evolutionary trading system, with special focus on Support Vector Machines (SVM) and kernel filtering for time delay compensation.

---

## 1. Supported ML Algorithms

### 1.1 Support Vector Machines (SVM)

**Primary Use Case**: Binary classification (bullish/bearish/neutral from three-bar classification)

**Kernel Types:**

#### Linear Kernel
- **Formula**: `K(x, y) = x^T y`
- **Use Case**: Linearly separable data, fast inference
- **Hyperparameters**: `C` (regularization parameter)
- **When to use**: Simple patterns, interpretable models

#### RBF (Radial Basis Function) Kernel
- **Formula**: `K(x, y) = exp(-γ ||x - y||²)`
- **Use Case**: Non-linear patterns, complex decision boundaries
- **Hyperparameters**: `C`, `gamma` (γ)
- **When to use**: Most common choice, handles complex relationships
- **Gamma interpretation**:
  - Low γ: Smooth decision boundary, larger influence radius
  - High γ: Complex boundary, localized influence

#### Polynomial Kernel
- **Formula**: `K(x, y) = (γ x^T y + r)^d`
- **Use Case**: Polynomial feature interactions
- **Hyperparameters**: `C`, `gamma` (γ), `degree` (d), `coef0` (r)
- **When to use**: Need to capture polynomial relationships
- **Degree options**: Typically 2, 3, 4, 5

#### Sigmoid Kernel
- **Formula**: `K(x, y) = tanh(γ x^T y + r)`
- **Use Case**: Neural network-like behavior
- **Hyperparameters**: `C`, `gamma` (γ), `coef0` (r)
- **When to use**: Less common, can be unstable
- **Note**: Not always valid kernel (Mercer condition)

**SVM Configuration in Genotype:**
```rust
MLAlgorithm::SVM {
    kernel: SVMKernel::RBF { gamma: 0.001 },
    c: 10.0,
    gamma: Some(0.001),  // For RBF/Polynomial/Sigmoid
    degree: None,         // For Polynomial only
}
```

### 1.2 Tree-Based Algorithms

#### XGBoost
- **Use Case**: Gradient boosting, handles non-linearity
- **Hyperparameters**: `max_depth`, `learning_rate`, `n_estimators`, `subsample`, `colsample_bytree`
- **Strengths**: Fast, handles missing values, feature importance

#### LightGBM
- **Use Case**: Fast gradient boosting, large datasets
- **Hyperparameters**: `num_leaves`, `learning_rate`, `n_estimators`, `min_data_in_leaf`
- **Strengths**: Very fast, memory efficient, good accuracy

#### Random Forest
- **Use Case**: Ensemble of decision trees
- **Hyperparameters**: `n_estimators`, `max_depth`, `min_samples_split`, `min_samples_leaf`
- **Strengths**: Robust, feature importance, handles non-linearity

### 1.3 Linear Models

#### Logistic Regression
- **Use Case**: Linear classification baseline
- **Hyperparameters**: `penalty` (L1/L2), `C`, `solver`
- **Strengths**: Fast, interpretable, good baseline

#### Ridge/Lasso/Elastic Net
- **Use Case**: Regularized linear models
- **Hyperparameters**: `alpha` (regularization strength), `l1_ratio` (for Elastic Net)
- **Strengths**: Prevents overfitting, feature selection (Lasso)

---

## 2. Kernel Filtering for Time Delay Compensation

### 2.1 Purpose

Kernel filtering is used to:
- **Compensate for time delays** in market data feeds
- **Smooth noisy signals** before ML model inference
- **Extract trend components** from noisy features
- **Prepare features** for better ML model performance

### 2.2 Kernel Types for Filtering

#### Gaussian Kernel
- **Formula**: `K(x) = exp(-x² / (2σ²))`
- **Use Case**: Smoothing, general-purpose filtering
- **Parameter**: `sigma` (σ) - controls smoothing width
- **When to use**: Default choice, well-behaved

#### Exponential Kernel
- **Formula**: `K(x) = exp(-λ|x|)`
- **Use Case**: Exponential decay weighting
- **Parameter**: `lambda` (λ) - decay rate
- **When to use**: Recent data more important

#### Polynomial Kernel
- **Formula**: `K(x) = (1 - x²)^d` (Epanechnikov-like)
- **Use Case**: Local polynomial fitting
- **Parameter**: `degree` (d), `c` (coefficient)
- **When to use**: Need polynomial trend extraction

#### RBF Kernel
- **Formula**: `K(x) = exp(-γ x²)`
- **Use Case**: Similar to Gaussian, different parameterization
- **Parameter**: `gamma` (γ)
- **When to use**: Alternative to Gaussian

#### Epanechnikov Kernel
- **Formula**: `K(x) = (1 - x²) * I(|x| ≤ 1)`
- **Use Case**: Optimal for mean squared error
- **Parameter**: `bandwidth`
- **When to use**: Theoretical optimality desired

### 2.3 Implementation

```rust
// crates/kernel_filter/src/lib.rs
pub struct KernelFilter {
    kernel_type: KernelType,
    window_size: usize,           // Number of past states to use
    delay_compensation: Duration,  // Time delay to compensate
}

impl KernelFilter {
    pub fn filter(&self, state_history: &[FeatureState]) -> KernelFilteredState {
        // Apply kernel weighting to historical states
        // Compensate for known delays
        // Return smoothed/compensated state
    }
    
    pub fn compensate_delay(&self, state: &FeatureState) -> FeatureState {
        // Estimate what state should be at current time
        // Based on historical trend
    }
}
```

### 2.4 Integration with ML Pipeline

```
FeatureState (raw)
    ↓
Kernel Filter (time delay compensation)
    ↓
KernelFilteredState (smoothed)
    ↓
Feature Engineering
    ↓
ML Model Inference (SVM, XGBoost, etc.)
    ↓
Trading Decision
```

---

## 3. Experiment Framework

### 3.1 Experiment Configuration

```rust
pub struct ExperimentConfig {
    // Algorithms to test
    pub algorithms: Vec<MLAlgorithm>,
    
    // Feature sets to try
    pub feature_sets: Vec<FeatureSet>,
    
    // Cross-validation config
    pub cross_validation: CrossValidationConfig {
        folds: usize,  // e.g., 5-fold CV
        shuffle: bool,
        stratify: bool,
    },
    
    // Metrics to compute
    pub metrics: Vec<Metric>,  // Accuracy, Precision, Recall, F1, AUC, Sharpe
}

pub struct FeatureSet {
    pub name: String,
    pub features: Vec<FeatureSelector>,
    pub kernel_filter: Option<KernelFilterConfig>,
}
```

### 3.2 Experiment Workflow

1. **Data Preparation**
   - Load labeled historical data
   - Apply kernel filtering (if configured)
   - Split into train/validation/test

2. **Algorithm Testing**
   - For each algorithm:
     - For each feature set:
       - Hyperparameter grid search
       - Cross-validation
       - Record metrics

3. **Model Selection**
   - Compare all algorithm/feature combinations
   - Select best based on validation metrics
   - Train final model on full training set

4. **Results Storage**
   - Model artifact (ONNX format)
   - Performance metrics
   - Hyperparameters
   - Feature importance (if available)

### 3.3 Example: SVM Kernel Comparison

```rust
let experiment = ExperimentConfig {
    algorithms: vec![
        MLAlgorithm::SVM {
            kernel: SVMKernel::Linear,
            c: 1.0,
            gamma: None,
            degree: None,
        },
        MLAlgorithm::SVM {
            kernel: SVMKernel::RBF { gamma: 0.001 },
            c: 10.0,
            gamma: Some(0.001),
            degree: None,
        },
        MLAlgorithm::SVM {
            kernel: SVMKernel::RBF { gamma: 0.01 },
            c: 10.0,
            gamma: Some(0.01),
            degree: None,
        },
        MLAlgorithm::SVM {
            kernel: SVMKernel::Polynomial {
                degree: 2,
                gamma: 0.001,
                coef0: 0.0,
            },
            c: 10.0,
            gamma: Some(0.001),
            degree: Some(2),
        },
    ],
    feature_sets: vec![
        FeatureSet {
            name: "all_features".to_string(),
            features: all_features(),
            kernel_filter: Some(KernelFilterConfig::Gaussian { sigma: 0.5 }),
        },
        FeatureSet {
            name: "selected_features".to_string(),
            features: selected_features(),
            kernel_filter: None,
        },
    ],
    cross_validation: CrossValidationConfig {
        folds: 5,
        shuffle: true,
        stratify: true,
    },
    metrics: vec![
        Metric::Accuracy,
        Metric::Precision,
        Metric::Recall,
        Metric::F1,
        Metric::AUC,
    ],
};

let results = experiment_runner.run_experiments(&labeled_data);
let best_model = results.iter().max_by_key(|r| r.f1_score);
```

### 3.4 Hyperparameter Tuning

**Grid Search:**
```rust
pub struct HyperparameterGrid {
    c_values: Vec<f64>,      // [0.1, 1.0, 10.0, 100.0]
    gamma_values: Vec<f64>,  // [0.0001, 0.001, 0.01, 0.1, 1.0]
    degree_values: Vec<usize>, // [2, 3, 4, 5]
}

pub fn grid_search_svm(
    data: &LabeledData,
    kernel: SVMKernel,
    grid: HyperparameterGrid,
) -> (SVMModel, ValidationMetrics);
```

**Random Search:**
```rust
pub struct HyperparameterSpace {
    c_range: (f64, f64),      // (min, max) for log-uniform
    gamma_range: (f64, f64),  // (min, max) for log-uniform
    n_iterations: usize,
}

pub fn random_search_svm(
    data: &LabeledData,
    kernel: SVMKernel,
    space: HyperparameterSpace,
) -> (SVMModel, ValidationMetrics);
```

---

## 4. Genotype Expression

### 4.1 ML Algorithm in Genotype

```rust
pub struct Genotype {
    // ... other fields ...
    
    pub ml_algorithm: MLAlgorithm,
    pub ml_hyperparameters: HashMap<String, HyperparameterValue>,
    pub kernel_filter: Option<KernelFilterConfig>,
}

// Example genotype JSON
{
  "ml_algorithm": {
    "type": "SVM",
    "kernel": {
      "type": "RBF",
      "gamma": 0.001
    },
    "c": 10.0
  },
  "kernel_filter": {
    "kernel_type": "Gaussian",
    "sigma": 0.5,
    "window_size": 10,
    "delay_compensation_ms": 50
  }
}
```

### 4.2 Evolutionary Operators

**Mutation:**
- Change kernel type (Linear → RBF → Polynomial)
- Adjust hyperparameters (C, gamma, degree)
- Enable/disable kernel filtering
- Change kernel filter parameters

**Crossover:**
- Combine hyperparameters from two parents
- Mix kernel types (if both parents use same algorithm)
- Average kernel filter parameters

**Selection:**
- Prefer genotypes with better ML model performance
- Fitness includes ML model metrics (accuracy, F1, Sharpe)

---

## 5. Implementation Recommendations

### 5.1 Rust Libraries

**SVM:**
- `linfa-svm` (Rust-native, limited)
- Python bridge via `pyo3` (scikit-learn)
- ONNX Runtime (load pre-trained models)

**Tree Models:**
- `linfa` (Rust-native, limited)
- Python bridge (XGBoost, LightGBM)
- ONNX Runtime

**Kernel Filtering:**
- Custom Rust implementation (recommended)
- Use `ndarray` for matrix operations

### 5.2 RPC Service Interface

```rust
// crates/api/proto/ml_training.proto
service MLTraining {
    rpc TrainSVM(SVMTrainRequest) returns (TrainResponse);
    rpc TrainXGBoost(XGBoostTrainRequest) returns (TrainResponse);
    rpc RunExperiment(ExperimentRequest) returns (ExperimentResponse);
    rpc ValidateModel(ValidateRequest) returns (ValidateResponse);
}

message SVMTrainRequest {
    repeated FeatureVector features = 1;
    repeated int32 labels = 2;
    SVMKernel kernel = 3;
    double c = 4;
    optional double gamma = 5;
    optional int32 degree = 6;
}

message ExperimentRequest {
    repeated LabeledData data = 1;
    ExperimentConfig config = 2;
}
```

### 5.3 Model Storage

**Format**: ONNX (Open Neural Network Exchange)
- Portable across languages
- Fast inference
- Standard format

**Storage:**
- Local: `models/{genotype_id}/{algorithm_type}.onnx`
- Cloud: S3/GCS bucket for backup

**Metadata:**
- Stored in database (PostgreSQL)
- Includes: algorithm type, hyperparameters, performance metrics, feature list

---

## 6. Best Practices

### 6.1 SVM Kernel Selection

1. **Start with RBF**: Most versatile, good default
2. **Try Linear**: Fast, interpretable, good baseline
3. **Try Polynomial**: If feature interactions important
4. **Avoid Sigmoid**: Unstable, rarely better than RBF

### 6.2 Hyperparameter Tuning

1. **Grid Search**: Exhaustive but slow
2. **Random Search**: Faster, often better
3. **Bayesian Optimization**: Best for expensive evaluations
4. **Cross-Validation**: Always use, prevents overfitting

### 6.3 Kernel Filtering

1. **Start without**: Baseline performance
2. **Add Gaussian**: Most common, well-behaved
3. **Tune sigma**: Balance smoothing vs. responsiveness
4. **Measure impact**: Compare with/without filtering

### 6.4 Feature Engineering

1. **Normalize features**: Required for SVM
2. **Handle missing values**: Tree models handle this, SVM doesn't
3. **Feature selection**: Reduce dimensionality
4. **Feature importance**: Use tree models to identify important features

---

## 7. Example Workflow

```
1. Label historical data (three-bar classification)
   ↓
2. Apply kernel filtering (time delay compensation)
   ↓
3. Run experiment:
   - Test SVM with Linear, RBF, Polynomial kernels
   - Test XGBoost, LightGBM
   - Cross-validate all combinations
   ↓
4. Select best model (highest F1 score + Sharpe)
   ↓
5. Train final model on full dataset
   ↓
6. Store model + config in genotype
   ↓
7. Deploy phenotype with selected model
   ↓
8. Monitor performance, evolve if needed
```

---

## 8. Future Extensions

- **Ensemble Methods**: Combine multiple models (voting, stacking)
- **Feature Engineering**: Automated feature selection/generation
- **Online Learning**: Update models incrementally
- **Multi-class Classification**: Extend beyond binary (bullish/bearish/neutral)
- **Regression Models**: For price prediction (not just classification)

