# Evolutionary Trading System - Architecture Document

## Executive Summary

This document defines the architecture for an evolutionary algorithmic trading system that:
- Processes real-time market data (orderbook, trades, illiquidity, entropy)
- Uses Kalman filters for prediction
- Employs evolutionary algorithms (GA/PSO) to evolve trading strategies
- Implements ML model generation and validation
- Manages multi-pair portfolios with entropy regime detection
- Integrates with Order Management Systems (OMS)

---

## 1. Repository Structure Decision

### **Recommendation: Monorepo (Single Repository)**

**Rationale:**
- **Tight coupling**: Components share data structures, configs, and interfaces
- **Atomic changes**: Evolutionary system changes affect multiple modules simultaneously
- **Simpler CI/CD**: Single pipeline for testing, validation, deployment
- **Easier refactoring**: Genotype/phenotype changes propagate across modules
- **Version consistency**: All components stay in sync

**Structure:**
```
trading-system/
├── crates/
│   ├── ingestor/          # Existing data ingestion (your current code)
│   ├── state/             # State vector management
│   ├── kalman/            # Kalman filter implementations
│   ├── kernel_filter/     # Kernel filtering for time delay compensation
│   ├── persistence/       # Multi-backend persistence
│   ├── labeling/          # Three-bar classification
│   ├── genotype/          # Genotype definition & encoding
│   ├── phenotype/         # Phenotype compilation
│   ├── evolution/         # Evolutionary algorithms
│   ├── fitness/           # Fitness metrics
│   ├── model_gen/         # AutoML model generation (SVM, XGBoost, etc.)
│   ├── ml_models/         # ML model implementations (SVM, tree-based, etc.)
│   ├── experiments/       # Experiment framework for ML algorithm testing
│   ├── validator/         # Forward testing
│   ├── entropy_regime/    # Entropy detection
│   ├── oms/               # Order Management System
│   ├── portfolio/         # Multi-pair management
│   └── api/               # poem-openapi REST API
├── config/
│   ├── genotypes/         # Genotype configurations
│   └── environments/      # Dev/staging/prod configs
├── data/
│   ├── local/             # Local parquet storage
│   └── cloud/             # Cloud sync configs
├── scripts/
│   ├── backtest/          # Backtesting scripts
│   └── deployment/        # Deployment scripts
├── tests/
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── forward/            # Forward testing
└── docs/                  # Architecture, API docs
```

**Alternative: Multi-Repo (if team scales)**
- `trading-ingestor/` - Data ingestion
- `trading-core/` - State, Kalman, persistence
- `trading-evolution/` - Evolutionary algorithms
- `trading-oms/` - Order management
- `trading-api/` - REST API layer

**Decision**: Start monorepo, split later if needed (team > 5 people)

---

## 2. Data Pipeline Architecture

### 2.1 Pipeline Flow

```
┌─────────────────┐
│  Market Feeds   │ (Binance WebSocket)
│  (WebSocket)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Ingestor      │ ← Your existing code
│  (Real-time)    │   - Orderbook updates
│                 │   - Trade updates
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Feature Engine │
│  (100ms ticks)  │   - Orderbook features
│                 │   - Trade features
│                 │   - Illiquidity metrics
│                 │   - Entropy metrics
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  State Vector   │
│  (FeatureState) │   - Aggregated snapshot
│                 │   - Timestamped
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌─────────┐ ┌──────────────┐
│ Kalman  │ │ Persistence  │
│ Filter  │ │ (Local+Cloud)│
└────┬────┘ └──────────────┘
     │
     ▼
┌─────────────────┐
│ Kernel Filter   │   - Time delay compensation
│ (Signal Proc)   │   - Multiple kernel types
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│  Entropy        │
│  Regime Detector│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Genotype       │
│  Evaluator      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Phenotype      │
│  (Trading Logic)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      OMS        │
│  (Order Exec)   │
└─────────────────┘
```

### 2.2 Data Storage Strategy

**Local (Primary):**
- **Format**: Parquet files (compressed, columnar)
- **Location**: `data/local/features/`
- **Retention**: Last 30 days (rolling window)
- **Purpose**: Fast access, backtesting, development

**Cloud (Archive + Backup):**
- **Format**: Parquet files in object storage
- **Services**: AWS S3 / GCS / Azure Blob
- **Retention**: All historical data
- **Purpose**: Long-term storage, disaster recovery, distributed access

**Database (Metadata):**
- **Service**: PostgreSQL / SQLite (for small deployments)
- **Purpose**: Genotype configurations, fitness metrics, algorithm lifecycle
- **Schema**: Genotypes, phenotypes, fitness_history, algorithm_registry

---

## 3. Cloud Services Architecture

### 3.1 Compute Strategy

**Option A: RPC to Larger Computer (Sufficient for MVP)**

**Architecture:**
```
┌──────────────────┐         gRPC/HTTP          ┌──────────────────┐
│  Trading System  │ ────────────────────────►  │  Compute Server  │
│  (Local/Cloud)   │                            │  (GPU/CPU)       │
│                  │                            │                  │
│  - Ingestor      │                            │  - ML Training   │
│  - State Mgmt    │                            │  - Backtesting   │
│  - Evolution     │                            │  - Validation    │
│  - OMS           │                            │  - Model Gen     │
└──────────────────┘                            └──────────────────┘
```

**When RPC is enough:**
- ✅ ML training is batch (not real-time)
- ✅ Backtesting is offline
- ✅ Model generation is periodic
- ✅ You have control over compute server

**RPC Implementation (Bidirectional):**
- **Protocol**: gRPC (efficient, typed) or HTTP/REST (simpler)
- **Library**: `tonic` (gRPC) or `poem-openapi` (REST)
- **Services**:
  - `ModelTrainingService` - Train ML models (bidirectional: request + model response)
  - `BacktestService` - Run historical validation
  - `ValidationService` - Forward testing
  - `GenotypeEvaluationService` - Evaluate fitness
  - `ModelUpdateService` - Push model updates to trading system

**Example gRPC Service (Bidirectional):**
```rust
// crates/api/proto/trading.proto
service ModelTraining {
    // Trading System → Compute Server
    rpc TrainModel(TrainRequest) returns (TrainResponse);
    rpc RunExperiment(ExperimentRequest) returns (ExperimentResponse);
    
    // Compute Server → Trading System (via response)
    // TrainResponse/ExperimentResponse includes:
    //   - model_artifact (ONNX binary)
    //   - model_metadata
    //   - performance_metrics
}

service ModelUpdate {
    // Compute Server → Trading System (push notification)
    rpc NotifyModelReady(ModelReadyNotification) returns (Ack);
    
    // Trading System → Compute Server (fetch model)
    rpc FetchModel(FetchModelRequest) returns (ModelResponse);
}

message TrainResponse {
    bytes model_artifact = 1;  // ONNX binary - sent back to trading system
    ModelMetadata metadata = 2;
    ValidationMetrics metrics = 3;
    string model_version = 4;
}
```

### 3.2 Cloud Services Recommendations

#### **Minimal Setup (RPC Only)**
- **Compute Server**: Your own machine / cloud VM (AWS EC2, GCP Compute Engine)
- **Storage**: Local filesystem + optional S3/GCS for backup
- **Database**: SQLite (embedded) or PostgreSQL (if multi-instance)

#### **Production Setup (Full Cloud)**

**AWS Stack:**
- **Compute**: 
  - EC2 (for trading system)
  - ECS/EKS (for containerized services)
  - SageMaker (optional, for ML training)
- **Storage**: 
  - S3 (parquet files, model artifacts)
  - EFS (shared filesystem if needed)
- **Database**: 
  - RDS PostgreSQL (genotype/fitness metadata)
  - DynamoDB (optional, for high-throughput metrics)
- **Messaging**: 
  - SQS (task queues for ML training)
  - EventBridge (event-driven architecture)
- **Monitoring**: 
  - CloudWatch (metrics, logs)
  - X-Ray (distributed tracing)

**GCP Stack:**
- **Compute**: 
  - Compute Engine / GKE
  - Vertex AI (ML training)
- **Storage**: 
  - Cloud Storage (GCS) - parquet files
  - Cloud SQL (PostgreSQL)
- **Messaging**: 
  - Pub/Sub (event streaming)
- **Monitoring**: 
  - Cloud Monitoring / Cloud Logging

**Azure Stack:**
- **Compute**: 
  - Virtual Machines / AKS
  - Azure ML (ML training)
- **Storage**: 
  - Blob Storage (parquet files)
  - Azure SQL Database
- **Messaging**: 
  - Service Bus / Event Hubs
- **Monitoring**: 
  - Azure Monitor / Application Insights

### 3.3 Hybrid Approach (Recommended)

**For Embedded Developer Transition:**

```
┌─────────────────────────────────────────────────────────┐
│  Trading System (Local Development / Staging)            │
│  - Ingestor (real-time)                                 │
│  - State management                                     │
│  - Evolution engine                                     │
│  - Phenotype execution                                  │
│  - OMS                                                  │
└───────────────────┬─────────────────────────────────────┘
                    │
                    │ Bidirectional RPC
                    │ (gRPC/HTTP)
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
   Request:              Response:
   Features + Labels    Model Artifact
   Algorithm Config      + Metadata
        │                       │
        └───────────┬───────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│  Remote Compute (Cloud VM / Your Server)                │
│  - ML model training (batch)                            │
│  - Experiment framework                                  │
│  - Backtesting (offline)                                │
│  - Forward validation                                   │
│  - Model generation & serialization                      │
└───────────────────┬─────────────────────────────────────┘
                    │
                    │ Store results
                    ▼
┌─────────────────────────────────────────────────────────┐
│  Cloud Storage (S3/GCS/Azure Blob)                      │
│  - Historical parquet files                             │
│  - Trained model artifacts (ONNX)                       │
│  - Backtest results                                     │
│  - Model metadata                                        │
└─────────────────────────────────────────────────────────┘

Model Update Flow:
1. Trading System → Compute Server: Request training
2. Compute Server: Trains model, serializes to ONNX
3. Compute Server → Trading System: Returns model artifact
4. Trading System: Stores model, updates genotype, updates phenotype
5. Trading System: Hot-swaps or restarts with new model
```

**Why this works:**
- Real-time components stay local (low latency)
- Heavy compute offloaded (ML training, backtesting)
- Cloud storage for scalability
- Simple to start, scales later

---

## 4. Component Architecture

### 4.1 State Vector Module

**Purpose**: Central data structure containing all computed features

**Structure:**
```rust
// crates/state/src/lib.rs
pub struct FeatureState {
    pub timestamp: DateTime<Utc>,
    
    // Orderbook features
    pub orderbook: OrderBookState,
    
    // Trade features
    pub trades: TradeState,
    
    // Illiquidity metrics
    pub illiquidity: IlliquidityState,
    
    // Entropy metrics
    pub entropy: EntropyState,
    
    // Kalman predictions (optional, computed on-demand)
    pub kalman_predictions: Option<KalmanPredictions>,
    
    // Kernel-filtered features (time delay compensated)
    pub kernel_filtered: Option<KernelFilteredState>,
    
    // Metadata
    pub pair: String,
    pub sequence_id: u64,
}
```

**Storage**: Serialized to parquet, loaded into memory for processing

### 4.2 Kalman Filter Module

**Purpose**: Multi-dimensional state estimation and prediction

**Implementation:**
```rust
// crates/kalman/src/lib.rs
pub struct MultiDimKalmanFilter {
    state_dim: usize,  // Price, spread, flow, etc.
    process_noise: Matrix,
    measurement_noise: Matrix,
    state_covariance: Matrix,
}

impl MultiDimKalmanFilter {
    pub fn predict(&mut self, measurement: &FeatureState) -> KalmanPredictions;
    pub fn update(&mut self, observation: &FeatureState);
}
```

**Integration**: Called from state module when predictions needed

### 4.3 Persistence Module

**Purpose**: Unified interface for local + cloud storage

**Trait-based design:**
```rust
// crates/persistence/src/traits.rs
pub trait PersistenceBackend: Send + Sync {
    async fn save_features(&self, features: &[FeatureState]) -> Result<()>;
    async fn load_features(&self, time_range: TimeRange) -> Result<Vec<FeatureState>>;
    async fn list_files(&self) -> Result<Vec<String>>;
}

// Implementations
pub struct LocalBackend { base_path: PathBuf }
pub struct S3Backend { bucket: String, client: S3Client }
pub struct GCSBackend { bucket: String, client: GCSClient }
```

**Usage**: Ingestor saves locally, background job syncs to cloud

### 4.4 Genotype/Phenotype System

**Genotype (Configuration):**
```rust
// crates/genotype/src/lib.rs
#[derive(Serialize, Deserialize, Clone)]
pub struct Genotype {
    pub id: String,
    pub version: u64,
    
    // Feature selection
    pub active_features: Vec<FeatureSelector>,
    
    // Kalman config
    pub kalman: KalmanConfig,
    
    // Kernel filter config (for time delay compensation)
    pub kernel_filter: Option<KernelFilterConfig>,
    
    // ML model config (algorithm selection + hyperparameters)
    pub ml_algorithm: MLAlgorithm,  // SVM, XGBoost, LightGBM, etc.
    pub ml_hyperparameters: HashMap<String, f64>,  // Algorithm-specific params
    
    // Trading rules
    pub entry_rules: Vec<Rule>,
    pub exit_rules: Vec<Rule>,
    
    // Entropy thresholds
    pub entropy: EntropyConfig,
    
    // Risk params
    pub risk: RiskConfig,
    
    // Multi-pair weights
    pub pair_weights: HashMap<String, f64>,
}
```

**Phenotype (Executable):**
```rust
// crates/phenotype/src/lib.rs
pub struct Phenotype {
    genotype_id: String,
    model_version: String,  // Track model version for updates
    compiled_logic: CompiledTradingLogic,
    kalman_filter: MultiDimKalmanFilter,
    kernel_filter: Option<KernelFilter>,  // Time delay compensation
    ml_model: Box<dyn MLModel>,  // SVM, XGBoost, etc. (loaded from ONNX)
    ml_algorithm: MLAlgorithm,  // Track which algorithm is used
    model_path: PathBuf,  // Path to ONNX model file
}

impl Phenotype {
    pub fn evaluate(&mut self, state: &FeatureState) -> TradingDecision;
    pub fn update(&mut self, new_state: &FeatureState);
    
    // Model update methods
    pub fn update_model(&mut self, model_path: &Path) -> Result<()>;
    pub fn load_model_from_onnx(&mut self, onnx_data: &[u8]) -> Result<()>;
    pub fn hot_swap_model(&mut self, new_model: Box<dyn MLModel>) -> Result<()>;
}
```

**Compilation**: Genotype → Phenotype happens when algorithm is "born"

**Model Update Flow:**
1. **Model Received**: Trading system receives model via RPC response
2. **Model Stored**: ONNX file saved to local storage + cloud backup
3. **Genotype Updated**: Genotype metadata updated with model info
4. **Phenotype Update**:
   - **Option A (Hot-swap)**: Load new model, swap in place (zero downtime)
   - **Option B (Restart)**: Recompile phenotype with new model, graceful restart
5. **Model Versioning**: Old model kept for rollback if needed
6. **Monitoring**: Track new model performance vs. old model

### 4.5 Evolutionary Engine

**Population Management:**
```rust
// crates/evolution/src/lib.rs
pub struct Population {
    genotypes: Vec<Genotype>,
    fitness_scores: Vec<f64>,
    generation: u64,
}

pub trait EvolutionaryAlgorithm {
    fn evolve(&mut self, population: &mut Population);
    fn crossover(&self, parent1: &Genotype, parent2: &Genotype) -> Genotype;
    fn mutate(&self, genotype: &mut Genotype);
    fn select(&self, population: &Population) -> Vec<usize>;
}
```

**Algorithms:**
- `GeneticAlgorithm` - For discrete/categorical spaces
- `ParticleSwarmOptimization` - For continuous parameters
- `HybridEvolution` - Combines both

### 4.6 Fitness Module

**Fitness Function:**
```rust
// crates/fitness/src/lib.rs
pub struct FitnessMetrics {
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_return: f64,
}

pub fn calculate_fitness(
    trades: &[Trade],
    risk_free_rate: f64,
) -> FitnessMetrics;
```

**Tracking**: Stored in database, used for algorithm lifecycle decisions

### 4.7 Entropy Regime Detector

**Implementation:**
```rust
// crates/entropy_regime/src/lib.rs
pub struct EntropyRegimeDetector {
    windows: Vec<Duration>,
    thresholds: EntropyThresholds,
}

pub enum Regime {
    HighEntropy,    // Chaos, do nothing
    LowEntropy,     // Momentum, trade
    Transition,     // Uncertain, be cautious
}

impl EntropyRegimeDetector {
    pub fn detect(&self, state: &FeatureState) -> Regime;
    pub fn should_trade(&self, regime: Regime) -> bool;
}
```

**Integration**: Called before genotype evaluation, gates trading decisions

### 4.8 Kernel Filter Module

**Purpose**: Time delay compensation using kernel filtering techniques

**Implementation:**
```rust
// crates/kernel_filter/src/lib.rs
pub enum KernelType {
    Gaussian { sigma: f64 },
    Exponential { lambda: f64 },
    Polynomial { degree: usize, c: f64 },
    RBF { gamma: f64 },
    Epanechnikov { bandwidth: f64 },
}

pub struct KernelFilter {
    kernel_type: KernelType,
    window_size: usize,
    delay_compensation: Duration,
}

impl KernelFilter {
    pub fn filter(&self, state_history: &[FeatureState]) -> KernelFilteredState;
    pub fn compensate_delay(&self, state: &FeatureState) -> FeatureState;
}
```

**Use Cases:**
- Compensate for sensor/feed delays
- Smooth noisy signals
- Extract trend components
- Prepare features for ML models

**Integration**: Applied to FeatureState before ML model inference

### 4.9 ML Model Generation Module

**Purpose**: AutoML pipeline supporting multiple ML algorithms (non-deep learning)

**Supported Algorithms:**
- **Support Vector Machines (SVM)**: Linear, RBF, Polynomial, Sigmoid kernels
- **Tree-based**: XGBoost, LightGBM, Random Forest, Gradient Boosting
- **Linear Models**: Logistic Regression, Ridge, Lasso, Elastic Net
- **Ensemble Methods**: Voting, Bagging, Stacking

**Implementation:**
```rust
// crates/model_gen/src/lib.rs
#[derive(Serialize, Deserialize, Clone)]
pub enum MLAlgorithm {
    SVM {
        kernel: SVMKernel,
        c: f64,
        gamma: Option<f64>,
        degree: Option<usize>,
    },
    XGBoost {
        max_depth: usize,
        learning_rate: f64,
        n_estimators: usize,
    },
    LightGBM {
        num_leaves: usize,
        learning_rate: f64,
        n_estimators: usize,
    },
    RandomForest {
        n_estimators: usize,
        max_depth: Option<usize>,
    },
    LogisticRegression {
        penalty: PenaltyType,
        c: f64,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub enum SVMKernel {
    Linear,
    RBF { gamma: f64 },
    Polynomial { degree: usize, gamma: f64, coef0: f64 },
    Sigmoid { gamma: f64, coef0: f64 },
}

pub struct ModelGenerator {
    algorithm: MLAlgorithm,
    feature_selector: FeatureSelector,
    hyperparameter_tuner: HyperparameterTuner,
}

impl ModelGenerator {
    pub fn train(&self, features: &[FeatureState], labels: &[Label]) -> Result<TrainedModel>;
    pub fn validate(&self, model: &TrainedModel, test_data: &[FeatureState]) -> ValidationMetrics;
}
```

**Experiment Framework:**
```rust
// crates/experiments/src/lib.rs
pub struct ExperimentConfig {
    algorithms: Vec<MLAlgorithm>,
    feature_sets: Vec<FeatureSet>,
    cross_validation: CrossValidationConfig,
    metrics: Vec<Metric>,
}

pub struct ExperimentRunner {
    config: ExperimentConfig,
}

impl ExperimentRunner {
    pub fn run_experiments(&self, data: &[LabeledData]) -> Vec<ExperimentResult>;
    pub fn compare_models(&self, results: &[ExperimentResult]) -> ComparisonReport;
}
```

**Integration**: 
- **Training**: Called via RPC for batch training (Trading System → Compute Server)
- **Model Delivery**: Trained model returned via RPC response (Compute Server → Trading System)
- **Phenotype Update**: Model loaded and phenotype recompiled with new model
- **Storage**: Model artifact stored locally + cloud, metadata in genotype

### 4.10 SVM Implementation Details

**Purpose**: Support Vector Machine with multiple kernel options for classification

**Kernel Types:**

1. **Linear Kernel**: `K(x, y) = x^T y`
   - Fast, interpretable
   - Good for linearly separable data
   - Hyperparameters: `C` (regularization)

2. **RBF (Radial Basis Function) Kernel**: `K(x, y) = exp(-γ ||x - y||²)`
   - Most common, handles non-linear patterns
   - Hyperparameters: `C`, `gamma` (γ)
   - Good for complex decision boundaries

3. **Polynomial Kernel**: `K(x, y) = (γ x^T y + r)^d`
   - Captures polynomial relationships
   - Hyperparameters: `C`, `gamma` (γ), `degree` (d), `coef0` (r)
   - Useful for feature interactions

4. **Sigmoid Kernel**: `K(x, y) = tanh(γ x^T y + r)`
   - Neural network-like behavior
   - Hyperparameters: `C`, `gamma` (γ), `coef0` (r)
   - Less common, can be unstable

**Implementation:**
```rust
// crates/ml_models/src/svm.rs
pub struct SVMModel {
    kernel: SVMKernel,
    support_vectors: Vec<FeatureVector>,
    dual_coefficients: Vec<f64>,
    intercept: f64,
    c: f64,
}

impl SVMModel {
    pub fn predict(&self, features: &FeatureVector) -> f64;
    pub fn predict_proba(&self, features: &FeatureVector) -> (f64, f64);  // (prob_class_0, prob_class_1)
}
```

**Hyperparameter Tuning:**
```rust
pub struct SVMHyperparameterSpace {
    c_range: Vec<f64>,  // e.g., [0.1, 1.0, 10.0, 100.0]
    gamma_range: Option<Vec<f64>>,  // For RBF/Polynomial/Sigmoid
    degree_range: Option<Vec<usize>>,  // For Polynomial
    coef0_range: Option<Vec<f64>>,  // For Polynomial/Sigmoid
}

pub fn grid_search_svm(
    data: &LabeledData,
    kernel: SVMKernel,
    param_space: SVMHyperparameterSpace,
) -> (SVMModel, ValidationMetrics);
```

**Experiment Workflow:**
1. Define experiment: Test all 4 kernel types
2. For each kernel:
   - Grid search hyperparameters
   - Cross-validate performance
   - Record metrics (accuracy, precision, recall, F1, AUC)
3. Compare results across kernels
4. Select best kernel + hyperparameters
5. Train final model on full dataset
6. Store in genotype

### 4.11 Genotype Expression of ML Algorithms

**Purpose**: Encode all ML algorithm configurations in genotype for evolution

**Genotype Structure:**
```rust
// crates/genotype/src/ml_config.rs
#[derive(Serialize, Deserialize, Clone)]
pub struct MLAlgorithmConfig {
    pub algorithm_type: MLAlgorithmType,
    pub hyperparameters: HashMap<String, HyperparameterValue>,
    pub feature_selection: FeatureSelectionConfig,
    pub preprocessing: PreprocessingConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum MLAlgorithmType {
    SVM(SVMConfig),
    XGBoost(XGBoostConfig),
    LightGBM(LightGBMConfig),
    RandomForest(RandomForestConfig),
    LogisticRegression(LogisticRegressionConfig),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SVMConfig {
    pub kernel: SVMKernelType,
    pub c: f64,
    pub gamma: Option<f64>,
    pub degree: Option<usize>,
    pub coef0: Option<f64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum SVMKernelType {
    Linear,
    RBF { gamma: f64 },
    Polynomial { degree: usize, gamma: f64, coef0: f64 },
    Sigmoid { gamma: f64, coef0: f64 },
}

#[derive(Serialize, Deserialize, Clone)]
pub enum HyperparameterValue {
    Float(f64),
    Int(i64),
    String(String),
    Bool(bool),
}
```

**Evolutionary Operators:**
- **Mutation**: Change kernel type, adjust hyperparameters
- **Crossover**: Combine hyperparameters from two parents
- **Selection**: Prefer genotypes with better ML model performance

**Example Genotype JSON:**
```json
{
  "ml_algorithm": {
    "algorithm_type": "SVM",
    "config": {
      "kernel": {
        "type": "RBF",
        "gamma": 0.001
      },
      "c": 10.0
    }
  },
  "kernel_filter": {
    "kernel_type": "Gaussian",
    "sigma": 0.5,
    "window_size": 10,
    "delay_compensation_ms": 50
  }
}
```

---

## 5. Technology Stack

### 5.1 Core Technologies

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Language** | Rust | Performance, safety, async support |
| **Async Runtime** | Tokio | Industry standard, full-featured |
| **API Framework** | poem-openapi | Type-safe OpenAPI, async |
| **Data Processing** | Polars | Fast DataFrame operations |
| **Storage (Local)** | Parquet | Columnar, compressed, efficient |
| **Storage (Cloud)** | object_store | Unified S3/GCS/Azure interface |
| **Database** | SQLx + PostgreSQL | Type-safe SQL, async |
| **gRPC** | Tonic | High-performance RPC |
| **Serialization** | Serde | Standard Rust serialization |

### 5.2 ML & Scientific Computing

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Kalman Filters** | Custom Rust impl | Control over performance |
| **Kernel Filtering** | Custom Rust impl | Signal processing, delay compensation |
| **ML Models** | candle (Rust) or ONNX | Rust-native or portable |
| **SVM** | linfa-svm or Python bridge | Support Vector Machines |
| **Tree Models** | linfa or Python bridge | XGBoost, LightGBM, Random Forest |
| **Numerical** | ndarray | Multi-dimensional arrays |
| **Statistics** | statrs | Statistical functions |

### 5.3 Cloud Services

| Service | Provider | Use Case |
|---------|----------|----------|
| **Object Storage** | S3 / GCS / Azure Blob | Parquet file archive |
| **Database** | RDS / Cloud SQL / Azure SQL | Metadata storage |
| **Compute** | EC2 / Compute Engine / VMs | ML training, backtesting |
| **Monitoring** | CloudWatch / GCP Monitoring | Metrics, logs |

---

## 6. Data Flow Examples

### 6.1 Real-Time Trading Flow

```
1. WebSocket receives orderbook update
   ↓
2. Ingestor processes → FeatureState
   ↓
3. State vector updated
   ↓
4. Entropy regime detector checks regime
   ↓
5. If LowEntropy:
   - Kalman filter predicts next state
   - Genotype evaluator checks conditions
   - Phenotype generates trading decision
   ↓
6. If decision != Hold:
   - OMS receives order
   - Order executed
   ↓
7. State persisted (local + cloud async)
```

### 6.2 ML Training Flow (Bidirectional RPC)

**Forward Path (Trading System → Compute Server):**
```
1. Trading System: Labeling pipeline reads historical data
   ↓
2. Trading System: Three-bar classification generates labels
   ↓
3. Trading System: Kernel filtering applied (time delay compensation)
   ↓
4. Trading System → Compute Server (RPC Request):
   - Send: Features + Labels + Algorithm Config
   - Request: Train model with experiment framework
   ↓
5. Compute Server:
   - Runs experiment framework (multiple algorithms)
   - Trains models (SVM with different kernels, XGBoost, etc.)
   - Validates performance (cross-validation)
   - Selects best model
   - Serializes model to ONNX format
```

**Return Path (Compute Server → Trading System):**
```
6. Compute Server → Trading System (RPC Response):
   - Model artifact (ONNX binary)
   - Model metadata (algorithm type, hyperparameters)
   - Performance metrics (accuracy, F1, Sharpe, etc.)
   - Model version ID
   ↓
7. Trading System: Receives model via RPC
   ↓
8. Trading System: Model storage:
   - Save ONNX file locally: models/{genotype_id}/{version}.onnx
   - Upload to cloud storage (S3/GCS) for backup
   - Store metadata in database
   ↓
9. Trading System: Genotype update:
   - Update genotype with:
     * Selected ML algorithm type
     * Hyperparameters
     * Model artifact path
     * Model version ID
     * Performance metrics
   - Store updated genotype in database
   ↓
10. Trading System: Phenotype update:
    - Load new model from ONNX file
    - Recompile phenotype with new model
    - Hot-swap phenotype (if running) or schedule restart
    - Update phenotype's ML model reference
    ↓
11. Trading System: Model deployment:
    - New phenotype now uses updated model
    - Old model kept for rollback if needed
    - Monitor new model performance
```

**Continuous Update Loop:**
```
12. Trading System: Monitor model performance
    ↓
13. If performance degrades OR new data available:
    - Trigger new training cycle (back to step 1)
    - Or: Request model retraining via RPC
    ↓
14. Repeat bidirectional flow
```

**RPC Service Interface (Bidirectional):**
```rust
// crates/api/proto/ml_training.proto
service ModelTraining {
    // Trading System → Compute Server
    rpc TrainModel(TrainRequest) returns (TrainResponse);
    rpc RunExperiment(ExperimentRequest) returns (ExperimentResponse);
    
    // Compute Server → Trading System (via response)
    // Model artifact included in TrainResponse/ExperimentResponse
}

message TrainRequest {
    repeated FeatureVector features = 1;
    repeated int32 labels = 2;
    MLAlgorithmConfig algorithm_config = 3;
    KernelFilterConfig kernel_filter = 4;
    string genotype_id = 5;  // For tracking
}

message TrainResponse {
    bytes model_artifact = 1;  // ONNX binary
    ModelMetadata metadata = 2;
    ValidationMetrics metrics = 3;
    string model_version = 4;
    string model_path = 5;  // Suggested storage path
}

message ModelMetadata {
    MLAlgorithmType algorithm_type = 1;
    map<string, HyperparameterValue> hyperparameters = 2;
    FeatureSelectionConfig feature_selection = 3;
    KernelFilterConfig kernel_filter = 4;
}
```

**Model Update Mechanisms:**

1. **Synchronous Update (Immediate):**
   - RPC response contains model artifact
   - Trading system immediately updates phenotype
   - Hot-swap if possible, else schedule restart

2. **Asynchronous Update (Polling):**
   - Compute server stores model in shared storage
   - Trading system polls for new models
   - Downloads and updates when available

3. **Push Notification:**
   - Compute server notifies trading system when model ready
   - Trading system then requests model via RPC
   - Or: Model pushed via message queue (SQS, Pub/Sub)

### 6.3 Evolutionary Flow

```
1. Population of genotypes exists
   ↓
2. Each genotype → phenotype (compiled)
   ↓
3. Forward testing evaluates fitness
   ↓
4. Fitness scores recorded
   ↓
5. Evolutionary algorithm:
   - Selects best performers
   - Crossover creates offspring
   - Mutation introduces variation
   ↓
6. New generation created
   ↓
7. Best genotypes deployed as phenotypes
   ↓
8. Continuous monitoring:
   - Fitness tracked
   - Underperformers "die"
   - New genotypes "born"
```

---

## 7. Deployment Architecture

### 7.1 Development Environment

```
Local Machine:
├── Ingestor (cargo run)
├── State management
├── Evolution engine
└── OMS (simulated)

Remote (via RPC):
└── ML training / Backtesting
```

### 7.2 Staging Environment

```
Cloud VM / Container:
├── Ingestor
├── State management
├── Evolution engine
├── OMS (paper trading)
└── API server (poem-openapi)

Remote Compute:
└── ML training / Backtesting

Cloud Storage:
└── S3/GCS for data archive
```

### 7.3 Production Environment

```
Kubernetes / ECS:
├── Ingestor (replicated)
├── State management (stateful)
├── Evolution engine
├── OMS (live trading)
└── API server

Managed Services:
├── RDS / Cloud SQL (database)
├── S3 / GCS (object storage)
└── CloudWatch / Monitoring

Remote Compute:
└── ML training cluster
```

---

## 8. CI/CD Pipeline

### 8.1 Pipeline Stages

```yaml
# .github/workflows/ci.yml
name: Trading System CI/CD

on: [push, pull_request]

jobs:
  test:
    - Unit tests
    - Integration tests
    - Linting (clippy)
    - Formatting (rustfmt)
  
  backtest:
    - Load historical data
    - Run backtest suite
    - Validate performance metrics
    - Generate backtest report
  
  forward_test:
    - Deploy to staging
    - Run forward testing (7 days)
    - Validate fitness metrics
    - Compare to baseline
  
  deploy_staging:
    - Build Docker images
    - Deploy to staging environment
    - Smoke tests
  
  deploy_production:
    - Manual approval required
    - Deploy to production
    - Monitor health metrics
```

### 8.2 Testing Strategy

**Unit Tests:**
- Individual component logic
- Genotype/phenotype compilation
- Kalman filter correctness
- Fitness calculations

**Integration Tests:**
- End-to-end data pipeline
- RPC communication
- Persistence backends
- OMS integration

**Backtest Validation:**
- Historical data replay
- Performance metrics validation
- Regime detection accuracy
- ML model performance

**Forward Testing:**
- Out-of-sample validation
- Real-time simulation
- Fitness tracking
- Algorithm lifecycle

---

## 9. Recommendations for Embedded Developer

### 9.1 Starting Point

1. **Keep your existing ingestor** - It's solid
2. **Add state vector module** - Wrap your features
3. **Implement local persistence first** - Get data flowing
4. **Build RPC client/server** - Start with simple HTTP, upgrade to gRPC
5. **Add Kalman filter** - Start with 1D (price), expand to multi-D
6. **Implement basic genotype** - Simple config structure
7. **Build phenotype compiler** - Convert config to executable logic
8. **Add entropy detector** - Gate trading decisions
9. **Implement fitness tracking** - Measure performance
10. **Add evolutionary loop** - Start simple, iterate

### 9.2 Cloud Services Priority

**Phase 1 (MVP):**
- ✅ Local filesystem (parquet)
- ✅ RPC to compute server (your machine or cloud VM)
- ✅ SQLite for metadata

**Phase 2 (Scale):**
- ✅ S3/GCS for archive
- ✅ PostgreSQL for metadata
- ✅ Cloud monitoring

**Phase 3 (Production):**
- ✅ Kubernetes/ECS
- ✅ Managed databases
- ✅ Full observability stack

### 9.3 RPC vs Full Cloud

**RPC is sufficient if:**
- ML training is batch (not real-time)
- You have control over compute server
- Latency requirements allow async processing
- You want to start simple

**Upgrade to full cloud when:**
- Need horizontal scaling
- Multiple trading pairs require more compute
- Team grows and needs shared infrastructure
- Regulatory requirements demand cloud

---

## 10. Next Steps

1. **Review this architecture** - Adjust based on your constraints
2. **Set up monorepo structure** - Create crate skeleton
3. **Implement state vector** - Central data structure
4. **Build RPC interface** - Define proto/OpenAPI spec
5. **Start with Kalman filter** - 1D implementation first
6. **Add persistence abstraction** - Local backend first
7. **Implement basic genotype** - Simple config structure
8. **Build phenotype compiler** - Convert to executable
9. **Add entropy detector** - Regime classification
10. **Implement fitness tracking** - Performance metrics

---

## Appendix: Key Design Decisions

1. **Monorepo**: Easier development, atomic changes
2. **RPC for heavy compute**: Simpler than full cloud, sufficient for MVP
3. **Parquet for storage**: Efficient, queryable, cloud-compatible
4. **Genotype/Phenotype**: Separates config from execution
5. **Evolutionary approach**: Adapts to market changes
6. **Entropy gating**: Reduces false signals
7. **Multi-pair portfolio**: Diversification, correlation management

