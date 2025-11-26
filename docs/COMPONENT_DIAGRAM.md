# Component Interaction Diagram

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Trading System                           │
│                         (Monorepo)                              │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  Market Feeds     │  Binance WebSocket
│  (External)       │
└────────┬──────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  DATA INGESTION LAYER                                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Ingestor   │  │   Feature    │  │    State     │          │
│  │  (WebSocket)│→ │   Engine     │→ │   Vector     │          │
│  │              │  │  (100ms)     │  │  (Snapshot)  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
         │
         ├─────────────────┬──────────────────┬──────────────────┐
         ▼                 ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Kalman     │  │  Entropy     │  │ Persistence  │  │   Labeling  │
│   Filter     │  │  Regime      │  │  (Local +    │  │   Pipeline  │
│              │  │  Detector    │  │   Cloud)     │  │             │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
         │                 │                  │                  │
         └─────────────────┴──────────────────┴──────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  INTELLIGENCE LAYER                                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Genotype    │  │  Phenotype   │  │   Model      │          │
│  │  (Config)    │→ │  (Compiled)  │← │   Generator  │          │
│  │              │  │              │  │   (AutoML)   │          │
│  │  - ML Algo   │  │  - ML Model  │  │   - SVM      │          │
│  │  - Kernels   │  │  - Kernels   │  │   - XGBoost  │          │
│  │  - Hyperparams│  │  - Filters   │  │   - LightGBM │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐                              │
│  │ Experiments  │  │  ML Models   │                              │
│  │ Framework    │→ │  (SVM, etc.) │                              │
│  │              │  │              │                              │
│  └──────────────┘  └──────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
         │                 │
         ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  EVOLUTIONARY LAYER                                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Evolution   │  │   Fitness    │  │  Algorithm   │          │
│  │  Engine      │← │   Tracker    │  │  Lifecycle   │          │
│  │  (GA/PSO)    │  │              │  │  (Birth/     │          │
│  │              │  │              │  │   Death)     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  EXECUTION LAYER                                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Portfolio   │  │     OMS      │  │  Validator   │          │
│  │  Manager     │→ │  (Order      │← │  (Forward    │          │
│  │  (Multi-pair)│  │   Exec)      │  │   Testing)   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow: Real-Time Trading

```
1. WebSocket Update
   │
   ▼
2. Ingestor → FeatureState
   │
   ▼
3. State Vector (timestamped snapshot)
   │
   ├─→ Kalman Filter (prediction)
   ├─→ Kernel Filter (time delay compensation)
   ├─→ Entropy Regime Detector
   └─→ Persistence (async save)
   │
   ▼
4. If LowEntropy regime:
   │
   ├─→ Genotype Evaluator
   │   │
   │   └─→ Phenotype (compiled logic)
   │       │
   │       ├─→ Kernel Filtered Features
   │       ├─→ ML Model (SVM/XGBoost/etc.)
   │       └─→ Trading Decision
   │
   └─→ Portfolio Manager
       │
       └─→ OMS (order execution)
```

## Data Flow: ML Training (Bidirectional RPC)

**Forward Path: Trading System → Compute Server**
```
1. Trading System: Labeling Pipeline
   │
   ├─→ Reads historical FeatureState
   ├─→ Three-bar classification
   └─→ Generates labels
   │
   ▼
2. Trading System: Kernel Filtering (time delay compensation)
   │
   ├─→ Apply kernel filter to features
   └─→ Generate kernel-filtered features
   │
   ▼
3. Trading System → Compute Server (RPC Request)
   │
   ├─→ Send: Features + Labels + Algorithm Config
   ├─→ Request: Train model with experiment framework
   └─→ Protocol: gRPC/HTTP
   │
   ▼
4. Compute Server: Training & Experimentation
   │
   ├─→ Runs experiment framework
   │   ├─→ Tests SVM (Linear, RBF, Polynomial, Sigmoid)
   │   ├─→ Tests XGBoost, LightGBM, Random Forest
   │   └─→ Cross-validates all combinations
   ├─→ Selects best model
   ├─→ Trains final model
   └─→ Serializes model to ONNX format
```

**Return Path: Compute Server → Trading System**
```
5. Compute Server → Trading System (RPC Response)
   │
   ├─→ Model artifact (ONNX binary)
   ├─→ Model metadata (algorithm type, hyperparameters)
   ├─→ Performance metrics (accuracy, F1, Sharpe)
   └─→ Model version ID
   │
   ▼
6. Trading System: Model Reception & Storage
   │
   ├─→ Receives model via RPC response
   ├─→ Saves ONNX file locally: models/{genotype_id}/{version}.onnx
   └─→ Uploads to cloud storage (S3/GCS) for backup
   │
   ▼
7. Trading System: Genotype Update
   │
   ├─→ Updates genotype with:
   │   ├─→ ML algorithm type
   │   ├─→ Hyperparameters
   │   ├─→ Model artifact path
   │   ├─→ Model version ID
   │   └─→ Performance metrics
   └─→ Stores updated genotype in database
   │
   ▼
8. Trading System: Phenotype Update
   │
   ├─→ Loads new model from ONNX file
   ├─→ Recompiles phenotype with new model
   ├─→ Hot-swaps phenotype (if running) OR schedules restart
   └─→ Updates phenotype's ML model reference
   │
   ▼
9. Trading System: Model Deployment
   │
   ├─→ New phenotype now uses updated model
   ├─→ Old model kept for rollback if needed
   └─→ Monitoring: Track new model performance
```

**Continuous Update Loop:**
```
10. Trading System: Monitor model performance
    │
    ├─→ If performance degrades OR new data available:
    │   ├─→ Trigger new training cycle (back to step 1)
    │   └─→ OR: Request model retraining via RPC
    │
    └─→ Repeat bidirectional flow
```

## Data Flow: Evolutionary Loop

```
1. Population of Genotypes
   │
   ├─→ Each genotype → phenotype (compiled)
   └─→ Phenotypes deployed
   │
   ▼
2. Forward Testing
   │
   ├─→ Each phenotype evaluated
   ├─→ Fitness metrics calculated
   └─→ Scores stored in database
   │
   ▼
3. Evolutionary Algorithm
   │
   ├─→ Select best performers
   ├─→ Crossover (create offspring)
   ├─→ Mutation (introduce variation)
   └─→ New generation created
   │
   ▼
4. Algorithm Lifecycle
   │
   ├─→ Best genotypes → phenotypes (birth)
   ├─→ Continuous fitness monitoring
   ├─→ Underperformers → death
   └─→ Replacement from population
```

## Storage Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Local Storage (Hot Data)                                │
│  - Parquet files (last 30 days)                          │
│  - Fast access, low latency                             │
│  - Location: data/local/                                │
└─────────────────────────────────────────────────────────┘
         │
         │ Async sync
         ▼
┌─────────────────────────────────────────────────────────┐
│  Cloud Storage (Archive)                                  │
│  - S3 / GCS / Azure Blob                                 │
│  - All historical data                                   │
│  - Disaster recovery                                     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Database (Metadata)                                      │
│  - PostgreSQL / SQLite                                   │
│  - Genotypes, fitness scores                             │
│  - Algorithm lifecycle                                   │
│  - Configuration                                         │
│  - Model metadata (algorithm, hyperparameters, versions) │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Model Storage                                            │
│  - Local: models/{genotype_id}/{version}.onnx            │
│  - Cloud: S3/GCS/Azure Blob (backup)                     │
│  - Format: ONNX (portable)                               │
│  - Versioning: Keep old models for rollback              │
└─────────────────────────────────────────────────────────┘
```

## RPC Architecture (Bidirectional)

```
┌─────────────────────────────────────────────────────────┐
│  Trading System (Main Process)                           │
│  - Real-time ingestion                                   │
│  - State management                                      │
│  - Evolution engine                                      │
│  - Phenotype execution & model updates                   │
│  - OMS                                                   │
└───────────────────┬──────────────────────────────────────┘
                    │
                    │ Bidirectional RPC (gRPC/HTTP)
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
   REQUEST:                RESPONSE:
   Features + Labels        Model Artifact (ONNX)
   Algorithm Config         + Metadata
   Training Request         + Metrics
        │                       │
        └───────────┬───────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│  Compute Server (Remote)                                  │
│  - ML model training (batch)                             │
│  - Experiment framework                                   │
│  - Backtesting (offline)                                 │
│  - Forward validation                                    │
│  - Model generation & serialization                       │
└───────────────────┬──────────────────────────────────────┘
                    │
                    │ Store results
                    ▼
┌─────────────────────────────────────────────────────────┐
│  Cloud Storage                                           │
│  - Model artifacts (ONNX)                                │
│  - Backtest results                                      │
│  - Historical data                                       │
│  - Model metadata                                        │
└─────────────────────────────────────────────────────────┘

Model Update Flow:
1. Trading System → Compute Server: RPC Request (training)
2. Compute Server: Trains model, serializes to ONNX
3. Compute Server → Trading System: RPC Response (model artifact)
4. Trading System: Stores model, updates genotype & phenotype
5. Trading System: Hot-swaps or restarts with new model
```

## Component Dependencies

```
ingestor (your existing code)
  └─→ state (new)
      ├─→ kalman (new)
      ├─→ kernel_filter (new)  # Time delay compensation
      ├─→ entropy_regime (new)
      └─→ persistence (enhanced)
          └─→ genotype (new)
              └─→ phenotype (new)
                  ├─→ evolution (new)
                  │   └─→ fitness (new)
                  ├─→ model_gen (new, RPC)
                  │   ├─→ ml_models (new)  # SVM, XGBoost, etc.
                  │   └─→ experiments (new)  # Experiment framework
                  ├─→ validator (new, RPC)
                  └─→ portfolio (new)
                      └─→ oms (new)
```

## API Layer (poem-openapi)

```
┌─────────────────────────────────────────────────────────┐
│  REST API (poem-openapi)                                 │
│  - GET /genotypes                                        │
│  - POST /genotypes                                       │
│  - GET /fitness/{id}                                     │
│  - GET /algorithms                                       │
│  - POST /backtest                                        │
│  - GET /portfolio                                        │
└─────────────────────────────────────────────────────────┘
```

