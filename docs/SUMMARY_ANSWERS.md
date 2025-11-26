# Summary: Direct Answers to Your Questions

## 1. Repository Structure

**Answer: Single Repo (Monorepo)**

- ✅ **Recommended**: Monorepo approach
- **Why**: Components are tightly coupled, atomic changes needed, simpler CI/CD
- **Structure**: `crates/` workspace with all modules
- **Split later**: Only if team > 5 people or clear service boundaries emerge

See: `docs/ARCHITECTURE.md` Section 1

---

## 2. Data Pipeline Structure

**Answer: Layered Pipeline with State Vector**

```
Market Feeds → Ingestor → Feature Engine → State Vector
                                      ↓
                    ┌─────────────────┴─────────────────┐
                    ↓                                     ↓
            Kalman Filter                          Persistence
                    ↓                                     ↓
            Entropy Regime                        (Local + Cloud)
                    ↓
            Genotype → Phenotype → OMS
```

**Key Points:**
- State Vector = central data structure containing ALL features
- Persistence = local (parquet) + cloud (S3/GCS) async
- Real-time path: Ingestor → State → Trading Logic → OMS
- Batch path: Historical data → Labeling → ML Training (Bidirectional RPC)

**Bidirectional Model Communication:**
- **Forward**: Trading System → Compute Server (Features + Labels + Config)
- **Return**: Compute Server → Trading System (Model Artifact + Metadata)
- **Update**: Trading System updates genotype & phenotype with new model
- **Deployment**: Hot-swap or restart phenotype with new model

See: `docs/ARCHITECTURE.md` Section 2, 6.2

---

## 3. Cloud Services: RPC vs Full Cloud

**Answer: RPC is Sufficient for MVP, Upgrade Later**

### RPC Approach (Start Here) ✅

**What you need:**
- Trading system (local or cloud VM)
- Compute server (your machine or cloud VM) for ML/backtesting
- Optional: S3/GCS bucket for backup

**When RPC is enough:**
- ✅ ML training is batch (not real-time)
- ✅ Backtesting is offline
- ✅ You control the compute server
- ✅ Latency allows async processing

**Bidirectional RPC Implementation:**
- **Protocol**: gRPC (tonic) or HTTP/REST (poem-openapi)
- **Forward (Request)**: Trading System → Compute Server
  - Features + Labels + Algorithm Config
  - Training request
- **Return (Response)**: Compute Server → Trading System
  - Model artifact (ONNX binary)
  - Model metadata (algorithm, hyperparameters)
  - Performance metrics
  - Model version ID
- **Services**: ModelTraining (bidirectional), Backtesting, Validation
- **Model Update**: Trading system receives model, updates genotype & phenotype

### Full Cloud (When to Upgrade)

**Upgrade when:**
- Need horizontal scaling
- Multiple trading pairs require parallel compute
- Team grows and needs shared infrastructure
- Regulatory requirements demand managed services

**Services needed:**
- Object Storage: S3 / GCS / Azure Blob
- Database: RDS / Cloud SQL / Azure SQL
- Compute: EC2 / Compute Engine / VMs
- Monitoring: CloudWatch / GCP Monitoring

**Cost**: $60-300/month (RPC) vs $320-1500/month (full cloud)

See: `docs/TOOLS_AND_SERVICES.md` Section "Cloud Services"

---

## 4. Tools & Technology Stack

### Core Stack
- **Language**: Rust (you're already using it)
- **Async**: Tokio (already in use)
- **API**: poem-openapi (for REST endpoints)
- **Data**: Polars (already in use)
- **Storage**: Parquet (already in use)
- **Cloud Storage**: `object_store` crate (unified S3/GCS/Azure)
- **Database**: SQLx + PostgreSQL (or SQLite for MVP)
- **RPC**: Tonic (gRPC) or poem-openapi (HTTP)

### ML & Scientific
- **Kalman**: Custom Rust implementation
- **ML**: candle (Rust) or ONNX (portable models)
- **Arrays**: ndarray
- **Stats**: statrs

See: `docs/TOOLS_AND_SERVICES.md` Section "Technology Stack"

---

## 5. Evolutionary Algorithm Choice

**Answer: Hybrid Approach (GA + PSO)**

### Genetic Algorithm (GA)
- **Use for**: Discrete/categorical genotype spaces
- **Structure**: Feature selection, rule combinations
- **Operators**: Crossover (uniform/one-point), Mutation (adaptive)

### Particle Swarm Optimization (PSO)
- **Use for**: Continuous parameter optimization
- **Parameters**: Kalman filter tuning, threshold optimization

### Hybrid Approach (Recommended)
- **GA**: Evolves structure (which features, which rules)
- **PSO**: Optimizes parameters (thresholds, weights)
- **Combined**: Best of both worlds

**Genotype Encoding:**
```rust
pub struct Genotype {
    // Discrete (GA)
    pub active_features: Vec<FeatureSelector>,
    pub entry_rules: Vec<Rule>,
    
    // Continuous (PSO)
    pub kalman_params: KalmanParams,
    pub thresholds: HashMap<String, f64>,
}
```

See: `docs/ARCHITECTURE.md` Section 4.5

---

## 6. Genotype/Phenotype System

**Answer: Genotype = Config, Phenotype = Executable**

### Genotype (Configuration)
- **Format**: JSON/YAML (serializable)
- **Contains**: Feature selection, Kalman params, ML config, rules, thresholds
- **Storage**: Database (PostgreSQL) or files
- **Evolution**: Modified by GA/PSO algorithms

### Phenotype (Executable)
- **Format**: Compiled Rust code (or interpreted rules)
- **Contains**: Compiled trading logic, loaded ML models, initialized Kalman filters
- **Execution**: Real-time evaluation of FeatureState → TradingDecision
- **Lifecycle**: Born from genotype, monitored, dies when fitness drops

**Compilation**: Genotype → Phenotype happens when algorithm is "born"

See: `docs/ARCHITECTURE.md` Section 4.4

---

## 7. Entropy Regime Detection

**Answer: Gate Trading on Entropy Regime**

**Implementation:**
```rust
pub enum Regime {
    HighEntropy,    // Chaos → Do nothing
    LowEntropy,     // Momentum → Trade
    Transition,     // Uncertain → Be cautious
}

// Before genotype evaluation:
let regime = entropy_detector.detect(&state);
if !entropy_detector.should_trade(regime) {
    return TradingDecision::Hold;
}
```

**Logic:**
- Compute entropy across multiple windows (1s, 5s, 10s, etc.)
- Classify regime based on entropy levels
- Only trade in LowEntropy (momentum) regimes
- Do nothing in HighEntropy (chaos) regimes

See: `docs/ARCHITECTURE.md` Section 4.7

---

## 8. Multi-Pair Sensitivity

**Answer: Portfolio-Level Genotype**

**Structure:**
```rust
pub struct PortfolioGenotype {
    pub pairs: HashMap<String, PairGenotype>,
    pub correlation_matrix: Matrix,
    pub portfolio_risk_limit: f64,
    pub position_sizing: PositionSizingConfig,
}
```

**Evaluation:**
- Evaluate each pair independently
- Apply correlation adjustments
- Generate portfolio-level position sizing
- Respect risk limits across all pairs

See: `docs/ARCHITECTURE.md` Section 4.12

---

## 9. CI/CD Pipeline

**Answer: GitHub Actions / GitLab CI**

**Stages:**
1. **Test**: Unit + integration tests
2. **Backtest**: Historical validation
3. **Forward Test**: Out-of-sample validation (7 days)
4. **Deploy Staging**: Docker images, smoke tests
5. **Deploy Production**: Manual approval required

**Key Tests:**
- Unit: Component logic
- Integration: End-to-end pipeline
- Backtest: Historical replay
- Forward: Real-time simulation

See: `docs/ARCHITECTURE.md` Section 8

---

## 10. Project Structure

**Answer: Monorepo with Workspace**

```
trading-system/
├── crates/
│   ├── ingestor/          # Your existing code
│   ├── state/             # State vector
│   ├── kalman/            # Kalman filters
│   ├── persistence/       # Multi-backend storage
│   ├── labeling/          # Three-bar classification
│   ├── genotype/          # Genotype definition
│   ├── phenotype/         # Phenotype compilation
│   ├── evolution/         # GA/PSO algorithms
│   ├── fitness/           # Fitness metrics
│   ├── model_gen/         # AutoML (RPC client)
│   ├── validator/         # Forward testing (RPC client)
│   ├── entropy_regime/    # Entropy detection
│   ├── portfolio/         # Multi-pair management
│   ├── oms/               # Order Management System
│   └── api/               # poem-openapi REST API
├── config/                # Genotypes, environments
├── data/                  # Local storage
├── scripts/               # Utilities
└── tests/                 # Test suites
```

See: `docs/ARCHITECTURE.md` Section 1

---

## 11. Evolutionary Mechanics

**Answer: Continuous Evolution with Fitness-Based Lifecycle**

**Flow:**
1. **Population**: Maintain pool of genotypes
2. **Birth**: Best genotypes → phenotypes (deployed)
3. **Monitoring**: Track fitness continuously
4. **Death**: Underperformers removed (fitness threshold)
5. **Evolution**: GA/PSO creates new genotypes
6. **Replacement**: New genotypes replace dead ones

**Fitness Function:**
- Sharpe ratio (risk-adjusted returns)
- Sortino ratio (downside risk)
- Calmar ratio (return/drawdown)
- Win rate, profit factor

**Key**: Algorithms evolve continuously, adapt to market changes

See: `docs/ARCHITECTURE.md` Section 4.5, 4.6, 4.9

---

## 12. Specific Algorithm Requirements

### Entropy Regime Detection
- ✅ Detect entropy regimes
- ✅ Fire momentum bets when entropy decreasing
- ✅ Do nothing in high entropy (chaos)

### Do-Nothing Logic
- ✅ Algorithm can do nothing (Hold decision)
- ✅ Conditions must be met to trade
- ✅ Entropy regime gates all decisions

### Multi-Pair Sensitivity
- ✅ Portfolio-level genotype
- ✅ Evaluate large number of pairs simultaneously
- ✅ Correlation-aware position sizing

See: `docs/ARCHITECTURE.md` Section 4.7, 4.12

---

## 13. ML Algorithms and Kernel Filtering

### Support Vector Machines (SVM)
- ✅ **Multiple Kernels**: Linear, RBF, Polynomial, Sigmoid
- ✅ **Hyperparameter Tuning**: Grid search, random search
- ✅ **Experiment Framework**: Compare all kernels automatically
- ✅ **Genotype Expression**: Kernel type + hyperparameters in genotype

### Kernel Filtering
- ✅ **Time Delay Compensation**: Compensate for feed delays
- ✅ **Signal Smoothing**: Gaussian, Exponential, Polynomial, RBF, Epanechnikov kernels
- ✅ **Integration**: Applied before ML model inference
- ✅ **Genotype Expression**: Kernel filter config in genotype

### ML Algorithm Support
- ✅ **SVM**: Linear, RBF, Polynomial, Sigmoid kernels
- ✅ **Tree-based**: XGBoost, LightGBM, Random Forest
- ✅ **Linear Models**: Logistic Regression, Ridge, Lasso
- ✅ **Experiment Framework**: Test multiple algorithms automatically

### Genotype/Phenotype Extension
- ✅ **ML Algorithm Selection**: Algorithm type in genotype
- ✅ **Hyperparameters**: All ML hyperparameters in genotype
- ✅ **Kernel Filter Config**: Time delay compensation in genotype
- ✅ **Evolution**: ML algorithm selection evolves with GA/PSO

See: `docs/ARCHITECTURE.md` Section 4.8, 4.9, 4.10, 4.11
See: `docs/ML_ALGORITHMS_GUIDE.md` (complete guide)

---

## Recommended Starting Point

### Phase 1 (Weeks 1-2)
1. Set up monorepo structure
2. Create state vector module
3. Implement local persistence
4. Build basic RPC interface (HTTP first)

### Phase 2 (Weeks 3-4)
5. Add Kalman filter (1D first)
6. Implement entropy regime detector
7. Create basic genotype structure

### Phase 3 (Weeks 5-6)
8. Build phenotype compiler
9. Implement fitness tracking
10. Add evolutionary loop (simple GA)

### Phase 4 (Weeks 7-8)
11. Add ML model generation (RPC)
12. Implement forward testing
13. Build OMS integration

### Phase 5 (Weeks 9-10)
14. Add multi-pair portfolio
15. Implement CI/CD pipeline
16. Add monitoring/observability

---

## Key Design Decisions Summary

1. ✅ **Monorepo**: Single repository for all components
2. ✅ **Bidirectional RPC for compute**: Sufficient for MVP, upgrade later
   - Forward: Training requests with data
   - Return: Trained models with metadata
   - Model updates: Genotype & phenotype updated with new models
3. ✅ **State Vector**: Central data structure
4. ✅ **Genotype/Phenotype**: Separates config from execution
5. ✅ **Hybrid Evolution**: GA + PSO
6. ✅ **Entropy Gating**: Reduces false signals
7. ✅ **Multi-Pair Portfolio**: Diversification, correlation
8. ✅ **Continuous Evolution**: Algorithms adapt to market

---

## Next Steps

1. **Review architecture docs** (`docs/ARCHITECTURE.md`)
2. **Review tools guide** (`docs/TOOLS_AND_SERVICES.md`)
3. **Review component diagram** (`docs/COMPONENT_DIAGRAM.md`)
4. **Set up monorepo** - Create workspace structure
5. **Start with state vector** - Central data structure
6. **Build incrementally** - One module at a time

---

## Questions Answered

✅ Single repo vs multi-repo → **Monorepo**
✅ Data pipeline structure → **Layered with state vector**
✅ Cloud services → **RPC sufficient, upgrade later**
✅ Tools → **Rust + Tokio + poem-openapi + Polars**
✅ Evolutionary algorithms → **Hybrid GA + PSO**
✅ Genotype/Phenotype → **Config vs Executable**
✅ Entropy detection → **Regime gating**
✅ Multi-pair → **Portfolio-level genotype**
✅ CI/CD → **GitHub Actions with stages**
✅ Project structure → **Monorepo workspace**
✅ ML Algorithms → **SVM (4 kernels), XGBoost, LightGBM, Random Forest, etc.**
✅ Kernel Filtering → **Time delay compensation with multiple kernel types**
✅ Experiment Framework → **Automated testing of all ML algorithms**
✅ Genotype ML Expression → **All ML configs (algorithm, hyperparameters, kernels) in genotype**

