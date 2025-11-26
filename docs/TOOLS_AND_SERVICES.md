# Tools and Services Quick Reference

## Repository Structure Decision

### ✅ **RECOMMENDED: Monorepo (Single Repository)**

**Why:**
- Components are tightly coupled (share data structures)
- Atomic changes across modules
- Simpler CI/CD (one pipeline)
- Easier refactoring
- Version consistency

**Structure:**
```
trading-system/
├── crates/          # All Rust crates (workspace)
├── config/          # Configuration files
├── data/            # Data storage
├── scripts/         # Utility scripts
├── tests/           # Test suites
└── docs/            # Documentation
```

**When to split:**
- Team > 5 people
- Clear service boundaries emerge
- Different deployment schedules needed

---

## Data Pipeline Structure

### Pipeline Flow

```
Market Feeds → Ingestor → Feature Engine → State Vector
                                      ↓
                    ┌─────────────────┴─────────────────┐
                    ↓                                     ↓
            Kalman Filter                          Persistence
                    ↓                                     ↓
            Entropy Regime                        (Local + Cloud)
                    ↓
            Genotype Evaluator
                    ↓
            Phenotype (Trading Logic)
                    ↓
                  OMS
```

### Storage Strategy

| Layer | Format | Location | Purpose |
|-------|--------|----------|---------|
| **Hot** | Parquet | Local SSD | Last 30 days, fast access |
| **Warm** | Parquet | S3/GCS | Historical archive |
| **Metadata** | PostgreSQL | Cloud DB | Genotypes, fitness, configs |

---

## Cloud Services Recommendations

### Minimal Setup (RPC Only) ✅ **START HERE**

**What you need:**
- Your trading system (local or cloud VM)
- Compute server (your machine or cloud VM) for ML/backtesting
- Optional: S3/GCS bucket for backup

**Architecture:**
```
┌──────────────────┐         gRPC/HTTP         ┌──────────────────┐
│  Trading System  │ ────────────────────────► │  Compute Server  │
│  (Main Process)  │                            │  (ML/Backtest)   │
└──────────────────┘                            └──────────────────┘
         │                                               │
         │ Save locally                                  │ Save results
         ▼                                               ▼
┌──────────────────┐                            ┌──────────────────┐
│  Local Parquet   │                            │  Model Artifacts  │
└──────────────────┘                            └──────────────────┘
```

**RPC is enough when:**
- ✅ ML training is batch (not real-time)
- ✅ Backtesting is offline
- ✅ You control the compute server
- ✅ Latency requirements allow async

**RPC Implementation:**
- **Protocol**: gRPC (tonic) or HTTP/REST (poem-openapi)
- **Services**:
  - `ModelTrainingService` - Train ML models
  - `BacktestService` - Historical validation
  - `ValidationService` - Forward testing

### Production Setup (Full Cloud)

**AWS Stack:**
- **Compute**: EC2 (trading), ECS/EKS (containers), SageMaker (ML)
- **Storage**: S3 (parquet), RDS PostgreSQL (metadata)
- **Messaging**: SQS (queues), EventBridge (events)
- **Monitoring**: CloudWatch (metrics/logs), X-Ray (tracing)

**GCP Stack:**
- **Compute**: Compute Engine / GKE, Vertex AI (ML)
- **Storage**: Cloud Storage (GCS), Cloud SQL (PostgreSQL)
- **Messaging**: Pub/Sub (events)
- **Monitoring**: Cloud Monitoring / Logging

**Azure Stack:**
- **Compute**: Virtual Machines / AKS, Azure ML
- **Storage**: Blob Storage, Azure SQL Database
- **Messaging**: Service Bus / Event Hubs
- **Monitoring**: Azure Monitor / Application Insights

---

## Technology Stack

### Core Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| **Language** | Rust | Performance, safety, async |
| **Async** | Tokio | Industry standard |
| **API** | poem-openapi | Type-safe, async REST |
| **Data** | Polars | Fast DataFrames |
| **Storage** | Parquet | Columnar, efficient |
| **Cloud Storage** | object_store | Unified S3/GCS/Azure |
| **Database** | SQLx + PostgreSQL | Type-safe, async |
| **RPC** | Tonic (gRPC) | High-performance |

### ML & Scientific

| Component | Technology | Why |
|-----------|-----------|-----|
| **Kalman** | Custom Rust | Performance control |
| **ML** | candle or ONNX | Rust-native or portable |
| **Arrays** | ndarray | Multi-dimensional |
| **Stats** | statrs | Statistical functions |

---

## Recommended Approach for Embedded Developer

### Phase 1: MVP (Weeks 1-4)

**Local Setup:**
- ✅ Keep existing ingestor
- ✅ Add state vector module
- ✅ Implement local persistence (parquet)
- ✅ Build basic RPC client/server (HTTP first, upgrade to gRPC)
- ✅ Add SQLite for metadata

**Remote Compute:**
- ✅ Your machine or cloud VM
- ✅ Python script for ML training (bridge to Rust)
- ✅ Simple HTTP server for RPC

**Storage:**
- ✅ Local parquet files
- ✅ Optional: S3 bucket for backup

### Phase 2: Scale (Weeks 5-8)

**Add:**
- ✅ PostgreSQL for metadata
- ✅ S3/GCS for archive
- ✅ Cloud monitoring
- ✅ Upgrade to gRPC

### Phase 3: Production (Weeks 9-12)

**Add:**
- ✅ Kubernetes/ECS deployment
- ✅ Managed databases
- ✅ Full observability
- ✅ Multi-region support

---

## RPC vs Full Cloud Decision Matrix

| Requirement | RPC Sufficient | Need Full Cloud |
|------------|----------------|-----------------|
| ML training (batch) | ✅ | ❌ |
| Real-time ML inference | ❌ | ✅ |
| Horizontal scaling | ❌ | ✅ |
| Multiple trading pairs | ✅ (if sequential) | ✅ (if parallel) |
| Team collaboration | ✅ (small team) | ✅ (large team) |
| Regulatory compliance | ✅ (if server compliant) | ✅ (managed services) |

**Recommendation**: Start with RPC, upgrade to full cloud when you hit scaling limits.

---

## Quick Start Checklist

- [ ] Set up monorepo structure
- [ ] Create Rust workspace (Cargo.toml)
- [ ] Set up local data directory
- [ ] Configure RPC server (compute side)
- [ ] Set up SQLite/PostgreSQL
- [ ] Optional: Create S3/GCS bucket
- [ ] Set up CI/CD pipeline (GitHub Actions)
- [ ] Configure monitoring (CloudWatch or local)

---

## Cost Estimation (Monthly)

### Minimal Setup (RPC)
- Compute server: $50-200 (cloud VM or your machine)
- Storage: $10-50 (S3/GCS, ~100GB)
- Database: $0 (SQLite) or $20-50 (managed PostgreSQL)
- **Total: $60-300/month**

### Production Setup
- Compute: $200-1000 (Kubernetes cluster)
- Storage: $50-200 (S3/GCS, ~1TB)
- Database: $50-200 (managed PostgreSQL)
- Monitoring: $20-100 (CloudWatch)
- **Total: $320-1500/month**

---

## Next Steps

1. **Review architecture document** (`docs/ARCHITECTURE.md`)
2. **Set up monorepo** - Create workspace structure
3. **Start with RPC** - Simple HTTP server for compute
4. **Build incrementally** - State vector → Kalman → Evolution
5. **Add cloud storage** - When you need archive/backup
6. **Scale to full cloud** - When you hit limits

