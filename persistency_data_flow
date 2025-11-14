┌──────────────────────┐
│  Exchange Streams     │  (L2 diff + trades @ ~100ms)
│  (Binance, etc.)      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Ingestor Runtime     │  (Tokio)
│ - WS readers         │
│ - time sync          │
└──────────┬───────────┘
           │ snapshots & trades
           ▼
┌──────────────────────┐
│ ConcurrentOrderBook  │  (depth, best bid/ask, microprice)
│ TradesLog            │  (aggr side, rate, VWAP) 
└──────────┬───────────┘
           │ 10 Hz tick (100ms)
           ▼
┌─────────────────────────────────────────────────────────┐
│ Analytics                                               │
│  - features: mid, spread, PWI, entropy, illiquidity     │
│  - volatility/moments/autocorr                          │
│  - filters:  Kalman( mid, OFI, microprice ), EMA        │
│  ▶ emits FeatureVector { ts, symbol, f1..fN }           │
└──────────┬───────────────────────────────┬──────────────┘
           │                               │
           │                               │
           │                               │
           │ live features                 │ persist
           ▼                               ▼
┌──────────────────────┐           ┌───────────────────────────────┐
│   OMS (Live)         │           │  Persistence Writer (Parquet) │
│ - strategy_mom/mm    │           │  - rolling files (time/size)  │
│ - risk/cancels       │           │  - schema hash + manifest     │
│ - exchange adapter   │           │  - compression: zstd/snappy   │
└──────────┬───────────┘           │  - partition: dt=YYYY-MM-DD/  │
           │ orders                └──────────┬────────────────────┘
           ▼                                   │
     ┌────────────┐                            │
     │ Exchange   │                            │ writes
     └────────────┘                            ▼
                                       ┌───────────────────────────────┐
                                       │   Data Lake (local/MinIO/S3)  │
                                       │   data/features/              │
                                       │     dt=2025-11-13/            │
                                       │       features_160533.parquet │
                                       │       features_160632.parquet │
                                       └──────────┬────────────────────┘
                                                  │
                                                  │ read (batch/interactive)
                                                  ▼
     ┌─────────────────────────────┐       ┌──────────────────────────────┐
     │  DuckDB / Polars / Pandas   │       │  Labeler  →  ModelCreator    │
     │  - fast SQL on Parquet      │       │  → Validator (backtests)     │
     │  - ad-hoc queries           │       │  - trains (SVM/XGB/ONNX)     │
     └──────────┬──────────────────┘       │  - writes models/metrics     │
                │                          └──────────┬───────────────────┘
                │ notebooks / scripts                  │
                ▼                                      │ model artifacts
        ┌──────────────────────┐                       ▼
        │  Jupyter / Scripts   │                ┌───────────────────┐
        │  - visualize/QA      │                │  MLflow (optional)│
        │  - feature tests     │                │  models/metrics    │
        └──────────────────────┘                └───────────────────┘

