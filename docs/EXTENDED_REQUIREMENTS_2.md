# EXTENDED_REQUIREMENTS_2: Hyperliquid Expansion Strategy

**Document Version:** 2.0
**Date:** 2026-01-17
**Status:** Strategic Planning
**Prerequisites:** EXTENDED_REQUIREMENTS_0.md, EXTENDED_REQUIREMENTS_1.md, SPECULATION_0.md

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Competence Assessment](#2-system-competence-assessment)
3. [Commercial Value Analysis](#3-commercial-value-analysis)
4. [Hyperliquid Platform Analysis](#4-hyperliquid-platform-analysis)
5. [Hyperliquid-Specific Value Proposition](#5-hyperliquid-specific-value-proposition)
6. [Feature Specification: Hyperliquid-Exclusive](#6-feature-specification-hyperliquid-exclusive)
7. [Technical Integration Requirements](#7-technical-integration-requirements)
8. [Go-to-Market Strategy](#8-go-to-market-strategy)
9. [Revenue Model](#9-revenue-model)
10. [Implementation Roadmap](#10-implementation-roadmap)
11. [Risk Analysis](#11-risk-analysis)
12. [Success Metrics](#12-success-metrics)
13. [Appendices](#appendices)

---

## 1. Executive Summary

### 1.1 Strategic Pivot Rationale

This document defines the strategic expansion of the Information-Guided Adaptive Trading System to **Hyperliquid as the primary platform**, based on the following analysis:

| Factor | Binance-Only | Hyperliquid-First | Advantage |
|--------|--------------|-------------------|-----------|
| Data transparency | Top-of-book only | Full order book on-chain | 3x feature depth |
| Competition | High (many providers) | Low (underserved) | First mover opportunity |
| Target customer | Mixed sophistication | High sophistication | Higher willingness to pay |
| Community access | Fragmented | Concentrated | Efficient marketing |
| Platform growth | Mature | Explosive (10x in 2024) | Ride growth wave |
| **Commercial value multiplier** | 1x | **2-3x** | Significant uplift |

### 1.2 Core Thesis

> **On-chain order book transparency enables unique microstructure features impossible on centralized exchanges. Combined with our existing entropy-based regime detection, this creates a differentiated analytics platform for the underserved Hyperliquid trading community.**

### 1.3 Target Outcome

- **Timeline:** 3-4 months to launch, 18-24 months to sustainability
- **Revenue Target:** $40-50k MRR ($500-600k ARR)
- **Customer Target:** 100-150 paying customers
- **Team Size:** 3-5 person engineering office

---

## 2. System Competence Assessment

### 2.1 Current Technical Capabilities

| Capability | Implementation Status | Quality Score | Notes |
|------------|----------------------|---------------|-------|
| Real-time ingestion | ✅ Complete | 8/10 | Binance WS, sub-100ms |
| Order book processing | ✅ Complete | 8/10 | Efficient L2 handling |
| Feature computation | ✅ Complete | 8/10 | 60+ features |
| Entropy metrics | ✅ Complete | 9/10 | 7 timeframes, tick + volume |
| Order flow analysis | ✅ Complete | 7/10 | OFI, imbalance, VWAP |
| Backtesting infrastructure | ✅ Complete | 8/10 | Walk-forward, fill sim |
| ML integration | ✅ Complete | 7/10 | KSG MI, regime detection |
| Persistence | ✅ Complete | 8/10 | Parquet, efficient |
| TUI interface | ✅ Complete | 7/10 | Functional |
| Multi-exchange support | ❌ Not started | 2/10 | Binance only |
| API layer | ❌ Not started | 0/10 | Required for commercial |
| Documentation | ⚠️ Partial | 3/10 | Internal only |

### 2.2 Competence Radar

```
                         Feature Depth
                              10
                               │
                          8 ───┼─── ████████
                               │
        Backtesting    8 ──────┼──────── 6    Latency
                    ████████   │      ██████
                               │
                          ─────●─────
                               │
     Differentiation   8 ──────┼──────── 2    Multi-exchange
                    ████████   │        ██
                               │
                          3 ───┼───
                               │
                        Documentation

     Overall Score: 7/10 - Strong foundation, gaps in breadth
```

### 2.3 Unique Competencies (Moat Potential)

1. **Information-theoretic approach**
   - Entropy-based regime detection
   - Academically grounded methodology
   - Rare in commercial products

2. **Integrated validation framework**
   - Walk-forward backtesting
   - Fill simulation
   - Most feature providers lack this

3. **MI-based feature selection**
   - Data-driven, not assumption-driven
   - KSG estimator implementation
   - Rigorous statistical foundation

4. **Microstructure depth**
   - Beyond "price + volume"
   - Order flow dynamics
   - Queue position modeling

### 2.4 Development Investment to Date

| Component | Estimated Effort | Value |
|-----------|------------------|-------|
| Exchange connectivity | 2-3 weeks | Foundation |
| Order book processing | 2-3 weeks | Core capability |
| Feature computation | 4-6 weeks | Primary value |
| Entropy engine | 2 weeks | Differentiation |
| Backtesting infrastructure | 4-6 weeks | Validation |
| Walk-forward validation | 2 weeks | Rigor |
| Persistence layer | 1-2 weeks | Infrastructure |
| TUI interface | 2-3 weeks | Usability |
| **Total invested** | **20-28 weeks** | **5-7 months** |

---

## 3. Commercial Value Analysis

### 3.1 Value Layer Assessment

| Layer | What We Offer | Market Reference | Our Position |
|-------|---------------|------------------|--------------|
| L1: Raw Data | N/A | $500-2000/mo (Kaiko) | Don't compete |
| L2: Computed Features | 60+ real-time features | $200-500/mo | Core offering |
| L3: Regime Detection | Entropy-based classification | Novel | Differentiation |
| L4: Backtesting | Walk-forward validation | $100-300/mo | Added value |
| L5: ML Platform | Managed training/inference | $500-2000/mo | Future expansion |

### 3.2 Total Addressable Market

```
Global crypto quantitative traders:     ~50,000 - 100,000
├── Hobbyist (low willingness to pay):  70% (~35-70k)
├── Serious retail:                     20% (~10-20k)
├── Professional/institutional:         10% (~5-10k)
└── Realistic target (1-5% of serious): 100 - 1,000 customers

Hyperliquid-specific:
├── Active Hyperliquid traders:         ~10,000 - 30,000
├── Sophisticated/quant-oriented:       30% (~3-9k)
├── Willing to pay for analytics:       20% (~600-1,800)
└── Realistic target:                   100 - 300 customers
```

### 3.3 Revenue Potential

| Scenario | Customers | Avg Price | MRR | ARR |
|----------|-----------|-----------|-----|-----|
| Conservative | 75 | $250 | $18,750 | $225,000 |
| Moderate | 140 | $320 | $44,800 | $537,600 |
| Optimistic | 250 | $400 | $100,000 | $1,200,000 |

### 3.4 Commercial Value Score

| Factor | Score | Rationale |
|--------|-------|-----------|
| Uniqueness | 8/10 | Entropy + on-chain = differentiated |
| Willingness to pay | 7/10 | Sophisticated HL traders have budget |
| Stickiness | 7/10 | Feature dependency creates lock-in |
| Scalability | 8/10 | Low marginal cost per customer |
| Defensibility | 6/10 | Can be replicated but requires expertise |
| **Overall** | **7.2/10** | **Viable commercial opportunity** |

---

## 4. Hyperliquid Platform Analysis

### 4.1 Platform Overview

**Hyperliquid** is a Layer 1 blockchain purpose-built for trading, featuring an on-chain order book (unlike AMM-based DEXes).

| Characteristic | Hyperliquid | Centralized Exchanges |
|----------------|-------------|----------------------|
| Order book | On-chain, fully transparent | Off-chain, opaque |
| Order visibility | ALL orders visible | Top-of-book only |
| Trade attribution | Wallet addresses known | Anonymous |
| Margin data | On-chain, public | Private |
| Daily volume | $2-5B (growing) | $15-30B (Binance) |
| User base | Technical, DeFi-native | Mixed |
| KYC requirement | None | Required |
| Composability | DeFi integrations possible | Isolated |

### 4.2 Data Transparency Advantages

```
┌─────────────────────────────────────────────────────────────────┐
│              HYPERLIQUID DATA TRANSPARENCY                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ON BINANCE (CEX):                ON HYPERLIQUID:               │
│  ─────────────────                ────────────────               │
│  • Top 20 bid/ask levels          • FULL order book depth       │
│  • Anonymous trades               • Trades with wallet address  │
│  • Hidden margin data             • Public margin/liquidation   │
│  • Unknown order modifications    • All order events on-chain   │
│  • Estimated whale activity       • Verified whale tracking     │
│  • Guessed MM behavior            • Observable MM patterns      │
│                                                                  │
│  RESULT: Limited features         RESULT: Unique features       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 Hyperliquid Growth Trajectory

```
Volume Growth (2024-2025):
├── Q1 2024: ~$500M daily average
├── Q2 2024: ~$1B daily average
├── Q3 2024: ~$2B daily average
├── Q4 2024: ~$3-5B daily average
└── Trajectory: 10x growth in 12 months

User Growth:
├── Active addresses: Growing ~20% month-over-month
├── Vault system adoption: Rapid expansion
├── Developer ecosystem: Emerging
└── Community: Highly engaged (Discord, Twitter)
```

### 4.4 Competitive Landscape on Hyperliquid

| Competitor | Offering | Depth | Threat Level |
|------------|----------|-------|--------------|
| Hyperliquid native | Basic dashboard | Shallow | Low |
| Coinalyze | Some HL data | Generic | Low |
| Velo Data | Limited HL coverage | Shallow | Low |
| DIY scripts | Custom solutions | Varies | Medium |
| **Gap:** | Deep microstructure analytics | **OPEN** | **OPPORTUNITY** |

**Key Finding:** No competitor offers entropy-based regime detection, real-time computed features via API, or backtesting infrastructure for Hyperliquid data.

---

## 5. Hyperliquid-Specific Value Proposition

### 5.1 Positioning Statement

> **"Deep microstructure analytics for Hyperliquid traders. Entropy-based regime detection, whale tracking, and liquidation risk—powered by on-chain transparency."**

### 5.2 Value Proposition by Segment

#### Segment A: Active Traders

| Pain Point | Our Solution | Value |
|------------|--------------|-------|
| "When is the market in a trending vs ranging regime?" | Real-time entropy regime detection | Better entry/exit timing |
| "Where are the whales positioned?" | Wallet tracking with size filters | Follow smart money |
| "Am I about to get liquidated by a cascade?" | Liquidation risk heatmap | Risk management |

#### Segment B: Quantitative Traders

| Pain Point | Our Solution | Value |
|------------|--------------|-------|
| "I need features for my models" | 60+ computed features via API | Reduce development time |
| "I need to backtest on HL data" | Walk-forward validation framework | Validate strategies |
| "I need real-time signals" | WebSocket feature streaming | Low-latency integration |

#### Segment C: Vault Managers

| Pain Point | Our Solution | Value |
|------------|--------------|-------|
| "What are competing vaults doing?" | Vault flow analytics | Competitive intelligence |
| "When should I adjust exposure?" | Regime-based risk signals | Better risk management |
| "How do I attract depositors?" | Performance analytics | Marketing data |

### 5.3 Feature Differentiation Matrix

| Feature Category | CEX Possible | HL Possible | HL Exclusive | We Offer |
|------------------|--------------|-------------|--------------|----------|
| Basic price/volume | ✅ | ✅ | ❌ | ✅ |
| Top-of-book entropy | ✅ | ✅ | ❌ | ✅ |
| Full-depth entropy | ❌ | ✅ | ✅ | ✅ Planned |
| Order flow imbalance | ✅ | ✅ | ❌ | ✅ |
| Wallet-attributed trades | ❌ | ✅ | ✅ | ✅ Planned |
| Whale order tracking | ❌ | ✅ | ✅ | ✅ Planned |
| Liquidation risk map | ❌ | ✅ | ✅ | ✅ Planned |
| Vault flow analysis | ❌ | ✅ | ✅ | ✅ Planned |
| Historical reconstruction | ⚠️ Limited | ✅ Full | ✅ | ✅ Planned |

---

## 6. Feature Specification: Hyperliquid-Exclusive

### 6.1 Tier 1: Transparency-Based Features

#### 6.1.1 Full Order Book Depth Analytics

```rust
/// Complete order book analysis (not just top N levels)
pub struct FullDepthFeatures {
    /// Total bid depth in USD
    pub total_bid_depth_usd: Decimal,
    /// Total ask depth in USD
    pub total_ask_depth_usd: Decimal,
    /// Depth imbalance ratio
    pub depth_imbalance: Decimal,
    /// Price levels with >$100k orders
    pub large_order_levels: Vec<PriceLevel>,
    /// Order book shape metrics
    pub bid_slope: Decimal,
    pub ask_slope: Decimal,
    /// Full-depth entropy (more accurate than top-of-book)
    pub full_depth_entropy: Decimal,
}
```

**Value:** More accurate regime detection with complete order book data.

#### 6.1.2 Wallet-Attributed Order Flow

```rust
/// Order flow segmented by wallet characteristics
pub struct WalletOrderFlow {
    /// Timestamp
    pub timestamp: i64,
    /// Symbol
    pub symbol: String,
    /// Flow by wallet size category
    pub whale_flow: Decimal,      // Wallets with >$1M
    pub large_flow: Decimal,      // $100k - $1M
    pub medium_flow: Decimal,     // $10k - $100k
    pub retail_flow: Decimal,     // <$10k
    /// Smart money indicator (historically profitable wallets)
    pub smart_money_flow: Decimal,
    /// Net flow imbalance by category
    pub whale_imbalance: Decimal,
    pub smart_money_imbalance: Decimal,
}
```

**Value:** Know who is buying/selling, not just that buying/selling is happening.

#### 6.1.3 Liquidation Risk Mapping

```rust
/// Liquidation risk analysis from on-chain margin data
pub struct LiquidationRisk {
    /// Current price
    pub current_price: Decimal,
    /// Estimated liquidation clusters
    pub long_liquidation_clusters: Vec<LiquidationCluster>,
    pub short_liquidation_clusters: Vec<LiquidationCluster>,
    /// Total value at risk within price ranges
    pub long_at_risk_5pct: Decimal,  // Longs liquidated if price drops 5%
    pub short_at_risk_5pct: Decimal, // Shorts liquidated if price rises 5%
    /// Cascade probability score (0-1)
    pub cascade_risk_score: Decimal,
}

pub struct LiquidationCluster {
    pub price_level: Decimal,
    pub estimated_volume_usd: Decimal,
    pub wallet_count: u32,
}
```

**Value:** Anticipate liquidation cascades before they happen.

#### 6.1.4 Whale Wallet Tracking

```rust
/// Track large wallet activity
pub struct WhaleActivity {
    /// Timestamp
    pub timestamp: i64,
    /// Recent whale orders (last N minutes)
    pub recent_whale_orders: Vec<WhaleOrder>,
    /// Aggregate whale positioning
    pub net_whale_position_change_1h: Decimal,
    pub net_whale_position_change_24h: Decimal,
    /// Top wallet movements
    pub top_wallet_movements: Vec<WalletMovement>,
}

pub struct WhaleOrder {
    pub wallet: String,
    pub side: Side,
    pub size_usd: Decimal,
    pub price: Decimal,
    pub order_type: OrderType,
    pub timestamp: i64,
}
```

**Value:** Follow smart money with verified on-chain data.

### 6.2 Tier 2: Adapted Existing Features

#### 6.2.1 Enhanced Entropy Metrics

```rust
/// Entropy features enhanced with full on-chain data
pub struct EnhancedEntropyMetrics {
    // Existing entropy metrics (from current implementation)
    pub tick_entropy_1s: Option<Decimal>,
    pub tick_entropy_5s: Option<Decimal>,
    pub tick_entropy_1m: Option<Decimal>,
    pub volume_tick_entropy_1s: Option<Decimal>,
    pub volume_tick_entropy_5s: Option<Decimal>,
    pub volume_tick_entropy_1m: Option<Decimal>,

    // NEW: Full-depth entropy (HL-exclusive)
    pub full_book_entropy: Option<Decimal>,

    // NEW: Wallet-segmented entropy (HL-exclusive)
    pub whale_flow_entropy: Option<Decimal>,
    pub retail_flow_entropy: Option<Decimal>,

    // NEW: Order lifetime entropy (HL-exclusive)
    pub order_lifetime_entropy: Option<Decimal>,

    // Regime classification
    pub regime: RegimeClassification,
    pub regime_confidence: Decimal,
}
```

#### 6.2.2 Order Flow Features (Enhanced)

```rust
/// Order flow features with wallet attribution
pub struct EnhancedOrderFlow {
    // Existing OFI metrics
    pub ofi: Decimal,
    pub trade_imbalance: Decimal,
    pub vwap_deviation: Decimal,

    // NEW: Wallet-attributed OFI (HL-exclusive)
    pub whale_ofi: Decimal,
    pub smart_money_ofi: Decimal,

    // NEW: Order modification metrics (HL-exclusive)
    pub order_cancel_rate: Decimal,
    pub order_modify_rate: Decimal,
    pub spoofing_indicator: Decimal,
}
```

### 6.3 Tier 3: Hyperliquid-Native Analytics

#### 6.3.1 Vault Flow Analytics

```rust
/// Analytics for Hyperliquid vault system
pub struct VaultAnalytics {
    /// Vault identifier
    pub vault_address: String,
    /// Current AUM
    pub aum_usd: Decimal,
    /// Flow metrics
    pub deposits_24h: Decimal,
    pub withdrawals_24h: Decimal,
    pub net_flow_24h: Decimal,
    /// Performance metrics
    pub pnl_24h: Decimal,
    pub pnl_7d: Decimal,
    pub pnl_30d: Decimal,
    /// Position exposure
    pub gross_exposure: Decimal,
    pub net_exposure: Decimal,
    /// Copy trading demand
    pub follower_count: u32,
    pub follower_growth_7d: i32,
}
```

**Value:** Competitive intelligence for vault managers.

#### 6.3.2 Funding Rate Prediction

```rust
/// Funding rate prediction based on order book state
pub struct FundingPrediction {
    /// Current funding rate
    pub current_funding: Decimal,
    /// Predicted next funding
    pub predicted_funding: Decimal,
    /// Prediction confidence
    pub confidence: Decimal,
    /// Contributing factors
    pub oi_imbalance_contribution: Decimal,
    pub order_book_skew_contribution: Decimal,
    pub recent_trade_imbalance_contribution: Decimal,
}
```

#### 6.3.3 Market Maker Identification

```rust
/// Identify likely market maker wallets from behavior patterns
pub struct MarketMakerAnalytics {
    /// Identified MM wallets (probabilistic)
    pub likely_mm_wallets: Vec<MMWallet>,
    /// Aggregate MM activity
    pub mm_bid_depth: Decimal,
    pub mm_ask_depth: Decimal,
    pub mm_spread_contribution: Decimal,
    /// MM inventory proxy
    pub estimated_mm_inventory: Decimal,
}

pub struct MMWallet {
    pub address: String,
    pub mm_probability: Decimal,  // 0-1 confidence score
    pub avg_daily_volume: Decimal,
    pub typical_spread: Decimal,
}
```

---

## 7. Technical Integration Requirements

### 7.1 Hyperliquid API Overview

#### 7.1.1 Available Endpoints

| Endpoint Type | Data Available | Latency | Our Use |
|---------------|----------------|---------|---------|
| REST - Info | Order book, trades, funding | ~100ms | Historical queries |
| REST - User | Positions, orders, fills | ~100ms | Not needed initially |
| WebSocket - Subscription | Real-time book, trades | ~10-50ms | Primary data source |
| Indexer API | Historical on-chain data | Varies | Backfill, analysis |

#### 7.1.2 Key Data Structures

```python
# Hyperliquid order book update
{
    "channel": "l2Book",
    "data": {
        "coin": "BTC",
        "levels": [
            [
                {"px": "43000.0", "sz": "1.5", "n": 3},  # price, size, order count
                {"px": "42999.0", "sz": "2.1", "n": 5},
                # ... full depth available
            ],
            [
                {"px": "43001.0", "sz": "1.2", "n": 2},
                # ... asks
            ]
        ],
        "time": 1704067200000
    }
}

# Hyperliquid trade with wallet attribution
{
    "channel": "trades",
    "data": [
        {
            "coin": "BTC",
            "side": "B",
            "px": "43000.5",
            "sz": "0.5",
            "time": 1704067200123,
            "hash": "0x...",          # Transaction hash
            "tid": 123456,
            "users": ["0xabc...", "0xdef..."]  # Maker and taker wallets
        }
    ]
}
```

### 7.2 Integration Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 HYPERLIQUID INTEGRATION LAYER                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  Hyperliquid WebSocket                   │    │
│  │  ├── l2Book (full depth order book)                     │    │
│  │  ├── trades (with wallet attribution)                   │    │
│  │  ├── userFills (for whale tracking)                     │    │
│  │  └── candles (for reference)                            │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                           │                                      │
│                           ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  Data Normalization                      │    │
│  │  ├── Convert HL format → Internal format                │    │
│  │  ├── Wallet categorization (whale/retail/MM)            │    │
│  │  └── Order book reconstruction                          │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                           │                                      │
│                           ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │               Existing Feature Engines                   │    │
│  │  ├── EntropyEngine (adapted for full depth)             │    │
│  │  ├── OrderFlowEngine (enhanced with wallet data)        │    │
│  │  └── NEW: HyperliquidExclusiveEngine                    │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                           │                                      │
│                           ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Feature Store                          │    │
│  │  ├── Real-time features (Redis)                         │    │
│  │  ├── Historical features (Parquet)                      │    │
│  │  └── Wallet database (PostgreSQL)                       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.3 Code Modifications Required

#### 7.3.1 New Module: `src/exchanges/hyperliquid.rs`

```rust
//! Hyperliquid exchange connector

use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures::{StreamExt, SinkExt};

pub struct HyperliquidConnector {
    ws_url: String,
    symbols: Vec<String>,
    reconnect_policy: ReconnectPolicy,
}

impl HyperliquidConnector {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            ws_url: "wss://api.hyperliquid.xyz/ws".to_string(),
            symbols,
            reconnect_policy: ReconnectPolicy::default(),
        }
    }

    pub async fn subscribe_orderbook(&self) -> impl Stream<Item = HLOrderBook> {
        // Implementation
    }

    pub async fn subscribe_trades(&self) -> impl Stream<Item = HLTrade> {
        // Implementation with wallet attribution
    }
}

/// Hyperliquid-specific order book with full depth
#[derive(Debug, Clone)]
pub struct HLOrderBook {
    pub symbol: String,
    pub timestamp: i64,
    pub bids: Vec<HLPriceLevel>,  // Full depth
    pub asks: Vec<HLPriceLevel>,  // Full depth
}

/// Hyperliquid trade with wallet information
#[derive(Debug, Clone)]
pub struct HLTrade {
    pub symbol: String,
    pub timestamp: i64,
    pub price: Decimal,
    pub size: Decimal,
    pub side: Side,
    pub maker_wallet: String,
    pub taker_wallet: String,
    pub tx_hash: String,
}
```

#### 7.3.2 New Module: `src/features/hyperliquid_features.rs`

```rust
//! Hyperliquid-exclusive feature computation

pub struct HyperliquidFeatureEngine {
    wallet_classifier: WalletClassifier,
    liquidation_tracker: LiquidationTracker,
    mm_identifier: MarketMakerIdentifier,
}

impl HyperliquidFeatureEngine {
    pub fn compute_wallet_flow(&self, trades: &[HLTrade]) -> WalletOrderFlow {
        // Segment order flow by wallet size/type
    }

    pub fn compute_liquidation_risk(
        &self,
        orderbook: &HLOrderBook,
        positions: &[Position]
    ) -> LiquidationRisk {
        // Estimate liquidation clusters
    }

    pub fn compute_full_depth_entropy(&self, orderbook: &HLOrderBook) -> Decimal {
        // Entropy on complete order book
    }
}
```

### 7.4 Infrastructure Requirements

| Component | Specification | Purpose |
|-----------|---------------|---------|
| Hyperliquid WS connection | Persistent, auto-reconnect | Real-time data |
| Wallet database | PostgreSQL, indexed | Wallet classification |
| Historical indexer | Batch job, daily | Backfill data |
| Redis pub/sub | Existing | Feature distribution |
| Additional storage | ~50GB/month | Full depth data |

---

## 8. Go-to-Market Strategy

### 8.1 Target Customer Acquisition

#### 8.1.1 Community Infiltration

```
Hyperliquid Community Channels:
├── Discord: Primary community hub (~50k members)
│   ├── #trading-chat - Active traders
│   ├── #algo-trading - Target audience
│   └── #vault-managers - Premium segment
├── Twitter/X: Influential accounts
│   ├── @HyperliquidX - Official
│   ├── Trading influencers with HL focus
│   └── Quant finance accounts
└── Telegram: Secondary groups
```

**Action Plan:**
1. Join Discord, participate genuinely for 2-4 weeks
2. Identify pain points through observation
3. Build relationships before promoting
4. Offer value (free insights) before asking for anything

#### 8.1.2 Content Strategy

| Content Type | Topic | Purpose | Frequency |
|--------------|-------|---------|-----------|
| Twitter thread | "Entropy Regimes on Hyperliquid" | Awareness | Weekly |
| Free tool | "HL Whale Tracker" | Lead generation | One-time |
| Blog post | "Liquidation Cascade Analysis" | SEO, credibility | Bi-weekly |
| Discord bot | Basic entropy alerts | Community value | One-time |
| Case study | "How I detected the [event]" | Proof of value | Monthly |

#### 8.1.3 Launch Sequence

```
Week 1-2: Soft Launch
├── Invite 10 traders from Discord DMs
├── Free access for feedback
├── Daily feedback calls
└── Iterate rapidly

Week 3-4: Beta Launch
├── Open to 50 users (waitlist)
├── $99/month beta pricing
├── Public testimonials
└── Case study from beta users

Week 5-6: Public Launch
├── Twitter announcement thread
├── Discord announcement
├── Full pricing enabled
└── Referral program active
```

### 8.2 Pricing Strategy

#### 8.2.1 Tier Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                      PRICING TIERS                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  FREE (Lead Generation)                                          │
│  ├── Delayed entropy indicator (1 hour delay)                   │
│  ├── Basic whale alert (>$500k trades)                          │
│  ├── Limited historical (7 days)                                │
│  └── Community Discord access                                    │
│                                                                  │
│  TRADER ($199/month)                                             │
│  ├── Real-time entropy features (all timeframes)                │
│  ├── Order flow indicators                                       │
│  ├── Whale alerts (configurable threshold)                      │
│  ├── 1 symbol                                                    │
│  ├── API access (1,000 calls/day)                               │
│  ├── 30 days historical                                          │
│  └── Email support                                               │
│                                                                  │
│  PRO ($499/month)                                                │
│  ├── Everything in Trader                                        │
│  ├── Full order book features                                    │
│  ├── Liquidation risk dashboard                                  │
│  ├── 10 wallet watchlist                                         │
│  ├── All symbols                                                 │
│  ├── API access (10,000 calls/day)                              │
│  ├── Backtesting access                                          │
│  ├── 90 days historical                                          │
│  └── Priority email support                                      │
│                                                                  │
│  VAULT MANAGER ($999/month)                                      │
│  ├── Everything in Pro                                           │
│  ├── Vault flow analytics                                        │
│  ├── Competitor vault tracking                                   │
│  ├── Custom alerts and webhooks                                  │
│  ├── Unlimited wallet watchlist                                  │
│  ├── API access (unlimited)                                      │
│  ├── Full historical                                             │
│  └── Slack support channel                                       │
│                                                                  │
│  ENTERPRISE (Custom)                                             │
│  ├── Everything in Vault Manager                                 │
│  ├── Custom feature development                                  │
│  ├── Dedicated infrastructure                                    │
│  ├── SLA guarantees                                              │
│  ├── On-call support                                             │
│  └── Annual contract                                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### 8.2.2 Pricing Psychology

- **$199** = Accessible to serious individual traders
- **$499** = Signals "professional tool" without being enterprise-only
- **$999** = Vault managers have AUM-based revenue, can afford premium
- **Free tier** = Lead generation, proves value before payment

### 8.3 Competitive Positioning

**Positioning Statement:**
> "The only analytics platform built specifically for Hyperliquid. Deep microstructure intelligence that's impossible on CEXes."

**Key Messages:**
1. "See what CEX traders can't see" (on-chain transparency)
2. "Know when regimes shift before the crowd" (entropy edge)
3. "Track the whales with verified data" (wallet attribution)
4. "Built by quants, for quants" (technical credibility)

---

## 9. Revenue Model

### 9.1 Revenue Projections

#### Year 1 Projections (Monthly)

| Month | Free | Trader | Pro | Vault | MRR | Notes |
|-------|------|--------|-----|-------|-----|-------|
| 1 | 50 | 5 | 2 | 0 | $1,993 | Beta launch |
| 2 | 100 | 15 | 5 | 1 | $5,480 | Public launch |
| 3 | 200 | 25 | 10 | 2 | $11,965 | Growth |
| 4 | 300 | 40 | 15 | 3 | $18,445 | |
| 5 | 400 | 55 | 20 | 4 | $24,925 | |
| 6 | 500 | 70 | 25 | 5 | $31,405 | |
| 7 | 600 | 80 | 30 | 6 | $36,890 | |
| 8 | 700 | 90 | 35 | 7 | $42,375 | |
| 9 | 800 | 100 | 40 | 8 | $47,860 | Sustainability threshold |
| 10 | 900 | 110 | 45 | 9 | $53,345 | |
| 11 | 1000 | 120 | 50 | 10 | $58,830 | |
| 12 | 1100 | 130 | 55 | 11 | $64,315 | |

**Year 1 Total ARR: ~$770k** (if growth trajectory holds)

#### Conservative Scenario

| Metric | Month 6 | Month 12 | Month 18 |
|--------|---------|----------|----------|
| Paying customers | 40 | 80 | 120 |
| MRR | $15,000 | $30,000 | $45,000 |
| ARR | $180,000 | $360,000 | $540,000 |

#### Optimistic Scenario

| Metric | Month 6 | Month 12 | Month 18 |
|--------|---------|----------|----------|
| Paying customers | 100 | 250 | 400 |
| MRR | $40,000 | $100,000 | $160,000 |
| ARR | $480,000 | $1,200,000 | $1,920,000 |

### 9.2 Unit Economics

| Metric | Value | Notes |
|--------|-------|-------|
| Average Revenue Per User (ARPU) | $350/month | Blended across tiers |
| Customer Acquisition Cost (CAC) | $100-300 | Content + community marketing |
| Lifetime Value (LTV) | $4,200 | 12-month avg lifetime |
| LTV:CAC Ratio | 14-42x | Healthy range |
| Gross Margin | 75-80% | After infrastructure costs |
| Payback Period | <1 month | Fast payback |

### 9.3 Cost Structure

| Category | Month 1-6 | Month 7-12 | Notes |
|----------|-----------|------------|-------|
| Infrastructure | $500-1,000 | $1,000-2,000 | Scales with users |
| Data/APIs | $0-500 | $500-1,000 | HL API is free currently |
| Domain/Services | $50 | $50 | Basic operational |
| Marketing | $500-1,000 | $1,000-2,000 | Content, tools |
| Legal/Compliance | $200 | $200 | Amortized |
| **Total OpEx** | **$1,250-2,750** | **$2,750-5,250** | |
| **Break-even** | **~10-15 customers** | | At $300 ARPU |

---

## 10. Implementation Roadmap

### 10.1 Phase 1: Hyperliquid Integration (Weeks 1-4)

```
Week 1: API Exploration & Connectivity
├── [ ] Study Hyperliquid API documentation
├── [ ] Implement WebSocket connector
├── [ ] Test order book streaming
├── [ ] Test trade streaming with wallet data
└── Deliverable: Raw data ingestion working

Week 2: Data Normalization
├── [ ] Create HLOrderBook struct
├── [ ] Create HLTrade struct with wallet attribution
├── [ ] Implement data validation
├── [ ] Handle reconnection and errors
└── Deliverable: Reliable data pipeline

Week 3: Feature Adaptation
├── [ ] Port entropy calculations to HL data
├── [ ] Adapt OFI calculations
├── [ ] Implement full-depth entropy
├── [ ] Test feature accuracy
└── Deliverable: Core features on Hyperliquid

Week 4: Hyperliquid-Exclusive Features v1
├── [ ] Implement basic whale tracking
├── [ ] Implement wallet categorization
├── [ ] Create wallet database schema
├── [ ] Initial liquidation risk metrics
└── Deliverable: Differentiated feature set
```

### 10.2 Phase 2: Commercial Layer (Weeks 5-8)

```
Week 5: API Foundation
├── [ ] Setup Axum REST API
├── [ ] Implement /v1/features/{symbol} endpoint
├── [ ] Implement /v1/features/{symbol}/history
├── [ ] Add request validation
└── Deliverable: Working REST API

Week 6: WebSocket & Auth
├── [ ] Implement WebSocket gateway
├── [ ] Add API key authentication
├── [ ] Implement rate limiting by tier
├── [ ] Create user management (PostgreSQL)
└── Deliverable: Secured API access

Week 7: Payments & Accounts
├── [ ] Integrate Stripe subscriptions
├── [ ] Implement tier enforcement
├── [ ] Create account management endpoints
├── [ ] Usage tracking
└── Deliverable: Monetization ready

Week 8: Documentation & Polish
├── [ ] Write API documentation
├── [ ] Create quick start guide
├── [ ] Code examples (Python, JavaScript)
├── [ ] Error message improvements
└── Deliverable: Developer-ready product
```

### 10.3 Phase 3: Launch Preparation (Weeks 9-10)

```
Week 9: Infrastructure & Testing
├── [ ] Setup CI/CD pipeline
├── [ ] Deploy to Fly.io / Railway
├── [ ] Load testing (target: 100 concurrent users)
├── [ ] Security review
├── [ ] Monitoring setup (Prometheus + Grafana)
└── Deliverable: Production-ready infrastructure

Week 10: Soft Launch
├── [ ] Recruit 10 beta users from HL Discord
├── [ ] Onboard and gather feedback
├── [ ] Bug fixes and improvements
├── [ ] Prepare launch content
└── Deliverable: Validated product
```

### 10.4 Phase 4: Public Launch (Weeks 11-12)

```
Week 11: Beta Launch
├── [ ] Open waitlist (50 slots)
├── [ ] Beta pricing ($99/month)
├── [ ] Daily feedback collection
├── [ ] Rapid iteration
└── Deliverable: Paying beta customers

Week 12: Public Launch
├── [ ] Twitter announcement thread
├── [ ] Discord announcement
├── [ ] Full pricing enabled
├── [ ] Referral program
└── Deliverable: Publicly available product
```

### 10.5 Phase 5: Growth & Expansion (Months 4-6)

```
Month 4: Dashboard MVP
├── [ ] Basic web dashboard
├── [ ] Real-time charts
├── [ ] Account management UI
└── Expands addressable market to non-API users

Month 5: Advanced Features
├── [ ] Vault flow analytics
├── [ ] Liquidation cascade prediction
├── [ ] Custom alert webhooks
└── Enables Vault Manager tier

Month 6: Platform Expansion
├── [ ] Consider dYdX integration
├── [ ] Consider Vertex integration
├── [ ] Evaluate based on customer demand
└── Reduces platform dependency risk
```

### 10.6 Milestone Summary

| Milestone | Target Date | Success Criteria |
|-----------|-------------|------------------|
| M1: HL Integration | Week 4 | Features computing on HL data |
| M2: API Launch | Week 8 | REST + WS APIs working |
| M3: Soft Launch | Week 10 | 10 beta users onboarded |
| M4: Public Launch | Week 12 | First paying customers |
| M5: Sustainability | Month 9 | $40k MRR reached |

---

## 11. Risk Analysis

### 11.1 Platform Dependency Risk

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Hyperliquid declines | Low | Critical | Build portable core, expand to other platforms |
| Hyperliquid builds native analytics | Medium | High | Move fast, differentiate on depth |
| API changes break integration | Medium | Medium | Abstract API layer, monitor announcements |
| Regulatory action against HL | Low | High | Geographic restrictions, platform diversification |

**Mitigation Strategy:**
- Core entropy/MI framework is exchange-agnostic
- Plan expansion to dYdX, Vertex by Month 6
- Monitor Hyperliquid team communications closely
- Build relationships with HL team if possible

### 11.2 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data quality issues | Medium | Medium | Validation, monitoring, alerts |
| Latency problems | Low | Medium | Optimize hot paths, edge deployment |
| Scaling bottlenecks | Medium | Medium | Load testing, horizontal scaling |
| Security breach | Low | Critical | Security audit, penetration testing |

### 11.3 Business Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Low customer adoption | Medium | High | Validate before building, content marketing |
| High churn | Medium | High | Focus on stickiness, feature depth |
| Competitor entry | High | Medium | First mover advantage, continuous innovation |
| Crypto bear market | High | High | Runway planning, focus on committed users |

### 11.4 Risk-Adjusted Probability of Success

| Outcome | Probability |
|---------|-------------|
| Sustainable business ($400-800k ARR) | 25-30% |
| Modest success ($100-400k ARR) | 30-35% |
| Acqui-hire or small exit | 10-15% |
| Pivot to different market | 10-15% |
| Shutdown | 15-20% |

**Combined "success" probability: ~55-65%** with Hyperliquid focus (vs ~50-55% generic approach).

---

## 12. Success Metrics

### 12.1 Key Performance Indicators

#### Product Metrics

| Metric | Month 3 Target | Month 6 Target | Month 12 Target |
|--------|----------------|----------------|-----------------|
| API Uptime | 99.5% | 99.9% | 99.95% |
| API Latency (p99) | <500ms | <200ms | <100ms |
| Feature Accuracy | >95% | >98% | >99% |
| WebSocket Connections | 100 | 500 | 2,000 |

#### Business Metrics

| Metric | Month 3 Target | Month 6 Target | Month 12 Target |
|--------|----------------|----------------|-----------------|
| Paying Customers | 25 | 80 | 200 |
| MRR | $8,000 | $30,000 | $70,000 |
| Monthly Churn | <10% | <5% | <3% |
| NPS Score | >30 | >40 | >50 |

#### Growth Metrics

| Metric | Month 3 Target | Month 6 Target | Month 12 Target |
|--------|----------------|----------------|-----------------|
| Free Users | 200 | 500 | 1,500 |
| Free → Paid Conversion | 5% | 10% | 15% |
| Twitter Followers | 500 | 2,000 | 5,000 |
| Discord Members | 100 | 500 | 1,500 |

### 12.2 Gate Criteria

#### Gate 1: Technical Validation (Week 4)
- [ ] Hyperliquid data ingestion working
- [ ] Core features computing correctly
- [ ] Feature accuracy validated against manual calculations
- [ ] System stable for 72+ hours

#### Gate 2: Product Validation (Week 10)
- [ ] 10 beta users actively using product
- [ ] NPS > 20 from beta users
- [ ] At least 3 users willing to pay
- [ ] No critical bugs in 7 days

#### Gate 3: Commercial Validation (Month 3)
- [ ] 25+ paying customers
- [ ] MRR > $5,000
- [ ] Monthly churn < 15%
- [ ] Positive unit economics

#### Gate 4: Sustainability (Month 9)
- [ ] MRR > $40,000
- [ ] Monthly churn < 5%
- [ ] Team of 2-3 supportable
- [ ] 6+ months runway

---

## Appendices

### Appendix A: Hyperliquid API Reference

#### A.1 WebSocket Subscription

```python
import websockets
import json

async def subscribe_hyperliquid():
    uri = "wss://api.hyperliquid.xyz/ws"
    async with websockets.connect(uri) as ws:
        # Subscribe to order book
        await ws.send(json.dumps({
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": "BTC"}
        }))

        # Subscribe to trades
        await ws.send(json.dumps({
            "method": "subscribe",
            "subscription": {"type": "trades", "coin": "BTC"}
        }))

        async for message in ws:
            data = json.loads(message)
            process_message(data)
```

#### A.2 REST API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/info` | POST | Market info, order books |
| `/exchange` | POST | Trading operations |
| `/explorer` | GET | On-chain data queries |

### Appendix B: Wallet Classification Heuristics

```python
class WalletClassifier:
    """Classify wallets based on historical behavior"""

    WHALE_THRESHOLD_USD = 1_000_000
    LARGE_THRESHOLD_USD = 100_000
    MEDIUM_THRESHOLD_USD = 10_000

    def classify(self, wallet: str, history: WalletHistory) -> WalletType:
        avg_trade_size = history.total_volume / history.trade_count

        if avg_trade_size > self.WHALE_THRESHOLD_USD:
            return WalletType.WHALE
        elif self.is_likely_mm(history):
            return WalletType.MARKET_MAKER
        elif avg_trade_size > self.LARGE_THRESHOLD_USD:
            return WalletType.LARGE
        elif avg_trade_size > self.MEDIUM_THRESHOLD_USD:
            return WalletType.MEDIUM
        else:
            return WalletType.RETAIL

    def is_likely_mm(self, history: WalletHistory) -> bool:
        """Heuristics for market maker identification"""
        checks = [
            history.two_sided_ratio > 0.8,  # Trades both sides
            history.avg_time_in_book < 60,  # Quick order turnover
            history.cancel_ratio > 0.5,      # High cancel rate
            history.daily_volume > 1_000_000, # High volume
        ]
        return sum(checks) >= 3
```

### Appendix C: Competitive Feature Matrix

| Feature | Us | Coinalyze | Velo | Native HL |
|---------|-----|-----------|------|-----------|
| Real-time entropy | ✅ | ❌ | ❌ | ❌ |
| Full-depth order book | ✅ | ❌ | ❌ | ⚠️ Basic |
| Wallet attribution | ✅ | ❌ | ❌ | ⚠️ Basic |
| Liquidation risk | ✅ | ⚠️ | ⚠️ | ❌ |
| Whale tracking | ✅ | ⚠️ | ⚠️ | ❌ |
| Vault analytics | ✅ | ❌ | ❌ | ⚠️ Basic |
| API access | ✅ | ✅ | ✅ | ❌ |
| Backtesting | ✅ | ❌ | ❌ | ❌ |
| MI-based features | ✅ | ❌ | ❌ | ❌ |

### Appendix D: Sample Customer Interview Questions

1. What analytics tools do you currently use for Hyperliquid trading?
2. What's the biggest pain point in your current workflow?
3. How much do you currently spend on trading tools/data?
4. If you could have one feature that doesn't exist, what would it be?
5. How important is API access vs dashboard for your use case?
6. What would make you switch from your current solution?
7. How do you currently track whale activity?
8. Have you ever been caught in a liquidation cascade?

### Appendix E: Technical Glossary

| Term | Definition |
|------|------------|
| Full-depth order book | Complete order book, not just top N levels |
| Wallet attribution | Linking trades to specific wallet addresses |
| Liquidation cluster | Price level with concentrated liquidation risk |
| Vault | Hyperliquid's copy-trading mechanism |
| MM (Market Maker) | Entity providing two-sided liquidity |
| OFI | Order Flow Imbalance |
| KSG | Kraskov-Stögbauer-Grassberger MI estimator |
| Entropy regime | Market state classified by information content |

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 2.0 | 2026-01-17 | System | Initial Hyperliquid expansion strategy |

---

## References

1. Hyperliquid Documentation: https://hyperliquid.gitbook.io/
2. EXTENDED_REQUIREMENTS_0.md - Core system design
3. EXTENDED_REQUIREMENTS_1.md - ML and validation framework
4. SPECULATION_0.md - Commercialization analysis

---

*This document represents strategic planning for platform expansion. All projections are estimates based on market analysis and should be validated through customer discovery.*
