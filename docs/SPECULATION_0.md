# SPECULATION_0: Commercialization Analysis

**Document Version:** 0.1
**Date:** 2026-01-17
**Status:** Speculative / Business Planning
**Related:** EXTENDED_REQUIREMENTS_1.md

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Value Stack Analysis](#2-value-stack-analysis)
3. [Business Model Options](#3-business-model-options)
4. [Technical Architecture for Commercial Platform](#4-technical-architecture-for-commercial-platform)
5. [API Specifications](#5-api-specifications)
6. [Regulatory Considerations](#6-regulatory-considerations)
7. [Competitive Landscape](#7-competitive-landscape)
8. [Revenue Projections](#8-revenue-projections)
9. [Implementation Roadmap](#9-implementation-roadmap)
10. [Risk Analysis](#10-risk-analysis)
11. [Summary and Recommendations](#11-summary-and-recommendations)

---

## 1. Executive Summary

This document explores commercialization pathways for the Information-Guided Adaptive Trading System, specifically focusing on exposing entropy-based market microstructure features through web interfaces and real-time APIs.

### Core Value Proposition

The system provides **information-theoretic market regime detection** that is:
- Mathematically grounded (KSG mutual information, Shannon entropy)
- Real-time capable (sub-second latency)
- Exchange-agnostic (methodology transfers across markets)
- Academically defensible (published literature foundation)

### Recommended Approach

**Hybrid Model**: Sell infrastructure and analytics while keeping proprietary trading edge private.

---

## 2. Value Stack Analysis

The commercialization opportunity exists across five distinct value layers:

### Layer 1: Raw Data (Lowest Value)
- Order book snapshots
- Trade streams
- Already commoditized by exchanges and data vendors
- **Margin potential**: Very low

### Layer 2: Computed Features (Medium Value)
- Entropy metrics (tick entropy, volume entropy)
- Order flow imbalance
- Microstructure indicators
- **Margin potential**: Moderate - requires expertise to compute correctly

### Layer 3: Regime Detection (High Value)
- Real-time regime classification
- Regime transition probabilities
- Confidence intervals
- **Margin potential**: High - actionable intelligence

### Layer 4: Predictive Signals (Highest Value)
- Forward-looking regime forecasts
- Optimal timing indicators
- Risk-adjusted opportunity scores
- **Margin potential**: Very high - direct alpha potential

### Layer 5: Execution (Keep Private)
- Actual trading strategies
- Position management
- Risk controls
- **Recommendation**: Never expose - this is your edge

### Value Layer Summary

| Layer | Product | Value | Recommendation |
|-------|---------|-------|----------------|
| 1 | Raw Data | $ | Don't compete here |
| 2 | Features | $$ | Viable SaaS product |
| 3 | Regimes | $$$ | Premium tier offering |
| 4 | Signals | $$$$ | Institutional only, careful exposure |
| 5 | Execution | $$$$$ | Keep proprietary |

---

## 3. Business Model Options

### Model A: Feature-as-a-Service (FaaS)

**Concept**: Provide real-time computed features via WebSocket API.

**Target Customers**:
- Quantitative researchers
- Algorithmic trading firms
- Academic institutions
- FinTech startups building trading tools

**Pricing Structure**:
```
Free Tier:      5 features, 1 symbol, 1-minute delayed
Starter:        $49/month  - 20 features, 5 symbols, real-time
Professional:   $199/month - All features, 25 symbols, real-time
Enterprise:     Custom     - Unlimited, priority support, SLA
```

**Pros**:
- Recurring revenue
- Sticky customers (integration cost)
- Scales well with infrastructure

**Cons**:
- Support burden
- Infrastructure costs scale with users
- Feature commoditization risk

### Model B: Analytics Platform (Dashboard SaaS)

**Concept**: Web-based dashboard for regime visualization and alerts.

**Target Customers**:
- Discretionary traders seeking edge
- Trading educators
- Market analysts
- Financial media

**Pricing Structure**:
```
Basic:          $29/month  - View-only dashboard, 3 symbols
Pro:            $99/month  - Alerts, historical analysis, 10 symbols
Team:           $299/month - Multi-user, API access, 50 symbols
```

**Pros**:
- Broader market (non-technical users)
- Lower support complexity
- Visual differentiation

**Cons**:
- UI/UX development cost
- Lower per-user revenue
- Churn risk if not sticky

### Model C: Research Data Product

**Concept**: Sell historical feature datasets for backtesting and research.

**Target Customers**:
- Quantitative hedge funds
- Academic researchers
- Financial data vendors (for redistribution)

**Pricing Structure**:
```
Sample:         Free       - 1 week of data
Monthly:        $500       - Rolling 30-day history
Historical:     $2,000     - Full historical archive
Enterprise:     $10,000+   - Raw + processed, custom features
```

**Pros**:
- One-time sales, low marginal cost
- No real-time infrastructure needed
- Academic citation potential

**Cons**:
- Smaller market
- One-time revenue vs recurring
- Data quality expectations high

### Model D: Educational Platform

**Concept**: Courses and content on entropy-based trading analysis.

**Target Customers**:
- Aspiring quants
- Trading educators (B2B)
- University programs

**Pricing Structure**:
```
Course:         $199-499   - Self-paced online course
Workshop:       $999       - Live intensive
Certification:  $1,499     - Full program with assessment
Licensing:      $5,000+    - University curriculum license
```

**Pros**:
- High margin
- Establishes thought leadership
- Feeds other business lines

**Cons**:
- Content creation cost
- Different skill set (education vs engineering)
- Slower scale

### Model E: Hybrid (Recommended)

**Concept**: Combine Models A + B with selective exposure.

**Architecture**:
```
Public Website (Free)
├── Educational content (thought leadership)
├── Live demo dashboard (limited features)
└── Documentation and examples

Paid Platform
├── Full dashboard with all visualizations
├── WebSocket API for real-time features
├── REST API for historical data
└── Alerting and notification system

Enterprise Tier
├── Dedicated infrastructure
├── Custom feature development
├── Direct support channel
└── SLA guarantees
```

**Revenue Mix**:
- 40% Dashboard subscriptions
- 35% API subscriptions
- 15% Enterprise contracts
- 10% Educational content

---

## 4. Technical Architecture for Commercial Platform

### System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     COMMERCIAL PLATFORM                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Binance    │    │    OKX       │    │   Bybit      │      │
│  │  WebSocket   │    │  WebSocket   │    │  WebSocket   │      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │               │
│         └───────────────────┼───────────────────┘               │
│                             ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Feature Computation Cluster                 │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐    │   │
│  │  │Entropy  │  │Order    │  │Trade    │  │Regime   │    │   │
│  │  │Engine   │  │Flow     │  │Impact   │  │Detector │    │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘    │   │
│  └─────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│                             ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Redis Pub/Sub                         │   │
│  │           (Feature Distribution Layer)                   │   │
│  └─────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│         ┌───────────────────┼───────────────────┐               │
│         ▼                   ▼                   ▼               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  WebSocket   │    │   REST API   │    │  Dashboard   │      │
│  │   Gateway    │    │   Gateway    │    │   Backend    │      │
│  │  (Rust/Go)   │    │  (Rust/Go)   │    │   (Rust)     │      │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│         │                   │                   │               │
└─────────┼───────────────────┼───────────────────┼───────────────┘
          │                   │                   │
          ▼                   ▼                   ▼
    ┌──────────┐        ┌──────────┐        ┌──────────┐
    │  Quant   │        │  Algo    │        │  Web     │
    │  Client  │        │  Trader  │        │  Browser │
    └──────────┘        └──────────┘        └──────────┘
```

### Component Specifications

#### 4.1 Data Ingestion Layer

**Purpose**: Connect to multiple exchanges, normalize data, ensure reliability.

```rust
// Simplified ingestion architecture
pub struct ExchangeConnector {
    exchange: Exchange,
    symbols: Vec<String>,
    reconnect_policy: ReconnectPolicy,
    health_check_interval: Duration,
}

pub enum Exchange {
    Binance,
    OKX,
    Bybit,
    Coinbase,
}

impl ExchangeConnector {
    pub async fn stream_orderbook(&self) -> impl Stream<Item = NormalizedOrderBook>;
    pub async fn stream_trades(&self) -> impl Stream<Item = NormalizedTrade>;
}
```

**Reliability Requirements**:
- Automatic reconnection with exponential backoff
- Health monitoring and alerting
- Failover to backup connections
- Message deduplication

#### 4.2 Feature Computation Cluster

**Purpose**: Compute all features in real-time with horizontal scalability.

**Design Principles**:
- Stateless computation where possible
- Shared state via Redis for regime detection
- Feature versioning for backward compatibility

**Scaling Strategy**:
```
Symbols 1-10:    Single instance
Symbols 10-50:   2-3 instances with symbol sharding
Symbols 50-200:  Kubernetes cluster with auto-scaling
Symbols 200+:    Multi-region deployment
```

#### 4.3 Distribution Layer (Redis)

**Purpose**: Decouple computation from delivery, enable fan-out.

**Channel Structure**:
```
features:{symbol}:entropy      # Entropy metrics
features:{symbol}:orderflow    # Order flow features
features:{symbol}:regime       # Regime classification
features:{symbol}:all          # Combined snapshot
alerts:{symbol}:transition     # Regime transition alerts
```

**Message Format**:
```json
{
  "symbol": "BTCUSDT",
  "timestamp": 1737100800000,
  "sequence": 12345678,
  "features": {
    "tick_entropy_1s": 0.693,
    "tick_entropy_5s": 0.721,
    "volume_entropy_1s": 0.654,
    "ofi": 0.234,
    "regime": "high_entropy",
    "regime_confidence": 0.87
  }
}
```

#### 4.4 API Gateways

**WebSocket Gateway**:
- Connection management
- Authentication (JWT/API key)
- Rate limiting per tier
- Subscription management
- Heartbeat/keepalive

**REST Gateway**:
- Historical data queries
- Account management
- API key provisioning
- Usage statistics

#### 4.5 Dashboard Backend

**Technology Stack**:
- Backend: Rust (Axum or Actix-web)
- Frontend: React/Vue with real-time charts
- Charts: TradingView lightweight-charts or Apache ECharts
- State: Redux/Vuex with WebSocket updates

---

## 5. API Specifications

### 5.1 WebSocket API

#### Connection

```
wss://api.entropymetrics.io/v1/stream
```

#### Authentication

```json
// Connect with API key in header
{
  "type": "auth",
  "api_key": "em_live_xxx...",
  "timestamp": 1737100800000,
  "signature": "hmac_sha256(...)"
}
```

#### Subscription Management

```json
// Subscribe to features
{
  "type": "subscribe",
  "channels": [
    "features:BTCUSDT:entropy",
    "features:ETHUSDT:entropy",
    "alerts:BTCUSDT:transition"
  ]
}

// Unsubscribe
{
  "type": "unsubscribe",
  "channels": ["features:BTCUSDT:entropy"]
}
```

#### Message Types

```json
// Feature update
{
  "type": "feature_update",
  "channel": "features:BTCUSDT:entropy",
  "data": {
    "symbol": "BTCUSDT",
    "timestamp": 1737100800000,
    "tick_entropy_1s": 0.693,
    "tick_entropy_5s": 0.721,
    "tick_entropy_10s": 0.698,
    "tick_entropy_30s": 0.712,
    "tick_entropy_1m": 0.705,
    "volume_tick_entropy_1s": 0.654,
    "volume_tick_entropy_5s": 0.678
  }
}

// Regime alert
{
  "type": "alert",
  "channel": "alerts:BTCUSDT:transition",
  "data": {
    "symbol": "BTCUSDT",
    "timestamp": 1737100800000,
    "from_regime": "low_entropy",
    "to_regime": "high_entropy",
    "confidence": 0.87,
    "entropy_delta": 0.15
  }
}

// Heartbeat
{
  "type": "heartbeat",
  "timestamp": 1737100800000,
  "server_time": 1737100800001
}
```

### 5.2 REST API

#### Base URL

```
https://api.entropymetrics.io/v1
```

#### Endpoints

```
GET  /features/{symbol}/current
GET  /features/{symbol}/history?start=&end=&interval=
GET  /regimes/{symbol}/current
GET  /regimes/{symbol}/history?start=&end=
GET  /symbols
GET  /account/usage
POST /account/api-keys
DELETE /account/api-keys/{key_id}
```

#### Example Responses

```json
// GET /features/BTCUSDT/current
{
  "symbol": "BTCUSDT",
  "timestamp": 1737100800000,
  "features": {
    "entropy": {
      "tick_entropy_1s": 0.693,
      "tick_entropy_5s": 0.721,
      "tick_entropy_10s": 0.698,
      "tick_entropy_30s": 0.712,
      "tick_entropy_1m": 0.705,
      "volume_tick_entropy_1s": 0.654,
      "volume_tick_entropy_5s": 0.678
    },
    "orderflow": {
      "ofi": 0.234,
      "trade_imbalance": 0.156,
      "vwap_deviation": -0.0023
    },
    "regime": {
      "current": "high_entropy",
      "confidence": 0.87,
      "duration_seconds": 342
    }
  }
}

// GET /features/BTCUSDT/history?start=2026-01-17T00:00:00Z&end=2026-01-17T01:00:00Z&interval=1m
{
  "symbol": "BTCUSDT",
  "interval": "1m",
  "data": [
    {
      "timestamp": 1737100800000,
      "tick_entropy_1m": 0.705,
      "volume_tick_entropy_1m": 0.678,
      "ofi_avg": 0.123,
      "regime": "high_entropy"
    },
    // ... more data points
  ]
}
```

### 5.3 Rate Limits

| Tier | WebSocket Connections | REST Requests/min | Symbols |
|------|----------------------|-------------------|---------|
| Free | 1 | 10 | 1 |
| Starter | 2 | 60 | 5 |
| Professional | 5 | 300 | 25 |
| Enterprise | Unlimited | Custom | Unlimited |

---

## 6. Regulatory Considerations

### 6.1 Data Licensing

**Exchange Data Terms**:
- Binance: Review Market Data Agreement for redistribution rights
- Most exchanges require licensing for commercial redistribution
- Raw data redistribution typically restricted
- Derived/computed features may have different terms

**Recommendation**:
- Consult legal counsel before launch
- Focus on computed features (typically less restricted)
- Obtain explicit redistribution licenses if needed

### 6.2 Financial Regulations

**Not Investment Advice**:
- Clear disclaimers required
- Features are informational, not recommendations
- No performance guarantees

**Potential Classifications**:
- Information service provider (most likely)
- Research provider (if selling signals)
- Investment adviser (if providing recommendations) - AVOID

**Jurisdictional Considerations**:
- US: SEC/CFTC oversight potential
- EU: MiFID II implications
- UK: FCA considerations
- Singapore: MAS regulations

**Recommendation**:
- Structure as information/analytics service
- Avoid signal language ("buy", "sell", "profitable")
- Geographic restrictions may be prudent initially

### 6.3 Privacy and Security

**Requirements**:
- GDPR compliance (if serving EU)
- SOC 2 Type II for enterprise customers
- Data encryption at rest and in transit
- API key security and rotation

---

## 7. Competitive Landscape

### 7.1 Direct Competitors

| Competitor | Offering | Pricing | Differentiation |
|------------|----------|---------|-----------------|
| Kaiko | Market data, indices | Enterprise ($$$) | Breadth of data |
| CryptoQuant | On-chain + market data | $99-399/mo | On-chain focus |
| Glassnode | On-chain analytics | $29-799/mo | On-chain metrics |
| The TIE | Sentiment + data | Enterprise | NLP/Sentiment |
| Nansen | On-chain analytics | $150-2500/mo | Wallet labeling |

### 7.2 Competitive Positioning

**Our Differentiation**:
1. **Information-theoretic approach** - Unique methodology
2. **Academic foundation** - Published literature backing
3. **Real-time regime detection** - Actionable, not just descriptive
4. **Microstructure focus** - Different from on-chain competitors

**Gap in Market**:
- Most competitors focus on on-chain data or basic market data
- Few provide real-time computed features
- None (publicly) use entropy-based regime detection
- Opportunity for "Bloomberg Terminal for Crypto Microstructure"

### 7.3 Moat Considerations

**Defensible Elements**:
- Proprietary feature computation (trade secrets)
- Accumulated historical data
- Customer relationships and integration
- Domain expertise and research

**Vulnerable Elements**:
- Entropy is a known concept (can be replicated)
- Exchange data is accessible to anyone
- Open-source tools exist for basic features

**Strategy**:
- Move fast, establish brand
- Continuous feature innovation
- Lock in enterprise customers with integration depth

---

## 8. Revenue Projections

### 8.1 Conservative Scenario (Year 1-3)

**Assumptions**:
- Slow growth, limited marketing
- Focus on product-market fit
- Bootstrap or minimal funding

| Metric | Year 1 | Year 2 | Year 3 |
|--------|--------|--------|--------|
| Free Users | 500 | 2,000 | 5,000 |
| Starter ($49/mo) | 20 | 80 | 200 |
| Professional ($199/mo) | 5 | 25 | 75 |
| Enterprise ($2k/mo) | 0 | 2 | 5 |
| **MRR** | $1,975 | $9,870 | $34,725 |
| **ARR** | $23,700 | $118,440 | $416,700 |

### 8.2 Moderate Scenario (Year 1-3)

**Assumptions**:
- Active marketing and content
- Seed funding for growth
- 2 FTE dedicated to platform

| Metric | Year 1 | Year 2 | Year 3 |
|--------|--------|--------|--------|
| Free Users | 2,000 | 10,000 | 30,000 |
| Starter ($49/mo) | 100 | 500 | 1,500 |
| Professional ($199/mo) | 25 | 150 | 500 |
| Enterprise ($2k/mo) | 2 | 10 | 30 |
| **MRR** | $13,825 | $84,350 | $232,000 |
| **ARR** | $165,900 | $1,012,200 | $2,784,000 |

### 8.3 Optimistic Scenario (Year 1-3)

**Assumptions**:
- Strong product-market fit
- Series A funding
- Aggressive expansion

| Metric | Year 1 | Year 2 | Year 3 |
|--------|--------|--------|--------|
| Free Users | 5,000 | 30,000 | 100,000 |
| Starter ($49/mo) | 300 | 2,000 | 8,000 |
| Professional ($199/mo) | 100 | 750 | 3,000 |
| Enterprise ($5k/mo) | 5 | 30 | 100 |
| **MRR** | $59,450 | $396,250 | $1,489,000 |
| **ARR** | $713,400 | $4,755,000 | $17,868,000 |

### 8.4 Cost Structure (Year 1)

| Category | Monthly Cost | Notes |
|----------|--------------|-------|
| Infrastructure | $500-2,000 | Scales with users |
| Exchange Data | $0-1,000 | May need licenses |
| Domain/SSL/Services | $50 | Basic operational |
| Legal/Compliance | $500 (amortized) | Initial setup |
| Marketing | $500-2,000 | Content, ads |
| **Total** | $1,550-5,050 | |

**Break-even**: ~30-100 paying customers depending on tier mix.

---

## 9. Implementation Roadmap

### Phase 1: Foundation (Months 1-2)

**Goal**: Minimal viable commercial platform.

**Deliverables**:
- [ ] Public website with landing page
- [ ] Authentication system (API keys)
- [ ] WebSocket gateway (single symbol)
- [ ] Basic REST API
- [ ] Stripe integration for payments
- [ ] Documentation site

**Technical Stack**:
- Landing: Next.js or static site
- Auth: Auth0 or Clerk
- WebSocket: Rust (tokio-tungstenite)
- REST: Rust (Axum)
- Payments: Stripe
- Docs: Docusaurus or GitBook

**Infrastructure**:
- Fly.io or Railway for API services
- Existing Ingestor infrastructure for computation
- Redis Cloud for pub/sub

### Phase 2: Dashboard MVP (Months 3-4)

**Goal**: Visual product for non-API users.

**Deliverables**:
- [ ] Real-time feature dashboard
- [ ] Historical charts
- [ ] Regime visualization
- [ ] Alert configuration UI
- [ ] Account management

**Technical Stack**:
- Frontend: React + TailwindCSS
- Charts: Lightweight-charts or Recharts
- State: Zustand or Redux
- WebSocket client: Native browser WebSocket

### Phase 3: Scale and Polish (Months 5-6)

**Goal**: Production-ready platform.

**Deliverables**:
- [ ] Multi-symbol support (10+ symbols)
- [ ] Historical data API
- [ ] Usage analytics
- [ ] Rate limiting and abuse prevention
- [ ] Monitoring and alerting
- [ ] SOC 2 preparation

### Phase 4: Enterprise Features (Months 7-9)

**Goal**: Enterprise-ready offering.

**Deliverables**:
- [ ] Dedicated instances
- [ ] SLA monitoring
- [ ] Custom feature development pipeline
- [ ] Bulk data export
- [ ] SSO integration
- [ ] Enterprise contracts and invoicing

### Phase 5: Market Expansion (Months 10-12)

**Goal**: Broader market coverage.

**Deliverables**:
- [ ] Additional exchanges (OKX, Bybit, Coinbase)
- [ ] Spot + Futures markets
- [ ] More trading pairs (50+)
- [ ] Mobile app (optional)
- [ ] Partner integrations

---

## 10. Risk Analysis

### 10.1 Technical Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Exchange API changes | High | Medium | Abstraction layer, monitoring |
| Infrastructure failure | High | Low | Redundancy, failover |
| Latency issues | Medium | Medium | Edge deployment, optimization |
| Security breach | Critical | Low | SOC 2, penetration testing |

### 10.2 Business Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Low adoption | High | Medium | Marketing, free tier, content |
| Competitor entry | Medium | High | Move fast, differentiate |
| Exchange licensing | High | Medium | Legal review, focus on derived data |
| Regulatory action | High | Low | Legal structure, disclaimers |

### 10.3 Market Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Crypto winter | High | Medium | Diversify to traditional markets |
| Feature commoditization | Medium | High | Continuous innovation |
| Enterprise sales cycle | Medium | High | PLG model, self-serve |

---

## 11. Summary and Recommendations

### 11.1 Key Takeaways

1. **Viable commercial opportunity exists** in the gap between raw data and actionable intelligence.

2. **Hybrid model recommended**: Dashboard SaaS + API access, keeping trading edge private.

3. **Information-theoretic approach is differentiated** from on-chain focused competitors.

4. **Conservative path to profitability**: 30-100 paying customers for break-even.

5. **Technical foundation already exists** in Ingestor project - primarily needs API layer and frontend.

### 11.2 Recommended Next Steps

1. **Immediate**: Validate demand through conversations with potential customers
2. **Short-term**: Build landing page and waitlist to gauge interest
3. **Medium-term**: MVP with single symbol, basic dashboard, API access
4. **Long-term**: Scale based on customer feedback and demand

### 11.3 Critical Success Factors

- **Speed to market**: First mover advantage in entropy-based analytics
- **Developer experience**: Clean APIs, good documentation
- **Trust**: Academic foundation, transparent methodology
- **Stickiness**: Integration depth, feature innovation

### 11.4 What NOT to Do

- Don't expose proprietary trading strategies
- Don't make performance claims or investment advice
- Don't skip legal/regulatory review
- Don't over-engineer before validating demand
- Don't compete on raw data (commoditized)

---

## Appendix A: Technology Alternatives

### A.1 WebSocket Frameworks (Rust)

| Framework | Pros | Cons |
|-----------|------|------|
| tokio-tungstenite | Lightweight, fast | More manual work |
| axum + tower-ws | Integrated with Axum | Newer |
| actix-web | Battle-tested | Actor model complexity |

### A.2 Deployment Platforms

| Platform | Pros | Cons |
|----------|------|------|
| Fly.io | Global edge, Rust support | Newer, less enterprise |
| Railway | Easy setup | Limited regions |
| Cloud Run | Scales to zero | Cold starts |
| AWS ECS | Enterprise ready | Complexity |

### A.3 Frontend Frameworks

| Framework | Pros | Cons |
|-----------|------|------|
| Next.js | Full-stack, SSR | JavaScript ecosystem |
| SvelteKit | Performance | Smaller community |
| Leptos (Rust) | Same language as backend | Immature |

---

## Appendix B: Sample Customer Personas

### Persona 1: Quantitative Researcher

**Profile**: PhD in physics, works at crypto fund
**Needs**: API access, historical data, documentation
**Budget**: $200-500/month (company pays)
**Pain Points**: Building features from scratch, data quality

### Persona 2: Algorithmic Trader

**Profile**: Solo trader, technical background
**Needs**: Real-time features, low latency, reliability
**Budget**: $50-200/month
**Pain Points**: Time spent on infrastructure vs strategy

### Persona 3: Trading Educator

**Profile**: YouTube channel, 50k subscribers
**Needs**: Visual dashboard, embeddable widgets
**Budget**: $100-300/month
**Pain Points**: Explaining concepts without tools

### Persona 4: Crypto Fund Manager

**Profile**: $50M AUM fund
**Needs**: Enterprise SLA, custom features, bulk data
**Budget**: $2,000-10,000/month
**Pain Points**: Vendor reliability, data quality

---

## Appendix C: Glossary

| Term | Definition |
|------|------------|
| ARR | Annual Recurring Revenue |
| MRR | Monthly Recurring Revenue |
| FaaS | Feature-as-a-Service |
| KSG | Kraskov-Stögbauer-Grassberger MI estimator |
| MI | Mutual Information |
| MiFID II | Markets in Financial Instruments Directive (EU) |
| PLG | Product-Led Growth |
| SLA | Service Level Agreement |
| SOC 2 | Service Organization Control 2 (security standard) |

---

*Document generated: 2026-01-17*
*Status: Speculative analysis for internal planning*
*This document does not constitute a business commitment*
