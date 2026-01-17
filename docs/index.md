# Ingestor Documentation Index

**Project:** Information-Guided Adaptive Trading System
**Last Updated:** 2026-01-17

---

## Overview

This documentation suite defines the architecture, implementation, and commercialization strategy for an entropy-based market microstructure analytics platform.

---

## Document Suite

### Core Technical Documents

| Document | Version | Description |
|----------|---------|-------------|
| [EXTENDED_REQUIREMENTS_0.md](./EXTENDED_REQUIREMENTS_0.md) | 0.2 | **Core System Design** - Foundation architecture, feature specifications, KSG mutual information framework, and Avellaneda-Stoikov market making integration |
| [EXTENDED_REQUIREMENTS_1.md](./EXTENDED_REQUIREMENTS_1.md) | 1.0 | **ML Framework & Validation** - Empirical validation protocols, HMM as testable hypothesis, walk-forward validation, 30 academic references, 5-milestone implementation roadmap |
| [EXTENDED_REQUIREMENTS_2.md](./EXTENDED_REQUIREMENTS_2.md) | 2.0 | **Hyperliquid Expansion Strategy** - Platform-specific features, on-chain transparency advantages, technical integration, 12-week launch roadmap, go-to-market strategy |

### Business Documents

| Document | Version | Description |
|----------|---------|-------------|
| [SPECULATION_0.md](./SPECULATION_0.md) | 0.1 | **Commercialization Analysis** - Business models, pricing tiers, API specifications, competitive landscape, revenue projections, risk analysis |

---

## Quick Reference

### What We're Building

```
Binance/Hyperliquid → Order Book/Trades → Feature Engines → Analytics API
                                              │
                                              ├── Entropy Metrics
                                              ├── Order Flow Analysis
                                              ├── Regime Detection
                                              └── Hyperliquid-Exclusive Features
```

### Key Differentiators

1. **Entropy-based regime detection** - Information-theoretic approach rare in commercial products
2. **On-chain transparency** (Hyperliquid) - Features impossible on centralized exchanges
3. **Integrated validation** - Walk-forward backtesting included
4. **Academic foundation** - 30+ referenced papers

### Target Metrics

| Metric | Target | Timeline |
|--------|--------|----------|
| Launch | Public API + Dashboard | 12 weeks |
| Sustainability | $40-50k MRR | 9 months |
| Team | 3-5 engineers | 18-24 months |

---

## Reading Order

**For Technical Understanding:**
1. EXTENDED_REQUIREMENTS_0.md (core concepts)
2. EXTENDED_REQUIREMENTS_1.md (ML and validation)
3. EXTENDED_REQUIREMENTS_2.md (Hyperliquid specifics)

**For Business Understanding:**
1. SPECULATION_0.md (commercialization)
2. EXTENDED_REQUIREMENTS_2.md (Section 8-9: GTM and revenue)

**For Implementation:**
1. EXTENDED_REQUIREMENTS_2.md (Section 10: Roadmap)
2. EXTENDED_REQUIREMENTS_1.md (Section 13: Milestones)

---

## Document Relationships

```
EXTENDED_REQUIREMENTS_0.md
         │
         │ builds upon
         ▼
EXTENDED_REQUIREMENTS_1.md
         │
         │ extends to
         ▼
EXTENDED_REQUIREMENTS_2.md ◄──── SPECULATION_0.md
    (Hyperliquid focus)          (Business model)
```

---

## Status Summary

| Area | Status | Next Action |
|------|--------|-------------|
| Core Features | ✅ Implemented | Port to Hyperliquid |
| Backtesting | ✅ Implemented | Validate on HL data |
| API Layer | ❌ Not started | Week 5-8 priority |
| Hyperliquid Integration | ❌ Not started | Week 1-4 priority |
| Dashboard | ❌ Not started | Month 4 |
| Commercialization | 📋 Planned | After API launch |

---

## Related Files

- [CLAUDE.md](./CLAUDE.md) - Project context for AI assistants
- [README.md](../README.md) - Project overview and setup

---

*This index provides navigation for the Ingestor documentation suite. Each document is self-contained but references others where appropriate.*
