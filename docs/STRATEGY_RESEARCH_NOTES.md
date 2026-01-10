# Strategy Research Notes

**Date:** 2025-01-09
**Status:** Ongoing Research
**Author:** Claude (AI Assistant) + Onat

---

## Executive Summary

This document captures strategic research discussions and conclusions about trading strategies for the Ingestor platform. It serves as institutional knowledge for future development decisions.

---

## 1. Current System Assessment

### 1.1 What We Have Built

| Component | Status | Description |
|-----------|--------|-------------|
| Feature Extraction | Complete | 60+ microstructure features from order book/trades |
| Tick Entropy | Complete | Regime detection via Shannon entropy |
| Market Making (Avellaneda-Stoikov) | Complete | Quote generation with spread/skew |
| Backtesting Infrastructure | Complete | Replay, harness, fill simulator, walk-forward |
| Momentum Trading (TSMOM) | Complete | Time-series momentum with circuit breakers |
| ML Spread/Skew | In Progress | Linear model for dynamic parameter adaptation |
| TUI | 68% Complete | Terminal UI with menu system |

### 1.2 Backtest Results Summary

**Market Making (Grid Search Best):**
- Spread: 1.0 bps
- Skew: 0.3
- High Entropy Threshold: 0.7
- Fill Probability: 10%
- Expected Return: +5.14% over 47 days
- Win Rate: 59.5%
- Trades: 452
- **Note:** All Sharpe ratios negative with realistic fill model

**Key Conclusions from Backtests:**
1. Only tight spreads (1 bps) are profitable
2. Entropy threshold has minimal effect (0.6/0.7/0.8 similar results)
3. Spread widening preferred over quote pulling in low entropy
4. Fill probability assumption (10%) dramatically affects results

---

## 2. Strategy Assessments

### 2.1 Strategy Ranking by Robustness

| Rank | Strategy | Robustness | Edge Type | Notes |
|------|----------|------------|-----------|-------|
| 1 | **Funding Rate Arbitrage** | High | Structural | Built into perp mechanics |
| 2 | **Cross-Exchange Arbitrage** | High | Inefficiency | Pure arbitrage |
| 3 | **Statistical Arbitrage** | Medium-High | Mean Reversion | Fundamental |
| 4 | **Market Making** | Medium | Adverse Selection | Requires edge in flow |
| 5 | **Momentum** | Medium-Low | Behavioral | Crowded, regime-dependent |
| 6 | **Technical Patterns** | Low | Behavioral | Mostly noise |

### 2.2 Momentum Trading Assessment

**Naive Momentum:**
- Crowded trade (alpha decay)
- Regime dependent (fails in chop)
- High transaction costs
- Adverse selection risk
- **Verdict:** Not viable alone

**Quality-Filtered Momentum (Recommended Approach):**
```
Signal = Positive Momentum + Low Entropy (orderly trend)

"Find assets going up AND doing so in orderly fashion"
```

**Why Quality-Filtered Momentum is Better:**
1. Low entropy = order flow agreement (buyers/sellers aligned)
2. Low entropy = lower reversal risk
3. Filters out noisy momentum likely to reverse
4. Cross-asset scanning exploits rotation

**Academic Support:**
- Momentum factor (documented)
- Low volatility anomaly (low entropy ≈ low vol)
- Quality factor (orderly price action = institutional flow)
- Cross-sectional momentum (scanning multiple symbols)

### 2.3 Funding Rate Arbitrage Assessment

**How It Works:**
```
LONG Spot BTC + SHORT Perp BTC = Delta Neutral
When funding > 0: Receive funding every 8 hours
Price moves cancel out (hedged)
```

**Why It Works:**
- Structural edge (perpetual mechanics, not behavioral)
- Retail bias keeps funding positive
- Predictable (funding rates known 8h in advance)
- Scalable (until you move the market)

**Realistic Returns:**
| Market Condition | Annualized Yield |
|------------------|------------------|
| Bull market | 15-40% APY |
| Sideways | 5-15% APY |
| Bear market | -5% to +10% |
| Extreme fear | Negative |

**Average expectation:** 10-20% APY in normal conditions

**Risks:**
- Funding flips negative (you pay instead)
- Liquidation on price spike (use low leverage)
- Exchange risk (spread across exchanges)
- Execution slippage
- Capital intensive

**Verdict:** Most reliable strategy in crypto. "Treasury bills of crypto."

---

## 3. Recommended System Architecture

### 3.1 Multi-Strategy Portfolio

```
┌─────────────────────────────────────────────────────────────────┐
│                    Capital Allocation                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│   │  Funding Arb    │  │  Quality Mom    │  │    Reserve      │ │
│   │     50%         │  │      30%        │  │      20%        │ │
│   │   (baseline)    │  │  (active alpha) │  │  (dry powder)   │ │
│   └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│                                                                  │
│   Funding Arb: Consistent base yield (10-20% APY)               │
│   Quality Mom: Alpha when conditions right                       │
│   Reserve: Opportunities + margin buffer                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Regime-Conditional Allocation

```
┌─────────────────────────────────────────────────────────────────┐
│              Regime-Conditional Allocator                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   TRENDING REGIME (low entropy, positive momentum):             │
│   → 60% momentum, 20% mean reversion, 20% funding arb           │
│                                                                  │
│   RANGING REGIME (high entropy, no clear direction):            │
│   → 20% momentum, 60% mean reversion, 20% funding arb           │
│                                                                  │
│   CRISIS REGIME (extreme volatility):                           │
│   → 0% momentum, 0% mean reversion, 100% cash/funding arb       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 Feature Streaming Product Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Feature Streaming Service                     │
├─────────────────────────────────────────────────────────────────┤
│  Binance ──┐                                                     │
│  Bybit ────┼──► Normalizer ──► Feature Engine ──► WebSocket API │
│  OKX ──────┤                        │                            │
│  Coinbase ─┘                        ▼                            │
│                              60+ Features/tick                   │
│                              - Microstructure                    │
│                              - Order flow                        │
│                              - Entropy/Regime                    │
└─────────────────────────────────────────────────────────────────┘
```

**Commercial Viability:**
- Target: Mid-tier quant shops who don't want to build infrastructure
- Value: Cross-exchange normalization + derived features
- Competition: Kaiko, Tardis, CryptoCompare (mostly raw data)
- Recommendation: Prove it works internally first, then commercialize

---

## 4. Quality-Filtered Momentum Implementation

### 4.1 Signal Construction

```rust
struct TrendQualitySignal {
    // Momentum component
    returns_1h: f64,
    returns_4h: f64,
    returns_24h: f64,

    // Quality component (low entropy)
    tick_entropy: f64,
    price_path_smoothness: f64,  // R² of linear regression
    volume_consistency: f64,      // Low variance in volume

    fn score(&self) -> f64 {
        let momentum = self.returns_4h;
        let quality = 1.0 - self.tick_entropy;  // Invert: low entropy = high quality

        if momentum > 0.0 && quality > THRESHOLD {
            momentum * quality
        } else {
            0.0
        }
    }
}
```

### 4.2 Multi-Asset Scanner Logic

```
Universe: Top 50 liquid tokens by volume

Every 15 minutes:
1. Calculate momentum score for each asset
2. Calculate entropy/quality score for each asset
3. Rank by combined score
4. Long top N assets with positive combined score
5. Exit when entropy spikes OR momentum reverses
```

### 4.3 Risk Controls

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max positions | 3-5 | Correlation reduces diversification |
| Position size | 5-10% each | Limit single-asset risk |
| Stop loss | -5% per position | Cut losers quickly |
| Entropy exit | > 0.7 | Exit when trend quality degrades |
| Momentum exit | Returns < 0 | Exit when trend reverses |
| Max drawdown | -15% portfolio | Circuit breaker |

---

## 5. Funding Arbitrage Implementation

### 5.1 Entry Process

```
1. Buy 1 BTC spot @ $40,000
2. Short 1 BTC perp @ $40,000 (1x leverage)
3. Total capital: ~$40,000 spot + ~$20,000 margin = $60,000

Ongoing:
- Funding rate: +0.01% every 8 hours (typical bull market)
- Daily yield: 0.03% × $40,000 = $12/day
- Annual yield: ~$4,380 (10.9% on $40k notional)
```

### 5.2 Risk Management

| Risk | Mitigation |
|------|------------|
| Funding flips negative | Monitor and exit when negative persists |
| Liquidation | Use 1-2x leverage, hold margin buffer |
| Exchange risk | Spread across 2-3 exchanges |
| Execution slippage | Use limit orders, enter during low vol |

### 5.3 Monitoring Signals

- Funding rate forecast (8h ahead)
- Basis (spot-perp spread)
- Open interest changes
- Liquidation data

---

## 6. Development Priorities

### 6.1 Near-Term (Next 4 Weeks)

| Priority | Task | Rationale |
|----------|------|-----------|
| 1 | Implement funding rate data ingestion | Needed for funding arb |
| 2 | Multi-asset data collection (top 20 alts) | Needed for quality momentum |
| 3 | Cross-asset momentum scanner | Core of quality momentum strategy |
| 4 | Funding arbitrage backtester | Validate funding arb strategy |

### 6.2 Medium-Term (4-12 Weeks)

| Priority | Task | Rationale |
|----------|------|-----------|
| 5 | TUI completion (TASKS_0_26.md) | Better UX for strategy management |
| 6 | Exchange adapters (Bybit, OKX) | Multi-exchange arbitrage |
| 7 | Paper trading for funding arb | Validate before live |
| 8 | Statistical arbitrage research | Diversify strategy mix |

### 6.3 Long-Term (3-6 Months)

| Priority | Task | Rationale |
|----------|------|-----------|
| 9 | Live trading integration | Go live with validated strategies |
| 10 | Feature streaming API | Commercialize infrastructure |
| 11 | Mean reversion strategy | Portfolio diversification |

---

## 7. Key Insights & Learnings

### 7.1 What We Learned

1. **Momentum alone is not enough** - Must filter by quality/regime
2. **Fill assumptions matter enormously** - 10% fill makes all MM strategies look bad
3. **Entropy is useful** - But for regime detection, not direct trading signal
4. **Structural edges > Behavioral edges** - Funding arb more reliable than momentum
5. **Multi-strategy is necessary** - No single strategy works in all regimes

### 7.2 What To Avoid

1. **Pure momentum strategies** - Too crowded, regime-dependent
2. **Unrealistic backtest assumptions** - Use conservative fill rates
3. **Single-asset focus** - Diversification across assets is key
4. **Ignoring transaction costs** - 10-20 bps round-trip is realistic
5. **Overfitting** - Walk-forward validation is essential

### 7.3 Questions for Future Research

1. Can we predict funding rate direction?
2. What's the optimal entropy threshold for momentum quality filter?
3. How correlated are alt momentum signals?
4. Can we use order flow imbalance to improve entry timing?
5. Is there alpha in cross-exchange price discrepancies?

---

## 8. Data Requirements

### 8.1 Current Data

| Data | Coverage | Location |
|------|----------|----------|
| BTCUSDT features | 47 days (Oct-Dec 2025) | ./data/features/ |
| ~73k events | ~97 Parquet files | Local storage |

### 8.2 Needed Data

| Data | Purpose | Priority |
|------|---------|----------|
| Multi-asset tick data (20+ alts) | Quality momentum | High |
| Funding rate history | Funding arb backtest | High |
| Open interest data | Sentiment indicator | Medium |
| Cross-exchange prices | Arbitrage | Medium |
| Liquidation data | Risk indicator | Low |

---

## 9. References

### 9.1 Academic Papers

- Avellaneda & Stoikov (2008) - Market making foundation
- Cont et al. (2014) - Price impact / queue position
- Moskowitz et al. (2012) - Time-series momentum
- Jegadeesh & Titman (1993) - Cross-sectional momentum
- Ang et al. (2006) - Low volatility anomaly

### 9.2 Project Documents

- `docs/REQUIREMENTS_TUI.md` - TUI requirements specification
- `docs/TASKS_0_26.md` - TUI implementation roadmap
- `docs/CLAUDE.md` - Project context for AI assistant
- `docs/REQUIREMENTS_V0.2.md` - V0.2 feature requirements

---

## Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-09 | Initial creation from strategy discussion |

---

*This document should be updated as research progresses and new insights emerge.*
