# Essential Reading: Momentum & Trend-Following Research

This document lists foundational academic papers and books for understanding the theoretical basis of the MARS (Momentum Adaptive Regime Strategy) approach.

---

## Foundational Papers (Must Read)

### 1. Returns to Buying Winners and Selling Losers (1993)
**Authors:** Narasimhan Jegadeesh, Sheridan Titman
**Publication:** Journal of Finance
**Key Finding:** Stocks that performed well in the past 3-12 months continue to outperform, yielding ~12% annual excess returns.
**Why It Matters:** The original momentum paper that started the field. Proves momentum is a persistent anomaly.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=227214) | [JSTOR](https://www.jstor.org/stable/2328882)

### 2. Time Series Momentum (2012)
**Authors:** Tobias J. Moskowitz, Yao Hua Ooi, Lasse Heje Pedersen
**Publication:** Journal of Financial Economics
**Key Finding:** Trend-following generates significant alpha across 58 futures markets over 25 years. Sharpe ratio ~1.0.
**Why It Matters:** Proves that looking at an asset's own past returns (time-series) works as well as cross-sectional comparison.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2089463) | [AQR](https://www.aqr.com/Insights/Research/Journal-Article/Time-Series-Momentum)

### 3. Value and Momentum Everywhere (2013)
**Authors:** Clifford S. Asness, Tobias J. Moskowitz, Lasse Heje Pedersen
**Publication:** Journal of Finance
**Key Finding:** Momentum works across equities, bonds, currencies, and commodities globally. Not country or asset-class specific.
**Why It Matters:** AQR founders prove momentum is universal, not a statistical artifact.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1363476) | [AQR](https://www.aqr.com/Insights/Research/Journal-Article/Value-and-Momentum-Everywhere)

### 4. Two Centuries of Trend Following (2014)
**Authors:** Y. Lempérière, C. Deremble, P. Seager, M. Potters, J.P. Bouchaud
**Publication:** Capital Fund Management Working Paper
**Key Finding:** Trend-following has worked consistently since 1800 across all major asset classes.
**Why It Matters:** 200 years of out-of-sample evidence. Not a recent phenomenon.
**Link:** [arXiv](https://arxiv.org/abs/1404.3274) | [CFM](https://www.cfm.fr/research/)

### 5. High-Frequency Trading in a Limit Order Book (2008)
**Authors:** Marco Avellaneda, Sasha Stoikov
**Publication:** Quantitative Finance
**Key Finding:** Optimal market making strategy that balances inventory risk with spread capture.
**Why It Matters:** Foundation of our execution layer. The A-S algorithm we use for order placement.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1096410)

---

## Cryptocurrency-Specific Research

### 6. Common Risk Factors in Cryptocurrency (2019)
**Authors:** Yukun Liu, Aleh Tsyvinski, Xi Wu
**Publication:** Yale Working Paper / Review of Financial Studies
**Key Finding:** Momentum factor generates 17% monthly returns in crypto (much higher than equities).
**Why It Matters:** Academic proof that momentum is stronger in crypto markets.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3379131)

### 7. Technical Trading and Cryptocurrencies (2020)
**Authors:** Dirk G. Baur, Lai T. Hoang
**Publication:** Annals of Operations Research
**Key Finding:** Simple moving average strategies generate significant alpha in Bitcoin.
**Why It Matters:** Confirms technical/trend signals work in crypto.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3115846)

---

## Risk Management & Implementation

### 8. Momentum Crashes (2016)
**Authors:** Kent Daniel, Tobias J. Moskowitz
**Publication:** Journal of Financial Economics
**Key Finding:** Momentum strategies occasionally crash violently (2009: -73% in 2 months). Crashes are predictable by market volatility.
**Why It Matters:** Critical for risk management. Explains why OCO stop-losses are essential.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2371227)

### 9. Optimal Execution of Portfolio Transactions (2001)
**Authors:** Robert Almgren, Neil Chriss
**Publication:** Journal of Risk
**Key Finding:** Mathematical framework for executing large orders with minimal market impact.
**Why It Matters:** Foundation for understanding execution costs and slippage.
**Link:** [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1293721)

---

## Practical Implementation Books

### 10. Following the Trend (2013)
**Author:** Andreas F. Clenow
**Publisher:** Wiley
**Key Finding:** Step-by-step guide to implementing CTA-style trend-following strategies.
**Why It Matters:** Practical, code-oriented approach. Explains position sizing, risk management, diversification.
**Link:** [Amazon](https://www.amazon.com/Following-Trend-Diversified-Managed-Futures/dp/1118410858)

---

## Additional Recommended Reading

### Signal Processing & Regime Detection

| Paper | Authors | Year | Topic |
|-------|---------|------|-------|
| "Detecting Regime Changes in the S&P 500" | Bulla, Mergner | 2008 | Hidden Markov Models for regime detection |
| "Wavelet Analysis of Financial Time Series" | Gençay, Selçuk, Whitcher | 2002 | Wavelet decomposition for trend extraction |

### Market Microstructure

| Paper | Authors | Year | Topic |
|-------|---------|------|-------|
| "Price Impact and Optimal Execution" | Cont, Kukanov, Stoikov | 2014 | Queue position dynamics |
| "The Market Microstructure of Central Bank Intervention" | Dominguez | 2003 | How large orders move markets |

---

## Key Takeaways

1. **Momentum is real**: 30+ years of academic evidence across all asset classes
2. **It's stronger in crypto**: 10-20% annual alpha vs 4-8% in equities
3. **But it crashes**: Risk management (OCO, stop-losses) is essential
4. **Execution matters**: A-S helps minimize slippage on entry/exit
5. **Cross-asset comparison works**: Ranking symbols by momentum improves signal quality

---

## Where to Access Papers

- **SSRN** (ssrn.com): Free preprints of most finance papers
- **Google Scholar** (scholar.google.com): Search and find PDFs
- **AQR Insights** (aqr.com/insights): Free access to AQR's published research
- **arXiv** (arxiv.org): Free physics/quant finance papers
- **Sci-Hub**: For papers behind paywalls (legally grey area)

---

*Document created: December 13, 2025*
*Last updated: December 13, 2025*
