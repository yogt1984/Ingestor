# Mutual Information-Based Regime Detection via Hypervolume Learning

**Document Type:** Research & Implementation Guide
**Version:** 1.0
**Date:** 2026-01-14
**Status:** Conceptual Design & Feasibility Analysis

---

## Executive Summary

This document explores a novel approach to regime detection using **mutual information (MI) between entropy features and monotonic price actions**, identifying regimes as **hypervolumes in n-dimensional Hilbert space**. This is a sophisticated information-theoretic approach that continuously learns regime boundaries rather than using fixed thresholds.

**Key Innovation:** Measure I(X; Y) where X = entropy features, Y = forward price monotonicity, then identify regions in feature space (hypervolumes) where MI is maximized.

---

## Table of Contents

1. [Concept Overview](#concept-overview)
2. [Originality and Validity Assessment](#originality-and-validity)
3. [Feasibility Analysis](#feasibility-and-implementation)
4. [Mathematical Foundation](#mathematical-foundation)
5. [Implementation Roadmap](#implementation-roadmap)
6. [Complete System Architecture](#system-architecture)
7. [Code Examples](#code-examples)
8. [Tools and Libraries](#tools-and-libraries)
9. [Academic References](#academic-references)
10. [Risk Assessment and Mitigation](#risk-assessment)

---

## Executive Summary

This document describes a **novel machine learning approach** for regime detection in algorithmic trading using:

1. **Mutual Information (MI)** between entropy features and price monotonicity
2. **Hypervolume identification** in high-dimensional feature space
3. **Continuous learning** to adapt to market non-stationarity

**Key Innovation:** Instead of fixed thresholds or HMMs, use information theory to identify regions (hypervolumes) in entropy feature space that maximize mutual information with future price monotonicity.

**Theoretical Foundation:** Information geometry + online learning + regime detection

**Practical Application:** Adaptive regime classification for the MARS trading system

---

## 1. Originality and Validity Assessment

### **Originality: 8/10 - Highly Original**

**What makes this novel:**

1. **Information-theoretic regime definition:** Most regime detection uses statistical models (HMM, variance switching) or simple thresholds. Using **mutual information between entropy features and price monotonicity** is a fundamentally different approach - you're measuring "how much knowing entropy features reduces uncertainty about price behavior."

2. **Hypervolume representation:** Treating regimes as **continuous regions in feature space** (hypervolumes) rather than discrete states or simple threshold boundaries is geometrically sophisticated.

3. **Continuous learning:** Most regime detectors are static. This proposal for **online MI computation and adaptive hypervolume updates** addresses the non-stationarity problem that kills most trading models.

4. **Meta-level information theory:** Using entropy features (already information-theoretic) and then applying MI (also information-theoretic) creates a "meta-information-theoretic" framework. You're measuring "information about information."

**Where similar ideas exist:**
- MI for feature selection in ML (common)
- Transfer entropy in finance (Schreiber 2000)
- Regime-switching models in econometrics
- But **NOT** this specific combination for online regime learning

**Academic precedent:**
- Closest: Diks & Panchenko (2006) - "A new statistic and practical guidelines for nonparametric Granger causality testing"
- Also: Dionisio et al. (2004) - "Mutual information: a measure of dependency for nonlinear time series"

**Your innovation:** Applying MI continuously to entropy feature → price action mapping for hypervolume regime identification in trading.

---

### **Validity: 9/10 - Theoretically Sound**

**Why this is valid:**

#### ✅ **1. Mutual Information is the Right Tool**

Mutual information captures **non-linear dependencies** that correlation misses:

```
I(X;Y) = H(X) + H(Y) - H(X,Y)

Where:
X = entropy features (sample_entropy, permutation_entropy, ...)
Y = price monotonicity (1 if monotonic up, 0 if not)
```

**Why MI > correlation:**
- Correlation: "Are X and Y linearly related?"
- MI: "How much does knowing X reduce uncertainty about Y?"

Example where MI wins:
```
X = sample_entropy = 0.3
Y = monotonic uptrend = TRUE

X = sample_entropy = 0.3
Y = monotonic downtrend = TRUE

Correlation: ~0 (both trends have same entropy)
MI: HIGH (knowing entropy = 0.3 tells you there WILL be a trend)
```

#### ✅ **2. Hypervolumes Capture Complex Regime Boundaries**

Regimes probably don't have simple boundaries like "sample_entropy < 0.5 AND permutation_entropy < 0.6".

Real boundary might be:
```
Trend Regime = {
  (sample_entropy ∈ [0.2, 0.5] AND permutation_entropy ∈ [0.3, 0.7])
  OR
  (sample_entropy ∈ [0.5, 0.8] AND kl_divergence < 0.3 AND orderbook_entropy > 0.8)
}
```

This is a **complex hypervolume** that simple thresholds can't capture.

#### ✅ **3. Continuous Learning Addresses Non-Stationarity**

Financial markets are non-stationary. A regime hypervolume that worked in Q1 2024 may not work in Q2 2024.

Continuous MI computation → adaptive hypervolumes → robust to regime drift.

#### ✅ **4. Theoretical Foundation: Hilbert Space Geometry + Information Theory**

You're essentially doing:
- **Feature embedding:** Map market states into n-dimensional Hilbert space
- **Information geometry:** Use MI to define "informative regions"
- **Regime manifolds:** Regimes are submanifolds (hypervolumes) in this space

This is theoretically grounded in:
- Amari (1998) - "Information geometry"
- Cover & Thomas (2006) - "Elements of Information Theory"

---

### **Potential Issues: What Could Go Wrong**

#### ⚠️ **1. Curse of Dimensionality**

With 10-20 entropy features, you're in high-dimensional space.

**Problem:** MI estimation requires exponentially more data as dimensions increase.

**Rule of thumb:** Need ~10^d samples for d dimensions (Kraskov et al. 2004)
- 5 features: Need ~100k samples ✅ (feasible)
- 10 features: Need ~10M samples ⚠️ (borderline)
- 20 features: Need ~100M samples ❌ (infeasible)

**Solution:** Dimensionality reduction (PCA, UMAP, or feature selection)

#### ⚠️ **2. Definition of "Monotonic Price Action"**

What exactly is Y (the target)?

**Option A: Binary monotonicity**
```
Y = 1 if next N ticks are monotonic (all up or all down)
Y = 0 otherwise
```

**Option B: Continuous monotonicity**
```
Y = percentage of ticks in dominant direction
Y ∈ [0.5, 1.0]
```

**Option C: Multi-class**
```
Y ∈ {strong_trend, weak_trend, mean_revert, avoid}
```

**Recommendation:** Start with Option B (continuous), more information.

#### ⚠️ **3. Computational Cost**

MI estimation is expensive:
- Naive binning: O(N²) for pairwise distances
- KSG estimator: O(N log N) with k-d trees
- Neural estimation (MINE): O(N × epochs)

For 10Hz data (100ms updates), this is challenging in real-time.

**Solution:** Compute MI asynchronously (every 1-10 seconds, not every tick)

---

## 2. Feasibility and Implementation Roadmap

### **Phase 1: Mutual Information Computation**

#### **Step 1.1: Define the MI Problem**

```python
# Inputs
X = entropy_features  # Shape: (n_samples, n_features)
                      # Example: 10,000 samples × 10 entropy features

Y = price_monotonicity  # Shape: (n_samples,)
                        # Value: ∈ [0.5, 1.0] (monotonicity percentage)

# Goal: Compute I(X; Y) and identify which regions of X maximize MI
```

#### **Step 1.2: Choose MI Estimator**

**Option A: KSG Estimator (Recommended for start)**

The **Kraskov-Stögbauer-Grassberger (2004)** estimator is the gold standard for continuous variables.

**Advantages:**
- Non-parametric (no assumptions about distributions)
- Works in moderate dimensions (5-10 features)
- Well-tested, many implementations

**Python implementation:**
```python
from sklearn.feature_selection import mutual_info_regression
import numpy as np

# Your entropy features
X = np.array([
    [sample_entropy, perm_entropy, ob_entropy, kl_div, ...],
    [...],
    ...
])  # Shape: (n_samples, n_features)

# Your target: forward monotonicity
# For each sample, compute monotonicity of next N ticks
Y = compute_forward_monotonicity(prices, window=20)  # Shape: (n_samples,)

# Compute MI for each feature
mi_scores = mutual_info_regression(X, Y, n_neighbors=5, random_state=42)

print("MI scores:")
for i, score in enumerate(mi_scores):
    print(f"  Feature {i}: {score:.4f} bits")
```

**Tool:** `sklearn.feature_selection.mutual_info_regression`

**Complexity:** O(N log N) with k-d trees

---

**Option B: MINE (Mutual Information Neural Estimation)**

For **high-dimensional** or **online learning**, use neural estimation.

**Advantages:**
- Scales to high dimensions
- Can be trained incrementally
- Captures complex dependencies

**Python implementation:**
```python
import torch
import torch.nn as nn

class MINENetwork(nn.Module):
    """Neural network for MI estimation"""
    def __init__(self, x_dim, y_dim, hidden_dim=128):
        super().__init__()
        self.fc1 = nn.Linear(x_dim + y_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, 1)

    def forward(self, x, y):
        xy = torch.cat([x, y], dim=1)
        h = torch.relu(self.fc1(xy))
        h = torch.relu(self.fc2(h))
        return self.fc3(h)

def mine_loss(joint_pred, marginal_pred):
    """Donsker-Varadhan representation of KL divergence"""
    return -(torch.mean(joint_pred) - torch.log(torch.mean(torch.exp(marginal_pred))))

# Training loop
mine_net = MINENetwork(x_dim=10, y_dim=1)
optimizer = torch.optim.Adam(mine_net.parameters(), lr=1e-3)

for epoch in range(100):
    # Sample joint distribution: (X_i, Y_i) pairs
    x_joint = torch.FloatTensor(X)
    y_joint = torch.FloatTensor(Y).unsqueeze(1)

    # Sample marginal: X_i paired with random Y_j
    y_marginal = y_joint[torch.randperm(len(y_joint))]

    # Compute statistics
    joint_pred = mine_net(x_joint, y_joint)
    marginal_pred = mine_net(x_joint, y_marginal)

    # MI estimate (lower bound)
    loss = mine_loss(joint_pred, marginal_pred)

    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    mi_estimate = -loss.item()
    print(f"Epoch {epoch}: MI = {mi_estimate:.4f} bits")
```

**Paper:** Belghazi et al. (2018) - "Mutual Information Neural Estimation" (ICML)

**Tool:** Custom PyTorch implementation (no standard library)

**Advantage:** Can run online, continuously updating MI estimates

---

#### **Step 1.3: Feature-Wise MI Ranking**

Compute MI for each feature individually to understand which entropy measures are most informative:

```python
import pandas as pd

feature_names = [
    'sample_entropy_30s', 'sample_entropy_1m', 'sample_entropy_5m',
    'perm_entropy_10s', 'perm_entropy_1m', 'perm_entropy_5m',
    'orderbook_entropy', 'ob_entropy_asymmetry',
    'kl_div_tick_1m', 'kl_div_volatility_1m',
]

# Compute MI for each feature
mi_df = pd.DataFrame({
    'feature': feature_names,
    'mi_score': mutual_info_regression(X, Y, n_neighbors=5)
})

mi_df = mi_df.sort_values('mi_score', ascending=False)
print(mi_df)

# Output example:
#                    feature  mi_score
# 0    sample_entropy_1m      0.245
# 8      kl_div_tick_1m      0.198
# 1   sample_entropy_30s      0.187
# 6   orderbook_entropy       0.156
# ...
```

**Use case:** Feature selection - drop features with MI < threshold (e.g., 0.05)

---

### **Phase 2: Hypervolume Identification**

Now that you have MI scores, identify **which regions of feature space** have high MI.

#### **Approach A: Decision Trees (Interpretable Hyperrectangles)**

Decision trees naturally partition feature space into **axis-aligned hyperrectangles**.

```python
from sklearn.tree import DecisionTreeRegressor
from sklearn.tree import plot_tree
import matplotlib.pyplot as plt

# Train decision tree to predict monotonicity from entropy features
tree = DecisionTreeRegressor(
    max_depth=5,           # Control complexity
    min_samples_split=100, # Ensure statistical significance
    min_samples_leaf=50
)

tree.fit(X, Y)

# Visualize tree
plt.figure(figsize=(20, 10))
plot_tree(tree, feature_names=feature_names, filled=True, fontsize=10)
plt.savefig('regime_tree.png')

# Extract hypervolume rules
from sklearn.tree import _tree

def extract_rules(tree, feature_names):
    tree_ = tree.tree_
    feature_name = [
        feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined!"
        for i in tree_.feature
    ]

    def recurse(node, depth, rules):
        indent = "  " * depth
        if tree_.feature[node] != _tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            print(f"{indent}if {name} <= {threshold:.3f}:")
            recurse(tree_.children_left[node], depth + 1, rules)
            print(f"{indent}else:  # {name} > {threshold:.3f}")
            recurse(tree_.children_right[node], depth + 1, rules)
        else:
            print(f"{indent}return {tree_.value[node][0][0]:.3f} (monotonicity)")

    recurse(0, 0, [])

extract_rules(tree, feature_names)
```

**Example output:**
```
if sample_entropy_1m <= 0.450:
  if kl_div_tick_1m <= 0.300:
    if orderbook_entropy > 0.700:
      return 0.85 (HIGH monotonicity - TREND regime)
    else:
      return 0.65 (moderate monotonicity)
  else:  # kl_div_tick_1m > 0.300
    return 0.52 (LOW monotonicity - AVOID regime)
else:  # sample_entropy_1m > 0.450
  if perm_entropy_1m <= 0.700:
    return 0.68 (moderate monotonicity)
  else:
    return 0.55 (LOW monotonicity - REVERT regime)
```

**Advantages:**
- ✅ Interpretable (you can see the rules)
- ✅ Fast to evaluate
- ✅ Naturally creates hypervolumes (hyperrectangles)
- ✅ Feature importance built-in

**Disadvantages:**
- ❌ Only axis-aligned splits (can't capture diagonal boundaries)
- ❌ Prone to overfitting (needs regularization)

**Tools:**
- `sklearn.tree.DecisionTreeRegressor`
- `xgboost.XGBRegressor` (gradient boosting for better accuracy)

---

#### **Approach B: Random Forests (Ensemble Hypervolumes)**

Random forests create an **ensemble of hyperrectangles** and average predictions.

```python
from sklearn.ensemble import RandomForestRegressor

# Train random forest
rf = RandomForestRegressor(
    n_estimators=100,
    max_depth=8,
    min_samples_split=50,
    min_samples_leaf=25,
    n_jobs=-1
)

rf.fit(X, Y)

# Feature importance (based on MI implicitly)
importances = rf.feature_importances_
for name, importance in zip(feature_names, importances):
    print(f"{name}: {importance:.4f}")

# Predict monotonicity for new samples
monotonicity_pred = rf.predict(X_test)

# Find high-MI regions by analyzing predictions
high_monotonicity_mask = monotonicity_pred > 0.80
print(f"Found {high_monotonicity_mask.sum()} samples in TREND regime")
```

**Advantages:**
- ✅ More robust than single tree
- ✅ Better generalization
- ✅ Still somewhat interpretable (feature importance)

**Disadvantages:**
- ❌ Less interpretable than single tree
- ❌ Slower to evaluate

---

#### **Approach C: Gaussian Mixture Models (Probabilistic Hypervolumes)**

GMMs model feature space as a mixture of Gaussian distributions - each Gaussian is a **soft hypervolume**.

```python
from sklearn.mixture import GaussianMixture
import numpy as np

# Fit GMM to entropy features
gmm = GaussianMixture(
    n_components=3,  # Assume 3 regimes: TREND, REVERT, AVOID
    covariance_type='full',  # Allow arbitrary hyperellipsoids
    random_state=42
)

gmm.fit(X)

# Predict regime for each sample
regime_labels = gmm.predict(X)

# Compute mean monotonicity for each regime
for regime in range(3):
    regime_mask = regime_labels == regime
    mean_monotonicity = Y[regime_mask].mean()
    print(f"Regime {regime}: mean monotonicity = {mean_monotonicity:.3f}")

# Output:
# Regime 0: mean monotonicity = 0.85 → TREND
# Regime 1: mean monotonicity = 0.62 → REVERT
# Regime 2: mean monotonicity = 0.53 → AVOID

# Extract hypervolume parameters
for i in range(3):
    mean = gmm.means_[i]
    covariance = gmm.covariances_[i]
    print(f"\nRegime {i} hypervolume:")
    print(f"  Center: {mean}")
    print(f"  Covariance:\n{covariance}")
```

**Advantages:**
- ✅ Captures non-axis-aligned boundaries (hyperellipsoids)
- ✅ Probabilistic (gives confidence scores)
- ✅ Fewer parameters than trees

**Disadvantages:**
- ❌ Assumes Gaussian distributions (may not hold)
- ❌ Requires choosing number of components

**Tools:** `sklearn.mixture.GaussianMixture`

---

#### **Approach D: Support Vector Machines (Maximum Margin Hypervolumes)**

SVMs with RBF kernel can create **arbitrary non-linear boundaries**.

```python
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler

# Standardize features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Train SVM regressor
svm = SVR(
    kernel='rbf',  # Radial basis function - captures non-linear boundaries
    C=1.0,         # Regularization
    gamma='scale'  # Kernel coefficient
)

svm.fit(X_scaled, Y)

# Predict monotonicity
Y_pred = svm.predict(X_scaled)

# Find decision boundary (where monotonicity = threshold)
threshold = 0.70
trend_regime_mask = Y_pred >= threshold
```

**Advantages:**
- ✅ Captures complex non-linear boundaries
- ✅ Maximum margin principle (robust)

**Disadvantages:**
- ❌ Not interpretable (black box)
- ❌ Slow for large datasets

---

#### **Approach E: HDBSCAN (Density-Based Hypervolume Discovery)**

HDBSCAN finds **dense regions** in feature space automatically.

```python
import hdbscan

# Cluster entropy feature space
clusterer = hdbscan.HDBSCAN(
    min_cluster_size=100,
    min_samples=50,
    metric='euclidean'
)

cluster_labels = clusterer.fit_predict(X)

# Compute monotonicity for each cluster
unique_clusters = np.unique(cluster_labels[cluster_labels >= 0])
for cluster_id in unique_clusters:
    cluster_mask = cluster_labels == cluster_id
    mean_monotonicity = Y[cluster_mask].mean()
    cluster_size = cluster_mask.sum()
    print(f"Cluster {cluster_id}: n={cluster_size}, monotonicity={mean_monotonicity:.3f}")
```

**Advantages:**
- ✅ No need to specify number of clusters
- ✅ Handles arbitrary shapes (not just hyperellipsoids)
- ✅ Identifies outliers automatically

**Disadvantages:**
- ❌ Sensitive to hyperparameters
- ❌ Can't easily predict regime for new samples (need nearest neighbor search)

**Tools:** `hdbscan` library

---

### **Phase 3: Continuous Learning (Online Adaptation)**

#### **Challenge: Non-Stationarity**

Financial markets change. A hypervolume that worked in January may fail in February.

**Solution:** Continuously update MI estimates and hypervolumes.

#### **Approach A: Sliding Window Retraining**

```python
import time
from collections import deque

# Ring buffer for feature history
window_size = 10000
X_buffer = deque(maxlen=window_size)
Y_buffer = deque(maxlen=window_size)

# Initialize model
model = RandomForestRegressor(n_estimators=100, max_depth=8)

while True:  # Continuous loop
    # Collect new data
    new_features = get_latest_entropy_features()
    new_monotonicity = compute_forward_monotonicity(prices, window=20)

    X_buffer.append(new_features)
    Y_buffer.append(new_monotonicity)

    # Retrain every N samples
    if len(X_buffer) >= window_size and len(X_buffer) % 1000 == 0:
        X_train = np.array(X_buffer)
        Y_train = np.array(Y_buffer)

        # Compute MI
        mi_scores = mutual_info_regression(X_train, Y_train)
        print(f"MI scores updated: {mi_scores}")

        # Retrain model
        model.fit(X_train, Y_train)
        print("Model retrained")

    # Predict regime
    current_monotonicity = model.predict([new_features])[0]
    if current_monotonicity > 0.75:
        regime = "TREND"
    elif current_monotonicity > 0.60:
        regime = "REVERT"
    else:
        regime = "AVOID"

    print(f"Current regime: {regime} (monotonicity={current_monotonicity:.3f})")

    time.sleep(10)  # Update every 10 seconds
```

**Frequency:** Retrain every 1000-10000 samples (1-10 minutes at 10Hz data)

---

#### **Approach B: Incremental Learning (Online Algorithms)**

Some algorithms support **online updates** without full retraining.

```python
from river import tree, ensemble, metrics

# Online random forest (using river library)
model = ensemble.AdaptiveRandomForestRegressor(
    n_models=10,
    max_depth=8,
    leaf_prediction='adaptive'
)

# Metric tracking
mae = metrics.MAE()

# Continuous learning loop
for features, target in stream_data():  # Infinite stream
    # Predict
    y_pred = model.predict_one(features)

    # Learn from this sample
    model.learn_one(features, target)

    # Update metric
    mae.update(target, y_pred)

    # Determine regime
    if y_pred > 0.75:
        regime = "TREND"
    elif y_pred > 0.60:
        regime = "REVERT"
    else:
        regime = "AVOID"

    print(f"Regime: {regime}, MAE: {mae.get():.4f}")
```

**Advantages:**
- ✅ No need to store history (constant memory)
- ✅ Adapts immediately to new data
- ✅ Low latency

**Tools:**
- `river` - Online machine learning library
- `scikit-multiflow` - Another online ML library

---

#### **Approach C: Drift Detection**

Detect when MI distribution changes significantly → trigger retraining.

```python
from scipy.stats import ks_2samp

# Store historical MI scores
historical_mi = []
current_mi = compute_mi(X_recent, Y_recent)

# Kolmogorov-Smirnov test for distribution change
if len(historical_mi) > 100:
    statistic, p_value = ks_2samp(historical_mi, current_mi)

    if p_value < 0.05:  # Significant change detected
        print("Drift detected! Retraining model...")
        model = retrain_model(X_recent, Y_recent)
        historical_mi = []  # Reset baseline
    else:
        historical_mi.append(current_mi)
```

**Tools:**
- `scipy.stats.ks_2samp` - Kolmogorov-Smirnov test
- `alibi-detect` - Drift detection library

---

### **Phase 4: Dimensionality Reduction (Critical for High-D)**

With 10-20 entropy features, curse of dimensionality becomes severe.

#### **Approach A: PCA (Linear Projection)**

```python
from sklearn.decomposition import PCA

# Reduce from 10 features to 3-5 principal components
pca = PCA(n_components=5)
X_reduced = pca.fit_transform(X)

# Explained variance
print(f"Explained variance: {pca.explained_variance_ratio_.sum():.2%}")

# Use reduced features for MI computation and hypervolume identification
mi_scores = mutual_info_regression(X_reduced, Y)
```

**Advantages:**
- ✅ Fast, well-established
- ✅ Reduces noise

**Disadvantages:**
- ❌ Linear only (may miss non-linear structure)
- ❌ Loses interpretability

---

#### **Approach B: UMAP (Non-Linear Projection)**

```python
import umap

# Reduce to 2-3 dimensions for visualization and MI computation
reducer = umap.UMAP(n_components=3, n_neighbors=15, min_dist=0.1)
X_reduced = reducer.fit_transform(X)

# Visualize in 3D
import plotly.graph_objects as go

fig = go.Figure(data=[go.Scatter3d(
    x=X_reduced[:, 0],
    y=X_reduced[:, 1],
    z=X_reduced[:, 2],
    mode='markers',
    marker=dict(
        size=3,
        color=Y,  # Color by monotonicity
        colorscale='Viridis',
        showscale=True
    )
)])

fig.update_layout(title='Entropy Feature Space (UMAP)')
fig.show()
```

**Advantages:**
- ✅ Preserves non-linear structure
- ✅ Better for visualization
- ✅ Often better clustering

---

#### **Approach C: Feature Selection (Keep Top-k by MI)**

```python
from sklearn.feature_selection import SelectKBest, mutual_info_regression

# Select top 5 features by MI
selector = SelectKBest(mutual_info_regression, k=5)
X_selected = selector.fit_transform(X, Y)

# Get selected feature names
selected_mask = selector.get_support()
selected_features = [name for name, selected in zip(feature_names, selected_mask) if selected]
print(f"Selected features: {selected_features}")
```

**Advantages:**
- ✅ Keeps interpretability
- ✅ Fast

---

## 3. Complete System Architecture

Here's how all pieces fit together:

```
┌─────────────────────────────────────────────────────────────┐
│                    CONTINUOUS DATA STREAM                    │
│            (Orderbook updates, Trades @ 10Hz)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              ENTROPY FEATURE EXTRACTION                      │
│  • Sample Entropy (30s, 1m, 5m)                             │
│  • Permutation Entropy (10s, 1m, 5m)                        │
│  • Order Book Entropy                                       │
│  • KL Divergence (1m, 5m)                                   │
│  → Output: X_t ∈ ℝ^10 (10-dimensional feature vector)       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           FORWARD MONOTONICITY COMPUTATION                   │
│  • For each time t, compute monotonicity of next N ticks     │
│  • Y_t = % of next 20 ticks in dominant direction           │
│  → Output: Y_t ∈ [0.5, 1.0]                                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│             ROLLING BUFFER (10,000 samples)                  │
│  Store (X_t, Y_t) pairs for MI computation and training     │
└──────────────────────┬──────────────────────────────────────┘
                       │
           ┌───────────┴───────────┐
           │                       │
           ▼                       ▼
┌──────────────────┐    ┌──────────────────────┐
│  MUTUAL INFO     │    │  HYPERVOLUME MODEL   │
│  COMPUTATION     │    │  (Random Forest /    │
│                  │    │   GMM / SVM)         │
│  Every 1000      │    │                      │
│  samples:        │    │  Predicts:           │
│  • Compute       │    │  monotonicity_pred   │
│    I(X;Y)        │    │  = f(X_t)           │
│  • Rank features │    │                      │
│  • Detect drift  │    │  Every 1000 samples: │
│                  │    │  • Retrain on buffer │
└────────┬─────────┘    └──────────┬───────────┘
         │                         │
         └───────────┬─────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │  REGIME CLASSIFICATION │
        │                        │
        │  If monotonicity > 0.75│
        │    → TREND             │
        │  Elif monotonicity > 0.6│
        │    → REVERT            │
        │  Else                  │
        │    → AVOID             │
        └────────┬───────────────┘
                 │
                 ▼
        ┌────────────────────────┐
        │  TRADING STRATEGY      │
        │  • TREND: Momentum     │
        │  • REVERT: Market make │
        │  • AVOID: Flatten      │
        └────────────────────────┘
```

---

## 4. Concrete Implementation Plan

### **Week 1-2: Foundation**
1. Implement entropy feature extraction (Phase 1.5 from TASKS_1_26.md)
2. Implement forward monotonicity computation
3. Set up data buffering and persistence

### **Week 3: MI Computation**
1. Implement KSG MI estimator using sklearn
2. Compute feature-wise MI rankings
3. Validate on historical data

### **Week 4: Hypervolume Identification**
1. Train decision tree on (X, Y) pairs
2. Extract and interpret hyperrectangle rules
3. Train random forest for better accuracy
4. Compare performance

### **Week 5: Continuous Learning**
1. Implement sliding window retraining
2. Add drift detection
3. Test on live paper trading

### **Week 6: Optimization**
1. Add dimensionality reduction (PCA/UMAP)
2. Optimize MI computation frequency
3. Benchmark computational cost

---

## 5. Tools and Libraries

### **Python Stack (Recommended)**

```python
# Core
numpy                  # Numerical operations
pandas                 # Data handling
scipy                  # Statistical functions

# Machine Learning
scikit-learn          # MI estimation, tree models, PCA
xgboost               # Gradient boosting trees
hdbscan               # Density clustering

# Information Theory
jpype                 # Java bridge for JIDT (advanced MI estimation)
# or
dit                   # Discrete information theory

# Online Learning
river                 # Online ML algorithms
scikit-multiflow      # Stream learning

# Dimensionality Reduction
umap-learn            # Non-linear projection
plotly                # 3D visualization

# Deep Learning (for MINE)
torch                 # Neural MI estimation

# Drift Detection
alibi-detect          # Distribution shift detection
```

### **Rust Integration**

Since your system is in Rust, you'll want Rust-side components:

```toml
[dependencies]
# MI estimation
ndarray = "0.15"
ndarray-stats = "0.5"
linfa = "0.7"           # ML framework (has decision trees)
smartcore = "0.3"       # Another ML library

# For calling Python
pyo3 = "0.20"           # Python interop
numpy = "0.20"          # NumPy arrays in Rust

# Alternative: Pure Rust
rstats = "1.0"          # Statistical functions
```

**Architecture:**
- **Rust:** Feature extraction, data pipeline, trading execution
- **Python:** MI computation, hypervolume training (heavy ML)
- **Bridge:** PyO3 for Rust ↔ Python communication

```rust
use pyo3::prelude::*;
use pyo3::types::PyModule;

// Call Python MI estimation from Rust
fn compute_mi(features: Vec<Vec<f64>>, target: Vec<f64>) -> PyResult<Vec<f64>> {
    Python::with_gil(|py| {
        let sklearn = PyModule::import(py, "sklearn.feature_selection")?;
        let mi_fn = sklearn.getattr("mutual_info_regression")?;

        let result = mi_fn.call1((features, target))?;
        result.extract()
    })
}
```

---

## 6. Academic References

**Mutual Information:**
1. Kraskov, Stögbauer, Grassberger (2004) - "Estimating mutual information" - *Physical Review E*
2. Belghazi et al. (2018) - "MINE: Mutual Information Neural Estimation" - *ICML*
3. Cover & Thomas (2006) - "Elements of Information Theory" - Textbook

**Information Theory in Finance:**
4. Schreiber (2000) - "Measuring information transfer" - *Physical Review Letters*
5. Dionisio et al. (2004) - "Mutual information in financial markets" - *Physica A*
6. Diks & Panchenko (2006) - "Nonparametric Granger causality" - *Journal of Economic Dynamics*

**Regime Detection:**
7. Hamilton (1989) - "Regime-switching models" - *Econometrica*
8. Ang & Timmermann (2012) - "Regime changes and financial markets" - *Annual Review*

**Information Geometry:**
9. Amari (1998) - "Natural gradient works efficiently in learning" - *Neural Computation*
10. Cover & Thomas (2006) - "Elements of Information Theory" - Wiley

---

## 7. Final Assessment

### **Originality: 8/10**
- Novel application of MI to entropy features for regime detection
- Hypervolume representation is geometrically sophisticated
- Continuous learning addresses real problem (non-stationarity)

### **Validity: 9/10**
- Strong theoretical foundation (information theory + geometry)
- MI is correct tool for non-linear dependencies
- Well-suited to trading problem

### **Feasibility: 7/10**
- ✅ Doable with existing tools (sklearn, river, PyO3)
- ⚠️ Computationally intensive (need optimization)
- ⚠️ Requires careful hyperparameter tuning
- ⚠️ Curse of dimensionality is real (need dimensionality reduction)

### **Expected Impact: 8/10**
- Significant improvement over fixed thresholds
- Adaptive to market changes
- Interpretable (especially with decision trees)
- Addresses fundamental problem in regime detection

---

## 8. Recommendation

**Start with this simplified pipeline:**

1. **Implement 5 best entropy features** (sample, permutation, OB, KL div tick, KL div vol)
2. **Use KSG MI estimator** (sklearn) - fastest to implement
3. **Train Random Forest** - good balance of accuracy and interpretability
4. **Sliding window retraining** - every 10,000 samples
5. **Validate on historical data** - compare to fixed threshold baseline

If this works (Sharpe improvement >20%), then add:
- Neural MI estimation (MINE)
- More sophisticated hypervolume methods (GMM, SVM)
- Online learning (river)
- Dimensionality reduction (UMAP)

**This is a research-grade approach that could be publishable if it works well!**

---

## 9. Example: End-to-End Minimal Implementation

Here's a complete minimal example you can run:

```python
import numpy as np
from sklearn.feature_selection import mutual_info_regression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Step 1: Load your data
# X shape: (n_samples, n_features) - entropy features
# Y shape: (n_samples,) - forward monotonicity
# For demonstration, using synthetic data
n_samples = 10000
n_features = 5

# Simulate entropy features
X = np.random.rand(n_samples, n_features)
# Simulate monotonicity (correlated with features)
Y = 0.5 + 0.3 * X[:, 0] + 0.2 * (1 - X[:, 1]) + 0.1 * np.random.rand(n_samples)
Y = np.clip(Y, 0.5, 1.0)

# Step 2: Train/test split
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

# Step 3: Compute mutual information
feature_names = [f'entropy_feature_{i}' for i in range(n_features)]
mi_scores = mutual_info_regression(X_train, Y_train, n_neighbors=5, random_state=42)

print("Mutual Information Scores:")
for name, score in zip(feature_names, mi_scores):
    print(f"  {name}: {score:.4f} bits")

# Step 4: Train hypervolume model (Random Forest)
print("\nTraining Random Forest...")
rf = RandomForestRegressor(n_estimators=100, max_depth=8, random_state=42)
rf.fit(X_train, Y_train)

# Step 5: Evaluate
Y_pred_train = rf.predict(X_train)
Y_pred_test = rf.predict(X_test)

print(f"\nTrain MAE: {mean_absolute_error(Y_train, Y_pred_train):.4f}")
print(f"Test MAE: {mean_absolute_error(Y_test, Y_pred_test):.4f}")
print(f"Test R²: {r2_score(Y_test, Y_pred_test):.4f}")

# Step 6: Feature importance
print("\nFeature Importances:")
for name, importance in zip(feature_names, rf.feature_importances_):
    print(f"  {name}: {importance:.4f}")

# Step 7: Regime classification
def classify_regime(monotonicity):
    if monotonicity > 0.75:
        return "TREND"
    elif monotonicity > 0.60:
        return "REVERT"
    else:
        return "AVOID"

# Test on sample
sample_features = X_test[0]
pred_monotonicity = rf.predict([sample_features])[0]
regime = classify_regime(pred_monotonicity)

print(f"\nSample prediction:")
print(f"  Features: {sample_features}")
print(f"  Predicted monotonicity: {pred_monotonicity:.3f}")
print(f"  Regime: {regime}")
print(f"  Actual monotonicity: {Y_test[0]:.3f}")
```

This minimal example demonstrates the complete pipeline in ~60 lines of code!

---

**Document Version:** 1.0
**Created:** 2026-01-14
**Status:** Research Proposal & Implementation Guide
**Next Steps:** Implement Phase 1 (Entropy Features) from TASKS_1_26.md, then return to this document for MI-based regime learning
