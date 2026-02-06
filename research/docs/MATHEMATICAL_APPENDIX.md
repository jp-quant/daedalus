# Mathematical Appendix: Orderbook Imbalance Signal

> *Formal definitions and derivations for the imbalance-based trading strategy*

---

## 1. Notation

| Symbol | Description |
|--------|-------------|
| $P_b^{(i)}$ | Bid price at level $i$ |
| $P_a^{(i)}$ | Ask price at level $i$ |
| $Q_b^{(i)}$ | Bid quantity at level $i$ |
| $Q_a^{(i)}$ | Ask quantity at level $i$ |
| $P_m$ | Midprice: $(P_a^{(0)} + P_b^{(0)})/2$ |
| $r_t^{(h)}$ | Forward return at horizon $h$: $(P_{t+h} - P_t)/P_t$ |
| $\tau$ | Lookback window for z-score normalization |

---

## 2. Imbalance Metrics

### 2.1 Level-Specific Imbalance

For each level $i \in \{0, 1, ..., L-1\}$ where $L=20$:

$$I_i = \frac{Q_b^{(i)} - Q_a^{(i)}}{Q_b^{(i)} + Q_a^{(i)}} \in [-1, 1]$$

**Interpretation:**
- $I_i = +1$: All liquidity on bid side at level $i$
- $I_i = -1$: All liquidity on ask side at level $i$
- $I_i = 0$: Equal liquidity on both sides

### 2.2 Total Imbalance

Aggregate across all levels:

$$I_{\text{total}} = \frac{\sum_{i=0}^{L-1} Q_b^{(i)} - \sum_{i=0}^{L-1} Q_a^{(i)}}{\sum_{i=0}^{L-1} Q_b^{(i)} + \sum_{i=0}^{L-1} Q_a^{(i)}}$$

### 2.3 Smart Depth Imbalance

Distance-weighted imbalance, giving more weight to near-the-money levels:

$$I_{\text{smart}} = \frac{\sum_{i=0}^{L-1} w_i \cdot Q_b^{(i)} - \sum_{i=0}^{L-1} w_i \cdot Q_a^{(i)}}{\sum_{i=0}^{L-1} w_i \cdot (Q_b^{(i)} + Q_a^{(i)})}$$

where the weight function is:

$$w_i = \exp\left(-\lambda \cdot d_i\right)$$

and $d_i$ is the distance from midprice:

$$d_i = \frac{|P_m - P^{(i)}|}{P_m}$$

---

## 3. Signal Construction

### 3.1 Rolling Z-Score Normalization

To create a stationary signal, we normalize using a rolling z-score:

$$z_t = \frac{I_t - \bar{I}_{t-\tau:t}}{\sigma_{I, t-\tau:t}}$$

where:
- $\bar{I}_{t-\tau:t} = \frac{1}{\tau} \sum_{s=t-\tau}^{t-1} I_s$ is the rolling mean
- $\sigma_{I, t-\tau:t} = \sqrt{\frac{1}{\tau-1} \sum_{s=t-\tau}^{t-1} (I_s - \bar{I})^2}$ is the rolling standard deviation
- $\tau = 600$ seconds in our implementation

### 3.2 Trading Rule

Define the position $\theta_t \in \{-1, 0, +1\}$:

$$\theta_t = \begin{cases}
+1 & \text{if } z_t > z_{\text{entry}} \text{ and } \theta_{t-1} \leq 0 \\
-1 & \text{if } z_t < -z_{\text{entry}} \text{ and } \theta_{t-1} \geq 0 \\
0 & \text{if } |z_t| < z_{\text{exit}} \text{ or } (t - t_{\text{entry}}) > \tau_{\text{max}} \\
\theta_{t-1} & \text{otherwise}
\end{cases}$$

**Optimal Parameters (from grid search):**
- $z_{\text{entry}} = 1.5$
- $z_{\text{exit}} = 0.5$
- $\tau_{\text{max}} = 300$ seconds

---

## 4. Performance Metrics

### 4.1 Gross Return

$$R_{\text{gross}} = \prod_{i=1}^{N} (1 + \theta_i \cdot r_i) - 1$$

where $r_i$ is the return on trade $i$ and $N$ is the total number of trades.

### 4.2 Net Return (after fees)

$$R_{\text{net}} = \prod_{i=1}^{N} (1 + \theta_i \cdot r_i - 2f) - 1$$

where $f$ is the one-way fee (paid on both entry and exit).

### 4.3 Breakeven Fee

Setting $R_{\text{net}} = 0$ and solving for $f$:

For small returns (Taylor approximation):

$$f^* \approx \frac{\sum_{i=1}^{N} r_i}{2N} = \frac{R_{\text{gross}}}{2N}$$

**Empirical Result:**

$$f^* = \frac{0.8455}{2 \times 10513} = 4.02 \times 10^{-5} = 0.402 \text{ bps (one-way)}$$

Since fees are charged round-trip:

$$f^*_{\text{round-trip}} = 0.27 \text{ bps per trade}$$

### 4.4 Sharpe Ratio

$$\text{Sharpe} = \frac{\mathbb{E}[r_i] - r_f}{\sigma(r_i)} \cdot \sqrt{T}$$

where $T$ is the annualization factor.

For our strategy (assuming 24/7 trading, ~2000 trades/day):

$$\text{Sharpe}_{\text{annual}} \approx \text{Sharpe}_{\text{daily}} \cdot \sqrt{365}$$

---

## 5. Correlation Analysis

### 5.1 Pearson Correlation

$$\rho(I, r^{(h)}) = \frac{\text{Cov}(I_t, r_t^{(h)})}{\sigma_I \cdot \sigma_{r^{(h)}}}$$

### 5.2 Information Coefficient

The IC measures rank correlation:

$$\text{IC} = \text{Spearman}(I_t, r_t^{(h)})$$

### 5.3 Empirical Results

| Horizon $h$ | $\rho(I_{\text{total}}, r^{(h)})$ | t-statistic | p-value |
|-------------|-----------------------------------|-------------|---------|
| 10s | 0.0813 | 157.2 | < 0.001 |
| 30s | 0.0845 | 163.4 | < 0.001 |
| 60s | 0.0750 | 145.0 | < 0.001 |
| 120s | 0.0612 | 118.3 | < 0.001 |
| 300s | 0.0592 | 114.4 | < 0.001 |

---

## 6. Signal Decay Model

The correlation between imbalance and forward returns follows an exponential decay:

$$\rho(h) = \rho_0 \cdot e^{-h/\tau_{1/2}} + \rho_{\infty}$$

**Fitted Parameters:**
- $\rho_0 = 0.085$ (initial correlation)
- $\tau_{1/2} = 45$ seconds (half-life)
- $\rho_{\infty} = 0.02$ (asymptotic correlation)

This suggests that ~50% of the predictive information decays within 45 seconds.

---

## 7. Optimal Position Sizing (Kelly Criterion)

Given win probability $p$ and average win/loss ratio $W/L$:

$$f^* = \frac{p \cdot W - (1-p) \cdot L}{W \cdot L}$$

For our strategy at 0.10 bps fee:
- $p = 0.467$ (win rate)
- $W = 0.0012$ (average winning trade)
- $L = 0.0008$ (average losing trade)

$$f^* = \frac{0.467 \times 0.0012 - 0.533 \times 0.0008}{0.0012 \times 0.0008} \approx 0.15$$

Recommended position size: **15% of capital per trade** (half-Kelly: 7.5%)

---

## 8. Statistical Significance

### 8.1 Hypothesis Test

$$H_0: \rho(I, r) = 0 \quad \text{vs} \quad H_1: \rho(I, r) \neq 0$$

Test statistic:

$$t = \rho \sqrt{\frac{n-2}{1-\rho^2}}$$

For $n = 3,745,920$ and $\rho = 0.082$:

$$t = 0.082 \sqrt{\frac{3745918}{1-0.082^2}} = 159.7$$

**Conclusion:** Reject $H_0$ at any conventional significance level (p < 10⁻¹⁰⁰⁰)

### 8.2 Multiple Testing Correction

Given 20 imbalance features tested at 5 horizons (100 tests), Bonferroni-corrected significance level:

$$\alpha^* = \frac{0.05}{100} = 0.0005$$

All reported correlations exceed this threshold.

---

## References

1. **Kelly, J. L.** (1956). A New Interpretation of Information Rate. *Bell System Technical Journal*, 35(4), 917-926.

2. **Cont, R., Kukanov, A., & Stoikov, S.** (2014). The Price Impact of Order Book Events. *Journal of Financial Econometrics*, 12(1), 47-88.

3. **De Prado, M. L.** (2018). *Advances in Financial Machine Learning*. Wiley.

---

*Document version: 1.0*  
*Last updated: February 2026*
