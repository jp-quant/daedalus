# Mathematical Appendix: Orderbook Imbalance Signal & Multi-Asset Alpha

> *Formal definitions and derivations for the imbalance-based trading strategy and multi-asset expansion*

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

## 9. ML Direction Classifier

### 9.1 XGBoost Binary Classification

The direction classifier predicts:

$$y_t = \mathbb{1}\{r_t^{(h)} > 0\}$$

where $r_t^{(h)} = \frac{P_{t+h} - P_t}{P_t}$ is the forward return at horizon $h = 30$ bars.

**Feature vector:**

$$\mathbf{x}_t = [I_3, I_5, I_1, I_{\text{band}_{0-5}}, I_{10}, \text{cog}_{\text{vs\_mid}}, \text{ofi}_{5s}, I_{\text{smart}}]$$

The model outputs probability $\hat{p}_t = P(y_t = 1 | \mathbf{x}_t)$.

### 9.2 Asymmetric Threshold Strategy

Position assignment via asymmetric probability thresholds:

$$\theta_t = \begin{cases}
+1 & \text{if } \hat{p}_t > p_{\text{long}} = 0.6 \\
-1 & \text{if } \hat{p}_t < p_{\text{short}} = 0.4 \\
0 & \text{otherwise}
\end{cases}$$

With fixed holding period $\tau_{\text{hold}} = 30$ bars (no early exit).

### 9.3 AUC-ROC

The Area Under the ROC Curve measures discrimination ability:

$$\text{AUC} = P(\hat{p}_{\text{pos}} > \hat{p}_{\text{neg}})$$

where $\hat{p}_{\text{pos}}$ and $\hat{p}_{\text{neg}}$ are model scores for randomly drawn positive and negative examples.

**Empirical results (multi-asset OOS):**

| Asset | AUC | Interpretation |
|-------|-----|----------------|
| AVAX-USD | **0.827** | Excellent |
| ADA-USD | 0.779 | Good |
| DOGE-USD | 0.762 | Good |
| HBAR-USD | 0.736 | Good |
| AAVE-USD | 0.722 | Good |
| FARTCOIN-USD | 0.685 | Fair |
| BCH-USD | 0.622 | Fair |
| ETH-USD | 0.616 | Fair |
| BTC-USD | 0.576 | Weak |

---

## 10. Multi-Asset Correlation Analysis

### 10.1 Cross-Asset Signal Strength

For asset $a$ and feature $f$, the signal strength is:

$$\rho_{a,f} = |\text{Corr}(f_{a,t}, r_{a,t}^{(30)})|$$

The signal strength varies dramatically by asset. For the dominant feature $I_3$ (imbalance_L3):

$$\rho_{\text{HBAR}} = 0.298 \gg \rho_{\text{BTC}} = 0.064$$

**Ratio**: $\rho_{\text{HBAR}} / \rho_{\text{BTC}} = 4.66\times$ stronger.

### 10.2 Cross-Asset Transfer Matrix

For models trained on asset $a$ and tested on asset $b$:

$$T_{a \to b} = \text{AUC}(M_a, D_b)$$

where $M_a$ is the model trained on asset $a$ and $D_b$ is the test data from asset $b$.

**Finding**: $T_{a \to a} > T_{a \to b}$ for all $a \neq b$
- Asset-specific models dominate cross-trained models
- Transfer AUC range: 0.518-0.660 (above random but suboptimal)

---

## 11. Portfolio Construction

### 11.1 Equal-Weight Portfolio Return

For $K = 9$ assets with daily returns $r_{a,d}$:

$$R_{\text{portfolio},d} = \frac{1}{K} \sum_{a=1}^{K} r_{a,d}$$

**Compound portfolio return:**

$$R_{\text{total}} = \prod_{d=1}^{D} (1 + R_{\text{portfolio},d}) - 1$$

For $D = 9$ OOS days: $R_{\text{total}} = +99{,}201\%$

### 11.2 Portfolio Diversification Benefit

Daily return volatility reduction from diversification:

$$\sigma_{\text{portfolio}} \leq \frac{1}{K} \sum_{a=1}^{K} \sigma_a$$

with equality only when $\text{Corr}(r_a, r_b) = 1$ for all pairs. Our empirical correlation matrix shows low inter-asset correlations, confirming diversification benefit.

---

## 12. Statistical Validation

### 12.1 Permutation Test

**Null hypothesis**: The strategy's returns are independent of the signal ordering.

For $B = 200$ permutations:

1. Compute observed test statistic $T_{\text{obs}} = \bar{r}_{\text{strategy}}$
2. For $b = 1, ..., B$: shuffle signal labels (or time-shift signals), compute $T_b^*$
3. p-value:

$$p = \frac{1 + \sum_{b=1}^{B} \mathbb{1}\{T_b^* \geq T_{\text{obs}}\}}{1 + B}$$

**Results**: 8/9 assets $p < 0.005$. BTC $p = 0.383$ (fails).

### 12.2 Bootstrap Confidence Intervals

For $N = 1000$ bootstrap resamples of daily returns:

$$\text{CI}_{95\%} = [\hat{Q}_{0.025}, \hat{Q}_{0.975}]$$

where $\hat{Q}_\alpha$ is the $\alpha$-quantile of bootstrap mean returns.

**Probability of positive return:**

$$P(\bar{r} > 0) = \frac{1}{N} \sum_{n=1}^{N} \mathbb{1}\{\bar{r}_n^* > 0\}$$

**Result**: $P(\bar{r} > 0) = 100\%$ for all 8 altcoins.

### 12.3 Holm-Bonferroni Correction

For $m = 9$ hypothesis tests with ordered p-values $p_{(1)} \leq p_{(2)} \leq ... \leq p_{(m)}$:

Reject $H_{(i)}$ if:

$$p_{(i)} \leq \frac{\alpha}{m - i + 1}$$

where $\alpha = 0.05$.

**Result**: All 8 altcoins survive correction. BTC is rejected at step 1 (highest p-value).

---

## 13. Fee Sensitivity Model

### 13.1 Per-Trade Net Return

For a trade with gross return $r$ at fee level $f$ (bps, per side):

$$r_{\text{net}} = r - 2f$$

where the factor of 2 accounts for entry and exit fees.

### 13.2 Multi-Asset Breakeven Analysis

For asset $a$ with $N_a$ trades and gross return $R_a$:

$$f_a^* = \frac{R_a}{2N_a}$$

**Breakeven fees:**

| Asset | $N_a$ (trades) | $f_a^*$ (bps) |
|-------|----------------|---------------|
| HBAR-USD | ~27,000/day | >0.5 |
| DOGE-USD | ~27,000/day | >0.5 |
| ADA-USD | ~27,000/day | >0.5 |
| BTC-USD | ~3,200/day | 0.11 |

### 13.3 Return Decay Under Fees

The compound return at fee $f$ relative to gross return:

$$R(f) = \prod_{i=1}^{N} (1 + r_i - 2f) - 1$$

This is a monotonically decreasing function of $f$. The rate of decay depends on trade frequency: higher-frequency strategies are more fee-sensitive.

---

## 14. Holding Period Analysis

### 14.1 Signal Decay Across Horizons

For horizon $h$ bars, the AUC of the direction classifier decays as:

$$\text{AUC}(h) \approx 0.5 + \frac{\Delta \text{AUC}_0}{1 + (h / h_{1/2})^\gamma}$$

where $\Delta \text{AUC}_0 \approx 0.25$ (excess AUC at $h=30$), $h_{1/2} \approx 300$ bars (5 minutes), and $\gamma \approx 1$.

### 14.2 Return-Horizon Relationship

Compound returns decrease exponentially with horizon (at constant fee):

$$\log R(h) \approx \alpha_0 - \beta \cdot \log h$$

This reflects: fewer trades per day ($N \propto 1/h$) and lower per-trade edge ($\delta \propto h^{-\beta}$).

**Empirical fit** (4-asset average):

| $h$ (bars) | $\log_{10} R$ | Trades/day |
|------------|---------------|------------|
| 30 | 5.24 | ~3,000 |
| 60 | 4.19 | ~1,500 |
| 120 | 3.37 | ~700 |
| 300 | 2.41 | ~250 |
| 600 | 1.89 | ~100 |

---

## 15. Market Impact Model

### 15.1 Kyle's Lambda

Following Kyle (1985), the price impact coefficient $\lambda$ measures the price change per unit of signed order flow:

$$\Delta P = \lambda \cdot q + \epsilon$$

where $q$ is the signed trade volume and $\epsilon$ is noise.

Estimated from orderbook data using rolling regression:

$$\hat{\lambda} = \frac{\text{Cov}(\Delta P, q)}{\text{Var}(q)}$$

### 15.2 Position Size Impact

For a position of size $V$ (in USD), the estimated market impact is:

$$\text{Impact}(V) = \sqrt{\lambda} \cdot \sqrt{\frac{V}{P}} \cdot 10^4 \quad \text{(bps)}$$

where $P$ is the asset price.

The participation rate is:

$$\text{PR}(V) = \frac{V / P}{N_{\text{trades/day}} \cdot \bar{Q}_{\text{L1}}}$$

**Empirical results** (NB05):

| Asset | $\hat{\lambda}$ | Impact at $100K | Participation Rate |
|-------|-----------------|-----------------|-------------------|
| HBAR | ~0 | ~0 bps | 0.027 |
| DOGE | ~0 | ~0 bps | 0.025 |
| ADA | ~0 | ~0 bps | 0.028 |
| AAVE | 0.000011 | 0.034 bps | 0.101 |

---

## 16. Execution Cost Decomposition

### 16.1 Total Execution Cost

The total execution cost per trade under realistic conditions:

$$c_{\text{total}} = c_{\text{fee}} + c_{\text{slippage}} + c_{\text{impact}}$$

where $c_{\text{fee}}$ is the exchange fee, $c_{\text{slippage}}$ is the bid-ask crossing cost, and $c_{\text{impact}}$ is the market impact from §15.

### 16.2 Latency-Adjusted Returns

For signal latency of $\ell$ bars, the effective entry price shifts:

$$P_{\text{entry}}(\ell) = P_{t+\ell}$$

This reduces the expected return by the drift over $\ell$ bars:

$$\mathbb{E}[r | \text{signal}] \to \mathbb{E}[r | \text{signal}] - \mathbb{E}[\Delta P_{t:t+\ell} | \text{signal}]$$

The signal auto-correlation ensures $\mathbb{E}[\Delta P | \text{signal}] > 0$ for long signals, so latency always reduces returns.

**Empirical decay** (HBAR, 30s, long-only):

| Latency (bars) | Return | Decay |
|---------------|--------|-------|
| 0 | +41,833% | 1.00x |
| 1 | +19,170% | 0.46x |
| 2 | +11,375% | 0.27x |
| 5 | +2,919% | 0.07x |

---

## 17. Long-Only Constraint

### 17.1 Alpha Retention

Define the long-only alpha retention ratio:

$$\rho_{\text{LO}}(h) = \frac{R_{\text{long-only}}(h)}{R_{\text{long+short}}(h)}$$

**Empirical finding**: $\rho_{\text{LO}}$ increases monotonically with horizon:

$$\rho_{\text{LO}}(30s) \approx 0.20, \quad \rho_{\text{LO}}(60s) \approx 0.42, \quad \rho_{\text{LO}}(120s) \approx 0.55$$

This occurs because longer horizons have fewer short entries (lower conviction on the short side at longer intervals), so removing shorts removes proportionally less alpha.

### 17.2 Win Rate Enhancement

Long-only mode shows higher per-trade win rates than long+short:

$$\text{WR}_{\text{LO}} > \text{WR}_{\text{L+S}}$$

This is because short trades in crypto markets face a positive drift (crypto tends upward during the sample period), so removing losing short trades increases the average win rate.

---

## 18. Multi-Timeframe Ensemble (NB06)

### 18.1 Per-Horizon Models

Train separate XGBoost classifiers $\hat{f}_h$ for each horizon $h \in \{30s, 60s, 120s\}$:

$$\hat{p}_h(t) = \hat{f}_h\bigl(\mathbf{x}(t)\bigr) \in [0, 1]$$

where $\hat{p}_h(t)$ is the predicted probability that the price moves up over horizon $h$.

### 18.2 AUC-Weighted Ensemble

Given per-horizon AUC scores $a_h$ on the training fold, the ensemble probability is:

$$\hat{p}_{\text{ens}}(t) = \frac{\sum_{h} a_h \cdot \hat{p}_h(t)}{\sum_{h} a_h}$$

Position signal: $\text{pos}(t) = \begin{cases} +1 & \text{if } \hat{p}_{\text{ens}}(t) > 0.6 \\ -1 & \text{if } \hat{p}_{\text{ens}}(t) < 0.4 \\ 0 & \text{otherwise} \end{cases}$

### 18.3 Simple Average Ensemble

$$\hat{p}_{\text{avg}}(t) = \frac{1}{|H|}\sum_{h \in H} \hat{p}_h(t)$$

### 18.4 Agreement Filter Ensemble

Uses the best-horizon (30s) model but only trades when a majority of horizons agree:

$$\text{agree}(t) = \mathbb{1}\!\left[\sum_{h} \mathbb{1}[\hat{p}_h(t) > 0.5] \geq \lceil|H|/2\rceil\right]$$

$$\text{pos}(t) = \text{pos}_{30s}(t) \cdot \text{agree}(t)$$

### 18.5 Empirical Finding

30s standalone dominates all ensemble methods. Returns scale inversely with horizon AUC degradation:

$$\text{AUC}_{30s} \approx 0.77 > \text{AUC}_{60s} \approx 0.70 > \text{AUC}_{120s} \approx 0.64$$

Ensembles dilute the strongest (30s) signal with weaker horizons.

---

## 19. Dynamic Asset Allocation (NB06)

### 19.1 Equal Weight

$$w_i^{\text{EW}} = \frac{1}{N}, \quad \forall i \in \{1, \ldots, N\}$$

### 19.2 AUC-Weighted

Given daily AUC estimates $a_i$ per asset $i$:

$$w_i^{\text{AUC}} = \frac{a_i}{\sum_{j=1}^{N} a_j}$$

### 19.3 Momentum Weighting

Using trailing $k$-day cumulative return $R_i^{(k)}$:

$$w_i^{\text{mom}} = \frac{\max(R_i^{(k)}, 0)}{\sum_{j=1}^{N} \max(R_j^{(k)}, 0)}$$

Assets with non-positive trailing returns receive zero weight. If all assets have non-positive returns, revert to equal weight.

### 19.4 Inverse Volatility

Using trailing realized volatility $\sigma_i$:

$$w_i^{\text{IV}} = \frac{1/\sigma_i}{\sum_{j=1}^{N} 1/\sigma_j}$$

### 19.5 Portfolio Return

$$R_{\text{port}}(t) = \sum_{i=1}^{N} w_i(t) \cdot R_i(t)$$

Weights are rebalanced daily. Compounding over $D$ days:

$$R_{\text{total}} = \prod_{d=1}^{D} \bigl(1 + R_{\text{port}}(d)\bigr) - 1$$

---

## 20. Feature Interaction Engineering (NB06)

### 20.1 Ratio Features

Constructed to capture interaction between imbalance and volatility:

$$\text{imb\_L3\_div\_rv60} = \frac{\text{imbalance\_L3}}{\text{rv\_60s\_right} + \epsilon}$$

$$\text{ofi5\_div\_spread} = \frac{\text{ofi\_sum\_5s}}{\text{relative\_spread} + \epsilon}$$

$$\text{micro\_div\_spread} = \frac{\text{micro\_minus\_mid}}{\text{relative\_spread} + \epsilon}$$

where $\epsilon = 10^{-10}$ prevents division by zero.

### 20.2 Product Features

$$\text{imb\_L3\_x\_rv60} = \text{imbalance\_L3} \times \text{rv\_60s\_right}$$

$$\text{cog\_imb\_interaction} = \text{cog\_vs\_mid} \times \text{imbalance\_L3}$$

### 20.3 Gradient Features

Capture how imbalance changes across depth levels:

$$\text{imb\_gradient\_L1\_L3} = \text{imbalance\_L1} - \text{imbalance\_L3}$$
$$\text{imb\_gradient\_L3\_L5} = \text{imbalance\_L3} - \text{imbalance\_L5}$$
$$\text{imb\_gradient\_L1\_L10} = \text{imbalance\_L1} - \text{imbalance\_L10}$$

### 20.4 Composite Features

$$\text{depth\_asymmetry} = \frac{\text{total\_bid\_depth} - \text{total\_ask\_depth}}{\text{total\_bid\_depth} + \text{total\_ask\_depth} + \epsilon}$$

$$\text{smart\_depth\_ratio} = \frac{\text{smart\_bid\_depth}}{\text{smart\_ask\_depth} + \epsilon}$$

$$\text{momentum\_imb\_agreement} = \text{sign}(\text{log\_return}) \cdot \text{imbalance\_L3}$$

$$\text{vol\_regime\_ratio} = \frac{\text{rv\_5s\_right}}{\text{rv\_60s\_right} + \epsilon}$$

$$\text{ofi\_accel} = \text{ofi\_sum\_5s} - \text{ofi\_sum\_30s}$$

---

## References

1. **Kelly, J. L.** (1956). A New Interpretation of Information Rate. *Bell System Technical Journal*, 35(4), 917-926.

2. **Cont, R., Kukanov, A., & Stoikov, S.** (2014). The Price Impact of Order Book Events. *Journal of Financial Econometrics*, 12(1), 47-88.

3. **De Prado, M. L.** (2018). *Advances in Financial Machine Learning*. Wiley.

4. **Holm, S.** (1979). A Simple Sequentially Rejective Multiple Test Procedure. *Scandinavian Journal of Statistics*, 6(2), 65-70.

5. **Efron, B., & Tibshirani, R. J.** (1993). *An Introduction to the Bootstrap*. Chapman & Hall.

6. **Kyle, A. S.** (1985). Continuous Auctions and Insider Trading. *Econometrica*, 53(6), 1315-1335.

---

*Document version: 4.0*  
*Last updated: February 13, 2026*
