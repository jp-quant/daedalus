# ETL Implementation Appendix

This appendix captures technical contracts, formulas, and invariants that ETL changes must preserve.

---

## 1) Directory-Aligned Partitioning Contract

For partition columns $P = \{exchange, symbol, year, month, day, hour\}$:

- values must exist as columns in each output record,
- values must match the directory path segments exactly,
- sanitization rules must be consistent between ingestion and ETL.

Example path:

`exchange=coinbaseadvanced/symbol=BTC-USD/year=2026/month=02/day=19/hour=14/`

Any deviation is a data contract failure.

---

## 2) Stateful Chronological Processing Invariant

For each `(exchange, symbol)` stream, snapshots must be processed in non-decreasing timestamp order.

Stateful features depend on previous state:

- OFI / MLOFI
- velocity / acceleration
- rolling OFI / return statistics
- Kyle’s lambda updates
- VPIN updates

If timestamp gaps exceed staleness threshold, `prev_*` state must reset before computing delta-based features.

---

## 3) Core Microstructure Formulas

### Microprice

$$
\text{microprice} = \frac{bid\_size \cdot ask\_price + ask\_size \cdot bid\_price}{bid\_size + ask\_size}
$$

### Relative Spread

$$
\text{relative\_spread} = \frac{ask - bid}{mid}
$$

### L1 Imbalance

$$
\text{imbalance}_{L1} = \frac{bid\_size - ask\_size}{bid\_size + ask\_size}
$$

### Trade Flow Imbalance (TFI)

$$
\text{TFI} = \frac{V_{buy} - V_{sell}}{V_{buy} + V_{sell}}
$$

### Kyle’s Lambda (rolling estimate)

$$
\lambda = \frac{\operatorname{Cov}(\Delta p, q)}{\operatorname{Var}(q)}
$$

where $q$ is signed flow and $\Delta p$ is return/price change.

### VPIN (conceptual)

Given volume buckets of fixed size, VPIN is a rolling average of order-flow imbalance per bucket:

$$
\text{VPIN} \approx \frac{1}{N}\sum_{i=1}^{N}\frac{|V^{buy}_i - V^{sell}_i|}{V^{buy}_i + V^{sell}_i}
$$

---

## 4) Resampling Safety (ASOF)

When resampling is enabled:

- only use last-known values up to timestamp $t$,
- never use future observations,
- enforce maximum staleness if configured.

This is required for backtest-safe and training-safe feature generation.

---

## 5) Quote Normalization Contract

If `normalize_quotes=True`, treat stable quote variants consistently (e.g., USD/USDC/USDT as canonical USD where configured).

Implications:

- partition filtering can match equivalent quote variants,
- joins between orderbook and trades must not silently fail due to quote naming mismatch,
- output symbols should follow canonical normalization policy.

---

## 6) ETL Change Checklist (Before Merge)

1. Does the change preserve partition columns and directory alignment?
2. Does stateful logic still process chronologically and reset correctly on stale gaps/symbol switches?
3. Does TFI behavior remain valid when trades are absent/present?
4. Are single-run, batched, and watcher semantics still compatible?
5. Was at least one targeted validation run recorded?
6. Were ETL continuity docs updated (`README`, prompt, context, appendix, index)?

If any answer is “no”, the change is incomplete.
