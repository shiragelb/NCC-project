# Interactive Chain Validation — Experiment Summary & Statistical Bounds

This README documents a **Chain Validation Tool** run that sampled and manually validated 40 chains drawn at random from the `chains_chapter_*.json` files.

## Experiment Design

* **Script:** `Interactive Chain Validation Tool - Sample 40 chains`
* **Sampling:** Uniform random selection over all `(chapter, chain_id)` pairs, without replacement, until reaching `target_samples = 40`.
* **Labels:**

  * **Clean** = no false positives in the chain.
  * **Contaminated** = ≥1 false positive in the chain.
* **Stats method:** **Wilson score 95% CI (z = 1.96)** for proportions.

---

## Results

* **Total sampled:** 40
* **Clean:** 39
* **Contaminated:** 1
* **Observed clean rate:** 39/40 = **97.5%**
* **Observed false-positive (FP) rate:** 1/40 = **2.5%**
* **Standard error (Wald, ref.):** ~ **2.47 percentage points**

### 95% Confidence Intervals (Wilson)

* **Clean rate:** **[87.12%, 99.56%]**
* **False-positive rate:** **[0.44%, 12.88%]**

> **Interpretation:** With 95% confidence, the true FP rate lies between **~0.44% and ~12.88%** given this sample.

---

## Conclusion on False Positives

* **Point estimate:** FP ≈ **2.5%**.
* **Bound (95% CI):** FP ∈ **[~0.44%, ~12.88%]**.

Further **budget for API calls** (e.g., to enrich upstream signals/features) can **substantially improve performance**, and would be expected to reduce the false-positive rate and/or tighten these statistical bounds in future runs.

---

## Contaminated Chains (for reference)

```
Chapter 13 — merged_chain_15_13_2012_chain_28_13_2019
```

---

## Reproducibility

Run from the directory containing `chains_chapter_*.json`:

```bash
python interactive_chain_validation.py
```

The tool loads all chapters, randomly samples chains, prompts for labels, and writes `validation_results.json`. Example summary from this run:

```json
{
  "statistics": {
    "total_sampled": 40,
    "clean_count": 39,
    "contaminated_count": 1,
    "clean_rate": 0.975
  }
}
```
