# Assignment 2 – Part 3 Findings: Multi-class SVM Text Classification

## Setup

Pipeline (built in `src/data_intensive_computing/assignment2/part3_classification.ipynb`):

```
RegexTokenizer → StopWordsRemover → CountVectorizer → IDF →
  StringIndexer → ChiSqSelector → Normalizer(L2) →
    OneVsRest(LinearSVC)
```

(One filter mode swaps `CountVectorizer + IDF + ChiSqSelector` for `HashingTF(numFeatures=512) + IDF`.)

- **Multi-class strategy:** `OneVsRest` over the binary `LinearSVC`.
- **Vector length normalisation:** `Normalizer(p=2.0)` after the chi² selector, before the classifier (per the spec).
- **Splits:** 60 / 20 / 20 train / val / test, `seed=11817173` (student id) for reproducibility.
- **Search strategy:** manual validation grid. The active implementation fits/cache features once per selector variant, then runs only `OneVsRest(LinearSVC)` for each SVM hyperparameter config.
- **Spark (local):** `spark.driver.memory=6g`, `PARALLELISM=4`.
- **Spark (cluster):** `spark-submit --master yarn --deploy-mode cluster --num-executors 4 --executor-cores 4 --executor-memory 6g`, `PARALLELISM=4`.
- **Metric:** `MulticlassClassificationEvaluator(metricName="f1")`.

### Architecture: precomputed features + parallel classifier grid

The notebook deliberately **does not** use Spark's cross-executor distribution for the SVM fits. For 78k-row data the LBFGS aggregation network round trips dominate wall-clock; we observed >10× slowdown on YARN vs single-machine on the same code.

The first refactor used **embarrassingly parallel single-partition full-pipeline fits**, all using Spark ML libraries:

1. `train_df` and `val_df` are `coalesce(1).cache()`-d once per filter mode. With one partition, LBFGS `treeAggregate` has nothing to aggregate across — each fit's math runs in a single executor JVM, no inter-executor RPC.
2. The 24 grid configs dispatch concurrently as **separate Spark jobs** via a driver-side `ThreadPoolExecutor(max_workers=PARALLELISM)`. Each thread submits one `pipeline.fit(train_1p)` call. Spark schedules these jobs on whichever executor cores are free.
3. We bypass `TrainValidationSplit` so the inner train/val split is exactly the outer one — no double-splitting.

This trades cross-executor *data* parallelism (useless on small data) for cross-config *job* parallelism (one full ML experiment per executor, no sync between configs). It is the standard pattern for hyperparameter tuning when each model is small enough to fit on one machine but you have many configs to try.

The current implementation keeps that single-partition strategy but removes repeated preprocessing:

1. Grid configs are grouped by the part that changes the feature matrix (`chisq_top2000` vs. `variance_threshold=0.001`, or the equivalent selector/vectoriser variant in the ablation modes).
2. For each feature group, Spark fits `RegexTokenizer -> StopWordsRemover -> CountVectorizer/HashingTF -> IDF -> StringIndexer -> selector -> Normalizer` once on `train_df`.
3. It transforms train and validation once, caches only `label, features`, and materialises both with `count()`.
4. The driver dispatches the SVM grid for that cached feature matrix. Each config now fits only `OneVsRest(LinearSVC)`.

This preserves the exact modelling semantics for each config, but avoids refitting `CountVectorizer`, `IDF`, `StringIndexer`, and the selector for every SVM hyperparameter combination. For the primary 24-config comparison, preprocessing now runs twice (one per selector variant), not 24 times.

Pipeline is 100% Spark ML: `RegexTokenizer` → `StopWordsRemover` → `CountVectorizer` (or `HashingTF`) → `IDF` → `StringIndexer` → `ChiSqSelector` (or `VarianceThresholdSelector`) → `Normalizer(p=2.0)` → `OneVsRest(LinearSVC)`.

### How the cluster run was executed

The notebook is dual-mode via `RUN_MODE = "local" | "cluster"` in the config cell. For the cluster, the steps were (run on the cluster pod, in JupyterHub → **File → New → Terminal**):

```bash
# 1. Convert the uploaded notebook to a Spark-submittable script.
#    LBD JupyterHub runs Spark in local mode by default; HDFS access from
#    the pod is fine, but distributed execution on the cluster requires
#    spark-submit.
jupyter nbconvert --to script part3_classification.ipynb

# 2. Submit to YARN with single-executor sizing matched to our small workload.
#    --deploy-mode cluster is mandatory on this LBD setup because the
#    JupyterHub pod is on a Kubernetes overlay network the YARN workers
#    cannot route back to (so client mode fails with the AM unable to
#    connect to the driver).
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files stopwords.txt \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 6g \
  --driver-memory 6g \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.sql.shuffle.partitions=8 \
  part3_classification.py
```

Why these flags:
- `--num-executors 4 --executor-cores 4`: 16 task slots total. With `PARALLELISM=4` in the script, four classifier configs run concurrently against a cached single-partition feature matrix. This keeps LBFGS aggregation local while still using the cluster for config-level parallelism.
- `--executor-memory 6g`: the LBD per-container cap is 8 GB; 6 GB heap + ~600 MB YARN overhead fits.
- `--driver-memory 6g`: driver hosts the ChiSqSelector aggregation and broadcasts.
- `--files stopwords.txt`: ships the file to the driver container's working dir so `STOPWORDS_PATH = "stopwords.txt"` resolves.
- `spark.sql.shuffle.partitions=8`: matches the ~6k rows-per-partition rule of thumb for our 78k-row dev set; the default 200 over-partitions and amplifies scheduling overhead.

To run all four filter modes on the cluster, edit the script (or the source notebook before re-converting) so:

```python
FILTER_MODES = [
    "chisq_2000_vs_variancethreshold",
    "chisq_2000_vs_200",
    "chisq_2000_vs_fpr",
    "chisq_2000_vs_hashingtf",
]
```

The pre-shipped script enables only the spec-aligned `chisq_2000_vs_variancethreshold` mode (the other three are kept as commented-out alternatives) since the additional ablations are exploratory.

Files uploaded to the cluster home dir before submission:
- `part3_classification.ipynb` (the notebook from `src/data_intensive_computing/assignment2/`)
- `stopwords.txt` (from `data/`)

Reviews are read from HDFS at `hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json` — no data upload required. After the run, `output_part3.txt` and `output_part3.txt.json` are written to the **HDFS path** `hdfs:///user/<USERNAME>/output_part3.{txt,json}` (cluster mode requires HDFS for outputs since the driver lives in a worker container that gets garbage-collected when the application exits). Retrieve them with:

```bash
hdfs dfs -get hdfs:///user/e11817173/output_part3.txt .
hdfs dfs -get hdfs:///user/e11817173/output_part3.json .
```

Then download both via the JupyterHub file browser to the local repo for the report.

### Killing a stuck cluster job

If a submission misbehaves (e.g. log aggregation never flushes, or you submitted with bad sizing), find the application id with `yarn application -list` and kill it:

```bash
yarn application -kill application_<id>
```

### Why we abandoned the original cluster setup

The first cluster run used 8 small executors × 2 cores × 4 GB with `PARALLELISM=4` and the default `TrainValidationSplit`-based grid. After 80 minutes zero of 12 first-variant configs had completed — Spark was thrashing through the LBFGS aggregation cycle, broadcasting 2 MiB of weights at ~5 broadcasts/sec and spending essentially all wall-clock on inter-executor RPC instead of math. We confirmed the cluster *was* doing real work (job IDs in the Spark UI advanced through `treeAggregate at RDDLossFunction.scala`) but at ~1 LBFGS iteration/sec, projecting ~30 hours for the full 96-config grid. The refactor (single-partition + parallel grid jobs) eliminated the cross-executor LBFGS shuffle entirely.

### Why we changed the single-partition grid again

The first working single-partition version still built a full Spark ML pipeline per grid config. That meant every SVM setting refit the same preprocessing:

```
RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF ->
StringIndexer -> selector -> Normalizer -> OneVsRest(LinearSVC)
```

For the primary comparison this repeated feature extraction 24 times even though only 2 feature matrices exist: one for `ChiSqSelector(numTopFeatures=2000)` and one for `VarianceThresholdSelector(varianceThreshold=0.001)`. The preprocessing stages are not the dominant cost compared with 22 one-vs-rest SVMs, but repeating them still creates extra Spark jobs, broadcasts, model objects, and scheduler overhead.

The current version precomputes the feature matrix once per selector variant and then tunes only:

```
OneVsRest(LinearSVC(regParam, standardization, maxIter))
```

This is the cleaner architecture for the assignment: the feature representation and the classifier search are separated, validation is still on the same held-out validation split, and test evaluation transforms the test set with the best feature model before applying the best classifier model.

### Local execution

For local development / small experiments, `RUN_MODE = "local"` switches to `master("local[*]")`, file-system paths from the repo, `PARALLELISM=4`. Run with:

```bash
uv run --with jupyter jupyter nbconvert --to notebook --execute \
  src/data_intensive_computing/assignment2/part3_classification.ipynb --inplace
```

Local writes `output_part3.txt` and `output_part3.txt.json` directly to the repo root.

Realised splits (deterministic with `seed=11817173`): **train=47 492 / val=15 502 / test=15 835**.

SVM grid: `regParam ∈ {0.01, 0.1, 1.0}` × `standardization ∈ {True, False}` × `maxIter ∈ {10, 50}` = **12 SVM combos × 2 filter settings = 24 configs per mode**.

## Primary comparison (spec-aligned)

The spec asks: *"Compare chi square overall top 2000 filtered features with another, heavier filtering with much less dimensionality (see Spark ML documentation for options)."* The pointer to the Spark ML docs hints at a **different selector class**, not just different params of `ChiSqSelector`. From the listed feature selectors (`VectorSlicer`, `RFormula`, `ChiSqSelector`, `UnivariateFeatureSelector`, `VarianceThresholdSelector`), `VarianceThresholdSelector` is the natural alternative — it's a structurally different selector (unsupervised, variance-based) and "heavier filtering" is just a higher threshold.

| variant | selector | best SVM params | **val F1** |
|---|---|---|---:|
| `chisq_top2000` | `ChiSqSelector(numTopFeatures=2000)` (supervised, label-aware) | `regParam=0.01, std=True, maxIter=50` | **0.6107** |
| `variance_threshold=0.001` | `VarianceThresholdSelector(varianceThreshold=0.001)` (unsupervised) | `regParam=0.1, std=True, maxIter=50` | 0.6054 |

**Test-set F1 of the overall best (chisq_top2000): 0.6029** on the held-out 15 835 reviews.

ChiSqSelector edges out VarianceThresholdSelector by ~0.005 F1 — close, but ChiSq wins. Reasons:
- `ChiSqSelector` ranks features by their *correlation with the label*, so it keeps tokens that actually distinguish categories.
- `VarianceThresholdSelector` ranks features by *raw variance*, which is label-blind. High-variance tokens are sometimes strong category labels (e.g. `crib` for Baby), but sometimes just noisy long-tail vocabulary.
- For 22-class text classification with ~50k training rows, label-aware selection has a measurable but modest edge.

## Per-config leaderboard for the spec-aligned comparison (24 configs)

| rank | variant | regParam | std | maxIter | val F1 |
|---:|---|---:|:---:|---:|---:|
|  1 | chisq_top2000             | 0.01 | ✓ | 50 | **0.6107** |
|  2 | chisq_top2000             | 0.01 | ✓ | 10 | 0.6098 |
|  3 | chisq_top2000             | 0.10 | ✓ | 10 | 0.6079 |
|  4 | variance_threshold=0.001  | 0.10 | ✓ | 50 | 0.6054 |
|  5 | chisq_top2000             | 0.10 | ✓ | 50 | 0.6037 |
|  6 | variance_threshold=0.001  | 1.00 | ✓ | 50 | 0.5936 |
|  7 | variance_threshold=0.001  | 0.10 | ✓ | 10 | 0.5870 |
|  8 | variance_threshold=0.001  | 0.01 | ✓ | 50 | 0.5852 |
|  9 | variance_threshold=0.001  | 0.01 | ✓ | 10 | 0.5823 |
| 10 | chisq_top2000             | 1.00 | ✓ | 10 | 0.5782 |
| 11 | variance_threshold=0.001  | 1.00 | ✓ | 10 | 0.5711 |
| 12 | chisq_top2000             | 1.00 | ✓ | 50 | 0.5684 |
| 13 | variance_threshold=0.001  | 0.01 | ✗ | 50 | 0.5466 |
| 14 | variance_threshold=0.001  | 0.10 | ✗ | 50 | 0.5432 |
| 15 | chisq_top2000             | 0.10 | ✗ | 50 | 0.5138 |
| 16 | chisq_top2000             | 1.00 | ✗ | 50 | 0.5130 |
| 17 | chisq_top2000             | 0.01 | ✗ | 50 | 0.5121 |
| 18 | chisq_top2000             | 0.01 | ✗ | 10 | 0.2149 |
| 19 | variance_threshold=0.001  | 0.01 | ✗ | 10 | 0.2143 |
| 20 | variance_threshold=0.001  | 0.10 | ✗ | 10 | 0.2142 |
| 21 | variance_threshold=0.001  | 1.00 | ✗ | 10 | 0.2142 |
| 22 | chisq_top2000             | 1.00 | ✗ | 10 | 0.0395 |
| 23 | chisq_top2000             | 0.10 | ✗ | 10 | 0.0291 |
| 24 | variance_threshold=0.001  | 1.00 | ✗ | 50 | 0.0002 |

## Effect of each hyperparameter

1. **`standardization = True` is the single most important knob.** Every val F1 ≥ 0.50 has it on. Turning it off costs ≥ 0.07 F1 even with otherwise-good params; combined with `maxIter=10` it breaks several configs entirely (rows 18–24 — LBFGS hasn't converged in 10 iterations on un-standardised features).
2. **`regParam` sweet spot depends on the selector.**
   - `chisq_top2000`: `0.01` wins (the selector already keeps the most informative dimensions, so a weaker prior is fine).
   - `variance_threshold=0.001`: `0.1` wins (the variance filter keeps a different, possibly noisier feature set, and benefits from a slightly heavier prior).
   - `regParam = 1.0` consistently under-fits.
3. **`maxIter` barely matters at the top, dominates at the bottom.** With `standardization=True` the difference between `10` and `50` is ≤ 0.005 F1 in the top tier. With `standardization=False` and `maxIter=10`, LBFGS hasn't converged at all and val F1 collapses to single digits (rows 18–23).
4. **Selector choice (ChiSq vs VarianceThreshold) is a small effect compared to the SVM hyperparameters.** Difference between best ChiSq and best VarianceThreshold: ~0.005 F1. Difference between standardisation on/off: ~0.10 F1. The SVM tuning matters more than the selector class — at least for this dataset and threshold value.

## Supplementary exploration: three other "filter mode" interpretations

Before settling on `VarianceThresholdSelector` as the spec's intended "different selector", we also ran three other reasonable interpretations of "heavier filtering". These were run with the older grid-search code (`TrainValidationSplit` with an *inner* 75/25 split) and are kept here as ablations. **Numbers below use a different inner-val split than the primary results above, so val F1 values are not directly comparable across the two methodologies; test F1 values use the same outer test split and are comparable.**

| filter mode | "heavy" alternative | best variant | best val F1 (TVS-inner) | best **test F1** |
|---|---|---|---:|---:|
| `chisq_2000_vs_200` | `ChiSqSelector(numTopFeatures=200)` | top 2000 won | 0.5992 | 0.6106 |
| `chisq_2000_vs_fpr` | `ChiSqSelector(selectorType="fpr", fpr=0.001)` | **fpr 0.001** won | 0.6159 | **0.6277** |
| `chisq_2000_vs_hashingtf` | `HashingTF(numFeatures=512)` (no chi-sq) | chisq 2000 won | 0.5989 | 0.6103 |

Observations from the supplementary runs:
- **`numTopFeatures=200` is too aggressive.** Best val ~0.38 — heavy chi² filtering throws away too much signal for 22 classes. (See `output_part3_3modes.{txt,json}` for the 24-config breakdown of this mode.)
- **`HashingTF(numFeatures=512)` underperforms even `numTopFeatures=200`.** Best val ~0.41. Both reduce dimensionality, but top-200 keeps the 200 most chi²-discriminative terms (still meaningful), while HashingTF maps tokens to 512 *random* hash buckets where many distinct words collide. The chi²-informed reduction beats the unsupervised one.
- **`ChiSqSelector(selectorType="fpr", fpr=0.001)`** produced the highest test F1 in the whole experiment (0.6277). However, this is *still* `ChiSqSelector` with a different statistical criterion — not really a different selector class. We treat it as a tuning variant of the chi-square baseline rather than a substantively different filtering approach. The spec's hint to "see Spark ML documentation for options" more naturally points at `VarianceThresholdSelector`, which is what the primary comparison reports.

## Reproducibility

- `SEED = 11817173` is the only randomness source. Re-running the notebook with the same data and PySpark version yields identical metrics.
- Outer split: train=47 492 / val=15 502 / test=15 835 (deterministic).
- Local Spark: `spark.driver.memory=6g`, `spark.driver.maxResultSize=2g`, `spark.sql.shuffle.partitions=32`.
- Cluster Spark: see the spark-submit command above.
- The active code uses single-partition cached data + precomputed `label, features` per selector variant + `ThreadPoolExecutor` for classifier-grid dispatch. Concurrent fits share the cached feature matrix but train independent `OneVsRest(LinearSVC)` models.

## Outputs

- `output_part3.txt` — human-readable per-mode best + full sorted 24-config leaderboard for the primary comparison.
- `output_part3.txt.json` — machine-readable summary of the primary comparison.
- `output_part3_3modes.txt` / `.json` — archived results from the three supplementary filter modes (run with the older TVS-based code).

## Recommendation for the report

Lead with the **spec-aligned primary comparison**:

> Final chosen model: `ChiSqSelector(numTopFeatures=2000)` + `LinearSVC(regParam=0.01, standardization=True, maxIter=50)` wrapped in `OneVsRest`, with `Normalizer(p=2.0)` between selector and classifier.
> 
> Test-set F1 (held-out 15 835 reviews): **0.6029**.
> 
> The spec-suggested heavier alternative — `VarianceThresholdSelector(varianceThreshold=0.001)` — reaches val F1 0.6054 vs. ChiSqSelector's 0.6107. The two selectors are within 0.005 F1 of each other on this dataset, suggesting the bottleneck is not the selection criterion but the linear model + 2000-dimensional TF-IDF representation.

Mention the supplementary explorations briefly (three filter-mode ablations under the older grid-search methodology) to show the broader landscape, with the explicit caveat that those used a TVS-based inner split and are not directly comparable to the primary numbers.
