# %% [markdown]
# # Assignment 2 -- Part 3: Multi-class SVM Text Classification
#
# Extends Part 2 by training a Support Vector Machine to predict the product category from a review's text.
#
# Pipeline:
# 1. `RegexTokenizer` + `StopWordsRemover` -- same delimiter regex / stopwords as Part 2.
# 2. `CountVectorizer` + `IDF` vectorisation.
# 3. Feature filtering comparison required by the assignment:
#    - `chisq_top2000`: `ChiSqSelector(numTopFeatures=2000)`
#    - `variance_threshold=0.001`: `VarianceThresholdSelector(varianceThreshold=0.001)`
# 4. `Normalizer(p=2.0)` -- L2 length normalisation before the classifier.
# 5. `StringIndexer` on the category label.
# 6. `OneVsRest(LinearSVC)` -- multi-class via one-vs-rest over the binary linear SVM.
#
# Experimental design:
# - 60 / 20 / 20 split into train / validation / test, fixed seed (`SEED=11817173`).
# - Manual grid search: fit/cache `label, features` once per selector variant, then dispatch `OneVsRest(LinearSVC)` configs with `ThreadPoolExecutor`.
# - SVM grid: `regParam ∈ {0.01, 0.1, 1.0}` × `standardization ∈ {True, False}` × `maxIter ∈ {10, 50}` = 12 SVM combos per selector, 24 configs total.
# - Metric: `MulticlassClassificationEvaluator(metricName="f1")`.
# - Dataset: `reviews_devset.json` -- per the spec, the development set is the evaluation target throughout.
#
# Run mode is chosen via the `RUN_MODE` toggle in the config cell. Cluster execution is intended through `spark-submit --master yarn --deploy-mode cluster`.
#
# Output: `output_part3.txt` summarising the best configuration and the test-set F1, plus a JSON dump for downstream plotting.


# %%
from __future__ import annotations

import json
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    ChiSqSelector,
    CountVectorizer,
    IDF,
    Normalizer,
    RegexTokenizer,
    StopWordsRemover,
    StringIndexer,
    VarianceThresholdSelector,
)


# %% [markdown]
# ## Configuration
#
# The active run uses the assignment-required comparison `chisq_top2000` vs `variance_threshold=0.001`. `PARALLELISM` controls how many SVM configs are submitted concurrently within each selector variant.


# %%
# Cluster-ready configuration. To run locally, flip RUN_MODE = "local".
RUN_MODE = "cluster"

if RUN_MODE == "cluster":
    INPUT_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
    STOPWORDS_PATH = "stopwords.txt"   # uploaded next to the script on the pod
    HDFS_USER = "e11817173"
    OUTPUT_PATH = f"hdfs:///user/{HDFS_USER}/output_part3.txt"
    OUTPUT_JSON_PATH = f"hdfs:///user/{HDFS_USER}/output_part3.json"
    PARALLELISM = 12
else:
    REPO_ROOT = Path.cwd()
    while REPO_ROOT != REPO_ROOT.parent and not (REPO_ROOT / "pyproject.toml").exists():
        REPO_ROOT = REPO_ROOT.parent
    INPUT_PATH = str(REPO_ROOT / "data" / "reviews_devset.json")
    STOPWORDS_PATH = str(REPO_ROOT / "data" / "stopwords.txt")
    OUTPUT_PATH = str(REPO_ROOT / "output_part3.txt")
    OUTPUT_JSON_PATH = OUTPUT_PATH + ".json"
    PARALLELISM = 4

SEED = 11817173
TRAIN_FRAC, VAL_FRAC, TEST_FRAC = 0.6, 0.2, 0.2
FILTER_MODE = "chisq_2000_vs_variancethreshold"
SEARCH_STRATEGY = "manual_precomputed_features_grid"

REG_PARAMS = [0.01, 0.1, 1.0]
STANDARDIZATIONS = [True, False]
MAX_ITERS = [10, 50]
VARIANCE_THRESHOLD = 0.001

INPUT_PATH, STOPWORDS_PATH, OUTPUT_PATH, OUTPUT_JSON_PATH, SEED, FILTER_MODE, SEARCH_STRATEGY, RUN_MODE


# %%
if RUN_MODE == "cluster":
    # On the cluster: rely entirely on spark-submit flags (--master, --num-executors,
    # --executor-cores, --executor-memory, --driver-memory, etc.). Setting these
    # via SparkSession.builder.config(...) AFTER the driver JVM has already started
    # is a no-op for driver-side options and a footgun for executor-side options
    # that may conflict with the submit flags.
    spark = (
        SparkSession.builder
        .appName("assignment2-part3-classification")
        .getOrCreate()
    )
else:
    spark = (
        SparkSession.builder
        .appName("assignment2-part3-classification")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("WARN")
spark

# %% [markdown]
# ## Load reviews and split
#
# Read the NDJSON file, drop rows with no category, and split 60 / 20 / 20 with the fixed seed. The combined `train + val` portion feeds the grid search; `test` is touched only at the end.

# %%
reviews = (
    spark.read.json(INPUT_PATH)
         .select("category", F.coalesce(F.col("reviewText"), F.lit("")).alias("text"))
         .where(F.col("category").isNotNull())
)
reviews.cache()
total = reviews.count()

train_df, val_df, test_df = reviews.randomSplit(
    [TRAIN_FRAC, VAL_FRAC, TEST_FRAC], seed=SEED
)
trainval_df = train_df.unionByName(val_df).cache()
_ = trainval_df.count()

train_n, val_n, test_n = train_df.count(), val_df.count(), test_df.count()
print(f"total={total}  train={train_n}  val={val_n}  test={test_n}")

# %%
with open(STOPWORDS_PATH, encoding="utf-8") as fh:
    stopwords = [line.strip() for line in fh if line.strip()]
len(stopwords)

# %% [markdown]
# ## Feature Pipeline Helpers
#
# The active experiment compares exactly two selector variants: chi-square top 2000 features versus variance-threshold filtering. Shared text preprocessing, label indexing, vectorisation, IDF, and L2 normalisation are fitted once per selector variant before the SVM grid runs.


# %%
DELIMITER_REGEX = r"[\s\d()\[\]{}.!?,;:+=_\"'`~#@&*%€$§\\/-]+"


def shared_text_stages():
    tokenizer = RegexTokenizer(
        inputCol="text", outputCol="tokens_raw",
        pattern=DELIMITER_REGEX, gaps=True,
        toLowercase=True, minTokenLength=2,
    )
    remover = StopWordsRemover(
        inputCol="tokens_raw", outputCol="tokens",
        stopWords=stopwords, caseSensitive=False,
    )
    label_indexer = StringIndexer(
        inputCol="category", outputCol="label",
        handleInvalid="skip",
    )
    return tokenizer, remover, label_indexer


# %% [markdown]
# ## Run the Grid Search
#
# Each selector variant has a stable feature matrix, so we precompute both feature matrices first and then run the full SVM grid against the cached single-partition features:
#
# - `train_df` and `val_df` are `coalesce(1).cache()`-d once. With one partition, LBFGS tree aggregation stays local instead of shuffling across executors for this small dataset.
# - Feature extraction is fitted once per selector variant and cached as single-partition `label, features`. Both selector variants stay cached while the classifier grid runs.
# - All 24 classifier configs are submitted to one driver-side `ThreadPoolExecutor`, so workers do not sit idle at the boundary between `chisq_top2000` and `variance_threshold=0.001`.
# - The manual loop uses the explicit train/validation/test split from the assignment and avoids repeatedly fitting identical preprocessing stages for every SVM parameter combination.
#
# This trades cross-executor data parallelism for cross-config classifier parallelism, while avoiding repeated vectoriser/IDF/selector fits.


# %%
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1",
)


def _materialise_single_partition(df):
    """Coalesce to one partition, cache, and force materialisation."""
    out = df.coalesce(1).cache()
    out.count()
    return out


def _svm_configs():
    return [
        {"regParam": rp, "standardization": std, "maxIter": mi}
        for rp, std, mi in product(REG_PARAMS, STANDARDIZATIONS, MAX_ITERS)
    ]


def _enumerate_grid():
    svm_configs = _svm_configs()
    variants = ["chisq_top2000", f"variance_threshold={VARIANCE_THRESHOLD}"]
    return [(variant, dict(params)) for variant in variants for params in svm_configs]


def _feature_key(variant):
    return (FILTER_MODE, variant)


def _feature_pipeline(variant):
    """Pipeline up to cached `label, features`; classifier params are excluded."""
    tokenizer, remover, label_indexer = shared_text_stages()
    cv = CountVectorizer(inputCol="tokens", outputCol="tf")
    idf = IDF(inputCol="tf", outputCol="tfidf")

    if variant == "chisq_top2000":
        selector = ChiSqSelector(
            featuresCol="tfidf", labelCol="label",
            outputCol="selectedFeatures", numTopFeatures=2000,
        )
    elif variant == f"variance_threshold={VARIANCE_THRESHOLD}":
        selector = VarianceThresholdSelector(
            featuresCol="tfidf", outputCol="selectedFeatures",
            varianceThreshold=VARIANCE_THRESHOLD,
        )
    else:
        raise ValueError(f"unknown selector variant: {variant}")

    normalizer = Normalizer(inputCol="selectedFeatures", outputCol="features", p=2.0)
    return Pipeline(stages=[
        tokenizer, remover, cv, idf, label_indexer, selector, normalizer,
    ])


def _cache_features(feature_model, df):
    features = feature_model.transform(df).select("label", "features").coalesce(1).cache()
    features.count()
    return features


def fit_one_classifier(config_id, total_configs, variant, params, train_features, val_features):
    """Fit only OneVsRest(LinearSVC) on precomputed `label, features`."""
    print(
        f"  starting config {config_id}/{total_configs} for {variant}: "
        f"regParam={params['regParam']} "
        f"standardization={params['standardization']} "
        f"maxIter={params['maxIter']}",
        flush=True,
    )
    t0 = time.time()
    svm = LinearSVC(
        featuresCol="features", labelCol="label",
        regParam=params["regParam"],
        standardization=params["standardization"],
        maxIter=params["maxIter"],
    )
    ovr = OneVsRest(classifier=svm, featuresCol="features", labelCol="label")
    model = ovr.fit(train_features)
    val_pred = model.transform(val_features)
    val_f1 = float(evaluator.evaluate(val_pred))
    elapsed = time.time() - t0
    return {"variant": variant, "val_f1": val_f1, **params}, model, elapsed, config_id


def run_one(filter_mode):
    if filter_mode != FILTER_MODE:
        raise ValueError(f"unsupported filter mode: {filter_mode}")

    print(f"\n=== Running filter_mode={filter_mode} ===", flush=True)
    t0 = time.time()
    grid = _enumerate_grid()

    feature_groups = {}
    for variant, params in grid:
        feature_groups.setdefault(_feature_key(variant), []).append((variant, params))

    print(
        f"  configs={len(grid)}  feature_sets={len(feature_groups)}  "
        f"parallelism={PARALLELISM}",
        flush=True,
    )

    train_1p = _materialise_single_partition(train_df)
    val_1p = _materialise_single_partition(val_df)

    rows = []
    best = None
    feature_sets = {}
    try:
        for key, configs in feature_groups.items():
            variant0, _ = configs[0]
            print(f"  fitting features {key} for {len(configs)} classifier configs", flush=True)
            feature_model = _feature_pipeline(variant0).fit(train_1p)
            train_features = _cache_features(feature_model, train_1p)
            val_features = _cache_features(feature_model, val_1p)
            feature_sets[key] = {
                "feature_model": feature_model,
                "train_features": train_features,
                "val_features": val_features,
                "configs": configs,
            }
            print(f"  cached features {key}", flush=True)

        submitted = []
        for key, feature_info in feature_sets.items():
            for variant, params in feature_info["configs"]:
                submitted.append((key, variant, params))

        with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
            futures = {}
            for config_id, (key, variant, params) in enumerate(submitted, start=1):
                feature_info = feature_sets[key]
                fut = pool.submit(
                    fit_one_classifier,
                    config_id, len(submitted),
                    variant, params,
                    feature_info["train_features"],
                    feature_info["val_features"],
                )
                futures[fut] = (config_id, key)

            print(f"  submitted {len(futures)} classifier configs across all feature sets", flush=True)
            for finished, fut in enumerate(as_completed(futures), start=1):
                _, key = futures[fut]
                row, classifier_model, fit_seconds, config_id = fut.result()
                row["filter_mode"] = filter_mode
                row["fit_seconds"] = fit_seconds
                rows.append(row)
                print(
                    f"  finished config {finished}/{len(submitted)} "
                    f"(id={config_id}/{len(submitted)}) for {row['variant']}: "
                    f"val_f1={row['val_f1']:.4f} "
                    f"regParam={row['regParam']} "
                    f"standardization={row['standardization']} "
                    f"maxIter={row['maxIter']} "
                    f"elapsed={fit_seconds:.1f}s "
                    f"remaining={len(submitted) - finished}",
                    flush=True,
                )
                if best is None or row["val_f1"] > best["val_f1"]:
                    best = {
                        "variant": row["variant"],
                        "val_f1": row["val_f1"],
                        "params": {
                            k: v for k, v in row.items()
                            if k not in {"variant", "val_f1", "filter_mode", "fit_seconds"}
                        },
                        "feature_model": feature_sets[key]["feature_model"],
                        "classifier_model": classifier_model,
                    }
    finally:
        for feature_info in feature_sets.values():
            feature_info["train_features"].unpersist()
            feature_info["val_features"].unpersist()
        train_1p.unpersist()
        val_1p.unpersist()

    elapsed = time.time() - t0
    print(f"  done in {elapsed:.1f}s  best_val_f1={best['val_f1']:.4f}", flush=True)
    return rows, best, elapsed


# %% [markdown]
# ## Fit each filter mode

# %%
all_rows = []
best_by_mode = {}
for mode in [FILTER_MODE]:
    rows, best, elapsed = run_one(mode)
    all_rows.extend(rows)
    best_by_mode[mode] = (best, elapsed)


# %% [markdown]
# ## Per-mode summary + test-set F1
#
# Score each mode's best model on the held-out `test_df`.

# %%
summary_per_mode = {}
for mode, (best, elapsed) in best_by_mode.items():
    test_features = best["feature_model"].transform(test_df).select("label", "features")
    test_pred = best["classifier_model"].transform(test_features)
    test_f1 = float(evaluator.evaluate(test_pred))
    summary_per_mode[mode] = {
        "variant": best["variant"],
        "best_val_f1": best["val_f1"],
        "test_f1": test_f1,
        "best_params": best["params"],
        "elapsed_seconds": elapsed,
    }
    print(
        f"{mode:<28}  variant={best['variant']:<14}  "
        f"val_f1={best['val_f1']:.4f}  test_f1={test_f1:.4f}  "
        f"params={best['params']}  ({elapsed:.1f}s)",
        flush=True,
    )


# %% [markdown]
# ## All grid rows (sorted by validation F1)

# %%
all_rows_sorted = sorted(all_rows, key=lambda r: -r["val_f1"])
for r in all_rows_sorted[:30]:
    rest = {k: v for k, v in r.items() if k not in {"val_f1", "filter_mode"}}
    print(f"  val_f1={r['val_f1']:.4f}  mode={r['filter_mode']:<28}  {rest}")
print(f"\n... ({len(all_rows_sorted)} rows total)")

# %% [markdown]
# ## Write summary

# %%
summary = {
    "seed": SEED,
    "search_strategy": SEARCH_STRATEGY,
    "filter_mode": FILTER_MODE,
    "split": {"train": train_n, "val": val_n, "test": test_n},
    "per_mode": summary_per_mode,
    "all_results": all_rows_sorted,
}

lines = [
    f"seed: {SEED}",
    f"search_strategy: {SEARCH_STRATEGY}",
    f"filter_mode: {FILTER_MODE}",
    f"split: train={train_n} val={val_n} test={test_n}",
    "",
    "best per filter mode:",
]
for mode, info in summary_per_mode.items():
    lines.append(
        f"  {mode:<28}  variant={info['variant']:<24}  "
        f"val_f1={info['best_val_f1']:.4f}  test_f1={info['test_f1']:.4f}  "
        f"params={info['best_params']}"
    )
lines.append("")
lines.append("all configs (sorted by val F1):")
for r in all_rows_sorted:
    rest = {k: v for k, v in r.items() if k not in {"val_f1", "filter_mode"}}
    lines.append(
        f"  val_f1={r['val_f1']:.4f}  mode={r['filter_mode']:<28}  {rest}"
    )

txt_content = "\n".join(lines) + "\n"
json_content = json.dumps(summary, indent=2, default=str) + "\n"


def _write_local(path, content):
    Path(path).write_text(content, encoding="utf-8")


def _write_hdfs(spark, path, content):
    """Write a single text file to an HDFS path via the Hadoop FileSystem API."""
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    Path_jvm = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    p = Path_jvm(path)
    fs = FileSystem.get(p.toUri(), hadoop_conf)
    out = fs.create(p, True)
    try:
        out.write(content.encode("utf-8"))
    finally:
        out.close()


if RUN_MODE == "cluster":
    _write_hdfs(spark, OUTPUT_PATH, txt_content)
    _write_hdfs(spark, OUTPUT_JSON_PATH, json_content)
    print(f"Wrote {OUTPUT_PATH} and {OUTPUT_JSON_PATH} (HDFS).")
    print(
        "Retrieve locally with:\n"
        f"  hdfs dfs -get {OUTPUT_PATH} .\n"
        f"  hdfs dfs -get {OUTPUT_JSON_PATH} ."
    )
else:
    _write_local(OUTPUT_PATH, txt_content)
    _write_local(OUTPUT_JSON_PATH, json_content)
    print(f"Wrote {OUTPUT_PATH} and {OUTPUT_JSON_PATH} (local).")


# %%
reviews.unpersist()
trainval_df.unpersist()
spark.stop()
