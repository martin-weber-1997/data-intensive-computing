# %% [markdown]
# # Assignment 2 -- Part 3 Full Dataset Best-Config Run
#
# This notebook is a separate full-dataset experiment. It does **not** perform grid search. It uses the best configuration found on the development set:
#
# - `ChiSqSelector(numTopFeatures=2000)`
# - `LinearSVC(regParam=0.01, standardization=True, maxIter=50)`
# - `OneVsRest` for multi-class category prediction
# - `Normalizer(p=2.0)` before the classifier
#
# Unlike the development-set grid notebook, this full-dataset run does **not** coalesce training data to one partition. On the full dataset, forcing one partition would serialize too much data onto one executor. Here Spark is allowed to keep the feature/training data distributed.


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
)


# %% [markdown]
# ## Configuration
#
# Cluster mode reads the full combined reviews dataset from HDFS. Local mode still points at the small development file because the full dataset is not expected to exist locally.


# %%
RUN_MODE = "cluster"

if RUN_MODE == "cluster":
    INPUT_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json"
    STOPWORDS_PATH = "stopwords.txt"
    HDFS_USER = "e11817173"
    OUTPUT_PATH = f"hdfs:///user/{HDFS_USER}/output_part3_full_best.txt"
    OUTPUT_JSON_PATH = f"hdfs:///user/{HDFS_USER}/output_part3_full_best.json"
else:
    REPO_ROOT = Path.cwd()
    while REPO_ROOT != REPO_ROOT.parent and not (REPO_ROOT / "pyproject.toml").exists():
        REPO_ROOT = REPO_ROOT.parent
    INPUT_PATH = str(REPO_ROOT / "data" / "reviews_devset.json")
    STOPWORDS_PATH = str(REPO_ROOT / "data" / "stopwords.txt")
    OUTPUT_PATH = str(REPO_ROOT / "output_part3_full_best.txt")
    OUTPUT_JSON_PATH = OUTPUT_PATH + ".json"

SEED = 11817173
TRAIN_FRAC, VAL_FRAC, TEST_FRAC = 0.6, 0.2, 0.2

BEST_VARIANT = "chisq_top2000"
BEST_PARAMS = {"regParam": 0.01, "standardization": True, "maxIter": 50}
OVR_PARALLELISM = 4

INPUT_PATH, STOPWORDS_PATH, OUTPUT_PATH, OUTPUT_JSON_PATH, BEST_VARIANT, BEST_PARAMS, OVR_PARALLELISM


# %%
builder = SparkSession.builder.appName("Assignment2-Part3-Full-BestConfig")
if RUN_MODE == "local":
    builder = builder.master("local[*]")
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("INFO")
spark


# %% [markdown]
# ## Load and Split Data
#
# The best hyperparameters were already selected on the development set. For this full-dataset experiment, the model is fitted on `train + validation` and evaluated once on the held-out test split.


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
trainval_n = trainval_df.count()
train_n, val_n, test_n = train_df.count(), val_df.count(), test_df.count()
print(
    f"total={total} train={train_n} val={val_n} "
    f"trainval={trainval_n} test={test_n}",
    flush=True,
)


# %%
with open(STOPWORDS_PATH, encoding="utf-8") as fh:
    stopwords = [line.strip() for line in fh if line.strip()]
len(stopwords)


# %% [markdown]
# ## Best-Config Pipeline
#
# No grid search and no `coalesce(1)`. The full dataset should stay distributed. `OneVsRest(parallelism=4)` lets Spark train a few binary category-vs-rest classifiers concurrently without launching all category classifiers at once.


# %%
DELIMITER_REGEX = r"""[\s\d()\[\]{}.!?,;:+=_"'`~#@&*%€$§\/-]+"""


def build_best_pipeline():
    tokenizer = RegexTokenizer(
        inputCol="text", outputCol="tokens_raw",
        pattern=DELIMITER_REGEX, gaps=True,
        toLowercase=True, minTokenLength=2,
    )
    remover = StopWordsRemover(
        inputCol="tokens_raw", outputCol="tokens",
        stopWords=stopwords, caseSensitive=False,
    )
    cv = CountVectorizer(inputCol="tokens", outputCol="tf")
    idf = IDF(inputCol="tf", outputCol="tfidf")
    label_indexer = StringIndexer(
        inputCol="category", outputCol="label",
        handleInvalid="skip",
    )
    selector = ChiSqSelector(
        featuresCol="tfidf", labelCol="label",
        outputCol="selectedFeatures", numTopFeatures=2000,
    )
    normalizer = Normalizer(inputCol="selectedFeatures", outputCol="features", p=2.0)
    svm = LinearSVC(
        featuresCol="features", labelCol="label",
        regParam=BEST_PARAMS["regParam"],
        standardization=BEST_PARAMS["standardization"],
        maxIter=BEST_PARAMS["maxIter"],
    )
    ovr = OneVsRest(
        classifier=svm,
        featuresCol="features", labelCol="label",
        parallelism=OVR_PARALLELISM,
    )
    return Pipeline(stages=[
        tokenizer, remover, cv, idf, label_indexer,
        selector, normalizer, ovr,
    ])


pipeline = build_best_pipeline()
pipeline


# %% [markdown]
# ## Fit and Evaluate
#
# This is expected to be much heavier than the development-set run. The expensive part is `OneVsRest(LinearSVC)`: one binary SVM per category, each with iterative full-dataset scans.


# %%
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1",
)

fit_t0 = time.time()
print("fitting full-dataset best-config pipeline", flush=True)
model = pipeline.fit(trainval_df)
fit_seconds = time.time() - fit_t0
print(f"fit done in {fit_seconds:.1f}s", flush=True)

eval_t0 = time.time()
print("evaluating on held-out test split", flush=True)
test_pred = model.transform(test_df)
test_f1 = float(evaluator.evaluate(test_pred))
eval_seconds = time.time() - eval_t0
print(f"test_f1={test_f1:.4f} eval_seconds={eval_seconds:.1f}s", flush=True)


# %% [markdown]
# ## Write Output

# %%
summary = {
    "seed": SEED,
    "input_path": INPUT_PATH,
    "split": {
        "total": total,
        "train": train_n,
        "val": val_n,
        "trainval": trainval_n,
        "test": test_n,
    },
    "variant": BEST_VARIANT,
    "best_params_from_devset": BEST_PARAMS,
    "ovr_parallelism": OVR_PARALLELISM,
    "coalesce_training_data": False,
    "fit_seconds": fit_seconds,
    "eval_seconds": eval_seconds,
    "test_f1": test_f1,
}

lines = [
    f"seed: {SEED}",
    f"input_path: {INPUT_PATH}",
    f"split: total={total} train={train_n} val={val_n} trainval={trainval_n} test={test_n}",
    f"variant: {BEST_VARIANT}",
    f"params: {BEST_PARAMS}",
    f"ovr_parallelism: {OVR_PARALLELISM}",
    "coalesce_training_data: False",
    f"fit_seconds: {fit_seconds:.1f}",
    f"eval_seconds: {eval_seconds:.1f}",
    f"test_f1: {test_f1:.4f}",
]

txt_content = "\n".join(lines) + "\n"
json_content = json.dumps(summary, indent=2, default=str) + "\n"


def _write_local(path, content):
    Path(path).write_text(content, encoding="utf-8")


def _write_hdfs(spark, path, content):
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
    print(f"Wrote {OUTPUT_PATH} and {OUTPUT_JSON_PATH} (HDFS).", flush=True)
else:
    _write_local(OUTPUT_PATH, txt_content)
    _write_local(OUTPUT_JSON_PATH, json_content)
    print(f"Wrote {OUTPUT_PATH} and {OUTPUT_JSON_PATH} (local).", flush=True)


# %%
reviews.unpersist()
trainval_df.unpersist()
spark.stop()

