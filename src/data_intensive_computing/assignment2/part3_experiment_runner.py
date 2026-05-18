from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from pathlib import Path

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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DELIMITER_REGEX = r"""[\s\d()\[\]{}.!?,;:+=_"'`~#@&*%€$§\/-]+"""
SEED = 11817173
TRAIN_FRAC, VAL_FRAC, TEST_FRAC = 0.6, 0.2, 0.2


def parse_csv(raw: str, cast):
    return [cast(v.strip()) for v in raw.split(",") if v.strip()]


def parse_bool_csv(raw: str):
    values = []
    for value in raw.split(","):
        value = value.strip().lower()
        if not value:
            continue
        if value in {"1", "true", "t", "yes", "y"}:
            values.append(True)
        elif value in {"0", "false", "f", "no", "n"}:
            values.append(False)
        else:
            raise argparse.ArgumentTypeError(f"invalid bool value: {value!r}")
    return values


def build_parser():
    parser = argparse.ArgumentParser(
        description="Run small Part 3 scheduling/config experiments with detailed timings."
    )
    parser.add_argument("--run-mode", choices=["local", "cluster"], default="local")
    parser.add_argument("--input-path")
    parser.add_argument("--stopwords-path")
    parser.add_argument("--output-json", default="part3_experiment_result.json")
    parser.add_argument(
        "--schedule",
        choices=["sequential-groups", "all-configs-one-pool"],
        default="all-configs-one-pool",
        help=(
            "sequential-groups caches/runs one selector group at a time; "
            "all-configs-one-pool caches both feature groups first and submits all configs."
        ),
    )
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--coalesce", choices=["yes", "no"], default="yes")
    parser.add_argument("--sample-fraction", type=float, default=1.0)
    parser.add_argument("--reg-params", default="0.01,0.1,1.0")
    parser.add_argument("--standardizations", type=parse_bool_csv, default=parse_bool_csv("true,false"))
    parser.add_argument("--max-iters", default="10,50")
    parser.add_argument("--variants", default="chisq_top2000,variance_threshold")
    parser.add_argument("--variance-threshold", type=float, default=0.001)
    parser.add_argument("--ovr-parallelism", type=int, default=1)
    parser.add_argument(
        "--limit-configs",
        type=int,
        default=0,
        help="Debug only: keep only the first N configs after grid enumeration.",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=12)
    parser.add_argument("--driver-memory", default="6g")
    parser.add_argument("--log-level", default="WARN")
    return parser


def default_paths(run_mode: str):
    if run_mode == "cluster":
        return (
            "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json",
            "stopwords.txt",
        )

    repo_root = Path.cwd()
    while repo_root != repo_root.parent and not (repo_root / "pyproject.toml").exists():
        repo_root = repo_root.parent
    return (
        str(repo_root / "data" / "reviews_devset.json"),
        str(repo_root / "data" / "stopwords.txt"),
    )


def make_spark(args):
    builder = (
        SparkSession.builder.appName("assignment2-part3-experiment-runner")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    )
    if args.run_mode == "local":
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(args.log_level)
    return spark


def shared_text_stages(stopwords):
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="tokens_raw",
        pattern=DELIMITER_REGEX,
        gaps=True,
        toLowercase=True,
        minTokenLength=2,
    )
    remover = StopWordsRemover(
        inputCol="tokens_raw",
        outputCol="tokens",
        stopWords=stopwords,
        caseSensitive=False,
    )
    label_indexer = StringIndexer(
        inputCol="category",
        outputCol="label",
        handleInvalid="skip",
    )
    return tokenizer, remover, label_indexer


def feature_pipeline(variant: str, args, stopwords):
    tokenizer, remover, label_indexer = shared_text_stages(stopwords)
    cv = CountVectorizer(inputCol="tokens", outputCol="tf")
    idf = IDF(inputCol="tf", outputCol="tfidf")

    if variant == "chisq_top2000":
        selector = ChiSqSelector(
            featuresCol="tfidf",
            labelCol="label",
            outputCol="selectedFeatures",
            numTopFeatures=2000,
        )
    elif variant == "variance_threshold":
        selector = VarianceThresholdSelector(
            featuresCol="tfidf",
            outputCol="selectedFeatures",
            varianceThreshold=args.variance_threshold,
        )
    else:
        raise ValueError(f"unknown variant: {variant}")

    normalizer = Normalizer(inputCol="selectedFeatures", outputCol="features", p=2.0)
    return Pipeline(
        stages=[tokenizer, remover, cv, idf, label_indexer, selector, normalizer]
    )


def maybe_single_partition(df, args):
    if args.coalesce == "yes":
        return df.coalesce(1).cache()
    return df.cache()


def materialize(df, name: str):
    t0 = time.time()
    n = df.count()
    elapsed = time.time() - t0
    print(f"materialized {name}: rows={n} elapsed={elapsed:.1f}s", flush=True)
    return n, elapsed


def cache_features(variant: str, model, train_df, val_df, args):
    t0 = time.time()
    train_features = maybe_single_partition(
        model.transform(train_df).select("label", "features"), args
    )
    train_n, train_seconds = materialize(train_features, f"{variant}.train_features")
    val_features = maybe_single_partition(
        model.transform(val_df).select("label", "features"), args
    )
    val_n, val_seconds = materialize(val_features, f"{variant}.val_features")
    return {
        "train_features": train_features,
        "val_features": val_features,
        "cache_seconds": time.time() - t0,
        "train_rows": train_n,
        "val_rows": val_n,
        "train_materialize_seconds": train_seconds,
        "val_materialize_seconds": val_seconds,
    }


def grid(args):
    variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    reg_params = parse_csv(args.reg_params, float)
    max_iters = parse_csv(args.max_iters, int)
    configs = [
        (
            variant,
            {"regParam": reg, "standardization": std, "maxIter": max_iter},
        )
        for variant in variants
        for reg, std, max_iter in product(reg_params, args.standardizations, max_iters)
    ]
    if args.limit_configs:
        configs = configs[: args.limit_configs]
    return configs


def fit_one_classifier(config_id, total_configs, variant, params, feature_info, args, evaluator):
    print(
        f"starting config {config_id}/{total_configs} variant={variant} "
        f"regParam={params['regParam']} standardization={params['standardization']} "
        f"maxIter={params['maxIter']}",
        flush=True,
    )
    t0 = time.time()
    svm = LinearSVC(
        featuresCol="features",
        labelCol="label",
        regParam=params["regParam"],
        standardization=params["standardization"],
        maxIter=params["maxIter"],
    )
    ovr = OneVsRest(
        classifier=svm,
        featuresCol="features",
        labelCol="label",
        parallelism=args.ovr_parallelism,
    )
    model = ovr.fit(feature_info["train_features"])
    pred = model.transform(feature_info["val_features"])
    val_f1 = float(evaluator.evaluate(pred))
    elapsed = time.time() - t0
    print(
        f"finished config {config_id}/{total_configs} variant={variant} "
        f"val_f1={val_f1:.4f} elapsed={elapsed:.1f}s",
        flush=True,
    )
    return {
        "config_id": config_id,
        "variant": variant,
        "val_f1": val_f1,
        "fit_seconds": elapsed,
        **params,
    }


def precompute_feature_set(variant, configs, train_df, val_df, args, stopwords):
    print(f"fitting features variant={variant} configs={len(configs)}", flush=True)
    t0 = time.time()
    model = feature_pipeline(variant, args, stopwords).fit(train_df)
    feature_fit_seconds = time.time() - t0
    print(f"fit features variant={variant} elapsed={feature_fit_seconds:.1f}s", flush=True)
    info = cache_features(variant, model, train_df, val_df, args)
    info.update(
        {
            "variant": variant,
            "feature_model": model,
            "feature_fit_seconds": feature_fit_seconds,
            "configs": configs,
        }
    )
    print(
        f"cached features variant={variant} "
        f"feature_fit={feature_fit_seconds:.1f}s cache={info['cache_seconds']:.1f}s",
        flush=True,
    )
    return info


def run_sequential_groups(configs, train_df, val_df, args, stopwords, evaluator):
    rows = []
    next_config_id = 1
    by_variant = {}
    for variant, params in configs:
        by_variant.setdefault(variant, []).append(params)

    for variant, variant_configs in by_variant.items():
        feature_info = precompute_feature_set(variant, variant_configs, train_df, val_df, args, stopwords)
        try:
            with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
                futures = {
                    pool.submit(
                        fit_one_classifier,
                        config_id,
                        len(configs),
                        variant,
                        params,
                        feature_info,
                        args,
                        evaluator,
                    ): config_id
                    for config_id, params in enumerate(
                        variant_configs, start=next_config_id
                    )
                }
                next_config_id += len(variant_configs)
                for fut in as_completed(futures):
                    rows.append(fut.result())
        finally:
            feature_info["train_features"].unpersist()
            feature_info["val_features"].unpersist()
    return rows


def run_all_configs_one_pool(configs, train_df, val_df, args, stopwords, evaluator):
    by_variant = {}
    for variant, params in configs:
        by_variant.setdefault(variant, []).append(params)

    feature_sets = {}
    rows = []
    try:
        for variant, variant_configs in by_variant.items():
            feature_sets[variant] = precompute_feature_set(
                variant, variant_configs, train_df, val_df, args, stopwords
            )

        print(f"submitting all {len(configs)} configs to one pool", flush=True)
        with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
            futures = {}
            for config_id, (variant, params) in enumerate(configs, start=1):
                futures[
                    pool.submit(
                        fit_one_classifier,
                        config_id,
                        len(configs),
                        variant,
                        params,
                        feature_sets[variant],
                        args,
                        evaluator,
                    )
                ] = config_id
            for finished, fut in enumerate(as_completed(futures), start=1):
                row = fut.result()
                row["finished_order"] = finished
                row["remaining"] = len(configs) - finished
                rows.append(row)
                print(
                    f"progress finished={finished}/{len(configs)} "
                    f"remaining={len(configs) - finished}",
                    flush=True,
                )
    finally:
        for info in feature_sets.values():
            info["train_features"].unpersist()
            info["val_features"].unpersist()
    return rows


def main():
    args = build_parser().parse_args()
    default_input, default_stopwords = default_paths(args.run_mode)
    args.input_path = args.input_path or default_input
    args.stopwords_path = args.stopwords_path or default_stopwords

    print("experiment args:", json.dumps(vars(args), indent=2, default=str), flush=True)

    spark = make_spark(args)
    try:
        with open(args.stopwords_path, encoding="utf-8") as fh:
            stopwords = [line.strip() for line in fh if line.strip()]

        reviews = (
            spark.read.json(args.input_path)
            .select("category", F.coalesce(F.col("reviewText"), F.lit("")).alias("text"))
            .where(F.col("category").isNotNull())
        )
        if args.sample_fraction < 1.0:
            reviews = reviews.sample(False, args.sample_fraction, seed=SEED)
        reviews = reviews.cache()
        total = reviews.count()

        train_df, val_df, test_df = reviews.randomSplit(
            [TRAIN_FRAC, VAL_FRAC, TEST_FRAC], seed=SEED
        )
        train_df = maybe_single_partition(train_df, args)
        val_df = maybe_single_partition(val_df, args)
        train_n, _ = materialize(train_df, "train_df")
        val_n, _ = materialize(val_df, "val_df")
        test_n = test_df.count()
        print(
            f"split total={total} train={train_n} val={val_n} test={test_n}",
            flush=True,
        )

        configs = grid(args)
        print(f"grid configs={len(configs)} configs={configs}", flush=True)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        )

        t0 = time.time()
        if args.schedule == "sequential-groups":
            rows = run_sequential_groups(
                configs, train_df, val_df, args, stopwords, evaluator
            )
        else:
            rows = run_all_configs_one_pool(
                configs, train_df, val_df, args, stopwords, evaluator
            )
        total_seconds = time.time() - t0

        rows_sorted = sorted(rows, key=lambda r: -r["val_f1"])
        summary = {
            "args": vars(args),
            "split": {"total": total, "train": train_n, "val": val_n, "test": test_n},
            "total_seconds": total_seconds,
            "best": rows_sorted[0] if rows_sorted else None,
            "results": rows_sorted,
        }

        print(f"total_seconds={total_seconds:.1f}", flush=True)
        print("best:", json.dumps(summary["best"], indent=2, default=str), flush=True)
        print("all results sorted:")
        for row in rows_sorted:
            print(json.dumps(row, sort_keys=True, default=str), flush=True)

        output_path = Path(args.output_json)
        output_path.write_text(json.dumps(summary, indent=2, default=str) + "\n")
        print(f"wrote {output_path}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
