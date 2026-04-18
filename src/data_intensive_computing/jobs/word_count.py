from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import asc, coalesce, col, desc, explode, lit, lower, split

from data_intensive_computing.spark import create_spark_session


def count_words(lines: DataFrame, text_column: str = "line") -> DataFrame:
    """Count words from a DataFrame that contains one text column."""
    normalized_text = lower(coalesce(col(text_column), lit("")))
    words = lines.select(explode(split(normalized_text, r"\W+")).alias("word"))

    return (
        words.where(col("word") != "")
        .groupBy("word")
        .count()
        .orderBy(desc("count"), asc("word"))
    )


def run_job(spark: SparkSession, input_path: str) -> DataFrame:
    """Read a text file and return word frequencies."""
    lines = spark.read.text(input_path).toDF("line")
    return count_words(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the sample word count Spark job.")
    parser.add_argument("input_path", help="Path to a local text file or directory.")
    parser.add_argument(
        "--output-path",
        help="Optional output path. Results are written as parquet when provided.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(app_name="word-count")
    results = run_job(spark, args.input_path)

    if args.output_path:
        results.write.mode("overwrite").parquet(args.output_path)
    else:
        results.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
