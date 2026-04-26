"""
Pipeline

    1/2 MRDocCounts  ->  {<category>: N_c, ...}
    2/2 MRChiSquare  ->  (category, [(term, chi^2), ...])
    after:             ->  output.txt (sorted lines + merged dictionary)

"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from data_intensive_computing.assignment1.chi_square import MRChiSquare
from data_intensive_computing.assignment1.doc_counts import MRDocCounts

# Path to the hadoop-streaming jar on the TUWien LBD cluster
DEFAULT_STREAMING_JAR = "/usr/lib/hadoop/tools/lib/hadoop-streaming.jar"


def _runner_args(runner: str, streaming_jar: str | None) -> list[str]:
    """Extra mrjob CLI args for a given runner (e.g. ``--hadoop-streaming-jar``)."""
    args = ["-r", runner]
    if runner == "hadoop":
        jar = streaming_jar or os.environ.get(
            "HADOOP_STREAMING_JAR", DEFAULT_STREAMING_JAR
        )
        args += ["--hadoop-streaming-jar", jar]
    return args


def compute_stats(
    input_path: str, runner: str, streaming_jar: str | None = None
) -> dict[str, int]:
    """Run :class:`MRDocCounts` and return ``{key: count}``."""
    job = MRDocCounts(args=[*_runner_args(runner, streaming_jar), input_path])
    stats: dict[str, int] = {}
    with job.make_runner() as run:
        run.run()
        for key, value in job.parse_output(run.cat_output()):
            stats[str(key)] = int(value)
    return stats


def compute_chi_square(
    input_path: str,
    stopwords_path: str,
    stats_path: str,
    runner: str,
    top_n: int,
    streaming_jar: str | None = None,
    reducers_s1: int = 60,
    reducers_s2: int = 60,
    reducers_s3: int = 22,
) -> dict[str, list[tuple[str, float]]]:
    """Run :class:`MRChiSquare` and collect ``{category: [(term, chi^2), ...]}``."""
    job = MRChiSquare(
        args=[
            *_runner_args(runner, streaming_jar),
            "--stopwords",
            stopwords_path,
            "--stats",
            stats_path,
            "--top-n",
            str(top_n),
            "--reducers-s1",
            str(reducers_s1),
            "--reducers-s2",
            str(reducers_s2),
            "--reducers-s3",
            str(reducers_s3),
            input_path,
        ]
    )
    results: dict[str, list[tuple[str, float]]] = {}
    with job.make_runner() as run:
        run.run()
        for category, pairs in job.parse_output(run.cat_output()):
            results[str(category)] = [(str(term), float(chi2)) for term, chi2 in pairs]
    return results


def write_output(
    results: dict[str, list[tuple[str, float]]],
    output_path: str,
) -> None:
    """Write output.txt in the format required by the assignment.

    * One line per category (categories alphabetical) with
      ``<category> term_1:chi_1 term_2:chi_2 ...``.
    * Final line: the union of all selected terms, space-separated and sorted.
    """
    lines: list[str] = []
    all_terms: set[str] = set()
    for category in sorted(results):
        pairs = results[category]
        tokens = " ".join(f"{term}:{chi2:.4f}" for term, chi2 in pairs)
        lines.append(f"{category} {tokens}")
        all_terms.update(term for term, _ in pairs)
    lines.append(" ".join(sorted(all_terms)))
    Path(output_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Assignment 1 chi-square pipeline.",
    )
    parser.add_argument(
        "input",
        help="Reviews JSON path. Local path or HDFS URI (hdfs:///...).",
    )
    parser.add_argument(
        "--stopwords",
        default="data/stopwords.txt",
        help="Stopwords file (default: data/stopwords.txt).",
    )
    parser.add_argument(
        "--output",
        default="output.txt",
        help="Path for the combined output.txt (default: output.txt).",
    )
    parser.add_argument(
        "--runner",
        default="inline",
        choices=["inline", "local", "hadoop"],
        help="mrjob runner. inline/local for laptop, hadoop for the cluster.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=75,
        help="Terms to keep per category (default: 75).",
    )
    parser.add_argument(
        "--hadoop-streaming-jar",
        default=None,
        help=(
            "Path to hadoop-streaming.jar on the cluster (required by "
            "mrjob's hadoop runner). Defaults to "
            f"{DEFAULT_STREAMING_JAR!r} or $HADOOP_STREAMING_JAR."
        ),
    )
    # Reducer counts per step of the chi-square MRJob. Defaults are the
    # values we settled on after experimenting on the LBD cluster (see
    # docs/performance.md for the measurements).
    parser.add_argument(
        "--reducers-s1",
        type=int,
        default=60,
        help="Reducers for Job 2 step 1 (term x category doc freq). Default 60.",
    )
    parser.add_argument(
        "--reducers-s2",
        type=int,
        default=60,
        help="Reducers for Job 2 step 2 (chi^2 per term). Default 60.",
    )
    parser.add_argument(
        "--reducers-s3",
        type=int,
        default=22,
        help="Reducers for Job 2 step 3 (top-N per category). Default 22.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    print(
        f"1/2 Counting documents per category via '{args.runner}' runner...",
        file=sys.stderr,
    )
    stats = compute_stats(args.input, args.runner, args.hadoop_streaming_jar)
    total = sum(stats.values())
    print(
        f"      N = {total:,} reviews across {len(stats)} categories",
        file=sys.stderr,
    )

    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8"
    ) as handle:
        json.dump(stats, handle)
        stats_path = handle.name

    try:
        print(
            f"2/2 Computing chi-square + top-{args.top_n} per category...",
            file=sys.stderr,
        )
        results = compute_chi_square(
            args.input,
            args.stopwords,
            stats_path,
            args.runner,
            args.top_n,
            args.hadoop_streaming_jar,
            args.reducers_s1,
            args.reducers_s2,
            args.reducers_s3,
        )
        write_output(results, args.output)
        print(
            f"      Wrote {args.output} ({len(results)} categories).",
            file=sys.stderr,
        )
    finally:
        Path(stats_path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
