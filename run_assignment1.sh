#!/usr/bin/env bash
# Run the Assignment 1 chi-square pipeline.
#
# Local testing (inline runner, the default):
#   ./run_assignment1.sh                           # uses data/reviews_devset.json
#   ./run_assignment1.sh path/to/input.json
#
# Hadoop streaming on the TUWien LBD cluster:
#   RUNNER=hadoop ./run_assignment1.sh hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
#   RUNNER=hadoop ./run_assignment1.sh hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json
#
# Reducer counts per step of the chi-square MRJob (defaults chosen after
# experimenting on the LBD cluster; see docs/performance.md):
#
#   REDUCERS_S1 (default 60) — step 1: term x category document frequency
#   REDUCERS_S2 (default 60) — step 2: chi^2 per term
#   REDUCERS_S3 (default 22) — step 3: top-N per category (capped at #categories)
#
# Example:
#   RUNNER=hadoop REDUCERS_S1=10 REDUCERS_S2=10 REDUCERS_S3=10 \
#     ./run_assignment1.sh hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
set -euo pipefail

# Resolve the script's own directory so this works regardless of the
# caller's cwd (important on the cluster where PYTHONPATH needs to point at this repo's src/ layout).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INPUT="${1:-data/reviews_devset.json}"
STOPWORDS="${2:-data/stopwords.txt}"
OUTPUT="${3:-output.txt}"
RUNNER="${RUNNER:-inline}"

# defaults for reducer amount
REDUCERS_S1="${REDUCERS_S1:-60}"
REDUCERS_S2="${REDUCERS_S2:-60}"
REDUCERS_S3="${REDUCERS_S3:-22}"

# prefer uv if present for local setup
if command -v uv >/dev/null 2>&1; then
    PY=(uv run python)
else
    PY=(python3)
    export PYTHONPATH="${SCRIPT_DIR}/src${PYTHONPATH:+:$PYTHONPATH}"
fi

EXTRA_ARGS=()
if [[ "$RUNNER" == "hadoop" ]]; then
    EXTRA_ARGS+=("--hadoop-streaming-jar"
                 "${HADOOP_STREAMING_JAR:-/usr/lib/hadoop/tools/lib/hadoop-streaming.jar}")
fi

# set all variables for script
"${PY[@]}" -m data_intensive_computing.assignment1.run \
    --stopwords "$STOPWORDS" \
    --output "$OUTPUT" \
    --runner "$RUNNER" \
    --reducers-s1 "$REDUCERS_S1" \
    --reducers-s2 "$REDUCERS_S2" \
    --reducers-s3 "$REDUCERS_S3" \
    ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} \
    "$INPUT"
