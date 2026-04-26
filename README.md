# data-intensive-computing
Exercises for the 194.048 Data-intensive Computing (VU 2,0) 2026S course

## PySpark setup with uv

This repository now includes a minimal `uv`-managed PySpark project that runs jobs in local mode and tests them with `pytest`.

### Prerequisites

- Python 3.12
- A local JDK on your `PATH` or `JAVA_HOME` set, because PySpark needs Java even in local mode
- `uv` installed

### Install dependencies

```bash
uv sync --dev
```

If you want `uv` to install Python 3.12 for you first:

```bash
uv python install 3.12
uv sync --dev
```

### Run the example Spark job locally

```bash
uv run python -m data_intensive_computing.jobs.word_count data/example.txt
```

To persist the result instead of printing it:

```bash
uv run python -m data_intensive_computing.jobs.word_count data/example.txt --output-path tmp/word-count-output
```

### Run tests locally

```bash
uv run pytest
```

### Format and lint the code

Format the repository:

```bash
uv run ruff format .
```

Run the PEP-style checker and import-order checks:

```bash
uv run ruff check .
```

Apply lint fixes automatically where possible:

```bash
uv run ruff check . --fix
```

## Assignment 1: Chi-square term selection (MapReduce / mrjob)

The Assignment 1 pipeline lives in `src/data_intensive_computing/assignment1/`
and is split into two `mrjob` jobs plus a driver:

| File | Role |
| --- | --- |
| `doc_counts.py` | 1-step MRJob computing `N` (total reviews) and `N_c` (reviews per category). |
| `chi_square.py` | 3-step MRJob computing chi-square per `(term, category)` and keeping the top 75 per category. |
| `run.py`        | Orchestrator: runs both jobs and writes the final `output.txt`. |

`<key, value>` pairs and step structure are documented at the top of each
module's docstring.

### Run it locally (development set)

```bash
./src/run_assignment1.sh
```

is equivalent to

```bash
uv run python -m data_intensive_computing.assignment1.run \
    --runner inline \
    --stopwords data/stopwords.txt \
    --output output.txt \
    data/reviews_devset.json
```

The devset (~57 MB, 78 829 reviews) finishes in ~20 s on a laptop with the
`inline` runner. The resulting `output.txt` has:

- 22 lines, one per category in alphabetical order, each formatted as
  `<category> term_1:chi_1 term_2:chi_2 ... term_75:chi_75` (descending).
- a final line with the alphabetically sorted union of all selected terms.

Pick a different runner with `--runner local` (subprocess-based) or
`--runner hadoop` (cluster). `inline` is the fastest for iterating.

### Run it on the TUWien LBD Hadoop cluster

The LBD cluster already runs Python 3.12.3 and has `mrjob` available on the
JupyterLab/login environment, so you don't need to set up `uv` there — the
provided `src/run_assignment1.sh` automatically falls back to `python3` when
`uv` isn't on `$PATH`.

Steps:

1. Copy the whole project (or at minimum the `src/` tree, `data/stopwords.txt`
   and `src/run_assignment1.sh`) to your home directory on the cluster. One
   option is to `scp` / `rsync` from a local checkout; another is to
   upload a zip through JupyterLab.
2. (Optional but recommended) `cp -r $HOME/dataLAB/demos $HOME/datalab-demos-copy`
   so you have an editable copy of the demos alongside your code.
3. Run the pipeline against the dev set first:

   ```bash
   RUNNER=hadoop ./src/run_assignment1.sh \
       hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
   ```

   Expected output: `output.txt` in the current directory, 22 category
   lines plus one merged-dictionary line. The job tracking URL
   (`http://lbdmg01.datalab.novalocal:9999/proxy/<app_id>/`) is printed by
   mrjob during each step — use it to inspect progress and counters.

4. Only after the dev-set output looks right, run on the full 56 GB set:

   ```bash
   RUNNER=hadoop ./src/run_assignment1.sh \
       hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json
   ```

Override the streaming jar path if needed:

```bash
HADOOP_STREAMING_JAR=/some/other/hadoop-streaming.jar \
    RUNNER=hadoop ./src/run_assignment1.sh ...
```

What the orchestrator passes to mrjob on the cluster:

- `-r hadoop` — use the Hadoop streaming runner.
- `--hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming.jar`
  (required on LBD; pulled from `$HADOOP_STREAMING_JAR` if set).
- `--stopwords data/stopwords.txt` — uploaded to every worker via
  `add_file_arg`.
- `--stats <tempfile>` — the `{N, N_c}` JSON produced by job 1; also
  uploaded via `add_file_arg`.

Both `doc_counts.py` and `chi_square.py` are intentionally self-contained
(no cross-package imports), so mrjob only has to ship one `.py` to each
worker — no `PYTHONPATH` or zip-bootstrap required.

### Where to look next

- [`docs/dataflow.md`](docs/dataflow.md) — schematic of the two MRJobs with
  every `<key, value>` pair at every step (the figure the report asks for).
- [`docs/performance.md`](docs/performance.md) — what to monitor on the
  cluster and where to turn the knobs if we're too slow on the full set.

### Test quickly with a tiny subset

```bash
head -n 100 data/reviews_devset.json > /tmp/tiny.json
uv run python -m data_intensive_computing.assignment1.run /tmp/tiny.json \
    --output /tmp/tiny_output.txt
head -c 400 /tmp/tiny_output.txt
```

You can also run each MRJob individually, e.g. to eyeball intermediate output:

```bash
uv run python -m data_intensive_computing.assignment1.doc_counts \
    -r inline data/reviews_devset.json
```
