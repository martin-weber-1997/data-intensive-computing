# data-intensive-computing
Exercises for the 194.048 Data-intensive Computing (VU 2,0) 2026S course

## PySpark setup with uv

This repository now includes a minimal `uv`-managed PySpark project that runs jobs in local mode and tests them with `pytest`.

### Prerequisites

- Python 3.11
- A local JDK on your `PATH` or `JAVA_HOME` set, because PySpark needs Java even in local mode
- `uv` installed

### Install dependencies

```bash
uv sync --dev
```

If you want `uv` to install Python 3.11 for you first:

```bash
uv python install 3.11
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


test 123
