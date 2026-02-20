# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Dev Setup

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
cd pipelines
uv sync
```

This creates a `.venv` with `pyspark`, `duckdb`, and the `requete` SDK installed as dev dependencies.

## Verification

After any code change, run all of the following:

```bash
cd pipelines
uv run ruff format .       # format
uv run ruff check . --fix  # lint
uv run basedpyright        # type check
```

## Architecture

This repo contains example data pipelines for the [Requete](https://github.com/requete-dev/requete) SDK. Each pipeline lives under `pipelines/<pipeline-name>/` and follows a node-based DAG structure.

### Two configuration layers

- `pipelines/pyproject.toml` — local IDE support and type checking only; has no effect on runtime
- `pipelines/<pipeline>/requete.yaml` — controls pipeline name, Python version, dependencies, and environment variables used by the Requete runtime

### Node types

Each node type corresponds to a subdirectory and a decorator from the `requete` SDK:

| Directory | Decorator | Purpose |
|-----------|-----------|---------|
| `sessions/` | `@sessions.session()` | Engine setup (Spark, DuckDB), one file per engine per environment |
| `sources/` | `@nodes.source()` / `@nodes.backfill_source()` | Data ingestion |
| `transforms/` | `@nodes.transform(depends_on=[...])` | Data transformation, declared dependencies form the DAG |
| `sinks/` | `@nodes.sink(depends_on=[...])` | Data writes |
| `promotes/` | `@nodes.promote()` | Validated writes with pre-promotion quality checks |

### Tests

Tests are embedded alongside node definitions using decorators (`@tests.source()`, `@tests.unit()`, `@tests.integration()`, `@tests.promotion()`). They are discovered and run by the Requete SDK — there is no pytest.

### Environments

Nodes can have environment-specific implementations for `dev`, `staging`, `prod`, `ci`, and `backfill`. Sinks typically write to a temp view in `dev`/`staging` and to a real table in `prod`.

### Multi-engine support

Both PySpark and DuckDB are supported. Engine-specific session files live in `sessions/spark.py` and `sessions/duckdb.py`. DuckDB sessions use `experimental.spark.sql.SparkSession` for a compatible API surface.
