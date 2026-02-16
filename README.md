# Requete Pipelines

Example pipelines for [Requete](https://github.com/requete-dev/requete).

## Dev Setup

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
cd pipelines
uv sync
```

This creates a `.venv` with `pyspark`, `duckdb`, and the `requete` SDK installed as dev dependencies.

> **Note:** `pyproject.toml` is purely for local development (IDE support, type checking). It has no effect on production. In production, Requete reads `requete.yaml` in each pipeline directory to determine the Python version, dependencies, and engine configuration for both running tests on dev and creating artifacts for prod/ci.

## Type Checking

[basedpyright](https://docs.basedpyright.com/) is configured via `[tool.basedpyright]` in `pyproject.toml`.

```bash
cd pipelines
npx basedpyright
```

IDEs like Zed will pick up the config automatically from `pyproject.toml`.

## Project Structure

```
pipelines/
├── pyproject.toml          # Dev dependencies + basedpyright config (dev/ide only, not for running tests via extension, that uses requete.yaml)
├── simple/                 # Simple example pipeline
│   ├── requete.yaml        # Pipeline config (name, python version, deps)
│   ├── sessions/           # Engine sessions (Spark, DuckDB)
│   ├── sources/            # Data sources
│   ├── transforms/         # Transformations
│   ├── sinks/              # Data sinks
│   └── promotes/           # Validated writes
└── ecommerce_analytics/    # More complex example pipeline
    ├── requete.yaml
    ├── sessions/
    ├── sources/
    ├── transforms/
    ├── sinks/
    └── promotes/
```
