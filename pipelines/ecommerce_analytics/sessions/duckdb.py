from duckdb.experimental.spark.sql import SparkSession

from requete import sessions


@sessions.session(
    tag="duckdb_dev_session",
    pipeline="ecommerce_analytics",
    engine="duckdb",
    env=["dev"],
)
def dev_session() -> SparkSession:
    """Create DuckDB session for ecommerce analytics dev environment."""
    return SparkSession.builder.remote("local").getOrCreate()


@sessions.session(
    tag="duckdb_staging_session",
    pipeline="ecommerce_analytics",
    engine="duckdb",
    env=["staging"],
)
def staging_session() -> SparkSession:
    """Create DuckDB session for ecommerce analytics staging environment."""
    return SparkSession.builder.remote("local").getOrCreate()


@sessions.session(
    tag="duckdb_ci_session",
    pipeline="ecommerce_analytics",
    engine="duckdb",
    env=["ci"],
)
def ci_session() -> SparkSession:
    """Create DuckDB session for ecommerce analytics CI environment."""
    return SparkSession.builder.remote("local").getOrCreate()
