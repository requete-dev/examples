from duckdb.experimental.spark.sql import SparkSession
from requete import sessions


@sessions.session(tag="duckdb_dev_session", pipeline="simple", engine="duckdb", env=["dev"])
def dev_xyz() -> SparkSession:
    """Create a DuckDB session for the dev environment.

    Configured with local mode for development and testing.
    """
    return SparkSession.builder.remote("local").getOrCreate()
