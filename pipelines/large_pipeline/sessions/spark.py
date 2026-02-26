from pyspark.sql import SparkSession
from requete import sessions


@sessions.session(
    tag="spark_dev_session",
    pipeline="large_pipeline",
    engine="spark",
    env=["dev"],
)
def dev_session() -> SparkSession:
    """Create Spark session for large_pipeline dev environment."""
    return (  # pyright: ignore[reportUnknownVariableType]
        SparkSession.builder.master("local[*]")  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        .appName("large_pipeline_dev")
        .getOrCreate()
    )
