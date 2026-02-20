from pyspark.sql import SparkSession
from requete import sessions


@sessions.session(tag="spark_dev_session", pipeline="simple", engine="spark", env=["dev"])
def dev_xyz() -> SparkSession:
    """Create a Spark session for the dev environment.

    Configured with local mode for unit testing and development.
    """
    return SparkSession.builder.master("local[*]").appName("UnitTests").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]


@sessions.session(tag="spark_staging_session", pipeline="simple", engine="spark", env=["staging"])
def staging_xyz() -> SparkSession:
    """Create a Spark session for the staging environment.

    Configured with local mode for integration testing.
    """
    return (  # pyright: ignore[reportUnknownVariableType]
        SparkSession.builder.master("local[*]")  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        .appName("IntegrationTests")
        .getOrCreate()
    )


@sessions.session(
    tag="spark_prod_session",
    pipeline="simple",
    engine="spark",
    env=["prod", "backfill"],
)
def prod_xyz() -> SparkSession:
    """Create a Spark session for the production and backfill environments.

    Configured with local mode for production workloads.
    """
    return SparkSession.builder.master("local[*]").appName("Prod").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
