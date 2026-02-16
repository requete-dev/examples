from pyspark.sql import SparkSession
from requete import sessions


@sessions.session(
    tag="spark_dev_session",
    pipeline="ecommerce_analytics",
    engine="spark",
    env=["dev"],
)
def dev_session() -> SparkSession:
    """Create Spark session for ecommerce analytics dev environment."""
    return (  # pyright: ignore[reportUnknownVariableType]
        SparkSession.builder.master("local[*]")  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        .appName("EcommerceAnalytics-Dev")
        .getOrCreate()
    )


@sessions.session(
    tag="spark_staging_session",
    pipeline="ecommerce_analytics",
    engine="spark",
    env=["staging"],
)
def staging_session() -> SparkSession:
    """Create Spark session for ecommerce analytics staging environment."""
    return (  # pyright: ignore[reportUnknownVariableType]
        SparkSession.builder.master("local[*]")  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        .appName("EcommerceAnalytics-Staging")
        .getOrCreate()
    )


@sessions.session(
    tag="spark_prod_session",
    pipeline="ecommerce_analytics",
    engine="spark",
    env=["prod", "backfill"],
)
def prod_session() -> SparkSession:
    """Create Spark session for ecommerce analytics production and backfill environments."""
    return (  # pyright: ignore[reportUnknownVariableType]
        SparkSession.builder.master("local[*]")  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        .appName("EcommerceAnalytics-Prod")
        .getOrCreate()
    )


@sessions.session(
    tag="spark_ci_session",
    pipeline="ecommerce_analytics",
    engine="spark",
    env=["ci"],
)
def ci_session() -> SparkSession:
    """Create Spark session for ecommerce analytics CI environment."""
    return (  # pyright: ignore[reportUnknownVariableType]
        SparkSession.builder.master("local[*]")  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
        .appName("EcommerceAnalytics-CI")
        .getOrCreate()
    )
