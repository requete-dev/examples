from pyspark.sql import DataFrame

from requete import nodes


@nodes.sink(
    tag="write_user_segments",
    pipeline="ecommerce_analytics",
    depends_on=["user_segments"],
    env=["dev", "ci"],
)
def write_dev(user_segments_df: DataFrame) -> None:
    """Writes user segments to dev table (overwrite mode for testing)"""
    user_segments_df.write.option(
        "path", "/tmp/requete/spark/warehouse/dev_user_segments"
    ).mode("overwrite").saveAsTable("dev_user_segments")


@nodes.sink(
    tag="write_user_segments",
    pipeline="ecommerce_analytics",
    depends_on=["user_segments"],
    env=["staging"],
)
def write_staging(user_segments_df: DataFrame) -> None:
    """Writes user segments to staging table (overwrite mode)"""
    user_segments_df.write.option(
        "path", "/tmp/requete/spark/warehouse/staging_user_segments"
    ).mode("overwrite").saveAsTable("staging_user_segments")


@nodes.sink(
    tag="write_user_segments",
    pipeline="ecommerce_analytics",
    depends_on=["user_segments"],
    env=["prod"],
)
def write_prod(user_segments_df: DataFrame) -> None:
    """
    Writes user segments to production table.
    Uses overwrite mode since we want the latest segmentation snapshot.
    In a real scenario, you might use merge/upsert to update only changed users.
    """
    user_segments_df.write.option(
        "path", "/tmp/requete/spark/warehouse/user_segments"
    ).mode("overwrite").saveAsTable("user_segments")
