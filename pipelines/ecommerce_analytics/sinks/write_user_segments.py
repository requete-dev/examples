from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(
    tag="write_user_segments",
    pipeline="ecommerce_analytics",
    depends_on=["user_segments"],
    env=["dev", "staging", "ci"],
)
def write_dev(user_segments_df: DataFrame) -> None:
    """Dev: write to temp view for validation only."""
    user_segments_df.createOrReplaceTempView("temp_user_segments")


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
    user_segments_df.write.mode("overwrite").saveAsTable("user_segments")
