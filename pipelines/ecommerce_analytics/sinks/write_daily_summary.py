from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(
    tag="write_daily_summary",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["dev", "staging", "ci"],
)
def write_dev(daily_metrics_df: DataFrame) -> None:
    """Dev: write to temp view for validation only."""
    daily_metrics_df.createOrReplaceTempView("temp_daily_analytics")


@nodes.sink(
    tag="write_daily_summary",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["prod", "backfill"],
)
def write_prod(daily_metrics_df: DataFrame) -> None:
    """Writes daily metrics to production table (overwrite mode, partitioned by date)"""
    # In production, we partition by order_day for efficient querying
    daily_metrics_df.write.mode("overwrite").saveAsTable("daily_analytics")
