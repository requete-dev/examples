from pyspark.sql import DataFrame

from requete import nodes


@nodes.sink(
    tag="write_daily_summary",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["dev", "ci"],
)
def write_dev(daily_metrics_df: DataFrame) -> None:
    """Writes daily metrics to dev table (overwrite mode for testing)"""
    daily_metrics_df.write.option(
        "path", "/tmp/requete/spark/warehouse/dev_daily_analytics"
    ).mode("overwrite").saveAsTable("dev_daily_analytics")


@nodes.sink(
    tag="write_daily_summary",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["staging"],
)
def write_staging(daily_metrics_df: DataFrame) -> None:
    """Writes daily metrics to staging table (overwrite mode)"""
    daily_metrics_df.write.option(
        "path", "/tmp/requete/spark/warehouse/staging_daily_analytics"
    ).mode("overwrite").saveAsTable("staging_daily_analytics")


@nodes.sink(
    tag="write_daily_summary",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["prod", "backfill"],
)
def write_prod(daily_metrics_df: DataFrame) -> None:
    """Writes daily metrics to production table (overwrite mode, partitioned by date)"""
    # In production, we partition by order_day for efficient querying
    daily_metrics_df.write.option(
        "path", "/tmp/requete/spark/warehouse/daily_analytics"
    ).mode("overwrite").partitionBy("order_day").saveAsTable("daily_analytics")
