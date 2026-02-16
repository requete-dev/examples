from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, countDistinct
from pyspark.sql.functions import sum as spark_sum
from requete import nodes, tests


@nodes.transform(
    tag="daily_metrics",
    pipeline="ecommerce_analytics",
    depends_on=["enrich_orders"],
)
def calculate_daily_metrics(enrich_orders_df: DataFrame) -> DataFrame:
    """
    Aggregates enriched orders to daily business metrics.

    Calculates key metrics per day:
    - Total orders
    - Unique customers
    - Revenue
    - Average order value
    - Units sold
    """
    daily_df = (
        enrich_orders_df.groupBy("order_day")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("user_id").alias("unique_customers"),
            spark_sum("total_amount").alias("revenue"),
            avg("total_amount").alias("avg_order_value"),
            spark_sum("quantity").alias("units_sold"),
        )
        .orderBy("order_day")
    )

    return daily_df


@tests.unit(tag="daily_metrics")
def test_basic_aggregation(sparkSession: SparkSession) -> None:
    """Test that daily aggregation works correctly"""
    # Create test enriched orders
    data = [
        (1, 101, datetime(2024, 1, 1), 100.0, 2),
        (2, 102, datetime(2024, 1, 1), 150.0, 1),
        (3, 101, datetime(2024, 1, 2), 200.0, 3),
    ]
    enrich_orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        data, ["order_id", "user_id", "order_day", "total_amount", "quantity"]
    )

    result = calculate_daily_metrics(enrich_orders)

    assert result.count() == 2
    day1 = result.filter(col("order_day") == datetime(2024, 1, 1)).first()
    assert day1 is not None
    assert day1["total_orders"] == 2
    assert day1["unique_customers"] == 2
    assert day1["revenue"] == 250.0


@tests.integration(tag="daily_metrics", env=["dev", "staging"])
def test_positive_values(daily_metrics_df: DataFrame) -> None:
    """Test that all metrics are positive"""
    negative_revenue = daily_metrics_df.filter(col("revenue") < 0).count()
    negative_units = daily_metrics_df.filter(col("units_sold") < 0).count()

    assert negative_revenue == 0, "Revenue should never be negative"
    assert negative_units == 0, "Units sold should never be negative"


@tests.integration(tag="daily_metrics", env=["dev", "staging", "prod"])
def test_completeness(daily_metrics_df: DataFrame) -> None:
    """Test that we have metrics for all expected days"""
    assert daily_metrics_df.count() > 0, "Should have at least one day of metrics"

    # All metrics should be non-null
    null_counts = daily_metrics_df.filter(
        col("total_orders").isNull()
        | col("unique_customers").isNull()
        | col("revenue").isNull()
    ).count()

    assert null_counts == 0, "No metrics should be null"
