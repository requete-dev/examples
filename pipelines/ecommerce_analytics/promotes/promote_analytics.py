from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from requete import nodes, tests


@nodes.promote(
    tag="promote_analytics",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["dev", "staging", "ci"],
)
def promote_dev(daily_metrics_df: DataFrame) -> None:
    """Dev: write to temp view for validation only."""
    daily_metrics_df.createOrReplaceTempView("temp_daily_analytics_promoted")


@nodes.promote(
    tag="promote_analytics",
    pipeline="ecommerce_analytics",
    depends_on=["daily_metrics"],
    env=["prod"],
)
def promote_prod(daily_metrics_df: DataFrame) -> None:
    """Promotes validated daily metrics to production promoted table"""
    daily_metrics_df.write.option("path", "/tmp/requete/spark/warehouse/daily_analytics_promoted").mode(
        "overwrite"
    ).partitionBy("order_day").saveAsTable("daily_analytics_promoted")


# Promotion tests - these run BEFORE promotion to validate data quality


@tests.promotion(tag="promote_analytics", env=["dev", "ci"])
def test_revenue_dev(daily_metrics_df: DataFrame) -> None:
    """Validate revenue in dev environment"""
    assert daily_metrics_df.count() > 0, "Data should not be empty"

    # No negative revenue
    negative_revenue = daily_metrics_df.filter(col("revenue") < 0).count()
    assert negative_revenue == 0, "Revenue should never be negative"


@tests.promotion(tag="promote_analytics", env=["staging"])
def test_revenue_staging(daily_metrics_df: DataFrame) -> None:
    """Validate revenue in staging environment"""
    assert daily_metrics_df.count() > 0, "Data should not be empty"

    # No negative revenue
    negative_revenue = daily_metrics_df.filter(col("revenue") < 0).count()
    assert negative_revenue == 0, "Revenue should never be negative"

    # All required columns present
    required_columns = [
        "order_day",
        "total_orders",
        "unique_customers",
        "revenue",
        "avg_order_value",
        "units_sold",
    ]
    for column in required_columns:
        assert column in daily_metrics_df.columns, f"Required column '{column}' is missing"


@tests.promotion(tag="promote_analytics", env=["prod"])
def test_revenue_prod(daily_metrics_df: DataFrame) -> None:
    """Validate revenue in production environment (strict checks)"""
    assert daily_metrics_df.count() > 0, "Data should not be empty"

    # No negative revenue
    negative_revenue = daily_metrics_df.filter(col("revenue") < 0).count()
    assert negative_revenue == 0, "Revenue should never be negative"

    # No null values in critical columns
    null_revenue = daily_metrics_df.filter(col("revenue").isNull()).count()
    null_orders = daily_metrics_df.filter(col("total_orders").isNull()).count()

    assert null_revenue == 0, "Revenue should not have null values"
    assert null_orders == 0, "Total orders should not have null values"

    # Revenue should be reasonable (basic sanity check)
    zero_revenue_with_orders = daily_metrics_df.filter((col("total_orders") > 0) & (col("revenue") == 0)).count()

    assert zero_revenue_with_orders == 0, "Days with orders should have non-zero revenue"


@tests.promotion(tag="promote_analytics", env=["prod"])
def test_completeness_prod(daily_metrics_df: DataFrame) -> None:
    """Validate data completeness in production"""
    # Check that total_orders matches unique order counts
    invalid_metrics = daily_metrics_df.filter((col("total_orders") < col("unique_customers"))).count()

    assert invalid_metrics == 0, "Total orders should be >= unique customers"

    # Check that units_sold is reasonable
    zero_units_with_revenue = daily_metrics_df.filter((col("revenue") > 0) & (col("units_sold") == 0)).count()

    assert zero_units_with_revenue == 0, "Days with revenue should have units sold"
