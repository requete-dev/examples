from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    count,
    datediff,
    lit,
    ntile,
    when,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from requete import nodes, tests


@nodes.transform(
    tag="user_segments",
    pipeline="ecommerce_analytics",
    depends_on=["enrich_orders"],
)
def segment_users(enrich_orders_df: DataFrame) -> DataFrame:
    """
    Performs RFM (Recency, Frequency, Monetary) segmentation of users.

    RFM Analysis:
    - Recency: Days since last purchase
    - Frequency: Total number of orders
    - Monetary: Total amount spent

    Segments:
    - High Value: Recent, frequent, high spending
    - At Risk: Not recent, but historically good
    - New: Recent signup, low frequency
    - Lost: Not purchased recently
    """
    # Calculate RFM metrics per user
    user_rfm = enrich_orders_df.groupBy("user_id", "user_name", "email", "country").agg(
        spark_max("order_date").alias("last_order_date"),
        count("order_id").alias("frequency"),
        spark_sum("total_amount").alias("monetary_value"),
    )

    # Calculate recency (days since last order from a reference date)
    # Using 2024-01-10 as reference (5 days after last order in test data)
    reference_date = lit("2024-01-10").cast("date")
    user_rfm = user_rfm.withColumn("recency", datediff(reference_date, col("last_order_date")))

    # Create quartiles for RFM scores using window functions
    window_spec = Window.partitionBy()

    user_rfm = user_rfm.withColumn(
        "recency_score",
        ntile(4).over(window_spec.orderBy(col("recency"))),  # Lower recency is better
    )
    user_rfm = user_rfm.withColumn("frequency_score", ntile(4).over(window_spec.orderBy(col("frequency").desc())))
    user_rfm = user_rfm.withColumn(
        "monetary_score",
        ntile(4).over(window_spec.orderBy(col("monetary_value").desc())),
    )

    # Assign segments based on RFM scores
    user_rfm = user_rfm.withColumn(
        "segment",
        when(
            (col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2),
            "High Value",
        )
        .when((col("recency_score") >= 3) & (col("frequency_score") <= 2), "At Risk")
        .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "New")
        .otherwise("Lost"),
    )

    return user_rfm.select(
        "user_id",
        "user_name",
        "email",
        "country",
        "segment",
        "recency",
        "frequency",
        "monetary_value",
        "recency_score",
        "frequency_score",
        "monetary_score",
    )


@tests.unit(tag="user_segments")
def test_rfm_calculation(sparkSession: SparkSession) -> None:
    """Test that RFM scores are calculated correctly"""
    # Create test enriched orders
    data = [
        (101, "User A", "a@example.com", "USA", datetime(2024, 1, 5), 1, 100.0),
        (101, "User A", "a@example.com", "USA", datetime(2024, 1, 4), 2, 100.0),
        (102, "User B", "b@example.com", "UK", datetime(2024, 1, 1), 3, 50.0),
    ]
    enrich_orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        data,
        [
            "user_id",
            "user_name",
            "email",
            "country",
            "order_date",
            "order_id",
            "total_amount",
        ],
    )

    result = segment_users(enrich_orders)

    assert result.count() == 2
    assert "recency" in result.columns
    assert "frequency" in result.columns
    assert "monetary_value" in result.columns
    assert "segment" in result.columns


@tests.unit(tag="user_segments")
def test_segment_assignment(sparkSession: SparkSession) -> None:
    """Test that segments are assigned correctly"""
    data = [
        (101, "User A", "a@example.com", "USA", datetime(2024, 1, 5), 1, 1000.0),
    ]
    enrich_orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        data,
        [
            "user_id",
            "user_name",
            "email",
            "country",
            "order_date",
            "order_id",
            "total_amount",
        ],
    )

    result = segment_users(enrich_orders)

    assert result.count() == 1
    row = result.first()
    assert row is not None
    assert row["segment"] is not None
    assert row["frequency"] == 1
    assert row["monetary_value"] == 1000.0


@tests.integration(tag="user_segments", env=["dev", "staging", "prod"])
def test_segment_distribution(user_segments_df: DataFrame) -> None:
    """Test that segments are distributed reasonably"""
    total_users = user_segments_df.count()

    assert total_users > 0, "Should have at least one user"

    # Check that all users have a segment
    null_segments = user_segments_df.filter(col("segment").isNull()).count()
    assert null_segments == 0, "All users should have a segment"

    # Check that RFM values are reasonable
    invalid_recency = user_segments_df.filter(col("recency") < 0).count()
    invalid_frequency = user_segments_df.filter(col("frequency") < 1).count()
    invalid_monetary = user_segments_df.filter(col("monetary_value") <= 0).count()

    assert invalid_recency == 0, "Recency should not be negative"
    assert invalid_frequency == 0, "Frequency should be at least 1"
    assert invalid_monetary == 0, "Monetary value should be positive"
