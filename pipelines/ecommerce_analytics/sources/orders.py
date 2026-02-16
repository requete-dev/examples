from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from requete import nodes, tests


@nodes.source(tag="orders", pipeline="ecommerce_analytics", env=["dev", "ci"])
def orders_dev(sparkSession: SparkSession) -> DataFrame:
    """Dev environment - in-memory test data with 20 sample orders"""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("order_date", DateType(), False),
            StructField("order_timestamp", TimestampType(), False),
        ]
    )

    # Sample data: 20 orders over 5 days, multiple products/users
    data = [
        # Day 1 - 2024-01-01
        (
            "ORD001",
            "U001",
            "P001",
            2,
            29.99,
            date(2024, 1, 1),
            datetime(2024, 1, 1, 10, 30),
        ),
        (
            "ORD002",
            "U002",
            "P002",
            1,
            49.99,
            date(2024, 1, 1),
            datetime(2024, 1, 1, 11, 15),
        ),
        (
            "ORD003",
            "U003",
            "P003",
            3,
            19.99,
            date(2024, 1, 1),
            datetime(2024, 1, 1, 14, 20),
        ),
        (
            "ORD004",
            "U001",
            "P004",
            1,
            99.99,
            date(2024, 1, 1),
            datetime(2024, 1, 1, 16, 45),
        ),
        # Day 2 - 2024-01-02
        (
            "ORD005",
            "U004",
            "P001",
            1,
            29.99,
            date(2024, 1, 2),
            datetime(2024, 1, 2, 9, 10),
        ),
        (
            "ORD006",
            "U002",
            "P005",
            2,
            15.99,
            date(2024, 1, 2),
            datetime(2024, 1, 2, 10, 5),
        ),
        (
            "ORD007",
            "U005",
            "P002",
            1,
            49.99,
            date(2024, 1, 2),
            datetime(2024, 1, 2, 13, 30),
        ),
        (
            "ORD008",
            "U003",
            "P006",
            4,
            9.99,
            date(2024, 1, 2),
            datetime(2024, 1, 2, 15, 20),
        ),
        # Day 3 - 2024-01-03
        (
            "ORD009",
            "U006",
            "P003",
            2,
            19.99,
            date(2024, 1, 3),
            datetime(2024, 1, 3, 8, 45),
        ),
        (
            "ORD010",
            "U001",
            "P007",
            1,
            79.99,
            date(2024, 1, 3),
            datetime(2024, 1, 3, 11, 0),
        ),
        (
            "ORD011",
            "U007",
            "P001",
            3,
            29.99,
            date(2024, 1, 3),
            datetime(2024, 1, 3, 12, 30),
        ),
        (
            "ORD012",
            "U004",
            "P008",
            1,
            129.99,
            date(2024, 1, 3),
            datetime(2024, 1, 3, 17, 10),
        ),
        # Day 4 - 2024-01-04
        (
            "ORD013",
            "U002",
            "P004",
            2,
            99.99,
            date(2024, 1, 4),
            datetime(2024, 1, 4, 10, 20),
        ),
        (
            "ORD014",
            "U008",
            "P009",
            1,
            39.99,
            date(2024, 1, 4),
            datetime(2024, 1, 4, 11, 45),
        ),
        (
            "ORD015",
            "U005",
            "P010",
            5,
            7.99,
            date(2024, 1, 4),
            datetime(2024, 1, 4, 14, 15),
        ),
        (
            "ORD016",
            "U009",
            "P002",
            1,
            49.99,
            date(2024, 1, 4),
            datetime(2024, 1, 4, 16, 30),
        ),
        # Day 5 - 2024-01-05
        (
            "ORD017",
            "U010",
            "P011",
            2,
            24.99,
            date(2024, 1, 5),
            datetime(2024, 1, 5, 9, 5),
        ),
        (
            "ORD018",
            "U003",
            "P012",
            1,
            149.99,
            date(2024, 1, 5),
            datetime(2024, 1, 5, 10, 40),
        ),
        (
            "ORD019",
            "U006",
            "P001",
            1,
            29.99,
            date(2024, 1, 5),
            datetime(2024, 1, 5, 13, 25),
        ),
        (
            "ORD020",
            "U001",
            "P013",
            3,
            12.99,
            date(2024, 1, 5),
            datetime(2024, 1, 5, 15, 50),
        ),
    ]

    return sparkSession.createDataFrame(data, schema)  # pyright: ignore[reportUnknownMemberType]


@nodes.source(tag="orders", pipeline="ecommerce_analytics", env=["staging", "prod"])
def orders_prod(sparkSession: SparkSession) -> DataFrame:
    """Staging/Prod - read from orders table"""
    return sparkSession.table("orders")


@nodes.backfill_source(tag="orders", pipeline="ecommerce_analytics", env=["backfill"])
def orders_backfill(sparkSession: SparkSession, context: dict[str, str]) -> DataFrame:
    """Backfill - filter by date range from context"""
    start_date = context.get("from_date")
    end_date = context.get("to_date")

    df = sparkSession.table("orders")

    if start_date and end_date:
        df = df.filter(
            (col("order_date") >= start_date) & (col("order_date") < end_date)
        )

    return df


@tests.source(tag="orders", env=["dev", "ci", "staging", "prod", "backfill"])
def orders_test(orders_df: DataFrame) -> None:
    """Common tests for orders source across all environments"""
    # Test that we have data
    assert orders_df.count() > 0, "Orders table should not be empty"

    # Test schema - all required columns exist
    columns = orders_df.columns
    required_columns = [
        "order_id",
        "user_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_date",
        "order_timestamp",
    ]
    for column in required_columns:
        assert column in columns, f"Required column '{column}' is missing"

    # Test data quality - no nulls in non-nullable fields
    assert orders_df.filter(col("order_id").isNull()).count() == 0, (
        "order_id should not have nulls"
    )
    assert orders_df.filter(col("user_id").isNull()).count() == 0, (
        "user_id should not have nulls"
    )
    assert orders_df.filter(col("product_id").isNull()).count() == 0, (
        "product_id should not have nulls"
    )

    # Test business logic - quantity and price should be positive
    assert orders_df.filter(col("quantity") <= 0).count() == 0, (
        "quantity should be positive"
    )
    assert orders_df.filter(col("unit_price") <= 0).count() == 0, (
        "unit_price should be positive"
    )
