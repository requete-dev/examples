from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, StringType, StructField, StructType
from requete import nodes, tests


@nodes.source(tag="users", pipeline="ecommerce_analytics", env=["dev", "ci"])
def users_dev(sparkSession: SparkSession) -> DataFrame:
    """Dev environment - in-memory test data with 10 sample users"""
    schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("user_name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("country", StringType(), False),
            StructField("signup_date", DateType(), False),
        ]
    )

    # Sample user data - 10 users from different countries
    data = [
        ("U001", "Alice Johnson", "alice@example.com", "USA", date(2023, 6, 15)),
        ("U002", "Bob Smith", "bob@example.com", "UK", date(2023, 7, 22)),
        ("U003", "Carol White", "carol@example.com", "Canada", date(2023, 8, 10)),
        ("U004", "David Brown", "david@example.com", "USA", date(2023, 9, 5)),
        ("U005", "Emma Davis", "emma@example.com", "Australia", date(2023, 10, 18)),
        ("U006", "Frank Wilson", "frank@example.com", "UK", date(2023, 11, 2)),
        ("U007", "Grace Lee", "grace@example.com", "USA", date(2023, 11, 20)),
        ("U008", "Henry Taylor", "henry@example.com", "Canada", date(2023, 12, 8)),
        ("U009", "Ivy Martinez", "ivy@example.com", "USA", date(2023, 12, 15)),
        ("U010", "Jack Anderson", "jack@example.com", "UK", date(2023, 12, 28)),
    ]

    return sparkSession.createDataFrame(data, schema)  # pyright: ignore[reportUnknownMemberType]


@nodes.source(tag="users", pipeline="ecommerce_analytics", env=["staging", "prod"])
def users_prod(sparkSession: SparkSession) -> DataFrame:
    """Staging/Prod - read from users table"""
    return sparkSession.table("users")


@nodes.backfill_source(tag="users", pipeline="ecommerce_analytics", env=["backfill"])
def users_backfill(sparkSession: SparkSession, _context: dict[str, str]) -> DataFrame:
    """Backfill - users table is typically not filtered by date for backfills"""
    return sparkSession.table("users")


@tests.source(tag="users", env=["dev", "ci", "staging", "prod", "backfill"])
def users_test(users_df: DataFrame) -> None:
    """Common tests for users source across all environments"""
    # Test that we have data
    assert users_df.count() > 0, "Users table should not be empty"

    # Test schema - all required columns exist
    columns = users_df.columns
    required_columns = ["user_id", "user_name", "email", "country", "signup_date"]
    for column in required_columns:
        assert column in columns, f"Required column '{column}' is missing"

    # Test data quality - no nulls in non-nullable fields
    assert users_df.filter(col("user_id").isNull()).count() == 0, "user_id should not have nulls"
    assert users_df.filter(col("user_name").isNull()).count() == 0, "user_name should not have nulls"
    assert users_df.filter(col("email").isNull()).count() == 0, "email should not have nulls"

    # Test uniqueness - user_id and email should be unique
    total_count = users_df.count()
    distinct_user_id = users_df.select("user_id").distinct().count()
    distinct_email = users_df.select("email").distinct().count()
    assert total_count == distinct_user_id, "user_id should be unique"
    assert total_count == distinct_email, "email should be unique"

    # Test email format - basic validation
    assert users_df.filter(~col("email").contains("@")).count() == 0, "email should contain '@' symbol"
