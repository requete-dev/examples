from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, StringType, StructField, StructType
from requete import nodes, tests


@nodes.source(tag="products", pipeline="ecommerce_analytics", env=["dev", "ci"])
def products_dev(sparkSession: SparkSession) -> DataFrame:
    """Dev environment - in-memory test data with 15 sample products across 3 categories"""
    schema = StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("list_price", DecimalType(10, 2), False),
        ]
    )

    # Sample product data - 15 products across 3 categories
    data = [
        # Electronics
        ("P001", "Wireless Mouse", "Electronics", Decimal("29.99")),
        ("P002", "Bluetooth Keyboard", "Electronics", Decimal("49.99")),
        ("P003", "USB-C Cable", "Electronics", Decimal("19.99")),
        ("P004", "Laptop Stand", "Electronics", Decimal("99.99")),
        ("P005", "Webcam HD", "Electronics", Decimal("79.99")),
        # Home & Kitchen
        ("P006", "Coffee Maker", "Home & Kitchen", Decimal("89.99")),
        ("P007", "Blender Pro", "Home & Kitchen", Decimal("129.99")),
        ("P008", "Air Fryer", "Home & Kitchen", Decimal("149.99")),
        ("P009", "Toaster", "Home & Kitchen", Decimal("39.99")),
        ("P010", "Water Filter", "Home & Kitchen", Decimal("59.99")),
        # Books
        ("P011", "Python Programming", "Books", Decimal("45.99")),
        ("P012", "Data Science Handbook", "Books", Decimal("54.99")),
        ("P013", "Cloud Architecture", "Books", Decimal("39.99")),
        ("P014", "Machine Learning Basics", "Books", Decimal("49.99")),
        ("P015", "Web Development Guide", "Books", Decimal("42.99")),
    ]

    return sparkSession.createDataFrame(data, schema)  # pyright: ignore[reportUnknownMemberType]


@nodes.source(tag="products", pipeline="ecommerce_analytics", env=["staging", "prod"])
def products_prod(sparkSession: SparkSession) -> DataFrame:
    """Staging/Prod - read from products table"""
    return sparkSession.table("products")


@nodes.backfill_source(tag="products", pipeline="ecommerce_analytics", env=["backfill"])
def products_backfill(
    sparkSession: SparkSession, _context: dict[str, str]
) -> DataFrame:
    """Backfill - products table is typically not filtered for backfills"""
    return sparkSession.table("products")


@tests.source(tag="products", env=["dev", "ci", "staging", "prod", "backfill"])
def products_test(products_df: DataFrame) -> None:
    """Common tests for products source across all environments"""
    # Test that we have data
    assert products_df.count() > 0, "Products table should not be empty"

    # Test schema - all required columns exist
    columns = products_df.columns
    required_columns = ["product_id", "product_name", "category", "list_price"]
    for column in required_columns:
        assert column in columns, f"Required column '{column}' is missing"

    # Test data quality - no nulls in non-nullable fields
    assert products_df.filter(col("product_id").isNull()).count() == 0, (
        "product_id should not have nulls"
    )
    assert products_df.filter(col("product_name").isNull()).count() == 0, (
        "product_name should not have nulls"
    )
    assert products_df.filter(col("category").isNull()).count() == 0, (
        "category should not have nulls"
    )

    # Test business logic - price should be positive
    assert products_df.filter(col("list_price") <= 0).count() == 0, (
        "list_price should be positive"
    )

    # Test uniqueness - product_id should be unique
    total_count = products_df.count()
    distinct_count = products_df.select("product_id").distinct().count()
    assert total_count == distinct_count, "product_id should be unique"
