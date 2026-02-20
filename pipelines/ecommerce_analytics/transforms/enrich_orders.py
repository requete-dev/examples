from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_format
from requete import nodes, tests


@nodes.transform(
    tag="enrich_orders",
    pipeline="ecommerce_analytics",
    depends_on=["orders", "users", "products"],
)
def enrich(orders_df: DataFrame, users_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Enriches orders with user and product information.
    Performs 3-way join and calculates derived fields.
    """
    # Join orders with users (left join to keep all orders)
    enriched = orders_df.join(users_df, on="user_id", how="left")

    # Join with products (left join to keep all orders)
    enriched = enriched.join(products_df, on="product_id", how="left")

    # Calculate total_amount = quantity * unit_price
    enriched = enriched.withColumn("total_amount", col("quantity") * col("unit_price"))

    # Extract order_day and order_month for analytics
    enriched = enriched.withColumn("order_day", col("order_date"))
    enriched = enriched.withColumn("order_month", date_format(col("order_date"), "yyyy-MM"))

    # Select and order columns logically
    return enriched.select(
        # Order information
        "order_id",
        "order_date",
        "order_day",
        "order_month",
        "order_timestamp",
        # User information
        "user_id",
        "user_name",
        "email",
        "country",
        "signup_date",
        # Product information
        "product_id",
        "product_name",
        "category",
        "list_price",
        # Order details
        "quantity",
        "unit_price",
        "total_amount",
    )


@tests.unit(tag="enrich_orders")
def test_basic_enrichment(sparkSession: SparkSession) -> None:
    """Test that enrichment works with basic test data"""
    # Create test orders
    orders_data = [
        (1, 101, 201, datetime(2024, 1, 15), datetime(2024, 1, 15, 10, 30), 2, 99.99),
        (2, 102, 202, datetime(2024, 1, 16), datetime(2024, 1, 16, 14, 45), 1, 149.99),
    ]
    orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        orders_data,
        [
            "order_id",
            "user_id",
            "product_id",
            "order_date",
            "order_timestamp",
            "quantity",
            "unit_price",
        ],
    )

    # Create test users
    users_data = [
        (101, "Alice Smith", "alice@example.com", "USA", datetime(2023, 1, 1)),
        (102, "Bob Jones", "bob@example.com", "Canada", datetime(2023, 2, 1)),
    ]
    users: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        users_data, ["user_id", "user_name", "email", "country", "signup_date"]
    )

    # Create test products
    products_data = [
        (201, "Widget A", "Electronics", 109.99),
        (202, "Widget B", "Home", 159.99),
    ]
    products: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        products_data, ["product_id", "product_name", "category", "list_price"]
    )

    # Call the transform
    result = enrich(orders, users, products)

    # Verify results
    assert result.count() == 2
    assert "total_amount" in result.columns
    assert "order_month" in result.columns


@tests.unit(tag="enrich_orders")
def test_total_amount_calculation(sparkSession: SparkSession) -> None:
    """Test that total_amount is calculated correctly"""
    orders_data = [(1, 101, 201, datetime(2024, 1, 15), datetime(2024, 1, 15, 10, 30), 3, 50.0)]
    orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        orders_data,
        [
            "order_id",
            "user_id",
            "product_id",
            "order_date",
            "order_timestamp",
            "quantity",
            "unit_price",
        ],
    )

    users_data = [(101, "Test User", "test@example.com", "USA", datetime(2023, 1, 1))]
    users: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        users_data, ["user_id", "user_name", "email", "country", "signup_date"]
    )

    products_data = [(201, "Test Product", "Test", 55.0)]
    products: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        products_data, ["product_id", "product_name", "category", "list_price"]
    )

    result = enrich(orders, users, products)
    row = result.first()
    assert row is not None

    assert row["total_amount"] == 150.0
