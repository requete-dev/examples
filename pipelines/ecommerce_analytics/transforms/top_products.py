from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.functions import sum as spark_sum
from requete import nodes, tests


@nodes.transform(
    tag="top_products",
    pipeline="ecommerce_analytics",
    depends_on=["enrich_orders"],
)
def rank_products(enrich_orders_df: DataFrame) -> DataFrame:
    """
    Ranks products by revenue within each category.
    Returns top 10 products per category using window functions.
    """
    # Calculate product-level metrics
    product_metrics = enrich_orders_df.groupBy(
        "product_id", "product_name", "category", "list_price"
    ).agg(
        spark_sum("quantity").alias("units_sold"),
        spark_sum("total_amount").alias("revenue"),
        count("order_id").alias("order_count"),
    )

    # Rank products within each category by revenue
    window_spec = Window.partitionBy("category").orderBy(col("revenue").desc())

    ranked = product_metrics.withColumn("rank", row_number().over(window_spec))

    # Filter to top 10 per category
    top_products = ranked.filter(col("rank") <= 10).orderBy("category", "rank")

    return top_products


@tests.unit(tag="top_products")
def test_ranking(sparkSession: SparkSession) -> None:
    """Test that products are correctly ranked by revenue within categories"""
    # Create test enriched orders
    data = [
        (1, "Product A", "Electronics", 100.0, 1000.0, 10),
        (2, "Product B", "Electronics", 100.0, 500.0, 5),
        (3, "Product C", "Home", 50.0, 800.0, 8),
        (4, "Product D", "Home", 50.0, 300.0, 3),
    ]
    enrich_orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        data,
        [
            "product_id",
            "product_name",
            "category",
            "list_price",
            "total_amount",
            "quantity",
        ],
    )

    result = rank_products(enrich_orders)

    assert result.count() == 4

    # Check Electronics category rankings
    electronics = (
        result.filter(col("category") == "Electronics").orderBy("rank").collect()
    )
    assert electronics[0]["product_name"] == "Product A"
    assert electronics[0]["rank"] == 1
    assert electronics[1]["rank"] == 2


@tests.unit(tag="top_products")
def test_top_10_limit(sparkSession: SparkSession) -> None:
    """Test that only top 10 products per category are returned"""
    # Create 15 products in one category
    data = [(i, f"Product {i}", "Test", 10.0, 100.0 - i, 1) for i in range(1, 16)]
    enrich_orders: DataFrame = sparkSession.createDataFrame(  # pyright: ignore[reportUnknownMemberType]
        data,
        [
            "product_id",
            "product_name",
            "category",
            "list_price",
            "total_amount",
            "quantity",
        ],
    )

    result = rank_products(enrich_orders)

    assert result.count() == 10
    assert result.filter(col("rank") > 10).count() == 0
