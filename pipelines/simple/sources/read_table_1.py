from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType
from requete import nodes, tests


@nodes.source(tag="read_table_1", pipeline="simple", env=["dev"])
def pyspark_dev(sparkSession: SparkSession) -> DataFrame:
    """Load table_1 data for dev environment.

    Creates a test DataFrame with sample data for development and testing.

    Returns:
        DataFrame with columns: a (int), b (int)
    """
    schema = StructType(
        [StructField("a", IntegerType(), True), StructField("b", IntegerType(), True)]
    )
    data = [(1, 11), (2, 12), (1, 13)]
    return sparkSession.createDataFrame(data, schema)  # pyright: ignore[reportUnknownMemberType]


@tests.source(tag="read_table_1", env=["dev", "staging", "prod", "backfill"])
def common_test(read_table_1_df: DataFrame):
    """Validate that read_table_1 produces non-empty data."""
    assert read_table_1_df.count() > 0


@nodes.source(tag="read_table_1", pipeline="simple", env=["staging", "prod"])
def pyspark_notdev(sparkSession: SparkSession) -> DataFrame:
    """Load table_1 data for staging and prod environments.

    Reads from the actual table_1 table in the database.

    Returns:
        DataFrame with columns: a, b
    """
    return sparkSession.table("table_1").select("a", "b")


@nodes.backfill_source(tag="read_table_1", pipeline="simple", env=["backfill"])
def pyspark_backfill(sparkSession: SparkSession, _context: dict[str, str]) -> DataFrame:
    """Load table_1 data for backfill operations.

    Supports date-range filtering via context parameters for historical data loading.

    Args:
        sparkSession: Spark session
        context: Dictionary containing backfill parameters (from_date, to_date)

    Returns:
        DataFrame with filtered historical data
    """
    # Access CLI args from dict
    # start = context.get('from_date')
    # end = context.get('to_date')
    # from pyspark.sql.functions import col
    # return sparkSession.table("events").filter(
    #     (col("date_column") >= start) & (col("date_column") < end)
    # )

    # Placeholder until you have date columns
    return sparkSession.table("table_1").limit(1)
