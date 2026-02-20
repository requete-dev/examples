from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType
from requete import nodes


@nodes.source(tag="read_table_2", pipeline="simple", env=["dev"])
def pyspark_abc(sparkSession: SparkSession) -> DataFrame:
    """Load table_2 data for dev environment.

    Creates test DataFrame with sample data for development.

    Returns:
        DataFrame with columns: b (int), c (int)
    """
    schema = StructType([StructField("b", IntegerType(), True), StructField("c", IntegerType(), True)])
    data = [(11, 21), (12, 22), (13, 23)]
    return sparkSession.createDataFrame(data, schema)  # pyright: ignore[reportUnknownMemberType]


@nodes.source(tag="read_table_2", pipeline="simple", env=["staging", "prod"])
def pyspark_xyz(sparkSession: SparkSession) -> DataFrame:
    """Load table_2 data for staging and prod environments.

    Reads from the actual table_2 table in the database.

    Returns:
        DataFrame with columns: b, c
    """
    return sparkSession.table("table_2").select("b", "c")


@nodes.backfill_source(tag="read_table_2", pipeline="simple", env=["backfill"])
def pyspark_backfill(sparkSession: SparkSession, _context: dict[str, str]) -> DataFrame:
    """Load table_2 data for backfill operations.

    Args:
        sparkSession: Spark session
        context: Backfill context parameters

    Returns:
        DataFrame with columns: b, c
    """
    return sparkSession.table("table_2").select("b", "c")
