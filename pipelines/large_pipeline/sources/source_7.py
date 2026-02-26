from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from requete import nodes, tests


@nodes.source(tag="source_7", pipeline="large_pipeline", env=["dev"])
def source_7(sparkSession: SparkSession) -> DataFrame:
    """Reads data for source_7.

    Args:
        sparkSession: The Spark session

    Returns:
        A DataFrame containing the source data
    """
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("val", StringType(), False),
        ]
    )
    return sparkSession.createDataFrame([(1, "data")], schema)  # pyright: ignore[reportUnknownMemberType]


@tests.source(tag="source_7", env=["dev"])
def source_7_test(source_7_df: DataFrame) -> None:
    """Test source_7 source properties."""
    assert source_7_df.count() > 0, "source_7 table should not be empty"
    assert source_7_df.filter(col("id").isNull()).count() == 0, "id should not have nulls"
