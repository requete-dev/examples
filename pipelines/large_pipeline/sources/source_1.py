from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from requete import nodes, tests


@nodes.source(tag="source_1", pipeline="large_pipeline", env=["dev"])
def source_1(sparkSession: SparkSession) -> DataFrame:
    """Reads data for source_1.

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


@tests.source(tag="source_1", env=["dev"])
def source_1_test(source_1_df: DataFrame) -> None:
    """Test source_1 source properties."""
    assert source_1_df.count() > 0, "source_1 table should not be empty"
    assert source_1_df.filter(col("id").isNull()).count() == 0, "id should not have nulls"
