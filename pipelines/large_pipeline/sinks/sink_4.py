from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(tag="sink_4", pipeline="large_pipeline", depends_on=["transform_l3_8"], env=["dev"])
def sink_4(transform_l3_8_df: DataFrame) -> None:
    """Writes data for sink_4.

    Args:
        transform_l3_8_df: Input DataFrame to sink from transform_l3_8
    """
    transform_l3_8_df.createOrReplaceTempView("transform_l3_8_df")
