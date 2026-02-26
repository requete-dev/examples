from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(tag="sink_1", pipeline="large_pipeline", depends_on=["transform_l3_9"], env=["dev"])
def sink_1(transform_l3_9_df: DataFrame) -> None:
    """Writes data for sink_1.

    Args:
        transform_l3_9_df: Input DataFrame to sink from transform_l3_9
    """
    transform_l3_9_df.createOrReplaceTempView("transform_l3_9_df")
