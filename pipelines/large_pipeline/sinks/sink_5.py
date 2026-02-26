from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(tag="sink_5", pipeline="large_pipeline", depends_on=["transform_l3_1"], env=["dev"])
def sink_5(transform_l3_1_df: DataFrame) -> None:
    """Writes data for sink_5.

    Args:
        transform_l3_1_df: Input DataFrame to sink from transform_l3_1
    """
    transform_l3_1_df.createOrReplaceTempView("transform_l3_1_df")
