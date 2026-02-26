from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(tag="sink_3", pipeline="large_pipeline", depends_on=["transform_l3_4"], env=["dev"])
def sink_3(transform_l3_4_df: DataFrame) -> None:
    """Writes data for sink_3.

    Args:
        transform_l3_4_df: Input DataFrame to sink from transform_l3_4
    """
    transform_l3_4_df.createOrReplaceTempView("transform_l3_4_df")
