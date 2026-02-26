from pyspark.sql import DataFrame
from requete import nodes, tests


@nodes.promote(tag="promote_1", pipeline="large_pipeline", depends_on=["transform_l3_9"], env=["dev"])
def promote_1(transform_l3_9_df: DataFrame) -> None:
    """Promotes data for promote_1.

    Args:
        transform_l3_9_df: Input DataFrame to promote from transform_l3_9
    """
    transform_l3_9_df.createOrReplaceTempView("temp_promote_1_promoted")


@tests.promotion(tag="promote_1", env=["dev"])
def test_promote_1_promotion(transform_l3_9_df: DataFrame) -> None:
    """Promotion test for promote_1"""
    assert transform_l3_9_df.count() > 0
