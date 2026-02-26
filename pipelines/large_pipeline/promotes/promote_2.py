from pyspark.sql import DataFrame
from requete import nodes, tests


@nodes.promote(tag="promote_2", pipeline="large_pipeline", depends_on=["transform_l3_6"], env=["dev"])
def promote_2(transform_l3_6_df: DataFrame) -> None:
    """Promotes data for promote_2.

    Args:
        transform_l3_6_df: Input DataFrame to promote from transform_l3_6
    """
    transform_l3_6_df.createOrReplaceTempView("temp_promote_2_promoted")


@tests.promotion(tag="promote_2", env=["dev"])
def test_promote_2_promotion(transform_l3_6_df: DataFrame) -> None:
    """Promotion test for promote_2"""
    assert transform_l3_6_df.count() > 0
