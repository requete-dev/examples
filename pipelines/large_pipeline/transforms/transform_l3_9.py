from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_9",
    pipeline="large_pipeline",
    depends_on=["transform_l2_17", "transform_l2_3", "transform_l2_2"],
)
def transform_l3_9(
    transform_l2_17_df: DataFrame, transform_l2_3_df: DataFrame, transform_l2_2_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l3_9.

    Args:
        transform_l2_17_df: Input DataFrame from transform_l2_17
        transform_l2_3_df: Input DataFrame from transform_l2_3
        transform_l2_2_df: Input DataFrame from transform_l2_2

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_17, transform_l2_3, transform_l2_2
    _ = (transform_l2_17_df, transform_l2_3_df, transform_l2_2_df)
    return transform_l2_17_df  # Simplified union/join


@tests.unit(tag="transform_l3_9")
def test_transform_l3_9_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_9"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_9", env=["dev"])
def test_transform_l3_9_integration(transform_l3_9_df: DataFrame) -> None:
    """Integration test for transform_l3_9"""
    assert transform_l3_9_df.count() > 0
