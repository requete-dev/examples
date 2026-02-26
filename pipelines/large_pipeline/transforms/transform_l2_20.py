from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_20",
    pipeline="large_pipeline",
    depends_on=["transform_l1_4", "transform_l1_14", "transform_l1_9"],
)
def transform_l2_20(
    transform_l1_4_df: DataFrame, transform_l1_14_df: DataFrame, transform_l1_9_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_20.

    Args:
        transform_l1_4_df: Input DataFrame from transform_l1_4
        transform_l1_14_df: Input DataFrame from transform_l1_14
        transform_l1_9_df: Input DataFrame from transform_l1_9

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_4, transform_l1_14, transform_l1_9
    _ = (transform_l1_4_df, transform_l1_14_df, transform_l1_9_df)
    return transform_l1_4_df  # Simplified union/join


@tests.unit(tag="transform_l2_20")
def test_transform_l2_20_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_20"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_20", env=["dev"])
def test_transform_l2_20_integration(transform_l2_20_df: DataFrame) -> None:
    """Integration test for transform_l2_20"""
    assert transform_l2_20_df.count() > 0
