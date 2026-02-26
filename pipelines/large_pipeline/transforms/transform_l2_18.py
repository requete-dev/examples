from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_18",
    pipeline="large_pipeline",
    depends_on=["transform_l1_14", "transform_l1_9", "transform_l1_16"],
)
def transform_l2_18(
    transform_l1_14_df: DataFrame, transform_l1_9_df: DataFrame, transform_l1_16_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_18.

    Args:
        transform_l1_14_df: Input DataFrame from transform_l1_14
        transform_l1_9_df: Input DataFrame from transform_l1_9
        transform_l1_16_df: Input DataFrame from transform_l1_16

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_14, transform_l1_9, transform_l1_16
    _ = (transform_l1_14_df, transform_l1_9_df, transform_l1_16_df)
    return transform_l1_14_df  # Simplified union/join


@tests.unit(tag="transform_l2_18")
def test_transform_l2_18_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_18"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_18", env=["dev"])
def test_transform_l2_18_integration(transform_l2_18_df: DataFrame) -> None:
    """Integration test for transform_l2_18"""
    assert transform_l2_18_df.count() > 0
