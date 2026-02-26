from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_16",
    pipeline="large_pipeline",
    depends_on=["transform_l1_5", "transform_l1_14"],
)
def transform_l2_16(transform_l1_5_df: DataFrame, transform_l1_14_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l2_16.

    Args:
        transform_l1_5_df: Input DataFrame from transform_l1_5
        transform_l1_14_df: Input DataFrame from transform_l1_14

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_5, transform_l1_14
    _ = (transform_l1_5_df, transform_l1_14_df)
    return transform_l1_5_df  # Simplified union/join


@tests.unit(tag="transform_l2_16")
def test_transform_l2_16_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_16"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_16", env=["dev"])
def test_transform_l2_16_integration(transform_l2_16_df: DataFrame) -> None:
    """Integration test for transform_l2_16"""
    assert transform_l2_16_df.count() > 0
