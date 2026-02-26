from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_13",
    pipeline="large_pipeline",
    depends_on=["transform_l1_8", "transform_l1_7", "transform_l1_18"],
)
def transform_l2_13(
    transform_l1_8_df: DataFrame, transform_l1_7_df: DataFrame, transform_l1_18_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_13.

    Args:
        transform_l1_8_df: Input DataFrame from transform_l1_8
        transform_l1_7_df: Input DataFrame from transform_l1_7
        transform_l1_18_df: Input DataFrame from transform_l1_18

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_8, transform_l1_7, transform_l1_18
    _ = (transform_l1_8_df, transform_l1_7_df, transform_l1_18_df)
    return transform_l1_8_df  # Simplified union/join


@tests.unit(tag="transform_l2_13")
def test_transform_l2_13_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_13"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_13", env=["dev"])
def test_transform_l2_13_integration(transform_l2_13_df: DataFrame) -> None:
    """Integration test for transform_l2_13"""
    assert transform_l2_13_df.count() > 0
