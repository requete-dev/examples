from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_8",
    pipeline="large_pipeline",
    depends_on=["transform_l1_12", "transform_l1_14", "transform_l1_10"],
)
def transform_l2_8(
    transform_l1_12_df: DataFrame, transform_l1_14_df: DataFrame, transform_l1_10_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_8.

    Args:
        transform_l1_12_df: Input DataFrame from transform_l1_12
        transform_l1_14_df: Input DataFrame from transform_l1_14
        transform_l1_10_df: Input DataFrame from transform_l1_10

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_12, transform_l1_14, transform_l1_10
    _ = (transform_l1_12_df, transform_l1_14_df, transform_l1_10_df)
    return transform_l1_12_df  # Simplified union/join


@tests.unit(tag="transform_l2_8")
def test_transform_l2_8_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_8"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_8", env=["dev"])
def test_transform_l2_8_integration(transform_l2_8_df: DataFrame) -> None:
    """Integration test for transform_l2_8"""
    assert transform_l2_8_df.count() > 0
