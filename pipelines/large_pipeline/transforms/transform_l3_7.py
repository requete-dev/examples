from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_7",
    pipeline="large_pipeline",
    depends_on=["transform_l2_14", "transform_l2_12", "transform_l2_8"],
)
def transform_l3_7(
    transform_l2_14_df: DataFrame, transform_l2_12_df: DataFrame, transform_l2_8_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l3_7.

    Args:
        transform_l2_14_df: Input DataFrame from transform_l2_14
        transform_l2_12_df: Input DataFrame from transform_l2_12
        transform_l2_8_df: Input DataFrame from transform_l2_8

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_14, transform_l2_12, transform_l2_8
    _ = (transform_l2_14_df, transform_l2_12_df, transform_l2_8_df)
    return transform_l2_14_df  # Simplified union/join


@tests.unit(tag="transform_l3_7")
def test_transform_l3_7_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_7"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_7", env=["dev"])
def test_transform_l3_7_integration(transform_l3_7_df: DataFrame) -> None:
    """Integration test for transform_l3_7"""
    assert transform_l3_7_df.count() > 0
