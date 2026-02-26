from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_6",
    pipeline="large_pipeline",
    depends_on=["transform_l2_13", "transform_l2_8", "transform_l2_20"],
)
def transform_l3_6(
    transform_l2_13_df: DataFrame, transform_l2_8_df: DataFrame, transform_l2_20_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l3_6.

    Args:
        transform_l2_13_df: Input DataFrame from transform_l2_13
        transform_l2_8_df: Input DataFrame from transform_l2_8
        transform_l2_20_df: Input DataFrame from transform_l2_20

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_13, transform_l2_8, transform_l2_20
    _ = (transform_l2_13_df, transform_l2_8_df, transform_l2_20_df)
    return transform_l2_13_df  # Simplified union/join


@tests.unit(tag="transform_l3_6")
def test_transform_l3_6_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_6"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_6", env=["dev"])
def test_transform_l3_6_integration(transform_l3_6_df: DataFrame) -> None:
    """Integration test for transform_l3_6"""
    assert transform_l3_6_df.count() > 0
