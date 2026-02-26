from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_2",
    pipeline="large_pipeline",
    depends_on=["transform_l2_16", "transform_l2_4", "transform_l2_10"],
)
def transform_l3_2(
    transform_l2_16_df: DataFrame, transform_l2_4_df: DataFrame, transform_l2_10_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l3_2.

    Args:
        transform_l2_16_df: Input DataFrame from transform_l2_16
        transform_l2_4_df: Input DataFrame from transform_l2_4
        transform_l2_10_df: Input DataFrame from transform_l2_10

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_16, transform_l2_4, transform_l2_10
    _ = (transform_l2_16_df, transform_l2_4_df, transform_l2_10_df)
    return transform_l2_16_df  # Simplified union/join


@tests.unit(tag="transform_l3_2")
def test_transform_l3_2_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_2"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_2", env=["dev"])
def test_transform_l3_2_integration(transform_l3_2_df: DataFrame) -> None:
    """Integration test for transform_l3_2"""
    assert transform_l3_2_df.count() > 0
