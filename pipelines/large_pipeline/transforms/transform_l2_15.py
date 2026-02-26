from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_15",
    pipeline="large_pipeline",
    depends_on=["transform_l1_17", "transform_l1_16", "transform_l1_12"],
)
def transform_l2_15(
    transform_l1_17_df: DataFrame, transform_l1_16_df: DataFrame, transform_l1_12_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_15.

    Args:
        transform_l1_17_df: Input DataFrame from transform_l1_17
        transform_l1_16_df: Input DataFrame from transform_l1_16
        transform_l1_12_df: Input DataFrame from transform_l1_12

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_17, transform_l1_16, transform_l1_12
    _ = (transform_l1_17_df, transform_l1_16_df, transform_l1_12_df)
    return transform_l1_17_df  # Simplified union/join


@tests.unit(tag="transform_l2_15")
def test_transform_l2_15_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_15"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_15", env=["dev"])
def test_transform_l2_15_integration(transform_l2_15_df: DataFrame) -> None:
    """Integration test for transform_l2_15"""
    assert transform_l2_15_df.count() > 0
