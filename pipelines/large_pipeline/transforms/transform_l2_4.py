from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_4",
    pipeline="large_pipeline",
    depends_on=["transform_l1_17", "transform_l1_13"],
)
def transform_l2_4(transform_l1_17_df: DataFrame, transform_l1_13_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l2_4.

    Args:
        transform_l1_17_df: Input DataFrame from transform_l1_17
        transform_l1_13_df: Input DataFrame from transform_l1_13

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_17, transform_l1_13
    _ = (transform_l1_17_df, transform_l1_13_df)
    return transform_l1_17_df  # Simplified union/join


@tests.unit(tag="transform_l2_4")
def test_transform_l2_4_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_4"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_4", env=["dev"])
def test_transform_l2_4_integration(transform_l2_4_df: DataFrame) -> None:
    """Integration test for transform_l2_4"""
    assert transform_l2_4_df.count() > 0
