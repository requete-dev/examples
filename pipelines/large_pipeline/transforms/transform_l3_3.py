from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_3",
    pipeline="large_pipeline",
    depends_on=["transform_l2_18", "transform_l2_5"],
)
def transform_l3_3(transform_l2_18_df: DataFrame, transform_l2_5_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l3_3.

    Args:
        transform_l2_18_df: Input DataFrame from transform_l2_18
        transform_l2_5_df: Input DataFrame from transform_l2_5

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_18, transform_l2_5
    _ = (transform_l2_18_df, transform_l2_5_df)
    return transform_l2_18_df  # Simplified union/join


@tests.unit(tag="transform_l3_3")
def test_transform_l3_3_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_3"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_3", env=["dev"])
def test_transform_l3_3_integration(transform_l3_3_df: DataFrame) -> None:
    """Integration test for transform_l3_3"""
    assert transform_l3_3_df.count() > 0
