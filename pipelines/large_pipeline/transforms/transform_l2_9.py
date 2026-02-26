from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_9",
    pipeline="large_pipeline",
    depends_on=["transform_l1_18"],
)
def transform_l2_9(transform_l1_18_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l2_9.

    Args:
        transform_l1_18_df: Input DataFrame from transform_l1_18

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_18
    return transform_l1_18_df


@tests.unit(tag="transform_l2_9")
def test_transform_l2_9_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_9"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_9", env=["dev"])
def test_transform_l2_9_integration(transform_l2_9_df: DataFrame) -> None:
    """Integration test for transform_l2_9"""
    assert transform_l2_9_df.count() > 0
