from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_14",
    pipeline="large_pipeline",
    depends_on=["transform_l1_5"],
)
def transform_l2_14(transform_l1_5_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l2_14.

    Args:
        transform_l1_5_df: Input DataFrame from transform_l1_5

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_5
    return transform_l1_5_df


@tests.unit(tag="transform_l2_14")
def test_transform_l2_14_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_14"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_14", env=["dev"])
def test_transform_l2_14_integration(transform_l2_14_df: DataFrame) -> None:
    """Integration test for transform_l2_14"""
    assert transform_l2_14_df.count() > 0
