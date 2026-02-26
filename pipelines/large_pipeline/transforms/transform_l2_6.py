from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_6",
    pipeline="large_pipeline",
    depends_on=["transform_l1_7"],
)
def transform_l2_6(transform_l1_7_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l2_6.

    Args:
        transform_l1_7_df: Input DataFrame from transform_l1_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_7
    return transform_l1_7_df


@tests.unit(tag="transform_l2_6")
def test_transform_l2_6_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_6"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_6", env=["dev"])
def test_transform_l2_6_integration(transform_l2_6_df: DataFrame) -> None:
    """Integration test for transform_l2_6"""
    assert transform_l2_6_df.count() > 0
