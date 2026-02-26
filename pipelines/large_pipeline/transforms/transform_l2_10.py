from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_10",
    pipeline="large_pipeline",
    depends_on=["transform_l1_7"],
)
def transform_l2_10(transform_l1_7_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l2_10.

    Args:
        transform_l1_7_df: Input DataFrame from transform_l1_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_7
    return transform_l1_7_df


@tests.unit(tag="transform_l2_10")
def test_transform_l2_10_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_10"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_10", env=["dev"])
def test_transform_l2_10_integration(transform_l2_10_df: DataFrame) -> None:
    """Integration test for transform_l2_10"""
    assert transform_l2_10_df.count() > 0
