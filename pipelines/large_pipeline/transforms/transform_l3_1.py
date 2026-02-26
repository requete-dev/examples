from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_1",
    pipeline="large_pipeline",
    depends_on=["transform_l2_17"],
)
def transform_l3_1(transform_l2_17_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l3_1.

    Args:
        transform_l2_17_df: Input DataFrame from transform_l2_17

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_17
    return transform_l2_17_df


@tests.unit(tag="transform_l3_1")
def test_transform_l3_1_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_1"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_1", env=["dev"])
def test_transform_l3_1_integration(transform_l3_1_df: DataFrame) -> None:
    """Integration test for transform_l3_1"""
    assert transform_l3_1_df.count() > 0
