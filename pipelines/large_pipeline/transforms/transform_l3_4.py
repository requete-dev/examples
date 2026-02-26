from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_4",
    pipeline="large_pipeline",
    depends_on=["transform_l2_4"],
)
def transform_l3_4(transform_l2_4_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l3_4.

    Args:
        transform_l2_4_df: Input DataFrame from transform_l2_4

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_4
    return transform_l2_4_df


@tests.unit(tag="transform_l3_4")
def test_transform_l3_4_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_4"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_4", env=["dev"])
def test_transform_l3_4_integration(transform_l3_4_df: DataFrame) -> None:
    """Integration test for transform_l3_4"""
    assert transform_l3_4_df.count() > 0
