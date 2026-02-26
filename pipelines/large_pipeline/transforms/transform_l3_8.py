from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_8",
    pipeline="large_pipeline",
    depends_on=["transform_l2_2"],
)
def transform_l3_8(transform_l2_2_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l3_8.

    Args:
        transform_l2_2_df: Input DataFrame from transform_l2_2

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_2
    return transform_l2_2_df


@tests.unit(tag="transform_l3_8")
def test_transform_l3_8_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_8"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_8", env=["dev"])
def test_transform_l3_8_integration(transform_l3_8_df: DataFrame) -> None:
    """Integration test for transform_l3_8"""
    assert transform_l3_8_df.count() > 0
