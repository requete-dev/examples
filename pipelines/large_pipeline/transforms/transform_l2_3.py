from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_3",
    pipeline="large_pipeline",
    depends_on=["transform_l1_8", "transform_l1_1", "transform_l1_2"],
)
def transform_l2_3(
    transform_l1_8_df: DataFrame, transform_l1_1_df: DataFrame, transform_l1_2_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_3.

    Args:
        transform_l1_8_df: Input DataFrame from transform_l1_8
        transform_l1_1_df: Input DataFrame from transform_l1_1
        transform_l1_2_df: Input DataFrame from transform_l1_2

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_8, transform_l1_1, transform_l1_2
    _ = (transform_l1_8_df, transform_l1_1_df, transform_l1_2_df)
    return transform_l1_8_df  # Simplified union/join


@tests.unit(tag="transform_l2_3")
def test_transform_l2_3_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_3"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_3", env=["dev"])
def test_transform_l2_3_integration(transform_l2_3_df: DataFrame) -> None:
    """Integration test for transform_l2_3"""
    assert transform_l2_3_df.count() > 0
