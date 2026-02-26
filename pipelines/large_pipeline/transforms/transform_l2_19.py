from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_19",
    pipeline="large_pipeline",
    depends_on=["transform_l1_2", "transform_l1_18", "transform_l1_7"],
)
def transform_l2_19(
    transform_l1_2_df: DataFrame, transform_l1_18_df: DataFrame, transform_l1_7_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_19.

    Args:
        transform_l1_2_df: Input DataFrame from transform_l1_2
        transform_l1_18_df: Input DataFrame from transform_l1_18
        transform_l1_7_df: Input DataFrame from transform_l1_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_2, transform_l1_18, transform_l1_7
    _ = (transform_l1_2_df, transform_l1_18_df, transform_l1_7_df)
    return transform_l1_2_df  # Simplified union/join


@tests.unit(tag="transform_l2_19")
def test_transform_l2_19_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_19"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_19", env=["dev"])
def test_transform_l2_19_integration(transform_l2_19_df: DataFrame) -> None:
    """Integration test for transform_l2_19"""
    assert transform_l2_19_df.count() > 0
