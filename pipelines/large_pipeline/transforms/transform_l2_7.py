from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l2_7",
    pipeline="large_pipeline",
    depends_on=["transform_l1_1", "transform_l1_15", "transform_l1_20"],
)
def transform_l2_7(
    transform_l1_1_df: DataFrame, transform_l1_15_df: DataFrame, transform_l1_20_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l2_7.

    Args:
        transform_l1_1_df: Input DataFrame from transform_l1_1
        transform_l1_15_df: Input DataFrame from transform_l1_15
        transform_l1_20_df: Input DataFrame from transform_l1_20

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l1_1, transform_l1_15, transform_l1_20
    _ = (transform_l1_1_df, transform_l1_15_df, transform_l1_20_df)
    return transform_l1_1_df  # Simplified union/join


@tests.unit(tag="transform_l2_7")
def test_transform_l2_7_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l2_7"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l2_7", env=["dev"])
def test_transform_l2_7_integration(transform_l2_7_df: DataFrame) -> None:
    """Integration test for transform_l2_7"""
    assert transform_l2_7_df.count() > 0
