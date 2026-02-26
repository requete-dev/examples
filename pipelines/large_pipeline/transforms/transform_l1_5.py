from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_5",
    pipeline="large_pipeline",
    depends_on=["source_7", "source_9", "source_10"],
)
def transform_l1_5(source_7_df: DataFrame, source_9_df: DataFrame, source_10_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_5.

    Args:
        source_7_df: Input DataFrame from source_7
        source_9_df: Input DataFrame from source_9
        source_10_df: Input DataFrame from source_10

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_7, source_9, source_10
    _ = (source_7_df, source_9_df, source_10_df)
    return source_7_df  # Simplified union/join


@tests.unit(tag="transform_l1_5")
def test_transform_l1_5_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_5"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_5", env=["dev"])
def test_transform_l1_5_integration(transform_l1_5_df: DataFrame) -> None:
    """Integration test for transform_l1_5"""
    assert transform_l1_5_df.count() > 0
