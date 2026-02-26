from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_7",
    pipeline="large_pipeline",
    depends_on=["source_9", "source_7", "source_2"],
)
def transform_l1_7(source_9_df: DataFrame, source_7_df: DataFrame, source_2_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_7.

    Args:
        source_9_df: Input DataFrame from source_9
        source_7_df: Input DataFrame from source_7
        source_2_df: Input DataFrame from source_2

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_9, source_7, source_2
    _ = (source_9_df, source_7_df, source_2_df)
    return source_9_df  # Simplified union/join


@tests.unit(tag="transform_l1_7")
def test_transform_l1_7_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_7"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_7", env=["dev"])
def test_transform_l1_7_integration(transform_l1_7_df: DataFrame) -> None:
    """Integration test for transform_l1_7"""
    assert transform_l1_7_df.count() > 0
