from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_12",
    pipeline="large_pipeline",
    depends_on=["source_5", "source_7", "source_9"],
)
def transform_l1_12(source_5_df: DataFrame, source_7_df: DataFrame, source_9_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_12.

    Args:
        source_5_df: Input DataFrame from source_5
        source_7_df: Input DataFrame from source_7
        source_9_df: Input DataFrame from source_9

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_5, source_7, source_9
    _ = (source_5_df, source_7_df, source_9_df)
    return source_5_df  # Simplified union/join


@tests.unit(tag="transform_l1_12")
def test_transform_l1_12_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_12"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_12", env=["dev"])
def test_transform_l1_12_integration(transform_l1_12_df: DataFrame) -> None:
    """Integration test for transform_l1_12"""
    assert transform_l1_12_df.count() > 0
