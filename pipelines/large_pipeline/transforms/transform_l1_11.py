from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_11",
    pipeline="large_pipeline",
    depends_on=["source_5", "source_2", "source_7"],
)
def transform_l1_11(source_5_df: DataFrame, source_2_df: DataFrame, source_7_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_11.

    Args:
        source_5_df: Input DataFrame from source_5
        source_2_df: Input DataFrame from source_2
        source_7_df: Input DataFrame from source_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_5, source_2, source_7
    _ = (source_5_df, source_2_df, source_7_df)
    return source_5_df  # Simplified union/join


@tests.unit(tag="transform_l1_11")
def test_transform_l1_11_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_11"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_11", env=["dev"])
def test_transform_l1_11_integration(transform_l1_11_df: DataFrame) -> None:
    """Integration test for transform_l1_11"""
    assert transform_l1_11_df.count() > 0
