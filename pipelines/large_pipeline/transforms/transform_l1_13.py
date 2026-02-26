from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_13",
    pipeline="large_pipeline",
    depends_on=["source_8", "source_1", "source_3"],
)
def transform_l1_13(source_8_df: DataFrame, source_1_df: DataFrame, source_3_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_13.

    Args:
        source_8_df: Input DataFrame from source_8
        source_1_df: Input DataFrame from source_1
        source_3_df: Input DataFrame from source_3

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_8, source_1, source_3
    _ = (source_8_df, source_1_df, source_3_df)
    return source_8_df  # Simplified union/join


@tests.unit(tag="transform_l1_13")
def test_transform_l1_13_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_13"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_13", env=["dev"])
def test_transform_l1_13_integration(transform_l1_13_df: DataFrame) -> None:
    """Integration test for transform_l1_13"""
    assert transform_l1_13_df.count() > 0
