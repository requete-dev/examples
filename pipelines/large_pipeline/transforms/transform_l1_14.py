from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_14",
    pipeline="large_pipeline",
    depends_on=["source_2", "source_1", "source_3"],
)
def transform_l1_14(source_2_df: DataFrame, source_1_df: DataFrame, source_3_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_14.

    Args:
        source_2_df: Input DataFrame from source_2
        source_1_df: Input DataFrame from source_1
        source_3_df: Input DataFrame from source_3

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_2, source_1, source_3
    _ = (source_2_df, source_1_df, source_3_df)
    return source_2_df  # Simplified union/join


@tests.unit(tag="transform_l1_14")
def test_transform_l1_14_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_14"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_14", env=["dev"])
def test_transform_l1_14_integration(transform_l1_14_df: DataFrame) -> None:
    """Integration test for transform_l1_14"""
    assert transform_l1_14_df.count() > 0
