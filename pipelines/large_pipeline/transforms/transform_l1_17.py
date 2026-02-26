from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_17",
    pipeline="large_pipeline",
    depends_on=["source_6", "source_3", "source_2"],
)
def transform_l1_17(source_6_df: DataFrame, source_3_df: DataFrame, source_2_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_17.

    Args:
        source_6_df: Input DataFrame from source_6
        source_3_df: Input DataFrame from source_3
        source_2_df: Input DataFrame from source_2

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_6, source_3, source_2
    _ = (source_6_df, source_3_df, source_2_df)
    return source_6_df  # Simplified union/join


@tests.unit(tag="transform_l1_17")
def test_transform_l1_17_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_17"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_17", env=["dev"])
def test_transform_l1_17_integration(transform_l1_17_df: DataFrame) -> None:
    """Integration test for transform_l1_17"""
    assert transform_l1_17_df.count() > 0
