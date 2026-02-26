from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_18",
    pipeline="large_pipeline",
    depends_on=["source_4", "source_7"],
)
def transform_l1_18(source_4_df: DataFrame, source_7_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_18.

    Args:
        source_4_df: Input DataFrame from source_4
        source_7_df: Input DataFrame from source_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_4, source_7
    _ = (source_4_df, source_7_df)
    return source_4_df  # Simplified union/join


@tests.unit(tag="transform_l1_18")
def test_transform_l1_18_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_18"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_18", env=["dev"])
def test_transform_l1_18_integration(transform_l1_18_df: DataFrame) -> None:
    """Integration test for transform_l1_18"""
    assert transform_l1_18_df.count() > 0
