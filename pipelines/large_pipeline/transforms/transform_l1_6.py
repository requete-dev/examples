from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_6",
    pipeline="large_pipeline",
    depends_on=["source_6", "source_3", "source_8"],
)
def transform_l1_6(source_6_df: DataFrame, source_3_df: DataFrame, source_8_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_6.

    Args:
        source_6_df: Input DataFrame from source_6
        source_3_df: Input DataFrame from source_3
        source_8_df: Input DataFrame from source_8

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_6, source_3, source_8
    _ = (source_6_df, source_3_df, source_8_df)
    return source_6_df  # Simplified union/join


@tests.unit(tag="transform_l1_6")
def test_transform_l1_6_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_6"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_6", env=["dev"])
def test_transform_l1_6_integration(transform_l1_6_df: DataFrame) -> None:
    """Integration test for transform_l1_6"""
    assert transform_l1_6_df.count() > 0
