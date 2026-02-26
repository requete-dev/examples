from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_8",
    pipeline="large_pipeline",
    depends_on=["source_7", "source_1"],
)
def transform_l1_8(source_7_df: DataFrame, source_1_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_8.

    Args:
        source_7_df: Input DataFrame from source_7
        source_1_df: Input DataFrame from source_1

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_7, source_1
    _ = (source_7_df, source_1_df)
    return source_7_df  # Simplified union/join


@tests.unit(tag="transform_l1_8")
def test_transform_l1_8_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_8"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_8", env=["dev"])
def test_transform_l1_8_integration(transform_l1_8_df: DataFrame) -> None:
    """Integration test for transform_l1_8"""
    assert transform_l1_8_df.count() > 0
