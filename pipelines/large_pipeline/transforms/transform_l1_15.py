from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_15",
    pipeline="large_pipeline",
    depends_on=["source_8", "source_7"],
)
def transform_l1_15(source_8_df: DataFrame, source_7_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_15.

    Args:
        source_8_df: Input DataFrame from source_8
        source_7_df: Input DataFrame from source_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_8, source_7
    _ = (source_8_df, source_7_df)
    return source_8_df  # Simplified union/join


@tests.unit(tag="transform_l1_15")
def test_transform_l1_15_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_15"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_15", env=["dev"])
def test_transform_l1_15_integration(transform_l1_15_df: DataFrame) -> None:
    """Integration test for transform_l1_15"""
    assert transform_l1_15_df.count() > 0
