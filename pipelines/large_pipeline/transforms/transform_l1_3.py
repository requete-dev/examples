from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_3",
    pipeline="large_pipeline",
    depends_on=["source_5", "source_9"],
)
def transform_l1_3(source_5_df: DataFrame, source_9_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_3.

    Args:
        source_5_df: Input DataFrame from source_5
        source_9_df: Input DataFrame from source_9

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_5, source_9
    _ = (source_5_df, source_9_df)
    return source_5_df  # Simplified union/join


@tests.unit(tag="transform_l1_3")
def test_transform_l1_3_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_3"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_3", env=["dev"])
def test_transform_l1_3_integration(transform_l1_3_df: DataFrame) -> None:
    """Integration test for transform_l1_3"""
    assert transform_l1_3_df.count() > 0
