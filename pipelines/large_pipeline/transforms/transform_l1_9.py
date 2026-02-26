from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_9",
    pipeline="large_pipeline",
    depends_on=["source_3"],
)
def transform_l1_9(source_3_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_9.

    Args:
        source_3_df: Input DataFrame from source_3

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_3
    return source_3_df


@tests.unit(tag="transform_l1_9")
def test_transform_l1_9_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_9"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_9", env=["dev"])
def test_transform_l1_9_integration(transform_l1_9_df: DataFrame) -> None:
    """Integration test for transform_l1_9"""
    assert transform_l1_9_df.count() > 0
