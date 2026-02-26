from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_10",
    pipeline="large_pipeline",
    depends_on=["source_5"],
)
def transform_l1_10(source_5_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_10.

    Args:
        source_5_df: Input DataFrame from source_5

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_5
    return source_5_df


@tests.unit(tag="transform_l1_10")
def test_transform_l1_10_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_10"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_10", env=["dev"])
def test_transform_l1_10_integration(transform_l1_10_df: DataFrame) -> None:
    """Integration test for transform_l1_10"""
    assert transform_l1_10_df.count() > 0
