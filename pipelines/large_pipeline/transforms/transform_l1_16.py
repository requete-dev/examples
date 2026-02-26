from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_16",
    pipeline="large_pipeline",
    depends_on=["source_2"],
)
def transform_l1_16(source_2_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_16.

    Args:
        source_2_df: Input DataFrame from source_2

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_2
    return source_2_df


@tests.unit(tag="transform_l1_16")
def test_transform_l1_16_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_16"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_16", env=["dev"])
def test_transform_l1_16_integration(transform_l1_16_df: DataFrame) -> None:
    """Integration test for transform_l1_16"""
    assert transform_l1_16_df.count() > 0
