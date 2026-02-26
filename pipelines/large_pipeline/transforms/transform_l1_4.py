from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_4",
    pipeline="large_pipeline",
    depends_on=["source_6"],
)
def transform_l1_4(source_6_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_4.

    Args:
        source_6_df: Input DataFrame from source_6

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_6
    return source_6_df


@tests.unit(tag="transform_l1_4")
def test_transform_l1_4_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_4"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_4", env=["dev"])
def test_transform_l1_4_integration(transform_l1_4_df: DataFrame) -> None:
    """Integration test for transform_l1_4"""
    assert transform_l1_4_df.count() > 0
