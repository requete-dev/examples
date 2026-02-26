from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_1",
    pipeline="large_pipeline",
    depends_on=["source_4"],
)
def transform_l1_1(source_4_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_1.

    Args:
        source_4_df: Input DataFrame from source_4

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_4
    return source_4_df


@tests.unit(tag="transform_l1_1")
def test_transform_l1_1_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_1"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_1", env=["dev"])
def test_transform_l1_1_integration(transform_l1_1_df: DataFrame) -> None:
    """Integration test for transform_l1_1"""
    assert transform_l1_1_df.count() > 0
