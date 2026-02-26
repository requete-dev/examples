from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l1_19",
    pipeline="large_pipeline",
    depends_on=["source_7"],
)
def transform_l1_19(source_7_df: DataFrame) -> DataFrame:
    """Transforms data for transform_l1_19.

    Args:
        source_7_df: Input DataFrame from source_7

    Returns:
        The transformed DataFrame
    """
    # Depends on: source_7
    return source_7_df


@tests.unit(tag="transform_l1_19")
def test_transform_l1_19_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l1_19"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l1_19", env=["dev"])
def test_transform_l1_19_integration(transform_l1_19_df: DataFrame) -> None:
    """Integration test for transform_l1_19"""
    assert transform_l1_19_df.count() > 0
