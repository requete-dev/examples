from pyspark.sql import DataFrame, SparkSession
from requete import nodes, tests


@nodes.transform(
    tag="transform_l3_10",
    pipeline="large_pipeline",
    depends_on=["transform_l2_19", "transform_l2_4", "transform_l2_20"],
)
def transform_l3_10(
    transform_l2_19_df: DataFrame, transform_l2_4_df: DataFrame, transform_l2_20_df: DataFrame
) -> DataFrame:
    """Transforms data for transform_l3_10.

    Args:
        transform_l2_19_df: Input DataFrame from transform_l2_19
        transform_l2_4_df: Input DataFrame from transform_l2_4
        transform_l2_20_df: Input DataFrame from transform_l2_20

    Returns:
        The transformed DataFrame
    """
    # Depends on: transform_l2_19, transform_l2_4, transform_l2_20
    _ = (transform_l2_19_df, transform_l2_4_df, transform_l2_20_df)
    return transform_l2_19_df  # Simplified union/join


@tests.unit(tag="transform_l3_10")
def test_transform_l3_10_unit(sparkSession: SparkSession) -> None:
    """Unit test for transform_l3_10"""
    _ = sparkSession
    assert True


@tests.integration(tag="transform_l3_10", env=["dev"])
def test_transform_l3_10_integration(transform_l3_10_df: DataFrame) -> None:
    """Integration test for transform_l3_10"""
    assert transform_l3_10_df.count() > 0
