from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import countDistinct, sum
from requete import nodes, tests


@nodes.transform(tag="group_by", pipeline="simple", depends_on=["join_tables"])
def grp(join_tables_df: DataFrame) -> DataFrame:
    """Group joined data by column a and count distinct values of c.

    Args:
        join_tables_df: Joined table data with columns a, c

    Returns:
        DataFrame with columns: a, c_modified (distinct count of c)
    """
    return join_tables_df.groupBy("a").agg(countDistinct("c").alias("c_modified"))


@tests.unit(tag="group_by")
def test_0(sparkSession: SparkSession):
    """Unit test for group_by transformation logic."""
    data = [(1, 11), (2, 12), (1, 13)]
    columns = ["a", "c"]
    input_df: DataFrame = sparkSession.createDataFrame(data, columns)  # pyright: ignore[reportUnknownMemberType]
    output_df = grp(input_df)
    assert output_df.count() == 2


@tests.integration(tag="group_by", env=["dev"])
def test_1(group_by_df: DataFrame):
    """Validate group_by output count in dev environment."""
    assert group_by_df.count() == 2


@tests.integration(tag="group_by", env=["staging"])
def test_2(group_by_df: DataFrame):
    """Validate group_by output has multiple rows in staging."""
    assert group_by_df.count() > 1


@tests.integration(tag="group_by", env=["dev", "staging"])
def test_3(group_by_df: DataFrame):
    """Validate aggregated c_modified sum is positive."""
    row = group_by_df.agg(sum("c_modified").alias("c_sum")).first()
    assert row is not None, "DataFrame was empty, aggregation returned no rows"
    assert row["c_sum"] > 1
