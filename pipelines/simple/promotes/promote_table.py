from pyspark.sql import DataFrame
from requete import nodes, tests


@nodes.promote(
    tag="promote",
    pipeline="simple",
    depends_on=["group_by"],
    env=["dev", "staging"],
)
def promote_dev(group_by_df: DataFrame) -> None:
    """Dev: write to temp view for validation only."""
    group_by_df.createOrReplaceTempView("temp_table_promoted")


@nodes.promote(tag="promote", pipeline="simple", depends_on=["group_by"], env=["prod", "backfill"])
def promote_prod(group_by_df: DataFrame) -> None:
    """Promotes validated data to production promoted table."""
    group_by_df.write.option("path", "/tmp/requete/spark/warehouse/table_promoted").mode("overwrite").saveAsTable(
        "table_promoted"
    )


@tests.promotion(tag="promote", env=["dev"])
def test_promote_dev(group_by_df: DataFrame):
    """Validates data before promotion in dev."""
    assert group_by_df.count() > 0, "Data should not be empty"
    assert "c_modified" in group_by_df.columns, "Expected column c_modified missing"


@tests.promotion(tag="promote", env=["staging"])
def test_promote_staging(group_by_df: DataFrame):
    """Validates data before promotion in staging."""
    assert group_by_df.count() > 0, "Data should not be empty"
    assert group_by_df.filter(group_by_df.c_modified.isNull()).count() == 0, "Nulls found in c_modified"


@tests.promotion(tag="promote", env=["prod", "backfill"])
def test_promote_prod(group_by_df: DataFrame):
    """Validates data before promotion in production."""
    assert group_by_df.count() > 0, "Data should not be empty"
