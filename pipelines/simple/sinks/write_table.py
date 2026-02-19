from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(
    tag="write",
    pipeline="simple",
    depends_on=["group_by"],
    env=["dev", "staging"],
)
def write_dev(group_by_df: DataFrame) -> None:
    """Dev: write to temp view for validation only."""
    group_by_df.createOrReplaceTempView("temp_table_final")


@nodes.sink(
    tag="write", pipeline="simple", depends_on=["group_by"], env=["prod", "backfill"]
)
def write_prod(group_by_df: DataFrame) -> None:
    """Writes aggregated results to production table."""
    group_by_df.write.option("path", "/tmp/requete/spark/warehouse/table_final").mode(
        "overwrite"
    ).saveAsTable("table_final")
