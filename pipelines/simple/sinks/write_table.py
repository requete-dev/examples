from pyspark.sql import DataFrame
from requete import nodes


@nodes.sink(tag="write", pipeline="simple", depends_on=["group_by"], env=["dev"])
def write_dev(group_by_df: DataFrame) -> None:
    """Writes aggregated results to dev table for testing."""
    group_by_df.write.option(
        "path", "/tmp/requete/spark/warehouse/dev_table_final"
    ).mode("overwrite").saveAsTable("dev_table_final")


@nodes.sink(tag="write", pipeline="simple", depends_on=["group_by"], env=["staging"])
def write_staging(group_by_df: DataFrame) -> None:
    """Writes aggregated results to staging table."""
    group_by_df.write.option(
        "path", "/tmp/requete/spark/warehouse/staging_table_final"
    ).mode("overwrite").saveAsTable("staging_table_final")


@nodes.sink(
    tag="write", pipeline="simple", depends_on=["group_by"], env=["prod", "backfill"]
)
def write_prod(group_by_df: DataFrame) -> None:
    """Writes aggregated results to production table."""
    group_by_df.write.option("path", "/tmp/requete/spark/warehouse/table_final").mode(
        "overwrite"
    ).saveAsTable("table_final")
