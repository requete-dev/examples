from pyspark.sql import DataFrame
from requete import nodes


@nodes.transform(tag="join_tables", pipeline="simple", depends_on=["read_table_1", "read_table_2"])
def join(read_table_1_df: DataFrame, read_table_2_df: DataFrame) -> DataFrame:
    """Join table_1 and table_2 on column b.

    Performs an inner join between the two input tables and selects relevant columns.

    Args:
        read_table_1_df: First input table with columns a, b
        read_table_2_df: Second input table with columns b, c

    Returns:
        DataFrame with columns: a, c
    """
    return read_table_1_df.join(read_table_2_df, on="b", how="inner").select("a", "c")
