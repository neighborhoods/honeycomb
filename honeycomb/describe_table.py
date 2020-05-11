import pandas as pd
from pyhive import presto


def describe_table(self, schema_name, table_name):
    """
    Retrieves the description of a specific table in hive

    Args:
        table_name (str): The name of the table to be queried
        schema_name (str): The name of the schema to search for the table in
    Returns:
        desc (pd.DataFrame): A dataframe containing descriptive information
            on the specified table
    """
    with presto.connect(self.addr) as conn:
        desc_query = "DESCRIBE EXTENDED {schema_name}.{table_name}".format(
            schema_name=schema_name,
            table_name=table_name)
        desc = pd.read_sql(desc_query, conn)
    return desc
