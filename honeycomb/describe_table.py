import pandas as pd

from honeycomb.connection import get_db_connection


def describe_table(schema_name, table_name, engine='presto'):
    """
    Retrieves the description of a specific table in hive

    Args:
        table_name (str): The name of the table to be queried
        schema_name (str): The name of the schema to search for the table in
    Returns:
        desc (pd.DataFrame): A dataframe containing descriptive information
            on the specified table
    """
    with get_db_connection(engine=engine, cursor=False) as conn:
        desc_query = 'DESCRIBE EXTENDED {schema_name}.{table_name}'.format(
            schema_name=schema_name,
            table_name=table_name)
        desc = pd.read_sql(desc_query, conn)
    return desc
