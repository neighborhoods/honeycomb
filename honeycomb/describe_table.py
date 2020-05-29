from honeycomb import run_query


def describe_table(table_name, schema_name='experimental', engine='presto'):
    """
    Retrieves the description of a specific table in hive

    Args:
        table_name (str): The name of the table to be queried
        schema_name (str): The name of the schema to search for the table in
    Returns:
        desc (pd.DataFrame): A dataframe containing descriptive information
            on the specified table
    """
    desc_query = 'DESCRIBE EXTENDED {schema_name}.{table_name}'.format(
        schema_name=schema_name,
        table_name=table_name)
    desc = run_query(desc_query)
    return desc
