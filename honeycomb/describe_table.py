from honeycomb import run_query as run


# Hive and Presto return 'DESCRIBE' queries differently, and
# Presto does not support the 'FORMATTED' keyword, so
# we're locking the engine for 'DESCRIBE' queries to Hive for now
def describe_table(table_name, schema_name='experimental',
                   include_metadata=False):
    """
    Retrieves the description of a specific table in hive

    Args:
        table_name (str): The name of the table to be queried
        schema_name (str): The name of the schema to search for the table in
    Returns:
        desc (pd.DataFrame): A dataframe containing descriptive information
            on the specified table
    """
    engine = 'hive'
    desc_query = 'DESCRIBE {formatted}{schema_name}.{table_name}'.format(
        formatted=('FORMATTED ' if include_metadata else ''),
        schema_name=schema_name,
        table_name=table_name)
    desc = run.lake_query(desc_query, engine)
    return desc
