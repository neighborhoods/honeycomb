from honeycomb import run_query as run


def check_schema_existence(schema_name):
    show_schemas_query = (
        'SHOW SCHEMAS LIKE \'{schema_name}\''.format(schema_name=schema_name)
    )

    similar_schemas = run.lake_query(show_schemas_query)
    if similar_schemas is not None:
        # NOTE: 'database' and 'schema' are interchangeable terms in Hive
        if schema_name in similar_schemas['database_name']:
            return True
    return False


def check_table_existence(schema_name, table_name, engine='presto'):
    """
    Checks if a specific table exists in a specific schema

    Args:
        schema_name (str): Which schema to check for the table in
        table_name (str): The name of the table to check for

    Returns:
        bool: Whether or not the specified table exists
    """
    show_tables_query = (
        'SHOW TABLES IN {schema_name} LIKE \'{table_name}\''.format(
            schema_name=schema_name,
            table_name=table_name)
    )

    similar_tables = run.lake_query(show_tables_query, engine='hive')
    if table_name in similar_tables['tab_name'].values:
        return True
    return False
