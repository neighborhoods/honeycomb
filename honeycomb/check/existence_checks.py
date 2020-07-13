from honeycomb.querying import run_query


def check_schema_existence(schema, engine='presto'):
    show_schemas_query = (
        'SHOW SCHEMAS LIKE \'{schema}\''.format(schema=schema)
    )

    similar_schemas = run_query(show_schemas_query, engine='hive')
    if similar_schemas is not None:
        # NOTE: 'database' and 'schema' are interchangeable terms in Hive
        if schema in similar_schemas['database_name']:
            return True
    return False


def check_table_existence(table_name, schema, engine='presto'):
    """
    Checks if a specific table exists in a specific schema

    Args:
        schema (str): Which schema to check for the table in
        table_name (str): The name of the table to check for

    Returns:
        bool: Whether or not the specified table exists
    """
    show_tables_query = (
        'SHOW TABLES IN {schema} LIKE \'{table_name}\''.format(
            schema=schema,
            table_name=table_name)
    )

    similar_tables = run_query(show_tables_query, engine='hive')
    if table_name in similar_tables['tab_name']:
        return True
    return False
