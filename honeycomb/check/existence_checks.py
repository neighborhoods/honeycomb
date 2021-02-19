from honeycomb import hive


def check_schema_existence(schema):
    """
    Checks if a given schema exists in the lake
    """
    show_schemas_query = (
        'SHOW SCHEMAS LIKE \'{schema}\''.format(schema=schema)
    )

    similar_schemas = hive.run_lake_query(show_schemas_query)
    if similar_schemas is not None:
        # NOTE: 'database' and 'schema' are interchangeable terms in Hive
        if schema in similar_schemas['database_name']:
            return True
    return False


def check_table_existence(table_name, schema):
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

    similar_tables = hive.run_lake_query(show_tables_query, engine='hive')
    if table_name in similar_tables['tab_name'].values:
        return True
    return False


def check_partition_existence(table_name, schema,
                              partition_values):
    """
    Checks if a specific partition exists in a specific table

    Args:
        schema (str): Which schema the table is in
        table_name (str): The name of the table to check in
        partition_values (dict<str:str>):
            A mapping from partition keys to the values being checked for

    Returns:
        bool: Whether or not the specified partition exists in the table
    """
    partition_value_strings = ', '.join(
        ['{}=\'{}\''.format(partition_name, partition_value)
         for partition_name, partition_value in partition_values.items()])
    partition_spec = ' PARTITION({})'.format(partition_value_strings)

    show_partitions_query = (
        'SHOW PARTITIONS {schema}.{table_name}{partition_spec}'.format(
            schema=schema,
            table_name=table_name,
            partition_spec=partition_spec)
    )

    similar_partitions = hive.run_lake_query(
        show_partitions_query, engine='hive')['partition']
    if len(similar_partitions):
        for partition in similar_partitions.str.split('/'):
            existing_partition_values = {
                partition_key: partition_value
                for partition_key, partition_value in [
                    partition_value_str.split('=')
                    for partition_value_str in partition
                ]
            }
            if partition_values == existing_partition_values:
                return True
    return False
