from honeycomb import hive, meta
from honeycomb.alter_table import build_partition_strings
from honeycomb.inform import inform


def build_and_run_analysis_command(table_name, schema,
                                   partition_clause='', columns_clause=''):
    analyze_command = (
        'ANALYZE TABLE {}.{} {}COMPUTE STATISTICS{}'
    ).format(schema, table_name, partition_clause, columns_clause)

    inform(analyze_command)
    hive.run_lake_query(analyze_command)


def analyze_table(table_name, schema):
    """

    Args:
        table_name (str):
        schema (str):
    """
    build_and_run_analysis_command(table_name, schema)


def analyze_columns(table_name, schema, columns=None):
    """

    Args:
        table_name (str):
        schema (str):
        columns (list<str>, optional):
    """
    columns_clause = get_columns_clause(columns)

    build_and_run_analysis_command(table_name, schema,
                                   columns_clause=columns_clause)


def analyze_partitions(table_name, schema, partition_values):
    """

    Args:
        table_name (str):
        schema (str):
        partition_values (dict<str:str>):
    """
    partition_clause = get_partition_clause(table_name, schema,
                                            partition_values)

    build_and_run_analysis_command(table_name, schema,
                                   partition_clause=partition_clause)


def analyze_partition_columns(table_name, schema,
                              partition_values, columns=None):
    """
    Args:
        table_name (str):
        schema (str):
        partition_values (dict<str:str>):
    """
    partition_clause = get_partition_clause(table_name, schema,
                                            partition_values)
    columns_clause = get_columns_clause(columns)

    build_and_run_analysis_command(table_name, schema,
                                   partition_clause=partition_clause,
                                   columns_clause=columns_clause)


def fully_analyze_table(table_name, schema):
    """
    """
    if not meta.is_partitioned_table(table_name, schema):
        analyze_table(table_name, schema)
        analyze_columns(table_name, schema)
    else:
        partition_clause = get_partition_clause(partition_values={})
        columns_clause = get_columns_clause(columns=None)

        build_and_run_analysis_command(table_name, schema,
                                       partition_clause=partition_clause,
                                       columns_clause=columns_clause)


def get_columns_clause(columns):
    """
    """
    if columns is None:
        columns = 'COLUMNS'
    else:
        columns = ', '.join(columns)
    columns_clause = 'FOR {}'.format(columns)
    return columns_clause


def get_partition_clause(table_name, schema, partition_values):
    """
    """
    partition_cols = meta.get_partition_cols(table_name, schema)
    for col in partition_cols:
        if col not in partition_values:
            partition_values[col] = ''

    partition_strings = build_partition_strings(partition_values)
    partition_clause = 'PARTITION ({}) '.format(partition_strings)

    return partition_clause
