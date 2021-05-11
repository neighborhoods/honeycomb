from honeycomb import hive, meta
from honeycomb.alter_table import build_partition_strings
from honeycomb.inform import inform

"""
Notes/Instructions

General Notes:
    a) There are two types of analysis: table-level (overall table statistics)
       and column-level (specific to each column). For the biggest performance
       boost, both should be performed.
    b) Analysis is unnecessary on any tables populated via an INSERT OVERWRITE
       command, as Hive is configured to automatically compute statistics
       on such tables.

    c) Analysis of a non-partitioned table can be done with a single command.
       Full analysis of a partitioned table requires that each partition
       is analyzed. This can be accomplished by running an analysis command
       for each partition, or by running a single analysis command, providing
       no specific values for the partition columns.

    d) Using the syntax for analyzing an entire non-partitioned table
       appears to work for analyzing an entire partitioned tables as well.
       However, the official Hive documentation states that this
       should not work, and as a result we are disallowing it.
       This has the additional benefit of ensuring that users are fully
       aware of the operation they're performing prior to running the command.

How to provide partition values:
    a) If an exact value is provided for each partition column, then a
       single partition will be analyzed. This is the anticipated
       primary use case.
    b) If some partition columns are given no value, partitions will
       be selected for any value of that column. For example, if a table
       is partitioned by month and day, and a value is provided for month,
       the analysis will be run for all partitions that match
       the month value regardless of its day value (in other words,
       for every day in the month)
    c) If no values are provided for any partition columns, every partition
       will be analyzed

How to provide column values:
    a) If no values are provided for columns, then analysis will be run
       on every column in the table/partition. This is the anticipated
       primary use case.
    b) If columns are provided, analysis will be ran on only those columns
       in the table/partition.
"""


def analyze_table(table_name, schema=None):
    """
    Convenience function for doing table-level analysis for a
    non-partitioned table. Cannot be used on a partitioned table, which
    must have each partition analyzed individually
    Args:
        table_name (str): The name of the table to analyze
        schema (str): The schema that contains the table
    """
    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if meta.is_partitioned_table(table_name, schema):
        raise TypeError((
            'The table {}.{} is partitioned. Use the '
            '"analyze_partitions" function instead.'
        ).format(schema, table_name))
    build_and_run_analysis_command(table_name, schema)


def analyze_columns(table_name, schema=None, columns=[]):
    """
    Convenienct function for doing column-level analysis for a
    non-partitioned table

    Args:
        table_name (str): The name of the table to analyze
        schema (str): The schema that contains the table
        columns (list<str>):
            The columns the user wants statistics to be computed for.
            See documentation at top of file for assembly instructions
    """
    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if meta.is_partitioned_table(table_name, schema):
        raise TypeError((
            'The table {}.{} is partitioned. Use the '
            '"analyze_partition_columns" function instead.'
        ).format(schema, table_name))
    columns_clause = get_columns_clause(columns)

    build_and_run_analysis_command(table_name, schema,
                                   columns_clause=columns_clause)


def analyze_partitions(table_name, schema=None, partition_values={}):
    """
    Convenience function for doing partition-level analysis for as
    partitioned table

    Args:
        table_name (str): The name of the table to analyze
        schema (str): The schema that contains the table
        partition_values (dict<str:str>):
            Dictionary from partition colname to partition value, used
            to filter partitions. See documentation at top of file for
            assembly instructions
    """
    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if not meta.is_partitioned_table(table_name, schema):
        raise TypeError((
            'The table {}.{} is not partitioned. Use the '
            '"analyze_table" function instead.'
        ).format(schema, table_name))

    partition_clause = get_partition_clause(table_name, schema,
                                            partition_values)

    build_and_run_analysis_command(table_name, schema,
                                   partition_clause=partition_clause)


def analyze_partition_columns(table_name, schema=None,
                              partition_values={}, columns=[]):
    """
    Convenience function for doing column-level analysis for a
    partitioned table

    Args:
        table_name (str): The name of the table to analyze
        schema (str): The schema that contains the table
        partition_values (dict<str:str>):
            Dictionary from partition colname to partition value, used
            to filter partitions. See documentation at top of file for
            assembly instructions
        columns (list<str>):
            The columns the user wants statistics to be computed for.
            See documentation at top of file for assembly instructions
    """
    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if not meta.is_partitioned_table(table_name, schema):
        raise TypeError((
            'The table {}.{} is not partitioned. Use the '
            '"analyze_columns" function instead.'
        ).format(schema, table_name))

    partition_clause = get_partition_clause(table_name, schema,
                                            partition_values)
    columns_clause = get_columns_clause(columns)

    build_and_run_analysis_command(table_name, schema,
                                   partition_clause=partition_clause,
                                   columns_clause=columns_clause)


def get_columns_clause(columns):
    """
    Builds a COLUMNS clause of an ANALYZE TABLE command based on
    the columns specified by the user

    Args:
        columns (list<str>):
            The columns the user wants statistics to be computed for.
            See documentation at top of file for assembly instructions
    """
    columns = ', '.join(columns)
    columns_clause = ' FOR COLUMNS {}'.format(columns)
    return columns_clause


def get_partition_clause(table_name, schema, partition_values={}):
    """
    Builds a PARTITION clause of an ANALYZE TABLE command based on
    the partition values specified by the user

    Args:
        partition_values (dict<str:str>):
            Dictionary from partition colname to partition value, used
            to filter partitions. See documentation at top of file for
            assembly instructions
    """
    partition_cols = meta.get_partition_cols(table_name, schema)
    for col in partition_cols:
        if col not in partition_values:
            partition_values[col] = ''

    partition_strings = build_partition_strings(partition_values)
    partition_clause = 'PARTITION({}) '.format(partition_strings)

    return partition_clause


def build_and_run_analysis_command(table_name, schema,
                                   partition_clause='', columns_clause=''):
    """
    Base function for ANALYZE TABLE commands. Builds the command
    based off of the parameters provided by the ANALYZE TABLE convenience
    functions.
    The Hive commands used for the different types of ANALYZE table commands
    are similar enough to be built off of one template, but semantically
    different enough that having separate convenience functions will
    help clarify the intent of the user.

    Args:
        table_name (str): The name of the table to analyze
        schema (str): The schema that contains the table
        partition_clause (str):
            Clause of the command designating which partitions are
            to be analyzed
        columns_clause (str):
            Clause of the command designating which columns are
            to be analyzed
    """
    analyze_command = (
        'ANALYZE TABLE {}.{} {}COMPUTE STATISTICS{}'
    ).format(schema, table_name, partition_clause, columns_clause)

    inform(analyze_command)
    hive.run_lake_query(analyze_command)
