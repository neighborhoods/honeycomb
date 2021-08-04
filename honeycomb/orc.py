import os

from honeycomb import hive, meta
from honeycomb.alter_table import build_partition_strings
from honeycomb.create_table.build_and_run_ddl_stmt import (
    build_and_run_ddl_stmt
)
from honeycomb.inform import inform
from honeycomb.__danger import __nuke_table


temp_table_name_template = '{}_temp_orc_conv'
temp_storage_type = 'parquet'
temp_schema = 'landing'


def create_orc_table_from_df(df, table_name, schema, col_defs,
                             bucket, path, filename,
                             col_comments=None, table_comment=None,
                             partitioned_by=None, partition_values=None,
                             hive_functions=None):
    """
    Wrapper around the additional steps required for creating an ORC table
    from a DataFrame, as opposed to any other storage format.

    This function is only needed if auto_upload_df in create_table_from_df
    is True. If the DataFrame doesn't need to be immediately uploaded,
    simply creating an ORC table to later append to can be handled there.

    Args:
        df (pd.DataFrame): DataFrame to create the table from
        table_name (str): Name of the table to be created
        schema (str): Schema to create the table in
        bucket (str): Bucket containing the table's files
        path (str): Path within bucket containing the table's files
        filename (str):
            Filename to store file as - this is mostly for debugging purposes,
            because we have no control over what hive names files that it
            writes when it converts data to ORC format. The file will be stored
            under this name only when uploaded to the temp table
        col_comments (dict<str:str>):
            Dictionary from column name keys to column descriptions.
        table_comment (str): Documentation on the table's purpose
        partitioned_by (dict<str:str>,
                        collections.OrderedDict<str:str>, or
                        list<tuple<str:str>>, optional):
            Dictionary or list of tuples containing a partition name and type.
            Cannot be a vanilla dictionary if using Python version < 3.6
        partition_values (dict<str:str>):
            List of tuples containing partition name and value to store
            the dataframe under
        hive_functions (dict<str:str> or dict<str:dict>):
            Specifications on what hive functions to apply to which columns.
            See inspected structure in documentation below
    """

    # Create temp table to store data in prior to ORC conversion
    temp_table_name = temp_table_name_template.format(table_name)
    temp_path = temp_table_name_template.format(path[:-1]) + '/'
    temp_filename = replace_file_extension(filename)

    build_and_run_ddl_stmt(df, temp_table_name, temp_schema, col_defs,
                           temp_storage_type, bucket, temp_path, temp_filename,
                           auto_upload_df=True)

    try:
        # We want the original table to use the original column dtype,
        # so we only apply the expected output type of the hive functions
        # when creating the ORC table
        if hive_functions is not None:
            col_defs = change_col_dtype_to_hive_fn_output(col_defs,
                                                          hive_functions)
        # Create actual ORC table
        storage_type = 'orc'
        build_and_run_ddl_stmt(df, table_name, schema, col_defs,
                               storage_type, bucket, path, filename,
                               col_comments, table_comment,
                               partitioned_by, partition_values,
                               auto_upload_df=False)

        insert_into_orc_table(table_name, schema, temp_table_name, temp_schema,
                              partition_values, hive_functions)
    finally:
        __nuke_table(temp_table_name, temp_schema)


def append_df_to_orc_table(df, table_name, schema,
                           bucket, path, filename,
                           partition_values=None,
                           hive_functions=None):
    """
    Wrapper around the additional steps required for appending a DataFrame
    to an ORC table, as opposed to any other storage format
    Args:
        df (pd.DataFrame): DataFrame to be appended to the ORC table
        table_name (str): Table to append df to
        schema (str): Schema that contains table
        bucket (str): Bucket containing the table's files
        path (str): Path within bucket containing the table's files
        filename (str):
            Filename to store file as - this is mostly for debugging purposes,
            because we have no control over what hive names files that it
            writes when it converts data to ORC format. The file will be stored
            under this name only when uploaded to the temp table
        partition_values (dict<str:str>, optional):
            List of tuples containing partition keys and values to
            store the dataframe under. If there is no partiton at the value,
            it will be created.
        hive_functions (dict<str:str> or dict<str:dict>):
            Specifications on what hive functions to apply to which columns.
            See inspected structure in documentation below
    """
    temp_table_name = temp_table_name_template.format(table_name)
    temp_path = temp_table_name_template.format(path[:-1]) + '/'
    temp_filename = replace_file_extension(filename)

    col_defs = meta.get_table_column_order(
        table_name, schema, include_dtypes=True)

    if hive_functions is not None:
        col_defs = change_col_dtype_to_hive_fn_output(col_defs, hive_functions)

    build_and_run_ddl_stmt(df, temp_table_name, temp_schema, col_defs,
                           temp_storage_type, bucket, temp_path, temp_filename,
                           auto_upload_df=True)
    try:
        insert_into_orc_table(table_name, schema, temp_table_name, temp_schema,
                              partition_values, hive_functions)
    finally:
        __nuke_table(temp_table_name, temp_schema)


def replace_file_extension(filename):
    """ Replaces extension of filename with the temp storage_type """
    return os.path.splitext(filename)[0] + '.' + temp_storage_type


def insert_into_orc_table(table_name, schema, source_table_name, source_schema,
                          partition_values=None, hive_functions=None,
                          matching_partitions=False,
                          allow_hive_reserved_words=False,
                          overwrite=False):
    """
    Inserts all the values in a particular table into its corresponding ORC
    table. We can't simple do a SELECT *, because that will include partition
    columns, which cannot be included in an INSERT statement (since they're
    technically metadata, rather than part of the dataset itself)

    Args:
        table_name (str): The ORC table to be inserted into
        schema (str): The schema that the destination table is stored in
        source_table_name (str): The table to insert from
        source_schema (str): The schema that the source table is stored in
        partition_values (dict<str:str>, Optional):
            The partition in the destination table to insert into
        matching_partitions (bool, default False):
            Whether the partition being inserted to has a matching partition
            in the source table. Used for inserting subsets of a source table
            rather than the entire thing.
        allow_hive_reserved_words (bool, default False):
            By default, Hive will not allow column names to use reserved words.
            This can be surpassed by wrapping the column name in backticks (`).
            Doing so is discouraged, but if the source table makes use of such
            columns and cannot be easily changed, setting this to True will
            allow the table to be inserted from
    """
    # List of reserved words in Hive that could reasonably be used as column
    # names. This list may expand with time
    hive_reserved_words = ['date', 'time', 'timestamp', 'order']

    # This discludes partition columns, which is desired behavior
    col_names = meta.get_table_column_order(table_name, schema)
    partition_strings = (
        ' PARTITION ({})'.format(build_partition_strings(partition_values))
        if partition_values
        else ''
    )

    for i in range(len(col_names)):
        if col_names[i] in hive_reserved_words:
            if allow_hive_reserved_words:
                col_names[i] = '`{}`'.format(col_names[i])
            else:
                raise ValueError(
                    'Source table has columns named with Hive reserved words. '
                    'If this is unavoidable, set "allow_hive_reserved_words" '
                    'to True.'
                )

    if overwrite:
        insert_type = 'OVERWRITE'
    else:
        insert_type = 'INTO'

    if hive_functions is not None:
        col_names = insert_hive_fns_into_col_names(col_names, hive_functions)

    where_clause = ''
    if matching_partitions:
        where_clause = '\nWHERE ' + ' AND '.join(
            ['source_table.{}="{}"'.format(partition_key, partition_value)
             for partition_key, partition_value in partition_values.items()])
    insert_command = (
        'INSERT {} {}.{}{}\n'.format(insert_type, schema, table_name,
                                     partition_strings) +
        'SELECT\n'
        '    {}\n'.format(',\n    '.join(col_names)) +
        'FROM {}.{} source_table'.format(source_schema, source_table_name) +
        '{}'.format(where_clause)
    )

    inform(insert_command)

    hive.run_lake_query(insert_command)


"""
Expected Structure of 'hive_functions' parameter

{
    # If the function takes a column as its only argument
    # and outputs data of the same type as the input column
    'a_column': 'a_hive_function'

    # If the function takes multiple arguments. The list
    # of arguments is order-sensitive. A '.' in the
    # list represents where the column goes in the argument order.
    # Function args meant to be interpreted as string literals by
    # Hive must have an additional set of quotes around them.
    'another_column': {
        'name': 'another_hive_function',
        'args': [1, '.', 2.0]
    }

    # If the function being applied has a different output type
    # than it's input type
    'yet_another_column': {
        'name': 'yet_another_hive_function',
        'output_type': 'different_type'
    }
}
"""


def insert_hive_fns_into_col_names(col_names, hive_functions):
    """
    Inserts Hive functions and (potentially) arguments into a list of column
    names, which will then be used in a CREATE TABLE command for an ORC table.
    Allows for leveraging hive-internal functionality, such as the 'pow'
    or 'unhex' functions, when adding data to an ORC table

    This can also be helpful in applying transformations to data in a way
    that is guaranteeably readable by Hive. For example, the reason for adding
    this functionality was because of a binary-type column, but the values were
    hex-encoded. Rather than making users manually call 'unhex' on the column
    every time they were querying it, this functionality allows for translation
    of the hex-encoded string to raw binary values during the ORC conversion
    process.

    Args:
        col_names (list<str>):
            The list of columns to be selected and inserted into the ORC table
        hive_functions (dict<str:str> or dict<str:dict>):
            Specifications on what hive functions to apply to which columns.
    Returns:
        col_names (list<str>):
            The list of columns with functions inserted
    """
    fn_template = '{}({})'
    for i in range(len(col_names)):
        col = col_names[i]
        if col in hive_functions:
            fn_info = hive_functions[col]
            if isinstance(fn_info, str):
                fn_name = fn_info
                fn_args = col
            elif isinstance(fn_info, dict):
                fn_name = fn_info['name']
                if 'args' in fn_info:
                    fn_args = ', '.join([str(arg) if arg != '.' else col
                                         for arg in fn_info['args']])
                else:
                    fn_args = col

            col_names[i] = fn_template.format(fn_name, fn_args)
    return col_names


def change_col_dtype_to_hive_fn_output(col_defs, hive_functions):
    """
    If Hive functions are being applied during the ORC conversion process,
    it is possible that the resulting column's type will be different than the
    origin table. If this occurs (and the table being inserted to is a
    brand-new table), this function can be used to ensure that the table
    being created & inserted to expects the correct type.

    Args:
        col_defs (pd.DataFrame):
            DataFrame containing column names ('col_name') and
            types ('dtype')
        hive_functions (dict<str:str> or dict<str:dict>):
            Specifications on what hive functions to apply to which columns.
    Returns:
        col_defs (pd.DataFrame):
            The column definitions with modified types according to what is
            output by particular Hive functions
    """
    for col in col_defs['col_name']:
        if col in hive_functions:
            if 'output_type' in hive_functions[col]:
                output_type = hive_functions[col]['output_type']
                col_defs.loc[col_defs['col_name'] == col,
                             'dtype'] = output_type
    return col_defs
