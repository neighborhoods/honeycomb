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
                             avro_schema=None):
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
        avro_schema (dict):
            Schema to use when writing a DataFrame to an Avro file. If not
            provided, one will be auto-generated.
    """

    # Create temp table to store data in prior to ORC conversion
    temp_table_name = temp_table_name_template.format(table_name)
    temp_path = temp_table_name_template.format(path[:-1]) + '/'
    temp_filename = replace_file_extension(filename)

    build_and_run_ddl_stmt(df, temp_table_name, temp_schema, col_defs,
                           temp_storage_type, bucket, temp_path, temp_filename,
                           auto_upload_df=True, avro_schema=avro_schema)

    try:
        # Create actual ORC table
        storage_type = 'orc'
        build_and_run_ddl_stmt(df, table_name, schema, col_defs,
                               storage_type, bucket, path, filename,
                               col_comments, table_comment,
                               partitioned_by, partition_values,
                               auto_upload_df=False)

        insert_into_orc_table(table_name, schema, temp_table_name, temp_schema,
                              partition_values)
    finally:
        __nuke_table(temp_table_name, temp_schema)


def append_df_to_orc_table(df, table_name, schema,
                           bucket, path, filename,
                           partition_values=None, avro_schema=None):
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
        avro_schema (dict, optional):
            Schema to use when writing a DataFrame to an Avro file. If not
            provided, one will be auto-generated. Only used if df contains
            complex types.
    """
    temp_table_name = temp_table_name_template.format(table_name)
    temp_path = temp_table_name_template.format(path[:-1]) + '/'
    temp_filename = replace_file_extension(filename)

    col_defs = meta.get_table_column_order(
        table_name, schema, include_dtypes=True)

    build_and_run_ddl_stmt(df, temp_table_name, temp_schema, col_defs,
                           temp_storage_type, bucket, temp_path, temp_filename,
                           auto_upload_df=True, avro_schema=avro_schema)
    try:
        insert_into_orc_table(table_name, schema, temp_table_name, temp_schema,
                              partition_values)
    finally:
        __nuke_table(temp_table_name, temp_schema)


def replace_file_extension(filename):
    """ Replaces extension of filename with the temp storage_type """
    return os.path.splitext(filename)[0] + '.' + temp_storage_type


def insert_into_orc_table(table_name, schema, source_table_name, source_schema,
                          partition_values=None, matching_partitions=False):
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
    """
    # This discludes partition columns, which is desired behavior
    col_names = meta.get_table_column_order(table_name, schema)
    partition_strings = (
        ' PARTITION ({})'.format(build_partition_strings(partition_values))
        if partition_values
        else ''
    )

    where_clause = ''
    if matching_partitions:
        where_clause = '\nWHERE ' + ' AND '.join(
            ['{}="{}"'.format(partition_key, partition_value)
             for partition_key, partition_value in partition_values.items()])
    insert_command = (
        'INSERT INTO {}.{}{}\n'.format(schema, table_name, partition_strings) +
        'SELECT\n'
        '    {}\n'.format(',\n    '.join(col_names)) +
        'FROM {}.{}'.format(source_schema, source_table_name) +
        '{}'.format(where_clause)
    )

    inform(insert_command)

    hive.run_lake_query(insert_command)
