import os

from honeycomb import hive, meta
from honeycomb.alter_table import build_partition_strings
from honeycomb.create_table_new.create_table import create_table
from honeycomb.__danger import __nuke_table


temp_table_name_template = '{}_temp_orc_conv'
temp_schema = 'landing'


def create_orc_table_from_df(df, table_name, schema, col_defs,
                             bucket, path, filename,
                             col_comments=None, table_comment=None,
                             partitioned_by=None, partition_values=None,
                             avro_schema=None):
    """
    Wrapper around the additional steps required for creating an ORC table
    from a DataFrame, as opposed to any other storage format

    Args:
        df (pd.DataFrame):
        table_name (str):
        schema (str):
        bucket (str):
        path (str):
        filename (str):
        col_comments (dict<str:str>):
        table_comment (str):
        partitioned_by (dict<str:str>):
        partition_values (dict<str:str>):
        avro_schema ():
    """

    # Create temp table to store data in prior to ORC conversion
    temp_table_name = temp_table_name_template.format(table_name)
    temp_storage_type = 'parquet'
    temp_filename = replace_file_extension(filename, temp_storage_type)
    create_table(df, temp_table_name, temp_schema, col_defs,
                 temp_storage_type, bucket, path, temp_filename,
                 auto_upload_df=True, avro_schema=avro_schema)

    try:
        # Create actual ORC table
        storage_type = 'orc'
        create_table(df, table_name, schema, col_defs,
                     storage_type, bucket, path, filename,
                     col_comments, table_comment,
                     partitioned_by, partition_values,
                     auto_upload_df=False)

        insert_into_orc_table(table_name, schema, temp_table_name, temp_schema)
    finally:
        __nuke_table(temp_table_name, temp_schema)


def append_df_to_orc_table(df, table_name, schema,
                           bucket, path, filename,
                           partition_values=None, avro_schema=None):
    """
    Wrapper around the additional steps required for appending a DataFrame
    to an ORC table, as opposed to any other storage format
    Args:
        df (pd.DataFrame):
        table_name (str):
        schema (str):
        bucket (str):
        path (str):
        filename (str):
        partition_values (dict<str:str>):
        avro_schema ():
    """
    temp_table_name = temp_table_name_template.format(table_name)
    temp_storage_type = 'parquet'
    temp_filename = replace_file_extension(filename, temp_storage_type)

    col_defs = meta.get_table_column_order(
        table_name, schema, include_dtypes=True).to_string(header=False)

    create_table(df, temp_table_name, temp_schema, col_defs,
                 temp_storage_type, bucket, path, temp_filename,
                 auto_upload_df=True, avro_schema=avro_schema)
    try:
        insert_into_orc_table(table_name, schema, temp_table_name, temp_schema,
                              partition_values)
    finally:
        __nuke_table(temp_table_name, temp_schema)


def replace_file_extension(filename, new_storage_type):
    """ Replaces extension of filename with the new storage_type """
    return os.path.splitext(filename)[0] + '.' + new_storage_type


def insert_into_orc_table(table_name, schema, temp_table_name, temp_schema,
                          partition_values=None):
    """
    Inserts all the values in a particular table into its corresponding ORC
    table. We can't simple do a SELECT *, because that will include partition
    columns, which cannot be included in an INSERT statement (since they're
    technically metadata, rather than part of the dataset itself)

    Args:
        table_name (str):
        schema (str):
        temp_table_name (str):
        temp_schema (str):
        partition_values (dict<str:str>):
    """
    # This discludes partition columns, which is desired behavior
    col_names = meta.get_table_column_order(table_name, schema)
    partition_strings = (
        ' PARTITION ({})'.format(build_partition_strings(partition_values))
        if partition_values
        else ''
    )
    insert_command = (
        'INSERT INTO {}.{}{}\n'.format(schema, table_name, partition_strings) +
        'SELECT\n'
        '    {}\n'.format(',\n    '.join(col_names)) +
        'FROM {}.{}'.format(temp_schema, temp_table_name)
    )

    hive.run_lake_query(insert_command)
