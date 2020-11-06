import river as rv

from honeycomb import check, meta, dtype_mapping
from honeycomb.alter_table import add_partition


def append_df_to_table(df, table_name, schema=None, dtypes=None,
                       filename=None, overwrite_file=False, timezones=None,
                       copy_df=True, partition_values=None,
                       require_identical_columns=True):
    """
    Uploads a dataframe to S3 and appends it to an already existing table.
    Queries existing table metadata to

    Args:
        df (pd.DataFrame): Which schema to check for the table in
        table_name (str): The name of the table to be created
        schema (str, optional): Name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        filename (str, optional):
            Name to store the file under. Can be left blank if writing to the
            experimental zone, in which case a name will be generated.
        overwrite_file (bool):
            Whether to overwrite the file if a file with a matching name
            to "filename" is already present in S3.
        timezones (dict<str, str>):
            Dictionary from datetime columns to the timezone they
            represent. If the column is timezone-naive, it will have the
            timezone added to its metadata, leaving the times themselves
            unmodified. If the column is timezone-aware and is in a different
            timezone than the one that is specified, the column's timezone
            will be converted, modifying the original times.
        copy_df (bool):
            Whether the operations performed on df should be performed on the
            original or a copy. Keep in mind that if this is set to False,
            the original df passed in will be modified as well - twice as
            memory efficient, but may be undesirable if the df is needed
            again later
        partition_values (dict<str:str>, optional):
            List of tuples containing partition keys and values to
            store the dataframe under. If there is no partiton at the value,
            it will be created.
        require_identical_columns (bool, default True):
            Whether extra/missing columns should be allowed and handled, or
            if they should lead to an error being raised.
    """
    if copy_df:
        df = df.copy()

    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    table_exists = check.table_existence(table_name, schema)
    if not table_exists:
        raise ValueError(
            'Table \'{schema}.{table_name}\' does not exist. '.format(
                schema=schema,
                table_name=table_name))

    table_metadata = meta.get_table_metadata(table_name, schema)

    bucket = table_metadata['bucket']
    path = table_metadata['path']
    storage_type = table_metadata['storage_type']

    if filename is None:
        filename = meta.gen_filename_if_allowed(schema, storage_type)
    if not filename.endswith(storage_type):
        raise ValueError(
            'The type specified in the filename does not match the '
            'filetype of the table.'
        )
    if not path.endswith('/'):
        path += '/'

    if partition_values:
        path += add_partition(table_name, schema, partition_values)

    path += filename

    if rv.exists(path, bucket) and not overwrite_file:
        raise KeyError('A file already exists at s3://{}/{}, '
                       'Which will be overwritten by this operation. '
                       'Specify a different filename to proceed.'.format(
                           bucket, path
                       ))

    df.columns = df.columns.str.lower()
    df = dtype_mapping.special_dtype_handling(
        df, spec_dtypes=dtypes, spec_timezones=timezones, schema=schema)
    df = reorder_columns_for_appending(df, table_name, schema,
                                       partition_values,
                                       require_identical_columns)

    storage_settings = meta.storage_type_specs[storage_type]['settings']
    rv.write(df, path, bucket, show_progressbar=False, **storage_settings)


def reorder_columns_for_appending(df, table_name, schema,
                                  partition_values,
                                  require_identical_columns):
    """
    Serialized formats such as Parquet don't necessarily have to worry
    about column order, but text-based formats like CSV rely entirely
    on column order to designate which column of a table each dataframe
    column maps to. As a result, ensuring that the dataframe has the same
    column order as the table is critical when using those formats.

    Because this operation is relatively inexpensive, we will perform it
    regardless of storage type, to prevent any potential issues.

    If there are extra columns in a dataframe, they will be ignored.
    Missing columns in a dataframe will be filled with 'None' when queried,
    but can cause incorrect mapping from dataframe column to table column
    if the missing columns aren't at the end of the column order.

    Args:
        df (pd.DataFrame): The dataframe to reorder
        table_name (str): The name of the table to be created
        schema (str): Name of the schema to create the table in
        partition_values (dict<str:str>, optional):
            List of tuples containing partition keys and values to
            store the dataframe under. If there is no partiton at the value,
            it will be created.
        require_identical_columns (bool):
            Whether extra/missing columns should be allowed and handled, or
            if they should lead to an error being raised.
    """
    table_col_order = meta.get_table_column_order(table_name, schema)
    if sorted(table_col_order) == sorted(df.columns):
        return df[table_col_order]

    elif not require_identical_columns:
        cols_missing_from_df = [col for col in table_col_order
                                if col not in df.columns]
        df = df.assign(**{col: None for col in cols_missing_from_df})

        extra_cols_in_df = [col for col in df.columns
                            if col not in table_col_order]

        new_df_col_order = table_col_order + extra_cols_in_df
        return df[new_df_col_order]
    else:
        in_table_not_df = set(table_col_order).difference(set(df.columns))
        in_df_not_table = set(df.columns).difference(set(table_col_order))
        raise ValueError(
            'The provided dataframe\'s columns do not match '
            'the columns of the table. To ignore this and '
            'proceed anyway, set "require_identical_columns" '
            'to False.\n'
            'Columns in table, but not dataframe: {}\n'
            'Columns in dataframe, but not table: {}'
            .format(in_table_not_df, in_df_not_table))
