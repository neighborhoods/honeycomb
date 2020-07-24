import river as rv

from honeycomb import check, meta


def append_table(df, table_name, schema=None, filename=None,
                 allow_mismatched_cols=False):
    """
    Uploads a dataframe to S3 and appends it to an already existing table.
    Queries existing table metadata to

    Args:
        df (pd.DataFrame): Which schema to check for the table in
        table_name (str): The name of the table to be created
        schema (str, optional): Name of the schema to create the table in
        filename (str, optional):
            Name to store the file under. Can be left blank if writing to the
            experimental zone, in which case a name will be generated.
        allow_mismatched_cols (bool, default False):
            Whether extra/missing columns should be allowed and handled, or
            if they should lead to an error being raised.
    """
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
    if not path.endswith('/'):
        path += '/'
    path += filename

    if rv.exists(path, bucket):
        raise KeyError('A file already exists at s3://' + bucket + path + ', '
                       'Which will be overwritten by this operation. '
                       'Specify a different filename to proceed.')

    df = reorder_columns_for_appending(df, table_name, schema,
                                       allow_mismatched_cols)

    storage_settings = meta.storage_type_specs[storage_type]['settings']
    rv.write(df, path, bucket, **storage_settings)


def reorder_columns_for_appending(df, table_name, schema,
                                  allow_mismatched_cols):
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
        allow_mismatched_cols (bool):
            Whether extra/missing columns should be allowed and handled, or
            if they should lead to an error being raised.
    """
    table_col_order = meta.get_table_column_order(table_name, schema)
    if sorted(table_col_order) == sorted(df.columns):
        df = df[table_col_order]
    elif allow_mismatched_cols:
        cols_missing_from_df = [col for col in table_col_order
                                if col not in df.columns]
        df = df.assign(**{col: None for col in cols_missing_from_df})

        extra_cols_in_df = [col for col in df.columns
                            if col not in table_col_order]

        new_df_col_order = table_col_order + extra_cols_in_df
        return df[new_df_col_order]
    else:
        raise ValueError('The provided dataframe\'s columns do not match '
                         'the columns of the table. To ignore this and '
                         'proceed anyway, set "allow_mismatched_cols" '
                         'to True.')
