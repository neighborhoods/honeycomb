import pandas as pd

"""
The pandas dtype 'timedelta64[ns]' can be mapped to the hive dtype 'INTERVAL',
but 'INTERVAL' is only available as a return value from querying - it cannot
be the dtype of a full column in a table. As a result, it is not included here

The pandas dtype 'category' is for categorical variables, but hive does not
have native support for categorical types. As a result, it is not included here
"""
dtype_map = {
    'object': 'COMPLEX',
    'int64': 'BIGINT',
    'float64': 'DOUBLE',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP',
}


def apply_spec_dtypes(df, spec_dtypes):
    """
    Maps specified columns in a DataFrame to another dtype

    Args:
        df (pd.DataFrame): The DataFrame to perform casting on
        spec_dtypes (dict<str:np.dtype or str): a dict from column names to
        dtypes
    Returns:
        df (pd.DataFrame): The DataFrame with casting applied
    Raises:
        TypeError: If casting 'df' to the new types fails
    """
    for col_name, new_dtype in spec_dtypes.items():
        if col_name not in df.dtypes.keys():
            raise KeyError('Additional dtype casting failed: '
                           '\'{col_name}\' not in DataFrame.'.format(
                               col_name=col_name))
        try:
            df[col_name] = df[col_name].astype(new_dtype)
        except TypeError as e:
            raise TypeError('Casting column \'{col_name}\' to type '
                            '\'{new_dtype}\' failed.'.format(
                                col_name=col_name,
                                new_dtype=new_dtype)) from e
    return df


def map_pd_to_db_dtypes(df):
    """
    Creates a mapping from the dtypes in a DataFrame to their corresponding
    dtypes in Hive

    Args:
        df (pd.DataFrame): The DataFrame to pull dtypes from
    Returns:
        db_dtypes (dict<str:str>): A dict from column names to database dtypes
    """
    if any(df.dtypes == 'category'):
        raise TypeError('Pandas\' \'categorical\' type is not supported. '
                        'Contact honeycomb devs for further info.')
    if any(df.dtypes == 'timedelta64[ns]'):
        raise TypeError('Pandas\' \'timedelta64[ns]\' type is not supported. '
                        'Contact honeycomb devs for further info.')
    db_dtypes = df.dtypes.copy()

    for orig_type, new_type in dtype_map.items():
        # dtypes can be compared to their string representations for equality
        db_dtypes[db_dtypes == orig_type] = new_type

    if any(db_dtypes.eq('COMPLEX')):
        db_dtypes = handle_complex_dtypes(
            df.loc[:, db_dtypes.eq('COMPLEX')], db_dtypes)
    return db_dtypes


def handle_complex_dtypes(df_complex_cols, db_dtypes):
    db_dtypes = {col: type for col, type in zip(db_dtypes.index, db_dtypes)}
    for col in df_complex_cols.columns:
        python_types = df_complex_cols[col].apply(type)

        if all(python_types.isin([type(None)])):
            db_dtypes[col] = 'STRING'
        if all(python_types.isin([str, type(None)])):
            db_dtypes[col] = 'STRING'
        elif all(python_types.isin([dict, type(None)])):
            dtype_str = 'STRUCT <'
            struct_df = pd.DataFrame()
            for struct in df_complex_cols[col]:
                struct_df = struct_df.append(
                    pd.DataFrame.from_records([struct]))
            struct_dtypes = map_pd_to_db_dtypes(struct_df)
            dtype_str += ', '.join(
                ['{}: {}'.format(col_name, col_type)
                 for col_name, col_type in struct_dtypes.items()])

            dtype_str += '>'
            db_dtypes[col] = dtype_str
        else:
            raise TypeError()
    return db_dtypes
