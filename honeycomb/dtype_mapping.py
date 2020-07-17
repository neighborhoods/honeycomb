dtype_map = {
    'object': 'STRING',
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
    db_dtypes = df.dtypes.copy()

    for orig_type, new_type in dtype_map.items():
        # dtypes can be compared to their string representations for equality
        db_dtypes[db_dtypes == orig_type] = new_type

    return db_dtypes
