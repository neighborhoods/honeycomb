from honeycomb.config import dtype_map


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
    new_dtypes = df.dtypes
    for col_name, new_dtype in spec_dtypes:
        if col_name not in new_dtypes.keys():
            print('Additional dtype casting for failed: '
                  '{col_name} not in DataFrame.'.format(col_name=col_name))
        else:
            new_dtypes[col_name] = new_dtype

    try:
        df = df.astype(new_dtypes)
        return df
    except ValueError:
        print('Casting to default or specified dtypes failed.')


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
        raise TypeError("Pandas' 'categorical' type is not currently "
                        "supported. Contact honeycomb devs for further info.")
    db_dtypes = df.dtypes

    for orig_type, new_type in dtype_map.items():
        # dtypes can be compared to their string representations for equality
        db_dtypes[db_dtypes == orig_type] = new_type

    return db_dtypes
