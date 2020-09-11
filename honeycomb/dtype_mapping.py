import logging

import pandas as pd
from pandas.core.dtypes.api import (is_datetime64_any_dtype,
                                    is_datetime64_dtype,
                                    is_datetime64tz_dtype)

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


def convert_to_spec_timezones(df, datetime_cols, spec_timezones):
    """
    Converts any columns with an entry in 'spec_timezones' to that timezone

    Args:
        df (pd.DataFrame): Dataframe being operated on
        datetime_cols (list<str>): List of datetime columns in the dataframe
        spec_timezones (dict<str, str>):
            Dictionary from datetime columns to the timezone they
            represent. If the column is timezone-naive, it will have the
            timezone added to its metadata, leaving the times themselves
            unmodified. If the column is timezone-aware, the timezone
            will be converted, likely modifying the stored times.
    """
    if spec_timezones:
        for col, timezone in spec_timezones.items():
            if col not in datetime_cols:
                logging.warning(
                    'Column "{}" included in timezones dictionary '
                    'but is not present in the dataframe.'.format(col))
            # If datetime is timezone-aware, use tz_convert
            if is_datetime64tz_dtype(df.dtypes[col]):
                df[col] = df[col].dt.tz_convert(timezone)
            # Else, use tz_localize
            elif is_datetime64_dtype(df.dtypes[col]):
                df[col] = df[col].dt.tz_localize(timezone)


def make_datetimes_timezone_naive(df, datetime_cols, schema):
    """
    Makes all datetimes timezone-naive. This automatically converts them to
    UTC, while dropping the timezone from their metadata. All times in the lake
    will be in UTC - Hive has limited support for timezones by design - so
    having a notion of timezone is unnecessary.

    Args:
        df (pd.DataFrame): Dataframe being operated on
        datetime_cols (list<str>): List of datetime columns in the dataframe
        schema (str): The schema of the table the df is being uploaded to
    """
    for col in datetime_cols:
        if is_datetime64tz_dtype(df.dtypes[col]):
            df[col] = df[col].dt.tz_convert(None)
        elif schema != 'experimental':
            raise TypeError('All datetime columns in non-experimental tables '
                            'must be timezone-aware.')


def special_dtype_handling(df, spec_dtypes, spec_timezones,
                           schema, copy_df=True):
    """
    Wrapper around functions for special handling of specific dtypes

    Args:
        Universal:
            df (pd.DataFrame): Dataframe being operated on
            spec_dtypes (dict<str:np.dtype or str>):
                a dict from column names to dtypes
            schema (str): The schema of the table the df is being uploaded to
            copy_df (bool):
                Whether the operations should be performed on the original df
                or a copy. Keep in mind that if this is set to False,
                the original df passed in will be modified as well - twice as
                memory efficient, but may be undesirable if the df is needed
                again later
        For datetimes:
            spec_timezones (dict<str, str>):
                Dictionary from datetime columns to the timezone they
                represent. If the column is timezone-naive, it will have the
                timezone added to its metadata, leaving the times themselves
                unmodified. If the column is timezone-aware, the timezone
                will be converted, likely modifying the stored times.

    """
    if copy_df:
        df = df.copy()

    df = apply_spec_dtypes(df, spec_dtypes)

    # All datetime columns, regardless of timezone naive/aware
    datetime_cols = [col for col in df.columns
                     if is_datetime64_any_dtype(df.dtypes[col])]

    convert_to_spec_timezones(df, datetime_cols, spec_timezones)
    make_datetimes_timezone_naive(df, datetime_cols, schema)

    return df


def apply_spec_dtypes(df, spec_dtypes):
    """
    Maps specified columns in a DataFrame to another dtype

    Args:
        df (pd.DataFrame): The DataFrame to perform casting on
        spec_dtypes (dict<str:np.dtype or str>):
            a dict from column names to dtypes
    Returns:
        df (pd.DataFrame): The DataFrame with casting applied
    Raises:
        TypeError: If casting 'df' to the new types fails
    """
    if spec_dtypes is not None:
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


def map_pd_to_db_dtypes(df, storage_type):
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
        complex_cols = db_dtypes.index[db_dtypes.eq('COMPLEX')]
        df.loc[:, complex_cols], db_dtypes = handle_complex_dtypes(
            df[complex_cols], db_dtypes, storage_type)
    return db_dtypes


def handle_complex_dtypes(df_complex, db_dtypes, storage_type):
    """
    """
    for col in df_complex.columns:
        df_complex.loc[:, col], db_dtypes.loc[col] = handle_complex_col(
            df_complex[col], storage_type)

    df_complex = storage_type_post_processing(
        df_complex, db_dtypes, storage_type)
    return df_complex, db_dtypes


def handle_complex_col(col, storage_type):
    """
    """
    reduced_type = reduce_complex_type(col)

    if reduced_type == 'string':
        return col, 'STRING'
    elif reduced_type == 'list':
        return handle_array_col(col, storage_type)
    elif reduced_type == 'dict':
        return handle_struct_col(col, storage_type)


def reduce_complex_type(col):
    """
    """
    python_types = col.apply(type)
    if all(python_types.isin([str, type(None)])):
        return 'string'
    elif all(python_types.isin([list, type(None)])):
        return 'list'
    elif all(python_types.isin([dict, type(None)])):
        return 'dict'
    else:
        raise TypeError(
            'Values passed to complex column "{}" are either of '
            'unsupported types of mixed types. Currently supported '
            'complex types are "STRING", ARRAY (list) and '
            '"STRUCT" (dictionary). Columns must contain '
            'homogenous types.'.format(col.name))


def handle_array_col(col, storage_type):
    """
    """
    dtype_str = 'ARRAY <'
    array_series = pd.Series()
    for array in col:
        array_series = array_series.append(pd.Series(array))
    array_dtype = dtype_map[array_series.dtype.name]
    if array_dtype == 'COMPLEX':
        reduced_type = reduce_complex_type(array_series)
        if reduced_type == 'string':
            array_dtype = 'STRING'

        elif reduced_type == 'list':
            array_list = []
            for row in col:
                for list in row:
                    array_list.append(list)
            _, array_dtype = handle_array_col(
                pd.Series(array_list), storage_type)

        elif reduced_type == 'dict':
            struct_list = []
            for row in col:
                for dict in row:
                    struct_list.append(dict)
            _, array_dtype = handle_struct_col(
                pd.Series(struct_list), storage_type)

    dtype_str += array_dtype + '>'
    return col, dtype_str


def handle_struct_col(col, storage_type):
    """
    """
    dtype_str = 'STRUCT <'
    struct_df = pd.DataFrame()
    for struct in col:
        struct_df = struct_df.append(
            pd.DataFrame.from_records([struct]))
    struct_dtypes = map_pd_to_db_dtypes(struct_df, storage_type)
    dtype_str += ', '.join(
        ['{}: {}'.format(col_name, col_type)
         for col_name, col_type in struct_dtypes.items()])

    dtype_str += '>'

    return col, dtype_str


def storage_type_post_processing(df_complex, db_dtypes, storage_type):
    """
    """
    if any(db_dtypes.str.contains('ARRAY|STRUCT')):
        if storage_type == 'avro':
            raise TypeError('Complex types are not yet supported in '
                            'the Avro storage format.')
        if storage_type == 'csv':
            array_count = db_dtypes.str.count('ARRAY')
            struct_count = db_dtypes.str.count('STRUCT')
            if any(array_count + struct_count) > 1:
                raise TypeError('Nested lists/dicts are not currently '
                                'supported in the CSV storage format.')

            array_columns = db_dtypes[db_dtypes.str.contains('ARRAY')].index
            df_complex.loc[:, array_columns] = (
                df_complex.loc[:, array_columns].apply(
                    lambda x: x.apply(
                        lambda y: '|'.join([str(z) for z in y])
                    )
                )
            )

            struct_columns = db_dtypes[db_dtypes.str.contains('STRUCT')].index
            df_complex.loc[:, struct_columns] = (
                df_complex.loc[:, struct_columns].apply(
                    lambda x: x.apply(
                        lambda y: str(list(y.values()))[1:-1].replace(
                            ', ', '|')
                    )
                )
            )
        if storage_type == 'pq':
            if any(db_dtypes.str.contains('ARRAY')):
                raise TypeError('Lists are not currently supported in the '
                                'Parquet storage format.')
    return df_complex
