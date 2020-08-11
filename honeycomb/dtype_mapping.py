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
    # db_dtypes = {col: type for col, type in zip(db_dtypes.index, db_dtypes)}
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
            raise TypeError('Values passed to complex column "{}" are either '
                            'of unsupported types of mixed types. Currently '
                            'supported complex types are "STRING" and '
                            '"STRUCT" (dictionary). Columns must contain '
                            'homogenous types.')
    return db_dtypes
