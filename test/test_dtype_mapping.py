import logging
import pytest

import pandas as pd
from pandas.core.dtypes.api import is_datetime64_any_dtype

from honeycomb.dtype_mapping import (apply_spec_dtypes,
                                     map_pd_to_db_dtypes,
                                     convert_to_spec_timezones,
                                     make_datetimes_timezone_naive)


def test_map_pd_to_db_dtypes(test_df_all_types):
    """Tests that dtype mapping behaves as expected under valid conditions"""
    mapped_dtypes = map_pd_to_db_dtypes(test_df_all_types, storage_type='csv')

    expected_dtypes = pd.Series({
        'intcol': 'BIGINT',
        'strcol': 'STRING',
        'floatcol': 'DOUBLE',
        'boolcol': 'BOOLEAN',
        'datetimecol': 'TIMESTAMP',
    })

    assert mapped_dtypes.equals(expected_dtypes)


def test_map_pd_to_db_dtypes_unsupported_fails():
    """
    Tests that dtype mapping fails if a dataframe contains the unsupported
    categorical type
    """
    cat_df = pd.DataFrame({
        'catcol': pd.Series(pd.Categorical([1, 2, 3, 4], categories=[1, 2, 3]))
    })

    with pytest.raises(TypeError, match='categorical.* not supported'):
        map_pd_to_db_dtypes(cat_df, storage_type='csv')

    td_df = pd.DataFrame({
        'timedeltacol': [pd.Timedelta('1 days'), pd.Timedelta('2 days')]
    })

    with pytest.raises(TypeError, match=r'timedelta64\[ns\].* not supported'):
        map_pd_to_db_dtypes(td_df, storage_type='csv')


def test_apply_spec_dtypes(test_df_all_types):
    """
    Tests that applying specified dtypes behaves as expected under
    valid conditions
    """
    casted_df = apply_spec_dtypes(
        test_df_all_types, spec_dtypes={
            'intcol': float,
            'floatcol': str
        })

    expected_df = test_df_all_types.assign(
        intcol=test_df_all_types['intcol'].astype(float),
        floatcol=test_df_all_types['floatcol'].astype(str)
    )
    assert casted_df.equals(expected_df)


def test_apply_spec_dtypes_extra_col(test_df_all_types):
    """
    Tests that applying specified dtypes fails if columns are specfied
    that are not present in the dataframe
    """
    with pytest.raises(KeyError, match='not in DataFrame'):
        apply_spec_dtypes(test_df_all_types, {'extracol': None})


def test_apply_spec_dtypes_invalid_type(test_df_all_types):
    """
    Tests that applying specified dtypes fails if a type that is
    incompatible with pandas is supplied
    """
    with pytest.raises(TypeError, match='Casting .* failed'):
        apply_spec_dtypes(test_df_all_types, {'intcol': 'invalid_type'})


def test_convert_to_spec_timezones_from_timezone_naive(test_df_all_types):
    """
    Tests that a timezone-naive datetime will be localized to a specified
    timezone, without modifying the time hours/minutes of the timestamp
    (the underlying seconds-since-the-epoch, however, will be modified)
    """
    datetime_cols = get_datetime_cols(test_df_all_types)

    converted_col = datetime_cols[0]
    converted_df = test_df_all_types.copy()
    timezones = {converted_col: 'EST'}
    convert_to_spec_timezones(converted_df, datetime_cols, timezones)

    assert converted_df[converted_col].dtype == 'datetime64[ns, EST]'
    assert converted_df[converted_col].dt.date.equals(
        test_df_all_types[converted_col].dt.date)
    assert converted_df[converted_col].dt.time.equals(
        test_df_all_types[converted_col].dt.time)


def test_convert_to_spec_timezones_from_timezone_aware(test_df_all_types):
    """
    Tests that a timezone-aware datetime will be converted to another
    specified timezone if one is provided
    """
    datetime_cols = get_datetime_cols(test_df_all_types)

    converted_col = datetime_cols[0]
    test_df_all_types[converted_col] = (
        test_df_all_types[converted_col].dt.tz_localize('UTC'))

    converted_df = test_df_all_types.copy()

    timezones = {converted_col: 'EST'}

    convert_to_spec_timezones(converted_df, datetime_cols, timezones)

    assert converted_df[converted_col].dtype == 'datetime64[ns, EST]'
    assert converted_df[converted_col].equals(
        test_df_all_types[converted_col].dt.tz_convert('EST'))


def test_convert_to_spec_timezones_addtl_col_warning(mocker,
                                                     test_df_all_types):
    """
    Tests that a warning will be raised if columns not present in the
    dataframe are provided in 'timestamps'
    """
    mocker.patch.object(logging, 'warning')
    datetime_cols = get_datetime_cols(test_df_all_types)

    timezones = {'intcol': 'EST'}

    convert_to_spec_timezones(test_df_all_types, datetime_cols, timezones)
    logging.warning.assert_called_once_with(
        'Column "intcol" included in timezones dictionary '
        'but is not present in the dataframe.')


def test_make_datetimes_timezone_naive(test_df_all_types):
    """
    Tests that a timezone-aware datetime is successfully converted to a
    timezone-naive datetime, measured in UTC seconds since the epoch
    """
    datetime_cols = get_datetime_cols(test_df_all_types)

    converted_col = datetime_cols[0]
    test_df_all_types[converted_col] = (
        test_df_all_types[converted_col].dt.tz_localize('America/Chicago'))

    converted_df = test_df_all_types.copy()

    make_datetimes_timezone_naive(converted_df, datetime_cols, 'experimental')
    assert converted_df[converted_col].equals(
        test_df_all_types[converted_col].dt.tz_convert(None))


def test_make_datetimes_timezone_naive_tznaive_experimental(test_df_all_types):
    """
    Tests that a dataframe with a timezone-naive datetime column will fail
    to be uploaded outside of the experimental zone, but will succeed inside
    the experimental zone.
    """
    datetime_cols = get_datetime_cols(test_df_all_types)
    converted_df = test_df_all_types.copy()
    with pytest.raises(TypeError,
                       match='non-experimental.*must be timezone-aware'):
        make_datetimes_timezone_naive(converted_df,
                                      datetime_cols,
                                      'curated')
    make_datetimes_timezone_naive(converted_df, datetime_cols, 'experimental')
    assert test_df_all_types.equals(converted_df)


def get_datetime_cols(df):
    return [col for col in df.columns
            if is_datetime64_any_dtype(df.dtypes[col])]
