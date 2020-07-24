import pytest

import pandas as pd

from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes


def test_map_pd_to_db_dtypes(test_df_all_types):
    """Tests that dtype mapping behaves as expected under valid conditions"""
    mapped_dtypes = map_pd_to_db_dtypes(test_df_all_types)

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
        map_pd_to_db_dtypes(cat_df)

    td_df = pd.DataFrame({
        'timedeltacol': [pd.Timedelta('1 days'), pd.Timedelta('2 days')]
    })

    with pytest.raises(TypeError, match=r'timedelta64\[ns\].* not supported'):
        map_pd_to_db_dtypes(td_df)


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
