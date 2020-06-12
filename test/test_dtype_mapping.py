import pytest

import numpy as np
import pandas as pd

from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes


def test_map_pd_to_db_dtypes(test_df_all_types):
    mapped_dtypes = map_pd_to_db_dtypes(test_df_all_types)

    expected_dtypes = pd.Series({
        'intcol': 'INT',
        'strcol': 'STRING',
        'floatcol': 'DOUBLE',
        'boolcol': 'BOOLEAN',
        'datetimecol': 'DATETIME',
        'timedeltacol': 'INTERVAL'
    })

    assert mapped_dtypes.equals(expected_dtypes)


def test_map_pd_to_db_dtypes_categorical_fails():
    df = pd.DataFrame({
        'catcol': pd.Series(pd.Categorical([1, 2, 3, 4], categories=[1, 2, 3]))
    })

    with pytest.raises(TypeError, match='categorical.* not supported'):
        map_pd_to_db_dtypes(df)


def test_apply_spec_dtypes(test_df_all_types):
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
    with pytest.raises(KeyError, match='not in DataFrame'):
        apply_spec_dtypes(test_df_all_types, {'extracol': None})


def test_apply_spec_dtypes_invalid_type(test_df_all_types):
    with pytest.raises(TypeError, match='Casting .* failed'):
        apply_spec_dtypes(test_df_all_types, {'intcol': 'invalid_type'})
