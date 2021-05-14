from honeycomb.orc import (
    insert_hive_fns_into_col_names, change_col_dtype_to_hive_fn_output)


def test_insert_hive_fns_into_col_names(test_df):
    col_names = list(test_df.columns)
    hive_functions = {
        'intcol': {
            'name': 'pow',
            'args': ['.', 2]
        },
        'strcol': 'upper',
        'floatcol': {
            'name': 'pow',
            'args': [3, '.']
        }
    }

    intcol_ind = col_names.index('intcol')
    strcol_ind = col_names.index('strcol')
    floatcol_ind = col_names.index('floatcol')

    col_names = insert_hive_fns_into_col_names(col_names, hive_functions)
    print(col_names)

    assert col_names[intcol_ind] == 'pow(intcol, 2)'
    assert col_names[strcol_ind] == 'upper(strcol)'
    assert col_names[floatcol_ind] == 'pow(3, floatcol)'


def test_change_col_dtype_to_hive_fn_output(test_df):
    col_defs = (
        test_df.dtypes.to_frame()
        .reset_index()
        .rename(columns={'index': 'col_name', 0: 'dtype'})
    )

    hive_functions = {
        'intcol': {
            'name': 'to_binary',
            'output_type': 'BINARY'
        },
        'strcol': 'upper',
        'floatcol': {
            'name': 'hex',
            'output_type': 'STRING'
        }
    }

    intcol_ind = col_defs.index[col_defs['col_name'] == 'intcol'][0]
    strcol_ind = col_defs.index[col_defs['col_name'] == 'strcol'][0]
    floatcol_ind = col_defs.index[col_defs['col_name'] == 'floatcol'][0]

    col_defs = change_col_dtype_to_hive_fn_output(col_defs, hive_functions)

    assert col_defs.loc[intcol_ind, 'dtype'] == 'BINARY'
    assert col_defs.loc[strcol_ind, 'dtype'] == 'object'
    assert col_defs.loc[floatcol_ind, 'dtype'] == 'STRING'
