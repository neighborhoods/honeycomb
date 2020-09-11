import pandas as pd

import honeycomb as hc

dtypes = {
    'array_of_primitives':
        pd.DataFrame({
            'c': [
                [1, 2],
                [3, 4]
            ]
        }),
    'array_of_arrays':
        pd.DataFrame({
            'c': [
                [[1, 2], [3, 4]],
                [[5, 6], [7, 8]]
            ]
        }),
    'array_of_structs':
        pd.DataFrame({
            'c': [
                [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}],
                [{'a': 5, 'b': 6}, {'a': 7, 'b': 8}]
            ]
        }),
    'struct_of_primitives':
        pd.DataFrame({
            'c': [
                {'q': 1, 'z': 2},
                {'q': 3, 'z': 4},
            ]
        }),
    'struct_of_arrays':
        pd.DataFrame({
            'c': [
                {'a': [1, 2]},
                {'a': [1, 2]},
            ]
        }),
    'struct_of_structs':
        pd.DataFrame({
            'c': [
                {'q': {'a': 1, 'b': 2}, 'z': {'a': 3, 'b': 4}},
                {'q': {'a': 5, 'b': 6}, 'z': {'a': 7, 'b': 8}},
            ]
        })
}


def validate_dtypes(dtypes_to_test=None, storage_format='json'):
    if dtypes_to_test is None:
        dtypes_to_test == dtypes.keys()
    if isinstance(dtypes_to_test, str):
        dtypes_to_test = [dtypes_to_test]
    hc.run_lake_query('CREATE SCHEMA IF NOT EXISTS experimental',
                      engine='hive')

    for dtype in dtypes_to_test:
        table_name = dtype
        df = dtypes[dtype]

        hc.create_table_from_df(df, table_name,
                                filename='test.{}'.format(storage_format),
                                overwrite=True)
        print(hc.run_lake_query(
            'SELECT * FROM experimental.{}'.format(table_name), engine='hive'))
