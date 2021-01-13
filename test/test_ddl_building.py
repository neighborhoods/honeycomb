import pandas as pd

from honeycomb.ddl_building import (add_comments_to_col_defs,
                                    format_col_defs)


def test_add_comments_to_col_defs(test_df):
    """Tests that comments are added to column definitions as expected"""
    col_defs = pd.DataFrame({
        'col_name': ['objcol', 'intcol', 'floatcol',
                     'boolcol', 'dtcol', 'timedeltacol'],
        'dtype': ['object', 'int64', 'float64',
                  'bool', 'datetime64', 'timedelta']
    })

    comments = {
        'objcol': 'This column is type "object"',
        'intcol': 'This column is type "int64"',
        'floatcol': 'This column is type "float64"',
        'boolcol': 'This column is type "bool"',
        'dtcol': 'This column is type "datetime64"',
        'timedeltacol': 'This column is type "timedelta"'
    }

    expected_df = col_defs.copy()
    expected_df['comment'] = [
        ' COMMENT \'This column is type "object"\'',
        ' COMMENT \'This column is type "int64"\'',
        ' COMMENT \'This column is type "float64"\'',
        ' COMMENT \'This column is type "bool"\'',
        ' COMMENT \'This column is type "datetime64"\'',
        ' COMMENT \'This column is type "timedelta"\''
    ]

    col_defs_w_comments = add_comments_to_col_defs(col_defs, comments)

    assert col_defs_w_comments.equals(expected_df)


def test_struct_col_nested_comments():
    base_ddl = (
        '{}STRUCT <nested_1: STRING{}, nested_2: DOUBLE{}>'
    )

    complex_col = 'struct_col'
    col_defs = pd.DataFrame({
        'col_name': ['index_col', complex_col],
        'dtype': ['BIGINT', base_ddl.format(*[''] * base_ddl.count('{}'))]
    })

    col_comments = {
        'index_col': 'A row index',
        complex_col: 'A struct column',
        complex_col + '.nested_1': 'A field nested in a struct',
        complex_col + '.nested_2': 'Another field nested in a struct'
    }

    expected_complex_field_ddl = base_ddl.format(
        complex_col + ' ',
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_1']),
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_2']),
    )

    column_ddl = format_col_defs(col_defs, col_comments)

    assert expected_complex_field_ddl in column_ddl


def test_array_of_struct_col_nested_comments():
    base_ddl = (
        '{}ARRAY <STRUCT <nested_1: STRING{}, nested_2: DOUBLE{}>>{}'
    )
    complex_col = 'array_of_struct_col'
    col_defs = pd.DataFrame({
        'col_name': ['index_col', complex_col],
        'dtype': ['BIGINT', base_ddl.format(*[''] * base_ddl.count('{}'))]
    })

    col_comments = {
        'index_col': 'A row index',
        complex_col: 'An array of struct column',
        complex_col + '.nested_1': 'A field nested in a struct',
        complex_col + '.nested_2': 'Another field nested in a struct'
    }

    expected_complex_field_ddl = base_ddl.format(
        complex_col + ' ',
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_1']),
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_2']),
        ' COMMENT \'{}\''.format(col_comments[complex_col])
    )

    column_ddl = format_col_defs(col_defs, col_comments)

    assert expected_complex_field_ddl in column_ddl


def test_nested_array_of_struct_col_nested_comments():
    base_ddl = (
        '{}ARRAY <STRUCT <nested_1: ARRAY <STRUCT <'
        'deeply_nested: STRING{}>>{}\', nested_2: DOUBLE{}>>{}'
    )
    complex_col = 'double_array_of_struct_col'
    col_defs = pd.DataFrame({
        'col_name': ['index_col', complex_col],
        'dtype': ['BIGINT', base_ddl.format(*[''] * base_ddl.count('{}'))]
    })

    col_comments = {
        'index_col':
            'A row index',
        complex_col:
            'An array of struct column',
        complex_col + '.nested_1':
            'An array of struct field nested in an array of structs',
        complex_col + '.nested_2':
            'A simple field nested in a struct',
        complex_col + '.nested_1.deeply_nested':
            'A deeply nested field'
    }

    expected_complex_field_ddl = base_ddl.format(
        complex_col + ' ',
        ' COMMENT \'{}\''.format(col_comments[
                                 complex_col + '.nested_1.deeply_nested']),
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_1']),
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_2']),
        ' COMMENT \'{}\''.format(col_comments[complex_col])
    )

    column_ddl = format_col_defs(col_defs, col_comments)

    assert expected_complex_field_ddl in column_ddl


def test_struct_of_struct_col_nested_comments():
    base_ddl = (
        '{}STRUCT <nested_1: STRUCT <double_nested_1: STRING{}, '
        'double_nested_2: DOUBLE{}>{}, nested_2: BIGINT{}>{}'
    )
    complex_col = 'struct_of_struct_col'
    col_defs = pd.DataFrame({
        'col_name': ['index_col', complex_col],
        'dtype': ['BIGINT', base_ddl.format(*[''] * base_ddl.count('{}'))]
    })

    col_comments = {
        'index_col':
            'A row index',
        complex_col:
            'A struct of struct column',
        complex_col + '.nested_1':
            'A struct nested in a struct',
        complex_col + '.nested_1.double_nested_1':
            'A double-nested simple field',
        complex_col + '.nested_1.double_nested_2':
            'Another double-nested simple field',
        complex_col + '.nested_2':
            'A simple field nested in a struct'
    }

    expected_complex_field_ddl = base_ddl.format(
        complex_col + ' ',
        ' COMMENT \'{}\''.format(col_comments[
                                 complex_col + '.nested_1.double_nested_1']),
        ' COMMENT \'{}\''.format(col_comments[
                                 complex_col + '.nested_1.double_nested_2']),
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_1']),
        ' COMMENT \'{}\''.format(col_comments[complex_col + '.nested_2']),
        ' COMMENT \'{}\''.format(col_comments[complex_col])
    )

    column_ddl = format_col_defs(col_defs, col_comments)

    print(expected_complex_field_ddl)
    print(column_ddl)
    assert expected_complex_field_ddl in column_ddl
