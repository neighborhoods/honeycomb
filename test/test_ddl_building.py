import pandas as pd

from honeycomb.ddl_building import add_comments_to_col_defs


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
