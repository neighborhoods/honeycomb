import pandas as pd
import pytest
import river as rv

from honeycomb.create_table import (create_table_from_df,
                                    add_comments_to_col_defs)


def test_create_table_from_df_csv(mocker, setup_bucket_wo_contents,
                                  test_bucket, test_df):
    """
    Tests that a table has successfully been created in the lake by checking
    for presence of the DataFrame at the expected location in S3
    """
    schema = 'experimental'
    mocker.patch.dict('honeycomb.create_table.schema_to_zone_bucket_map',
                      {schema: test_bucket}, clear=True)
    mocker.patch('honeycomb.run_query.lake_query', return_value=False)
    mocker.patch('honeycomb.check.table_existence', return_value=False)

    table_name = 'test_table'
    filename = 'test_file.csv'
    create_table_from_df(test_df, table_name=table_name,
                         schema=schema, filename=filename,
                         table_comment='table for testing')

    path = table_name + '/' + filename
    df = rv.read(path, test_bucket, header=None)

    assert (df.values == test_df.values).all()


def test_create_table_from_df_already_exists(mocker, test_df):
    """
    Tests that creating a table will fail if a table already exists
    with the same name
    """
    mocker.patch('honeycomb.check.table_existence', return_value=True)

    with pytest.raises(ValueError, match='already exists'):
        create_table_from_df(test_df, 'test_table')


def test_add_comments_to_col_defs(test_df):
    """Tests that comments are added to column definitions as expected"""
    col_defs = pd.Series(
        data=[
            'object', 'int64', 'float64',
            'bool', 'datetime64', 'timedelta'],
        index=['objcol', 'intcol', 'floatcol',
               'boolcol', 'dtcol', 'timedeltacol']
    )

    comments = {
        'objcol': 'This column is type "object"',
        'intcol': 'This column is type "int64"',
        'floatcol': 'This column is type "float64"',
        'boolcol': 'This column is type "bool"',
        'dtcol': 'This column is type "datetime64"',
        'timedeltacol': 'This column is type "timedelta"'
    }

    expected_df = col_defs.to_frame(name='dtype')
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
