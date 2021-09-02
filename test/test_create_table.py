import pytest

import rivet as rv

from honeycomb.create_table.create_table_from_df import create_table_from_df


def test_create_table_from_df_csv(mocker, setup_bucket_wo_contents,
                                  test_bucket, test_df):
    """
    Tests that a table has successfully been created in the lake by checking
    for presence of the DataFrame at the expected location in S3
    """
    schema = 'experimental'
    mocker.patch.dict(
        'honeycomb.create_table.common.schema_to_zone_bucket_map',
        {schema: test_bucket}, clear=True)

    mocker.patch('honeycomb.hive.run_lake_query', return_value=False)
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
