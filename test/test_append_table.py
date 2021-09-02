import pytest

import rivet as rv

from honeycomb import append_df_to_table


def test_append_df_to_table(mocker, setup_bucket_w_contents,
                            test_schema, test_bucket, test_df_key, test_df):
    """
    Tests that appending a DataFrame to an existing table works as planned,
    as shown by the DataFrame being present at the expected location in S3
    """
    mocker.patch('honeycomb.check.table_existence', return_value=True)
    mocker.patch('honeycomb.meta.get_table_column_order',
                 return_value=test_df.columns.to_list())

    storage_type = 'csv'
    filename = test_df_key.split('.')[0]
    appended_filename = filename + '_2.' + storage_type

    mocker.patch('honeycomb.meta.get_table_metadata', return_value={
        'bucket': test_bucket,
        'path': test_schema,
        'storage_type': storage_type
    })
    append_df_to_table(test_df, 'test_table',
                       schema=test_schema, filename=appended_filename)

    path = test_schema + '/' + appended_filename
    df = rv.read(path, test_bucket, header=None)

    assert (df.values == test_df.values).all()


def test_append_df_to_table_already_exists(mocker, test_df):
    """
    Tests that table appending will fail if the specified
    table does not exist
    """
    mocker.patch('honeycomb.check.table_existence', return_value=False)

    with pytest.raises(ValueError, match='Table .* does not exist'):
        append_df_to_table(test_df, 'test_table')
