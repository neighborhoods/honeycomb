import pytest

import river as rv

from honeycomb.append_table import append_table


def test_append_table(mocker, setup_bucket_w_contents,
                      test_schema, test_bucket, test_df_key, test_df):
    mocker.patch('honeycomb.check.table_existence', return_value=True)

    storage_type = 'csv'
    filename = test_df_key.split('.')[0]
    appended_filename = filename + '_2.' + storage_type

    mocker.patch('honeycomb.meta.get_table_metadata', return_value={
        'bucket': test_bucket,
        'path': test_schema,
        'storage_type': storage_type
    })
    append_table(test_df, 'test_table',
                 schema=test_schema, filename=appended_filename)

    path = test_schema + '/' + appended_filename
    df = rv.read(path, test_bucket, header=None)
    print(df)
    print(test_df)

    assert (df.values == test_df.values).all()


def test_append_table_already_exists(mocker, test_df):
    mocker.patch('honeycomb.check.table_existence', return_value=False)

    with pytest.raises(ValueError, match='Table .* does not exist'):
        append_table(test_df, 'test_table')
