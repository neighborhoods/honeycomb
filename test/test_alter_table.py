from honeycomb.alter_table import add_partitions


def test_add_partitions_builds_path(mocker):
    mocker.patch('honeycomb.run_query.lake_query', return_value=False)
    partitions = {
        'year_partition': '2020',
        'month_partition': '01'
    }
    input_path = 's3://test_bucket/table_folder/'
    expected_path = (
        input_path +
        partitions['year_partition'] + '/' +
        partitions['month_partition'] + '/')

    actual_path = add_partitions(table_name='table', schema='experimental',
                                 partitions=partitions, path=input_path)

    assert actual_path == expected_path
