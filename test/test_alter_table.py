from honeycomb.alter_table import add_partition


def test_add_partition_builds_path(mocker):
    mocker.patch('honeycomb.hive.run_lake_query', return_value=False)
    mocker.patch('honeycomb.check.partition_existence', return_value=False)

    partition_values = {'year_partition': '2020', 'month_partition': '01'}

    expected_path = '/'.join(partition_values.values()) + '/'

    actual_path = add_partition(table_name='table', schema='experimental',
                                partition_values=partition_values)

    assert actual_path == expected_path
