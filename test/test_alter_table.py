from honeycomb.alter_table import add_partitions


def test_add_partitions_builds_path(mocker):
    mocker.patch('honeycomb.run_query.lake_query', return_value=False)
    partitions = [('year_partition', '2020'), ('month_partition', '01')]
    expected_path = '/'.join([partition[1] for partition in partitions]) + '/'

    actual_path = add_partitions(table_name='table', schema='experimental',
                                 partitions=partitions)

    assert actual_path == expected_path
