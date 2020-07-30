from honeycomb import run_query as run


def add_partitions(table_name, schema, partitions):
    partition_strings = ['{}=\'{}\''.format(partition_name, partition_value)
                         for partition_name, partition_value in partitions]

    partition_path = (
        '/'.join([partition[1] for partition in partitions]) + '/')

    add_partition_query = (
        'ALTER TABLE {}.{} ADD PARTITION ({}) LOCATION \'{}\''.format(
            schema, table_name, ', '.join(partition_strings), partition_path))
    print(add_partition_query)

    run.lake_query(add_partition_query, engine='hive')

    return partition_path
