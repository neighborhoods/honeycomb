from honeycomb import run_query as run


def add_partitions(table_name, schema, partitions, path):
    partition_strings = ['{}={}'.format(partition_name, partition_value) for
                         partition_name, partition_value in partitions.items()]

    path += (
        '/'.join([partition_val for partition_val in partitions.values()]) +
        '/'
    )

    run.lake_query(
        'ALTER TABLE {}.{} ADD PARTITION ({}) LOCATION \'{}\''.format(
            table_name,
            schema,
            ', '.join(partition_strings),
            path
        ))

    return path
