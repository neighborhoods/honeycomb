from honeycomb import run_query as run


def add_partition(table_name, schema, partitions):
    partition_strings = ['{}={}'.format(partition_name, partition_value) for
                         partition_name, partition_value in partitions.items()]

    run.lake_query('ALTER TABLE {}.{} ADD PARTITION ({})'.format(
        table_name,
        schema,
        ', '.join(partition_strings)
    ))
