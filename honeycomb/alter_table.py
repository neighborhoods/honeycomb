import logging

from honeycomb import check, hive, inform


def add_partition(table_name, schema, partition_values):
    partition_strings = [
        '{}=\'{}\''.format(partition_key, partition_value)
        for partition_key, partition_value in partition_values.items()]
    partition_path = (
        '/'.join(partition_values.values()) + '/')

    if not check.partition_existence(table_name, schema, partition_values):
        add_partition_query = (
            'ALTER TABLE {}.{} ADD IF NOT EXISTS '
            'PARTITION ({}) LOCATION \'{}\''.format(
                schema,
                table_name,
                ', '.join(partition_strings),
                partition_path)
        )
        inform(add_partition_query)

        hive.run_lake_query(add_partition_query, engine='hive')
    else:
        logging.warn(
            'Partition {} already exists in table.'.format(partition_strings))

    return partition_path
