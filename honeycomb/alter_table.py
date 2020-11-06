from honeycomb import check, hive, meta


def add_partition(table_name, schema, partition_values):
    partition_values = meta.confirm_ordered_partition_dicts(partition_values)

    partition_strings = [
        '{}=\'{}\''.format(partition_key, partition_value)
        for partition_key, partition_value in partition_values.items()]
    partition_path = (
        '/'.join(partition_values.values()) + '/')

    if not check.partition_existence(table_name, schema, partition_values):
        partition_key_order = meta.get_partition_key_order(table_name, schema)
        if partition_key_order != partition_values.keys():
            raise ValueError(
                'Partition keys provided do not match the keys of the table. '
                'Order must be identical.\n'
                'Keys provided: {}\n'
                'Keys in table: {}'.format(
                    ', '.join(partition_values.keys()),
                    ', '.join(partition_key_order)))

        add_partition_query = (
            'ALTER TABLE {}.{} ADD IF NOT EXISTS '
            'PARTITION ({}) LOCATION \'{}\''.format(
                schema,
                table_name,
                ', '.join(partition_strings),
                partition_path)
        )
        print(add_partition_query)

        hive.run_lake_query(add_partition_query, engine='hive')
    else:
        print(
            'Partition {} already exists in table.'.format(partition_strings))

    return partition_path
