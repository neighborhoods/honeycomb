from honeycomb import check
from honeycomb.hive import run_lake_query


def add_partition(table_name, schema, partition_values):
    partition_strings = [
        '{}=\'{}\''.format(partition_key, partition_value)
        for partition_key, partition_value in partition_values.items()]
    partition_path = (
        '/'.join(partition_values.values()) + '/')

    if not check.partition_existence(table_name, schema, partition_values):
        add_partition_query = (
            'ALTER TABLE {}.{} ADD PARTITION ({}) LOCATION \'{}\''.format(
                schema,
                table_name,
                ', '.join(partition_strings),
                partition_path)
        )
        print(add_partition_query)

        run_lake_query(add_partition_query, engine='hive')
    else:
        print(
            'Partition {} already exists in table.'.format(partition_strings))

    return partition_path
