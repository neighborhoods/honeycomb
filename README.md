# honeycomb #
`honeycomb` serves as an abstraction around connections to a Hadoop/Hive-based
data lake and other databases, built with extensibility to other services in
mind. It handles connections, type conversions, a degree of query generation,
and more for users, in order to greatly reduce the degree of technical
expertise and Python knowledge required to interact with the NHDS data lake
and other data repositories. In doing this, we aim to increase data
availability throughout the organization, and facilitate any individual
member feeling empowered to interact with data.

## Usage
Much of the functionality of `honeycomb` is completely under-the-hood, and as
a result the average use case of it is very straightforward.

### General
1. Supported engines are currently:
    * Runs against data lake:
       1. Presto - Runs against data lake, and is recommended for most cases.
       Presto runs queries more quickly than other engines, but has query size
       limitations. Best for ad-hoc querying and experiments.
       Must provide a schema in queries.
       2. Hive - Runs against data lake. Queries are slightly slower than Presto,
       but has effectively has no limit on query size and has advanced
       recovery/reliability functionality. Best used for production-level tasks,
       large queries, and specific actions such as table creation.
       Must provide a schema in queries.
    * Does not run against data lake
        1. Google BigQuery - Runs against data stored in Google BigQuery. Currently,
        the only data stored there is clickstream data and cost information on
        our GA/GBQ accounts. Must provide a project ID, dataset, and table name
        in queries.

### Running Queries
The `run_query` function allows for running queries through multiple different
query engines.

```
import honeycomb as hc

df0 = hc.run_query('SELECT COUNT(*) FROM experimental.test_table',
                   engine='presto')

df1 = hc.run_query('SELECT COUNT(*) FROM experimental.test_table',
                   engine='hive')

df2 = hc.run_query('SELECT COUNT(*) FROM `places-clickstream`.12345678.20200501',
                   engine='gbq', project_id='places-clickstream')
```

### Table Creation
`honeycomb` only supports table creation using `hive` as the engine. To create
a table in the data lake, all that is required is a dataframe, a table name -
it defaults to writing to the 'experimental' zone.

`create_table_from_df` will infer the file type to create the table with based
on the extension of the provided `filename`. If `filename` is not provided or
does not contain an extension, it will default to Parquet.

Additional parameters:
1. `schema`: The schema to write to. Most users will only have access to write
to the 'experimental' zone.
2. `dtypes`: A dictionary from column names to data types. `honeycomb` automatically
converts Python/pandas data types into Hive-compliant data types, but this parameter
allows for manual override of this. For example, `pd.DateTime`s are normally converted to
`hive` datetimes, but if specified in this parameter, it could be saved as a string instead.
3. `path`: Where the files that this table references should be stored in S3. It defaults
to the table's name, and under most circumstances should be kept this way.
4. `filename`: The name to store the file containing the DataFrame. If writing
to the experimental zone, this is optional, and a name based on a timestamp will
be generated. However, when writing to any other lake zone, a specified name
is required, in order to ensure that the table's underlying filesystem will
be navegable in the future.
5. `table_comment`: A plaintext description of what data the table contains
and what its purpose is. When writing to the experimental zone, this is optional,
but with any other zone it is required.
6. `column_comments`: A dictionary from column names to a plaintext description
of what the column contains. This is optional when writing to the experimental zone,
but with any other zone it is required.
7. `overwrite`: States whether it is okay to overwrite a file if this function's
write operation encounters an identically named file in S3. - WILL BE MODIFIED SOON
```
import pandas as pd
import honeycomb as hc

df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
hc.create_table_from_df(df, table_name='test_table')
```

### Table Appending
`honeycomb` only supports using `hive` as the engine for table appending.
To append a DataFrame to a table, all that is needed is the table name.
Schema can optionally be provided and defaults to 'experimental'.

```
import pandas as pd
import honeycomb as hc

df = pd.DataFrame({'col1': [7, 8, 9], 'col2': [10, 11, 12]})
hc.append_table(df, table_name='test_table')
```

As with `create_table_from_df`, a filename can be provided as well. If one is not,
a name is generated based on a timestamp. However, if writing anywhere other than
the experimental zone, a specified filename is required.

### Table Describing
`honeycomb` can be used to obtain information on tables in the lake, such
as column names and dtypes, and if `include_metadata` is set to true,
a table description and column comments.

```
import honeycomb as hc

hc.describe_table('test_table', schema='curated', include_metadata=True)
```
