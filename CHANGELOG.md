# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.0] 2021-09-02

### Changed
- Switched from `river` to open-source `rivet`


## [1.6.4] 2021-08-05

### Added
- `insert_into_orc_table` function can now use overwrite option

### Changed
- Fixed incorrect regex matching syntax in detection of `INSERT OVERWRITE` commands

## [1.6.3] 2021-06-14

### Changed
- Added 'order' to list of reserved words that can be auto-escaped when
inserting into ORC tables
- Updated river dependency to fix errors when inserting into ORC tables
from tables w/ "invalid" storage paths

## [1.6.2] 2021-05-24

### Changed
- ORC operations now allow for source tables that make use of Hive reserved
words as column names. While using these reserved words this way is discouraged,
there are times where the column names of source tables cannot feasibly be
controlled.

## [1.6.1] 2021-05-13

### Added
- Creation of/appending to ORC tables now supports application of Hive functions
on data prior to its addition to the final table

## [1.6.0] 2021-05-12

### Added
- ORC file format conversion functionality
- Complex types are now supported for Parquet format. As a result, Parquet
is now the recommended filetype for all non-prod-level use cases, with a
further conversion to ORC being optional
- Functionality to perform `ANALYZE TABLE` commands, including on partitions
and on columns. This can be used to increase query performance on specific tables

### Changed
- Added convenience syntax to `add_partition` for providing schema and
table name in the same argument (was missing previously)
- Moved functionality for getting secrets from SSM (AWS parameter store)
from the Salesforce module to its own module

## [1.5.5] 2021-04-05

### Added
- `INSERT OVERWRITE` commands will now work for ORC tables

### Changed
- Bugfix: `INSERT OVERWRITE` commands are now case-insensitive
- Bugfix: Table paths that include more than 1 '/' character will no longer be
treated as invalid

## [1.5.4] 2021-03-18

### Added
- Support for Pandas v1.0+ nullable integer, nullable string, and nullable boolean types
- Support for passing a custom path to `add_partition`
- Support for datetimes as partition values in `add_partition`

### Changed
- Column comments will now be properly added to the Avro schema in struct fields
nested within arrays

## [1.5.3] 2021-02-12

### Added
- Session-level configuration Functionality
- Verbosity option: disable all non-logging output, for use in notebooks or pipelines.

### Changed
- Bugfix: CTAS can now overwrite a table that it is selecting from. Previously,
if CTAS was used to overwrite a table that was in the select statement,
the data from that source table would not be in the resulting table
- `pd.io.sql.DatabaseError`s will now be raised properly when raised in an un-handleable
way from within `_hive_query`
- Column comments in Avro tables are now injected into the Avro schema, so they will
properly be added to a Hive metastore.

## [1.5.2] - 2021-01-22

### Changed
- Only enforce overwrite protection on the Curated zone, as opposed to all non-Experimental zones

## [1.5.1] - 2021-01-20

### Changed
- Bugfix: CTAS functionality now works with `JOIN` queries that involve complex columns
- Bugfix: `INSERT OVERWRITE` commands are now prevented from overwriting the entirety of a bucket
- Bugfix: `_query_returns_df` now properly identifies query types even when
the query contains leading whitespace

### Notes
- Discovered a `hive` bug, in v3.1.2. Performing queries that `SELECT` complex-type columns from a
`JOIN` clause only works if the two tables are of different underlying file types -
in the context of filetypes that `honeycomb` supports complex type columns with,
that would mean one table using Avro, and the other using Parquet.
- This is because `hive` attempts to run the query as vectorized when it should not.
- Query vectorization in `hive` only works on queries that exclusively involve
primitive-type columns. If `hive` sees that a complex-type column is
involved in a query, it is supposed to disable vectorization for that query.
- When the query involves a `JOIN`, it will properly do so if the tables
were, as mentioned above, using different underlying storage formats.
- If the tables are using the same storage format though - for `honeycomb`,
both Parquet or both Avro - `hive` would, for currently unknown reasons, fail to
disable vectorization, and it would attempt to run a non-vectorizable
query as vectorized.
- Hive ticket created here: https://issues.apache.org/jira/browse/HIVE-24647#

## [1.5.0] - 2021-01-12

### Added
- CTAS functionality

### Changed
- Bucket names, SSM paths to Salesforce credentials, and default AWS regions
are now set via environment variables.

## [1.4.1] - 2021-01-06

### Changed
- `pandavro` additions were merged to master and released, so now using the publicly available version

## [1.4.0] - 2021-01-05

### Added
- Support for automatic insertion of comments for nested fields during table creation

### Changed
- Fields in `_version.py` are now available and viewable in-code

## [1.3.5] - 2020-12-09

### Added
- Username argument and column name prefix handling to permit `JOIN` queries through the hive engine

## [1.3.4] - 2020-11-25

### Changed
- Default query engine for `run_lake_query` changed to `'hive'` from `'presto'`
- Columns are no longer changed to be all-lowercase before uploading to S3. This
was originally done to match the fact that `hive` queries expect all lowercase
field names, but in practice this ended up causing issues when comparing
files uploaded to the lake with their original sources.
- Functionality to automatically escape Hive reserved keywords during table creation has been removed.
This means `honeycomb` will no longer facilitate the usage of these keywords as column names.

## [1.3.3] - 2020-11-18

### Added
- Added `pandas-gbq` to the requirements for the `bigquery` module - as it always should have been

## [1.3.2] - 2020-11-17

### Added
- Table creation and appending can now make use of a pre-created Avro schema

## [1.3.1] - 2020-11-11

### Changed
- The generation of `SELECT *` query strings in the Salesforce module is now
available as its own function.

## [1.3.0] - 2020-11-06

### Added
- Partiion nuking functionality - use at your own risk
- `run_gbq_query` can now accept credentials as an argument

### Changed
- Moved `__nuke_table` to `__danger.py`
- Simplified import in `describe_table.py`
- `bigquery` is now a package extra

## [1.2.0] - 2020-10-29

### Added
- Flash update functionality: Tables that are based on single files can be overwritten with minimal downtime.
Mostly for production situations in which a whole table needs to be replaced regularly.

### Changed
- `create_table_from_df` can now overwrite tables outside of the experimental zone if
the `HC_PROD_ENV` envvar is set.
- Relaxed `pandas` dependency requirement

## [1.1.4] - 2020-10-26

### Changed
- If column mismatches are found in `append_df_to_table`, the mismatched
columns are now printed
- Slight logic changes in table creation and metadata access
  - Getting the column order of a table no longer includes partition keys in the returned list
  - Adding comments to column definitions now expects `col_defs` as a DataFrame rather than a Series
- Output formatting

## [1.1.3] - 2020-10-23

### Changed
- Changed `__nuke_table` to use `river` instead of `awscli`

## [1.1.2] - 2020-10-22

### Changed
- Creating an Avro table from a dataframe will no longer generate the Avro
schema twice
- Attempting to use `add_partition` to add a partition that already exists
will no longer raise an exception
- Removed bug in append logic that would occasionally result in errors being
raised from non-existent column mismatches.

## [1.1.1] - 2020-10-20

### Changed
- honeycomb will now check for the env var `HC_LAKE_ADDR` to determine which
address to connect to

## [1.1.0] - 2020-10-16

### Added
- Complex type support for Avro type

### Changed
- Optimized schema assumption logic for complex types
- Bugfix in table appending logic for JSON tables

## [1.0.1] - 2020-09-21

### Added
- NHDS standard CI pipeline

### Changed
- Columns containing OrderedDicts are now also recognized as `STRUCT` columns
- Fixed Salesforce module imports/function signatures

## [1.0] - 2020-09-17

### Added
- Table appending logic
- Functionality to create a table with both table and column comments
- Can create tables in Parquet format
- Can generate filenames for new files being added to S3
- Unit tests added for `create_table.py`, `append_df_to_table.py`, and `dtype_mapping.py`
- General usage documentation
- `create_table_from_df` now has overwrite protection. If a table already exists
with the name specified, the function can either fail or be set to drop the
old table and remove its underlying files.
- `append_df_to_table` now has overwrite protection. If a conflicting key is already
present in S3, the function will fail.
- Can create tables in Avro format
- Can add partitions to tables
- `create_table_from_df` now has an `auto_upload_df` parameter. If `False`,
the function will use the dataframe to generate a `CREATE TABLE` statement
and will run it, but will not upload the dataframe itself to the lake.
- Automatic timezone conversion for table creation/appending. Outside of the
experimental zone, all datetime columns will be expected to be timezone-aware,
or have a timezone value provided for `honeycomb` to use to make them timezone-aware.
- Support for `STRUCT` data type
- Support for `ARRAY` data type
- Module for Salesforce querying
- Support for creating tables stored as JSON, allowing for nested fields

### Changed
- Renamed `append_df` to `append_df_to_table` for more explicit language
- Full table and column comments are required for creating a table outside of
the experimental zone.
- `append_df_to_table` now reorders columns when needed and can handle missing/extra columns.


## [0.1.0] - 2020-05-11

### Added
- First version of package.
- Can create tables in EMR from a `pandas` DataFrame
    * This includes handling of casting from `pandas` types to `hive` types
- Can query those tables using either Hive or Presto.
- Can get table descriptions, including table/column comments
