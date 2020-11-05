# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] - 2020-11-06

### Added
- Partiion nuking functionality - use at your own risk

### Changed
- Moved `__nuke_table` to `__danger.py`
- Simplified import in `describe_table.py`

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
