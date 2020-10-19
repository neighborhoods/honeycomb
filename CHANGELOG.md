# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
test
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
