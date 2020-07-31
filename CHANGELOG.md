# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Table appending logic
- Functionality to create a table with both table and column comments
- Can create tables in Parquet format
- Can generate filenames for new files being added to S3
- Unit tests added for `create_table.py`, `append_table.py`, and `dtype_mapping.py`
- General usage documentation
- `create_table_from_df` now has overwrite protection. If a table already exists
with the name specified, the function can either fail or be set to drop the
old table and remove its underlying files.
- `append_table` now has overwrite protection. If a conflicting key is already
present in S3, the function will fail.
- Can create tables in Avro format
- Support for `STRUCT` data type

### Changed
- Full table and column comments are required for creating a table outside of
the experimental zone.
- `append_table` now reorders columns when needed and can handle missing/extra columns.


## [0.1.0] - 2020-05-11

### Added
- First version of package.
- Can create tables in EMR from a `pandas` DataFrame
    * This includes handling of casting from `pandas` types to `hive` types
- Can query those tables using either Hive or Presto.
- Can get table descriptions, including table/column comments
