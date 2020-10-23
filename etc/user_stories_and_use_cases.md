# Pre v0.1 #
1. Data Science
    * As a data scientist, I want to be able to query data stored in the data lake
    with manually-written SQL (or equivalent) queries, to ensure flexibility.
        * Easy query-builder not needed
    * As a data scientist, I want to be able to create and view plaintext
    descriptions of tables and columns, to provide confidence in data
    validity and availability - and that the data being worked with is really
    the data that is wanted.
    * As a data engineer, I want to be able to use a single tool for both
    local/adhoc/experimental querying and production-level tasks such as
    ETL pipelines.
    * As a data engineer, I want to be able to submit queries to multiple
    data sources/engines using a single tool. I also want to be able to
    extend this functionality to new sources/engines with minimal modification
    of existing code.
    * No currently known use case for cursors, but may be worthwhile in a
    post-v1.0 release
2. Analytics
    * As an analyst, I want to be able to query data stored in the data lake
    using a tool with strong SQL support.
        * Easy query-builder not needed
