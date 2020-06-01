dtype_map = {
    'object': 'STRING',
    'int64': 'INT',
    'float64': 'DOUBLE',
    'bool': 'BOOLEAN',
    'datetime64': 'DATETIME',
    'timedelta': 'INTERVAL'
    # 'category': None
}

storage_type_specs = {
    'csv': {
        'settings': {
            'index': False,
            'header': False
        },
        'ddl': """
               ROW FORMAT DELIMITED
               FIELDS TERMINATED BY ','
               LINES TERMINATED BY '\\n'
               """
    },
    'pq': {
        'settings': {},
        'ddl': 'STORED AS PARQUET'
    }
}
