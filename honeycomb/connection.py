from pyhive import hive, presto


def get_db_connection(engine='hive', addr='localhost', cursor=True,
                      configuration=None):
    """
    Initializes and returns a connection to the specified database engine.

    Args:
        engine (str): String specifying which engine to return a connection for

    Returns:
        conn (str): Something of a misnomer - 'conn' is a cursor
        of a connection rather than a connection object itself.
    """
    if engine == 'hive':
        port = 10000
        engine_module = hive
    elif engine == 'presto':
        if configuration is not None:
            raise ValueError(
                'Non-default configurations with Presto are not supported.')
        port = 8889
        engine_module = presto
    else:
        raise ValueError('Specified engine is not supported: ' + engine)

    conn = engine_module.connect(addr, port=port, username='hadoop',
                                 configuration=configuration)
    if cursor:
        conn = conn.cursor()
    return conn
