from pyhive import hive, presto


def get_db_connection(engine='hive', addr='localhost', cursor=True,
                      configuration=None):
    """
    Initializes and returns a connection to the specified database engine.

    Args:
        engine (str): String specifying which engine to return a connection for

    Returns:
        conn (pyhive.hive.Client, pyhive.presto.Client,
              pyhive.hive.Cursor, or pyhive.presto.Cursor):
            For either Hive or Presto, a connection (passed to pandas if
            a query that will pull results is being run) or a cursor (used
            to issue commands that will not pull results into the session)
    """
    if engine == 'hive':
        port = 10000
        engine_module = hive
    elif engine == 'presto':
        if configuration is not None:
            # Presto doesn't use persistent connections, so persistent
            # configs cannot be achieved the same way
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
