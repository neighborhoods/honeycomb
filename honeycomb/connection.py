from pyhive import hive, presto


def get_db_connection(engine='hive', addr='localhost', cursor=True):
    """
    Initializes and returns a connection to the specified database engine.

    Args:
        engine (str): String specifying which engine to return a connection for

    Returns:
        conn (str): Something of a misnomer - 'conn' is a cursor
        of a connection rather than a connection object itself.
    """
    if engine == 'hive':
        conn = hive.connect(addr)
        if cursor:
            conn = conn.cursor()
    elif engine == 'presto':
        conn = presto.connect(addr)
        if cursor:
            conn = conn.cursor()
    else:
        raise ValueError('Specified engine is not supported: ' + engine)
    return conn
