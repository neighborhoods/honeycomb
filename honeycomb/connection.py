from pyhive import hive, presto


# TODO BigQuery connection?
# TODO context decorator
def get_db_connection(engine='hive', addr='localhost'):
    """
    Initializes and returns a connection to the specified database engine.

    Args:
        engine (str): String specifying which engine to return a connection for

    Returns:
        conn (str): Something of a misnomer - 'conn' is a cursor
        of a connection rather than a connection object itself.
    """
    if engine == 'hive':
        conn = hive.connect(addr).cursor()
    elif engine == 'presto':
        conn = presto.connect(addr).cursor()
    else:
        raise ValueError('Specified engine is not supported: ' + engine)
    return conn
