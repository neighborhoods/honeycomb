from pyhive import hive, presto

from honeycomb.connection import get_db_connection


def test_get_db_connection_cursor():
    hive_conn = get_db_connection('hive', addr='testing')
    presto_conn = get_db_connection('presto', addr='testing')
    assert isinstance(hive_conn, hive.Cursor)
    assert isinstance(presto_conn, presto.Cursor)


def test_get_db_connection_actual_connection():
    hive_conn = get_db_connection('hive', addr='testing', cursor=False)
    presto_conn = get_db_connection('presto', addr='testing', cursor=False)
    assert isinstance(hive_conn, hive.Connection)
    assert isinstance(presto_conn, presto.Connection)
