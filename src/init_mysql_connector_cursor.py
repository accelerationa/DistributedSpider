import pymysql
import json

def init_mysql_connector_and_cursor(use_local_database = False):
    if use_local_database:
        conn = pymysql.connect(
            host = '127.0.0.1',
            user = 'local-test',
            password = '111222333QQQWWWEEE',
        )
        return conn, conn.cursor()

    with open('accounts.json') as f:
        accounts = json.load(f)

    assert accounts.get('mysql')
    metadata = accounts.get('mysql')
    assert metadata.get('ip')
    assert metadata.get('password')
    assert metadata.get('user')

    conn = pymysql.connect(
        host = metadata.get('ip'),
        user = metadata.get('user'),
        password = metadata.get('password'),
    )
    return conn, conn.cursor()