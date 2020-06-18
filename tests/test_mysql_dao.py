import unittest
import pymysql
import mysql_dao
import json

class MysqlDaoTestNoMock(unittest.TestCase):
    def test_findAndReturnAnUnprocessedTask(self):

        with open('accounts.json') as f:
            accounts = json.load(f)

        assert accounts.get('mysql')
        metadata = accounts.get('mysql')
        assert metadata.get('ip')
        assert metadata.get('password')
        assert metadata.get('user')

        self.db = pymysql.connect(
            host = metadata.get('ip'),
            user = metadata.get('user'),
            password = metadata.get('password'),
            database = "SpiderTaskQueue"
        )


        cursor = self.db.cursor()
        cursor.execute("CREATE TABLE abcde")




