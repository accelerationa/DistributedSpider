import unittest
import pymysql
import mysql_dao
from mysql_dao import TaskDBMySqlDao
import json
from task_status import TaskStatus

class MysqlDaoTestNoMock(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.database = "MysqlDaoTestNoMockDatabase"
        self.table = "MysqlDaoTestNoMockTable"

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
        )

        self.cursor = self.db.cursor()

        # Create database and table.
        self.cursor.execute("CREATE DATABASE {};".format(self.database))
        self.cursor.execute("USE {};".format(self.database))
        self.cursor.execute("CREATE TABLE {} (url CHAR(40), status CHAR(20), createdOn INT(64));".format(self.table))

        # Write to table.
        self.cursor.execute("INSERT INTO {} (url, status, createdOn) VALUES ('url1', 'New', 1);".format(self.table))
        self.db.commit()

        self.dao = TaskDBMySqlDao(self.database, self.table)

    @classmethod
    def tearDownClass(self):
        self.cursor.execute("DROP DATABASE {};".format(self.database))


    def test_findAndReturnAnUnprocessedTask_removeTask(self):
        url = self.dao.findAndReturnAnUnprocessedTask()
        assert url == 'url1'

        self.dao.removeTask(url)
        # Get one new entry.    
        self.cursor.execute("SELECT * FROM {} WHERE status='{}';".format(self.table, TaskStatus.new.name))

        assert not self.cursor.fetchone()


