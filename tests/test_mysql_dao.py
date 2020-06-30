import unittest
import pymysql
import mysql_dao
from mysql_dao import TaskDBMySqlDao
import json
from task_status import TaskStatus
import uuid
import time
from init_mysql_connector_cursor import init_mysql_connector_and_cursor
from test_utils import what_env

class MysqlDaoTestNoMock(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.skip = (what_env() == 'Mongo')
        if self.skip:
            return 

        self.database = "MysqlDaoTestNoMockDatabase"
        self.table = "MysqlDaoTestNoMockTable"

        conn, cursor = init_mysql_connector_and_cursor(use_local_database=True)
        # Create database and table.
        cursor.execute("CREATE DATABASE {};".format(self.database))
        cursor.execute("USE {};".format(self.database))
        cursor.execute("CREATE TABLE {} (url CHAR(40), status CHAR(20), createdOn INT(64));".format(self.table))
        # Write to table.
        cursor.execute("INSERT INTO {} (url, status, createdOn) VALUES ('url1', 'New', 1);".format(self.table))
        conn.commit()

        self.dao = TaskDBMySqlDao(self.database, self.table, use_local_database = True)
        
        cursor.close()
        conn.close()

    @classmethod
    def tearDownClass(self):
        if self.skip:
            return

        conn, cursor = init_mysql_connector_and_cursor(use_local_database=True)
        cursor.execute("DROP DATABASE {};".format(self.database))
        cursor.close()
        conn.close()

    @unittest.skipIf(what_env() == 'Mongo', "MySQL not installed on this node.")
    def test_findAndReturnAnUnprocessedTask_removeTask(self):
        conn, cursor = init_mysql_connector_and_cursor(use_local_database=True)
        cursor.execute("USE {};".format(self.database))

        url = self.dao.findAndReturnAnUnprocessedTask()
        assert url == 'url1'

        self.dao.removeTask(url)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM {} WHERE status='{}';".format(self.table, TaskStatus.new.name))
        assert not cursor.fetchone()
        cursor.close()
        conn.close()

    @unittest.skipIf(what_env() == 'Mongo', "MySQL not installed on this node.")
    def test_newTask(self):
        conn, cursor = init_mysql_connector_and_cursor(use_local_database=True)
        cursor.execute("USE {};".format(self.database))
        url = str(uuid.uuid4())

        self.dao.newTask(url)
        cursor.execute("SELECT * FROM {} WHERE url='{}';".format(self.table, url))
        assert cursor.fetchone()[0] == url
        
        cursor.close()
        conn.close()
