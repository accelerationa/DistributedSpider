import pymysql
import logging
from task_status import TaskStatus

class TaskDBMySqlDao:
    def __init__(table):
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="lyc135790", # NOTE: Password doesn't work.
            database="SpiderTaskQueue"
        )
        self.table = table

    def findAndReturnAnUnprocessedTask():
        cursor = self.db.cursor()

        try:
            # Lock table.
            cursor.execute("LOCK TABLES {0} WRITE".format(self.table))
        except Error as e: 
            raise Exception('Unable to lock table {0}, error message {1}'.format(self.table, e))

        try: 
            # Get one unprocessed entry.        
            cursor.execute("SELECT * FROM {0} WHERE status='{1}' LIMIT 1;".format(self.table, TaskStatus.new))
            task_url = cursor.fetchone()[0][0]
            # Update entry status.        
            cursor.execute("UPDATE {0} SET status='{1}' WHERE url='{2}';".format(self.table, TaskStatus.downloading, task_url))
        except Error as e:
            raise Exception('Unable to findone and update {0}, error message {1}'.format(self.table, e))
        finally
            # Unlock table.
            cursor.execute("UNLOCK TABLES;")

    def removeTask(task_url):
        cursor = self.db.cursor()
        # Remove entry.
        cursor.execute("DELETE FROM {0} WHERE url='{1}';".format(self.table, task_url))