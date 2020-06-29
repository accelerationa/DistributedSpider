import pymysql
import logging
from task_status import TaskStatus
import json
import time
from init_mysql_connector_cursor import init_mysql_connector_and_cursor

def log_and_execute(cursor, query):
        print(query)
        cursor.execute(query)

class TaskDBMySqlDao(TaskDaoInterface):
    def __init__(self, database, table, use_local_database = False):
        self.table = table
        self.database = database
        self.use_local_database = use_local_database

    # Returns task URL. Returns None if there's no unprocessed task.
    def findAndReturnAnUnprocessedTask(self):
        conn, cursor = init_mysql_connector_and_cursor(use_local_database=self.use_local_database)
        try:
            log_and_execute(cursor, "USE {};".format(self.database))

            # Get one unprocessed entry and update to downloading. 
            log_and_execute(cursor, "UPDATE {} SET status='{}', url = (SELECT @update_url := url) WHERE status='{}' LIMIT 1;".format(self.table, TaskStatus.downloading.value, TaskStatus.new.value))
            log_and_execute(cursor, "SELECT @update_url;")
            task = cursor.fetchone()
            conn.commit()

            print("Entry fetched from table.")
            if not task or not task[0]:
                print("Empty Entry...")
                # After returning from try, it automatucally goes to finally block.
                return None

            task_url = task[0]
            print("Task url is: {}.".format(task_url))

        except Exception as e:
            raise Exception('Unable to find one and update {}. Error message: {}'.format(self.table, e))
        finally:
            # Clean up
            cursor.close()
            conn.close()
        
        return task_url

    def removeTask(self, task_url):
        conn, cursor = init_mysql_connector_and_cursor(use_local_database=self.use_local_database)

        try:
            log_and_execute(cursor, "USE {};".format(self.database))

            # Remove entry.
            log_and_execute(cursor, "DELETE FROM {} WHERE url='{}';".format(self.table, task_url))
            conn.commit()
            print("Entry with url {} removed.".format(task_url))
        except Exception as e:
            raise Exception('Unable to remove {}. Error message: {}'.format(url, e))
        finally:
            # Clean up
            cursor.close()
            conn.close()

    def newTask(self, url):
        conn, cursor = init_mysql_connector_and_cursor(use_local_database=self.use_local_database)

        try:
            log_and_execute(cursor, "USE {};".format(self.database))
            log_and_execute(cursor, "INSERT INTO {} (url, status, createdOn) VALUES ('{}', 'New', {});".format(self.table, url, time.time()))
            conn.commit()

            print("Entry with url {} inserted.".format(url))

        except Exception as e:
            raise Exception('Unable to insert database {}. Error message: {}'.format(self.database, e))
        finally:
            # Clean up
            cursor.close()
            conn.close()
        