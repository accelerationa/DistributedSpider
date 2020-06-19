import pymysql
import logging
from task_status import TaskStatus
import json

# Call this function to initialize connector in every public method
def init_connector():
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
    return conn

class TaskDBMySqlDao:
    def __init__(self, database, table):
        self.table = table
        self.database = database

    # Call this function to initialize connector in every public method

    def findAndReturnAnUnprocessedTask(self):
        conn = init_connector()
        cursor = conn.cursor()

        try:
            cursor.execute("USE {};".format(self.database))
            print("{} selected.".format(self.database))

            # Lock table.
            print("Locking table...\n")
            cursor.execute("LOCK TABLES {} WRITE".format(self.table))
            print("Table locked.\n")

        except Error as e: 
            raise Exception('Unable to lock table {}, error message {}'.format(self.table, e))

        try: 
            # Get one unprocessed entry.    
            cursor.execute("SELECT * FROM {} WHERE status='{}' LIMIT 1;".format(self.table, TaskStatus.new.name))
            print("Entry fetched from table.\n")
            task_url = cursor.fetchone()[0]
            print("Task url is: {}.\n".format(task_url))

            # Update entry status.        
            cursor.execute("UPDATE {} SET status='{}' WHERE url='{}';".format(self.table, TaskStatus.downloading.name, task_url))
            conn.commit()
            print("Updated task status to Downloading.\n")
        except Exception as e:
            raise Exception('Unable to find one and update {}. Error message: {}'.format(self.table, e))
        finally:
            # Unlock table.
            print("Unlocking table...\n")
            cursor.execute("UNLOCK TABLES;\n")
            print("Table unlocked.\n")

            # Clean up
            cursor.close()
            conn.close()
        
        return task_url

    def removeTask(self, task_url):
        conn = init_connector()
        cursor = conn.cursor()

        try:
            cursor.execute("USE {};".format(self.database))
            print("{} selected.".format(self.database))

            # Remove entry.
            cursor.execute("DELETE FROM {} WHERE url='{}';".format(self.table, task_url))
            print("DELETE FROM {} WHERE url='{}';".format(self.table, task_url))

            conn.commit()
            print("Entry with url {} removed.".format(task_url))
        except Exception as e:
            raise Exception('Unable to remove database {}. Error message: {}'.format(self.database, e))
        finally:
            # Clean up
            cursor.close()
            conn.close()
        