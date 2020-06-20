import time
from init_mysql_connector_cursor import init_mysql_connector_and_cursor
import uuid
from mysql_dao import TaskDBMySqlDao
import argparse
import sys

# This method generates task entries with (url, 'New', timestamp) format.
# Make sure database and table exists before calling this method.
def generate_test_entries(num_entries, database, table, use_local_database):

    dao = TaskDBMySqlDao(database=database, table=table, use_local_database=use_local_database)
    for i in range(num_entries):
        dao.newTask(url=str(uuid.uuid4()))

if __name__ == "__main__":
    generate_test_entries(10, database="SpiderTaskQueue", table="SpiderTaskQueue", use_local_database = True)



def getOptions(args):
    parser = argparse.ArgumentParser(description="Options to run a spider worker.")

    parser.add_argument("-e", "--entries", type=int, help="Number of entries to generate. Default is 10.", default=10)
    parser.add_argument("-t", "--table", help="Table name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("-d", "--database", help="Database name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("--test", type=bool, help="Enbales test mode. Test mode generates entries in local MySQL database. Default is False.", default=False)
    options = parser.parse_args(args)
    return options

if __name__ == "__main__":
    options = getOptions(sys.argv[1:])
    generate_test_entries(database=options.database, table=options.table, use_local_database=options.test, num_entries=options.entries)