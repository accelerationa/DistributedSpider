import time
from init_mysql_connector_cursor import init_mysql_connector_and_cursor
import uuid
from mysql_dao import TaskDBMySqlDao
from mongo_dao import TaskDBMongoDao
from database_enum import DatabaseStack
from database_enum import Database
import argparse
import sys
import numpy
from init_mongo_client import init_mongo_client

# This method generates read/write loads on MongoDB with user specifed r / w rate.
# Read -> find()
# Write -> ramdomly insert and delete
# Make sure database and collection exists before calling this method.
def mongodb_load_test(database, collection, db_stack, rwratio):
    client = init_mongo_client(db_stack)
    col = client.SpiderTaskQueue.SpiderTaskQueue

    while True:
        op = numpy.random.choice(['r', 'w'], p=[1.0/(rwratio+1)*rwratio,  1.0/(rwratio+1)])
        if op == 'r':
            col.find_one()
        else:
            wop = numpy.random.choice(['d', 'i'], p=[0.5, 0.5])
            if wop == 'd':
                col.delete_one({'url': task_url}) 
            else wop == 'i':
                col.insert_one({'status': 'New', 'url': url, 'createdOn': time.time()})

def getOptions(args):
    parser = argparse.ArgumentParser(description="Options to run a task entry generator.")

    parser.add_argument("-t", "--collection", help="Collection name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("-d", "--database", help="Database name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("--rwratio", type=float, help="Read / write ratio of the load test. Defaults to 10.", default=10)
    parser.add_argument("-b", "--db_type", type=Database, 
        help="Which database type to use as a task queue. Options are (mysql, mongo). Defaults to mongo.", 
        default=Database.Mongo, choices=list(Database))
    parser.add_argument("--db_stack", type=DatabaseStack,
        help="Choose which stack to generate entries to. Currently, MongoDB supports (local, aws, atlas); MySQL supports (local, aws). Defaults to local.", 
        default=DatabaseStack.Local, choices=list(DatabaseStack))

    options = parser.parse_args(args)
    return options

if __name__ == "__main__":
    options = getOptions(sys.argv[1:])
    mongodb_load_test(database=options.database, collection=options.collection, db_stack=options.db_stack, rwratio=options.rwratio)