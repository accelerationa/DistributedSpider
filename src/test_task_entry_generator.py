import time
from init_mysql_connector_cursor import init_mysql_connector_and_cursor
import uuid
from mysql_dao import TaskDBMySqlDao
from mongo_dao import TaskDBMongoDao
from database_enum import DatabaseStack
from database_enum import Database
import argparse
import sys

# This method generates task entries with (url, 'New', timestamp) format.
# Make sure database and table exists before calling this method.
def generate_test_entries(num_entries, database, table, db_type, db_stack):

    if db_type == Database.Mongo:
        dao = TaskDBMongoDao(database_name=database, collection_name=table, stack=db_stack)
    else:
        dao = TaskDBMySqlDao(database=database, table=table, stack=db_stack)
    for i in range(num_entries):
        dao.newTask(url=str(uuid.uuid4()))

def getOptions(args):
    parser = argparse.ArgumentParser(description="Options to run a task entry generator.")

    parser.add_argument("-e", "--entries", type=int, help="Number of entries to generate. Default is 10.", default=10)
    parser.add_argument("-t", "--table", help="Table name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("-d", "--database", help="Database name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
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

    if options.db_type != Database.Mongo and options.db_stack == DatabaseStack.Atlas:
                raise("Atlas is only applicable with mongodb stack. \
                    You are using {} stack with {} database type.".format(options.db_stack, options.db_type))

    generate_test_entries(database=options.database, table=options.table, num_entries=options.entries, db_type=options.db_type, db_stack=options.db_stack)