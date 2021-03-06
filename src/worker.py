import logging
import threading
import time
import random
import uuid
from mysql_dao import TaskDBMySqlDao
from mongo_dao import TaskDBMongoDao
from database_enum import Database
from database_enum import DatabaseStack
import argparse
import sys
from cloudwatch_agent import CloudWatchAgent
import requests
from database_enum import Database

# fetch_fake_content() generates (1 - URL_GENERATION_CNT_UPPER_BOUND) 
# urls with probability GENERATION_PROBABILITY_NON_ZEROS;
# generates 0 urls with probability GENERATION_PROBABILITY_ZEROS.
URL_GENERATION_CNT_UPPER_BOUND = 4

# Probability of generating (1 - URL_GENERATION_CNT_UPPER_BOUND) urls
GENERATION_PROBABILITY_NON_ZEROS = 0.1

# Probability of generating zero urls
GENERATION_PROBABILITY_ZEROS = 0.6

class Worker:
    # Parameters:
    #   num_spiders: the number of spiders on this worker node, default to 1.
    #   test_mode: generate fake contents without sleeping.
    def __init__(self, database, table, spider_name, metrics_sample_mods, db_type, db_stack, test_mode = False, num_spiders = 1):
        self.table = table
        self.database = database
        self.test_mode = test_mode
        self.num_spiders = num_spiders
        self.spider_name = spider_name
        self.metrics_sample_mods = metrics_sample_mods
        self.db_stack = db_stack
        self.db_type = db_type

    def run(self):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")

        logging.info("WorkerNode    : started")

        for i in range(self.num_spiders):
            spider = threading.Thread(target=self.spider_thread, args = (self.spider_name + str(i),))
            spider.daemon = True
            spider.start()

        spider.join()

    def spider_thread(self, name):
        logging.info("Spider %s: started", name)
        cw = CloudWatchAgent()

        while True:
            start = time.time()
            if self.db_type == Database.Mongo:
                dao = TaskDBMongoDao(self.database, self.table, self.db_stack)
            else:
                dao = TaskDBMySqlDao(self.database, self.table, self.db_stack)
            url = dao.findAndReturnAnUnprocessedTask()

            if url:
                dao.removeTask(url)

            new_urls = self.fetch_fake_content()
            # Write new task url into database.
            for new_url in new_urls:
                dao.newTask(new_url)
            
            # Only sleep in test mode
            if self.test_mode: 
                time.sleep(10)
                logging.info("Sleeping for 10 second...")
            cw.put_latency_metrics(latency=time.time() - start, worker_ip=requests.get('http://169.254.169.254/latest/meta-data/public-ipv4').content, spider_name=name, metrics_sample_mods=self.metrics_sample_mods) 

    def fetch_fake_content(self):
        random_list = []
        for i in range(1, URL_GENERATION_CNT_UPPER_BOUND + 1):
            random_list += [i] * int(GENERATION_PROBABILITY_NON_ZEROS * 100)
        random_list += [0] * int(GENERATION_PROBABILITY_ZEROS * 100)
            
        url_len = random.choice(random_list)
        urls = []
        for i in range(url_len):
            urls.append(str(uuid.uuid4()))
        return urls

def getOptions(args):
    parser = argparse.ArgumentParser(description="Options to run a spider worker.")

    # Defaults to 1 because CloudWatch client is not thread safe.
    parser.add_argument("-s", "--spiders", type=int, help="Number of spiders to run on a worker node. Default is 1.", default=1)
    parser.add_argument("-t", "--table", help="Table name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("-d", "--database", help="Database name. Default is SpiderTaskQueue.", default='SpiderTaskQueue')
    parser.add_argument("-n", "--name", help="Spider name. Default is spider.", default='spider')
    parser.add_argument("--test", type=bool, help="Enbales test mode. Test mode runs with local MySQL database; each spider sleeps 10 seconds after each run. Default is False.", default=False)
    parser.add_argument("-m", "--metrics_sample_mods", type=int, help="The number by which CW put metrics fequency is divided by. Defaults to 10", default=10)
    parser.add_argument("-b", "--db_type", type=Database, 
        help="Which database type to use as a task queue. Options are (mysql, mongo). Defaults to mongo.", 
        default=Database.Mongo, choices=list(Database))
    parser.add_argument("--db_stack", type=DatabaseStack,
        help="Choose database stack. Currently, MongoDB supports (local, aws, atlas); MySQL supports (local, aws). Defaults to local.", 
        default=DatabaseStack.Local, choices=list(DatabaseStack))
    options = parser.parse_args(args)
    return options

if __name__ == "__main__":
    options = getOptions(sys.argv[1:])
    Worker(database=options.database, table=options.table,
        num_spiders=options.spiders, spider_name=options.name, metrics_sample_mods=options.metrics_sample_mods, 
        db_type=options.db_type, db_stack=options.db_stack).run()
