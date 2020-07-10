import logging
from task_status import TaskStatus
import time
import pymongo
from init_mongo_client import init_mongo_client

class TaskDBMongoDao:
    def __init__(self, database_name, collection_name, stack):
        self.client = init_mongo_client(stack=stack)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    # Returns task URL. Returns None if there's no unprocessed task.
    def findAndReturnAnUnprocessedTask(self):
        task = self.collection.find_one_and_update(
                { "status" : TaskStatus.new.value },
                { "$set": { "status" : TaskStatus.downloading.value}})

        if not task: 
            print("Empty Entry...")
            return None
        task_url = task['url']
        print("Task url is: {}.".format(task_url))
        return task_url

    def removeTask(self, task_url):
        self.collection.delete_one({'url': task_url}) 

    def newTask(self, url):
        self.collection.insert_one({'status': 'New', 'url': url, 'createdOn': time.time()})
        
