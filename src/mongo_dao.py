import logging
from task_status import TaskStatus
import time
import pymongo

class TaskDBMongoDao:
    def __init__(self, database_name, collection_name, use_local_database = False):
        self.client = init_mongo_client(use_local_database=use_local_database)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    # Returns task URL. Returns None if there's no unprocessed task.
    def findAndReturnAnUnprocessedTask(self):
        try:
            task = self.collection.find_one_and_update(
                { "status" : TaskStatus.new.value },
                { "$set": { "status" : TaskStatus.downloading.value}})
        except Exception as e:
            print("Failed to find one and update. Error message: {}.", e)

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
        