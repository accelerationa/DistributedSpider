import unittest
from mongo_dao import TaskDBMongoDao
from task_status import TaskStatus
import uuid
import time
import pymongo
from init_mongo_client import init_mongo_client

class MongoDaoTestNoMock(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.database_name = 'MongoDaoTestNoMockDatabase'
        collection_name = 'MongoDaoTestNoMockCollection'

        # Create database and collection.
        self.client = init_mongo_client(use_local_database=True)
        self.database = self.client[self.database_name]
        self.collection = self.database[collection_name]
        self.collection.insert_one({'url': 'url1', 'status': TaskStatus.new.value, 'createOn': str(time.time())})
        
        # Init dao
        self.dao = TaskDBMongoDao(database_name=self.database_name, collection_name=collection_name, use_local_database = True)

    @classmethod
    def tearDownClass(self):
        self.client.drop_database(self.database_name)

    def test_findAndReturnAnUnprocessedTask_removeTask(self):
        url = self.dao.findAndReturnAnUnprocessedTask()
        print(url)
        assert url == 'url1'

        self.dao.removeTask(url)
        assert not self.collection.find_one()

    def test_newTask(self):
        url = str(uuid.uuid4())
        self.dao.newTask(url)
        assert self.collection.find_one({'url': url}).get('url') == url
