import unittest
from worker import Worker
import logging

class WorkerTest(unittest.TestCase):
    def test_fetch_fake_content_uuid_generation(self):
        # iterations = 10000

        # worker = Worker("Dummy", "Dummuy", spider_name="test_spider", test_mode = True)
        # cnt = 0
        # for i in range(iterations):
        #     cnt += len(worker.fetch_fake_content())

        # self.assertGreaterEqual(cnt, iterations * 0.97)
        # self.assertLessEqual(cnt, iterations * 1.03)
