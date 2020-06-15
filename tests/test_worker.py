import unittest
import worker

class WorkerTest(unittest.TestCase):
    def test_fetch_fake_content_uuid_generation(self):
        iterations = 10000

        cnt = 0
        for i in range(iterations):
            cnt += len(worker.fetch_fake_content(test_mode = True))

        self.assertGreaterEqual(cnt, iterations * 0.97)
        self.assertLessEqual(cnt, iterations * 1.03)