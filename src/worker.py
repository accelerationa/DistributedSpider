import logging
import threading
import time
import random
import uuid

SPIDER_CNT_PER_NODE = 10

# fetch_fake_content() generates (1 - URL_GENERATION_CNT_UPPER_BOUND) 
# urls with probability GENERATION_PROBABILITY_NON_ZEROS;
# generates 0 urls with probability GENERATION_PROBABILITY_ZEROS.
URL_GENERATION_CNT_UPPER_BOUND = 4

# Probability of generating (1 - URL_GENERATION_CNT_UPPER_BOUND) urls
GENERATION_PROBABILITY_NON_ZEROS = 0.1

# Probability of generating zero urls
GENERATION_PROBABILITY_ZEROS = 0.6

def push_task_queue(task_uuids):
    return

def poll_task_queue(task_uuid):
    return


def get_new_task():
    return
    # read a task that has status New from DB
    # Mark as downloading
    # return UUID

def fetch_fake_content(test_mode = False):
    # Only sleep in test mode
    if not test_mode: 
        time.sleep(1)

    random_list = []
    for i in range(1, URL_GENERATION_CNT_UPPER_BOUND + 1):
        random_list += [i] * int(GENERATION_PROBABILITY_NON_ZEROS * 100)
    random_list += [0] * int(GENERATION_PROBABILITY_ZEROS * 100)
        
    url_len = random.choice(random_list)
    urls = []
    for i in range(url_len):
        urls.append(str(uuid.uuid4()))
    return urls

def spider_thread(name):
    logging.info("Spider %s: startws", name)
    while True:
        task_uuid = get_new_task()
        push_task_queue(fetch_fake_content())
        poll_task_queue(task_uuid)


if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    logging.info("WorkerNode    : started")

    for i in range(SPIDER_CNT_PER_NODE):
        spider = threading.Thread(target=spider_thread, args=(i,))
        spider.start()