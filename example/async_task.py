# from simple_async_task.queue_ import Queue
import os
# python3 example/async_task.py
import sys
from time import time
sys.path.append(os.path.join(os.getcwd()))

import json
import redis
from asyncify.queue_ import Queue
from asyncify.producer import Producer

redis_client = redis.Redis()
message_queue = Queue("test-queue-1", ack=False, serialize_factory=json.dumps, unserialize_factory=json.loads, redis_client=redis_client)
producer = Producer(message_queue)

@producer.register_task
def async_add(a, b):
    return a + b

@producer.register_task
def async_reduce(a, b):
    return a - b

if __name__ == "__main__":
    import random
    import time
    while True:
        time.sleep(random.randint(1, 3))
        async_add.delay(1,2)
        async_reduce.delay(2,1)