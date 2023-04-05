# python3 example/run_async_task.py

import sys
import os
sys.path.append(os.path.join(os.getcwd()))

from example.async_task import message_queue
from asyncify.consumer import Consumer

consumer = Consumer(message_queue)

if __name__ == "__main__":
    consumer.run()