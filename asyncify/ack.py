import json
import threading
import time
from datetime import datetime

from redis import Redis

from .logger import Logger
from .message_data import MessageData
from .queue_ import Queue

logger = Logger(__name__)


class AckCheck:
    def __init__(self, ack_queue, ack_timeout: int, message_queue: Queue) -> None:
        """
        Initialize the class. This is called by the : class : ` ~twisted. python. net. Socket ` when it is created.

        @param ack_queue - The queue to which acknowledgements are sent.
        @param ack_timeout - The timeout in seconds for each acknowledgement.
        @param message_queue - The queue to which messages are sent.

        @return A reference to the newly created object. It is the responsibility of the caller to close the object when finished
        """
        self.__ack_queue = ack_queue
        self.__ack_timeout = ack_timeout
        self.__message_queue = message_queue

    def check(self, check_interval_time: float):
        """
        Checks to see if there are messages that have not been acknowledged. If so the message is reposted to the queue

        @param check_interval_time - time in seconds to
        """
        # Acknowledges messages from the queue.
        while True:
            redis_client: Redis = self.__ack_queue.redis_client
            # Acknowledges all messages in the queue.
            if redis_client.exists(self.__ack_queue.name):
                ack_map = redis_client.hgetall(self.__ack_queue.name)
                keys = ack_map.keys()
                logger.info(
                    "[ack] There are {} messages that have not been confirmed.".format(
                        str(len(keys))
                    )
                )
                # Remove all messages from the queue.
                for key in keys:
                    key = key.decode()
                    message_dict = redis_client.hget(self.__ack_queue.name, key)
                    # If message_dict is empty continue to skip.
                    if not message_dict:
                        continue
                    message_dict = json.loads(message_dict.decode())
                    # Send a message to the ack queue.
                    if (
                        datetime.now().timestamp() - message_dict["start_time"]
                    ) > self.__ack_timeout:
                        del message_dict["start_time"]
                        logger.error(
                            f"message_id: {message_dict['id']}, more than {self.__ack_timeout} seconds have not been ack, will be reposted to the queue"
                        )
                        self.__message_queue.send_message(message_dict)
                        self.__ack_queue.hdel(self.__ack_queue.name, key)
                        logger.error(
                            f"message_id: {message_dict['id']} has been reposted to queue"
                        )
            time.sleep(check_interval_time)

    def run(self):
        """
        Run the test in a seperate thread to avoid deadlock. This is called by the run ()
        """
        t = threading.Thread(target=self.check, args=(10,))
        t.start()


class AckQueue:
    def __init__(self, name, redis_client: Redis) -> None:
        """
        Initialize the class with a name and a redis client. This is used to make sure we don't accidentally get out of sync with the redis server

        @param name - The name of the redis server
        @param redis_client - The redis client to use for the connection

        @return True if the connection was successful False if it was not ( in which case the client should be re - used
        """
        self.name = name
        self.redis_client = redis_client

    def no_ack_add(self, key, value):
        """
        Add a value to the hash without acknowledging the change. This is useful for cases where you don't want to wait for the value to be added to the hash before it's acknowledged.

        @param key - The key to add the value to. If the key already exists it will be overwritten.
        @param value - The value to add to the hash. This can be any type
        """
        self.redis_client.hset(self.name, key, value)

    def ack(self, key):
        """
        Acknowledge receipt of a message. This is used to remove the message from the hash table. If the key does not exist nothing happens

        @param key - The key to acknowledge
        """
        # Remove the key from the redis cache
        if self.redis_client.exists(self.name):
            self.redis_client.hdel(self.name, key)


class Ack:
    def __init__(self, queue: Queue) -> None:
        """
        Initialize the class. This is called by the : class : ` ~burp. Queue ` when it is created.

        @param queue - The queue to operate on. Must be a : class : ` ~burp. Queue `
        """
        self.__queue = queue
        self.__ack_queue = AckQueue(
            f"async_message_ack_queue:{self.__queue.__name__}",
            redis_client=queue.redis_client,
        )
        self.need_ack = self.__queue.ack

        # Ack check if we need to ack the queue.
        if self.need_ack:
            ack_check = AckCheck(
                self.__ack_queue, self.__queue.ack_timeout, self.__queue
            )
            ack_check.run()

    def entry(self, message_data: MessageData):
        """
        Add a message to the queue. This is a no - op if there is no need to acknowledge the message

        @param message_data - The message to add to the queue

        @return True if the message was added False if it was already in the queue ( no ack is needed for this
        """
        # If need_ack is set to true the ack is not required.
        if not self.need_ack:
            return
        message_data.start_time = int(datetime.now().timestamp())
        self.__ack_queue.no_ack_add(message_data.id_, json.dumps(message_data.__dict__))

    def ack(self, message_data_id: str):
        """
        Acknowledge the message with the given message_data_id. This is a no - op if there is no need to acknowledge the message.

        @param message_data_id - The id of the message

        @return True if acknowledgement was
        """
        # If need_ack is set to true the ack is not required.
        if not self.need_ack:
            return
        logger.info("[+]message {} is ack".format(message_data_id))
        self.__ack_queue.ack(message_data_id)

    def no_ack(self, message_data: MessageData):
        """
        Send a message without acknowledging it. This is useful for messages that have been acknowledged in the meantime.

        @param message_data - The message to send. Must be a MessageData object

        @return True if the message was sent
        """
        # If need_ack is set to true the ack is not required.
        if not self.need_ack:
            return
        self.__queue.send_message(message_data)
        self.__ack_queue.ack(message_data.id_)
