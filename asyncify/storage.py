import json
from abc import ABC, abstractmethod, abstractproperty
from typing import Any

import redis


class StorageBase(ABC):
    @abstractproperty
    def size(self):
        """
        The number of elements in the array. This is a constant value in the sense that it can be used to determine the size of the array
        """
        ...

    @abstractmethod
    def set(self, message: Any):
        """
        Set the message. This is called by the : class : ` Event ` to notify the event that something has changed.

        @param message - The message to set in the event's
        """
        ...

    @abstractmethod
    def get(self):
        """
        Get the value. This is a no - op if the value is not set. Returns result :
        """
        ...


class Storage(StorageBase):
    def __init__(
        self,
        storage_name: str,
        serialize_factory=json.dumps,
        unserialize_factory=json.loads,
        redis_client=redis.Redis(),
    ) -> None:
        """
        Initialize the class. This is the constructor for the Queue class. You can pass a factory for serializing and deserializing objects

        @param storage_name - The name of the storage to use
        @param serialize_factory - A factory for serializing objects defaults to json. dumps
        @param unserialize_factory - A factory for deserializing objects defaults to json. loads
        @param redis_client - A redis client for communicating with the queue defaults to redis. Redis

        @return A reference to the Queue class for use in __init__ (... ) calls. Note that the serialization is done in a thread
        """
        self.redis_client = redis_client
        self.serialize_factory = serialize_factory
        self.unserialize_factory = unserialize_factory
        self.__message_list_key = f"message-queue-{storage_name}"

    @property
    def size(self):
        """
        Returns the number of messages in the queue. This is an alias for LLEN. The return value is cached so it's safe to call multiple times without re - allocating the queue.


        @return The number of messages in the queue or - 1 if there was an error during the operation ( due to queue overflow
        """
        return self.redis_client.llen(self.__message_list_key)

    def __put_list(self, item):
        """
        Put a list into the queue. This is used to store items that are in the queue. The item is put in the LPUSH list

        @param item - The item to put
        """
        self.redis_client.lpush(self.__message_list_key, item)

    def __pop_list(self) -> str:
        """
        Pop and return the first item from the list. This is used to get the list of messages that have been sent to the server.


        @return The first item in the list or None if there are no items in the list ( no error is raised
        """
        (_, item) = self.redis_client.brpop(self.__message_list_key, timeout=0)
        return item.decode()

    def set(self, message: Any):
        """
        Set a message to be sent. This is a method that can be used to send a message to the server.

        @param message - The message to be sent. It must be serializable to JSON

        @return A tuple of HTTP status code and
        """

        serialized_message = self.serialize_factory(message)

        self.__put_list(serialized_message)

        return 200, "ok"

    def get(self):
        """
        Get a message from the queue. This is a blocking call. If there are no messages to return the queue is empty.


        @return The message that was popped from the queue or None if none was found. Note that it is possible that the queue is empty
        """
        item = self.__pop_list()
        message = self.unserialize_factory(item)
        return message
