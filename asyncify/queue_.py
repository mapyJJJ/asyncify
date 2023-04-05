import json
from abc import ABC, abstractmethod
from typing import Any, Callable

from .storage import Storage


class QueueBase(ABC):
    @abstractmethod
    def send_message(self, message: Any):
        """
        Send a message to the client. This is called by the : class : ` Client ` when it wants to send a message to the client.

        @param message - The message to send to the client. It can be any of the types supported by
        """
        ...

    @abstractmethod
    def get_message(self):
        """
        Returns the message to display to the user. This is a tuple of ( message text ) where text is the message that will be displayed
        """
        ...


class Queue(QueueBase):
    def __init__(
        self,
        name: str,
        redis_client: Any,
        serialize_factory: Callable = json.dumps,
        unserialize_factory: Callable = json.loads,
        ack: bool = False,
        ack_timeout: int = 30 * 60,
    ) -> None:
        """
        Initialize the storage with a name. This is the constructor for the Storage class. You can override this in your subclass if you want to do something other than create a : class : ` Storage ` object yourself.

        @param name - The name of the storage. It will be used to refer to it in the event of a change in the storage's state.
        @param ack - Whether to acknowledge the request that is made or not.
        @param serialize_factory - A callable that takes a string and returns a serializable object.
        @param unserialize_factory - A callable that takes a string and returns a deserialized object.

        @return An instance of the Storage class that can be used to interact with the storage and get access to it
        """
        self.__name__ = name
        self.ack = ack
        self.ack_timeout = ack_timeout
        self.redis_client = redis_client
        self.__storage = Storage(
            storage_name=name,
            serialize_factory=serialize_factory,
            unserialize_factory=unserialize_factory,
            redis_client=redis_client,
        )
        self.callable_ident_map = {}

    def queue_size(self):
        """
        Get the size of the queue. This is used to determine how many items are in the queue for a given job.


        @return The number of items in the queue in bytes or None if there are
        """
        return self.__storage.size

    def get_message(self) -> dict:
        """
        Get the message from the storage. This is a low - level method that should be used by clients to get the message that is stored in the storage.


        @return The message that was stored in the storage or None if there was no message stored in the storage at
        """
        return self.__storage.get()

    def send_message(self, message: Any):
        """
        Send a message to the server. This is a low - level method and should not be called directly by user code.

        @param message - The message to send. Must be serializable.

        @return True if the message was sent False otherwise. Note that it is possible to send messages that are unserializable
        """
        self.__storage.set(message)
        return
