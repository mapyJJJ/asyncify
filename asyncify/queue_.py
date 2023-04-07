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
        ack: bool = False,
        max_retry_count: int = 3,
        ack_timeout: int = 30 * 60,
        serialize_factory: Callable = json.dumps,
        unserialize_factory: Callable = json.loads,
    ) -> None:
        """
        Initialize the storage with a name. 
        This is the constructor for the Storage class. 
        You can override this in your subclass if you want to do something other than create a : class : ` Storage ` object yourself.

        @param name - 消息队列名称
        @param ack - 是够需要开启ack机制
        @param ack_timeout - 消息处理超时时间（s）
        @param serialize_factory - 序列化器
        @param unserialize_factory - 反序列化器
        @param redis_client - redis客户端对象
        @param max_retry_count - 最大重试次数
        """
        # queue.__name__
        self.__name__ = name

        # queue.ack
        self.ack = ack

        # queue.ack_timeout
        self.ack_timeout = ack_timeout

        # queue.redis_client
        self.redis_client = redis_client

        # queue.max_retry_count
        self.max_retry_count = max_retry_count

        # queue.__storage object
        self.__storage = Storage(
            storage_name=name,
            serialize_factory=serialize_factory,
            unserialize_factory=unserialize_factory,
            redis_client=redis_client,
        )

        # 用于缓存注册在队列上task信息
        self.callable_ident_map = {}

    def queue_size(self) -> int:
        """
        获取队列当前总长度
        Get the size of the queue. 
        This is used to determine how many items are in the queue for a given job.
        """
        return self.__storage.size

    def get_message(self) -> dict:
        """
        pop 队列中的消息，该方法是阻塞的
        Get the message from the storage. This is a low - level method that should be used by clients to get the message that is stored in the storage.


        @return The message that was stored in the storage or None if there was no message stored in the storage at
        """
        return self.__storage.get()

    def send_message(self, message: Any):
        """
        将消息投放到队列中
        Send a message to the server. This is a low - level method and should not be called directly by user code.

        @param message - The message to send. Must be serializable.

        @return True if the message was sent False otherwise. Note that it is possible to send messages that are unserializable
        """
        self.__storage.set(message)
        return
