import random
from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from typing import Callable, Optional

from redis.exceptions import ConnectionError, RedisError

from .message_data import MessageData
from .queue_ import Queue


class ProducerBase(ABC):
    @abstractmethod
    def register_task(self, cls: Optional[Callable] = None):
        """
        Register a task to be executed. This is a no - op if the task is already registered.

        @param timeout - Time in seconds to wait for the task to complete. Defaults to 30 minutes. A timeout of 0 means no timeout.
        @param timeout_retry - Whether to retry on timeout. Defaults to False.
        @param ack - Whether to acknowledgement with the task's status
        """
        ...

    @abstractmethod
    def delay(self, **kwargs):
        """
        Delay the execution of the task. This is a no - op if the task doesn't have a delay
        """
        ...


id_factory = lambda: str(datetime.now().timestamp()) + str(random.randint(0, 100000))


class ClsIsNotCallable(Exception):
    def __init__(self, *args: object) -> None:
        """
        Initialize the : class : ` ~kivy. kivy. Key `. This is the constructor for the

        @param args - Arguments to pass to the constructor

        @return True if initialization was successful False if it was not ( or the key already exists in the key space
        """
        super().__init__(*args)


class Producer(ProducerBase):
    def __init__(self, queue: Queue) -> None:
        """
        Initialize the task. This is called by the : class : ` Task ` when it is created.

        @param queue - The queue to operate on. It must be a : class : ` Queues ` instance.

        @return The newly created task or None if there was no task to operate on. : 0. 5 Added support for ` queue `
        """
        self.__queue = queue

    def register_task(
        self,
        cls: Optional[Callable] = None,
        ack_timeout: int = 30 * 60,
        max_retry_count: int = 0
    ):
        """
        Register a ta`sk` to be executed when : meth : ` run ` is called. This is useful for tasks that need to be executed in a different thread than the one that will be running.

        @param cls - The class to register. If None the class will be registered as a function that does nothing.
        @param timeout - The timeout in seconds for the task to run.
        @param ack - Whether to acknowledgement the task with the server.

        @return A decorator that can be used to unregister the task from the queue. Example :. from twisted. internet import
        """
        self.ack_timeout = ack_timeout
        self.max_retry_count = max_retry_count

        # Returns a callable that will be called when a task is registered.
        if not cls:
            return lambda cls: self.register_task(cls=cls)
        # Raise ClsIsNotCallable if cls is not callable.
        if not callable(cls):
            raise ClsIsNotCallable()

        callable_ident = self.__queue.__name__ + ":" + cls.__name__
        cls.delay = partial(self.delay, callable_ident=callable_ident)
        self.__queue.callable_ident_map[callable_ident] = cls
        return cls

    def delay(self, *args, callable_ident: str, **kwargs):
        """
        Delay a call to a callable. This is useful for delaying an event that is triggered by a user - defined function such as a function of some sort.

        @param callable_ident - Identifies the callable to call when the event is
        """
        ack_timeout = self.ack_timeout or self.__queue.ack_timeout
        max_retry_count = self.max_retry_count or self.__queue.max_retry_count
        
        message_data = MessageData(
            id_=id_factory(),
            message=(args, kwargs),
            ack_timeout=ack_timeout,
            max_retry_count=max_retry_count,
            callable_func_ident=callable_ident,
        ).__dict__

        try:
            self.__queue.send_message(message_data)
        except RedisError as e:
            if isinstance(e, ConnectionError):
                raise ConnectionError()
