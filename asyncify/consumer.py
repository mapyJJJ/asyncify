import textwrap
from abc import ABC, abstractmethod
from typing import Callable

from asyncify.message_data import MessageData

from .ack import Ack
from .logger import Logger
from .queue_ import Queue

logger = Logger(__name__)


def echo_flag():
    """
    Print flag to stdout for use with -- echo - flag. Args : None. None. Side Effects : Prints flag
    """
    logger.info(
        textwrap.dedent(
            """
                                 _  __               _____          _                            
     /\                        (_)/ _|             / ____|        | |                           
    /  \   ___ _   _ _ __   ___ _| |_ _   _ ______| |    _   _ ___| |_ ___  _ __ ___   ___ _ __ 
   / /\ \ / __| | | | '_ \ / __| |  _| | | |______| |   | | | / __| __/ _ \| '_ ` _ \ / _ \ '__|
  / ____ \\__ \ |_| | | | | (__| | | | |_| |      | |___| |_| \__ \ || (_) | | | | | |  __/ |   
 /_/    \_\___/\__, |_| |_|\___|_|_|  \__, |       \_____\__,_|___/\__\___/|_| |_| |_|\___|_|   
                __/ |                  __/ |                                                    
               |___/                  |___/                                                     """
        )
    )


class ConsumerBase(ABC):
    @abstractmethod
    def run(self):
        """
        Runs the test. Subclasses should override this method to perform the actual test. This method is called by the run method
        """
        """
        Runs the test. Subclasses should override this method to perform the actual test
        """
        ...


class Consumer(ConsumerBase, Ack):
    def __str__(self) -> str:
        """
        Returns a string representation of the consumer. This is used to print the name of the consumer when it is accessed by the : class : ` ~plexapi. core. Consumer ` object.


        @return A string representation of the consumer for debugging purposes. Example :. >>> consumer = Consumer ( name ='myconsumer '
        """
        return f"<consumer {id(self)}>"

    def __init__(self, queue: Queue) -> None:
        """
        Initialize the object. This is called by the : class : ` ~kombu. Queue ` when it is created.

        @param queue - The queue to use for this task. This must be a : class : ` kombu. Queue ` instance.

        @return The newly created queue or None if there was no queue associated with this task. : 0. 5 Added the __name__
        """
        super().__init__(queue)
        self.__queue = queue
        self.__name__ = self.__str__
        self.__repr__ = self.__str__

    def run_task(self, message_data: MessageData, callable_func: Callable):
        """
        Run task and log it. If task fails retry it again. In case of retry we call no_ack method to keep queue up to date

        @param message_data - MessageData containing message to run task
        @param callable_func - Callable to run task with args and kwargs

        @return Task result or None if task failed to run or not acked by queue. no_ack method
        """
        try:
            self.entry(message_data)
            args, kwargs = message_data.message
            task_res = callable_func(*args, **kwargs)
            logger.info(
                "message {} {} task result: {}".format(
                    str(message_data.id_), str(message_data.callable_func_ident), str(task_res)
                )
            )
        except Exception as e:
            # error retry
            # Run a task with a retry count.
            if message_data.retry_count < message_data.max_retry_count:
                message_data.retry_count += 1
                return self.run_task(message_data, callable_func)
            logger.error(f"task executor error : {e}")
            # If the queue is empty or not acked
            if self.__queue.ack:
                self.no_ack(message_data)
            return

        self.ack(message_data.id_)

    def run(self):
        """
        Main loop of the queue. Gets messages from the queue and dispatches them to the callable_func
        """
        echo_flag()
        logger.info("queue: {}".format(self.__queue.__name__))
        task_idents = list(self.__queue.callable_ident_map.keys())
        # register a task to the task_idents list
        for task_ident in task_idents:
            # Add a newline to the task_ident string
            if task_idents[-1] == task_ident:
                task_ident += "\n" * 3
            logger.info("[+]register task: {}".format(task_ident))

        # get all messages from the queue and run the callable function
        while True:
            message_data_dict = self.__queue.get_message()
            message_data = MessageData(**message_data_dict)
            logger.info("[+]get message: {} {}".format(str(message_data.id_), str(message_data.callable_func_ident)))
            callable_func = self.__queue.callable_ident_map[
                message_data_dict["callable_func_ident"]
            ]
            logger.info("[+]handle message: {}".format(str(message_data.id_), str(message_data.callable_func_ident)))
            self.run_task(message_data, callable_func)
            logger.info("-" * 20)
