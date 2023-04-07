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
    `consumer` ascii string
    """
    logger.info(
        textwrap.dedent(
            """                                                
   ___ ___  _ __  ___ _   _ _ __ ___   ___ _ __ 
  / __/ _ \| '_ \/ __| | | | '_ ` _ \ / _ \ '__|
 | (_| (_) | | | \__ \ |_| | | | | | |  __/ |   
  \___\___/|_| |_|___/\__,_|_| |_| |_|\___|_|   
                                                
            """
        )
    )


class ConsumerBase(ABC):
    """
    consumer abstract 
    """
    @abstractmethod
    def run(self):
        """
        subclasses should override this method
        """
        Ellipsis

class Consumer(ConsumerBase, Ack):
    def __str__(self) -> str:
        return f"<consumer {id(self)}>"

    def __init__(self, queue: Queue) -> None:
        """
        Initialize the object. This is called by the : class : ` ~kombu. Queue ` when it is created.

        @param queue - The queue to use for this task. This must be a : class : ` kombu. Queue ` instance.

        @return The newly created queue or None if there was no queue associated with this task. : 0. 5 Added the __name__
        """
        # Acl.__init__(queue)
        # 消费端ack确认机制，初始化将会启动ack子线程检查, 处理任务并进行确认
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
        # entry逻辑会提前将消息加入到no_ack queue中，处理完成后，将会从队列中移除
        # 如果超时还未移出no_ack队列，则会重新将消息投入队列中，等待下次被消费
        # 如果需要ack确认机制，请将 queue.ack 设为 True
        self.entry(message_data)
        args, kwargs = message_data.message

        try:
            # 执行task
            task_res = callable_func(*args, **kwargs)
        except Exception as e:
            # 执行task失败后，进行重试
            # 
            if message_data.retry_count < message_data.max_retry_count:
                message_data.retry_count += 1
                return self.run_task(message_data, callable_func)
            logger.error(f"task executor error : {e}")
            # If the queue is empty or not acked
            if self.__queue.ack:
                self.no_ack(message_data)
            return

        logger.info(
            "message {} {} task result: {}".format(
                str(message_data.id_), str(message_data.callable_func_ident), str(task_res)
            )
        )
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
