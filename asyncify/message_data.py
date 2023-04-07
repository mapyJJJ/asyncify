from datetime import datetime
from dataclasses import dataclass

from typing import Dict, Optional, Tuple


@dataclass
class MessageData:
    # 消息唯一id
    id_: str
    # task name
    callable_func_ident: str
    # task params
    message: Tuple[Tuple, Dict]

    # 重试次数(无需设置)
    retry_count: int = 0
    # 最大重试次数
    max_retry_count: int = 3
    # 消息超时时间（s)
    ack_timeout: int = 30 * 60
    # 消息开始时间
    start_time: Optional[int] = int(datetime.now().timestamp())
