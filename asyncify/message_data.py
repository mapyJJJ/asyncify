from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class MessageData:
    id_: str
    message: Tuple[Tuple, Dict]
    callable_func_ident: str
    max_retry_count: int = 3
    retry_count: int = 0
    start_time: Optional[int] = None
