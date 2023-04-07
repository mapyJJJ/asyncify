# Asyncify
> 轻量级的异步任务框架


[![License](https://img.shields.io/static/v1?label=db&message=redis&color=red)]()
[![License](https://img.shields.io/static/v1?label=language&message=python&color=orange)]()
[![License](https://img.shields.io/static/v1?label=cli&message=click&color=red)]()

---
#### 使用示例：

(1) - 安装：
```bash
python setup.py install
```
(2) - 使用：
创建一个`task.py`
```python
# task.py

import json
import redis
from asyncify.queue_ import Queue
from asyncify.producer import Producer

# 创建redis 连接
redis_client = redis.Redis()

# 创建message_queue
message_queue = Queue("demo_queue", ack=False, serialize_factory=json.dumps, unserialize_factory=json.loads, redis_client=redis_client)

# 创建producer
producer = Producer(message_queue)

# producer.register_task 加到需要异步的方法上
@producer.register_task
def add(a, b):
    return a + b

if __name__ == "__main__":
    # 模拟每隔1s，调用一次 add 方法
    import time
    while True:
        time.sleep(1)
        async_add.delay(1,2)
```

运行当前task.py脚本，脚本每个1s会调用add方法，每次调用会产生一个消息，这些消息将等待消费者的执行

- 使用python运行: `python3 task.py`

- 启动消费者：`asyncify-ctl --queue task.message_queue customer`



消费者运行截图:

![x](consumer-info.PNG)
