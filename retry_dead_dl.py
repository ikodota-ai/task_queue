#!/usr/bin/env python3
"""
定时复活下载死信任务，建议 crontab:
  0 3 * * * venv/bin/python retry_dead_dl.py >> logs/retry_dead_dl.log 2>&1
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from task_queue_robust import TaskQueue
from config import cfg

queues = ["dl:ig", "dl:x"]

tq = TaskQueue()
tq.redis = tq.redis.from_url(
    f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
    if cfg["queue_redis_password"]
    else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
    decode_responses=True,
)

for q in queues:
    dead = tq.dead_count(q)
    if dead:
        print(f"Requeuing {dead} dead tasks from '{q}'")
        tq.retry_dead(q)
    else:
        print(f"'{q}' dead queue is empty")

print("Done.")
