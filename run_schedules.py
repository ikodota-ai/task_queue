"""
调度执行器 — 由系统 cron 每分钟触发，扫描 Redis 调度表并执行到期任务。

用法（crontab）:
  * * * * * cd /path/to/task_queue && python run_schedules.py
"""
import json
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta

import redis

from config import cfg

logger = logging.getLogger("SchedExec")


def _get_redis():
    return redis.Redis(
        host=cfg["queue_redis_host"],
        port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"],
        db=cfg["queue_redis_db"],
        decode_responses=True,
        socket_timeout=5,
    )


def _cron_match(value, pattern):
    if pattern == "*":
        return True
    if "/" in pattern:
        _, step = pattern.split("/")
        return value % int(step) == 0
    for p in pattern.split(","):
        if str(value) == p.strip():
            return True
    return False


def _cron_next(cron_expr, from_ts=None):
    """简单 cron 解析，返回下次执行时间戳。空=一次性。"""
    if not cron_expr or not cron_expr.strip():
        return None
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return None
    minute, hour, day, month, weekday = parts
    now = datetime.fromtimestamp(from_ts or time.time()) + timedelta(minutes=1)
    for _ in range(525600):
        if (_cron_match(now.minute, minute) and _cron_match(now.hour, hour) and
                _cron_match(now.day, day) and _cron_match(now.month, month) and
                _cron_match(now.weekday(), weekday)):
            return int(now.timestamp())
        now += timedelta(minutes=1)
    return None


def run():
    r = _get_redis()
    ids = r.smembers("schedule:index")
    if not ids:
        return

    now = int(time.time())
    executed = 0
    basedir = os.path.dirname(os.path.abspath(__file__))

    for sid in ids:
        s = r.hgetall(f"schedule:{sid}")
        if not s or s.get("enabled") != "1":
            continue

        next_run = int(s.get("next_run", 0))

        # 未计算过下次时间 → 计算一次
        if next_run == 0:
            cron = s.get("cron", "")
            if cron and cron.strip():
                next_ts = _cron_next(cron, now)
                r.hset(f"schedule:{sid}", "next_run", str(next_ts or 0))
            continue

        # 未到期 → 跳过
        if next_run > now:
            continue

        # === 执行 ===
        cmd = s.get("command", "")
        args = s.get("args", "")
        script = f"{cmd}.py"
        full_cmd = f"python {script} {args}"

        logger.info(f"[{s.get('name')}] → {full_cmd}")
        try:
            subprocess.Popen(full_cmd, shell=True, cwd=basedir)
            executed += 1
        except Exception as e:
            logger.error(f"[{s.get('name')}] Failed: {e}")

        # 更新状态
        r.hset(f"schedule:{sid}", "last_run", str(now))
        cron = s.get("cron", "")
        if cron and cron.strip():
            next_ts = _cron_next(cron, now)
            r.hset(f"schedule:{sid}", "next_run", str(next_ts or 0))
        else:
            r.hset(f"schedule:{sid}", mapping={"enabled": "0", "next_run": "0"})

    r.close()
    if executed:
        logger.info(f"Executed {executed} schedules")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()
