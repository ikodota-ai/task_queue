"""
run_schedules.py — 调度执行器

被系统 cron 每分钟触发一次，连 Redis 检查哪些调度任务到期了，
到期的就 subprocess 调对应的 Python 脚本去执行。

=== 整体流程 ===

  crontab (每分钟) → run_schedules.py
                          │
                          ├─ 连 Redis 读 schedule:index (Set)
                          │   里面存着所有调度的 id
                          │
                          ├─ 对每个 id：
                          │   ├─ 读 schedule:{id} (Hash)
                          │   │  字段：name/command/args/cron/enabled/next_run/last_run
                          │   │
                          │   ├─ enabled != 1  → 跳过
                          │   ├─ next_run == 0 → 没算过下次时间，调用 _cron_next 算一次
                          │   ├─ next_run >  now → 没到期，跳过
                          │   └─ next_run <= now → 到期！执行 ↓
                          │
                          └─ subprocess.Popen("python scheduler.py --platform ig ...")
                              执行完后更新 last_run / next_run

=== Redis 数据结构 ===

  schedule:index                     Set    {"abc123", "def456", ...}
  schedule:abc123                    Hash   {
      "name":    "IG增量-6h",         # 显示名称
      "command": "scheduler",         # Python 脚本名（不含 .py）
      "args":    "--platform ig --interval 21600",   # 命令行参数
      "cron":    "0 */6 * * *",       # cron 表达式（空 = 一次性）
      "enabled": "1",                 # 1=启用 0=暂停
      "last_run":"1717300000",        # 上次执行时间戳
      "next_run":"1717321600",        # 下次执行时间戳
      "created": "1717200000"         # 创建时间
  }

=== cron 表达式语法 ===

  分 时 日 月 周
  *  *  *  *  *      每分钟
  0  */6 * * *        每 6 小时整点
  0  8   * * *        每天 8:00

用法（添加到 crontab）:
  * * * * * cd /home/unis/dev/cc/task_queue && venv/bin/python run_schedules.py >> logs/schedule.log 2>&1
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


# ----------------------------------------------------------------
# Redis 连接 — 使用队列 Redis（db=1），与 monitor.py 读写同一个库
# ----------------------------------------------------------------
def _get_redis():
    """连接队列 Redis，读取 schedule:* 配置。"""
    return redis.Redis(
        host=cfg["queue_redis_host"],
        port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"],
        db=cfg["queue_redis_db"],
        decode_responses=True,
        socket_timeout=5,
    )


# ----------------------------------------------------------------
# Cron 表达式解析 — 判断某个时间点是否命中
# ----------------------------------------------------------------
def _cron_match(value: int, pattern: str) -> bool:
    """
    检查 value 是否匹配 cron 的单个字段。

    pattern 支持三种格式：
      "*"        → 任意值都匹配
      "*/6"      → 能被 6 整除的值（0,6,12,18,24,30,36,42,48,54）
      "0,30"     → 等于 0 或 30

    示例：
      _cron_match(15, "*")      → True   (任意分钟)
      _cron_match(12, "*/6")    → True   (12 能被 6 整除)
      _cron_match(15, "*/6")    → False  (15 不能被 6 整除)
      _cron_match(0,  "0,30")   → True   (0 在列表中)
      _cron_match(15, "0,30")   → False  (15 不在列表中)
    """
    if pattern == "*":
        return True
    if "/" in pattern:
        # "*/6" → 每隔 6 个单位
        _, step = pattern.split("/")
        return value % int(step) == 0
    # "0,30" → 精确匹配列表中的值
    for p in pattern.split(","):
        if str(value) == p.strip():
            return True
    return False


def _cron_next(cron_expr: str, from_ts: int = None) -> int:
    """
    根据 cron 表达式，计算「从 from_ts 之后」第一次触发的时间戳。

    参数：
      cron_expr: 5 段 cron 表达式，如 "0 */6 * * *"
      from_ts:   起始时间戳，不传则用当前时间

    返回：
      下次触发的时间戳（秒）。如果 cron 为空返回 None。

    算法：
      从起始时间开始，每分钟往后推，找到第一个满足 cron 的时间点。
      最多往后找一年（525600 分钟），找不到返回 None。
    """
    # 空 cron = 一次性任务，不需要算下次
    if not cron_expr or not cron_expr.strip():
        return None

    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return None

    minute, hour, day, month, weekday = parts

    # 从起始时间 +1 分钟开始往后找
    now = datetime.fromtimestamp(from_ts or time.time()) + timedelta(minutes=1)

    # 每分钟检查一次，最多找一年
    for _ in range(525600):
        if (_cron_match(now.minute, minute) and
                _cron_match(now.hour, hour) and
                _cron_match(now.day, day) and
                _cron_match(now.month, month) and
                _cron_match(now.weekday(), weekday)):
            return int(now.timestamp())
        now += timedelta(minutes=1)

    return None


# ----------------------------------------------------------------
# 主逻辑：扫描 Redis → 找到期任务 → 执行
# ----------------------------------------------------------------
def run():
    """
    被 crontab 每分钟调用一次。

    流程：
      1. 读 schedule:index 拿到所有调度 id
      2. 逐条读 schedule:{id} 拿到详细配置
      3. enabled != 1 的跳过（已暂停）
      4. next_run == 0 的：还没算过下次时间，用 _cron_next 算一次然后跳过
      5. next_run > now 的：还没到时间，跳过
      6. next_run <= now：到期了！subprocess 调用对应 Python 脚本
      7. 执行后更新 last_run 和 next_run
      8. 如果是一性次任务（cron 为空），执行后自动禁用
    """
    r = _get_redis()

    # Step 1: 拿到所有调度 id
    ids = r.smembers("schedule:index")
    if not ids:
        return  # 没有任何调度，直接返回

    now = int(time.time())
    executed = 0
    basedir = os.path.dirname(os.path.abspath(__file__))  # 脚本所在目录，执行时以此为工作目录

    # Step 2-8: 逐条检查
    for sid in ids:
        s = r.hgetall(f"schedule:{sid}")
        if not s or s.get("enabled") != "1":
            continue   # 已暂停或数据异常，跳过

        next_run = int(s.get("next_run", 0))

        # --- 还没算过下次时间（新建的调度，或重置过）---
        if next_run == 0:
            cron = s.get("cron", "")
            if cron and cron.strip():
                # 根据 cron 算出下次时间，写入 Redis
                next_ts = _cron_next(cron, now)
                r.hset(f"schedule:{sid}", "next_run", str(next_ts or 0))
            # 本次不执行，等下一轮算了 next_run 再判断
            continue

        # --- 还没到时间 ---
        if next_run > now:
            continue

        # ============ 到期！开始执行 ============

        # 拼命令：python scheduler.py --platform ig --interval 21600
        cmd = s.get("command", "")       # 脚本名，例如 "scheduler"
        args = s.get("args", "")         # 参数，例如 "--platform ig"
        script = f"{cmd}.py"             # "scheduler" → "scheduler.py"
        full_cmd = f"python {script} {args}"

        logger.info(f"[{s.get('name')}] → {full_cmd}")
        try:
            # 用 subprocess.Popen 异步启动，不阻塞等待
            subprocess.Popen(full_cmd, shell=True, cwd=basedir)
            executed += 1
        except Exception as e:
            logger.error(f"[{s.get('name')}] Failed: {e}")

        # --- 执行后更新状态 ---
        r.hset(f"schedule:{sid}", "last_run", str(now))

        cron = s.get("cron", "")
        if cron and cron.strip():
            # 重复性任务：计算下一次触发时间
            next_ts = _cron_next(cron, now)
            r.hset(f"schedule:{sid}", "next_run", str(next_ts or 0))
        else:
            # 一次性任务：执行后自动禁用
            r.hset(f"schedule:{sid}", mapping={"enabled": "0", "next_run": "0"})

    r.close()

    if executed:
        logger.info(f"Executed {executed} schedules")


# ----------------------------------------------------------------
# 入口：配好日志，执行 run()
# ----------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run()

