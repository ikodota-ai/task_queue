"""
调度器 — 按时间间隔为已完成全量的用户自动投增量任务。

用法:
  python scheduler.py --platform ig                     # IG 增量调度 (默认 6h)
  python scheduler.py --platform x  --interval 3600     # X 增量调度 (1h)
  python scheduler.py --platform all --interval 7200    # 全部平台, 2h
  python scheduler.py --platform ig --dry-run           # 只看不动
  python scheduler.py --platform ig --limit 20          # 最多入队 20 个

逻辑:
  1. 扫描业务 Redis instagram:*:state / twitter:*:state
  2. 筛选 full_done=1 的用户
  3. 检查 incr_last_time，距现在超过 interval 秒的入队
  4. 去重：已在 incr 队列中或正在抓取的用户跳过
"""

import argparse
import json
import logging
import time
import uuid

import redis

from config import cfg

logger = logging.getLogger("Scheduler")

PLATFORM_CONFIG = {
    "ig": {
        "prefix": "instagram",
        "queue": "crawl:ig:incr",
        "func": "ig_incremental_crawl",
    },
    "x": {
        "prefix": "twitter",
        "queue": "crawl:x:incr",
        "func": "x_incremental_crawl",
    },
}


def _get_state_redis():
    return redis.Redis(
        host=cfg["redis_host"],
        port=cfg["redis_port"],
        password=cfg["redis_password"],
        db=cfg["redis_db"],
        decode_responses=True,
        socket_timeout=10,
    )


def _get_queue_redis():
    return redis.Redis(
        host=cfg["queue_redis_host"],
        port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"],
        db=cfg["queue_redis_db"],
        decode_responses=True,
        socket_timeout=10,
    )


def _incr_queue_users(qr, queue_name):
    """返回已在 incr 队列(pending+retry+processing)中的 user_id 集合，用于去重。"""
    existing = set()
    for raw in qr.lrange(f"queue:{queue_name}", 0, -1):
        try:
            d = json.loads(raw)
            if d.get("args"):
                existing.add(str(d["args"][0]))
        except Exception:
            pass
    for raw in qr.zrange(f"retry:{queue_name}", 0, -1):
        try:
            d = json.loads(raw)
            if d.get("args"):
                existing.add(str(d["args"][0]))
        except Exception:
            pass
    # processing 集合
    for tid in qr.hkeys(f"processing:{queue_name}"):
        data = qr.get(f"processing_data:{tid}")
        if data:
            try:
                d = json.loads(data)
                if d.get("args"):
                    existing.add(str(d["args"][0]))
            except Exception:
                pass
    return existing


def _crawling_users(sr, prefix):
    """返回当前正在被其他 worker 抓取的用户集合（有 crawl lock 的）。"""
    crawling = set()
    for k in sr.keys(f"{prefix}:*:crawling"):
        uid = k.split(":")[1]
        if uid:
            crawling.add(uid)
    return crawling


def run(platform: str, interval: int, dry_run: bool = False, limit: int = 0):
    sr = _get_state_redis()
    qr = _get_queue_redis()
    now = int(time.time())

    platforms = ["ig", "x"] if platform == "all" else [platform]
    total = 0

    for plat in platforms:
        conf = PLATFORM_CONFIG[plat]
        prefix = conf["prefix"]
        queue_name = conf["queue"]
        func_name = conf["func"]

        # 1. 扫描所有 state key
        state_keys = list(sr.keys(f"{prefix}:*:state"))
        if not state_keys:
            logger.info(f"[{plat}] No state keys found")
            continue

        # 2. 去重集合
        existing_in_queue = _incr_queue_users(qr, queue_name)
        crawling = _crawling_users(sr, prefix)

        # 3. Pipeline 批量读 state
        pipe = sr.pipeline()
        uids = []
        for k in state_keys:
            uid = k.split(":")[1]
            uids.append(uid)
            pipe.hget(k, "full_done")
            pipe.hget(k, "incr_last_time")
            pipe.hget(k, "last_scrape_time")
        results = pipe.execute()

        # 4. 筛选
        candidates = []
        for i, uid in enumerate(uids):
            full_done = results[i * 3]
            incr_last = results[i * 3 + 1]
            last_scrape = results[i * 3 + 2]

            if full_done != "1":
                continue
            if uid in existing_in_queue:
                continue
            if uid in crawling:
                continue

            last_ts = int(float(incr_last or last_scrape or 0))
            if last_ts == 0:
                candidates.append((uid, 0))
            elif now - last_ts >= interval:
                candidates.append((uid, now - last_ts))

        logger.info(f"[{plat}] state_keys={len(state_keys)} filtered={len(candidates)} "
                     f"(in_queue={len(existing_in_queue)} crawling={len(crawling)})")

        if limit and total + len(candidates) > limit:
            candidates = candidates[:limit - total]

        enqueued = 0
        for uid, elapsed in candidates:
            if dry_run:
                ago = f"{elapsed // 3600}h ago" if elapsed else "never"
                logger.info(f"  [DRY-RUN] {uid:30s}  {ago}")
                enqueued += 1
                continue

            task_data = {
                "task_id": str(uuid.uuid4()),
                "func_name": func_name,
                "args": [uid, 0],          # db_task_id=0，不再写 MySQL
                "kwargs": {},
                "queue_name": queue_name,
                "retry_count": 0,
                "enqueued_at": time.time(),
            }
            qr.rpush(f"queue:{queue_name}", json.dumps(task_data))
            enqueued += 1

        total += enqueued
        logger.info(f"[{plat}] enqueued {enqueued} to {queue_name}")

    qr.close()
    sr.close()

    if dry_run:
        logger.info(f"DRY-RUN: would enqueue {total} users total")
    else:
        logger.info(f"Done: {total} users enqueued total")


def main():
    parser = argparse.ArgumentParser(description="增量调度器")
    parser.add_argument("--platform", type=str, default="ig",
                        choices=["ig", "x", "all"],
                        help="平台 (default: ig)")
    parser.add_argument("--interval", type=int, default=21600,
                        help="增量间隔秒数 (default: 21600 = 6h)")
    parser.add_argument("--dry-run", action="store_true",
                        help="只显示不实际入队")
    parser.add_argument("--limit", type=int, default=0,
                        help="最多入队用户数 (0=不限制)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    run(args.platform, args.interval, args.dry_run, args.limit)


if __name__ == "__main__":
    main()
