"""
新用户入队 — 扫描 la_star_info 中从未被爬过的用户，加入全量抓取队列。

用法:
  python new_user_enqueue.py --platform ig --dry-run       # IG 预览
  python new_user_enqueue.py --platform x                  # X 新用户入队
  python new_user_enqueue.py --platform all --maxpage 50   # 全平台, 50 页
  python new_user_enqueue.py --platform ig --limit 10      # 最多入队 10 个

与 batch_enqueue.py 的区别：
  - batch_enqueue 面向已有 Redis state 的用户（full_done 等检查）
  - new_user_enqueue 只面向 la_star_info 中有账号但 Redis 中无任何记录的新用户
"""

import argparse
import json
import logging
import time
import uuid

import pymysql
import redis

from config import cfg

logger = logging.getLogger("NewUserEnqueue")

PLATFORM_CONFIG = {
    "ig": {
        "db_field": "x",
        "queue": "crawl:ig:full",
        "func": "ig_full_crawl",
    },
    "x": {
        "db_field": "twitter",
        "queue": "crawl:x:full",
        "func": "x_full_crawl",
    },
}


def _get_db():
    return pymysql.connect(
        host=cfg["mysql_host"],
        port=cfg["mysql_port"],
        user=cfg["mysql_user"],
        password=cfg["mysql_password"],
        database=cfg["mysql_db"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
        read_timeout=30,
        autocommit=False,
    )


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


def _existing_queue_users(qr, queue_name):
    """返回已在 full 队列(pending)中的 user_id 集合。"""
    existing = set()
    for raw in qr.lrange(f"queue:{queue_name}", 0, -1):
        try:
            d = json.loads(raw)
            if d.get("args"):
                existing.add(str(d["args"][0]))
        except Exception:
            pass
    return existing


def run(platform: str, maxpage: int, dry_run: bool = False, limit: int = 0):
    db = _get_db()
    cur = db.cursor()
    sr = _get_state_redis()
    qr = _get_queue_redis()

    platforms = ["ig", "x"] if platform == "all" else [platform]
    total = 0

    for plat in platforms:
        conf = PLATFORM_CONFIG[plat]
        field = conf["db_field"]
        queue_name = conf["queue"]
        func_name = conf["func"]

        # 1. 查 la_star_info 中有账号的所有用户（不限国家）
        cur.execute(f"""
            SELECT id as star_id, {field} as username
            FROM la_star_info
            WHERE {field} IS NOT NULL AND {field} != ''
            ORDER BY id
        """)
        rows = cur.fetchall()
        logger.info(f"[{plat}] la_star_info 有 {field} 字段的用户: {len(rows)}")

        if not rows:
            continue

        # 2. Pipeline 批量查 Redis state 是否存在
        pipe = sr.pipeline()
        for r in rows:
            pipe.exists(f"instagram:{r['username']}:state" if plat == "ig" else f"twitter:{r['username']}:state")
        results = pipe.execute()

        # 3. 去重：已在 full 队列中的
        queue_existing = _existing_queue_users(qr, queue_name)

        # 4. 筛选无 Redis 记录的新用户，直接入队
        enqueued = 0

        for i, row in enumerate(rows):
            username = row["username"]
            has_state = results[i]

            if has_state:
                continue  # 已有状态记录，不是新用户
            if username in queue_existing:
                continue  # 已在队列中

            if limit and total + enqueued >= limit:
                break

            if dry_run:
                logger.info(f"  [DRY-RUN] {username:30s}  (star_id={row['star_id']})")
                enqueued += 1
                continue

            task_data = {
                "task_id": str(uuid.uuid4()),
                "func_name": func_name,
                "args": [username, 0, maxpage],   # db_task_id=0
                "kwargs": {},
                "queue_name": queue_name,
                "retry_count": 0,
                "enqueued_at": time.time(),
            }
            qr.rpush(f"queue:{queue_name}", json.dumps(task_data))
            enqueued += 1

            if enqueued % 100 == 0:
                logger.info(f"  ... {enqueued}")

        total += enqueued
        logger.info(f"[{plat}] enqueued {enqueued} new users to {queue_name} "
                     f"(maxpage={maxpage}, has_state={sum(results)}, in_queue={len(queue_existing)})")

    db.close()
    qr.close()
    sr.close()

    if dry_run:
        logger.info(f"DRY-RUN: would enqueue {total} new users total")
    else:
        logger.info(f"Done: {total} new users enqueued total")


def main():
    parser = argparse.ArgumentParser(description="新用户全量入队")
    parser.add_argument("--platform", type=str, default="ig",
                        choices=["ig", "x", "all"],
                        help="平台 (default: ig)")
    parser.add_argument("--maxpage", type=int, default=10,
                        help="首次全量最大翻页数 (default: 10)")
    parser.add_argument("--dry-run", action="store_true",
                        help="只显示不实际入队")
    parser.add_argument("--limit", type=int, default=0,
                        help="最多入队用户数 (0=不限制)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    run(args.platform, args.maxpage, args.dry_run, args.limit)


if __name__ == "__main__":
    main()
