"""
从 la_couple 取 star → 查 twitter 用户名 → 入队 crawl:x:timeline

用法:
  python couple_timeline_enqueue.py                # 预览 (dry-run)
  python couple_timeline_enqueue.py --apply        # 实际入队
  python couple_timeline_enqueue.py --max-new 3    # 每人最多抓3条新帖
  python couple_timeline_enqueue.py --cutoff 86400 # 只看24h内

同时支持额外的媒体账号（通过 --extra-users 指定）
"""

import argparse
import json
import time
import uuid

import pymysql
import redis

from config import cfg


def main():
    parser = argparse.ArgumentParser(description="couple star timeline 入队")
    parser.add_argument("--apply", action="store_true", help="实际入队（默认仅预览）")
    parser.add_argument("--max-new", type=int, default=1, help="每人最多抓几帖 (default: 1)")
    parser.add_argument("--cutoff", type=int, default=604800, help="时间截止秒数 (default: 604800 = 7天)")
    parser.add_argument("--extra-users", type=str, default=None,
                        help="额外媒体账号，逗号分隔，如 'user1,user2'")
    args = parser.parse_args()

    db = pymysql.connect(
        host=cfg["mysql_host"], port=cfg["mysql_port"],
        user=cfg["mysql_user"], password=cfg["mysql_password"],
        database=cfg["mysql_db"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5, read_timeout=30,
    )
    cur = db.cursor()

    # 1. 收集所有 couple star_ids
    cur.execute("SELECT star FROM la_couple")
    star_ids = set()
    for r in cur.fetchall():
        for sid in json.loads(r["star"]):
            star_ids.add(sid)

    # 2. 查对应的 X 用户名
    if star_ids:
        placeholders = ",".join(["%s"] * len(star_ids))
        cur.execute(
            f"SELECT id, twitter FROM la_star_info "
            f"WHERE id IN ({placeholders}) AND twitter IS NOT NULL AND twitter != ''",
            list(star_ids),
        )
        users = [(r["twitter"], r["id"]) for r in cur.fetchall()]
    else:
        users = []

    # 3. 额外媒体账号
    extra_users = []
    if args.extra_users:
        extra_users = [u.strip() for u in args.extra_users.split(",") if u.strip()]

    db.close()

    print(f"Couple stars with X: {len(users)}")
    if extra_users:
        print(f"Extra media users: {len(extra_users)}")

    # 去重（couple stars 和 extra_users 可能有重叠）
    all_users = list(set([u[0] for u in users] + extra_users))
    print(f"Unique users: {len(all_users)}")

    if not all_users:
        print("No users to enqueue.")
        return

    # 检查 blocked
    sr = redis.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=cfg["redis_db"],
        decode_responses=True, socket_timeout=5,
    )
    pipe = sr.pipeline()
    for u in all_users:
        pipe.hget(f"twitter:{u}:state", "blocked")
    results = pipe.execute()

    blocked = 0
    available = []
    for i, u in enumerate(all_users):
        if results[i] == "1":
            blocked += 1
        else:
            available.append(u)

    print(f"Blocked: {blocked}")
    print(f"Available: {len(available)}")

    if blocked:
        print(f"  Blocked users: {', '.join(u for i, u in enumerate(all_users) if results[i] == '1')}")

    if not available:
        print("No available users to enqueue.")
        return

    print(f"\n  Queue: crawl:x:timeline")
    print(f"  max_new_posts: {args.max_new}")
    print(f"  cutoff: {args.cutoff}s ({args.cutoff // 86400}d)")
    print(f"  Mode: {'DRY RUN' if not args.apply else 'LIVE'}")
    print()

    if not args.apply:
        print("Preview (all available):")
        for u in available:
            print(f"  {u}")
        print(f"\n  Would enqueue {len(available)} users.")
        return

    # 入队
    qr = redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True, socket_timeout=5,
    )

    enqueued = 0
    for u in available:
        task_data = {
            "task_id": str(uuid.uuid4()),
            "func_name": "x_timeline_crawl",
            "args": [u, 0, args.max_new, args.cutoff],
            "kwargs": {},
            "queue_name": "crawl:x:timeline",
            "retry_count": 0,
            "enqueued_at": time.time(),
        }
        qr.rpush("queue:crawl:x:timeline", json.dumps(task_data))
        enqueued += 1

    print(f"\nDone. {enqueued} users enqueued to crawl:x:timeline")
    print(f"Queue length: {qr.llen('queue:crawl:x:timeline')}")


if __name__ == "__main__":
    main()
