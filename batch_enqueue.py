"""
批量入队全量抓取任务 (IG / X)
用法:
  # 列出所有非泰国 IG 用户
  python batch_enqueue.py --platform ig --list-countries

  # 预览 (不入队)
  python batch_enqueue.py --platform ig --maxpage 30 --dry-run

  # 入队所有非泰国 IG 用户
  python batch_enqueue.py --platform ig --maxpage 30

  # 入队指定国家
  python batch_enqueue.py --platform ig --maxpage 30 --country 美国,日本

  # 入队指定用户
  python batch_enqueue.py --platform ig --maxpage 30 --user user1,user2

  # X 平台
  python batch_enqueue.py --platform x --maxpage 50 --country 美国
"""
import argparse
import json
import time
import uuid

from config import cfg
import pymysql
import redis

THAILAND_COUNTRY_ID = 5

PLATFORM_CONFIG = {
    "ig": {
        "db_field": "x",           # la_star_info 中 IG 用户名字段
        "queue": "crawl:ig:full",
        "func": "ig_full_crawl",
    },
    "x": {
        "db_field": "twitter",     # la_star_info 中 X 用户名字段
        "queue": "crawl:x:full",
        "func": "x_full_crawl",
    },
}


def _get_db():
    return pymysql.connect(
        host=cfg["mysql_host"], port=cfg["mysql_port"],
        user=cfg["mysql_user"], password=cfg["mysql_password"],
        database=cfg["mysql_db"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5, read_timeout=10,
    )


def _get_state_redis():
    return redis.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=cfg["redis_db"],
        decode_responses=True, socket_timeout=5,
    )


def _get_queue_redis():
    return redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True, socket_timeout=5,
    )


def list_countries(platform: str):
    """列出所有非泰国国家及其对应平台的用户数"""
    db = _get_db()
    cur = db.cursor()
    field = PLATFORM_CONFIG[platform]["db_field"]

    cur.execute("SELECT value, name FROM la_dict_data WHERE type_id=6")
    country_names = {r["value"]: r["name"] for r in cur.fetchall()}

    cur.execute(f"""
        SELECT country, COUNT(*) as cnt
        FROM la_star_info
        WHERE country != %s AND {field} IS NOT NULL AND {field} != ''
        GROUP BY country ORDER BY cnt DESC
    """, (THAILAND_COUNTRY_ID,))

    for r in cur.fetchall():
        cname = country_names.get(str(r["country"]), f"unknown({r['country']})")
        print(f"  {cname:8s}  {r['cnt']:4d}  (country_id={r['country']})")

    db.close()


def get_users(platform: str, country_filter=None, user_filter=None, skip_full_done=True):
    """获取待入队的用户列表，返回 [(user_id, country_id), ...]"""
    db = _get_db()
    cur = db.cursor()
    field = PLATFORM_CONFIG[platform]["db_field"]
    pfx = "instagram" if platform == "ig" else "twitter"

    if user_filter:
        # 指定用户，不去重
        users = [(u, None) for u in user_filter]
    elif country_filter:
        placeholders = ",".join(["%s"] * len(country_filter))
        cur.execute(f"""
            SELECT {field} as username, country
            FROM la_star_info
            WHERE country != %s
              AND {field} IS NOT NULL AND {field} != ''
              AND country IN ({placeholders})
            ORDER BY country, {field}
        """, [THAILAND_COUNTRY_ID] + list(country_filter))
        users = [(r["username"], r["country"]) for r in cur.fetchall()]
    else:
        cur.execute(f"""
            SELECT {field} as username, country
            FROM la_star_info
            WHERE country != %s AND {field} IS NOT NULL AND {field} != ''
            ORDER BY country, {field}
        """, (THAILAND_COUNTRY_ID,))
        users = [(r["username"], r["country"]) for r in cur.fetchall()]

    db.close()

    if not skip_full_done:
        return users

    sr = _get_state_redis()
    # Pipeline 批量查 full_done，避免逐条网络往返
    pipe = sr.pipeline()
    for uid, _ in users:
        pipe.hget(f"{pfx}:{uid}:state", "full_done")
    results = pipe.execute()

    result = []
    skipped = 0
    for i, (uid, cid) in enumerate(users):
        if results[i] == "1":
            skipped += 1
            continue
        result.append((uid, cid))

    if skipped:
        print(f"[skip] {skipped} users already have full_done=1")
    return result


def batch_enqueue(platform: str, users, maxpage: int, dry_run: bool = False):
    """批量入队"""
    if not users:
        print("No users to enqueue.")
        return

    conf = PLATFORM_CONFIG[platform]
    queue_name = conf["queue"]
    func_name = conf["func"]
    queue_key = f"queue:{queue_name}"

    print(f"\n  Platform : {platform.upper()}")
    print(f"  Queue    : {queue_name}")
    print(f"  Func     : {func_name}")
    print(f"  Maxpage  : {maxpage}")
    print(f"  Users    : {len(users)}")
    print(f"  Mode     : {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print(f"  Queue len: {_get_queue_redis().llen(queue_key)} (before)")

    if dry_run:
        print("\nPreview (first 30):")
        for uid, cid in users[:30]:
            print(f"  {uid}" + (f" (country={cid})" if cid else ""))
        if len(users) > 30:
            print(f"  ... and {len(users) - 30} more")
        return

    qr = _get_queue_redis()
    table = cfg["table_prefix"] + "crawl_tasks"

    # 一次性加载队列中已有的 user_id，避免每次 lrange O(n^2)
    existing = set()
    for item in qr.lrange(queue_key, 0, -1):
        try:
            d = json.loads(item)
            if d.get("args"):
                existing.add(str(d["args"][0]))
        except Exception:
            pass
    if existing:
        print(f"  Queue has {len(existing)} unique users, skipping duplicates")

    db = _get_db()
    cur = db.cursor()

    enqueued = 0
    for uid, _ in users:
        if uid in existing:
            continue
        try:
            cur.execute(
                f"INSERT INTO {table} (platform, task_type, user_id, status) "
                "VALUES (%s, 'full', %s, 'pending')",
                (platform, uid),
            )
            db_task_id = cur.lastrowid

            tid = str(uuid.uuid4())
            task_data = {
                "task_id": tid,
                "func_name": func_name,
                "args": [uid, db_task_id, maxpage],
                "kwargs": {},
                "queue_name": queue_name,
                "retry_count": 0,
                "enqueued_at": time.time(),
            }
            qr.rpush(queue_key, json.dumps(task_data))
            enqueued += 1

            if enqueued % 100 == 0:
                db.commit()
                print(f"  ... {enqueued}/{len(users)}")

        except Exception as e:
            print(f"  [ERROR] {uid}: {e}")
            db.rollback()
            continue

    db.commit()
    db.close()
    print(f"\nDone. {enqueued} users enqueued to {queue_name}")
    print(f"Queue length: {qr.llen(queue_key)} (after)")


def main():
    parser = argparse.ArgumentParser(description="批量入队全量抓取任务 (IG/X)")
    parser.add_argument("--platform", type=str, required=True, choices=["ig", "x"],
                        help="平台: ig 或 x")
    parser.add_argument("--maxpage", type=int, default=30,
                        help="最大抓取页数 (default: 30)")
    parser.add_argument("--dry-run", action="store_true",
                        help="只列出用户，不入队")
    parser.add_argument("--list-countries", action="store_true",
                        help="列出所有非泰国国家及用户数")
    parser.add_argument("--country", type=str, default=None,
                        help="只入队指定国家，逗号分隔，如 '美国,日本'")
    parser.add_argument("--user", type=str, default=None,
                        help="指定用户入队，逗号分隔，如 'user1,user2'")
    parser.add_argument("--no-skip-fd", action="store_true",
                        help="不跳过 full_done=1 的用户（默认跳过）")

    args = parser.parse_args()

    if args.list_countries:
        print(f"=== 非泰国国家 {args.platform.upper()} 用户分布 ===\n")
        list_countries(args.platform)
        return

    # 解析用户 / 国家
    if args.user:
        user_filter = [u.strip() for u in args.user.split(",") if u.strip()]
        country_filter = None
    elif args.country:
        user_filter = None
        db = _get_db()
        cur = db.cursor()
        names = [n.strip() for n in args.country.split(",")]
        placeholders = ",".join(["%s"] * len(names))
        cur.execute(
            f"SELECT value FROM la_dict_data WHERE type_id=6 AND name IN ({placeholders})",
            names,
        )
        country_filter = [r["value"] for r in cur.fetchall()]
        db.close()
        if not country_filter:
            print(f"ERROR: No matching countries for: {args.country}")
            print("Use --list-countries to see available names")
            return
        print(f"Matching country IDs: {country_filter}")
    else:
        user_filter = None
        country_filter = None

    skip_fd = not args.no_skip_fd

    users = get_users(
        args.platform,
        country_filter=country_filter,
        user_filter=user_filter,
        skip_full_done=skip_fd,
    )
    print(f"Target users: {len(users)}")

    batch_enqueue(args.platform, users, args.maxpage, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
