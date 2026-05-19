"""
补全所有缺失的图片：查出 processed 集合中有但 DB 中没有的帖子，
从 processed 中移除，然后重入增量队列。

用法: python fix_missing_images.py
"""
import sys
sys.path.insert(0, ".")

import pymysql
import redis
from task_queue_robust import TaskQueue
from config import cfg

r = redis.Redis(
    host=cfg["redis_host"], port=cfg["redis_port"],
    password=cfg["redis_password"], db=cfg["redis_db"],
    decode_responses=True,
)
db = pymysql.connect(
    host=cfg["mysql_host"], port=cfg["mysql_port"],
    user=cfg["mysql_user"], password=cfg["mysql_password"],
    database=cfg["mysql_db"], charset="utf8mb4",
)

# 只查 la_couple 里引用的明星
cur = db.cursor()
cur.execute("""
    SELECT DISTINCT si.id, si.x FROM la_star_info si
    WHERE si.x != '' AND si.id IN (
        SELECT JSON_EXTRACT(star, '$[0]') FROM la_couple
        UNION SELECT JSON_EXTRACT(star, '$[1]') FROM la_couple
    )
""")
users = [(r[0], r[1]) for r in cur.fetchall()]
print(f"检查 {len(users)} 个用户 (来自 la_couple)...\n")

cur = db.cursor()
total_missing = 0
needs_fix = []

# 一次查出所有涉及明星的 batch（避免逐用户全表扫描）
target_stars = set(sid for sid, _ in users)
from collections import defaultdict
db_batches_by_star = defaultdict(set)
cur.execute("SELECT star_id, batch FROM la_star_instagram WHERE star_id IN (%s)" %
    ",".join(str(s) for s in target_stars))
for star_id, batch in cur.fetchall():
    if batch:
        pid = batch.strip("/").split("/")[-1]
        db_batches_by_star[star_id].add(pid)

for star_id, uid in users:
    if not r.exists(f"instagram:{uid}:processed"):
        continue

    db_set = db_batches_by_star[star_id]
    pids = list(r.sscan_iter(f"instagram:{uid}:processed"))
    missing = [p for p in pids if p not in db_set]

    if missing:
        for pid in missing:
            r.srem(f"instagram:{uid}:processed", pid)
        total_missing += len(missing)
        needs_fix.append(uid)
        print(f"  {uid} (star_id={star_id}): {len(missing)} 缺失帖已清除")

db.close()

print(f"\n总计: {total_missing} 帖缺失, {len(needs_fix)} 用户需补抓")

if needs_fix:
    tq = TaskQueue()
    tq.redis = tq.redis.from_url(
        f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
        if cfg["queue_redis_password"]
        else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
        decode_responses=True,
    )
    for uid in needs_fix:
        tq.enqueue("crawl:ig:incr", "ig_incremental_crawl", uid)
    print(f"已入队 {len(needs_fix)} 个增量任务")
