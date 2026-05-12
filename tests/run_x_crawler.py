"""
X Crawler 测试：phinyanech 全量 → 增量

用法:
    source venv/bin/activate
    python tests/run_x_crawler.py
"""
import time
import sys
sys.path.insert(0, ".")

import pymysql
import redis as r_module
from task_queue_robust import TaskQueue
from config import cfg


TARGET_UID = "phinyanech"
PLATFORM = "x"

print("=" * 60)
print(f"X Crawler 全量+增量测试: {TARGET_UID}")
print("=" * 60)

# 1. 检查 auth_token
token = cfg.get("x_auth_token") or ""
if not token:
    print("ERROR: .env 中未设置 X_AUTH_TOKEN"); sys.exit(1)
print(f"[OK] X_AUTH_TOKEN: ...{token[-4:]}")

# 2. 检查 star_id
db = pymysql.connect(
    host=cfg["mysql_host"], port=cfg["mysql_port"],
    user=cfg["mysql_user"], password=cfg["mysql_password"],
    database=cfg["mysql_db"], charset="utf8mb4",
)
cur = db.cursor()
cur.execute("SELECT id, name FROM la_star_info WHERE twitter = %s", (TARGET_UID,))
row = cur.fetchone()
if row:
    print(f"[OK] star_id={row[0]} name={row[1]}")
else:
    print(f"[WARN] No star_id for {TARGET_UID}")

# 3. 插入全量任务到 MySQL
table = cfg["table_prefix"] + "crawl_tasks"
cur.execute(
    f"INSERT INTO {table} (platform, task_type, user_id, status) VALUES (%s, %s, %s, 'pending')",
    (PLATFORM, "full", TARGET_UID),
)
full_task_id = cur.lastrowid
db.commit()
print(f"[OK] Inserted full task id={full_task_id}")

# 4. 清理旧 Redis 队列
tq = TaskQueue()
tq.redis = tq.redis.from_url(
    f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
    if cfg["queue_redis_password"]
    else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
    decode_responses=True,
)
for k in tq.redis.keys("*crawl:x*"): tq.redis.delete(k)
for k in tq.redis.keys("processing_data:*"): tq.redis.delete(k)

# 5. 检查 producer 是否运行
import subprocess
procs = subprocess.run("ps aux | grep 'python.*producer' | grep -v grep", shell=True, capture_output=True, text=True)
if not procs.stdout.strip():
    print("[WARN] Producer 未运行，直接入队 Redis（跳过 MySQL 流转）")
    tid = tq.enqueue("crawl:x:full", "x_full_crawl", TARGET_UID, full_task_id)
    print(f"[OK] Enqueued: {tid}")
else:
    print(f"[OK] Producer running, it will pick up task {full_task_id} in < 30s")

# 6. 启动命令
print()
print("=" * 60)
print("执行以下命令启动 X Crawler:")
print(f"  cd /home/unis/dev/cc/task_queue")
print(f"  source venv/bin/activate")
print(f"  python -u x_crawler.py --mode full")
print()
print("监控 MySQL 状态:")
print(f"  SELECT * FROM {table} WHERE user_id='{TARGET_UID}' ORDER BY id DESC;")
print("=" * 60)

db.close()
