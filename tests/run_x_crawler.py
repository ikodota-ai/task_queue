"""
X Crawler 真实测试执行脚本

用法:
    source venv/bin/activate
    python tests/run_x_crawler.py                    # 只入队检查
    python tests/run_x_crawler.py --start            # 入队 + 信息 + 提示启动命令
"""
import subprocess
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pymysql
import redis as r_module
from task_queue_robust import TaskQueue
from config import cfg


TARGET_USER = "phinyanech"

print("=" * 60)
print(f"X Crawler 测试: {TARGET_USER}")
print("=" * 60)

# 1. 检查 auth_token
print("\n[1/5] 检查 X_AUTH_TOKEN ...")
token = cfg.get("x_auth_token") or ""
if not token:
    print("  ERROR: .env 中未设置 X_AUTH_TOKEN!")
    print("  请编辑 .env，添加: X_AUTH_TOKEN=你的token")
    print("  获取方式: 浏览器登录 x.com → F12 → Application → Cookies → auth_token")
    sys.exit(1)
print(f"  OK (token: ...{token[-4:]})")

# 2. 检查 star_id
print("\n[2/5] 查找 star_id ...")
db = pymysql.connect(
    host=cfg["mysql_host"], port=cfg["mysql_port"],
    user=cfg["mysql_user"], password=cfg["mysql_password"],
    database=cfg["mysql_db"], charset="utf8mb4",
)
cur = db.cursor()
cur.execute(
    "SELECT id, name FROM la_star_info "
    "WHERE JSON_UNQUOTE(JSON_EXTRACT(original, '$.twitter')) = %s",
    (TARGET_USER,)
)
row = cur.fetchone()
db.close()
if row:
    star_id, name = row
    print(f"  OK: star_id={star_id}, name={name}")
else:
    star_id = None
    print(f"  WARNING: {TARGET_USER} 不在 la_star_info 中，DB 写入会被跳过")

# 3. 检查旧 processed 数据
print(f"\n[3/5] 检查旧 processed 数据 ...")
r = r_module.Redis(
    host=cfg["redis_host"], port=cfg["redis_port"],
    password=cfg["redis_password"], db=cfg["redis_db"],
    decode_responses=True,
)
processed_key = f"twitter:{TARGET_USER}:processed"
cnt = r.scard(processed_key)
if cnt:
    print(f"  {processed_key}: {cnt} 条已处理")
else:
    print(f"  {processed_key}: (空)")

# 4. 清理旧队列 + 入队
print(f"\n[4/5] 入队全量抓取任务 ...")
tq = TaskQueue()
tq.redis = tq.redis.from_url(
    f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
    if cfg["queue_redis_password"]
    else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
    decode_responses=True,
)
for k in tq.redis.keys("*crawl:x*"):
    tq.redis.delete(k)
for k in tq.redis.keys("processing_data:*"):
    tq.redis.delete(k)

tid = tq.enqueue("crawl:x:full", "x_full_crawl", TARGET_USER)
print(f"  Task: {tid}")
print(f"  Queue crawl:x:full: {tq.queue_length('crawl:x:full')} tasks")

# 5. 启动命令
print(f"\n[5/5] 执行以下命令启动 X Crawler:")
print()
print(f"  cd {os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}")
print(f"  source venv/bin/activate")
print(f"  python -u x_crawler.py --mode full")
print()
print("=" * 60)
print("启动后观察 Chrome 窗口:")
print("  1. auth_token 登录是否成功")
print("  2. 是否滚动 media 列表页")
print("  3. 多图推文是否点击弹出 gallery 并翻页")
print("  4. 图片 URL 是否加上了 ?format=jpg&name=orig")
print("=" * 60)
