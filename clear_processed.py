"""
清理用户已处理帖，用于重新抓取

用法:
  python clear_processed.py _pundao                    # 清空全部
  python clear_processed.py _pundao POST1 POST2 ...   # 只删指定帖
"""
import sys
sys.path.insert(0, ".")
import redis
from config import cfg

r = redis.Redis(
    host=cfg["redis_host"], port=cfg["redis_port"],
    password=cfg["redis_password"], db=cfg["redis_db"],
    decode_responses=True,
)

if len(sys.argv) < 2:
    print("用法: python clear_processed.py <user_id> [post_id ...]")
    sys.exit(1)

uid = sys.argv[1]
key = f"instagram:{uid}:processed"
state_key = f"instagram:{uid}:state"

if len(sys.argv) == 2:
    # 清空全部
    cnt = r.scard(key)
    r.delete(key)
    r.delete(state_key)
    print(f"已清空 {uid}: {cnt} posts, state 也删了")
else:
    # 只删指定的
    pids = sys.argv[2:]
    for pid in pids:
        ok = r.srem(key, pid)
        print(f"  {pid}: {'done' if ok else 'not found'}")
    print(f"剩余: {r.scard(key)} posts")
