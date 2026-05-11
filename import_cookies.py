"""
import_cookies.py — 将 Instagram cookies JSON 导入业务 Redis

用法:
  python import_cookies.py < cookies.json
  python import_cookies.py /path/to/cookies.json
"""

import json
import sys
import redis as redis_module
from config import cfg


def import_cookies(cookies_data):
    r = redis_module.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=cfg["redis_db"],
        decode_responses=True,
    )
    # 统一格式：如果传入的是字符串，解析为列表
    if isinstance(cookies_data, str):
        cookies_data = json.loads(cookies_data)
    r.setex("instagram:cookies", 86400 * 30, json.dumps(cookies_data))
    print(f"✅ 已导入 {len(cookies_data)} 条 cookies，有效期 30 天")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            data = f.read()
    else:
        data = sys.stdin.read()
        print(cookies_data)
    import_cookies(data)
