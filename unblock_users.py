"""
清除所有 blocked=1 的用户标记，使其能重新入队抓取。

用法:
  python unblock_users.py          # 预览 (dry-run)
  python unblock_users.py --apply  # 实际执行
"""

import argparse
import redis
from config import cfg


def main():
    parser = argparse.ArgumentParser(description="清除 blocked 标记")
    parser.add_argument("--apply", action="store_true", help="实际执行（默认仅预览）")
    parser.add_argument("--platform", type=str, default="all", choices=["ig", "x", "all"])
    args = parser.parse_args()

    sr = redis.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=cfg["redis_db"],
        decode_responses=True, socket_timeout=10,
    )

    platforms = []
    if args.platform in ("ig", "all"):
        platforms.append(("instagram", "IG"))
    if args.platform in ("x", "all"):
        platforms.append(("twitter", "X"))

    for pfx, label in platforms:
        cursor = 0
        blocked = []
        total = 0
        while True:
            cursor, keys = sr.scan(cursor, match=f"{pfx}:*:state", count=1000)
            total += len(keys)
            for k in keys:
                if sr.hget(k, "blocked") == "1":
                    blocked.append(k)
            if cursor == 0:
                break

        print(f"[{label}] state keys: {total}, blocked: {len(blocked)}")

        if not blocked:
            print("  No blocked users.\n")
            continue

        print(f"  Sample: {', '.join(k.split(':')[1] for k in blocked[:5])}")

        if args.apply:
            pipe = sr.pipeline()
            for k in blocked:
                pipe.hdel(k, "blocked", "blocked_reason", "blocked_at")
            pipe.execute()
            print(f"  ✅ Cleared {len(blocked)} blocked marks")
        else:
            print(f"  (dry-run, use --apply to execute)")

        print()


if __name__ == "__main__":
    main()
