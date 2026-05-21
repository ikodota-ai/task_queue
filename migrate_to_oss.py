"""
本地图片批量迁移到 OSS（多线程）

用法:
    source venv/bin/activate
    python migrate_to_oss.py                        # 迁移 SUB_DOWNLOAD_DIR 下所有图片
    python migrate_to_oss.py --dry-run              # 先看有多少文件
    python migrate_to_oss.py --threads 10           # 10 线程并行
    python migrate_to_oss.py --prefix ig/           # 只迁移 ig/ 目录
"""
import os
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, ".")
from storage import _get_backend, get_url

backend = _get_backend()
if backend.__class__.__name__ == "LocalBackend":
    print("ERROR: STORAGE_BACKEND 是 local，请先改为 aliyun")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="只统计不迁移")
parser.add_argument("--threads", type=int, default=5, help="并发数 (默认 5)")
parser.add_argument("--prefix", default="", help="只迁移指定前缀目录")
parser.add_argument("--keep", action="store_true", help="保留本地文件 (默认删除以释放磁盘)")
parser.add_argument("--move-to", default="", help="迁完后移动到指定目录而非删除")
args = parser.parse_args()

# 准备迁移目录
_move_dir = None
if args.move_to:
    _move_dir = args.move_to
    os.makedirs(_move_dir, exist_ok=True)

base = os.getenv("STORAGE_LOCAL_DIR", "/home/www/uploads")
print(f"本地目录: {base}")
print(f"存储后端: {backend.__class__.__name__}")
print(f"迁移前缀: {args.prefix or '(全部)'}")
print(f"并发数: {args.threads}")
print()

# 收集所有文件
files = []
for root, dirs, filenames in os.walk(base):
    for f in filenames:
        full = os.path.join(root, f)
        rel = os.path.relpath(full, base)
        if not args.prefix or rel.startswith(args.prefix):
            files.append((full, rel))

total_size = sum(os.path.getsize(f) for f, _ in files)
print(f"共 {len(files)} 个文件, {total_size / 1024**3:.1f} GB")

if args.dry_run:
    print("(dry-run, 未迁移)")
    sys.exit(0)

# 迁移
uploaded = 0
failed = 0
total = len(files)
start = time.time()

def upload(args_tuple):
    full_path, rel_path = args_tuple
    try:
        with open(full_path, "rb") as f:
            backend.put(rel_path, f.read())
        # 上传成功 → 清理本地文件
        if not args.keep:
            if _move_dir:
                dest = os.path.join(_move_dir, rel_path)
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                os.rename(full_path, dest)
            else:
                os.remove(full_path)
        return (rel_path, True, None)
    except Exception as e:
        return (rel_path, False, str(e))

with ThreadPoolExecutor(max_workers=args.threads) as pool:
    futures = {pool.submit(upload, f): f for f in files}
    for future in as_completed(futures):
        rel, ok, err = future.result()
        if ok:
            uploaded += 1
        else:
            failed += 1
            if failed <= 10:
                print(f"  FAIL: {rel} - {err}")
        if (uploaded + failed) % 100 == 0:
            elapsed = time.time() - start
            rate = (uploaded + failed) / elapsed if elapsed > 0 else 0
            eta = (total - uploaded - failed) / rate if rate > 0 else 0
            print(f"  {uploaded + failed}/{total} ({uploaded} ok, {failed} fail) "
                  f"{rate:.1f} files/s, 剩余 {eta:.0f}s")

elapsed = time.time() - start
print(f"\n完成: {uploaded} ok, {failed} fail, 耗时 {elapsed:.0f}s")
