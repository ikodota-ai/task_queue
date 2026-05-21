"""
子任务 Worker 多线程版 — 并行下载 + 上传

用法:
    python sub_task_worker_mt.py                     # 默认 5 线程，监听 dl:ig + dl:x
    python sub_task_worker_mt.py --mode x --threads 8 # 8 线程，仅 X
    python sub_task_worker_mt.py --threads 3          # 3 线程

环境变量:
    SUB_TASK_THREADS=5   # 线程数
"""
import logging
import os
import random
import signal
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pymysql
import redis as redis_module

import argparse
sys.path.insert(0, ".")
from storage import upload_from_url
from task_queue_robust import TaskQueue, Worker, register_task, FUNC_REGISTRY
from config import cfg

logger = logging.getLogger("SubMT")

_DEQUEUE_LOCK = Lock()
_db_connections = {}  # 线程本地 DB 连接缓存


def _get_db():
    import threading
    tid = threading.get_ident()
    if tid not in _db_connections:
        _db_connections[tid] = pymysql.connect(
            host=cfg["mysql_host"], port=cfg["mysql_port"],
            user=cfg["mysql_user"], password=cfg["mysql_password"],
            database=cfg["mysql_db"], charset="utf8mb4",
        )
    return _db_connections[tid]


def _process_one(task, tq):
    """单线程处理一个下载任务"""
    task_id = task.task_id[:8]
    args = task.args
    url = args[0]
    save_path = args[1] if len(args) > 1 else "unknown"
    db_id = args[2] if len(args) > 2 else None
    platform = args[3] if len(args) > 3 else None

    # 随机延迟
    time.sleep(random.uniform(0.3, 1.5))

    # 下载 + 上传到存储
    try:
        file_url = upload_from_url(url, save_path)
    except Exception as e:
        logger.error(f"[{task_id}] Upload failed: {e}")
        raise

    # 更新 DB
    if db_id and platform:
        table = f"{cfg['table_prefix']}star_instagram"
        try:
            db = _get_db()
            cur = db.cursor()
            cur.execute(
                f"UPDATE `{table}` SET status = 'Y', verify_time = %s WHERE id = %s",
                (int(time.time()), db_id),
            )
            db.commit()
        except Exception as e:
            logger.error(f"[{task_id}] DB update failed: {e}")

    return file_url


class MultiThreadWorker:
    def __init__(self, queue_names, num_threads=5, worker_id="sub-mt"):
        self.queue_names = queue_names
        self.num_threads = num_threads
        self.worker_id = worker_id
        self.running = False
        self.tq = TaskQueue()
        self.tq.redis = self.tq.redis.from_url(
            f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
            if cfg["queue_redis_password"]
            else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
            decode_responses=True,
        )

    def start(self):
        self.running = True
        logger.info(f"Multi-thread worker ({self.num_threads} threads) listening: {self.queue_names}")

        import threading
        def heartbeat():
            while self.running:
                self.tq.worker_heartbeat(self.worker_id)
                time.sleep(30)
        threading.Thread(target=heartbeat, daemon=True).start()

        with ThreadPoolExecutor(max_workers=self.num_threads) as pool:
            futures = []
            while self.running:
                # 补充任务到线程池
                while len(futures) < self.num_threads:
                    task = self._safe_dequeue()
                    if not task:
                        break
                    futures.append(pool.submit(_process_one_task, task, self))

                if not futures:
                    time.sleep(1)
                    continue

                # 等任意一个完成，清理掉，回到外层补充新任务
                done = as_completed(futures, timeout=1)
                for f in done:
                    try:
                        f.result()
                    except Exception:
                        pass
                    futures.remove(f)
                    break

        logger.info(f"Worker {self.worker_id} stopped")

    def _safe_dequeue(self):
        with _DEQUEUE_LOCK:
            for q in self.queue_names:
                task = self.tq.dequeue(q, timeout=0)
                if task:
                    return task
            return None

    def stop(self):
        self.running = False


def _process_one_task(task, worker):
    try:
        result = _process_one(task, worker.tq)
        worker.tq.ack(task)
        logger.debug(f"[{task.task_id[:8]}] OK -> {result}")
    except Exception as e:
        logger.error(f"[{task.task_id[:8]}] FAILED: {e}")
        worker.tq.nack(task, str(e))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("ig", "x", "all"), default="all")
    parser.add_argument("--threads", type=int, default=int(os.getenv("SUB_TASK_THREADS", 5)))
    args = parser.parse_args()

    if args.mode == "ig":
        queues = ["dl:ig"]
        wid = "sub-mt-ig"
    elif args.mode == "x":
        queues = ["dl:x"]
        wid = "sub-mt-x"
    else:
        queues = ["dl:ig", "dl:x"]
        wid = "sub-mt"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    worker = MultiThreadWorker(queues, args.threads, wid)

    def shutdown(sig, frame):
        worker.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(f"Sub-task MT worker starting ({args.threads} threads, queues: {queues})")
    worker.start()


if __name__ == "__main__":
    main()
