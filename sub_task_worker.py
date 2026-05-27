"""
子任务 Worker — 处理图片下载和数据库写入，支持线程池并发。
注册两个函数到 FUNC_REGISTRY:
  - sub_download_image  -> 监听 dl:ig / dl:x 队列
  - sub_db_write        -> 监听 sub:dbwrite 队列
用法:
  python sub_task_worker.py --mode ig --threads 10
"""
import logging
import os
import random
import signal
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pymysql
import requests

from config import cfg
from task_queue_robust import register_task, TaskQueue, Worker

logger = logging.getLogger("SubTaskWorker")

# -----------------------------------------------------------
# 注册子任务函数
# -----------------------------------------------------------

@register_task("sub_download_image")
def sub_download_image(url: str, save_path: str, db_id: int = None, platform: str = None, user_id: str = None) -> str:
    """下载图片并上传到存储，可选更新 DB 状态"""
    from storage import upload_from_url

    file_url = upload_from_url(url, save_path)
    save_path = file_url

    if db_id:
        table = f"{cfg['table_prefix']}star_instagram"
        try:
            db = _get_thread_db()
            cur = db.cursor()
            cur.execute(
                f"UPDATE `{table}` SET status = 'Y', verify_time = %s WHERE id = %s",
                (int(time.time()), db_id),
            )
            db.commit()
        except Exception as e:
            logger.error(f"DB update failed for {table} id={db_id}: {e}")

    return save_path


@register_task("sub_db_write")
def sub_db_write(table: str, data: dict, condition: dict = None) -> int:
    """写入数据到 MySQL"""
    full_table = f"{cfg['table_prefix']}{table}" if not table.startswith(cfg['table_prefix']) else table

    db = _get_thread_db()
    cursor = db.cursor()

    if condition:
        set_clause = ", ".join(f"`{k}` = %s" for k in data)
        where_clause = " AND ".join(f"`{k}` = %s" for k in condition)
        sql = f"UPDATE `{full_table}` SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(condition.values())
        cursor.execute(sql, params)
    else:
        cols = ", ".join(f"`{k}`" for k in data)
        placeholders = ", ".join("%s" for _ in data)
        sql = f"INSERT INTO `{full_table}` ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, list(data.values()))

    db.commit()
    affected = cursor.rowcount
    return affected


# -----------------------------------------------------------
# 线程安全的 DB 连接（每个线程一个长连接）
# -----------------------------------------------------------

_thread_local = threading.local()


def _get_thread_db() -> pymysql.Connection:
    """获取当前线程的 MySQL 长连接"""
    conn = getattr(_thread_local, "db", None)
    if conn is None or not conn.open:
        conn = pymysql.connect(
            host=cfg["mysql_host"],
            port=cfg["mysql_port"],
            user=cfg["mysql_user"],
            password=cfg["mysql_password"],
            database=cfg["mysql_db"],
            charset="utf8mb4",
            autocommit=False,
        )
        _thread_local.db = conn
    return conn


# -----------------------------------------------------------
# 线程池 Worker
# -----------------------------------------------------------

class ThreadPoolWorker:
    """多线程从队列取任务并处理。每个线程独立 dequeue → process。"""

    def __init__(self, task_queue: TaskQueue, queue_names: list, worker_id: str = None,
                 num_threads: int = 5):
        self.task_queue = task_queue
        self.queue_names = queue_names
        self.worker_id = worker_id or "sub-worker"
        self.num_threads = num_threads
        self.running = False
        self.tasks_processed = 0
        self._lock = threading.Lock()

    def _process_loop(self, thread_id: int):
        """单个线程的主循环"""
        from task_queue_robust import FUNC_REGISTRY

        tname = f"{self.worker_id}-t{thread_id}"
        logger.info(f"Thread {tname} started")

        while self.running:
            try:
                task = None
                for qname in self.queue_names:
                    task = self.task_queue.dequeue(qname, timeout=1)
                    if task:
                        break

                if not task:
                    continue

                func_name = task["func_name"]
                args = task.get("args", [])
                kwargs = task.get("kwargs", {})

                func = FUNC_REGISTRY.get(func_name)
                if func:
                    func(*args, **kwargs)
                    with self._lock:
                        self.tasks_processed += 1
                else:
                    logger.warning(f"Unknown task function: {func_name}")

            except Exception as e:
                logger.error(f"Thread {tname} error: {e}")
                time.sleep(0.5)

        # 清理线程 DB 连接
        conn = getattr(_thread_local, "db", None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        logger.info(f"Thread {tname} stopped")

    def start(self):
        self.running = True
        logger.info(f"{self.worker_id} starting with {self.num_threads} threads, "
                    f"queues: {self.queue_names}")

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            for i in range(self.num_threads):
                futures.append(executor.submit(self._process_loop, i))

            try:
                for f in as_completed(futures):
                    f.result()
            except KeyboardInterrupt:
                pass

        logger.info(f"{self.worker_id} stopped, total processed: {self.tasks_processed}")

    def stop(self):
        self.running = False


# -----------------------------------------------------------
# 启动器
# -----------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("ig", "x", "all"), default="all",
                        help="ig=仅 IG, x=仅 X, all=两者 (默认)")
    parser.add_argument("--threads", type=int, default=5,
                        help="并发线程数 (默认 5)")
    opt_args = parser.parse_args()

    if opt_args.mode == "ig":
        queue_names = ["dl:ig"]
        worker_id = "sub-worker-ig"
    elif opt_args.mode == "x":
        queue_names = ["dl:x"]
        worker_id = "sub-worker-x"
    else:
        queue_names = ["dl:ig", "dl:x"]
        worker_id = "sub-worker"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    tq = TaskQueue()
    tq.redis = tq.redis.from_url(
        f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
        if cfg["queue_redis_password"]
        else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
        decode_responses=True,
    )

    worker = ThreadPoolWorker(tq, queue_names, worker_id=worker_id,
                              num_threads=opt_args.threads)

    def shutdown(sig, frame):
        logger.info("Shutting down...")
        worker.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    worker.start()


if __name__ == "__main__":
    main()
