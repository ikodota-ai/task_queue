"""
Producer — 从 MySQL 读取待抓取任务，发布到 Redis 任务队列。

MySQL 表结构（需提前创建）:
    CREATE TABLE IF NOT EXISTS {prefix}crawl_tasks (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        platform VARCHAR(10) NOT NULL COMMENT 'ig / x',
        task_type VARCHAR(20) NOT NULL COMMENT 'full / incremental',
        user_id VARCHAR(255) NOT NULL,
        status ENUM('pending','queued','processing','done','failed') DEFAULT 'pending',
        error TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_status (status),
        INDEX idx_platform_type (platform, task_type)
    );
"""

import time
import logging
import sys
from typing import Optional

import pymysql

from config import cfg
from task_queue_robust import TaskQueue

logger = logging.getLogger("Producer")

# 平台 -> task_type -> Redis 队列名映射
QUEUE_MAP = {
    ("ig", "full"):        "crawl:ig:full",
    ("ig", "incremental"): "crawl:ig:incr",
    ("x", "full"):         "crawl:x:full",
    ("x", "incremental"):  "crawl:x:incr",
}


class Producer:
    def __init__(self):
        self.running = False
        self._db: Optional[pymysql.Connection] = None
        self._tq: Optional[TaskQueue] = None

    @property
    def _tasks_table(self):
        return f"{cfg['table_prefix']}crawl_tasks"

    def connect(self):
        """连接 MySQL 和 Redis（SSH 隧道需提前在 bash 中建好）"""
        self._db = pymysql.connect(
            host=cfg["mysql_host"],
            port=cfg["mysql_port"],
            user=cfg["mysql_user"],
            password=cfg["mysql_password"],
            database=cfg["mysql_db"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        logger.info(f"MySQL connected ({cfg['mysql_host']}:{cfg['mysql_port']})")

        self._tq = TaskQueue()
        self._tq.redis = self._tq.redis.from_url(
            f"redis://:{cfg['queue_redis_password']}@{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}"
            if cfg["queue_redis_password"]
            else f"redis://{cfg['queue_redis_host']}:{cfg['queue_redis_port']}/{cfg['queue_redis_db']}",
            decode_responses=True,
        )
        logger.info(f"Queue Redis connected ({cfg['queue_redis_host']}:{cfg['queue_redis_port']})")

    def close(self):
        if self._db:
            self._db.close()

    def poll_once(self) -> int:
        """查询 MySQL 中 status=pending 的任务并发布到 Redis，返回发布数"""
        if not self._db or not self._db.open:
            logger.warning("MySQL disconnected, reconnecting...")
            self.connect()

        try:
            cursor = self._db.cursor()
            cursor.execute(
                f"SELECT id, platform, task_type, user_id FROM {self._tasks_table} "
                "WHERE status = 'pending' ORDER BY id ASC LIMIT 100"
            )
            rows = cursor.fetchall()
        except pymysql.Error as e:
            logger.error(f"MySQL query failed: {e}")
            self._db.close()
            self.connect()
            return 0

        if not rows:
            return 0

        count = 0
        for row in rows:
            key = (row["platform"].lower(), row["task_type"].lower())
            queue_name = QUEUE_MAP.get(key)
            if queue_name is None:
                logger.warning(f"Unknown task type: platform={row['platform']}, task_type={row['task_type']}")
                self._mark_failed(row["id"], f"unknown task_type: {row['task_type']}")
                continue

            function_name = {
                ("ig", "full"):        "ig_full_crawl",
                ("ig", "incremental"): "ig_incremental_crawl",
                ("x", "full"):         "x_full_crawl",
                ("x", "incremental"):  "x_incremental_crawl",
            }.get(key, "unknown_task")

            try:
                self._tq.enqueue_unique(queue_name, function_name, row["user_id"], row["id"])
                self._mark_queued(row["id"])
                count += 1
                logger.info(f"Enqueued {queue_name} user_id={row['user_id']} (task_id={row['id']})")
            except Exception as e:
                logger.error(f"Failed to enqueue task {row['id']}: {e}")
                self._mark_failed(row["id"], str(e))

        return count

    def _mark_queued(self, task_id: int):
        cursor = self._db.cursor()
        cursor.execute(f"UPDATE {self._tasks_table} SET status = 'queued' WHERE id = %s", (task_id,))
        self._db.commit()

    def _mark_failed(self, task_id: int, error: str):
        cursor = self._db.cursor()
        cursor.execute(
            f"UPDATE {self._tasks_table} SET status = 'failed', error = %s WHERE id = %s",
            (error[:500], task_id),
        )
        self._db.commit()

    def run(self, interval: int = None):
        """持续轮询 MySQL"""
        self.running = True
        interval = interval or cfg["producer_poll_interval"]
        logger.info(f"Producer started, poll interval={interval}s")

        try:
            while self.running:
                count = self.poll_once()
                if count:
                    logger.info(f"Published {count} tasks")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            self.close()


def main():
    # 单实例锁：防止重复运行
    import os as _os
    _pidfile = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".producer.pid")
    if _os.path.exists(_pidfile):
        try:
            with open(_pidfile) as f:
                old_pid = int(f.read().strip())
            _os.kill(old_pid, 0)
            print(f"Producer is already running (PID {old_pid}). Exiting.")
            sys.exit(1)
        except (OSError, ValueError):
            _os.remove(_pidfile)
    with open(_pidfile, "w") as f:
        f.write(str(_os.getpid()))

    try:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        producer = Producer()
        producer.connect()
        producer.run()
    finally:
        _os.remove(_pidfile)


if __name__ == "__main__":
    main()
