"""
子任务 Worker — 运行在指定服务器上，处理图片下载和数据库写入。
注册两个函数到 FUNC_REGISTRY:
  - sub_download_image  -> 监听 dl:ig / dl:x 队列
  - sub_db_write        -> 监听 sub:dbwrite 队列
"""

import logging
import os
import random
import signal
import time
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
    """下载图片到本地，可选更新对应平台表状态

    Args:
        url: 图片 URL
        save_path: 相对路径（相对于 SUB_DOWNLOAD_DIR）
        db_id: 记录 ID，提供则下载后更新 status='Y'
        platform: 'ig' 或 'x'，用于确定存储路径
        user_id: Instagram 用户名（预留，日志用）

    Returns: 最终文件路径
    """
    # 使用统一存储后端（本地/阿里云/七牛/腾讯云）
    # OSS 直拉模式无需延迟——服务器只调 API，不下载
    from storage import upload_from_url, get_url
    file_url = upload_from_url(url, save_path)
    save_path = file_url  # 返回的是可访问 URL，后续 DB 更新用这个

    logger.info(f"Uploaded {url} -> {save_path}")

    # 更新 DB 状态（ig/x 共表 la_star_instagram，source 字段区分）
    if db_id:
        table = f"{cfg['table_prefix']}star_instagram"
        try:
            db = _get_db()
            cur = db.cursor()
            cur.execute(
                f"UPDATE `{table}` SET status = 'Y', verify_time = %s WHERE id = %s",
                (int(time.time()), db_id),
            )
            db.commit()
            logger.info(f"DB updated: {table} id={db_id} status=Y")
        except Exception as e:
            logger.error(f"DB update failed for {table} id={db_id}: {e}")

    return save_path


@register_task("sub_db_write")
def sub_db_write(table: str, data: dict, condition: dict = None) -> int:
    """写入数据到 MySQL

    Args:
        table: 表名 (不含前缀，如 'star_instagram')
        data: 要插入/更新的字段
        condition: WHERE 条件，存在时执行 UPDATE，否则 INSERT

    Returns: 受影响行数
    """
    full_table = f"{cfg['table_prefix']}{table}" if not table.startswith(cfg['table_prefix']) else table

    db = _get_db()
    cursor = db.cursor()

    if condition:
        # UPDATE
        set_clause = ", ".join(f"`{k}` = %s" for k in data)
        where_clause = " AND ".join(f"`{k}` = %s" for k in condition)
        sql = f"UPDATE `{full_table}` SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(condition.values())
        cursor.execute(sql, params)
    else:
        # INSERT
        cols = ", ".join(f"`{k}`" for k in data)
        placeholders = ", ".join("%s" for _ in data)
        sql = f"INSERT INTO `{full_table}` ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, list(data.values()))

    db.commit()
    affected = cursor.rowcount
    logger.info(f"DB write: {sql} -> {affected} rows")
    return affected


def _get_db() -> pymysql.Connection:
    """获取 MySQL 连接"""
    return pymysql.connect(
        host=cfg["mysql_host"],
        port=cfg["mysql_port"],
        user=cfg["mysql_user"],
        password=cfg["mysql_password"],
        database=cfg["mysql_db"],
        charset="utf8mb4",
    )


# -----------------------------------------------------------
# 启动器
# -----------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("ig", "x", "all"), default="all",
                        help="ig=仅 IG, x=仅 X, all=两者 (默认)")
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

    worker = Worker(tq, queue_names, worker_id=worker_id)

    def shutdown(sig, frame):
        logger.info("Shutting down sub-task worker...")
        worker.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(f"Sub-task worker starting (queues: {', '.join(queue_names)})")
    worker.start()


if __name__ == "__main__":
    main()
