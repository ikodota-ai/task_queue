"""
配置加载 — 从 .env 读取并注入 os.environ，已有的 task_queue_robust.py 的
os.getenv 调用会自动生效。
"""

import os
from pathlib import Path
from dotenv import load_dotenv


def load_config(env_file: str = None):
    if env_file is None:
        env_file = Path(__file__).parent / ".env"
    env_file = Path(env_file)
    if env_file.exists():
        load_dotenv(env_file, override=True)
        print(f"[config] Loaded {env_file}")
    else:
        print(f"[config] {env_file} not found, using system env")

    return {
        # 队列 Redis（Broker）
        "queue_redis_host": os.getenv("QUEUE_REDIS_HOST", "localhost"),
        "queue_redis_port": int(os.getenv("QUEUE_REDIS_PORT", 6379)),
        "queue_redis_password": os.getenv("QUEUE_REDIS_PASSWORD", ""),
        "queue_redis_db": int(os.getenv("QUEUE_REDIS_DB", 0)),

        # 业务 Redis（抓取器缓存/去重）
        "redis_host": os.getenv("REDIS_HOST", "localhost"),
        "redis_port": int(os.getenv("REDIS_PORT", 6379)),
        "redis_password": os.getenv("REDIS_PASSWORD", ""),
        "redis_db": int(os.getenv("REDIS_DB", 1)),

        # MySQL
        "mysql_host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "mysql_port": int(os.getenv("MYSQL_PORT", 3306)),
        "mysql_user": os.getenv("MYSQL_USER", "root"),
        "mysql_password": os.getenv("MYSQL_PASSWORD", ""),
        "mysql_db": os.getenv("MYSQL_DB", "xigscraper"),
        "table_prefix": os.getenv("TABLE_PREFIX", ""),

        # Queue
        "task_visibility_timeout": int(os.getenv("TASK_VISIBILITY_TIMEOUT", 600)),
        "max_retries": int(os.getenv("MAX_RETRIES", 3)),
        "retry_delay_base": int(os.getenv("RETRY_DELAY_BASE", 60)),

        # Worker
        "worker_heartbeat_interval": int(os.getenv("WORKER_HEARTBEAT_INTERVAL", 30)),
        "worker_heartbeat_timeout": int(os.getenv("WORKER_HEARTBEAT_TIMEOUT", 90)),
        "max_tasks_per_worker": int(os.getenv("MAX_TASKS_PER_WORKER", 100)),

        # Producer
        "producer_poll_interval": int(os.getenv("PRODUCER_POLL_INTERVAL", 30)),

        # Instagram
        "ig_chrome_path": os.getenv("IG_CHROME_PATH", ""),
        "ig_chromedriver_path": os.getenv("IG_CHROMEDRIVER_PATH", ""),
        "ig_username": os.getenv("IG_USERNAME", ""),
        "ig_password": os.getenv("IG_PASSWORD", ""),

        # X (Twitter)
        "x_auth_token": os.getenv("X_AUTH_TOKEN", ""),
    }


# 模块导入时自动加载
cfg = load_config()
