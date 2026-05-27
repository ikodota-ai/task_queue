"""
task_queue_robust.py
加固版分布式任务队列 - 支持任务确认、重试退避、批量入队、Worker心跳
"""

import json
import uuid
import time
import logging
import traceback
import signal
import sys
import os
from typing import Callable, Dict, Any, Optional, List, Union
import redis

# ------------------------------------------------------------
# 配置
# ------------------------------------------------------------
# 队列 Redis（Broker）
QUEUE_REDIS_HOST = os.getenv("QUEUE_REDIS_HOST", "localhost")
QUEUE_REDIS_PORT = int(os.getenv("QUEUE_REDIS_PORT", 6379))
QUEUE_REDIS_PASSWORD = os.getenv("QUEUE_REDIS_PASSWORD", "")
QUEUE_REDIS_DB = int(os.getenv("QUEUE_REDIS_DB", 0))

# 任务超时（秒），超过此时间未完成则认为Worker崩溃，重新入队
TASK_VISIBILITY_TIMEOUT = 60 * 10   # 10分钟，可根据任务调整
# 最大重试次数
MAX_RETRIES = 3
# 重试延迟基础秒数（指数退避：delay = base * 2^(retry_count-1)）
RETRY_DELAY_BASE = 60

# Worker 心跳间隔（秒）
WORKER_HEARTBEAT_INTERVAL = 30
# Worker 心跳超时（秒），超过此时间未收到心跳，认为Worker死亡
WORKER_HEARTBEAT_TIMEOUT = 90

# 每个Worker最大任务处理数（超过后自动退出，让守护进程重启；0=不限制）
MAX_TASKS_PER_WORKER = int(os.getenv("MAX_TASKS_PER_WORKER", 100))

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("TaskQueue")

# ------------------------------------------------------------
# 函数注册表
# ------------------------------------------------------------
FUNC_REGISTRY: Dict[str, Callable] = {}

# 当前正在执行的任务（供长任务续期心跳使用）
_current_task: Optional["Task"] = None

def register_task(func_name: str = None):
    def decorator(func: Callable):
        name = func_name or func.__name__
        FUNC_REGISTRY[name] = func
        return func
    return decorator

# ------------------------------------------------------------
# 任务类（增加重试计数）
# ------------------------------------------------------------
class Task:
    def __init__(self, func_name: str, args: tuple, kwargs: dict, queue_name: str,
                 task_id: str = None, retry_count: int = 0):
        self.task_id = task_id or str(uuid.uuid4())
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs
        self.queue_name = queue_name
        self.retry_count = retry_count
        self.enqueued_at = time.time()

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "func_name": self.func_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "queue_name": self.queue_name,
            "retry_count": self.retry_count,
            "enqueued_at": self.enqueued_at
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        task = cls(
            func_name=data["func_name"],
            args=data["args"],
            kwargs=data["kwargs"],
            queue_name=data["queue_name"],
            task_id=data["task_id"],
            retry_count=data.get("retry_count", 0)
        )
        task.enqueued_at = data.get("enqueued_at", time.time())
        return task

    def execute(self):
        if self.func_name not in FUNC_REGISTRY:
            raise NameError(f"Function '{self.func_name}' not registered")
        return FUNC_REGISTRY[self.func_name](*self.args, **self.kwargs)

# ------------------------------------------------------------
# 队列管理器（加固版）
# ------------------------------------------------------------
class TaskQueue:
    def __init__(self, redis_client=None):
        self.redis = redis_client or redis.Redis(
            host=QUEUE_REDIS_HOST, port=QUEUE_REDIS_PORT, password=QUEUE_REDIS_PASSWORD, db=QUEUE_REDIS_DB,
            decode_responses=True   # 自动解码为字符串
        )
        # Key 规则
        self.queue_key = lambda q: f"queue:{q}"                 # 待处理队列 (List)
        self.processing_key = lambda q: f"processing:{q}"       # 处理中集合 (Hash: task_id -> expiry_timestamp)
        self.retry_key = lambda q: f"retry:{q}"                 # 延迟重试队列 (Sorted Set, score=重试时间戳)
        self.dead_key = lambda q: f"dead:{q}"                   # 死信队列 (List or Set)
        self.failed_key = lambda q: f"failed:{q}"               # 兼容旧版失败集合，暂时保留但不推荐
        self.task_meta_key = lambda q, tid: f"task_meta:{q}:{tid}"  # 任务元数据 (Hash)

    def enqueue(self, queue_name: str, func: Union[str, Callable], *args, **kwargs) -> str:
        """添加任务到队列"""
        return self.enqueue_batch(queue_name, [(func, args, kwargs)])[0]

    def enqueue_batch(self, queue_name: str, tasks: List[tuple]) -> List[str]:
        """
        批量添加任务
        tasks: [(func, args, kwargs), ...]
        """
        task_ids = []
        pipeline = self.redis.pipeline()
        for func, args, kwargs in tasks:
            func_name = func if isinstance(func, str) else func.__name__
            if func_name not in FUNC_REGISTRY and callable(func):
                register_task(func_name)(func)
            task = Task(func_name, args, kwargs, queue_name)
            task_json = json.dumps(task.to_dict())
            pipeline.rpush(self.queue_key(queue_name), task_json)
            # 存储元数据
            pipeline.hset(self.task_meta_key(queue_name, task.task_id), mapping={
                "func": func_name,
                "args": str(args),
                "kwargs": str(kwargs),
                "queue": queue_name,
                "status": "pending",
                "retry_count": 0,
                "enqueued_at": task.enqueued_at
            })
            pipeline.expire(self.task_meta_key(queue_name, task.task_id), 86400 * 7)
            task_ids.append(task.task_id)
        pipeline.execute()
        return task_ids

    def dequeue(self, queue_name: str, timeout: int = 0) -> Optional[Task]:
        """
        从队列取出任务（使用 processing 集合防止丢失）
        返回 None 表示超时或无任务
        """
        # 1. 先检查延迟重试队列，将到期的任务移回主队列
        self._requeue_due_retry(queue_name)
        # 2. 恢复超时的 processing 任务（Worker崩溃）
        self._recover_timeout_tasks(queue_name)
        # 3. 从主队列 pop 一个任务
        result = self.redis.blpop(self.queue_key(queue_name), timeout=timeout)
        if result is None:
            return None
        _, task_json = result
        task = Task.from_dict(json.loads(task_json))
        # 任务可能从旧队列迁移而来，用当前队列名覆盖
        task.queue_name = queue_name
        # 4. 将任务移入 processing 集合，记录超时时间
        now = time.time()
        self.redis.hset(self.processing_key(queue_name), task.task_id, str(now + TASK_VISIBILITY_TIMEOUT))
        # 保存完整任务 JSON 供 recovery 使用（worker 可能在 _process_task 之前崩溃）
        self.redis.setex(f"processing_data:{task.task_id}", TASK_VISIBILITY_TIMEOUT + 60, json.dumps(task.to_dict()))
        # 更新元数据状态
        self.redis.hset(self.task_meta_key(task.queue_name, task.task_id), "status", "processing")
        return task

    def _requeue_due_retry(self, queue_name: str):
        """将延迟重试队列中到期的任务移回主队列"""
        now = time.time()
        # 从 Sorted Set 中取出 score <= now 的所有任务
        tasks = self.redis.zrangebyscore(self.retry_key(queue_name), 0, now)
        if tasks:
            pipeline = self.redis.pipeline()
            for task_json in tasks:
                pipeline.rpush(self.queue_key(queue_name), task_json)
                pipeline.zrem(self.retry_key(queue_name), task_json)
            pipeline.execute()

    def _recover_timeout_tasks(self, queue_name: str):
        """恢复超时的 processing 任务（Worker崩溃）"""
        now = time.time()
        processing_dict = self.redis.hgetall(self.processing_key(queue_name))
        timeout_task_ids = [tid for tid, expiry in processing_dict.items() if float(expiry) < now]
        if timeout_task_ids:
            pipeline = self.redis.pipeline()
            for tid in timeout_task_ids:
                # 获取任务 JSON（存储在哪？）
                # 简化：从 task_meta 重建任务，但需要完整参数。更好的做法是将任务 JSON 也存入 processing 的 value。
                # 这里为了简洁，我们额外维护一个 processing:data:{task_id} 存储完整任务 JSON
                task_json = self.redis.get(f"processing_data:{tid}")
                if task_json:
                    pipeline.rpush(self.queue_key(queue_name), task_json)
                    pipeline.delete(f"processing_data:{tid}")
                pipeline.hdel(self.processing_key(queue_name), tid)
                # 更新元数据状态为 pending
                pipeline.hset(self.task_meta_key(queue_name, tid), "status", "pending")
            pipeline.execute()
            logger.warning(f"Recovered {len(timeout_task_ids)} timeout tasks from queue '{queue_name}'")

    def ack(self, task: Task):
        """任务成功完成，从 processing 集合中移除"""
        self.redis.hdel(self.processing_key(task.queue_name), task.task_id)
        self.redis.delete(f"processing_data:{task.task_id}")
        self.redis.hset(self.task_meta_key(task.queue_name, task.task_id), mapping={"status": "done", "finished_at": time.time()})

    def nack(self, task: Task, error: str):
        """
        任务失败，根据重试次数决定进入延迟重试队列或死信队列
        """
        if task.retry_count < MAX_RETRIES:
            # 指数退避延迟
            delay = RETRY_DELAY_BASE * (2 ** task.retry_count)
            retry_at = time.time() + delay
            task.retry_count += 1
            task_json = json.dumps(task.to_dict())
            # 放入延迟重试队列（Sorted Set）
            self.redis.zadd(self.retry_key(task.queue_name), {task_json: retry_at})
            status = f"retry_{task.retry_count}"
            logger.warning(f"Task {task.task_id} failed (retry {task.retry_count}/{MAX_RETRIES}), scheduled at {retry_at}")
        else:
            # 超过重试次数，移入死信队列
            task_json = json.dumps(task.to_dict())
            self.redis.rpush(self.dead_key(task.queue_name), task_json)
            status = "dead"
            logger.error(f"Task {task.task_id} moved to dead letter queue after {MAX_RETRIES} retries")
        # 清理 processing 状态
        self.redis.hdel(self.processing_key(task.queue_name), task.task_id)
        self.redis.delete(f"processing_data:{task.task_id}")
        self.redis.hset(self.task_meta_key(task.queue_name, task.task_id), mapping={"status": status, "error": error[:500]})

    _QUEUE_FUNC_MAP = {
        "crawl:ig:full": "ig_full_crawl",
        "crawl:ig:incr": "ig_incremental_crawl",
        "crawl:ig:max1000": "ig_max1000_crawl",
        "crawl:x:full": "x_full_crawl",
        "crawl:x:incr": "x_incremental_crawl",
        "crawl:x:max1000": "x_max1000_crawl",
        "dl:ig": "sub_download_image",
        "dl:x": "sub_download_image",
    }

    def move_task(self, from_queue: str, to_queue: str):
        """将 from_queue 中所有任务移到 to_queue，自动修正 func_name"""
        import json as _json
        new_func = self._QUEUE_FUNC_MAP.get(to_queue)
        moved = 0
        # 主队列
        while True:
            task_json = self.redis.rpop(self.queue_key(from_queue))
            if not task_json:
                break
            t = _json.loads(task_json)
            t["queue_name"] = to_queue
            t["retry_count"] = 0
            if new_func:
                t["func_name"] = new_func
            self.redis.rpush(self.queue_key(to_queue), _json.dumps(t))
            moved += 1
        # retry 队列
        for task_json in self.redis.zrange(self.retry_key(from_queue), 0, -1):
            t = _json.loads(task_json)
            t["queue_name"] = to_queue
            t["retry_count"] = 0
            if new_func:
                t["func_name"] = new_func
            self.redis.zrem(self.retry_key(from_queue), task_json)
            self.redis.rpush(self.queue_key(to_queue), _json.dumps(t))
            moved += 1
        return moved

    def retry_dead(self, queue_name: str, task_id: str = None):
        """手动将死信队列中的任务重新入队"""
        dead_key = self.dead_key(queue_name)
        if task_id is None:
            # 重试全部
            dead_tasks = self.redis.lrange(dead_key, 0, -1)
            for task_json in dead_tasks:
                task_dict = json.loads(task_json)
                task_dict["retry_count"] = 0
                self.redis.rpush(self.queue_key(queue_name), json.dumps(task_dict))
                self.redis.hset(self.task_meta_key(queue_name, task_dict["task_id"]), "status", "pending")
            self.redis.delete(dead_key)
        else:
            # 重试单个
            dead_tasks = self.redis.lrange(dead_key, 0, -1)
            for task_json in dead_tasks:
                if json.loads(task_json)["task_id"] == task_id:
                    task_dict = json.loads(task_json)
                    task_dict["retry_count"] = 0
                    self.redis.lrem(dead_key, 0, task_json)
                    self.redis.rpush(self.queue_key(queue_name), json.dumps(task_dict))
                    self.redis.hset(self.task_meta_key(queue_name, task_id), "status", "pending")
                    break

    # 监控方法
    def queue_length(self, queue_name: str) -> int:
        return self.redis.llen(self.queue_key(queue_name))

    def processing_count(self, queue_name: str) -> int:
        return self.redis.hlen(self.processing_key(queue_name))

    def dead_count(self, queue_name: str) -> int:
        return self.redis.llen(self.dead_key(queue_name))

    def list_queues(self) -> List[str]:
        keys = self.redis.keys("queue:*")
        return [k.split(":", 1)[1] for k in keys]

    def worker_heartbeat(self, worker_id: str):
        """Worker 心跳，用于健康检查（含机器名、当前任务）"""
        import socket
        host = socket.gethostname().split(".")[0]
        task_info = ""
        elapsed = ""
        tid = ""
        if _current_task:
            qname = _current_task.queue_name
            # 抓取任务：队列:用户；下载任务：plat:star_id:user
            if "dl:" in qname:
                a = _current_task.args
                plat = str(a[3]) if len(a) > 3 else "?"
                usr = str(a[4]) if len(a) > 4 else "?"
                import re
                m = re.search(r'/image/(\d+)/', str(a[1]) if len(a) > 1 else "")
                task_info = f"{plat}:{m.group(1) if m else '?'}:{usr}"
            else:
                task_info = f"{qname}:{str(_current_task.args[0]) if _current_task.args else '?'}"
            elapsed = str(int(time.time() - _current_task.enqueued_at))
            tid = _current_task.task_id[:8]
        else:
            qname = ""
        self.redis.setex(f"worker:heartbeat:{worker_id}", WORKER_HEARTBEAT_TIMEOUT,
                         f"{host}|{time.time()}|{task_info}|{elapsed}|{qname}|{tid}")

    def get_active_workers(self) -> List[str]:
        """返回活跃 Worker ID 列表"""
        keys = self.redis.keys("worker:heartbeat:*")
        return [k.split(":", 2)[2] for k in keys]

# ------------------------------------------------------------
# Worker（加固版）
# ------------------------------------------------------------
class Worker:
    def __init__(self, task_queue: TaskQueue, queue_names: List[str], worker_id: str = None):
        self.task_queue = task_queue
        self.queue_names = queue_names
        if not worker_id:
            worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        # 加随机后缀确保同机器多实例不覆盖
        self.worker_id = f"{worker_id}-{uuid.uuid4().hex[:4]}"
        self.running = False
        self.tasks_processed = 0
        self.logger = logging.getLogger(f"Worker-{self.worker_id}")

    def start(self):
        self.running = True
        self.logger.info(f"Worker {self.worker_id} started, listening to queues: {self.queue_names}")
        import threading

        def heartbeat_loop():
            while self.running:
                self.task_queue.worker_heartbeat(self.worker_id)
                time.sleep(WORKER_HEARTBEAT_INTERVAL)
        heart_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heart_thread.start()

        # 键盘监听（Windows Ctrl+C 不可靠时按任意键退出）
        def key_listener():
            try:
                import msvcrt  # Windows
                while self.running:
                    if msvcrt.kbhit():
                        ch = msvcrt.getch()
                        if ch in (b'\x03', b'\x1a'):  # Ctrl+C / Ctrl+Z
                            self.logger.info("Keyboard interrupt, stopping...")
                            self.running = False
                            break
                    time.sleep(0.5)
            except ImportError:
                pass  # Linux — signals 够用
        threading.Thread(target=key_listener, daemon=True).start()

        while self.running:
            for qname in self.queue_names:
                task = self.task_queue.dequeue(qname, timeout=1)
                if task:
                    self._process_task(task)
                    self.tasks_processed += 1
                    # 如果达到最大任务数，主动退出（让守护进程重启，释放 Chrome 资源）
                    if self.tasks_processed >= MAX_TASKS_PER_WORKER:
                        self.logger.info(f"Reached max tasks {MAX_TASKS_PER_WORKER}, exiting for cleanup")
                        self.running = False
                        break
                    break  # 处理完一个任务后重新扫描
        self.logger.info(f"Worker {self.worker_id} stopped")

    def _process_task(self, task: Task):
        global _current_task
        _current_task = task
        self.logger.info(f"Processing task {task.task_id} from queue '{task.queue_name}'")
        try:
            result = task.execute()
            self.task_queue.ack(task)
            self.task_queue.redis.hset(self.task_queue.task_meta_key(task.queue_name, task.task_id), "result", str(result)[:1000])
            self.logger.info(f"Task {task.task_id} succeeded")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            self.logger.error(f"Task {task.task_id} failed: {error_msg}")
            self.task_queue.nack(task, error_msg)
        finally:
            _current_task = None

    def stop(self):
        self.running = False

# ------------------------------------------------------------
# 监控器（增强版）
# ------------------------------------------------------------
class Monitor:
    def __init__(self, task_queue: TaskQueue):
        self.tq = task_queue

    def status(self):
        queues = self.tq.list_queues()
        print("\n=== Task Queue Status ===")
        for q in queues:
            pending = self.tq.queue_length(q)
            processing = self.tq.processing_count(q)
            dead = self.tq.dead_count(q)
            print(f"Queue '{q}': pending={pending}, processing={processing}, dead={dead}")
        workers = self.tq.get_active_workers()
        print(f"Active workers: {workers}")
        print("==========================\n")

# ------------------------------------------------------------
# 示例任务（带资源清理）
# ------------------------------------------------------------
@register_task()
def full_crawl_task(user_id: str, platform: str):
    driver = None
    try:
        from selenium import webdriver
        import sys
        if sys.platform == 'win32':
            driver_path = "C:/chromedriver.exe"
        elif sys.platform == 'darwin':
            driver_path = "/usr/local/bin/chromedriver"
        else:
            driver_path = "/usr/bin/chromedriver"
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        driver = webdriver.Chrome(executable_path=driver_path, options=options)
        # 模拟抓取
        time.sleep(2)
        urls = [f"https://{platform}.com/{user_id}/img_{i}.jpg" for i in range(5)]
        tq = TaskQueue()
        tq.enqueue_batch("download", [("download_image", (url, user_id, platform), {}) for url in urls])
        # 标记已抓取
        redis_client = tq.redis
        redis_client.setex(f"user:fetched:{platform}:{user_id}", 86400*30, "1")
        return len(urls)
    finally:
        if driver:
            driver.quit()

@register_task()
def download_image(url: str, user_id: str, platform: str):
    import requests
    save_dir = f"/data/images/{platform}/{user_id}"
    os.makedirs(save_dir, exist_ok=True)
    filename = url.split('/')[-1] or "image.jpg"
    filepath = os.path.join(save_dir, filename)
    resp = requests.get(url, stream=True, timeout=30)
    if resp.status_code == 200:
        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        return filepath
    else:
        raise Exception(f"HTTP {resp.status_code}")

# ------------------------------------------------------------
# 命令行入口（与之前类似，可复用）
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["worker", "monitor", "enqueue", "retry_dead"])
    parser.add_argument("--queues", nargs="+", help="Queue names for worker")
    parser.add_argument("--queue", help="Queue name for enqueue/retry")
    parser.add_argument("--func", help="Function name")
    parser.add_argument("--args", nargs="*", help="Arguments")
    args = parser.parse_args()
    tq = TaskQueue()
    if args.command == "worker":
        if not args.queues:
            print("Need --queues")
            sys.exit(1)
        worker = Worker(tq, args.queues)
        def shutdown(sig, frame):
            worker.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
        worker.start()
    elif args.command == "monitor":
        Monitor(tq).status()
    elif args.command == "enqueue":
        if not args.queue or not args.func:
            print("Need --queue and --func")
            sys.exit(1)
        tq.enqueue(args.queue, args.func, *args.args)
        print("Enqueued")
    elif args.command == "retry_dead":
        if not args.queue:
            print("Need --queue")
            sys.exit(1)
        tq.retry_dead(args.queue)
        print(f"Retried dead tasks from {args.queue}")