"""
test_task_queue.py — 任务队列生命周期测试

覆盖：enqueue → dequeue → ack / nack → retry → dead → recover
"""
import json
import time
import pytest
import redis

import sys
sys.path.insert(0, ".")

from task_queue_robust import (
    TaskQueue, Task, Worker, register_task, FUNC_REGISTRY,
    TASK_VISIBILITY_TIMEOUT,
)
from config import cfg


# 使用队列 Redis 的测试专用 db，不影响生产
TEST_DB = 15


@pytest.fixture
def tq():
    """测试用 TaskQueue（独立 db 15）"""
    r = redis.Redis(
        host=cfg["queue_redis_host"],
        port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"],
        db=TEST_DB,
        decode_responses=True,
    )
    r.flushdb()
    t = TaskQueue(r)
    yield t
    r.flushdb()


@pytest.fixture(autouse=True)
def cleanup_registry():
    """每个测试后清理函数注册表"""
    yield
    FUNC_REGISTRY.clear()


# -----------------------------------------------------------
# 1. 发布 → 消费 → 成功 (ack)
# -----------------------------------------------------------
def test_enqueue_dequeue_ack(tq):
    @register_task("echo")
    def echo(msg):
        return msg.upper()

    tid = tq.enqueue("test:q", "echo", "hello")
    assert tid

    task = tq.dequeue("test:q", timeout=1)
    assert task is not None
    assert task.func_name == "echo"
    assert tuple(task.args) == ("hello",)  # JSON 反序列化后是 list

    result = task.execute()
    assert result == "HELLO"

    tq.ack(task)
    # ack 后 processing 集合应清空
    assert tq.processing_count("test:q") == 0


# -----------------------------------------------------------
# 2. 失败 → nack → retry 队列
# -----------------------------------------------------------
def test_nack_goes_to_retry(tq):
    @register_task("fail_once")
    def fail_once(msg):
        raise RuntimeError("boom")

    tid = tq.enqueue("test:q", "fail_once", "test")
    task = tq.dequeue("test:q", timeout=1)

    try:
        task.execute()
    except RuntimeError:
        tq.nack(task, "boom")

    # 应在 retry 队列（Sorted Set）
    retry_entries = tq.redis.zrange("retry:test:q", 0, -1)
    assert len(retry_entries) == 1

    retry_task = Task.from_dict(json.loads(retry_entries[0]))
    assert retry_task.retry_count == 1

    # 主队列应为空
    assert tq.queue_length("test:q") == 0


# -----------------------------------------------------------
# 3. retry 到期 → 自动回主队列 + 重新消费
# -----------------------------------------------------------
def test_retry_requeue_and_retry(tq):
    exec_count = []

    @register_task("retry_test")
    def retry_test(msg):
        exec_count.append(1)
        if len(exec_count) < 2:
            raise RuntimeError("fail")
        return "ok"

    tq.enqueue("test:q", "retry_test", "x")

    # 第一次：失败 → nack → retry
    task = tq.dequeue("test:q")
    try:
        task.execute()
    except RuntimeError:
        tq.nack(task, "fail")

    assert len(exec_count) == 1

    # 模拟 retry 到期：手动把 score 设为过去
    retry_data = tq.redis.zrange("retry:test:q", 0, -1, withscores=True)
    task_json, _ = retry_data[0]
    tq.redis.zadd("retry:test:q", {task_json: 0})

    # dequeue 应把到期 retry 移回主队列并消费
    task2 = tq.dequeue("test:q")
    assert task2 is not None
    assert task2.retry_count == 1

    result = task2.execute()
    assert result == "ok"
    assert len(exec_count) == 2
    tq.ack(task2)


# -----------------------------------------------------------
# 4. 超过 MAX_RETRIES → 死信队列
# -----------------------------------------------------------
def test_max_retries_to_dead(tq):
    @register_task("always_fail")
    def always_fail(msg):
        raise RuntimeError("permanent")

    tq.enqueue("test:q", "always_fail", "x")
    task = tq.dequeue("test:q")

    # 手动设 retry_count 到上限
    task.retry_count = 3
    try:
        task.execute()
    except RuntimeError:
        tq.nack(task, "permanent")

    # 应在死信队列
    dead = tq.redis.lrange("dead:test:q", 0, -1)
    assert len(dead) == 1
    dead_task = Task.from_dict(json.loads(dead[0]))
    assert dead_task.retry_count == 3

    # retry 队列应为空
    assert tq.redis.zcard("retry:test:q") == 0


# -----------------------------------------------------------
# 5. 死信恢复 (retry_dead)
# -----------------------------------------------------------
def test_retry_dead(tq):
    @register_task("ok")
    def ok(msg):
        return "ok"

    # 直接塞一个死信
    task = Task("ok", ("x",), {}, "test:q", task_id="dead-1")
    tq.redis.rpush("dead:test:q", json.dumps(task.to_dict()))

    assert tq.dead_count("test:q") == 1

    # 恢复
    tq.retry_dead("test:q")
    assert tq.dead_count("test:q") == 0
    assert tq.queue_length("test:q") == 1

    # 能正常消费
    task2 = tq.dequeue("test:q")
    assert task2.task_id == "dead-1"
    assert task2.execute() == "ok"
    tq.ack(task2)


# -----------------------------------------------------------
# 6. Worker 心跳 + 崩溃恢复
# -----------------------------------------------------------
def test_recover_timeout_tasks(tq):
    @register_task("slow")
    def slow(msg):
        time.sleep(60)
        return "too slow"

    tq.enqueue("test:q", "slow", "x")
    task = tq.dequeue("test:q")

    # 模拟 Worker 崩溃：processing 记录已过期
    tq.redis.hset(
        "processing:test:q",
        task.task_id,
        str(time.time() - TASK_VISIBILITY_TIMEOUT - 1),
    )
    # 保存 processing_data 供恢复
    tq.redis.setex(
        f"processing_data:{task.task_id}",
        999,
        json.dumps(task.to_dict()),
    )

    # dequeue 应恢复超时任务回主队列
    task2 = tq.dequeue("test:q")
    assert task2 is not None
    assert task2.task_id == task.task_id


# -----------------------------------------------------------
# 7. 批量入队
# -----------------------------------------------------------
def test_batch_enqueue(tq):
    tasks = [("echo_test", ("a",), {}), ("echo_test", ("b",), {})]

    @register_task("echo_test")
    def echo_test(msg):
        return msg

    ids = tq.enqueue_batch("test:q", tasks)
    assert len(ids) == 2
    assert tq.queue_length("test:q") == 2

    a = tq.dequeue("test:q")
    b = tq.dequeue("test:q")
    assert a.execute() == "a"
    assert b.execute() == "b"


# -----------------------------------------------------------
# 8. Worker 实际启动/停止（不依赖真实任务）
# -----------------------------------------------------------
def test_worker_start_stop(tq):
    w = Worker(tq, ["test:w"], worker_id="tester")
    assert w.running is False

    # 心跳注册
    tq.worker_heartbeat("tester")
    workers = tq.get_active_workers()
    assert "tester" in workers


# -----------------------------------------------------------
# 9. 队列状态查看
# -----------------------------------------------------------
def test_queue_status(tq):
    tq.enqueue("q1", "echo_test", "1")
    tq.enqueue("q1", "echo_test", "2")
    tq.enqueue("q2", "echo_test", "3")

    @register_task("echo_test")
    def echo_test(msg):
        return msg

    queues = tq.list_queues()
    assert set(queues) == {"q1", "q2"}

    assert tq.queue_length("q1") == 2
    assert tq.queue_length("q2") == 1
    assert tq.queue_length("nonexistent") == 0
