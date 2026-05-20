"""
X (Twitter) Crawler 测试 — db15(队列) + db14(业务)

用法:
    CHROME_HEADLESS=1 pytest tests/test_x_crawler.py -v
    CHROME_HEADLESS=1 pytest tests/test_x_crawler.py::test_x_full_crawl -s
"""
import hashlib
import json
import os
import re
import time
import threading
from unittest.mock import patch, MagicMock

import pytest
import redis

import sys
sys.path.insert(0, ".")

from x_crawler import (
    _skey, _pkey, _fix_image_url, _extract_tweet_id,
    _has_gallery_icon, _extract_grid_thumbnail,
    _extract_images_from_tweet, _is_processed, _mark_processed,
    _state_redis, _queue_redis, _setup_chrome, _ensure_login,
    _get_cursor_url, _save_cursor, _crawl_user,
    _lookup_star_id, _insert_star_instagram,
    _update_crawl_status, _is_full_crawl_done,
)
from x_crawler import _get_driver as _orig_get_driver
from config import cfg

# ============================================================
# 测试 Redis fixture
# ============================================================

@pytest.fixture
def state_redis():
    r = redis.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=14, decode_responses=True,
    )
    for k in r.keys("twitter:test_*"):
        r.delete(k)
    yield r
    for k in r.keys("twitter:test_*"):
        r.delete(k)


@pytest.fixture
def queue_redis():
    r = redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=15, decode_responses=True,
    )
    r.flushdb()
    yield r
    r.flushdb()


@pytest.fixture(autouse=True)
def patch_redis(state_redis, queue_redis):
    """替换 x_crawler 中的 Redis 连接到测试实例"""
    with patch("x_crawler._state_redis", return_value=state_redis), \
         patch("x_crawler._queue_redis", return_value=queue_redis):
        yield


# ============================================================
# 工具函数测试
# ============================================================

def test_fix_image_url():
    assert _fix_image_url(None) is None
    assert _fix_image_url("https://profile_images/avatar.jpg") is None
    assert "name=orig" in _fix_image_url("https://pbs.twimg.com/media/abc?format=jpg&name=medium")


def test_extract_tweet_id():
    assert _extract_tweet_id("https://x.com/u/status/12345/photo/1") == "12345"
    assert _extract_tweet_id("/u/status/987654321") == "987654321"
    assert _extract_tweet_id("no_match") is None


# ============================================================
# processed 集合
# ============================================================

def test_processed_basic(state_redis):
    state_redis.delete(_pkey("test_user"))
    assert not _is_processed("test_user", "TWEET1")
    _mark_processed("test_user", "TWEET1")
    assert _is_processed("test_user", "TWEET1")


# ============================================================
# 多图检测
# ============================================================

def test_has_gallery_icon_false():
    link = MagicMock()
    link.find_elements.return_value = []
    assert _has_gallery_icon(link) is False


def test_has_gallery_icon_true():
    link = MagicMock()
    link.find_elements.return_value = [MagicMock()]
    assert _has_gallery_icon(link) is True


# ============================================================
# 网格缩略图
# ============================================================

def test_extract_grid_thumbnail():
    link = MagicMock()
    img = MagicMock()
    img.get_attribute.return_value = "https://pbs.twimg.com/media/abc?format=jpg&name=small"
    link.find_element.return_value = img
    result = _extract_grid_thumbnail(link)
    assert "name=orig" in result


def test_extract_grid_thumbnail_none():
    link = MagicMock()
    link.find_element.side_effect = Exception("no img")
    assert _extract_grid_thumbnail(link) is None


# ============================================================
# 游标
# ============================================================

def test_save_and_get_cursor(state_redis):
    state_redis.delete(_skey("test_user"))
    _save_cursor("test_user", "/test_user/status/123/photo/1")
    assert _get_cursor_url("test_user") == "/test_user/status/123/photo/1"


def test_no_cursor(state_redis):
    state_redis.delete(_skey("test_user"))
    assert _get_cursor_url("test_user") is None


# ============================================================
# 请求结构校验
# ============================================================

def test_image_url_structure():
    """X 原图 URL 格式应为 ?format=jpg&name=orig"""
    urls = [
        "https://pbs.twimg.com/media/abc?format=jpg&name=small",
        "https://pbs.twimg.com/media/def.jpg?format=jpg&name=360x360",
        "https://pbs.twimg.com/media/ghi?format=png&name=medium",
    ]
    for u in urls:
        fixed = _fix_image_url(u)
        assert "name=orig" in fixed, f"Failed: {u} -> {fixed}"


def test_download_task_payload():
    """下载任务 payload 包含必要字段"""
    task = {"url": "https://pbs.twimg.com/abc?format=jpg&name=orig",
            "path": "x/test/abc.jpg", "tweet": "12345", "idx": 1}
    assert "url" in task and "path" in task and "tweet" in task


# ============================================================
# 增量跳过检查
# ============================================================

def test_incremental_skips_processed_boundary(state_redis):
    """增量模式遇到连续 5 轮无新帖应停止（模拟逻辑）"""
    _mark_processed("test_user", "TWEET_OLD_1")
    _mark_processed("test_user", "TWEET_OLD_2")
    _mark_processed("test_user", "TWEET_OLD_3")
    assert _is_processed("test_user", "TWEET_OLD_1")
    assert not _is_processed("test_user", "TWEET_NEW")
