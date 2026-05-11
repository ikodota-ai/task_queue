"""
test_ig_crawler.py — IG 爬虫测试（模拟 Selenium）

覆盖：全量抓取 / 增量抓取 / Cookie 恢复 / 游标续跑 / DB 插入
"""
import json
import time
from unittest.mock import patch, MagicMock

import pytest
import redis

import sys
sys.path.insert(0, ".")

import ig_crawler
from ig_crawler import (
    _is_processed, _mark_processed, _save_cursor, _get_cursor_url,
    _skey, _pkey, _save_cookies, _load_saved_cookies, _COOKIE_KEY,
    _lookup_star_id, _insert_star_instagram, _setup_chrome,
    _ensure_login, _navigate_to_user, _crawl_user,
)
from config import cfg


TEST_DB = 15


@pytest.fixture
def state_redis():
    """业务 Redis（独立 db，测试前清空）"""
    r = redis.Redis(
        host=cfg["redis_host"],
        port=cfg["redis_port"],
        password=cfg["redis_password"],
        db=cfg["redis_db"],
        decode_responses=True,
    )
    # 只清测试相关的 key
    for k in r.keys("instagram:test_user:*"):
        r.delete(k)
    r.delete("instagram:cookies")
    yield r
    for k in r.keys("instagram:test_user:*"):
        r.delete(k)
    r.delete("instagram:cookies")


# -----------------------------------------------------------
# Cookie 恢复
# -----------------------------------------------------------
def test_load_saved_cookies_no_data(state_redis):
    """无 cookie 数据时应返回 False"""
    state_redis.delete("instagram:cookies")
    mock_driver = MagicMock()
    result = _load_saved_cookies(mock_driver)
    assert result is False


def test_load_saved_cookies_success(state_redis):
    """有 cookie 数据且验证成功应返回 True"""
    cookies = [{"name": "sessionid", "value": "abc", "domain": ".instagram.com"}]
    state_redis.setex("instagram:cookies", 86400, json.dumps(cookies))

    mock_driver = MagicMock()
    mock_driver.current_url = "https://www.instagram.com/"
    # 第一次 get 后 url 已正常，不再含 login
    mock_driver.find_element.side_effect = Exception("no login btn")

    result = _load_saved_cookies(mock_driver)
    assert result is True


def test_save_cookies(state_redis):
    """保存 cookies 到 Redis"""
    mock_driver = MagicMock()
    mock_driver.get_cookies.return_value = [
        {"name": "sid", "value": "xyz", "domain": ".instagram.com"}
    ]
    _save_cookies(mock_driver)
    raw = state_redis.get("instagram:cookies")
    assert raw
    assert "xyz" in raw


# -----------------------------------------------------------
# 游标
# -----------------------------------------------------------
def test_save_and_get_cursor(state_redis):
    """保存并读取续跑游标"""
    state_redis.delete(_skey("test_user"))
    _save_cursor("test_user", "/test_user/p/POST456/")
    cursor = _get_cursor_url("test_user")
    assert cursor == "/test_user/p/POST456/"


def test_no_cursor_for_new_user(state_redis):
    """新用户无游标应返回 None"""
    state_redis.delete(_skey("test_user"))
    assert _get_cursor_url("new_user") is None


# -----------------------------------------------------------
# processed 集合
# -----------------------------------------------------------
def test_is_mark_processed(state_redis):
    """标记并检查已处理帖"""
    state_redis.delete(_pkey("test_user"))
    assert not _is_processed("test_user", "POST1")
    _mark_processed("test_user", "POST1")
    assert _is_processed("test_user", "POST1")


# -----------------------------------------------------------
# 登录 — 密码失败提示 cookies
# -----------------------------------------------------------
def test_ensure_login_raises_when_no_cookies_and_no_password(state_redis):
    """无 cookie 且未配置密码 → 抛异常提示用 cookies"""
    state_redis.delete("instagram:cookies")
    mock_driver = MagicMock()
    mock_driver.current_url = "https://www.instagram.com/accounts/login/"

    with patch("ig_crawler._load_saved_cookies", return_value=False), \
         patch("ig_crawler.cfg", {
             "ig_username": "", "ig_password": "",
             "redis_host": cfg["redis_host"],  # 保底，让 _state_redis 不炸
         }):
        with pytest.raises(RuntimeError, match="Cookie 已过期"):
            _ensure_login(mock_driver)


# -----------------------------------------------------------
# full crawl — 模拟 Selenium 页面
# -----------------------------------------------------------
def test_full_crawl_new_user(state_redis):
    """全量抓取新用户：应处理所有可见帖并保存游标"""
    state_redis.delete(_skey("test_user"))
    state_redis.delete(_pkey("test_user"))

    mock_driver = MagicMock()
    mock_driver.current_url = "https://www.instagram.com/test_user/"

    mock_img = MagicMock()
    mock_img.get_attribute.side_effect = lambda attr: {
        "src": "https://scontent.cdninstagram.com/img1.jpg",
        "alt": "Photo",
    }[attr]

    def make_link(href):
        link = MagicMock()
        link.get_attribute.side_effect = lambda attr: {"href": href}[attr]
        link.find_element.return_value = mock_img
        link.find_elements.return_value = []
        return link

    links = [make_link(f"https://www.instagram.com/p/POST{i}/") for i in range(1, 4)]
    mock_driver.find_elements.return_value = links
    mock_driver.execute_script.side_effect = lambda script: (
        None if "scrollTo" in script else 2000
    )

    with patch("ig_crawler._get_driver", return_value=mock_driver), \
         patch("ig_crawler._start_heartbeat"), \
         patch("ig_crawler._navigate_to_user", return_value=True), \
         patch("ig_crawler._lookup_star_id", return_value=999), \
         patch("ig_crawler._insert_star_instagram", return_value=1001), \
         patch("task_queue_robust.TaskQueue.enqueue", return_value="mock-tid"):
        processed = _crawl_user("test_user", incremental=False)

    assert processed >= 3
    assert _is_processed("test_user", "POST1")
    assert _is_processed("test_user", "POST2")
    assert _is_processed("test_user", "POST3")


# -----------------------------------------------------------
# incremental crawl — 碰已处理帖应停止
# -----------------------------------------------------------
def test_incremental_crawl_stops_at_processed_boundary(state_redis):
    """增量抓取：新帖正常处理，碰已处理帖后连续无新 → 停止"""
    state_redis.delete(_skey("test_user"))
    state_redis.delete(_pkey("test_user"))

    _mark_processed("test_user", "POST3")
    _mark_processed("test_user", "POST4")
    _mark_processed("test_user", "POST5")

    mock_driver = MagicMock()
    mock_driver.current_url = "https://www.instagram.com/test_user/"

    img_src = "https://scontent.cdninstagram.com/img.jpg"

    def make_link(href):
        link = MagicMock()
        link.get_attribute.side_effect = lambda attr: {"href": href}[attr]
        img = MagicMock()
        img.get_attribute.side_effect = lambda attr: {"src": img_src, "alt": "Photo"}[attr]
        link.find_element.return_value = img
        link.find_elements.return_value = []
        return link

    links = [make_link(f"https://www.instagram.com/p/POST{i}/") for i in range(1, 6)]
    mock_driver.find_elements.return_value = links
    mock_driver.execute_script.side_effect = lambda script: (
        None if "scrollTo" in script else 2000
    )

    with patch("ig_crawler._get_driver", return_value=mock_driver), \
         patch("ig_crawler._start_heartbeat"), \
         patch("ig_crawler._navigate_to_user", return_value=True), \
         patch("ig_crawler._lookup_star_id", return_value=None), \
         patch("ig_crawler._insert_star_instagram", return_value=1001), \
         patch("task_queue_robust.TaskQueue.enqueue", return_value="mock-tid"):
        processed = _crawl_user("test_user", incremental=True)

    # POST1, POST2 处理，POST3-5 跳过（已处理边界）
    assert processed == 2


# -----------------------------------------------------------
# 导航验证
# -----------------------------------------------------------
def test_navigate_to_user_success():
    """导航到目标用户主页成功"""
    mock_driver = MagicMock()
    mock_driver.current_url = "https://www.instagram.com/test_user/"

    result = _navigate_to_user(mock_driver, "test_user")
    assert result is True
    mock_driver.get.assert_called_with("https://www.instagram.com/test_user/")


def test_navigate_to_user_login_redirect():
    """导航被重定向到登录页应重试"""
    mock_driver = MagicMock()
    # 前两次返回 login，第三次成功
    mock_driver.current_url = "https://www.instagram.com/accounts/login/"

    with patch("ig_crawler._load_saved_cookies", return_value=True):
        result = _navigate_to_user(mock_driver, "test_user", retries=2)

    assert result is False  # 全部重试都失败
    assert mock_driver.get.call_count == 2


# -----------------------------------------------------------
# DB 操作
# -----------------------------------------------------------
def test_lookup_star_id_not_found():
    """找不到 star_id 返回 None"""
    with patch("ig_crawler._get_db") as mock_db:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_db.return_value.cursor.return_value = mock_cursor

        result = _lookup_star_id("nonexistent_user")
        assert result is None


def test_insert_star_instagram():
    """插入图片记录返回自增 ID"""
    with patch("ig_crawler._get_db") as mock_db:
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 12345
        mock_db.return_value.cursor.return_value = mock_cursor

        db_id = _insert_star_instagram(999, "image/999/abc.jpg", "/user/p/POST1/", "abcdef123456")
        assert db_id == 12345
