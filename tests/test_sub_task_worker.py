"""
test_sub_task_worker.py — 子任务 Worker 测试

覆盖：下载 / DB 更新 / 平台路径 / 错误处理
"""
import os
import time
from unittest.mock import patch, MagicMock, mock_open

import pytest

import sys
sys.path.insert(0, ".")

import sub_task_worker
from sub_task_worker import sub_download_image, sub_db_write
from config import cfg


# -----------------------------------------------------------
# 下载 + DB 更新
# -----------------------------------------------------------
@patch("sub_task_worker.requests.get")
@patch("sub_task_worker._get_db")
@patch("builtins.open", new_callable=mock_open)
def test_download_with_db_update(mock_file, mock_get_db, mock_requests):
    """下载图片后应更新 DB 状态为 Y"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"fake-image-data"]
    mock_requests.return_value = mock_response

    mock_cursor = MagicMock()
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mock_get_db.return_value = mock_db

    result = sub_download_image(
        "https://scontent.cdninstagram.com/img.jpg",
        "image/999/abc.jpg",
        db_id=42,
        platform="ig",
    )

    assert "image/999/abc.jpg" in result
    mock_file.assert_called_once()
    mock_requests.assert_called_once()

    # 验证 DB UPDATE
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE" in sql
    assert "la_star_instagram" in sql
    assert "status = 'Y'" in sql or "status = %s" in sql
    mock_db.commit.assert_called_once()


# -----------------------------------------------------------
# 无 db_id 时跳过 DB 更新
# -----------------------------------------------------------
@patch("sub_task_worker.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_download_without_db_id(mock_file, mock_requests):
    """不传 db_id 时只下载不更新 DB"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"data"]
    mock_requests.return_value = mock_response

    result = sub_download_image("https://example.com/img.jpg", "old/path.jpg")

    assert "old/path.jpg" in result


# -----------------------------------------------------------
# 下载失败不更新 DB
# -----------------------------------------------------------
@patch("sub_task_worker.requests.get")
def test_download_http_error_no_db_update(mock_requests):
    """HTTP 错误时不更新 DB，直接抛出"""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = Exception("404")
    mock_requests.return_value = mock_response

    with pytest.raises(Exception):
        sub_download_image(
            "https://example.com/bad.jpg", "path.jpg", db_id=42, platform="ig"
        )


# -----------------------------------------------------------
# 平台路径差异
# -----------------------------------------------------------
@patch("sub_task_worker.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_platform_ig_path(mock_file, mock_requests):
    """IG 平台图片应存到 ig/ 子目录"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"x"]
    mock_requests.return_value = mock_response

    result = sub_download_image(
        "https://scontent.cdninstagram.com/img.jpg",
        "image/999/abc.jpg",
        platform="ig",
    )
    assert os.path.join("ig", "image", "999", "abc.jpg").replace("\\", "/") in result.replace("\\", "/")


@patch("sub_task_worker.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_platform_x_path(mock_file, mock_requests):
    """X 平台图片应存到 x/ 子目录"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"x"]
    mock_requests.return_value = mock_response

    result = sub_download_image(
        "https://pbs.twimg.com/media/img.jpg",
        "image/999/abc.jpg",
        platform="x",
    )
    assert os.path.join("x", "image", "999", "abc.jpg").replace("\\", "/") in result.replace("\\", "/")


@patch("sub_task_worker.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_no_platform_no_subdir(mock_file, mock_requests):
    """无 platform 参数时不分子目录"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"x"]
    mock_requests.return_value = mock_response

    result = sub_download_image(
        "https://example.com/img.jpg",
        "plain/path.jpg",
    )
    assert "plain/path.jpg" in result


# -----------------------------------------------------------
# DB 延迟验证
# -----------------------------------------------------------
@patch("sub_task_worker.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_download_has_random_delay(mock_file, mock_requests):
    """下载前应有随机延迟"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [b"x"]
    mock_requests.return_value = mock_response

    with patch("sub_task_worker.time.sleep") as mock_sleep:
        sub_download_image("https://example.com/img.jpg", "path.jpg")
        # 至少调用了一次 sleep
        assert mock_sleep.call_count >= 1


# -----------------------------------------------------------
# sub_db_write
# -----------------------------------------------------------
@patch("sub_task_worker._get_db")
def test_db_write_insert(mock_get_db):
    """INSERT 操作"""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mock_get_db.return_value = mock_db

    result = sub_db_write("test_table", {"name": "foo", "val": 123})
    assert result == 1
    mock_cursor.execute.assert_called_once()
    mock_db.commit.assert_called_once()


@patch("sub_task_worker._get_db")
def test_db_write_update(mock_get_db):
    """UPDATE 操作"""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mock_get_db.return_value = mock_db

    result = sub_db_write(
        "test_table",
        {"status": "done"},
        condition={"id": 42},
    )
    assert result == 1
    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE" in sql
