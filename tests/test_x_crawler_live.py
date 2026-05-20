"""
X Crawler 真实环境测试 — 使用 db15(队列) + db14(业务)，不写 MySQL

用法:
    source venv/bin/activate
    python tests/test_x_crawler_live.py kiraanaaq
"""
import hashlib
import json
import logging
import os
import re
import sys
import time
import threading
from typing import List, Optional

sys.path.insert(0, ".")

import redis as redis_module
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from config import cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("XTest")

# ============================================================
# 测试专用 Redis（db15=队列, db14=业务）
# ============================================================

_queue_redis = redis_module.Redis(
    host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
    password=cfg["queue_redis_password"], db=15, decode_responses=True,
)
_state_redis = redis_module.Redis(
    host=cfg["redis_host"], port=cfg["redis_port"],
    password=cfg["redis_password"], db=14, decode_responses=True,
)

# ============================================================
# 状态 key
# ============================================================

def _skey(uid):     return f"twitter:{uid}:test-state"
def _pkey(uid):     return f"twitter:{uid}:test-processed"

def _is_processed(uid, pid):
    return _state_redis.sismember(_pkey(uid), pid)

def _mark_processed(uid, pid):
    _state_redis.sadd(_pkey(uid), pid)

# ============================================================
# URL 工具
# ============================================================

_re_tweet_id = re.compile(r"/status/(\d+)")

def _extract_tweet_id(url):
    m = _re_tweet_id.search(url)
    return m.group(1) if m else None

def _fix_image_url(url):
    if not url:
        return None
    if "profile_images" in url or "emoji" in url or "video_thumb" in url:
        return None
    base = url.split("?")[0]
    if base.endswith((".jpg", ".jpeg")):
        return f"{base}?format=jpg&name=orig"
    elif base.endswith(".png"):
        return f"{base}?format=png&name=orig"
    else:
        return f"{base}?format=jpg&name=orig"

# ============================================================
# Chrome
# ============================================================

def _setup_chrome():
    cp, cdp = cfg["ig_chrome_path"], cfg["ig_chromedriver_path"]
    opt = Options()
    opt.binary_location = cp
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1920,1080")
    if os.getenv("CHROME_HEADLESS", "") in ("1", "true", "yes"):
        opt.add_argument("--headless=new")
    opt.add_argument(f"--user-data-dir=/tmp/chrome_xtest_{int(time.time())}")
    return webdriver.Chrome(service=Service(executable_path=cdp), options=opt)

# ============================================================
# 登录
# ============================================================

def _ensure_login(driver):
    token = cfg.get("x_auth_token", "")
    if not token:
        raise RuntimeError("X_AUTH_TOKEN not set")
    driver.get("https://x.com")
    time.sleep(3)
    driver.add_cookie({"name": "auth_token", "value": token, "domain": ".x.com", "path": "/"})
    driver.refresh()
    time.sleep(6)
    if "login" in driver.current_url:
        raise RuntimeError("Login failed")
    logger.info("Login OK")

# ============================================================
# 列表页提取
# ============================================================

def _has_gallery_icon(link):
    try:
        return len(link.find_elements(By.XPATH, ".//*[local-name()='svg']")) > 0
    except:
        return False

def _extract_grid_thumbnail(link):
    try:
        src = link.find_element(By.XPATH, ".//img").get_attribute("src")
        return _fix_image_url(src)
    except:
        return None

def _extract_images_from_tweet(driver, link):
    try:
        link.click()
    except:
        return []
    time.sleep(2)
    images, seen = [], set()

    def _grab():
        for img in driver.find_elements(By.XPATH, "//div[@data-testid='tweetPhoto']//img"):
            fixed = _fix_image_url(img.get_attribute("src"))
            if fixed and fixed not in seen:
                seen.add(fixed)
                images.append(fixed)

    _grab()
    for _ in range(50):
        before = len(images)
        try:
            driver.find_element(By.XPATH, "//button[@aria-label='Next']").click()
            time.sleep(0.8)
            _grab()
        except:
            break
        if len(images) == before:
            break

    driver.back()
    time.sleep(1.5)
    return images

# ============================================================
# 主循环
# ============================================================

def crawl_user(user_id: str, max_scrolls=20):
    _state_redis.delete(_skey(user_id), _pkey(user_id))

    driver = _setup_chrome()
    try:
        _ensure_login(driver)

        logger.info(f"Visiting {user_id}/media")
        driver.get(f"https://x.com/{user_id}/media")
        time.sleep(8)

        total_images = 0
        total_tweets = 0
        seen_urls = set()
        no_new = 0
        prev_h = 0
        same_h = 0

        for scroll in range(max_scrolls):
            links = driver.find_elements(By.XPATH, "//a[contains(@href, '/photo/')]")
            new_found = 0

            for link in links:
                href = link.get_attribute("href")
                if not href:
                    continue
                clean = href.split("?")[0]
                if clean in seen_urls:
                    continue
                seen_urls.add(clean)

                tweet_id = _extract_tweet_id(clean)
                if not tweet_id:
                    continue
                if _is_processed(user_id, tweet_id):
                    continue

                images = (
                    _extract_images_from_tweet(driver, link)
                    if _has_gallery_icon(link)
                    else [u for u in [_extract_grid_thumbnail(link)] if u]
                )

                for idx, url in enumerate(images, 1):
                    fn = f"{tweet_id}_{idx:04d}.jpg"
                    check_code = hashlib.md5(fn.encode()).hexdigest()
                    _queue_redis.lpush("queue:test:x:download",
                        json.dumps({"url": url, "path": f"x/test/{check_code}.jpg",
                                    "tweet": tweet_id, "idx": idx}))
                    total_images += 1

                _mark_processed(user_id, tweet_id)
                total_tweets += 1
                new_found += 1
                time.sleep(0.5)
                break  # stale links after navigation

            logger.info(f"Scroll {scroll+1}: +{new_found} tweets ({total_images} images)")

            if new_found == 0:
                no_new += 1
                if no_new >= 5:
                    logger.info("Boundary reached")
                    break
            else:
                no_new = 0

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(3)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == prev_h:
                same_h += 1
                if same_h >= 10:
                    break
            else:
                same_h = 0
            prev_h = h

        return total_images
    finally:
        driver.quit()


if __name__ == "__main__":
    uid = sys.argv[1] if len(sys.argv) > 1 else "kiraanaaq"
    print(f"=== X Crawler Test: {uid} ===")
    n = crawl_user(uid)
    print(f"Done: {n} images")
