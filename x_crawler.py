"""
x_crawler.py — X (Twitter) 爬虫消费者（任务队列版）

从用户 media 页收集推文链接 → 进入推文详情页提取所有图片 URL → 发下载子任务

注册函数:
  x_full_crawl(user_id)        — 全量抓取
  x_incremental_crawl(user_id) — 增量抓取（从顶部直到碰已处理帖）
"""
import hashlib
import json
import logging
import re
import signal
import threading
import time
from typing import List, Optional

import pymysql
import redis as redis_module
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from config import cfg
from task_queue_robust import register_task, TaskQueue, Worker, _current_task

logger = logging.getLogger("XCrawler")

# ============================================================
# Redis 连接
# ============================================================

def _state_redis():
    return redis_module.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=cfg["redis_db"],
        decode_responses=True,
    )

def _queue_redis():
    return redis_module.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True,
    )

# -----------------------------------------------------------
# 爬虫状态（业务 Redis）
# -----------------------------------------------------------

def _skey(uid):     return f"twitter:{uid}:state"
def _pkey(uid):     return f"twitter:{uid}:processed"

def _get_cursor_url(uid):
    return _state_redis().hget(_skey(uid), "cursor_url")

def _save_cursor(uid, url):
    _state_redis().hset(_skey(uid), "cursor_url", url)

def _update_state(uid, **kw):
    _state_redis().hset(_skey(uid), mapping=kw)

def _is_processed(uid, pid):
    return _state_redis().sismember(_pkey(uid), pid)

def _mark_processed(uid, pid):
    _state_redis().sadd(_pkey(uid), pid)

# -----------------------------------------------------------
# DB 写入
# -----------------------------------------------------------

def _get_db():
    return pymysql.connect(
        host=cfg["mysql_host"], port=cfg["mysql_port"],
        user=cfg["mysql_user"], password=cfg["mysql_password"],
        database=cfg["mysql_db"], charset="utf8mb4",
    )

def _lookup_star_id(user_id: str) -> Optional[int]:
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT id FROM la_star_info WHERE JSON_UNQUOTE(JSON_EXTRACT(original, '$.twitter')) = %s",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        db.close()

def _insert_star_instagram(star_id: int, image: str, batch: str, check_code: str, source: str = "x") -> int:
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            f"INSERT IGNORE INTO {cfg['table_prefix']}star_instagram "
            "(star_id, check_code, image, batch, status, source, create_time) "
            "VALUES (%s, %s, %s, %s, 'N', %s, %s)",
            (star_id, check_code, image, batch, source, int(time.time())),
        )
        db.commit()
        return cur.lastrowid
    finally:
        db.close()


# -----------------------------------------------------------
# Chrome 单例
# -----------------------------------------------------------

_driver = None
_driver_lock = threading.Lock()

def _get_driver():
    global _driver
    with _driver_lock:
        if _driver is None:
            _driver = _setup_chrome()
            _ensure_login(_driver)
    return _driver

def _close_driver():
    global _driver
    with _driver_lock:
        if _driver:
            try:
                _driver.quit()
            except Exception:
                pass
            _driver = None


# -----------------------------------------------------------
# 心跳
# -----------------------------------------------------------

_heartbeat_started = False

def _start_heartbeat(interval=120):
    global _heartbeat_started
    if _heartbeat_started:
        return
    _heartbeat_started = True

    def _beat():
        while True:
            time.sleep(interval)
            task = _current_task
            if task is None:
                continue
            try:
                r = _queue_redis()
                r.hset(f"processing:{task.queue_name}", task.task_id, str(time.time() + 600))
            except Exception:
                pass
    threading.Thread(target=_beat, daemon=True).start()

# -----------------------------------------------------------
# Chrome
# -----------------------------------------------------------

def _setup_chrome(headless=False):
    cp = cfg["ig_chrome_path"]
    cdp = cfg["ig_chromedriver_path"]
    if not cp or not cdp:
        raise RuntimeError("IG_CHROME_PATH and IG_CHROMEDRIVER_PATH must be set")

    opt = Options()
    opt.binary_location = cp
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_experimental_option("excludeSwitches", ["enable-automation"])
    opt.add_experimental_option("useAutomationExtension", False)
    opt.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1920,1080")
    if headless:
        opt.add_argument("--headless")
    opt.add_argument(f"--user-data-dir=/tmp/chrome_x_{int(time.time())}")

    driver = webdriver.Chrome(service=Service(executable_path=cdp), options=opt)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver

# -----------------------------------------------------------
# 登录（auth_token cookie）
# -----------------------------------------------------------

_AUTH_TOKEN_KEY = "x:auth_token"

def _inject_auth_token(driver) -> bool:
    """注入 auth_token cookie 登录 X"""
    token = cfg["x_auth_token"]
    if not token:
        return False

    driver.get("https://x.com")
    time.sleep(3)

    driver.add_cookie({
        "name": "auth_token",
        "value": token,
        "domain": ".x.com",
        "path": "/",
    })
    driver.refresh()
    time.sleep(8)

    current = driver.current_url
    if "login" in current or "i/flow" in current:
        return False

    logger.info("auth_token login OK")
    return True


def _load_saved_auth_token(driver) -> bool:
    """从 Redis 加载已保存的 auth_token 恢复 session"""
    r = _state_redis()
    token = r.get(_AUTH_TOKEN_KEY)
    if not token:
        return False

    driver.get("https://x.com")
    time.sleep(2)

    driver.add_cookie({
        "name": "auth_token",
        "value": token,
        "domain": ".x.com",
        "path": "/",
    })
    driver.refresh()
    time.sleep(6)

    if "login" in driver.current_url or "i/flow" in driver.current_url:
        return False

    logger.info("Session restored from auth_token")
    return True


def _save_auth_token(driver):
    cookies = driver.get_cookies()
    for c in cookies:
        if c["name"] == "auth_token":
            _state_redis().setex(_AUTH_TOKEN_KEY, 86400 * 30, c["value"])
            logger.info("Saved auth_token to Redis")


def _ensure_login(driver):
    # 1. 优先从 Redis 恢复
    if _load_saved_auth_token(driver):
        return

    # 2. 尝试 config 中的 auth_token
    token = cfg["x_auth_token"]
    if not token:
        raise RuntimeError(
            "未配置 x_auth_token 且 Redis 中无缓存，"
            "请设置 X_AUTH_TOKEN 或先手动导入"
        )

    try:
        if not _inject_auth_token(driver):
            raise RuntimeError(
                "auth_token 注入失败，X 可能已过期。"
                "请更新 .env 中的 X_AUTH_TOKEN"
            )
        _save_auth_token(driver)
    except Exception as e:
        raise RuntimeError(
            f"X 登录失败 ({e})，请检查 X_AUTH_TOKEN 是否有效"
        )


# -----------------------------------------------------------
# 列表页提取
# -----------------------------------------------------------

_re_tweet_id = re.compile(r"/status/(\d+)")

def _extract_tweet_id(url):
    m = _re_tweet_id.search(url)
    return m.group(1) if m else None

def _fix_image_url(url):
    """将缩略图 URL 转为原图 URL"""
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


def _extract_grid_thumbnail(link) -> Optional[str]:
    """单图推文：从列表页链接中提取缩略图 URL 并转原图"""
    try:
        img = link.find_element(By.XPATH, ".//img")
        return _fix_image_url(img.get_attribute("src"))
    except Exception:
        return None


def _is_multi_image(link) -> bool:
    """检测推文是否有多张图（X 用 aria-label 标记）"""
    try:
        el = link.find_elements(By.XPATH, ".//div[@aria-label and contains(@aria-label, 'Image')]")
        if el:
            label = el[0].get_attribute("aria-label") or ""
            # X 的图片 aria-label 如 "Image 1 of 3"
            if "of" in label:
                parts = label.split("of")
                if len(parts) == 2 and parts[1].strip().isdigit():
                    return int(parts[1].strip()) > 1
    except Exception:
        pass
    return False


def _extract_gallery_images(driver, link) -> List[str]:
    """点击多图推文，在 gallery 弹窗中翻页提取所有图片 URL"""
    grid_url = driver.current_url

    try:
        link.click()
    except Exception:
        return []
    time.sleep(2)

    # X 点击后通常打开一个侧边/弹窗 panel
    if driver.current_url != grid_url:
        # 导航到了推文页（旧版 X）
        images = _extract_images_from_detail(driver)
        driver.back()
        time.sleep(2)
        return images

    # 弹窗模式：获取当前可见图片，然后翻页
    images = _extract_gallery_slides(driver)
    _close_gallery(driver)
    return images


def _extract_gallery_slides(driver) -> List[str]:
    """在弹窗中循环翻页获取所有图片"""
    images = []
    seen = set()

    def _grab():
        # 弹窗里的 img — 多种可能的选择器
        xpaths = [
            "//div[@aria-label='Image']//img",
            "//div[@role='dialog']//img",
            "//div[@data-testid='swipe-to-dismiss']//img",
            "//article//div[@data-testid='tweetPhoto']//img",
        ]
        for xp in xpaths:
            for img in driver.find_elements(By.XPATH, xp):
                src = img.get_attribute("src")
                fixed = _fix_image_url(src)
                if fixed and fixed not in seen:
                    seen.add(fixed)
                    images.append(fixed)

    _grab()
    logger.info(f"  Gallery opened, initial: {len(images)} images")

    no_new_streak = 0
    for _ in range(50):
        before = len(images)
        try:
            next_btn = driver.find_element(By.XPATH, "//button[@aria-label='Next']")
            next_btn.click()
            time.sleep(1)
            _grab()
        except Exception:
            break
        if len(images) > before:
            no_new_streak = 0
        else:
            no_new_streak += 1
            if no_new_streak >= 3:
                break

    return images


def _extract_images_from_detail(driver) -> List[str]:
    """从推文详情页提取图片（降级方案）"""
    images = []
    try:
        imgs = driver.find_elements(
            By.XPATH,
            "//article[@data-testid='tweet']//div[@data-testid='tweetPhoto']//img"
        )
        for img in imgs:
            fixed = _fix_image_url(img.get_attribute("src"))
            if fixed and fixed not in images:
                images.append(fixed)
    except Exception:
        pass
    return images


def _close_gallery(driver):
    """关闭 gallery 弹窗"""
    try:
        close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Close']")
        close_btn.click()
        time.sleep(1)
    except Exception:
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1)
        except Exception:
            pass


# -----------------------------------------------------------
# 核心：收集推文 URL + 进入详情页提取图片
# -----------------------------------------------------------

def _crawl_user(user_id: str, incremental: bool = False) -> int:
    _start_heartbeat()
    driver = _get_driver()
    processed = 0

    logger.info(f"Visiting media page for {user_id}")
    driver.get(f"https://x.com/{user_id}/media")
    time.sleep(5)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//article"))
        )
    except Exception:
        logger.warning("Media page load timeout")

    # 查找 star_id
    star_id = _lookup_star_id(user_id)
    if star_id is None:
        logger.warning(f"No star_id found for {user_id}, DB insert disabled")

    # 全量续跑游标
    cursor_url = None if incremental else _get_cursor_url(user_id)
    cursor_found = not cursor_url
    if cursor_url:
        logger.info(f"Resume full crawl, fast-forward past cursor")

    tq = TaskQueue()
    tq.redis = _queue_redis()

    seen_urls = set()
    no_new = 0
    same_height = 0
    prev_height = 0
    since_cursor_save = 0

    for scroll_idx in range(500):
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href, '/status/')]",
        )
        logger.info(f"Scroll {scroll_idx+1}: {len(links)} tweet links on page")

        new_found = 0

        for link in links:
            href = link.get_attribute("href")
            if not href:
                continue
            clean = href.split("?")[0]

            # 游标续跑
            if not cursor_found:
                if clean == cursor_url:
                    cursor_found = True
                    logger.info("Found cursor, resuming crawl")
                continue

            if clean in seen_urls:
                continue
            seen_urls.add(clean)

            tweet_id = _extract_tweet_id(clean)
            if not tweet_id:
                continue

            # GUI 跳过已处理
            if cursor_url is None and _is_processed(user_id, tweet_id):
                continue

            # ---- 点击推文弹窗提取图片 ----
            image_urls = _extract_gallery_images(driver, link)
            dom_changed = True

            if not image_urls:
                _mark_processed(user_id, tweet_id)
                continue

            # ---- 写入 DB + 发下载子任务 ----
            for idx, img_url in enumerate(image_urls, 1):
                ext = ".png" if ".png" in img_url else ".jpg"
                filename = f"{tweet_id}_{idx:04d}{ext}"
                check_code = hashlib.md5(filename.encode()).hexdigest()
                batch = f"/{user_id}/status/{tweet_id}/"
                if star_id:
                    try:
                        db_id = _insert_star_instagram(
                            star_id, f"image/{star_id}/{check_code}{ext}",
                            batch, check_code, "x"
                        )
                        save_path = f"image/{star_id}/{check_code}{ext}"
                    except Exception as e:
                        logger.error(f"DB insert failed: {e}")
                        db_id = None
                        save_path = f"{user_id}/{filename}"
                else:
                    db_id = None
                    save_path = f"{user_id}/{filename}"
                if db_id == 0:
                    continue
                tq.enqueue(
                    f"dl:x", "sub_download_image",
                    img_url, save_path, db_id, "x", user_id,
                )

            _mark_processed(user_id, tweet_id)
            processed += len(image_urls)
            new_found += 1

            if not incremental:
                since_cursor_save += 1
                if since_cursor_save >= 20:
                    _save_cursor(user_id, clean)
                    since_cursor_save = 0

            time.sleep(0.5)

            if dom_changed:
                # gallery 弹窗关闭后 DOM 恢复，跳出当前循环，下轮 scroll 重新找链接
                break

        if new_found:
            logger.info(f"Scroll {scroll_idx+1}: +{new_found} tweets ({processed} images)")
            no_new = 0
        else:
            no_new += 1
            if incremental and no_new >= 5:
                logger.info("No new tweets for 5 scrolls, boundary reached")
                break
            if not incremental and no_new >= 20:
                logger.info("No new tweets for 20 scrolls, stopping")
                break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == prev_height:
            same_height += 1
            if same_height >= 10:
                logger.info("Page height not growing, stopping")
                break
        else:
            same_height = 0
        prev_height = new_h

    if processed:
        _update_state(user_id, last_scrape_time=time.time())
    elif cursor_url and not incremental:
        _state_redis().hdel(_skey(user_id), "cursor_url")
        logger.info(f"All tweets already processed, cursor cleared for {user_id}")

    logger.info(f"Crawl done for {user_id}, {processed} images")
    return processed


# -----------------------------------------------------------
# 注册任务
# -----------------------------------------------------------

@register_task("x_full_crawl")
def x_full_crawl(user_id: str) -> str:
    result = f"full crawl: {_crawl_user(user_id, incremental=False)} images"
    tq = TaskQueue()
    tq.redis = _queue_redis()
    tq.enqueue("crawl:x:incr", "x_incremental_crawl", user_id)
    logger.info(f"Auto-enqueued incremental task for {user_id}")
    return result

@register_task("x_incremental_crawl")
def x_incremental_crawl(user_id: str) -> str:
    return f"incremental crawl: {_crawl_user(user_id, incremental=True)} images"


# -----------------------------------------------------------
# CLI
# -----------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("full", "incr", "all"), default="all",
                        help="full=全量, incr=增量, all=两者")
    opt_args = parser.parse_args()

    if opt_args.mode == "full":
        queue_names = ["crawl:x:full"]
        worker_id = "x-crawler-full"
    elif opt_args.mode == "incr":
        queue_names = ["crawl:x:incr"]
        worker_id = "x-crawler-incr"
    else:
        queue_names = ["crawl:x:full", "crawl:x:incr"]
        worker_id = "x-crawler"

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

    for q in queue_names:
        dead = tq.dead_count(q)
        if dead:
            logger.info(f"Requeuing {dead} dead tasks from '{q}'")
            tq.retry_dead(q)

    def shutdown(sig, frame):
        worker.stop()
        _close_driver()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(f"X crawler worker ready (queues: {', '.join(queue_names)})")
    worker.start()
    _close_driver()


if __name__ == "__main__":
    main()
