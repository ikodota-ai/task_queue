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
import os
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
from task_queue_robust import register_task, Task, TaskQueue, Worker, _current_task

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

_cached_db = None

def _get_db():
    global _cached_db
    if _cached_db is None or not _cached_db.open:
        _cached_db = pymysql.connect(
            host=cfg["mysql_host"], port=cfg["mysql_port"],
            user=cfg["mysql_user"], password=cfg["mysql_password"],
            database=cfg["mysql_db"], charset="utf8mb4",
            autocommit=False,
        )
    return _cached_db

def _close_db():
    global _cached_db
    if _cached_db:
        try:
            _cached_db.close()
        except Exception:
            pass
        _cached_db = None

def _lookup_star_id(user_id: str) -> Optional[int]:
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT id FROM la_star_info WHERE twitter = %s",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        pass  # 复用连接


def _mark_full_done_db(user_id: str, maxpage: int, platform: str = "x"):
    """全量完成时写入 la_star_info"""
    db = _get_db()
    try:
        cur = db.cursor()
        col_done = f"{platform}_full_done"
        col_maxpage = f"{platform}_maxpage"
        cur.execute(
            f"UPDATE la_star_info SET {col_done}=1, {col_maxpage}=%s WHERE twitter=%s",
            (maxpage, user_id),
        )
        db.commit()
        logger.info(f"DB updated: la_star_info.{col_done}=1 for {user_id}")
    except Exception as e:
        logger.error(f"Failed to update DB full_done for {user_id}: {e}")


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
        pass  # 复用连接


# -----------------------------------------------------------
# 抓取任务状态跟踪
# -----------------------------------------------------------

def _update_crawl_status(db_task_id: int, status: str, images_count: int = None):
    """更新 la_crawl_tasks 状态"""
    db = _get_db()
    try:
        cur = db.cursor()
        if images_count is not None:
            cur.execute(
                f"UPDATE {cfg['table_prefix']}crawl_tasks SET status = %s, images_count = %s, updated_at = NOW() WHERE id = %s",
                (status, images_count, db_task_id),
            )
        else:
            cur.execute(
                f"UPDATE {cfg['table_prefix']}crawl_tasks SET status = %s, updated_at = NOW() WHERE id = %s",
                (status, db_task_id),
            )
        db.commit()
    finally:
        pass  # 复用连接

def _is_full_crawl_done(user_id: str) -> bool:
    """检查用户全量抓取是否已完成"""
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            f"SELECT status FROM {cfg['table_prefix']}crawl_tasks "
            "WHERE platform = 'x' AND user_id = %s AND task_type = 'full' "
            "ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if row and row[0] == "done":
            return True
    finally:
        pass  # 复用连接

    if _state_redis().scard(_pkey(user_id)) > 0:
        return True

    return False


# -----------------------------------------------------------
# Chrome 单例
# -----------------------------------------------------------

_driver = None
_driver_lock = threading.Lock()

def _get_driver():
    global _driver
    with _driver_lock:
        if _driver is not None:
            try:
                _driver.current_url
            except Exception:
                logger.warning("Chrome connection lost, recreating driver")
                try:
                    _driver.quit()
                except Exception:
                    pass
                _driver = None
        if _driver is None:
            _driver = _setup_chrome()
            _ensure_login(_driver)
    return _driver

def _close_driver():
    global _driver
    with _driver_lock:
        if _driver:
            try:
                import glob as _glob, shutil as _shutil
                for d in _glob.glob("/tmp/chrome_x_*"):
                    _shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
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
                r = redis_module.Redis(
                    host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
                    password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
                    decode_responses=True, socket_keepalive=True,
                    socket_connect_timeout=5, socket_timeout=5,
                )
                r.hset(f"processing:{task.queue_name}", task.task_id, str(time.time() + 600))
                r.close()
            except Exception as e:
                logger.warning(f"Heartbeat extend failed: {e}")
    threading.Thread(target=_beat, daemon=True).start()

# -----------------------------------------------------------
# Chrome
# -----------------------------------------------------------

def _setup_chrome(headless=False):
    cp = cfg["ig_chrome_path"]
    cdp = cfg["ig_chromedriver_path"]
    if not cp or not cdp:
        raise RuntimeError("IG_CHROME_PATH and IG_CHROMEDRIVER_PATH must be set")

    import glob as _glob, shutil as _shutil
    for old in _glob.glob("/tmp/chrome_x_*") + _glob.glob("/tmp/chrome_ig_*"):
        try:
            parts = os.path.basename(old).split("_")
            pid = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
            if pid and os.kill(pid, 0):
                continue
        except (OSError, ValueError, IndexError):
            pass
        try:
            _shutil.rmtree(old, ignore_errors=True)
        except Exception:
            pass

    opt = Options()
    opt.binary_location = cp
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_experimental_option("excludeSwitches", ["enable-automation", "disable-popup-blocking"])
    opt.add_experimental_option("useAutomationExtension", False)
    opt.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--disable-usb")
    # 内存优化
    opt.add_argument("--max_old_space_size=512")
    opt.add_argument("--js-flags=--max-old-space-size=512")
    opt.add_argument("--disable-background-networking")
    opt.add_argument("--disable-sync")
    opt.add_argument("--disable-translate")
    opt.add_argument("--disable-extensions")
    opt.add_argument("--window-size=1920,1080")
    if headless or os.getenv("CHROME_HEADLESS", "") in ("1", "true", "yes"):
        opt.add_argument("--headless=new")
        opt.add_argument("--disable-gpu")
        opt.add_argument("--no-zygote")
        opt.add_argument("--disable-features=VizDisplayCompositor,TranslateUI")
        opt.add_argument("--no-first-run")
        opt.add_argument("--disable-default-apps")
    opt.add_argument(f"--user-data-dir=/tmp/chrome_x_{os.getpid()}_{int(time.time())}")

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


def _has_gallery_icon(link) -> bool:
    """检测列表页推文链接内是否包含多图 SVG 图标"""
    try:
        svg = link.find_elements(By.XPATH, ".//*[local-name()='svg']")
        return len(svg) > 0
    except Exception:
        return False


def _extract_grid_thumbnail(link) -> Optional[str]:
    """单图推文：从列表页网格提取缩略图 URL 转原图"""
    try:
        img = link.find_element(By.XPATH, ".//img")
        return _fix_image_url(img.get_attribute("src"))
    except Exception:
        return None


# -----------------------------------------------------------
# 多图帖子：点击弹框 → 翻页取所有图片
# -----------------------------------------------------------

def _extract_carousel_images(driver, link) -> List[str]:
    """点击推文 → 弹窗打开 → 翻页取图 → 关闭弹窗"""
    try:
        link.click()
    except Exception:
        return []
    time.sleep(2)
    images = _extract_images_from_tweet(driver)
    if not images:
        # 即使没图也要关弹窗
        _close_modal(driver)
    return images


def _extract_images_from_tweet(driver) -> List[str]:
    """在已打开的弹窗中翻页取图，最后关闭弹窗"""
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.XPATH, "//div[@aria-roledescription='carousel']"))
        )
    except Exception:
        return []
    images, seen = [], set()

    def _grab():
        for xp in (
            "//div[@aria-roledescription='carousel']//img",
            "//article//img[contains(@src, 'pbs.twimg.com')]",
        ):
            for img in driver.find_elements(By.XPATH, xp):
                fixed = _fix_image_url(img.get_attribute("src"))
                if fixed and fixed not in seen:
                    seen.add(fixed)
                    images.append(fixed)

    
    _grab()
    logger.info(f"  Tweet modal opened, initial: {len(images)} images")

    no_new_streak = 0
    for _ in range(50):
        before = len(images)
        next_btn = None
        for aria in ("Next slide", "下一页", "下一步"):
            btns = driver.find_elements(By.XPATH, f"//button[@aria-label='{aria}']")
            if btns:
                next_btn = btns[0]
                break
        if not next_btn:
            break
        try:
            next_btn.click()
        except:
            break
        time.sleep(0.8)
        _grab()
        if len(images) > before:
            no_new_streak = 0
        else:
            no_new_streak += 1
            if no_new_streak >= 3:
                break

    _close_modal(driver)
    return images


def _close_modal(driver):
    """关闭 X 弹窗，回到列表页"""
    for xp in (
        "//div[@aria-roledescription='carousel']//button[@aria-label='close']",
        "//button[@aria-label='close']",
        "//div[@role='dialog']//button[@aria-label='Close']",
    ):
        try:
            driver.find_element(By.XPATH, xp).click()
            break
        except:
            continue
    else:
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except:
            pass
    time.sleep(1)


def _close_dialog(driver):
    """关闭弹框"""

    # close_btn = driver.find_elements(
    #     By.XPATH,
    #     "//div[@role='button']//svg[@aria-label='关闭']"
    # )
    # driver.execute_script("""
    #     var els = document.querySelectorAll('div[role="button"] svg[aria-label="关闭"]');
    #     if (els.length) els[0].parentElement.click();
    # """)

    # 关闭弹窗（依次尝试多种 close）
    for xp in (
        "//div[@aria-roledescription='carousel']//button[@aria-label='close']",
        "//button[@aria-label='close']",
        "//div[@role='dialog']//button[@aria-label='Close']",
    ):
        try:
            driver.find_element(By.XPATH, xp).click()
            break
        except:
            continue
    else:
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except:
            pass

    return
   
# -----------------------------------------------------------
# 核心：收集推文 URL + 进入详情页提取图片
# -----------------------------------------------------------

def _crawl_user(user_id: str, incremental: bool = False, maxpage: int = 500) -> int:
    _start_heartbeat()

    lock_key = f"x:{user_id}:crawling"
    if not _state_redis().set(lock_key, "1", nx=True, ex=7200):
        logger.warning(f"{user_id} is already being crawled by another worker, skipping")
        return 0

    try:
        return _do_crawl(user_id, incremental, maxpage)
    finally:
        _state_redis().delete(lock_key)


def _do_crawl(user_id: str, incremental: bool = False, maxpage: int = 500) -> int:

    # 全量已完成检查：full_done=1 且 last_maxpage >= 本次目标则跳过
    if not incremental:
        full_done = _state_redis().hget(_skey(user_id), "full_done")
        saved = int(_state_redis().hget(_skey(user_id), "last_maxpage") or _state_redis().hget(_skey(user_id), "maxpage") or 0)
        if full_done == "1" and saved >= maxpage:
            logger.info(f"Full crawl for {user_id}: full_done=1, last_maxpage {saved} >= target {maxpage}, skipping")
            return 0
        if full_done == "1":
            logger.info(f"Full crawl for {user_id}: last_maxpage {saved} < target {maxpage}, deeper crawl")
        cursor = _get_cursor_url(user_id)
        if not cursor and _state_redis().scard(_pkey(user_id)) > 0:
            logger.info(f"Full crawl for {user_id}: no cursor, will skip already-processed posts")

    driver = _get_driver()
    processed = 0

    logger.info(f"Visiting media page for {user_id}")
    driver.get(f"https://x.com/{user_id}/media")
    time.sleep(5)

    try:
        WebDriverWait(driver, 8).until(
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

    start_page = int(_state_redis().hget(_skey(user_id), "pages_done") or 0) if not incremental else 0
    for scroll_idx in range(start_page, maxpage):
        links = driver.find_elements(
            By.XPATH,
            "//section[@role='region']//li[@role='listitem']//a",
        )
        logger.info(f"Scroll {scroll_idx+1}: {len(links)} tweet links on page")

        new_found = 0
        link_idx = 0

        while link_idx < len(links):
            try:
                href = links[link_idx].get_attribute("href")
            except Exception:
                link_idx += 1
                continue
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

            # 跳过已处理
            if cursor_url is None and _is_processed(user_id, tweet_id):
                continue

            # ---- 提取图片 ----
            image_urls: List[str] = []

            # 点开弹窗取所有图
            image_urls = _extract_carousel_images(driver, link)

            if not image_urls:
                _mark_processed(user_id, tweet_id)
                links = driver.find_elements(
                    By.XPATH, "//section[@role='region']//li[@role='listitem']//a")
                link_idx = 0
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
                            star_id, f"x/image/{star_id}/{check_code}{ext}",
                            batch, check_code, "x"
                        )
                        save_path = f"x/image/{star_id}/{check_code}{ext}"
                    except Exception as e:
                        logger.error(f"DB insert failed: {e}")
                        db_id = None
                        save_path = f"x/{user_id}/{filename}"
                else:
                    db_id = None
                    save_path = f"x/{user_id}/{filename}"
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
            links = driver.find_elements(
                By.XPATH, "//section[@role='region']//li[@role='listitem']//a")
            link_idx = 0

        if new_found:
            logger.info(f"Scroll {scroll_idx+1}: +{new_found} tweets ({processed} images)")
            no_new = 0
        else:
            no_new += 1
            if incremental and no_new >= 5:
                logger.info("No new tweets for 5 scrolls, boundary reached")
                break

        if not incremental and scroll_idx % 3 == 0:
            _state_redis().hset(_skey(user_id), "pages_done", str(scroll_idx))

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == prev_height:
            same_height += 1
            if same_height >= 10:
                logger.info("Page height not growing, reached bottom")
                break
        else:
            same_height = 0
        prev_height = new_h

    _state_redis().hdel(_skey(user_id), "pages_done")

    # 全量完成时写入 last_maxpage 和 full_done（增量不写）
    actual_pages = scroll_idx + 1
    if same_height >= 10:
        logger.info(f"Reached bottom at page {actual_pages}")
        if not incremental:
            _state_redis().hset(_skey(user_id), "last_maxpage", str(maxpage))
            _state_redis().hset(_skey(user_id), "full_done", "1")
            _mark_full_done_db(user_id, maxpage, "x")
    else:
        logger.info(f"Reached maxpage limit ({maxpage} pages)")
        if not incremental:
            _state_redis().hset(_skey(user_id), "last_maxpage", str(maxpage))
            _state_redis().hset(_skey(user_id), "full_done", "1")
            _mark_full_done_db(user_id, maxpage, "x")

    if processed:
        _update_state(user_id, last_scrape_time=time.time())
    if incremental and processed >= 0:
        r = _state_redis()
        cnt = r.hincrby(_skey(user_id), "incr_count", 1)
        r.hset(_skey(user_id), "incr_last_time", str(int(time.time())))
        logger.info(f"Incremental #{cnt} done for {user_id}")
    elif cursor_url and not incremental:
        _state_redis().hdel(_skey(user_id), "cursor_url")
        logger.info(f"All tweets already processed, cursor cleared for {user_id}")

    logger.info(f"Crawl done for {user_id}, {processed} images")
    return processed


# -----------------------------------------------------------
# 注册任务
# -----------------------------------------------------------

@register_task("x_full_crawl")
def x_full_crawl(user_id: str, db_task_id: int = None, maxpage: int = None) -> str:
    if maxpage is None:
        maxpage = int(os.getenv("MAX_PAGE", 500))
    if db_task_id:
        _update_crawl_status(db_task_id, "processing")
    try:
        count = _crawl_user(user_id, incremental=False, maxpage=maxpage)
        if db_task_id:
            _update_crawl_status(db_task_id, "done", count)
        result = f"full crawl: {count} images"
        # 全量完成后自动投增量（写 MySQL）
        db_task_id = 0
        try:
            db = _get_db()
            cur = db.cursor()
            cur.execute(
                f"INSERT INTO {cfg['table_prefix']}crawl_tasks (platform, task_type, user_id, status) VALUES ('x', 'incr', %s, 'queued')",
                (user_id,),
            )
            db.commit()
            db_task_id = cur.lastrowid
        except Exception:
            pass
        tq = TaskQueue()
        tq.redis = _queue_redis()
        tq.enqueue_unique("crawl:x:incr", "x_incremental_crawl", user_id, db_task_id)
        logger.info(f"Auto-enqueued incremental for {user_id} (db_id={db_task_id})")
        return result
    except Exception:
        if db_task_id:
            _update_crawl_status(db_task_id, "failed")
        raise

@register_task("x_incremental_crawl")
def x_incremental_crawl(user_id: str, db_task_id: int = None) -> str:
    # 检查全量是否完成
    if not _is_full_crawl_done(user_id):
        msg = f"Skipping incremental for {user_id}: full crawl not done"
        logger.warning(msg)
        if db_task_id:
            _update_crawl_status(db_task_id, "skipped")
        return msg

    if db_task_id:
        _update_crawl_status(db_task_id, "processing")
    try:
        count = _crawl_user(user_id, incremental=True)
        if db_task_id:
            _update_crawl_status(db_task_id, "done", count)
        result = f"incremental crawl: {count} images"
        # 增量自循环：6 小时后再入队（写 MySQL）
        db_task_id = 0
        try:
            db = _get_db()
            cur = db.cursor()
            cur.execute(
                f"INSERT INTO {cfg['table_prefix']}crawl_tasks (platform, task_type, user_id, status) VALUES ('x', 'incr', %s, 'queued')",
                (user_id,),
            )
            db.commit()
            db_task_id = cur.lastrowid
        except Exception:
            pass
        task = Task("x_incremental_crawl", (user_id, db_task_id), {}, "crawl:x:incr")
        tq = TaskQueue()
        tq.redis = _queue_redis()
        tq.redis.zadd(tq.retry_key("crawl:x:incr"),
                      {json.dumps(task.to_dict()): time.time() + 6 * 3600})
        logger.info(f"Scheduled next incremental for {user_id} in 6h (db_id={db_task_id})")
        return result
    except Exception:
        if db_task_id:
            _update_crawl_status(db_task_id, "failed")
        raise


# -----------------------------------------------------------
# CLI
# -----------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("full", "incr", "all"), default="all",
                        help="full=全量, incr=增量, all=全量+增量")
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
