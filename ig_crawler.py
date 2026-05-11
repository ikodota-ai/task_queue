"""
ig_crawler.py — Instagram 爬虫消费者（任务队列版）

直接从帖子列表页处理:
  - 单图帖子 → 网格缩略图 URL 直接发下载（最快）
  - 多图帖子 → 点击后弹框加载，翻页取所有图片 URL

注册函数:
  ig_full_crawl(user_id)        — 全量抓取
  ig_incremental_crawl(user_id) — 增量抓取（从顶部直到碰已处理帖）
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

logger = logging.getLogger("IGCrawler")

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

def _skey(uid):     return f"instagram:{uid}:state"
def _pkey(uid):     return f"instagram:{uid}:processed"

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
    """根据 Instagram 用户名查 la_star_info.id"""
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT id FROM la_star_info WHERE JSON_UNQUOTE(JSON_EXTRACT(original, '$.instagram')) = %s",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        db.close()

def _insert_star_instagram(star_id: int, image: str, batch: str, check_code: str) -> int:
    """插入 la_star_instagram，返回自增 ID"""
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            f"INSERT IGNORE INTO {cfg['table_prefix']}star_instagram "
            "(star_id, check_code, image, batch, status, source, create_time) "
            "VALUES (%s, %s, %s, %s, 'N', 'ins', %s)",
            (star_id, check_code, image, batch, int(time.time())),
        )
        db.commit()
        return cur.lastrowid
    finally:
        db.close()


# -----------------------------------------------------------
# Chrome 单例（Worker 生命周期内复用，避免反复启动）
# -----------------------------------------------------------

_driver = None
_driver_lock = threading.Lock()

def _get_driver():
    """获取或创建 Chrome 实例（已登录）"""
    global _driver
    with _driver_lock:
        if _driver is None:
            _driver = _setup_chrome()
            try:
                _ensure_login(_driver)
            except Exception:
                try:
                    _driver.quit()
                except Exception:
                    pass
                _driver = None
                raise
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
# 心跳（长任务续期）
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
    opt.add_argument(f"--user-data-dir=/tmp/chrome_ig_{int(time.time())}")

    driver = webdriver.Chrome(service=Service(executable_path=cdp), options=opt)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver

# -----------------------------------------------------------
# 登录（Cookie 持久化，避免每次手机验证）
# -----------------------------------------------------------

_COOKIE_KEY = "instagram:cookies"

def _load_saved_cookies(driver) -> bool:
    """加载已保存 cookies 恢复 session，成功返回 True"""
    r = _state_redis()
    raw = r.get(_COOKIE_KEY)
    if not raw:
        return False

    driver.get("https://www.instagram.com/")
    time.sleep(2)

    cookies = json.loads(raw)
    for c in cookies:
        c.pop("sameSite", None)
        try:
            driver.add_cookie(c)
        except Exception:
            pass

    # 验证 session
    driver.get("https://www.instagram.com/")
    time.sleep(3)
    if "accounts/login" in driver.current_url:
        return False
    try:
        driver.find_element(By.XPATH, "//div[@aria-label='登录']")
        return False
    except Exception:
        pass

    logger.info("Session restored from cookies")
    return True


def _save_cookies(driver):
    cookies = driver.get_cookies()
    _state_redis().setex(_COOKIE_KEY, 86400 * 30, json.dumps(cookies))
    logger.info(f"Saved {len(cookies)} cookies")


def _ensure_login(driver):
    """先试 cookie 恢复，失败则账号密码登录"""
    u, p = cfg["ig_username"], cfg["ig_password"]
    if not u or not p:
        return
    if _load_saved_cookies(driver):
        return
    logger.info("Cookie expired, logging in with password...")

    driver.get("https://www.instagram.com/accounts/login/")
    wait = WebDriverWait(driver, 15)
    wait.until(EC.presence_of_element_located((By.NAME, "email"))).send_keys(u)
    time.sleep(1)
    driver.find_element(By.NAME, "pass").send_keys(p)
    time.sleep(1)
    driver.find_element(By.XPATH, "//div[@aria-label='登录']").click()
    time.sleep(8)

    for _ in range(2):
        for _ in range(3):
            try:
                driver.find_element(By.XPATH, "//div[@role='button']").click()
                time.sleep(1)
                break
            except Exception:
                time.sleep(2)
        try:
            driver.find_element(By.XPATH, "//div[@role='dialog']//button[2]").click()
            time.sleep(1)
        except Exception:
            pass

    logger.info("Login OK")
    _save_cookies(driver)
    _close_login_dialog(driver)

# -----------------------------------------------------------
# 工具
# -----------------------------------------------------------

_re_post_id = re.compile(r"/(?:p|reel)/([^/?]+)")

def _extract_post_id(url):
    m = _re_post_id.search(url)
    return m.group(1) if m else None

def _close_login_dialog(driver):
    try:
        driver.find_element(
            By.XPATH, "//div[@role='dialog']//div[@role='button']"
        ).click()
        time.sleep(0.5)
    except Exception:
        pass

# -----------------------------------------------------------
# 检测多图帖子
# -----------------------------------------------------------

def _is_video(link) -> bool:
    """检查帖子链接上是否有『视频』标记"""
    indicators = [
        ".//*[local-name()='svg' and contains(@aria-label, '视频片段')]",
        ".//*[local-name()='svg' and contains(@aria-label, 'video')]",
    ]
    for xp in indicators:
        if link.find_elements(By.XPATH, xp):
            return True
    return False


def _is_carousel(link) -> bool:
    """检查帖子链接上是否有『多图』标记"""
    indicators = [
        ".//*[local-name()='svg' and contains(@aria-label, '轮播')]",
        ".//*[local-name()='svg' and contains(@aria-label, 'carousel')]",
    ]
    for xp in indicators:
        if link.find_elements(By.XPATH, xp):
            return True
    return False


# -----------------------------------------------------------
# 从网格缩略图取单张图片 URL
# -----------------------------------------------------------

def _extract_grid_thumbnail(link) -> Optional[str]:
    """单图帖子：从网格链接中提取缩略图 URL"""
    try:
        img = link.find_element(By.XPATH, ".//img[contains(@src, 'cdninstagram.com')]")
        return img.get_attribute("src")
    except Exception:
        return None


# -----------------------------------------------------------
# 多图帖子：点击弹框 → 翻页取所有图片
# -----------------------------------------------------------

def _extract_carousel_images(driver, link) -> List[str]:
    """
    点击多图帖子，在弹框中翻页提取所有图片 URL。
    返回图片 URL 列表（可能含 CDN 缩略图和全尺寸图）。
    """
    grid_url = driver.current_url

    # 点击帖子
    try:
        link.click()
    except Exception:
        return []
    time.sleep(2)

    # 情形 A：弹框模式（URL 不变，dialog 出现）
    # if driver.current_url == grid_url:
    images = _extract_from_dialog(driver)
    _close_dialog(driver)
    return images

    # 情形 B：导航到了帖子页（旧版 Instagram）
    # logger.info("Navigated to post page, extracting there")
    # images = _extract_images_from_post_page(driver)
    # driver.back()
    # time.sleep(2)
    # return images


def _extract_from_dialog(driver) -> List[str]:
    """从弹框中提取所有图片。

    注意：Instagram 将轮播所有图片同时渲染在 DOM 中（多个 <li><img>），
    用 transform: translateX 切换显示，因此第一次 _grab 就能拿到全部 URL，
    不需要翻页。保留 JS 翻页作为兼容后备。
    """
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='dialog']"))
        )



    except Exception:
        return []

    images = []
    seen = set()

    img = driver.find_elements(By.XPATH,"//article[@role='presentation']//img[contains(@src, 'cdninstagram.com')]")
    img_url = img[0].get_attribute("src")
    # print(f"img_Url:{img_url}")
    # images.append(img_url)
    logger.info(f"Carousel opened, initial images: {len(images)}")
    # time.sleep(2)
    while True:
        try:
            # next_button = driver.find_elements(By.XPATH, "//button[@aria-label='下一步']")
            next_button = driver.find_elements(By.XPATH, "//button[contains(@class, '_afxw')]")
            next_button[1].click()
            time.sleep(1)
            try:
                new_images = driver.find_elements(By.XPATH,"//article[@role='presentation']//img[contains(@src, 'cdninstagram.com')]")
                for img in new_images:
                        img_url = img.get_attribute("src")
                        if img_url and "s150x150" not in img_url and "profile" not in img_url and img_url not in seen:
                            seen.add(img_url)
                            images.append(img_url)
                            

            except:
                break
            
        except Exception:
            # print(Exception)
            logger.info(f"Next button gone, pagination done ({len(images)} images)")
            break
        

    return images


def _close_dialog(driver):
    """关闭弹框"""
    #document.querySelectorAll('div[role="button"] svg[aria-label="关闭"]')[0].parentElement.click()
    # try:
    close_btn = driver.find_elements(
        By.XPATH,
        "//div[@role='button']//svg[@aria-label='关闭']"
    )
    driver.execute_script("""
        var els = document.querySelectorAll('div[role="button"] svg[aria-label="关闭"]');
        if (els.length) els[0].parentElement.click();
    """)

    # close_btn.parentElement.click()
    # time.sleep(1)
    return
    # except Exception:
    #     pass
    # 按 Escape 关闭
    # try:
    #     WebDriverWait(driver, 3).until(
    #         EC.presence_of_element_located((By.TAG_NAME, "body"))
    #     ).send_keys(Keys.ESCAPE)
    #     time.sleep(1)
    # except Exception:
    #     pass


# def _extract_images_from_post_page(driver) -> List[str]:
#     """在帖子页提取所有图片 URL（备选方案）"""
#     images = []
#     try:
#         imgs = driver.find_elements(
#             By.XPATH,
#             "//div[@role='presentation']//img[contains(@src, 'cdninstagram.com')]"
#         )
#         if not imgs:
#             imgs = driver.find_elements(
#                 By.XPATH,
#                 "//div[@role='button']//img[contains(@src, 'cdninstagram.com')]"
#             )
#         for img in imgs:
#             src = img.get_attribute("src")
#             if src and "profile" not in src and "avatar" not in src:
#                 if src not in images:
#                     images.append(src)

#         # 多图翻页
#         if len(images) > 1:
#             while True:
#                 try:
#                     nbtn = driver.find_element(
#                         By.XPATH, "//button[contains(@class, '_afxw')]"
#                     )
#                     nbtn.click()
#                     time.sleep(1.5)
#                     new_imgs = driver.find_elements(
#                         By.XPATH,
#                         "//div[@role='presentation']//img[contains(@src, 'cdninstagram.com')]"
#                     )
#                     for img in new_imgs:
#                         src = img.get_attribute("src")
#                         if src and "profile" not in src and src not in images:
#                             images.append(src)
#                 except Exception:
#                     break
#     except Exception:
#         pass
#     return images


# -----------------------------------------------------------
# 核心：滚动网格 + 处理每个帖子
# -----------------------------------------------------------

def _navigate_to_user(driver, user_id, retries=3) -> bool:
    """Navigate to user profile page with verification and retry"""
    for attempt in range(retries):
        driver.get(f"https://www.instagram.com/{user_id}/")
        time.sleep(5)

        current = driver.current_url
        if "login" in current:
            logger.warning(f"Redirected to login (attempt {attempt+1}), reloading cookies")
            _load_saved_cookies(driver)
            time.sleep(2)
            continue
        if user_id.lower() in current.lower():
            logger.info(f"On profile: {user_id}")
            return True

        logger.warning(f"Unexpected URL: {current} (attempt {attempt+1})")
        time.sleep(2)

    logger.error(f"Failed to navigate to {user_id} after {retries} attempts")
    return False


def _crawl_user(user_id: str, incremental: bool = False) -> int:
    _start_heartbeat()
    driver = _get_driver()
    processed = 0

    if not _navigate_to_user(driver, user_id):
        logger.error("Navigation failed, aborting crawl")
        return 0

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//article"))
        )
    except Exception:
        logger.warning("Page load timeout")

    logger.info(f"Page loaded (checking links...)")

    # 全量抓取续跑游标：指向上次已处理的最后一个帖子 URL
    cursor_url = None if incremental else _get_cursor_url(user_id)
    cursor_found = not cursor_url  # 无游标 → 从头开始
    if cursor_url:
        logger.info(f"Resume full crawl, fast-forward past cursor")

    tq = TaskQueue()
    tq.redis = _queue_redis()
    logger.info("Entering scroll loop...")

    seen_urls = set()
    no_new = 0
    same_height = 0
    prev_height = 0
    since_cursor_save = 0

    # 查一次 star_id（全量任务内不变）
    star_id = _lookup_star_id(user_id)
    if star_id is None:
        logger.warning(f"No star_id found for {user_id}, DB insert disabled")

    for scroll_idx in range(500):
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href, '/p/') or contains(@href, '/reel/')]",
        )
        logger.info(f"Scroll {scroll_idx+1}: {len(links)} links on page")
        new_found = 0

        link_idx = 0
        while link_idx < len(links):
            try:
                href = links[link_idx].get_attribute("href")
            except Exception:
                link_idx += 1
                continue
            if not href:
                link_idx += 1
                continue
            clean = href.split("?")[0]

            # ---- 续跑：快进到游标位置 ----
            if not cursor_found:
                if clean == cursor_url:
                    cursor_found = True
                    logger.info("Found cursor, resuming crawl")
                link_idx += 1
                continue

            if clean in seen_urls:
                link_idx += 1
                continue
            seen_urls.add(clean)

            post_id = _extract_post_id(clean)
            if not post_id:
                link_idx += 1
                continue

            # 跳过视频
            if "/reel/" in clean or _is_video(links[link_idx]):
                link_idx += 1
                continue

            # 跳过已处理（全量续跑由游标控制位置，不跳过以防标记过但未下载）
            if cursor_url is None and _is_processed(user_id, post_id):
                link_idx += 1
                continue

            # ---- 提取图片 ----
            image_urls: List[str] = []
            dom_changed = False

            if _is_carousel(links[link_idx]):
                image_urls = _extract_carousel_images(driver, links[link_idx])
                # dom_changed = True  # 弹框操作导致 DOM 变化，后续 link 引用失效
            else:
                thumb = _extract_grid_thumbnail(links[link_idx])
                if thumb:
                    image_urls = [thumb]

            # if not image_urls:
            #     _mark_processed(user_id, post_id)
            #     if dom_changed:
            #         # 弹框已关闭，DOM 恢复；重新拉取链接，继续处理同页其他帖子
            #         links = driver.find_elements(
            #             By.XPATH,
            #             "//a[contains(@href, '/p/') or contains(@href, '/reel/')]",
            #         )
            #         link_idx = 0
            #     else:
            #         link_idx += 1
            #     continue

            # ---- 写入 DB + 发下载子任务 ----
            for idx, img_url in enumerate(image_urls, 1):
                ext = ".png" if ".png" in img_url else ".jpg"
                filename = f"{post_id}_{idx:04d}{ext}"
                check_code = hashlib.md5(filename.encode()).hexdigest()
                batch = f"/{user_id}/p/{post_id}/"
                if star_id:
                    try:
                        db_id = _insert_star_instagram(star_id, f"image/{star_id}/{check_code}{ext}", batch, check_code)
                        save_path = f"image/{star_id}/{check_code}{ext}"
                    except Exception as e:
                        logger.error(f"DB insert failed: {e}")
                        db_id = None
                        save_path = f"{user_id}/{filename}"
                else:
                    db_id = None
                    save_path = f"{user_id}/{filename}"
                # db_id=0 表示 check_code 已存在，跳过下载
                if db_id == 0:
                    continue
                tq.enqueue(
                    f"dl:{'ig'}", "sub_download_image",
                    img_url, save_path, db_id, "ig", user_id,
                )

            _mark_processed(user_id, post_id)
            processed += len(image_urls)
            new_found += 1

            # 每处理 20 个帖子保存一次游标
            if not incremental:
                since_cursor_save += 1
                if since_cursor_save >= 20:
                    _save_cursor(user_id, clean)
                    since_cursor_save = 0

            # 短延迟避免检测
            time.sleep(0.5)

            # if dom_changed:
            #     # 弹框已关闭，DOM 恢复；重新拉取链接，继续处理同页其他帖子
            #     links = driver.find_elements(
            #         By.XPATH,
            #         "//a[contains(@href, '/p/') or contains(@href, '/reel/')]",
            #     )
            #     link_idx = 0
            # else:
            #     link_idx += 1

        if new_found:
            logger.info(
                f"Scroll {scroll_idx+1}: +{new_found} posts ({processed} images)"
            )
            no_new = 0
        else:
            no_new += 1
            # 增量模式：连滚 5 次无新帖 = 已碰到全量边界，停止
            if incremental and no_new >= 5:
                logger.info("No new posts for 5 scrolls, boundary reached")
                break
            # 全量模式：连滚 20 次无新帖 = 到底了
            if not incremental and no_new >= 20:
                logger.info("No new posts for 20 scrolls, stopping")
                break
        # 滚动
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
        logger.info(f"All posts already processed, cursor cleared for {user_id}")

    logger.info(f"Crawl done for {user_id}, {processed} images")
    return processed


# -----------------------------------------------------------
# 注册任务
# -----------------------------------------------------------

@register_task("ig_full_crawl")
def ig_full_crawl(user_id: str) -> str:
    result = f"full crawl: {_crawl_user(user_id, incremental=False)} images"
    # 全量完成后自动投增量任务，后续无需手动加
    tq = TaskQueue()
    tq.redis = _queue_redis()
    tq.enqueue("crawl:ig:incr", "ig_incremental_crawl", user_id)
    logger.info(f"Auto-enqueued incremental task for {user_id}")
    return result

@register_task("ig_incremental_crawl")
def ig_incremental_crawl(user_id: str) -> str:
    return f"incremental crawl: {_crawl_user(user_id, incremental=True)} images"


# -----------------------------------------------------------
# CLI
# -----------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("full", "incr", "all"), default="all",
                        help="full=全量抓取, incr=增量抓取, all=两者 (默认)")
    opt_args = parser.parse_args()

    if opt_args.mode == "full":
        queue_names = ["crawl:ig:full"]
        worker_id = "ig-crawler-full"
    elif opt_args.mode == "incr":
        queue_names = ["crawl:ig:incr"]
        worker_id = "ig-crawler-incr"
    else:
        queue_names = ["crawl:ig:full", "crawl:ig:incr"]
        worker_id = "ig-crawler"

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

    # 启动时自动将死信队列任务重新入队
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

    logger.info(f"IG crawler worker ready (queues: {', '.join(queue_names)})")
    worker.start()
    _close_driver()


if __name__ == "__main__":
    main()
