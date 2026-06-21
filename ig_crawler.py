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
import os
import re
import signal
import sys
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
    """根据 Instagram 用户名查 la_star_info.id"""
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT id FROM la_star_info WHERE x = %s",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        pass  # 复用连接，不关闭

def _mark_full_done_db(user_id: str, maxpage: int, platform: str = "ig"):
    """全量完成时写入 la_star_info"""
    db = _get_db()
    try:
        cur = db.cursor()
        col_done = f"{platform}_full_done"
        col_maxpage = f"{platform}_maxpage"
        cur.execute(
            f"UPDATE la_star_info SET {col_done}=1, {col_maxpage}=%s WHERE x=%s",
            (maxpage, user_id),
        )
        db.commit()
        logger.info(f"DB updated: la_star_info.{col_done}=1 for {user_id}")
    except Exception as e:
        logger.error(f"Failed to update DB full_done for {user_id}: {e}")


def _is_full_crawl_done(user_id: str) -> bool:
    return _state_redis().hget(_skey(user_id), "full_done") == "1"

def _insert_star_instagram(star_id: int, image: str, batch: str, check_code: str, post_ts: int = None) -> int:
    """插入 la_star_instagram，返回自增 ID。post_ts 为帖子实际时间戳，为空则用当前时间。"""
    db = _get_db()
    try:
        cur = db.cursor()
        cur.execute(
            f"INSERT IGNORE INTO {cfg['table_prefix']}star_instagram "
            "(star_id, check_code, image, batch, status, source, create_time) "
            "VALUES (%s, %s, %s, %s, 'N', 'ins', %s)",
            (star_id, check_code, image, batch, post_ts or int(time.time())),
        )
        db.commit()
        return cur.lastrowid
    finally:
        pass  # 复用连接，不关闭


# -----------------------------------------------------------
# Chrome 单例（Worker 生命周期内复用，避免反复启动）
# -----------------------------------------------------------

_driver = None
_driver_lock = threading.Lock()
_user_data_dir = None   # 当前 Chrome user-data-dir
_service_pid = None      # chromedriver 进程 PID，用于跨平台杀进程树
_stop_requested = False  # 优雅中断：Ctrl+C 时置 True，_do_crawl 检测后提前退出
_force_headless = None   # None=读 .env, True=强制headless, False=强制有头


def _kill_driver_tree():
    """杀掉 chromedriver 及其所有 Chrome 子进程（跨平台）。"""
    global _service_pid
    pid = _service_pid
    _service_pid = None
    if not pid:
        return
    try:
        if sys.platform == "win32":
            import subprocess as _sp
            _sp.run(["taskkill", "/F", "/T", "/PID", str(pid)],
                    capture_output=True, timeout=10)
        else:
            # Linux / macOS: 先找子进程，再杀父进程
            import subprocess as _sp
            try:
                r = _sp.run(["pgrep", "-P", str(pid)],
                           capture_output=True, text=True, timeout=5)
                child_pids = [int(p) for p in r.stdout.strip().split() if p.isdigit()]
            except Exception:
                child_pids = []
            for p in child_pids + [pid]:
                try:
                    os.kill(p, signal.SIGTERM)
                except OSError:
                    pass
            time.sleep(1.0)
            for p in child_pids + [pid]:
                try:
                    os.kill(p, signal.SIGKILL)
                except OSError:
                    pass
    except Exception as e:
        logger.warning(f"Failed to kill driver process tree (PID={pid}): {e}")


def _rm_old_dir(user_data_dir: str):
    """删除旧的 user-data-dir。"""
    if not user_data_dir:
        return
    try:
        import shutil as _sh
        _sh.rmtree(user_data_dir, ignore_errors=True)
        logger.info(f"Cleaned up old Chrome dir: {user_data_dir}")
    except Exception:
        pass


def _cleanup_stale_dirs():
    """启动时一次性清理：删除已死进程残留的 chrome_ig_* 目录。"""
    import glob as _g, tempfile
    my_pid = os.getpid()
    tmpdir = tempfile.gettempdir()
    for old_dir in _g.glob(os.path.join(tmpdir, "chrome_ig_*")):
        try:
            parts = os.path.basename(old_dir).split("_")
            pid = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
        except (ValueError, IndexError):
            pid = 0
        if pid == my_pid:
            continue
        if pid and pid != 0:
            try:
                os.kill(pid, 0)       # Unix: 进程存活 → 跳过
                continue
            except OSError:
                pass                  # 进程已死 → 清理
        _rm_old_dir(old_dir)


def _get_driver():
    """获取或创建 Chrome 实例（已登录），崩溃后自动重建并清理旧进程"""
    global _driver, _user_data_dir
    with _driver_lock:
        if _driver is not None:
            try:
                _driver.current_url  # 探测连接是否存活
            except Exception:
                logger.warning("Chrome connection lost, recreating driver")
                old_dir = _user_data_dir
                try:
                    _driver.quit()
                except Exception:
                    pass
                _driver = None
                _kill_driver_tree()
                _rm_old_dir(old_dir)
        if _driver is None:
            _cleanup_stale_dirs()     # 首次/重建时清理历史残留
            _driver = _setup_chrome()
            try:
                _ensure_login(_driver)
            except Exception:
                old_dir = _user_data_dir
                try:
                    _driver.quit()
                except Exception:
                    pass
                _driver = None
                _kill_driver_tree()
                _rm_old_dir(old_dir)
                raise
    return _driver


def _reset_driver():
    """强制重置 driver（Chrome 崩溃后调用），只清理自己的进程和目录"""
    global _driver, _user_data_dir
    with _driver_lock:
        old_dir = _user_data_dir
        if _driver:
            try:
                _driver.quit()
            except Exception:
                pass
            _driver = None
        _kill_driver_tree()
        _rm_old_dir(old_dir)


def _close_driver():
    _reset_driver()


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
    global _user_data_dir, _service_pid

    cp = cfg["ig_chrome_path"]
    cdp = cfg["ig_chromedriver_path"]
    if not cp or not cdp:
        raise RuntimeError("IG_CHROME_PATH and IG_CHROMEDRIVER_PATH must be set")

    # 清理同进程上一次可能残留的旧目录（进程 PID 不变，但 driver 可能被重建）
    import tempfile, glob as _glob, shutil as _shutil
    my_pid = os.getpid()
    tmpdir = tempfile.gettempdir()
    for old in _glob.glob(os.path.join(tmpdir, f"chrome_ig_{my_pid}_*")):
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
    opt.add_argument("--disable-component-extensions-with-background-pages")
    opt.add_argument("--disable-ipc-flooding-protection")
    opt.add_argument("--window-size=1920,1080")
    # 参数优先 → 环境变量兜底
    use_headless = headless
    if _force_headless is not None:
        use_headless = _force_headless
    elif os.getenv("CHROME_HEADLESS", "") in ("1", "true", "yes"):
        use_headless = True

    if use_headless:
        opt.add_argument("--headless=new")
        opt.add_argument("--disable-gpu")
        opt.add_argument("--no-zygote")
        opt.add_argument("--disable-features=VizDisplayCompositor,TranslateUI")
        opt.add_argument("--no-first-run")
        opt.add_argument("--disable-default-apps")
    _user_data_dir = os.path.join(tempfile.gettempdir(),
                                   f"chrome_ig_{os.getpid()}_{int(time.time())}")
    opt.add_argument(f"--user-data-dir={_user_data_dir}")

    driver = webdriver.Chrome(service=Service(executable_path=cdp), options=opt)
    _service_pid = driver.service.process.pid
    logger.info(f"Chrome started, user-data-dir={_user_data_dir}, service_pid={_service_pid}")
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
    """先试 cookie 恢复，失败则账号密码登录，均失败则抛异常提示用 cookies"""
    # 1. 优先 cookie 恢复
    if _load_saved_cookies(driver):
        return

    u, p = cfg["ig_username"], cfg["ig_password"]
    if not u or not p:
        raise RuntimeError(
            "Cookie 已过期且未配置 IG_USERNAME/IG_PASSWORD，"
            "请用 import_cookies.py 导入新的 cookies"
        )

    # 2. 降级为密码登录
    logger.info("Cookie expired, logging in with password...")
    try:
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
    except Exception as e:
        raise RuntimeError(
            f"密码登录失败 ({e})，Instagram 可能要求手机验证。"
            "请用浏览器手动登录后导出 cookies，再用 import_cookies.py 导入"
        )

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


# -----------------------------------------------------------
# 点击弹框 → 提取图片 + 帖子时间戳
# -----------------------------------------------------------

def _extract_carousel_images(driver, link) -> tuple:
    """
    点击多图帖子，在弹框中翻页提取所有图片 URL 和帖子时间戳。
    返回 (images: List[str], post_ts: int or None)
    """
    grid_url = driver.current_url

    # 点击帖子
    try:
        link.click()
    except Exception:
        return [], None
    time.sleep(0.1)

    # 情形 A：弹框模式（URL 不变，dialog 出现）
    images, post_ts = _extract_from_dialog(driver)
    _close_dialog(driver)
    return images, post_ts

    # 情形 B：导航到了帖子页（旧版 Instagram）
    # logger.info("Navigated to post page, extracting there")
    # images = _extract_images_from_post_page(driver)
    # driver.back()
    # time.sleep(2)
    # return images


def _is_valid_image(url: str) -> bool:
    """过滤 Instagram 占位图和无效应答"""
    if not url:
        return False
    bad = ("s150x150", "profile", "null.jpg", "rsrc.php")
    for b in bad:
        if b in url:
            return False
    return True


def _extract_post_timestamp(driver):
    """从弹框中提取帖子日期，转为 Unix 时间戳。失败返回 None。"""
    try:
        t_el = driver.find_element(
            By.XPATH, "(//article[@role='presentation']//div//a/span/time)[1]"
        )
        dt_str = t_el.get_attribute("datetime")
        if dt_str:
            # ISO 8601: "2024-01-15T10:30:00.000Z"
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
    except Exception:
        pass
    return None


def _extract_from_dialog(driver) -> tuple:
    """从弹框中提取所有图片和帖子时间戳。
    返回 (images: List[str], post_ts: int or None)
    """
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='dialog']"))
        )
    except Exception:
        return [], None

    images = []
    seen = set()

    post_ts = _extract_post_timestamp(driver)

    img = driver.find_elements(By.XPATH,"//article[@role='presentation']//img[contains(@src, 'cdninstagram.com')]")
    img_url = None
    for attempt in range(3):
        try:
            imgs = driver.find_elements(By.XPATH,"//article[@role='presentation']//img[contains(@src, 'cdninstagram.com')]")
            if imgs:
                img_url = imgs[0].get_attribute("src")
            break
        except Exception:
            time.sleep(0.5)
    if _is_valid_image(img_url):
        seen.add(img_url)
        images.append(img_url)
    logger.info(f"Carousel opened, initial images: {len(images)}")
    time.sleep(2)
    while True:
        # 依次尝试多种语言/地区的 Next 按钮
        next_btn = None
        for selector in [
            '//button[contains(@class, "_afxw")]',
            '//button[@aria-label="下一步"]',
            '//button[@aria-label="Next"]',
        ]:
            btns = driver.find_elements(By.XPATH, selector)
            if "_afxw" in selector:
                btns = [b for b in btns if b.is_displayed()]
                if len(btns) >= 2:
                    next_btn = btns[1]  # 第二个 _afxw 通常是 Next
            elif btns:
                next_btn = btns[0]
            if next_btn:
                break

        if not next_btn:
            logger.info(f"Next button gone, pagination done ({len(images)} images)")
            break

        try:
            next_btn.click()
        except Exception:
            logger.info(f"Next button stale, pagination done ({len(images)} images)")
            break

        time.sleep(1)
        try:
            new_images = driver.find_elements(By.XPATH,"//article[@role='presentation']//img[contains(@src, 'cdninstagram.com')]")
            for img in new_images:
                    img_url = img.get_attribute("src")
                    if _is_valid_image(img_url) and img_url not in seen:
                        seen.add(img_url)
                        images.append(img_url)
        except:
            break

    return images, post_ts


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


def _extract_images_from_post_page(driver) -> List[str]:
    """在帖子页提取所有图片 URL（备选方案）"""
    images = []
    try:
        imgs = driver.find_elements(
            By.XPATH,
            "//div[@role='presentation']//img[contains(@src, 'cdninstagram.com')]"
        )
        if not imgs:
            imgs = driver.find_elements(
                By.XPATH,
                "//div[@role='button']//img[contains(@src, 'cdninstagram.com')]"
            )
        for img in imgs:
            src = img.get_attribute("src")
            if _is_valid_image(src) and "avatar" not in src:
                if src not in images:
                    images.append(src)

        # 多图翻页
        if len(images) > 1:
            while True:
                try:
                    nbtn = driver.find_element(
                        By.XPATH, "//button[contains(@class, '_afxw')]"
                    )
                    nbtn.click()
                    time.sleep(1.5)
                    new_imgs = driver.find_elements(
                        By.XPATH,
                        "//div[@role='presentation']//img[contains(@src, 'cdninstagram.com')]"
                    )
                    for img in new_imgs:
                        src = img.get_attribute("src")
                        if _is_valid_image(src) and src not in images:
                            images.append(src)
                except Exception:
                    break
    except Exception:
        pass
    return images


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


def _crawl_user(user_id: str, incremental: bool = False, maxpage: int = 500) -> int:
    _start_heartbeat()

    lock_key = f"ig:{user_id}:crawling"
    if not _state_redis().set(lock_key, "1", nx=True, ex=7200):
        logger.warning(f"{user_id} is already being crawled by another worker, skipping")
        return 0

    try:
        return _do_crawl(user_id, incremental, maxpage)
    finally:
        _state_redis().delete(lock_key)


def _do_crawl(user_id: str, incremental: bool = False, maxpage: int = 500) -> int:

    state = _state_redis().hgetall(_skey(user_id))

    # 全量已完成检查：reached_bottom=1 直接跳过；否则 full_done=1 且 maxpage 够大才跳过
    if not incremental:
        saved_maxpage = int(state.get("last_maxpage") or state.get("maxpage") or 0)
        if state.get("reached_bottom") == "1":
            logger.info(f"Full crawl for {user_id}: reached_bottom=1, fully done, skipping")
            return 0
        if state.get("full_done") == "1" and saved_maxpage >= maxpage:
            logger.info(f"Full crawl for {user_id}: full_done=1, last_maxpage {saved_maxpage} >= target {maxpage}, skipping")
            return 0
        if state.get("full_done") == "1":
            logger.info(f"Full crawl for {user_id}: last_maxpage {saved_maxpage} < target {maxpage}, resuming deeper crawl")
        cursor = _get_cursor_url(user_id)
        if not cursor and _state_redis().scard(_pkey(user_id)) > 0:
            logger.info(f"Full crawl for {user_id}: no cursor, will skip processed posts")

    try:
        driver = _get_driver()
    except Exception:
        _reset_driver()
        raise
    processed = 0

    if not _navigate_to_user(driver, user_id):
        raise RuntimeError(f"Navigation to {user_id} failed (cookie expired or network error)")

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

    posts_done = int(state.get("posts_done", "0")) if not incremental else 0
    scroll_idx = 0
    while scroll_idx < maxpage:
        if _stop_requested:
            logger.info("Stop requested, aborting crawl loop")
            break
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href, '/p/') or contains(@href, '/reel/')]",
        )
        # 记录本次滚动初始可见的 hrefs，只处理这些，跳过懒加载新增的
        scroll_hrefs = set()
        for _l in links:
            try:
                _h = _l.get_attribute("href")
                if _h:
                    scroll_hrefs.add(_h.split("?")[0])
            except Exception:
                pass
        logger.info(f"Scroll {scroll_idx+1}/{maxpage}: {len(scroll_hrefs)} links on page")
        new_found = 0

        link_idx = 0
        while link_idx < len(links):
            if _stop_requested:
                logger.info("Stop requested, aborting post processing")
                break
            try:
                href = links[link_idx].get_attribute("href")
            except Exception:
                link_idx += 1
                continue
            if not href:
                link_idx += 1
                continue
            clean = href.split("?")[0]

            # 跳过懒加载新增的 link（不在本次滚动初始集合中）
            if clean not in scroll_hrefs:
                link_idx += 1
                continue

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

            # 统一走弹窗：单图和多图都用 dialog，才能拿到时间戳
            image_urls, post_ts = _extract_carousel_images(driver, links[link_idx])
            dom_changed = True  # 弹框操作导致 DOM 变化，后续 link 引用失效

            if not image_urls:
                _mark_processed(user_id, post_id)
                if dom_changed:
                    links = driver.find_elements(
                        By.XPATH,
                        "//a[contains(@href, '/p/') or contains(@href, '/reel/')]",
                    )
                    link_idx = 0
                else:
                    link_idx += 1
                continue

            # ---- 写入 DB + 发下载子任务 ----
            for idx, img_url in enumerate(image_urls, 1):
                ext = ".png" if ".png" in img_url else ".jpg"
                filename = f"{post_id}_{idx:04d}{ext}"
                check_code = hashlib.md5(filename.encode()).hexdigest()
                batch = f"/{user_id}/p/{post_id}/"
                if star_id:
                    try:
                        db_id = _insert_star_instagram(star_id, f"ig/image/{star_id}/{check_code}{ext}", batch, check_code, post_ts)
                        save_path = f"ig/image/{star_id}/{check_code}{ext}"
                    except Exception as e:
                        logger.error(f"DB insert failed: {e}")
                        db_id = None
                        save_path = f"ig/{user_id}/{filename}"
                else:
                    db_id = None
                    save_path = f"ig/{user_id}/{filename}"
                # db_id=0 表示 check_code 已存在，跳过下载
                if db_id == 0:
                    db_id = None
                tq.enqueue(
                    f"dl:{'ig'}", "sub_download_image",
                    img_url, save_path, db_id, "ig", user_id,
                )

            _mark_processed(user_id, post_id)
            processed += len(image_urls)
            new_found += 1
            posts_done += 1

            # 每处理 10 个帖子保存一次游标 + posts_done
            if not incremental:
                since_cursor_save += 1
                if since_cursor_save >= 10:
                    _save_cursor(user_id, clean)
                    _state_redis().hset(_skey(user_id), "posts_done", str(posts_done))
                    since_cursor_save = 0

            # 短延迟避免检测
            time.sleep(0.5)

            if dom_changed:
                links = driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href, '/p/') or contains(@href, '/reel/')]",
                )
                link_idx = 0
            else:
                link_idx += 1

        if new_found:
            logger.info(
                f"Scroll {scroll_idx+1}: +{new_found} posts ({processed} images)"
            )
            no_new = 0
        else:
            no_new += 1
            # 增量模式：连滚 5 次无新帖 = 已碰到全量边界，停止
            if incremental and no_new >= 2:
                logger.info("No new posts for 5 scrolls, boundary reached")
                break
        # 滚动
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
            scroll_idx += 1
        prev_height = new_h

    # 已完成，清除进度
    _state_redis().hdel(_skey(user_id), "posts_done")

    # processed==0 且到底 → 可能限流/无帖/私密，标记 blocked 后抛异常进 retry
    if processed == 0 and same_height >= 10:
        if not incremental:
            _state_redis().hset(_skey(user_id), mapping={
                "blocked": "1",
                "blocked_reason": "0 posts (rate-limited)",
                "blocked_at": str(int(time.time())),
            })
        raise RuntimeError(
            f"IG crawl for {user_id}: 0 posts found, likely rate-limited. "
            "Task will retry with exponential backoff.")
    # 增量抓取 0 条是正常的（没新帖），不应标记 blocked
    if processed == 0 and not incremental:
        logger.warning(f"IG crawl for {user_id}: 0 posts found (possibly rate-limited)")
        _state_redis().hset(_skey(user_id), mapping={
            "blocked": "1",
            "blocked_reason": "0 posts (rate-limited)",
            "blocked_at": str(int(time.time())),
        })

    # 全量完成时写入状态（增量不写这些字段）
    actual_pages = scroll_idx + 1
    if same_height >= 10:
        logger.info(f"Reached bottom at page {actual_pages}")
        if not incremental and processed > 0:
            _state_redis().hset(_skey(user_id), mapping={
                "last_maxpage": str(maxpage),
                "full_done": "1",
                "reached_bottom": "1",
            })
            _mark_full_done_db(user_id, maxpage, "ig")
    else:
        logger.info(f"Reached maxpage limit ({maxpage} pages)")
        if not incremental and processed > 0:
            _state_redis().hset(_skey(user_id), mapping={
                "last_maxpage": str(maxpage),
                "full_done": "1",
            })
            _mark_full_done_db(user_id, maxpage, "ig")

    if processed:
        _update_state(user_id, last_scrape_time=time.time(), last_images=str(processed))
    if incremental and processed >= 0:
        r = _state_redis()
        cnt = r.hincrby(_skey(user_id), "incr_count", 1)
        r.hset(_skey(user_id), mapping={
            "incr_last_time": str(int(time.time())),
            "incr_last_images": str(processed),
        })
        logger.info(f"Incremental #{cnt} done for {user_id}")
    elif cursor_url and not incremental:
        _state_redis().hdel(_skey(user_id), "cursor_url")
        logger.info(f"All posts already processed, cursor cleared for {user_id}")

    logger.info(f"Crawl done for {user_id}, {processed} images")
    return processed


# -----------------------------------------------------------
# 注册任务
# -----------------------------------------------------------

@register_task("ig_full_crawl")
def ig_full_crawl(user_id: str, db_task_id: int = None, maxpage: int = None) -> str:
    if maxpage is None:
        maxpage = int(os.getenv("MAX_PAGE", 500))
    count = _crawl_user(user_id, incremental=False, maxpage=maxpage)
    result = f"full crawl: {count} images"
    state = _state_redis().hgetall(_skey(user_id))
    if state.get("full_done") == "1":
        logger.info(f"Full crawl completed for {user_id} (ready for scheduler)")
    else:
        logger.info(f"Full crawl for {user_id} did not reach bottom, skipping incr")
    return result

@register_task("ig_incremental_crawl")
def ig_incremental_crawl(user_id: str, db_task_id: int = None) -> str:
    if not _is_full_crawl_done(user_id):
        msg = f"Skipping incremental for {user_id}: full crawl not done"
        logger.warning(msg)
        return msg

    count = _crawl_user(user_id, incremental=True)
    result = f"incremental crawl: {count} images"
    return result


# -----------------------------------------------------------
# CLI
# -----------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("full", "incr", "all"), default="all",
                        help="full=全量, incr=增量, all=全量+增量")
    parser.add_argument("--headless", dest="headless", action="store_const", const=True,
                        help="强制 headless 模式")
    parser.add_argument("--no-headless", dest="headless", action="store_const", const=False,
                        help="强制有头模式（调试用，覆盖 .env）")
    opt_args = parser.parse_args()

    global _force_headless
    if opt_args.headless is not None:
        _force_headless = opt_args.headless

    if opt_args.mode == "full":
        queue_names = ["crawl:ig:full"]
        worker_id = "ig-crawler-full"
    elif opt_args.mode == "incr":
        queue_names = ["crawl:ig:incr"]
        worker_id = "ig-crawler-incr"
    else:
        queue_names = ["crawl:ig:incr", "crawl:ig:full"]
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
            tq.retry_dead(q, state_redis=_state_redis())

    def shutdown(sig, frame):
        global _stop_requested
        _stop_requested = True
        worker.stop()
        # 不在这里 _close_driver()，否则会杀死爬虫正在使用的 Chrome。
        # 等 _do_crawl 检测 _stop_requested 退出后，worker.start() 返回再清理。

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(f"IG crawler worker ready (queues: {', '.join(queue_names)})")
    worker.start()
    _close_driver()  # worker 退出后安全清理


if __name__ == "__main__":
    main()
