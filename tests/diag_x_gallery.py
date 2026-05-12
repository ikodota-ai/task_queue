"""
X Gallery DOM 探测脚本 — 点击推文弹窗后 dump HTML 结构
"""
import json, time
import sys
sys.path.insert(0, ".")
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from config import cfg

TARGET = "kiraanaaq"

opt = Options()
opt.binary_location = cfg["ig_chrome_path"]
opt.add_argument("--no-sandbox")
opt.add_argument("--disable-dev-shm-usage")
opt.add_argument("--window-size=1920,1080")
opt.add_argument(f"--user-data-dir=/tmp/chrome_x_diag_{int(time.time())}")

driver = webdriver.Chrome(service=Service(executable_path=cfg["ig_chromedriver_path"]), options=opt)

try:
    # 1. 注入 auth_token
    print("1. 登录 X ...")
    driver.get("https://x.com")
    time.sleep(3)
    driver.add_cookie({
        "name": "auth_token", "value": cfg["x_auth_token"],
        "domain": ".x.com", "path": "/",
    })
    driver.refresh()
    time.sleep(6)
    print(f"   当前 URL: {driver.current_url}")

    # 2. 访问 media 页
    url = f"https://x.com/{TARGET}/media"
    print(f"2. 访问 {url}")
    driver.get(url)
    time.sleep(5)

    # 3. 找推文链接
    links = driver.find_elements(By.XPATH, "//a[contains(@href, '/status/')]")
    print(f"3. 找到 {len(links)} 个推文链接")

    # 4. 点第一个
    if links:
        first = links[0]
        href = first.get_attribute("href")
        print(f"4. 点击: {href}")
        first.click()
        time.sleep(3)

        # 5. dump DOM 片段
        print("\n" + "="*60)
        print("5. 弹窗 HTML 结构")
        print("="*60)

        # 查找弹窗容器
        containers = [
            ("role=dialog", "//div[@role='dialog']"),
            ("aria-label=Image", "//div[@aria-label='Image']"),
            ("photo modal", "//div[@data-testid='tweetPhoto']/ancestor::div[4]"),
            ("swipe-to-dismiss", "//div[@data-testid='swipe-to-dismiss']"),
        ]
        for name, xpath in containers:
            try:
                el = driver.find_element(By.XPATH, xpath)
                html = el.get_attribute("outerHTML")
                print(f"\n--- {name} ({len(html)} chars) ---")
                print(html[:3000])
                print("...")
            except Exception as e:
                print(f"\n--- {name}: NOT FOUND ({e}) ---")

        # 6. 查找翻页按钮
        print("\n" + "="*60)
        print("6. 翻页按钮")
        print("="*60)
        btn_queries = [
            "//button[@aria-label='Next']",
            "//button[contains(@aria-label, 'next')]",
            "//div[@role='button' and contains(@aria-label, 'Next')]",
            "//button[contains(@data-testid, 'arrow')]",
            "//div[@aria-label and contains(@aria-label, 'Next')]",
        ]
        for q in btn_queries:
            try:
                btns = driver.find_elements(By.XPATH, q)
                if btns:
                    for b in btns[:2]:
                        print(f"  FOUND: {q}  aria={b.get_attribute('aria-label')}  class={b.get_attribute('class')[:80]}")
                else:
                    print(f"  none: {q}")
            except:
                pass

        # 7. 查找所有 button
        print("\n" + "="*60)
        print("7. 弹窗内所有 button")
        print("="*60)
        try:
            dialog = driver.find_element(By.XPATH, "//div[@role='dialog'] | //div[@aria-label='Image']")
            btns = dialog.find_elements(By.TAG_NAME, "button")
            for b in btns[:10]:
                aria = b.get_attribute("aria-label") or ""
                dti = b.get_attribute("data-testid") or ""
                cls = b.get_attribute("class") or ""
                print(f"  aria='{aria[:60]}'  dti='{dti[:40]}'  cls='{cls[:60]}'")
        except Exception as e:
            print(f"  error: {e}")

        # 8. 尝试点击 Next 并抓图
        print("\n" + "="*60)
        print("8. 尝试翻页抓图")
        print("="*60)
        for slide in range(5):
            imgs = driver.find_elements(By.XPATH, "//div[@aria-label='Image']//img | //div[@role='dialog']//img")
            print(f"  slide {slide+1}: {len(imgs)} visible images")
            for i, img in enumerate(imgs[:3]):
                src = img.get_attribute("src") or ""
                alt = img.get_attribute("alt") or ""
                print(f"    [{i}] src={src[:120]}")
                print(f"        alt='{alt[:80]}'")

            try:
                next_btn = driver.find_element(By.XPATH, "//button[@aria-label='Next']")
                next_btn.click()
                time.sleep(1.5)
            except:
                print("  No Next button, done")
                break

    time.sleep(2)

finally:
    driver.quit()
