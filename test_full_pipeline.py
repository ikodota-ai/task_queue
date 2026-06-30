"""
完整模拟测试：couple → 入队 → 模拟爬取 → 翻译 → 入库

用法:
  python test_full_pipeline.py          # 完整流程（含真实 DeepSeek + DB）
  python test_full_pipeline.py --crawl  # 真实 Chrome 爬取 1 个用户
"""

import json, os, sys, time, uuid
sys.path.insert(0, ".")

import redis
from config import cfg

# ---- 测试用户（选一个 couple star）----
TEST_USER = "kiraanaaq"   # couple_id=30, star_id=1058, 有 recent posts

def step1_show_couple_stars():
    """步骤1: 展示 couple stars 的 X 账号"""
    print("=" * 60)
    print("Step 1: Couple stars → X accounts")
    print("=" * 60)
    os.system(f"{sys.executable} couple_timeline_enqueue.py 2>&1 | tail -20")

def step2_enqueue_one():
    """步骤2: 只入队 1 个用户"""
    print("\n" + "=" * 60)
    print(f"Step 2: Enqueue 1 user ({TEST_USER})")
    print("=" * 60)

    qr = redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True,
    )

    # 清除旧任务
    for item in qr.lrange("queue:crawl:x:timeline", 0, -1):
        try:
            d = json.loads(item)
            if d.get("args") and d["args"][0] == TEST_USER:
                qr.lrem("queue:crawl:x:timeline", 0, item)
        except: pass

    task_data = {
        "task_id": str(uuid.uuid4()),
        "func_name": "x_timeline_crawl",
        "args": [TEST_USER, 0, 3, 604800],  # max 3 new posts, 7 days
        "kwargs": {},
        "queue_name": "crawl:x:timeline",
        "retry_count": 0,
        "enqueued_at": time.time(),
    }
    qr.rpush("queue:crawl:x:timeline", json.dumps(task_data))
    print(f"  ✅ 1 task enqueued: {TEST_USER} (max_new=3, cutoff=7d)")
    print(f"  Queue length: {qr.llen('queue:crawl:x:timeline')}")

def step3_worker_instructions():
    """步骤3: 启动 worker 说明"""
    print("\n" + "=" * 60)
    print("Step 3: Start worker (另开终端)")
    print("=" * 60)
    print(f"  venv/bin/python x_crawler.py --mode timeline")
    print(f"\n  任务 {TEST_USER} 已在 crawl:x:timeline 队列中等待")
    print(f"  Worker 启动后会自动取走并执行")

def step4_article_pipeline_mock():
    """步骤4: 模拟文章管线（不启动 Chrome）"""
    print("\n" + "=" * 60)
    print("Step 4: Article pipeline (mock crawl, real AI+DB)")
    print("=" * 60)

    from x_crawler import (
        _translate_text, _match_movie, _build_tweet_html,
        _insert_tweet_article, _load_star_couple_map, _find_couple_ids,
        _get_star_avatar,
    )

    test_tweet = {
        "user_id": "kiraanaaq",
        "tweet_id": "2024510277421838555",
        "star_id": 1058,
        "text": (
            "ลาหล่ะค้าบบบบบบบบ "
            "#ศักดินาวิทยาลัยตอนจบ"
        ),
        "image_paths": ["x/image/1058/test.jpg"],
        "hashtags": ["ศักดินาวิทยาลัยตอนจบ"],
        "post_ts": int(time.time()) - 3600,
    }

    print(f"\n  User: @{test_tweet['user_id']}  star_id={test_tweet['star_id']}")
    print(f"  原文: {test_tweet['text']}")

    # 1. Couple 映射
    couples = _find_couple_ids(test_tweet["star_id"])
    print(f"\n  [1] Couples: {[c['name'] for c in couples]}")

    # 2. AI 翻译
    print(f"\n  [2] Translating...")
    t0 = time.time()
    trans = _translate_text(test_tweet["text"])
    print(f"      耗时: {time.time() - t0:.1f}s")
    print(f"      中文: {trans['cn_text']}")
    print(f"      Tags:  {trans['hashtags']}")
    if trans["error"]:
        print(f"      ⚠️ Error: {trans['error']}")

    # 3. 电影匹配
    all_tags = list(set(test_tweet["hashtags"] + trans.get("hashtags", [])))
    movie = _match_movie(all_tags)
    print(f"\n  [3] Movie: {movie}")

    # 4. Avatar
    avatar = _get_star_avatar(test_tweet["star_id"])
    print(f"\n  [4] Avatar: {avatar}")

    # 5. HTML
    html = _build_tweet_html(trans["cn_text"], test_tweet["text"], test_tweet["image_paths"])
    print(f"\n  [5] HTML:")
    for line in html.split("\n"):
        print(f"      {line[:120]}")

    # 6. 入库
    print(f"\n  [6] Insert into la_article...")
    article_ids = _insert_tweet_article(
        test_tweet["user_id"], test_tweet["tweet_id"],
        test_tweet["star_id"], test_tweet["text"],
        test_tweet["image_paths"], all_tags, test_tweet["post_ts"],
        avatar_url=avatar,
    )
    if article_ids:
        print(f"      ✅ Inserted: ids={article_ids}")
        from x_crawler import _get_db
        db = _get_db()
        cur = db.cursor()
        for aid in article_ids:
            cur.execute("SELECT id, cpid, title, movies, image, url FROM la_article WHERE id=%s", (aid,))
            r = cur.fetchone()
            print(f"      Verify: id={r[0]} cpid={r[1]} title={r[2][:60]} movies={r[3]} image={r[4][:60]} url='{r[5]}'")
    else:
        print(f"      ⚠️ No articles inserted")

    print("\n" + "=" * 60)
    print("Pipeline test done!")
    print("=" * 60)

def step5_crawl_real():
    """步骤5: 真实 Chrome 爬取（需要 --crawl 参数）"""
    print("\n" + "=" * 60)
    print("Step 5: REAL Chrome crawl")
    print("=" * 60)
    from x_crawler import x_timeline_crawl
    result = x_timeline_crawl(TEST_USER, max_new_posts=3, cutoff_seconds=604800)
    print(f"\n  结果: {result}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--crawl", action="store_true", help="真实 Chrome 爬取")
    parser.add_argument("--enqueue-only", action="store_true", help="仅入队")
    args = parser.parse_args()

    if args.crawl:
        step2_enqueue_one()
        step5_crawl_real()
    elif args.enqueue_only:
        step1_show_couple_stars()
        step2_enqueue_one()
        step3_worker_instructions()
    else:
        # 默认：模拟文章管线
        step1_show_couple_stars()
        step4_article_pipeline_mock()
