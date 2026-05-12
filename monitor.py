"""
监控面板 — 大屏实时查看抓取/下载动态
用法: python monitor.py (默认 http://0.0.0.0:5000)
"""
import json
import time
from collections import defaultdict
from flask import Flask, jsonify, render_template_string

import pymysql
import redis

from config import cfg

app = Flask(__name__)

_TABLE = cfg["table_prefix"] + "crawl_tasks"


def _get_db():
    return pymysql.connect(
        host=cfg["mysql_host"], port=cfg["mysql_port"],
        user=cfg["mysql_user"], password=cfg["mysql_password"],
        database=cfg["mysql_db"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def _get_queue_redis():
    return redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True,
    )


def _get_state_redis():
    return redis.Redis(
        host=cfg["redis_host"], port=cfg["redis_port"],
        password=cfg["redis_password"], db=cfg["redis_db"],
        decode_responses=True,
    )


@app.route("/api/status")
def api_status():
    db = _get_db()
    qr = _get_queue_redis()
    sr = _get_state_redis()

    # ===== MySQL 任务概览 =====
    cur = db.cursor()
    cur.execute(f"""
        SELECT platform, task_type, status, COUNT(*) as cnt
        FROM {_TABLE} GROUP BY platform, task_type, status
    """)
    task_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for row in cur.fetchall():
        task_stats[row["platform"]][row["task_type"]][row["status"]] = row["cnt"]

    # 最近 10 条任务
    cur.execute(f"""
        SELECT id, platform, task_type, user_id, status,
               DATE_FORMAT(updated_at, '%%m-%%d %%H:%%i') as upd
        FROM {_TABLE} ORDER BY id DESC LIMIT 10
    """)
    recent_tasks = list(cur.fetchall())

    # ===== Redis 队列状态 =====
    queues = {}
    for q in ["crawl:ig:full", "crawl:ig:incr", "crawl:x:full", "crawl:x:incr",
              "dl:ig", "dl:x"]:
        queues[q] = {
            "pending": qr.llen(f"queue:{q}"),
            "processing": len(qr.hgetall(f"processing:{q}")),
            "dead": qr.llen(f"dead:{q}"),
            "retry": qr.zcard(f"retry:{q}"),
        }

    # ===== Worker 心跳 =====
    workers = {}
    for k in qr.keys("worker:heartbeat:*"):
        wid = k.split(":", 2)[2]
        ts = float(qr.get(k) or 0)
        age = int(time.time() - ts)
        workers[wid] = {"alive": age < 90, "last_seen_sec": age}

    # ===== 正在处理的抓取任务 =====
    active_crawls = []
    for q in ["crawl:ig:full", "crawl:ig:incr", "crawl:x:full", "crawl:x:incr"]:
        for tid, expiry in qr.hgetall(f"processing:{q}").items():
            meta = qr.hgetall(f"task_meta:{q}:{tid}")
            args_str = meta.get("args", "[]")
            try:
                args = eval(args_str)
                user_id = args[0] if args else "?"
            except Exception:
                user_id = "?"
            active_crawls.append({
                "queue": q,
                "user_id": user_id,
                "task_id": tid[:8],
                "started": meta.get("enqueued_at", "")[:10],
            })

    # ===== 正在下载的图片 =====
    active_downloads = []
    for q in ["dl:ig", "dl:x"]:
        for tid, expiry in qr.hgetall(f"processing:{q}").items():
            meta = qr.hgetall(f"task_meta:{q}:{tid}")
            args_str = meta.get("args", "[]")
            try:
                args = eval(args_str)
                save_path = args[1] if len(args) > 1 else "?"
            except Exception:
                save_path = "?"
            active_downloads.append({
                "queue": q,
                "save_path": str(save_path)[-60:],
                "task_id": tid[:8],
            })

    # ===== 已处理统计 (业务 Redis) =====
    processed = {}
    for prefix in ["instagram:", "twitter:"]:
        count = 0
        total_posts = 0
        for k in sr.keys(f"{prefix}*:processed"):
            count += 1
            total_posts += sr.scard(k)
        platform = "ig" if "instagram" in prefix else "x"
        processed[platform] = {"users": count, "total_posts": total_posts}

    db.close()

    return jsonify({
        "task_stats": task_stats,
        "recent_tasks": recent_tasks,
        "queues": queues,
        "workers": workers,
        "active_crawls": active_crawls,
        "active_downloads": active_downloads,
        "processed": processed,
        "ts": int(time.time()),
    })


HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="10">
<title>抓取监控面板</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0f1923;color:#c0d0e0;font:14px monospace;padding:15px}
h2{color:#5af;margin:15px 0 8px;border-bottom:1px solid #1a3a4a;padding-bottom:4px}
.row{display:flex;gap:15px;flex-wrap:wrap}
.card{background:#1a2a3a;border-radius:6px;padding:12px 16px;flex:1;min-width:120px}
.card .n{font-size:28px;font-weight:bold}
.card .l{font-size:11px;color:#7a9ab0;margin-top:3px}
.n.green{color:#5f5} .n.yellow{color:#ff5} .n.red{color:#f55} .n.blue{color:#5af} .n.white{color:#fff}
table{width:100%;border-collapse:collapse;margin-top:5px}
th,td{padding:4px 8px;text-align:left;border-bottom:1px solid #1a3a4a}
th{color:#7a9ab0;font-weight:normal;font-size:12px}
td{font-size:13px}
.tag{padding:1px 6px;border-radius:3px;font-size:11px}
.tag-pending{background:#553} .tag-queued{background:#355;color:#5af}
.tag-processing{background:#5af;color:#000} .tag-done{background:#050}
.tag-failed{background:#500} .tag-skipped{background:#333;color:#888}
.alive{color:#5f5} .dead{color:#f55}
.workers{display:flex;gap:10px;flex-wrap:wrap}
.worker{border:1px solid #2a4a5a;border-radius:4px;padding:8px 12px}
</style>
</head>
<body>
<h1 style="color:#5af">&#x1f4ca; 抓取监控面板</h1>

<!-- 任务统计 -->
<h2>&#x1f4cb; 抓取任务</h2>
<div class="row" id="task-cards"></div>

<!-- 队列 -->
<h2>&#x1f4e6; 队列状态</h2>
<div id="queues"></div>

<!-- Worker -->
<h2>&#x1f3ad; Worker</h2>
<div class="workers" id="workers"></div>
<div id="active-crawls" style="margin-top:8px"></div>

<!-- 下载 -->
<h2>&#x2b07; 下载队列</h2>
<div id="active-downloads"></div>

<!-- 已处理 -->
<h2>&#x2705; 已处理总量</h2>
<div id="processed"></div>

<!-- 最近任务 -->
<h2>&#x1f4c4; 最近任务</h2>
<table id="recent"></table>

<script>
async function refresh(){
  const r = await fetch('/api/status');
  const d = await r.json();
  const S = {pending:'yellow', queued:'blue', processing:'blue', done:'green', failed:'red', skipped:'white'};

  // 任务统计卡片
  let tc = '';
  for(const [plat, types] of Object.entries(d.task_stats||{})){
    for(const [tt, statuses] of Object.entries(types)){
      for(const [st, cnt] of Object.entries(statuses)){
        tc += `<div class="card"><div class="n ${S[st]||'white'}">${cnt}</div><div class="l">${plat} ${tt} ${st}</div></div>`;
      }
    }
  }
  document.getElementById('task-cards').innerHTML = tc || '<div class="card">暂无数据</div>';

  // 队列
  let qh = '<table><tr><th>队列</th><th>待处理</th><th>处理中</th><th>重试</th><th>死信</th></tr>';
  for(const [q, v] of Object.entries(d.queues||{})){
    qh += `<tr><td>${q}</td><td>${v.pending}</td><td>${v.processing}</td><td>${v.retry}</td><td>${v.dead}</td></tr>`;
  }
  qh += '</table>';
  document.getElementById('queues').innerHTML = qh;

  // Worker
  let wh = '';
  for(const [w, s] of Object.entries(d.workers||{})){
    wh += `<div class="worker"><span class="${s.alive?'alive':'dead'}">&#x25cf;</span> ${w} <span style="color:#7a9ab0">${s.last_seen_sec}s ago</span></div>`;
  }
  document.getElementById('workers').innerHTML = wh || '无活跃 Worker';

  // 活跃抓取
  let ac = '';
  if(d.active_crawls?.length){
    ac = '<table><tr><th>队列</th><th>用户</th><th>任务ID</th></tr>';
    for(const c of d.active_crawls) ac += `<tr><td>${c.queue}</td><td>${c.user_id}</td><td>${c.task_id}</td></tr>`;
    ac += '</table>';
  }
  document.getElementById('active-crawls').innerHTML = ac;

  // 下载
  let ad = '';
  if(d.active_downloads?.length){
    ad = '<table><tr><th>队列</th><th>路径</th><th>任务ID</th></tr>';
    for(const dl of d.active_downloads) ad += `<tr><td>${dl.queue}</td><td>${dl.save_path}</td><td>${dl.task_id}</td></tr>`;
    ad += '</table>';
  }
  document.getElementById('active-downloads').innerHTML = ad;

  // 已处理
  let ph = '';
  for(const [p, v] of Object.entries(d.processed||{})){
    ph += `<div class="card"><div class="n blue">${v.users}</div><div class="l">${p} 用户</div></div>`;
    ph += `<div class="card"><div class="n blue">${v.total_posts}</div><div class="l">${p} 已处理帖</div></div>`;
  }
  document.getElementById('processed').innerHTML = ph || '暂无';

  // 最近任务
  let rh = '<tr><th>ID</th><th>平台</th><th>类型</th><th>用户</th><th>状态</th><th>时间</th></tr>';
  for(const t of d.recent_tasks||[]){
    rh += `<tr><td>${t.id}</td><td>${t.platform}</td><td>${t.task_type}</td><td>${t.user_id}</td><td><span class="tag tag-${t.status}">${t.status}</span></td><td>${t.upd}</td></tr>`;
  }
  document.getElementById('recent').innerHTML = rh;

  document.title = `监控 ${new Date().toTimeString().slice(0,8)}`;
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
