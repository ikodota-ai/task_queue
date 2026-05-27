"""
监控面板 — 大屏实时查看抓取/下载动态
用法: python monitor.py (默认 http://0.0.0.0:5000)
"""
import json
import time
from collections import defaultdict
from flask import Flask, jsonify

import pymysql
import redis

from config import cfg

from task_queue_robust import TaskQueue

app = Flask(__name__)

_TABLE = cfg["table_prefix"] + "crawl_tasks"


def _get_tq():
    tq = TaskQueue()
    tq.redis = redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True, socket_timeout=5,
    )
    return tq


@app.route("/api/enqueue", methods=["POST"])
def api_enqueue():
    """手动入队：platform=ig/x, type=full/incr, user_id=xxx"""
    from flask import request

    platform = request.form.get("platform", "").strip().lower()
    task_type = request.form.get("type", "").strip().lower()
    user_id = request.form.get("user_id", "").strip()
    auto_repeat = request.form.get("auto_repeat") == "1"

    if platform not in ("ig", "x") or task_type not in ("full", "incr") or not user_id:
        return jsonify({"error": "invalid params"}), 400

    # 写入 MySQL
    db = _get_db()
    cur = db.cursor()
    cur.execute(
        f"INSERT INTO {_TABLE} (platform, task_type, user_id, status) VALUES (%s, %s, %s, 'pending')",
        (platform, task_type, user_id),
    )
    task_id = cur.lastrowid
    db.commit()
    db.close()

    # 入队 Redis
    tq = _get_tq()
    queue_name = f"crawl:{platform}:{task_type}"
    func_name = f"{platform}_full_crawl" if task_type == "full" else f"{platform}_incremental_crawl"
    tid = tq.enqueue(queue_name, func_name, user_id, task_id)

    return jsonify({
        "ok": True,
        "db_task_id": task_id,
        "queue_name": queue_name,
        "redis_task_id": tid,
    })


def _get_db():
    return pymysql.connect(
        host=cfg["mysql_host"], port=cfg["mysql_port"],
        user=cfg["mysql_user"], password=cfg["mysql_password"],
        database=cfg["mysql_db"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5, read_timeout=10,
    )


def _get_queue_redis():
    return redis.Redis(
        host=cfg["queue_redis_host"], port=cfg["queue_redis_port"],
        password=cfg["queue_redis_password"], db=cfg["queue_redis_db"],
        decode_responses=True, socket_timeout=5, socket_connect_timeout=3,
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

    # ===== MySQL 任务概览 (单次查询) =====
    cur = db.cursor()
    cur.execute(f"""
        SELECT platform, task_type, status, COUNT(*) as cnt
        FROM {_TABLE} GROUP BY platform, task_type, status
    """)
    task_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for row in cur.fetchall():
        task_stats[row["platform"]][row["task_type"]][row["status"]] = row["cnt"]

    cur.execute(f"""
        SELECT id, platform, task_type, user_id, status,
               DATE_FORMAT(updated_at, '%m-%d %H:%i') as upd
        FROM {_TABLE} ORDER BY id DESC LIMIT 10
    """)
    recent_tasks = list(cur.fetchall())

    # 今日进度
    cur.execute(f"""
        SELECT platform, task_type, status, COUNT(*) as cnt
        FROM {_TABLE}
        WHERE DATE(updated_at) = CURDATE()
        GROUP BY platform, task_type, status
    """)
    today_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for row in cur.fetchall():
        today_stats[row["platform"]][row["task_type"]][row["status"]] = row["cnt"]

    # 工作量汇总：今日/昨日/本周/本月
    work_periods = {}
    periods = {
        "today":    "DATE(updated_at) = CURDATE()",
        "yesterday":"DATE(updated_at) = CURDATE() - INTERVAL 1 DAY",
        "week":     "YEARWEEK(updated_at) = YEARWEEK(CURDATE())",
        "month":    "DATE_FORMAT(updated_at, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m')",
    }
    for key, cond in periods.items():
        cur.execute(f"""
            SELECT platform,
                   COUNT(DISTINCT user_id) as users,
                   SUM(images_count) as images
            FROM {_TABLE}
            WHERE {cond} AND status = 'done'
            GROUP BY platform
        """)
        work_periods[key] = {}
        for row in cur.fetchall():
            work_periods[key][row["platform"]] = {"users": row["users"] or 0, "images": row["images"] or 0}

    # ===== 全量/增量覆盖统计 =====
    sr = _get_state_redis()
    full_done_cnt = {"ig": 0, "x": 0}
    incr_24h = {"ig": 0, "x": 0}
    now_ts = int(time.time())
    for plat, prefix in [("ig", "instagram:"), ("x", "twitter:")]:
        for k in sr.keys(f"{prefix}*:state"):
            data = sr.hgetall(k)
            if data.get("full_done") == "1":
                full_done_cnt[plat] += 1
            last = int(data.get("incr_last_time", 0))
            if last > now_ts - 86400:
                incr_24h[plat] += 1
    coverage = {"full_done": full_done_cnt, "incr_24h": incr_24h}

    db.close()

    # ===== task_meta 概况 (一次 keys 分类) =====
    tm_total = 0
    tm_by_q = {"dl:ig": 0, "dl:x": 0, "crawl": 0}
    for k in qr.keys("task_meta:*"):
        tm_total += 1
        if ":dl:ig:" in k: tm_by_q["dl:ig"] += 1
        elif ":dl:x:" in k: tm_by_q["dl:x"] += 1
        elif ":crawl:" in k: tm_by_q["crawl"] += 1

    # ===== Redis — 用 pipeline 批量查 =====
    pipe = qr.pipeline()
    for q in ["crawl:ig:full", "crawl:ig:incr",
              "crawl:x:full", "crawl:x:incr",
              "dl:ig", "dl:x"]:
        pipe.llen(f"queue:{q}")
        pipe.hlen(f"processing:{q}")
        pipe.llen(f"dead:{q}")
        pipe.zcard(f"retry:{q}")
    results = pipe.execute()

    queues = {}
    qnames = ["crawl:ig:full", "crawl:ig:incr",
              "crawl:x:full", "crawl:x:incr",
              "dl:ig", "dl:x"]
    for i, q in enumerate(qnames):
        queues[q] = {
            "pending": results[i*4] or 0,
            "processing": results[i*4+1] or 0,
            "dead": results[i*4+2] or 0,
            "retry": results[i*4+3] or 0,
        }

    # ===== Worker 心跳 =====
    workers = {}
    for k in qr.keys("worker:heartbeat:*"):
        wid = k.split(":", 2)[2]
        raw = qr.get(k) or "|0"
        parts = raw.split("|")
        host = parts[0] if len(parts) > 0 else "?"
        ts = float(parts[1]) if len(parts) > 1 else 0
        activity = parts[2] if len(parts) > 2 else ""
        elapsed = parts[3] if len(parts) > 3 else ""
        queue = parts[4] if len(parts) > 4 else ""
        tid = parts[5] if len(parts) > 5 else ""
        age = int(time.time() - ts)
        workers[wid] = {"alive": age < 90, "last_seen_sec": age, "host": host,
                        "activity": activity, "elapsed": elapsed, "queue": queue,
                        "tid": tid}

    # ===== 活跃抓取 (只取 processing key，不逐个查 meta) =====
    active_crawls = []
    for q in ["crawl:ig:full", "crawl:ig:incr", "crawl:x:full", "crawl:x:incr"]:
        if queues[q]["processing"]:
            pipe2 = qr.pipeline()
            tids = list(qr.hgetall(f"processing:{q}").keys())
            for tid in tids:
                pipe2.hget(f"task_meta:{q}:{tid}", "args")
            args_list = pipe2.execute()
            for j, tid in enumerate(tids):
                user_id = "?"
                try:
                    a = eval(args_list[j] or "[]")
                    user_id = str(a[0]) if a else "?"
                except Exception:
                    user_id = "?"
                active_crawls.append({
                    "queue": q,
                    "user_id": user_id,
                    "task_id": tid[:8],
                })

    # ===== 下载活跃 (取前 10，含用户和帖子) =====
    active_downloads = []
    for q in ["dl:ig", "dl:x"]:
        if queues[q]["processing"]:
            tids = list(qr.hgetall(f"processing:{q}").keys())[:12]
            pipe2 = qr.pipeline()
            for tid in tids:
                pipe2.hget(f"task_meta:{q}:{tid}", "args")
            args_list = pipe2.execute()
            for j, tid in enumerate(tids):
                info = {"queue": q, "task_id": tid[:8], "user": "-", "post": "-"}
                try:
                    a = eval(args_list[j] or "[]")
                    if len(a) >= 5:
                        info["user"] = str(a[4])  # user_id
                    # 从 save_path 提取 post_id:  image/1065/abc.jpg 或 user/POST_0001.jpg
                    if len(a) >= 2:
                        fn = str(a[1]).rsplit("/", 1)[-1]  # 取文件名
                        # 如果是 POST_0001.jpg 格式，提取 POST
                        pid = fn.rsplit("_", 1)[0] if "_" in fn else fn[:16]
                        info["post"] = pid
                except Exception:
                    pass
                active_downloads.append(info)

    # ===== 当前活跃抓取详情 (取第一个) =====
    current_crawl = None
    if active_crawls:
        c = active_crawls[0]
        current_crawl = {
            "user": c["user_id"],
            "platform": c["queue"].split(":")[1],
            "type": c["queue"].split(":")[2],
            "task_id": c["task_id"],
        }

    # ===== 最近完成的抓取任务 (scan task_meta 取 done, 限 20) =====
    completed_crawls = []
    done_keys = []
    for k in qr.keys("task_meta:crawl:*"):
        done_keys.append(k)
        if len(done_keys) >= 50:
            break
    # pipeline 批量查 status
    pipe2 = qr.pipeline()
    for k in done_keys:
        pipe2.hget(k, "status")
    results = pipe2.execute()
    for i, k in enumerate(done_keys):
        if results[i] != "done":
            continue
        tid = k.rsplit(":", 1)[-1][:8]
        q = ":".join(k.split(":")[1:4])
        args_str = qr.hget(k, "args") or ""
        user_id = "?"
        try:
            a = eval(args_str)
            user_id = str(a[0]) if a else "?"
        except Exception:
            user_id = "auto"  # 自动入队任务无 args
        completed_crawls.append({"queue": q, "user_id": user_id, "task_id": tid})
    completed_crawls = completed_crawls[-6:][::-1]

    return jsonify({
        "task_stats": task_stats,
        "today_stats": today_stats,
        "work": work_periods,
        "current_crawl": current_crawl,
        "recent_tasks": recent_tasks,
        "queues": queues,
        "workers": workers,
        "active_downloads": active_downloads,
        "active_crawls": active_crawls,
        "completed_crawls": completed_crawls,
        "coverage": coverage,
        "dl_pending": sum(q["pending"] for q in queues.values() if q["pending"]),
        "task_meta": {"total": tm_total, "by_type": tm_by_q},
        "ts": int(time.time()),
    })


HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>抓取监控</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1a25;color:#bcc8d4;font:12px/1.5 monospace;font-size:1.5rem;padding:10px 14px;max-height:100vh;overflow:hidden}
h1{font-size:16px;font-size:1.5rem;color:#5af;margin-bottom:10px}
h2{font-size:13px;font-size:1.5rem;color:#5af;margin:10px 0 4px;border-bottom:1px solid #1a3444;padding-bottom:2px}
.row{display:flex;gap:10px}
.card{background:#162636;border-radius:4px;padding:6px 12px;text-align:center;min-width:80px}
.card .n{font-size:22px;font-size:1.5rem;font-weight:bold}
.card .l{font-size:10px;font-size:1.5rem;color:#6a8a9e}
.n.yellow{color:#fa0} .n.blue{color:#59f} .n.green{color:#5e5} .n.red{color:#e55} .n.white{color:#ddd}
.col{flex:1;min-width:0}
table{width:100%;border-collapse:collapse}
th,td{padding:2px 6px;border-bottom:1px solid #1a3040;text-align:left;font-size:11px;font-size:1.5rem;}
th{color:#6a8a9e;font-weight:normal;font-size:10px;font-size:1.5rem;}
.tag{padding:0 4px;border-radius:2px;font-size:10px;font-size:1.5rem;display:inline-block;min-width:50px;text-align:center}
.tag-pending{background:#442} .tag-queued{background:#244;color:#59f}
.tag-processing{background:#59f;color:#000} .tag-done{background:#141}
.tag-failed{background:#400} .tag-skipped{background:#222;color:#777}
.alive{color:#5f5} .dead{color:#f55}
#active-now{font-size:14px;font-size:1.5rem;color:#fa0;min-height:20px}
</style>
</head>
<body>
<h1>&#x25c9; 抓取监控 <span id="clock" style="color:#6a8a9e;font-size:11px;float:right;font-size:1.5rem;"></span></h1>



<!-- 总览 + 最近完成 + 队列 并排 -->
<div class="row" style="gap:10px;margin-bottom:6px">
  <div style="flex:1;min-width:0" id="overview" class="row"></div>
  <div style="flex:1;min-width:0">
    <h2>&#x1f4c4; 最近完成</h2>
    <table id="recent"></table>
  </div>
  <div style="flex:1;min-width:220px">
    <h2>&#x2630; 队列</h2>
    <table id="queues-table"></table>
  </div>
</div>

<!-- 抓取 Worker -->
<h2>&#x25b6; 抓取 Worker</h2>
<table id="crawl-table"></table>

<!-- 下载 Worker -->
<h2>&#x21e9; 下载 Worker</h2>
<table id="dl-table"></table>

<!-- 下载 -->
<div style="margin-top:6px">
  <span>&#x21e9; 下载队列: <b id="dl-total" style="color:#fa0">0</b></span>
  <span id="dl-summary" style="font-size:11px;font-size:1.5rem;color:#6a8a9e;margin-left:10px"></span>
  <span id="tm-info" style="font-size:10px;font-size:1.5rem;color:#6a8a9e;margin-left:10px"></span>
</div>

<!-- 手动入队 -->
<div class="row" style="margin-top:8px">
  <div style="flex:2">
    <h2>&#x2795; 手动入队</h2>
    <form id="enq-form" style="display:flex;gap:6px;align-items:center;flex-wrap:wrap" onsubmit="return enqueue()">
      <select id="enq-plat" style="background:#162636;color:#bcc8d4;border:1px solid #2a4a5a;padding:4px 8px;border-radius:3px;font-size:11px;font-size:1.5rem;">
        <option value="ig">IG</option><option value="x">X</option>
      </select>
      <select id="enq-type" style="background:#162636;color:#bcc8d4;border:1px solid #2a4a5a;padding:4px 8px;border-radius:3px;font-size:11px;font-size:1.5rem;">
        <option value="full">全量</option><option value="incr">增量</option>
      </select>
      <input id="enq-user" placeholder="user_id" style="background:#162636;color:#bcc8d4;border:1px solid #2a4a5a;padding:4px 8px;border-radius:3px;font-size:11px;font-size:1.5rem;;width:140px">
      <button type="submit" style="background:#5af;color:#000;border:none;padding:4px 12px;border-radius:3px;font-size:11px;font-size:1.5rem;;cursor:pointer;font-weight:bold">入队</button>
      <span id="enq-msg" style="font-size:11px;font-size:1.5rem;;color:#5f5;margin-left:6px"></span>
    </form>
  </div>
</div>

<script>
async function refresh(){
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    const S = {pending:'yellow', queued:'blue', processing:'blue', done:'green', failed:'red', skipped:'white'};

    // ===== 顶部：今日 + 昨日 + 覆盖 =====
    let ov = '';
    for(const [plat, color] of [['ig','#e4405f'],['x','#1da1f2']]){
      const today = d.work?.today?.[plat] || {};
      const yesterday = d.work?.yesterday?.[plat] || {};
      const cov = d.coverage || {};
      ov += `<div class="card" style="border-top:3px solid ${color}">`;
      ov += `<div class="l">${plat.toUpperCase()}</div>`;
      ov += `<table style="margin-top:2px"><tr><th></th><th>人</th><th>图</th></tr>`;
      ov += `<tr><td>今日</td><td style="color:#5af">${today.users||0}</td><td style="color:#fa0">${today.images||0}</td></tr>`;
      ov += `<tr><td>昨日</td><td style="color:#5af">${yesterday.users||0}</td><td style="color:#fa0">${yesterday.images||0}</td></tr>`;
      ov += `<tr><td>全量完成</td><td style="color:#5e5" colspan=2>${cov.full_done?.[plat]||0} 人</td></tr>`;
      ov += `<tr><td>24h增量</td><td style="color:#fa0" colspan=2>${cov.incr_24h?.[plat]||0} 人</td></tr>`;
      ov += `</table></div>`;
    }
    document.getElementById('overview').innerHTML = ov;

    // ===== 活跃抓取 =====
    let now = '';
    let at = '';
    if(d.current_crawl){
      const c = d.current_crawl;
      now = `<b>${c.platform.toUpperCase()} ${c.type}</b> 正在抓取 <b style="color:#fa0">@${c.user}</b>`;
      if(c.scroll) now += ` | 滚动${c.scroll}次 | ${c.images||0}张图`;
    } else {
      now = '<span style="color:#6a8a9e">等待任务...</span>';
    }
    // ===== 抓取 Worker 表 =====
    const allW = Object.entries(d.workers||{});
    const crawlW = allW.filter(([id]) => id.includes('crawler'));
    const subW = allW.filter(([id]) => id.includes('sub'));
    const renderW = (list, cols, elemId) => {
      let h = `<tr>${cols.map(c=>`<th>${c}</th>`).join('')}</tr>`;
      for(const [wid, ws] of list){
        const alive = ws.alive ? 'alive' : 'dead';
        h += `<tr>`;
        h += `<td>${ws.queue||'-'}</td>`;
        h += `<td style="color:#5af">@${ws.host||'?'}</td>`;
        h += `<td><span class="${alive}">&#x25cf;</span> ${wid}</td>`;
        h += `<td style="color:#fa0">${ws.activity||'空闲'}</td>`;
        h += `<td style="color:#6a8a9e">${ws.tid||'-'}</td>`;
        h += `<td style="color:#6a8a9e">${ws.elapsed ? ws.elapsed+'s' : '-'}</td>`;
        h += '</tr>';
      }
      if(!list.length) h += '<tr><td colspan=6>无</td></tr>';
      document.getElementById(elemId).innerHTML = h;
    };
    renderW(crawlW, ['队列','机器','Worker','状态','任务ID','耗时'], 'crawl-table');
    renderW(subW, ['队列','机器','Worker','状态','任务ID','耗时'], 'dl-table');

    // ===== 队列表 (独立) =====
    document.getElementById('queues-table').innerHTML = (function(){
      let h = '<tr><th>队列</th><th>待处理</th><th>处理中</th><th>重试</th><th>死信</th></tr>';
      for(const [q, v] of Object.entries(d.queues||{})){
        h += `<tr><td>${q}</td><td style="color:#fa0">${v.pending||0}</td><td>${v.processing||0}</td><td style="color:#e55">${v.retry||0}</td><td style="color:#e55">${v.dead||0}</td></tr>`;
      }
      return h;
    })();

    // task_meta 概况
    const tm = d.task_meta;
    document.getElementById('tm-info').innerHTML = tm
      ? `Redis: ${tm.total} 条记录 | IG下载 ${tm.by_type['dl:ig']||0} | X下载 ${tm.by_type['dl:x']||0} | 抓取 ${tm.by_type['crawl']||0}`
      : '';

    // 下载
    const dli = d.queues['dl:ig']?.pending||0;
    const dlx = d.queues['dl:x']?.pending||0;
    document.getElementById('dl-total').innerText = (d.dl_pending||dli+dlx);
    let ds = `IG: ${dli} 待下载 | X: ${dlx} 待下载 | 处理中: ${d.active_downloads?.length||0}`;
    if(d.active_downloads?.length){
      ds += '<br>';
      for(const dl of d.active_downloads.slice(0,6)){
        ds += `<span style="font-size:10px;color:#7a9ab0">@${dl.user}/${dl.post}</span> `;
      }
    }
    document.getElementById('dl-summary').innerHTML = ds;

    // 最近完成的抓取 (Redis task_meta)
    let rh = '<tr><th>队列</th><th>用户</th><th>任务ID</th></tr>';
    for(const c of d.completed_crawls||[])
      rh += `<tr><td>${c.queue}</td><td>@${c.user_id}</td><td>${c.task_id}</td></tr>`;
    document.getElementById('recent').innerHTML = rh || '<tr><td colspan=3>暂无</td></tr>';

    document.getElementById('clock').innerText = new Date().toLocaleTimeString();
  }catch(e){}
}
async function enqueue(){
  const plat = document.getElementById('enq-plat').value;
  const type = document.getElementById('enq-type').value;
  const uid = document.getElementById('enq-user').value.trim();
  if(!uid) return false;
  const body = new URLSearchParams({platform:plat, type, user_id:uid});
  const r = await fetch('/api/enqueue', {method:'POST', body});
  const d = await r.json();
  const el = document.getElementById('enq-msg');
  if(d.ok){ el.innerText = `OK: ${d.queue_name} #${d.db_task_id}`; document.getElementById('enq-user').value=''; }
  else el.innerText = 'ERR: '+ (d.error||'?');
  setTimeout(()=>el.innerText='', 4000);
  refresh();
  return false;
}
refresh();
setInterval(refresh, 4000);
</script>
</body>
</html>"""


@app.route("/")
def index():
    return HTML


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
