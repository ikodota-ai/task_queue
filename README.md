# IG/X Scraper — 分布式任务队列版

Instagram / X 图片抓取系统，基于 Redis 任务队列 + Selenium + MySQL。

## 文件说明

| 文件 | 用途 |
|------|------|
| `task_queue_robust.py` | 分布式任务队列框架（入队/出队/重试/死信/心跳） |
| `ig_crawler.py` | IG 爬虫消费者（全量 + 增量） |
| `sub_task_worker.py` | 子任务消费者（图片下载 + DB 更新） |
| `producer.py` | MySQL → Redis 任务投递 |
| `config.py` | 配置加载（从 .env 读取） |
| `import_cookies.py` | 导入浏览器 cookies 到 Redis |

## 部署

### 1. 安装依赖
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置
```bash
cp .env.example .env
# 编辑 .env，填入 Redis/MySQL/Chrome 连接信息 和 IG 账号密码
```

### 3. 导入 cookies（免手机验证）
```bash
python import_cookies.py < cookies.json
```

### 4. 启动生产者（投递 MySQL 待抓任务到 Redis）
```bash
python producer.py &
```

### 5. 启动消费者
```bash
# IG 全量抓取
python ig_crawler.py --mode full &

# IG 增量抓取
python ig_crawler.py --mode incr &

# 图片下载（可多开）
python sub_task_worker.py &
```

## 依赖服务

- Redis（任务队列 + 业务状态）
- MySQL（明星信息 + 图片记录）
- Chrome + ChromeDriver（爬虫）
- SSH 隧道（连接远程 Redis/MySQL）
