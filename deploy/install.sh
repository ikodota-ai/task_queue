#!/bin/bash
# 一键部署 systemd 服务 / 更新 .service 文件
# 用法: sudo bash deploy/install.sh
set -e

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE_DIR="/etc/systemd/system"

echo "安装目录: $APP_DIR"
echo "部署 systemd 服务..."

# 生成 .service 文件
cat > "$SERVICE_DIR/task-monitor.service" << EOF
[Unit]
Description=Task Queue Monitor
After=network.target
[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python monitor.py
Restart=always
RestartSec=5
User=$(whoami)
[Install]
WantedBy=multi-user.target
EOF

cat > "$SERVICE_DIR/task-producer.service" << EOF
[Unit]
Description=Task Queue Producer
After=network.target
[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python producer.py
Restart=always
RestartSec=10
User=$(whoami)
[Install]
WantedBy=multi-user.target
EOF

# 爬虫和下载 Worker 模板（按需启用）
for PLAT in ig x; do
for TYPE in full incr; do
cat > "$SERVICE_DIR/task-crawler-${PLAT}-${TYPE}.service" << EOF
[Unit]
Description=Task Crawler: $PLAT $TYPE
After=network.target
[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python ${PLAT}_crawler.py --mode $TYPE
Restart=always
RestartSec=15
User=$(whoami)
[Install]
WantedBy=multi-user.target
EOF
done
done

for PLAT in ig x; do
cat > "$SERVICE_DIR/task-downloader-${PLAT}.service" << EOF
[Unit]
Description=Task Downloader: $PLAT
After=network.target
[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python sub_task_worker.py --mode $PLAT
Restart=always
RestartSec=5
User=$(whoami)
[Install]
WantedBy=multi-user.target
EOF
done

systemctl daemon-reload
echo ""
echo "部署完成！常用命令："
echo "  # 启用开机自启"
echo "  sudo systemctl enable task-producer task-monitor"
echo ""
echo "  # 启动全部"
echo "  for s in task-producer task-monitor task-crawler-ig-full task-downloader-ig task-downloader-x; do"
echo "    sudo systemctl start \$s"
echo "  done"
echo ""
echo "  # 查看状态"
echo "  systemctl status 'task-*'"
echo ""
echo "  # 查看日志"
echo "  journalctl -u task-crawler-ig-full -f"
