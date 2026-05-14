#!/bin/bash
# 生成 supervisor 配置（自动替换路径）
# 用法: bash deploy/gen-supervisor.sh > /etc/supervisor/conf.d/task_queue.conf
DIR="$(cd "$(dirname "$0")/.." && pwd)"
sed "s|/path/to/task_queue|$DIR|g" "$DIR/deploy/supervisor.conf"
echo "# 生成完毕，请执行:"
echo "#   bash deploy/gen-supervisor.sh | sudo tee /etc/supervisor/conf.d/task_queue.conf"
echo "#   sudo supervisorctl reread && sudo supervisorctl update"
