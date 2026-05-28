#!/bin/bash
# IG/X Crawler Worker — 自动重启 (maxpage 由入队时指定，worker 不用传)
# 用法: ./deploy/run_worker.sh                  (默认: ig_crawler.py --mode all)
#       ./deploy/run_worker.sh full             (仅全量)
#       ./deploy/run_worker.sh incr             (仅增量)
#       ./deploy/run_worker.sh all x_crawler    (X 平台)
#       ./deploy/run_worker.sh incr ig_crawler 0 (增量, 不限任务数)

set -e

MODE="${1:-all}"
SCRIPT="${2:-ig_crawler}"
MAX_TASKS="${3:-20}"

# 切换到项目根目录 (deploy 的上级)
cd "$(dirname "$0")/.."
PROJECT_ROOT="$(pwd)"
echo "Project root: $PROJECT_ROOT"

PYTHON="venv/bin/python"
if [ ! -f "$PYTHON" ]; then
    echo "ERROR: $PYTHON not found"
    exit 1
fi

COUNT=0
while true; do
    COUNT=$((COUNT + 1))
    echo "========================================"
    echo "[$(date)] Worker #$COUNT starting: $PYTHON -u $SCRIPT.py --mode $MODE (MAX_TASKS=$MAX_TASKS)"
    echo "========================================"

    MAX_TASKS_PER_WORKER=$MAX_TASKS "$PYTHON" -u "$SCRIPT.py" --mode "$MODE" || true

    echo "[$(date)] Worker #$COUNT exited (code: $?), restarting in 3s..."
    sleep 3 &
    wait $! 2>/dev/null
done
