#!/bin/bash
# 统一管理所有服务：启动 / 停止 / 状态
# 用法: bash service.sh start|stop|status

cd "$(dirname "$0")"
source venv/bin/activate

SERVICES=(
    "producer:python producer.py"
    "ig-crawler-full:python ig_crawler.py --mode full"
    "ig-crawler-incr:python ig_crawler.py --mode incr"
    "x-crawler-full:python x_crawler.py --mode full"
    "x-crawler-incr:python x_crawler.py --mode incr"
    "sub-worker-1:python sub_task_worker.py"
    "sub-worker-2:python sub_task_worker.py"
    "monitor:python monitor.py"
)

start_all() {
    echo "=== 启动所有服务 ==="
    for svc in "${SERVICES[@]}"; do
        name="${svc%%:*}"
        cmd="${svc#*:}"
        if pgrep -f "$cmd" > /dev/null 2>&1; then
            echo "  [$name] 已在运行"
        else
            nohup $cmd > "logs/${name}.log" 2>&1 &
            echo "  [$name] 启动 (PID $!)"
        fi
    done
    echo "完成"
}

stop_all() {
    echo "=== 停止所有服务 ==="
    for svc in "${SERVICES[@]}"; do
        name="${svc%%:*}"
        cmd="${svc#*:}"
        while pgrep -f "$cmd" > /dev/null 2>&1; do
            pkill -f "$cmd" 2>/dev/null
            sleep 1
        done
        echo "  [$name] 已停止"
    done
    # 清理残留 Chrome 进程
    pkill -f "chrome.*user-data-dir=/tmp/chrome_" 2>/dev/null || true
    echo "完成"
}

status_all() {
    echo "=== 服务状态 ==="
    for svc in "${SERVICES[@]}"; do
        name="${svc%%:*}"
        cmd="${svc#*:}"
        if pgrep -f "$cmd" > /dev/null 2>&1; then
            echo "  [$name] 运行中"
        else
            echo "  [$name] 未启动"
        fi
    done
}

mkdir -p logs

case "${1:-status}" in
    start)  start_all ;;
    stop)   stop_all ;;
    status) status_all ;;
    *)      echo "用法: bash service.sh {start|stop|status}" ;;
esac
