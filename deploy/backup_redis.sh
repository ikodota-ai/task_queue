#!/bin/bash
# Redis 备份脚本 — 保留最近 7 天，每天凌晨执行
# 用法: bash deploy/backup_redis.sh
# cron: 0 3 * * * bash /path/to/task_queue/deploy/backup_redis.sh

set -e

REDIS_DATA_DIR="/var/lib/redis"
BACKUP_DIR="/backup/redis"
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR"

# 1. 触发 Redis 生成最新 RDB（BGSAVE 异步，不阻塞）
redis-cli -a 'admin@8899' -p 6381 --no-auth-warning BGSAVE > /dev/null 2>&1 || true
sleep 5  # 等 BGSAVE 完成

# 2. 复制 RDB
TIMESTAMP=$(date +%Y%m%d_%H%M)
cp "$REDIS_DATA_DIR/dump.rdb" "$BACKUP_DIR/dump_$TIMESTAMP.rdb"
echo "[$(date)] Backup saved: $BACKUP_DIR/dump_$TIMESTAMP.rdb"

# 3. 删除 7 天前的
DELETED=$(find "$BACKUP_DIR" -name "dump_*.rdb" -mtime +$RETENTION_DAYS -delete -print | wc -l)
echo "[$(date)] Removed $DELETED old backups (>${RETENTION_DAYS}d)"
