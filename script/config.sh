#!/bin/sh

SCRIPT_DIR=/data/scripts/redis
REDIS_VERSION=3.0.5
SERVICE_DIR=/data/services
REDIS_BASE=$SERVICE_DIR/redis-$REDIS_VERSION
REDIS_HOME=/usr/local/redis
LOGS_DIR=/data/logs/redis
DATAS_DIR=/data/data/redis

REDIS_TEMPLATE=$SCRIPT_DIR/redis_TEMPLATE.conf
SENTINEL_TEMPLATE=$SCRIPT_DIR/sentinel_TEMPLATE.conf

BIN=$REDIS_HOME/bin/redis.sh
SENTINEL_BIN=$REDIS_HOME/bin/sentinel.sh
