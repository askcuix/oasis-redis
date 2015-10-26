#!/bin/sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LANG=C

[ $# -lt 2 ] && echo "Usage:$0 port action(createMaster|createSlave|remove) masterIP masterPORT" && exit 1
echo "$0 $@"

. /data/scripts/redis/config.sh

PORT=$1
ACTION=$2

CONFFILE=$REDIS_HOME/conf/redis_$PORT.conf

DATADIR=$REDIS_HOME/data/redis_$PORT
LOGFILE=$REDIS_HOME/logs/redis_$PORT.log
PIDFILE=$REDIS_HOME/run/redis_$PORT.pid

case "$ACTION" in
	createMaster)
		[ -f $CONFFILE ] && { mv $CONFFILE{,.bak.$(date +%F)};echo "$CONFFILE exists and backup it as ${CONFFILE}.bak.$(date +%F) already."; }
		sed -e "s/##PORT##/$PORT/g" $REDIS_TEMPLATE > $CONFFILE
		mkdir -p $DATADIR || { echo "fail make directory to store redis file: $DATADIR"; exit 1; }

		echo "CONF\t\t:$CONFFILE"
		echo "Storage Dir\t:$DATADIR"

		echo "Starting Redis master server..."
		$BIN $PORT start
		;;
	createSlave)
		MASTER_IP=$3
		MASTER_PORT=$4

		[ -f $CONFFILE ] && { mv $CONFFILE{,.bak.$(date +%F)};echo "$CONFFILE exists and backup it as ${CONFFILE}.bak.$(date +%F) already."; }
		sed -e "s/##PORT##/$PORT/g" -e "/# slaveof/c slaveof $MASTER_IP $MASTER_PORT" $REDIS_TEMPLATE > $CONFFILE
		mkdir -p $DATADIR || { echo "fail make directory to store redis file: $DATADIR"; exit 1; }

		echo "CONF\t\t:$CONFFILE"
		echo "Storage Dir\t:$DATADIR"

		echo "Starting Redis slave server..."
		$BIN $PORT start
		;;
	remove)
		echo "Shutdowning Redis server..."
		$BIN $PORT stop

		echo "Clean Redis folder..."
		rm -rf $CONFFILE $LOGFILE $PIDFILE $DATADIR
		;;
	*)
		echo "Unknow action(createMaster|createSlave|remove)." && exit 1
		;;
esac
