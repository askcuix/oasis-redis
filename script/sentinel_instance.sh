#!/bin/sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LANG=C

[ $# -lt 2 ] && echo "Usage:$0 port action(create|remove) masterName(optional) masterIP(optional) masterPORT(optional)" && exit 1
echo "$0 $@"

. /data/scripts/redis/config.sh

PORT=$1
ACTION=$2

CONFFILE=$REDIS_HOME/conf/sentinel_$PORT.conf
DATADIR=$REDIS_HOME/data/sentinel_$PORT
LOGFILE=$REDIS_HOME/logs/sentinel_$PORT.log
PIDFILE=$REDIS_HOME/run/sentinel_$PORT.pid

case "$ACTION" in
	create)
		QUORUM=2
		MASTER_NAME=$3
		MASTER_IP=$4
		MASTER_PORT=$5

		[ -f $CONFFILE ] && { mv $CONFFILE{,.bak.$(date +%F)};echo "$CONFFILE exists and backup it as ${CONFFILE}.bak.$(date +%F) already."; }
		sed -e "s/##PORT##/$PORT/g" -e "s/##MASTER_NAME##/$MASTER_NAME/g" -e "s/##MASTER_IP##/$MASTER_IP/g" -e "s/#MASTER_PORT#/$MASTER_PORT/g" -e "s/##QUORUM##/$QUORUM/g" $SENTINEL_TEMPLATE > $CONFFILE
		mkdir -p $DATADIR || { echo "fail make directory for sentinel working: $DATADIR"; exit 1; }

		echo "CONF\t\t:$CONFFILE"

		$SENTINEL_BIN $PORT start
		;;
	remove)
		$SENTINEL_BIN $PORT stop

		echo "Clean sentinel files..."
		rm -rf $CONFFILE $LOGFILE $DATADIR $PIDFILE
		;;
	*)
		echo "Unknow action(create|remove)." && exit 1
		;;
esac
