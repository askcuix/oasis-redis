#!/bin/sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LANG=C

[ $# -lt 2 ] && echo "Usage:$0 port action(start|stop)" && exit 1
echo "$0 $@"

PORT=$1
ACTION=$2

CLIEXEC=/usr/local/bin/redis-cli
SENEXEC=/usr/local/bin/redis-sentinel

REDIS_HOME=##REDIS_HOME##
CONFFILE=$REDIS_HOME/conf/sentinel_$PORT.conf

case "$ACTION" in
	start)
		echo "Starting Redis sentinel..."
		$SENEXEC $CONFFILE
		echo "Redis sentinel started."
		;;
	stop)
		echo "Shutdowning Redis sentinel..."
		$CLIEXEC -p $PORT shutdown
		echo "Redis sentinel stopped."
		;;
	*)
		echo "Unknown action(start|stop)." && exit 1
		;;
esac
