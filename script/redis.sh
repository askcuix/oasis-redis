#!/bin/sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LANG=C

[ $# -ne 2 ] && echo "Usage:$0 port action(start|restart|stop)" && exit 1
echo "$0 $@"

PORT=$1
ACTION=$2

REDIS_HOME=##REDIS_HOME##
CONF=$REDIS_HOME/conf/redis_$PORT.conf
PIDFILE=$REDIS_HOME/run/redis_$PORT.pid

BIN=$REDIS_HOME/bin/redis.sh
EXEC=/usr/local/bin/redis-server
CLIEXEC=/usr/local/bin/redis-cli
SENEXEC=/usr/local/bin/redis-sentinel

case "$ACTION" in
	start)
		if [ -f $PIDFILE ]
		then
			echo "$PIDFILE exists, process is already running or crashed."
		else
			echo "Starting Redis server..."
			$EXEC $CONF
			echo "Redis server started."
		fi
		;;
	restart)
		echo "Retarting Redis server..."
		bash $0 $PORT stop
		STATUS=$?
		if [ $STATUS -eq 0 ]
		then
			bash $0 $PORT start
			STATUS=$?
		fi
		;;
	stop)
		if [ ! -f $PIDFILE ]
		then
			echo "$PIDFILE does not exist, process is not running."
		else
			PID=$(cat $PIDFILE)
			echo "Stopping Redis server..."
			$CLIEXEC -p $PORT shutdown
			while [ -x /proc/${PID} ]
			do
				echo "Waiting for Redis to shutdown ..."
				sleep 1
			done
			echo "Redis server stopped."
		fi
		;;
	*)
		echo "Unknown action(start|restart|stop)." && exit 1
		;;
esac
