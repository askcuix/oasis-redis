#!/bin/sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LANG=C

. /data/scripts/redis/config.sh

echo "Getting Redis server..."
cd $SERVICE_DIR
wget -q http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz
tar xzf redis-$REDIS_VERSION.tar.gz

echo "Installing Redis server..."
cd $REDIS_BASE
make
make test
make install

echo "Initialize Redis server..."
rm -rf $REDIS_HOME
ln -s $REDIS_BASE $REDIS_HOME
cd $REDIS_HOME
mkdir conf run bin
mkdir -p $LOGS_DIR && ln -s $LOGS_DIR logs
mkdir -p $DATAS_DIR && ln -s $DATAS_DIR data

sed -e "s@##REDIS_HOME##@$REDIS_HOME@g" $SCRIPT_DIR/redis.sh > $REDIS_HOME/bin/redis.sh
sed -e "s@##REDIS_HOME##@$REDIS_HOME@g" $SCRIPT_DIR/sentinel.sh > $REDIS_HOME/bin/sentinel.sh

chmod 755 $REDIS_HOME/bin/redis.sh
chmod 755 $REDIS_HOME/bin/sentinel.sh

ls -lh $REDIS_HOME
ls -lh $REDIS_HOME/

echo "Redis server is ready."
