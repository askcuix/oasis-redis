package com.oasis.redis.pool.shard;

import com.oasis.redis.pool.RedisPool;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisShardPool extends RedisPool {
    protected String shardName;

    public RedisShardPool(String shardName, String host, int port, JedisPoolConfig poolConfig) {
        this(shardName, host, port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, poolConfig);
    }

    public RedisShardPool(String shardName, String host, int port, int timeout, JedisPoolConfig poolConfig) {
        this(shardName, host, port, timeout, null, Protocol.DEFAULT_DATABASE, poolConfig);
    }

    public RedisShardPool(String shardName, String host, int port, int timeout, String password,
            JedisPoolConfig poolConfig) {
        this(shardName, host, port, timeout, password, Protocol.DEFAULT_DATABASE, poolConfig);
    }

    public RedisShardPool(String shardName, String host, int port, int timeout, String password, int database,
            JedisPoolConfig poolConfig) {
        super(host, port, timeout, password, database, poolConfig);

        this.shardName = shardName;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

}
