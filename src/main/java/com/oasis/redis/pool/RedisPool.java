package com.oasis.redis.pool;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisPool extends JedisPool {
    protected String host;
    protected int port;
    protected int timeout;
    protected String password;
    protected int database;

    public RedisPool(String host, int port, JedisPoolConfig poolConfig) {
        this(host, port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, poolConfig);
    }

    public RedisPool(String host, int port, int timeout, JedisPoolConfig poolConfig) {
        this(host, port, timeout, null, Protocol.DEFAULT_DATABASE, poolConfig);
    }

    public RedisPool(String host, int port, int timeout, String password, JedisPoolConfig poolConfig) {
        this(host, port, timeout, password, Protocol.DEFAULT_DATABASE, poolConfig);
    }

    public RedisPool(String host, int port, int timeout, String password, int database,
            JedisPoolConfig poolConfig) {
        super(poolConfig, host, port, timeout, password, database);

        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.password = password;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

}
