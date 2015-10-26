package com.oasis.redis.pool;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * Redis connection pool base class.
 * 
 * @author Chris
 *
 */
public abstract class RedisConnectionPool {

    /** redis config **/
    protected JedisPoolConfig poolConfig = null;
    protected int timeout = Protocol.DEFAULT_TIMEOUT;
    protected String password;
    protected int database = Protocol.DEFAULT_DATABASE;

    /**
     * initialize redis resource. Must be called after instance created.
     * 
     * Configured as spring bean init-method.
     */
    public void init() {
        // do nothing
    }

    /**
     * Destroy redis resource. Must be called before instance destroy.
     * 
     * Configured as spring bean destroy-method.
     */
    public void destroy() {
        // do nothing
    }

    /**
     * Get master pool for write operation.
     * 
     * @return null if no available master
     */
    public abstract RedisPool getMasterPool();

    /**
     * Get slave pool for read operation.
     * 
     * If all of the redis is down, then return null.
     * 
     * If only master available, then return master pool.
     * 
     * @return slave pool
     */
    public abstract RedisPool getSlavePool();

    /**
     * Remove broken connection pool.
     * 
     * @param pool
     */
    public abstract void returnBrokenPool(RedisPool pool);

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setPoolConfig(JedisPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

}
