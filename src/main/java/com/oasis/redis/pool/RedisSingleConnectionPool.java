package com.oasis.redis.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Connection pool for single Redis deployment model.
 * 
 * @author Chris
 *
 */
public class RedisSingleConnectionPool extends RedisConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisSingleConnectionPool.class);

    private HostAndPort redisServer;
    private RedisPool redisPool;

    public RedisSingleConnectionPool(String server) {
        this(server, new JedisPoolConfig());
    }

    public RedisSingleConnectionPool(String server, JedisPoolConfig poolConfig) {
        this.redisServer = RedisUtil.toHostAndPort(server);
        this.poolConfig = poolConfig;
    }

    @Override
    public void init() {
        super.init();

        redisPool = new RedisPool(redisServer.getHost(), redisServer.getPort(), timeout, password, database,
                poolConfig);

        logger.info("Initialized redis pool at {}.", redisServer.toString());
    }

    /**
     * Get redis pool.
     * 
     * @return redis pool
     */
    public RedisPool getRedisPool() {
        if (redisPool == null) {
            logger.error("Must be invoke init method before use or no available redis server.");
            return null;
        }

        return redisPool;
    }

    @Override
    public RedisPool getMasterPool() {
        return getRedisPool();
    }

    @Override
    public RedisPool getSlavePool() {
        return getRedisPool();
    }

    @Override
    public void returnBrokenPool(RedisPool pool) {
        if (pool == null) {
            return;
        }

        try {
            Jedis jedis = pool.getResource();
            boolean alive = RedisUtil.ping(jedis);

            if (alive) {
                logger.info("Redis[{}:{}] still alive, retain to use.", pool.getHost(), pool.getPort());
                return;
            }

            if (redisPool != null
                    && (redisPool.getHost().equals(pool.getHost()) && redisPool.getPort() == pool.getPort())) {
                redisPool.destroy();
                redisPool = null;

                logger.warn("Redis[{}:{}] is not available and removed it.", pool.getHost(), pool.getPort());
                return;
            }
        } catch (Exception e) {
            logger.error("Handle borken connection pool error.", e);
        }
    }

    @Override
    public void destroy() {
        super.destroy();

        if (redisPool != null) {
            try {
                if (!redisPool.isClosed()) {
                    redisPool.destroy();
                }
            } catch (Exception e) {
                logger.warn("Destroy redis pool[" + redisServer.toString() + "] error.", e);
            }
        }

        logger.info("Destroyed redis pool at {}.", redisServer.toString());
    }

}
