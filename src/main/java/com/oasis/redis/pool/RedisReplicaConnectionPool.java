package com.oasis.redis.pool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Connection pool for Redis master-slave deployment model.
 * 
 * @author Chris
 *
 */
public class RedisReplicaConnectionPool extends RedisConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisReplicaConnectionPool.class);

    private final static AtomicInteger sequences = new AtomicInteger();

    private volatile RedisPool redisMasterPool;
    private volatile List<RedisPool> redisSlavePool = new ArrayList<RedisPool>();

    private List<HostAndPort> redisServers = new ArrayList<HostAndPort>();

    public RedisReplicaConnectionPool(List<String> servers) {
        this(servers, new JedisPoolConfig());
    }

    public RedisReplicaConnectionPool(List<String> servers, JedisPoolConfig poolConfig) {
        if (servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("Redis servers is required!");
        }

        this.poolConfig = poolConfig;

        for (String server : servers) {
            redisServers.add(RedisUtil.toHostAndPort(server));
        }
    }

    @Override
    public void init() {
        logger.info("Initialize Redis replica connection pool start......");

        super.init();

        for (HostAndPort redis : redisServers) {
            Jedis jedis = null;
            try {
                jedis = new Jedis(redis.getHost(), redis.getPort(), timeout);
                jedis.connect();

                if (null != password) {
                    jedis.auth(password);
                }

                String info = jedis.info();
                boolean isMaster = RedisUtil.isMaster(info);

                RedisPool pool = new RedisPool(redis.getHost(), redis.getPort(), timeout, password, database,
                        poolConfig);
                if (isMaster) {
                    if (redisMasterPool == null) {
                        redisMasterPool = pool;
                    } else {
                        logger.warn("Duplicate redis master: {}, add to slave.", redis.toString());
                        redisSlavePool.add(pool);
                    }
                } else {
                    redisSlavePool.add(pool);
                }
            } catch (Exception e) {
                logger.warn("Initial redis[" + redis.toString() + "] error.", e);
                continue;
            } finally {
                RedisUtil.destroyJedis(jedis);
            }
        }

        if (redisMasterPool == null) {
            logger.error("No available redis master!");
        }

        logger.info("Initialized Redis replica connection pool.");
    }

    @Override
    public RedisPool getMasterPool() {
        if (redisMasterPool == null) {
            logger.error("No available redis master!");
            return null;
        }

        return redisMasterPool;
    }

    @Override
    public RedisPool getSlavePool() {
        if (redisSlavePool.isEmpty()) {
            logger.warn("No available redis slave!");

            if (redisMasterPool != null) {
                return redisMasterPool;
            }

            return null;
        } else if (redisSlavePool.size() == 1) {
            return redisSlavePool.get(0);
        }

        int index = sequences.getAndIncrement();
        if ((index >= Integer.MAX_VALUE) || (index < 0)) {
            sequences.set(0);
            index = sequences.getAndIncrement();
        }

        return redisSlavePool.get(index % redisSlavePool.size());
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

            if (redisMasterPool != null && (redisMasterPool.getHost().equals(pool.getHost())
                    && redisMasterPool.getPort() == pool.getPort())) {
                redisMasterPool.destroy();
                redisMasterPool = null;

                logger.warn("Redis master[{}:{}] is not available and removed it.", pool.getHost(), pool.getPort());
                return;
            }

            Iterator<RedisPool> slavePoolIterator = redisSlavePool.iterator();
            while (slavePoolIterator.hasNext()) {
                RedisPool slave = slavePoolIterator.next();

                if (slave != null && (slave.getHost().equals(pool.getHost()) && slave.getPort() == pool.getPort())) {
                    try {
                        slave.destroy();
                    } catch (Exception e) {
                        logger.warn("Destroy unavailable slave pool error.", e);
                    }
                    slavePoolIterator.remove();

                    logger.warn("Redis slave[{}:{}] is not available and removed it.", pool.getHost(), pool.getPort());
                    break;
                }

            }
        } catch (Exception e) {
            logger.error("Handle borken connection pool error.", e);
        }
    }

    @Override
    public void destroy() {
        logger.info("Destroy redis replica connection pool start......");

        super.destroy();

        if (redisMasterPool != null) {
            try {
                redisMasterPool.destroy();

                logger.info("Redis master pool[{}:{}] destroyed.", redisMasterPool.getHost(),
                        redisMasterPool.getPort());
            } catch (Exception e) {
                logger.warn("Destroy master pool[" + redisMasterPool.getHost() + ":" + redisMasterPool.getPort()
                        + "] error.", e);
            }
        }

        if (!redisSlavePool.isEmpty()) {
            for (RedisPool pool : redisSlavePool) {
                try {
                    pool.destroy();
                    logger.info("Redis slave pool[{}:{}] destroyed.", pool.getHost(), pool.getPort());
                } catch (Exception e) {
                    logger.warn("Destroy slave pool error.", e);
                }
            }

            redisSlavePool = new ArrayList<RedisPool>();

            logger.info("Redis slave pools destroyed.");
        }

        logger.info("Redis replica connection pool destroyed!");
    }

}
