package com.oasis.redis.pool.sentinel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.pool.RedisConnectionPool;
import com.oasis.redis.pool.RedisPool;
import com.oasis.redis.sentinel.SentinelEventListener;
import com.oasis.redis.sentinel.SentinelEventReceiver;
import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Connection pool for Redis sentinel deployment model.
 * 
 * @author Chris
 * 
 */
public class RedisSentinelConnectionPool extends RedisConnectionPool implements SentinelEventReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RedisSentinelConnectionPool.class);

    private String masterName;
    private List<String> sentinels;

    private SentinelEventListener sentinelEventListener;

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    private volatile HostAndPort currentMaster;
    private volatile RedisPool redisMasterPool;
    private volatile List<RedisPool> redisSlavePool = new ArrayList<RedisPool>();
    private final static AtomicInteger sequences = new AtomicInteger();

    public RedisSentinelConnectionPool(String masterName, List<String> sentinels) {
        this(masterName, sentinels, new JedisPoolConfig());
    }

    public RedisSentinelConnectionPool(String masterName, List<String> sentinels, JedisPoolConfig poolConfig) {
        this.masterName = masterName;
        this.sentinels = sentinels;
        this.poolConfig = poolConfig;
    }

    @Override
    public void init() {
        if (masterName == null || masterName.trim().length() == 0) {
            throw new IllegalArgumentException("masterName is required!");
        }

        if ((sentinels == null) || sentinels.isEmpty()) {
            throw new IllegalArgumentException("sentinels is required!");
        }

        super.init();

        sentinelEventListener = new SentinelEventListener(this);
        sentinelEventListener.start();

        logger.info("Initialized Redis sentinel connection pool.");
    }

    @Override
    public String getMasterName() {
        return masterName;
    }

    @Override
    public List<String> getSentinels() {
        return sentinels;
    }

    public HostAndPort getCurrentMaster() {
        return currentMaster;
    }

    @Override
    public void refreshMasterPool(HostAndPort master) {
        writeLock.lock();

        try {
            if (master == null) {
                logger.error("[{}] No available master.", masterName);

                if (redisMasterPool != null) {
                    try {
                        redisMasterPool.destroy();
                        redisMasterPool = null;
                    } catch (Exception e) {
                        logger.warn("Destroy unavailable master pool error.", e);
                    }

                    return;
                }
            }

            if (master.equals(currentMaster)) {
                return;
            }

            currentMaster = master;

            RedisPool newRedisPool = new RedisPool(master.getHost(), master.getPort(), timeout, password, database,
                    poolConfig);
            if (redisMasterPool == null) {
                redisMasterPool = newRedisPool;
            } else {
                RedisPool oldMasterPool = redisMasterPool;
                redisMasterPool = newRedisPool;

                try {
                    oldMasterPool.destroy();
                    oldMasterPool = null;
                } catch (Exception e) {
                    logger.warn("Destroy old master pool error.", e);
                }
            }

            logger.info("[{}] Created master pool at {}:{}", masterName, redisMasterPool.getHost(),
                    redisMasterPool.getPort());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public RedisPool getMasterPool() {
        readLock.lock();

        try {
            if (redisMasterPool == null) {
                logger.error("No available redis master!");
                return null;
            }

            return redisMasterPool;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void refreshSlavePool(List<HostAndPort> slaveList) {
        writeLock.lock();

        try {
            if (slaveList == null || slaveList.isEmpty()) {
                logger.error("[{}] No available slave.", masterName);

                for (RedisPool slavePool : redisSlavePool) {
                    try {
                        slavePool.destroy();
                    } catch (Exception e) {
                        logger.warn("Destroy unavailable slave pool error.", e);
                    }
                }

                redisSlavePool.clear();
                return;
            }

            // remove unavailable slaves
            Iterator<RedisPool> slavePoolIterator = redisSlavePool.iterator();
            while (slavePoolIterator.hasNext()) {
                RedisPool slavePool = slavePoolIterator.next();
                if (slavePool == null) {
                    slavePoolIterator.remove();
                    continue;
                }

                boolean available = false;
                for (HostAndPort slave : slaveList) {
                    if (slavePool.getHost().equals(slave.getHost()) && slavePool.getPort() == slave.getPort()) {
                        available = true;
                        break;
                    }
                }

                if (!available) {
                    try {
                        slavePool.destroy();
                    } catch (Exception e) {
                        logger.warn("Destroy unavailable slave pool error.", e);
                    }
                    slavePoolIterator.remove();
                }
            }

            // add new slaves
            for (HostAndPort slave : slaveList) {
                boolean exists = false;

                for (RedisPool slavePool : redisSlavePool) {
                    if (slavePool.getHost().equals(slave.getHost()) && slavePool.getPort() == slave.getPort()) {
                        exists = true;
                        break;
                    }
                }

                if (!exists) {
                    RedisPool slavePool = new RedisPool(slave.getHost(), slave.getPort(), timeout, password, database,
                            poolConfig);
                    redisSlavePool.add(slavePool);
                }
            }

            logger.info("[{}] Available slave pools: {}", masterName, RedisUtil.extractServerInfo(redisSlavePool));
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public RedisPool getSlavePool() {
        readLock.lock();

        try {
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
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void returnBrokenPool(RedisPool pool) {
        if (pool == null) {
            return;
        }

        writeLock.lock();

        try {
            Jedis jedis = pool.getResource();
            boolean alive = RedisUtil.ping(jedis);

            if (alive) {
                logger.info("Redis[{}:{}] still alive, retain to use.", pool.getHost(), pool.getPort());
                return;
            }

            if (redisMasterPool != null
                    && (redisMasterPool.getHost().equals(pool.getHost()) && redisMasterPool.getPort() == pool.getPort())) {
                redisMasterPool.destroy();
                redisMasterPool = null;

                logger.warn("Redis master[{}:{}] is not available and removed it.", pool.getHost(), pool.getPort());
                return;
            }

            Iterator<RedisPool> slavePoolIterator = redisSlavePool.iterator();
            while (slavePoolIterator.hasNext()) {
                RedisPool slave = slavePoolIterator.next();

                if (slave.getHost().equals(pool.getHost()) && slave.getPort() == pool.getPort()) {
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
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void destroy() {
        logger.info("Destroy redis sentinel connection pool start......");

        super.destroy();

        sentinelEventListener.stop();

        if (redisMasterPool != null) {
            try {
                redisMasterPool.destroy();

                logger.info("Redis master pool[{}:{}] destroyed.", redisMasterPool.getHost(), redisMasterPool.getPort());
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

        logger.info("Redis sentinel connection pool destroyed!");
    }

}
