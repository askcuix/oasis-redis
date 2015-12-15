package com.oasis.redis.service;

import java.net.InetAddress;
import java.security.SecureRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.oasis.redis.pool.RedisConnectionPool;

/**
 * 基于Redis的分布式锁。
 * 
 * 可用于分布式应用的定时任务，每次任务只有其中一台服务器执行。
 * 
 * 锁的过期时间尽量接近于任务的执行周期，避免多台服务器的任务启动时间差，导致在执行周期内被多台服务器执行。
 * 
 * @author cuixiang
 * 
 */
public class DistributedLock {
    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    private RedisConnectionPool redisConnectionPool = null;
    private String lockKey;
    private int expireSec;

    private String serverId;

    public DistributedLock(RedisConnectionPool redisPool, String lockKey) {
        this(redisPool, lockKey, 5);
    }

    public DistributedLock(RedisConnectionPool redisPool, String lockKey, int expireSec) {
        this.redisConnectionPool = redisPool;
        this.lockKey = lockKey;
        this.expireSec = expireSec;

        this.serverId = generateServerId();
    }

    /**
     * Get lock.
     * 
     * @return true for got lock, false for not got lock
     */
    public boolean getLock() {
        Jedis jedis = null;

        try {
            jedis = redisConnectionPool.getMasterPool().getResource();

            String currentLock = jedis.get(lockKey);

            if (currentLock == null) {
                String result = jedis.set(lockKey, serverId, "NX", "EX", expireSec);
                if (result != null && result.equals("OK")) {
                    logger.info("Get lock[{}]: {}", lockKey, serverId);

                    return true;
                }

                return false;
            }

            logger.info("Lock {} is got by: {}", lockKey, currentLock);

            // should be no this situation
            if (serverId.equals(currentLock)) {
                jedis.expire(lockKey, expireSec);

                logger.info("Lock {} still available: {}", lockKey, serverId);

                return true;
            }

            return false;
        } finally {
            if (jedis != null) {
                try {
                    jedis.close();
                } catch (Exception e) {
                    logger.error("Release jedis error.", e);
                }
            }
        }

    }

    /**
     * 主动释放锁。
     * 
     * 主要用于服务停止时，释放该服务所持有的锁。
     */
    public void releaseLock() {
        Jedis jedis = null;

        try {
            jedis = redisConnectionPool.getMasterPool().getResource();

            String currentLock = jedis.get(lockKey);

            if (serverId.equals(currentLock)) {
                jedis.del(lockKey);
            }
        } catch (Throwable t) {
            logger.error("Release lock[" + lockKey + "] error.", t);
        } finally {
            if (jedis != null) {
                try {
                    jedis.close();
                } catch (Exception e) {
                    logger.error("Release jedis error.", e);
                }
            }
        }
    }

    /**
     * 生成服务器标识。
     * 
     * 当前采用主机名加随机数，子类可重新实现该方法。
     * 
     * @return 服务器标识
     */
    protected String generateServerId() {
        String server = "localhost";
        try {
            server = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            logger.warn("Fail to get hostName, use localhost as default.", e);
        }
        server = server + "-" + new SecureRandom().nextInt(10000);

        return server;
    }

}
