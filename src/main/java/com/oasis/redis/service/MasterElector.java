package com.oasis.redis.service;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.pool.RedisConnectionPool;

import redis.clients.jedis.Jedis;

/**
 * 基于Redis的Master选举。
 * 
 * 当前实例用setNX争夺Master成功后，会不断的更新expireTime，直到实例挂掉。
 * 其它Slave会定时检查Master Key是否已过期，如果已过期则重新发起争夺。
 * 
 * 当前实例挂掉的情况下，可能两倍的检查周期内没有Master。
 * 
 * @author Chris
 *
 */
public class MasterElector {
    private static final Logger logger = LoggerFactory.getLogger(MasterElector.class);

    private RedisConnectionPool redisConnectionPool = null;
    private int interval;
    private String masterName;

    private AtomicBoolean master = new AtomicBoolean(false);
    private String serverId;
    private int expire;

    private ScheduledExecutorService scheduledExecutor;
    @SuppressWarnings("rawtypes")
    private ScheduledFuture electorJob;

    public MasterElector(RedisConnectionPool redisPool, int interval) {
        this(redisPool, interval, "master");
    }

    public MasterElector(RedisConnectionPool redisPool, int interval, String masterName) {
        this.redisConnectionPool = redisPool;
        this.interval = interval;
        this.masterName = masterName;

        this.expire = interval + (interval / 2);
    }

    /**
     * 当前实例是否master。
     */
    public boolean isMaster() {
        return master.get();
    }

    /**
     * 启动抢夺线程。
     */
    public void start() {
        serverId = generateServerId();

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        electorJob = scheduledExecutor.scheduleAtFixedRate(new ElectorTask(), 0, interval, TimeUnit.SECONDS);

        logger.info("Master[{}] elector start, current server ID: {}", masterName, serverId);
    }

    /**
     * 停止抢夺线程。
     * 
     * 若当前实例是Master，则删除key。
     */
    public void stop() {
        if (master.get()) {
            try {
                Jedis jedis = redisConnectionPool.getMasterPool().getResource();
                jedis.del(masterName);
            } catch (Exception e) {
                logger.error("Give up master role failed.", e);
            }

            master.set(false);
        }

        electorJob.cancel(false);

        if (scheduledExecutor != null) {
            try {
                scheduledExecutor.shutdownNow();

                if (!scheduledExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    logger.warn("Elector thread pool did not terminated.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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

    private class ElectorTask implements Runnable {

        public void run() {
            Jedis jedis = null;

            try {
                jedis = redisConnectionPool.getMasterPool().getResource();

                String currentMaster = jedis.get(masterName);

                if (currentMaster == null) {
                    String result = jedis.set(masterName, serverId, "NX", "EX", expire);
                    if (result != null && result.equals("OK")) {
                        master.set(true);
                        logger.info("Grabed master[{}]: {}", masterName, serverId);

                        return;
                    }

                    master.set(false);
                    return;
                }

                logger.info("Current master[{}] is {}", masterName, currentMaster);

                if (serverId.equals(currentMaster)) {
                    jedis.expire(masterName, expire);
                    master.set(true);
                    return;
                }

                master.set(false);
            } catch (Throwable e) {
                logger.error("Execute task error.", e);
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
    }

}
