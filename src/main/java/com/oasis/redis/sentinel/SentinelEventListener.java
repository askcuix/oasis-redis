package com.oasis.redis.sentinel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.util.CollectionUtil;
import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class SentinelEventListener {
    private static final Logger logger = LoggerFactory.getLogger(SentinelEventListener.class);

    private SentinelEventReceiver eventReceiver;
    private SentinelWorker worker;

    private String masterName;
    private List<String> sentinels;

    public SentinelEventListener(SentinelEventReceiver eventReceiver) {
        this.eventReceiver = eventReceiver;

        this.masterName = eventReceiver.getMasterName();
        this.sentinels = eventReceiver.getSentinels();

        if (masterName == null || masterName.trim().equals("")) {
            throw new IllegalArgumentException("MasterName is required!");
        }

        if ((sentinels == null) || sentinels.isEmpty()) {
            throw new IllegalArgumentException("Sentinels is required!");
        }
    }

    public void start() {
        HostAndPort master = null;
        List<HostAndPort> slaveList = null;
        boolean sentinelAvailable = false;

        logger.info("Trying to find master from available sentinels...");

        for (String sentinel : sentinels) {
            HostAndPort hap = RedisUtil.toHostAndPort(sentinel);

            logger.info("Connecting to sentinel {}", hap);

            Jedis sentinelJedis = null;
            try {
                sentinelJedis = new Jedis(hap.getHost(), hap.getPort());

                master = RedisUtil.lookupMasterBySentinel(masterName, sentinelJedis);

                // connected to sentinel...
                sentinelAvailable = true;

                if (master == null) {
                    logger.warn("Can not get master[{}] addr from sentinel: {}. ", masterName, hap.toString());

                    continue;
                }
                logger.info("Found redis master[{}] at {}", masterName, master.toString());

                slaveList = RedisUtil.lookupSlavesBySentinel(masterName, sentinelJedis);
                if (slaveList == null) {
                    slaveList = new ArrayList<HostAndPort>();
                }
                logger.info("Found redis slaves[{}] at {}", masterName, CollectionUtil.join(slaveList, ", "));

                break;
            } catch (JedisConnectionException e) {
                logger.warn("Can't connect to sentinel: {}. Trying next one.", hap.toString());
            } finally {
                RedisUtil.destroyJedis(sentinelJedis);
            }
        }

        if (master == null) {
            if (sentinelAvailable) {
                // can connect to sentinel, but master name seems to not
                // monitored
                throw new JedisException("Can connect to sentinel, but " + masterName + " seems not monitored...");
            } else {
                throw new JedisConnectionException(
                        "All sentinels down, cannot determine where is master " + masterName + " running...");
            }
        }

        logger.info("Redis master running at {}, starting Sentinel listeners...", master.toString());

        // initialize pool
        eventReceiver.refreshMasterPool(master);
        eventReceiver.refreshSlavePool(slaveList);

        // start to listen
        worker = new SentinelWorker();
        worker.start();

        logger.info("SentinelEventListener start to listening.");
    }

    public void stop() {
        if (worker == null) {
            return;
        }

        worker.shutdown();

        // wait for the SentinelWorker thread finish
        try {
            logger.info("Waiting for SentinelEventListener stop Listening...");

            worker.join();

            logger.info("SentinelEventListener stoped.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private class SentinelWorker extends Thread {
        private static final String THREAD_NAME_PREFIX = "SentinelEventListener-";

        private HostAndPort currentSentinel;
        private Jedis sentinelJedis;
        private JedisPubSub subscriber;

        private AtomicBoolean running = new AtomicBoolean(true);
        private int subscribeRetryWaitTimeMillis = 5000;

        public SentinelWorker() {
            super(THREAD_NAME_PREFIX + eventReceiver.getMasterName());
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    logger.info("Start to find available sentinel to listen event...");
                    sentinelJedis = pickupSentinel(eventReceiver.getSentinels());

                    if (sentinelJedis != null) {
                        logger.info("Register event listener to sentinel: {}", currentSentinel);

                        subscriber = new SentinelEventSubscriber(currentSentinel, eventReceiver);

                        sentinelJedis.subscribe(subscriber, "+switch-master", "+sdown", "-sdown", "+slave");
                    } else {
                        logger.info("All sentinels down, sleep {}ms and try to connect again.",
                                subscribeRetryWaitTimeMillis);

                        sleep(subscribeRetryWaitTimeMillis);
                    }
                } catch (JedisConnectionException e) {
                    if (running.get()) {
                        logger.warn("Lost connection to sentinel at {}. Sleeping {}ms and retry.", currentSentinel,
                                subscribeRetryWaitTimeMillis);

                        sleep(subscribeRetryWaitTimeMillis);
                    }
                } catch (Exception e) {
                    logger.error("Sentinel event listener error.", e);
                    sleep(1000);
                } finally {
                    RedisUtil.destroyJedis(sentinelJedis);
                    sentinelJedis = null;
                }
            }
        }

        /**
         * Shutdown listener.
         */
        public void shutdown() {
            try {
                logger.info("Shutting down sentinel listener on {}", currentSentinel);

                running.set(false);
                this.interrupt();

                if (subscriber != null) {
                    subscriber.unsubscribe();
                }

                logger.info("Sentinel listener stoped on {}", currentSentinel);
            } catch (Exception e) {
                logger.error("Caught exception while shutting down sentinel listener.", e);
            }
        }

        /**
         * Pick up available sentinel.
         * 
         * @return sentinel
         */
        private Jedis pickupSentinel(List<String> sentinelList) {
            for (String sentinel : sentinelList) {
                HostAndPort hap = RedisUtil.toHostAndPort(sentinel);

                try {
                    Jedis jedis = new Jedis(hap.getHost(), hap.getPort());

                    if (RedisUtil.ping(jedis)) {
                        currentSentinel = hap;

                        return jedis;
                    }
                } catch (Exception e) {
                    logger.warn("Lost connection to sentinel at {}.", hap.toString());
                }
            }

            return null;
        }

        private void sleep(int millseconds) {
            try {
                Thread.sleep(millseconds);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

    }

}
