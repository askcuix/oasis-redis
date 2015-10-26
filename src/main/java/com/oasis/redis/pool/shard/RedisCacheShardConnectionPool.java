package com.oasis.redis.pool.shard;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.pool.RedisPool;
import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Hashing;

/**
 * Connection pool for Redis cache sharding deployment model.
 * 
 * @author Chris
 *
 */
public class RedisCacheShardConnectionPool extends RedisShardConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisCacheShardConnectionPool.class);

    private Map<String, List<String>> shardServers;

    private final Hashing algo = Hashing.MURMUR_HASH;
    private TreeMap<Long, String> nodes = new TreeMap<Long, String>();

    private final ConcurrentMap<String, RedisShardPool> redisMasterPoolMap = new ConcurrentHashMap<String, RedisShardPool>();
    private final ConcurrentMap<String, List<RedisShardPool>> redisSlavePoolsMap = new ConcurrentHashMap<String, List<RedisShardPool>>();
    private final ConcurrentMap<String, AtomicInteger> seqMap = new ConcurrentHashMap<String, AtomicInteger>();

    public RedisCacheShardConnectionPool(Map<String, List<String>> shardServers) {
        this(shardServers, new JedisPoolConfig());
    }

    public RedisCacheShardConnectionPool(Map<String, List<String>> shardServers, JedisPoolConfig poolConfig) {
        if (shardServers == null || shardServers.isEmpty()) {
            throw new IllegalArgumentException("Redis shard servers is required!");
        }

        this.shardServers = shardServers;
        this.poolConfig = poolConfig;
    }

    @Override
    public void init() {
        logger.info("Initialize Redis cache shard connection pool start......");

        super.init();

        for (Entry<String, List<String>> shard : shardServers.entrySet()) {
            String shardName = shard.getKey();

            List<String> shardServer = shard.getValue();
            for (String redis : shardServer) {
                HostAndPort server = RedisUtil.toHostAndPort(redis);

                Jedis jedis = null;
                try {
                    jedis = new Jedis(server.getHost(), server.getPort(), timeout);
                    jedis.connect();

                    if (null != password) {
                        jedis.auth(password);
                    }

                    String info = jedis.info();
                    boolean isMaster = RedisUtil.isMaster(info);

                    RedisShardPool pool = new RedisShardPool(shardName, server.getHost(), server.getPort(), timeout,
                            password, database, poolConfig);
                    if (isMaster) {
                        if (!redisMasterPoolMap.containsKey(shardName)) {
                            redisMasterPoolMap.put(shardName, pool);
                        } else {
                            logger.warn("Duplicate redis shard[{}] master: {}, add to slave.", shardName,
                                    redis.toString());

                            redisSlavePoolsMap.getOrDefault(shardName, new ArrayList<RedisShardPool>()).add(pool);
                        }
                    } else {
                        redisSlavePoolsMap.getOrDefault(shardName, new ArrayList<RedisShardPool>()).add(pool);
                    }
                } catch (Exception e) {
                    logger.warn("Initial redis[" + redis.toString() + "] error.", e);
                    continue;
                } finally {
                    RedisUtil.destroyJedis(jedis);
                }
            }

            if (redisMasterPoolMap.get(shardName) == null) {
                logger.error("No available redis master for shard[{}], removed from sharding.", shardName);

                redisMasterPoolMap.remove(shardName);

                List<RedisShardPool> slavePools = redisSlavePoolsMap.get(shardName);
                for (RedisShardPool pool : slavePools) {
                    try {
                        pool.destroy();
                    } catch (Exception e) {
                        logger.warn("Destroy unavailable shard[" + shardName + "] slave pool error.", e);
                    }
                }
                slavePools.clear();
                redisSlavePoolsMap.remove(shardName);
            }

        }

        initNodes(redisMasterPoolMap.keySet());

        logger.info("Initialized Redis cache shard connection pool.");
    }

    private void initNodes(Set<String> shardNameSet) {
        nodes.clear();

        for (String shardName : shardNameSet) {
            for (int n = 0; n < 160; n++) {
                nodes.put(this.algo.hash(shardName + "-NODE-" + n), shardName);
            }
        }
    }

    @Override
    public String getShard(String key) {
        String shardName = null;

        SortedMap<Long, String> tail = nodes.tailMap(algo.hash(key));
        if (tail.isEmpty()) {
            // the last node, back to first.
            shardName = nodes.get(nodes.firstKey());
        } else {
            shardName = tail.get(tail.firstKey());
        }

        logger.debug("Key[{}] in shard: {}", key, shardName);

        return shardName;
    }

    @Override
    public RedisShardPool getMasterPool(String key) {
        String shardName = getShard(key);
        RedisShardPool redisMasterPool = redisMasterPoolMap.get(shardName);

        if (redisMasterPool == null) {
            logger.error("No available redis master!");
            return null;
        }

        return redisMasterPool;
    }

    @Override
    public RedisShardPool getSlavePool(String key) {
        String shardName = getShard(key);
        List<RedisShardPool> redisSlavePool = redisSlavePoolsMap.get(shardName);

        if (redisSlavePool.isEmpty()) {
            logger.warn("No available redis slave!");

            if (redisMasterPoolMap.get(shardName) != null) {
                return redisMasterPoolMap.get(shardName);
            }

            redisMasterPoolMap.remove(shardName);
            redisSlavePoolsMap.remove(shardName);

            initNodes(redisSlavePoolsMap.keySet());

            return null;
        } else if (redisSlavePool.size() == 1) {
            return redisSlavePool.get(0);
        }

        AtomicInteger sequences = seqMap.getOrDefault(shardName, new AtomicInteger());
        int index = sequences.getAndIncrement();
        if ((index >= Integer.MAX_VALUE) || (index < 0)) {
            sequences.set(0);
            index = sequences.getAndIncrement();
        }

        return redisSlavePool.get(index % redisSlavePool.size());
    }

    @Override
    public void returnBrokenPool(RedisShardPool pool) {
        if (pool == null) {
            return;
        }

        try {
            Jedis jedis = pool.getResource();
            boolean alive = RedisUtil.ping(jedis);

            if (alive) {
                logger.info("[{}] Redis[{}:{}] still alive, retain to use.", pool.getShardName(), pool.getHost(),
                        pool.getPort());
                return;
            }

            RedisShardPool redisMasterPool = redisMasterPoolMap.get(pool.getShardName());
            if (redisMasterPool != null && (redisMasterPool.getHost().equals(pool.getHost())
                    && redisMasterPool.getPort() == pool.getPort())) {
                try {
                    redisMasterPool.destroy();
                } catch (Exception e) {
                    logger.warn("[" + pool.getShardName() + "] Destroy unavailable master pool error.", e);
                }
                redisMasterPool = null;

                logger.warn("[{}] Redis master[{}:{}] is not available and removed it.", pool.getShardName(),
                        pool.getHost(), pool.getPort());
                return;
            }

            Iterator<RedisShardPool> slavePoolIterator = redisSlavePoolsMap
                    .getOrDefault(pool.getShardName(), new ArrayList<RedisShardPool>()).iterator();
            while (slavePoolIterator.hasNext()) {
                RedisPool slave = slavePoolIterator.next();

                if (slave != null && (slave.getHost().equals(pool.getHost()) && slave.getPort() == pool.getPort())) {
                    try {
                        slave.destroy();
                    } catch (Exception e) {
                        logger.warn("[" + pool.getShardName() + "] Destroy unavailable slave pool error.", e);
                    }
                    slavePoolIterator.remove();

                    logger.warn("[{}] Redis slave[{}:{}] is not available and removed it.", pool.getShardName(),
                            pool.getHost(), pool.getPort());
                    break;
                }

            }
        } catch (Exception e) {
            logger.error("Handle borken connection pool error.", e);
        }
    }

    @Override
    public void destroy() {
        logger.info("Destroy redis cache shard connection pool start......");

        super.destroy();

        for (RedisShardPool redisMasterPool : redisMasterPoolMap.values()) {
            if (redisMasterPool != null) {
                try {
                    redisMasterPool.destroy();

                    logger.info("[{}] Redis master pool[{}:{}] destroyed.", redisMasterPool.getShardName(),
                            redisMasterPool.getHost(), redisMasterPool.getPort());
                } catch (Exception e) {
                    logger.warn("Destroy shard[" + redisMasterPool.getShardName() + "] master pool["
                            + redisMasterPool.getHost() + ":" + redisMasterPool.getPort() + "] error.", e);
                }
            }
        }
        redisMasterPoolMap.clear();

        for (List<RedisShardPool> redisSlavePool : redisSlavePoolsMap.values()) {
            if (!redisSlavePool.isEmpty()) {
                String shardName = null;
                for (RedisShardPool pool : redisSlavePool) {
                    shardName = pool.getShardName();
                    try {
                        pool.destroy();
                        logger.info("[{}] Redis slave pool[{}:{}] destroyed.", pool.getShardName(), pool.getHost(),
                                pool.getPort());
                    } catch (Exception e) {
                        logger.warn("Destroy shard[" + pool.getShardName() + "] slave pool[" + pool.getHost() + ":"
                                + pool.getPort() + "] error.", e);
                    }
                }

                redisSlavePool.clear();

                logger.info("[{}] Redis slave pools destroyed.", shardName);
            }
        }
        redisSlavePoolsMap.clear();

        logger.info("Redis cache shard connection pool destroyed!");
    }

}
