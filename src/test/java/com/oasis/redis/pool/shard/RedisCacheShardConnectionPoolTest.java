package com.oasis.redis.pool.shard;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

@Ignore
public class RedisCacheShardConnectionPoolTest {

    private static String redisShard1Name = "shard1";
    private static HostAndPort redisShard1Master = new HostAndPort("10.21.8.225", 6380);
    private static HostAndPort redisShard1Slave = new HostAndPort("10.21.8.225", 6381);

    private static String redisShard2Name = "shard2";
    private static HostAndPort redisShard2Master = new HostAndPort("10.21.8.226", 6380);
    private static HostAndPort redisShard2Slave = new HostAndPort("10.21.8.226", 6381);

    private static String redisShard3Name = "shard3";
    private static HostAndPort redisShard3Master = new HostAndPort("10.21.8.227", 6380);
    private static HostAndPort redisShard3Slave = new HostAndPort("10.21.8.227", 6381);

    private static RedisCacheShardConnectionPool connPool;

    @BeforeClass
    public static void setUp() {
        Map<String, List<String>> shardMap = new HashMap<String, List<String>>();

        List<String> shard1 = new ArrayList<String>();
        shard1.add(redisShard1Master.toString());
        shard1.add(redisShard1Slave.toString());
        shardMap.put(redisShard1Name, shard1);

        List<String> shard2 = new ArrayList<String>();
        shard2.add(redisShard2Master.toString());
        shard2.add(redisShard2Slave.toString());
        shardMap.put(redisShard2Name, shard2);

        List<String> shard3 = new ArrayList<String>();
        shard3.add(redisShard3Master.toString());
        shard3.add(redisShard3Slave.toString());
        shardMap.put(redisShard3Name, shard3);

        connPool = new RedisCacheShardConnectionPool(shardMap);
        connPool.init();
    }
    
    @Test
    public void testShard() {
        String key1 = "test.key1";
        String key2 = "test.key2";
        
        String shard1 = connPool.getShard(key1);
        String shard2 = connPool.getShard(key2);
        
        assertNotEquals(shard1, shard2);
    }

    @Test
    public void testStringAction() throws Exception {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        Jedis writeJedis = connPool.getMasterPool(key).getResource();
        Jedis readJedis = connPool.getSlavePool(key).getResource();

        writeJedis.set(key, value);
        Thread.sleep(1000L);
        assertEquals(readJedis.get(key), value);
        assertNull(readJedis.get(notExistKey));

        writeJedis.del(key);
        Thread.sleep(1000L);
        assertNull(readJedis.get(key));
    }

    @AfterClass
    public static void tearDown() {
        if (connPool != null) {
            connPool.destroy();
        }
    }
}
