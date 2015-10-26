package com.oasis.redis.pool;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

@Ignore
public class RedisReplicaConnectionPoolTest {

    private static HostAndPort redisMaster = new HostAndPort("127.0.0.1", 6379);
    private static HostAndPort redisSlave = new HostAndPort("127.0.0.1", 6380);

    private RedisReplicaConnectionPool connPool;

    @Before
    public void setUp() {
        List<String> redisServers = new ArrayList<String>();
        redisServers.add(redisMaster.toString());
        redisServers.add(redisSlave.toString());

        connPool = new RedisReplicaConnectionPool(redisServers);
        connPool.init();
    }

    @Test
    public void testStringAction() throws Exception {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        Jedis writeJedis = connPool.getMasterPool().getResource();
        Jedis readJedis = connPool.getSlavePool().getResource();

        writeJedis.set(key, value);
        Thread.sleep(1000L);
        assertEquals(readJedis.get(key), value);
        assertNull(readJedis.get(notExistKey));

        writeJedis.del(key);
        Thread.sleep(1000L);
        assertNull(readJedis.get(key));
    }

    @After
    public void tearDown() {
        if (connPool != null) {
            connPool.destroy();
        }
    }
}
