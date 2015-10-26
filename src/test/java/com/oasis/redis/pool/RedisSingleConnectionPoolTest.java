package com.oasis.redis.pool;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

@Ignore
public class RedisSingleConnectionPoolTest {

    private static HostAndPort redisServer = new HostAndPort("127.0.0.1", 6379);

    private RedisSingleConnectionPool connPool;
    private Jedis jedis;

    @Before
    public void setUp() {
        connPool = new RedisSingleConnectionPool(redisServer.toString());
        connPool.init();

        jedis = connPool.getRedisPool().getResource();

        Jedis j = new Jedis(redisServer.getHost(), redisServer.getPort());
        j.connect();
        j.flushAll();
        j.disconnect();
        j.close();
    }

    @Test
    public void testStringAction() {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        jedis.set(key, value);
        assertEquals(jedis.get(key), value);
        assertNull(jedis.get(notExistKey));
    }

    @After
    public void tearDown() {
        if (connPool != null) {
            connPool.destroy();
        }
    }

}
