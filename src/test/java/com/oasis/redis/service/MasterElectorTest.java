package com.oasis.redis.service;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.oasis.redis.pool.RedisConnectionPool;
import com.oasis.redis.pool.RedisSingleConnectionPool;

import redis.clients.jedis.HostAndPort;

@Ignore
public class MasterElectorTest {

    private static HostAndPort redisServer = new HostAndPort("127.0.0.1", 6379);

    private RedisConnectionPool redisConnectionPool;

    @Before
    public void setUp() {
        redisConnectionPool = new RedisSingleConnectionPool(redisServer.toString());
        redisConnectionPool.init();
    }

    @Test
    public void testElector() throws Exception {
        MasterElector elector = new MasterElector(redisConnectionPool, 5);
        elector.start();

        Thread.sleep(2000L);

        assertTrue(elector.isMaster());

        elector.stop();

        Thread.sleep(1000L);

        assertFalse(elector.isMaster());
    }

    @After
    public void tearDown() {
        if (redisConnectionPool != null) {
            redisConnectionPool.destroy();
        }
    }

}
