package com.oasis.redis.pool.sentinel;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.oasis.redis.sentinel.SentinelTestUtil;

@Ignore
public class RedisSentinelConnectionPoolTest {
    private static final String MASTER_NAME = "haSentinel";

    private static HostAndPort sentinel1 = new HostAndPort("10.21.8.225", 26379);
    private static HostAndPort sentinel2 = new HostAndPort("10.21.8.226", 26379);
    private static HostAndPort sentinel3 = new HostAndPort("10.21.8.227", 26379);

    private static List<String> sentinels = new ArrayList<String>();
    private static Jedis sentinelJedis1;
    private static Jedis sentinelJedis2;

    @BeforeClass
    public static void setUp() {
        sentinels.add(sentinel1.toString());
        sentinels.add(sentinel2.toString());
        sentinels.add(sentinel3.toString());

        sentinelJedis1 = new Jedis(sentinel1.getHost(), sentinel1.getPort());
        sentinelJedis2 = new Jedis(sentinel2.getHost(), sentinel2.getPort());
    }

    @Test(expected = JedisConnectionException.class)
    public void initializeWrongSentinel() {
        List<String> wrongSentinels = new ArrayList<String>();
        wrongSentinels.add(new HostAndPort("localhost", 65432).toString());
        wrongSentinels.add(new HostAndPort("localhost", 65431).toString());

        RedisSentinelConnectionPool pool = new RedisSentinelConnectionPool(MASTER_NAME, wrongSentinels);

        pool.init();

        pool.destroy();
    }

    @Test
    public void testStringAction() throws Exception {
        RedisSentinelConnectionPool connPool = new RedisSentinelConnectionPool(MASTER_NAME, sentinels);
        connPool.init();

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

        connPool.destroy();
    }

    @Test
    public void testFailover() throws Exception {
        RedisSentinelConnectionPool connPool = new RedisSentinelConnectionPool(MASTER_NAME, sentinels);
        connPool.init();

        forceFailover(connPool);

        connPool.destroy();
    }

    @Test
    public void testOperationAfterFailover() throws Exception {
        RedisSentinelConnectionPool connPool = new RedisSentinelConnectionPool(MASTER_NAME, sentinels);
        connPool.init();

        String key = "test.string.key";
        String value = "123";

        Jedis writeJedis = connPool.getMasterPool().getResource();
        Jedis readJedis = connPool.getSlavePool().getResource();

        writeJedis.set(key, value);
        Thread.sleep(1000L);
        assertEquals(readJedis.get(key), value);

        forceFailover(connPool);

        writeJedis = connPool.getMasterPool().getResource();
        readJedis = connPool.getSlavePool().getResource();

        assertEquals(readJedis.get(key), value);

        writeJedis.del(key);
        Thread.sleep(2000L);
        assertNull(readJedis.get(key));

        connPool.destroy();
    }

    private void forceFailover(RedisSentinelConnectionPool pool) throws InterruptedException {
        HostAndPort oldMaster = pool.getCurrentMaster();

        // jedis connection should be master
        Jedis beforeFailoverJedis = pool.getMasterPool().getResource();
        assertEquals("PONG", beforeFailoverJedis.ping());

        waitForFailover(pool);

        Jedis afterFailoverJedis = pool.getMasterPool().getResource();
        assertEquals("PONG", afterFailoverJedis.ping());

        System.out.println("Old master: " + oldMaster + ", New master: " + pool.getCurrentMaster());

        // returning both connections to the pool should not throw
        beforeFailoverJedis.close();
        afterFailoverJedis.close();
    }

    private void waitForFailover(RedisSentinelConnectionPool pool) throws InterruptedException {
        HostAndPort newMaster = SentinelTestUtil.waitForNewPromotedMaster(MASTER_NAME, sentinelJedis1, sentinelJedis2);

        waitForJedisSentinelPoolRecognizeNewMaster(pool, newMaster);
    }

    private void waitForJedisSentinelPoolRecognizeNewMaster(RedisSentinelConnectionPool pool, HostAndPort newMaster)
            throws InterruptedException {

        while (true) {
            HostAndPort currentHostMaster = pool.getCurrentMaster();

            if (newMaster.equals(currentHostMaster)) {
                break;
            }

            System.out.println("RedisSentinelConnectionPool master is not yet changed, sleep...");

            Thread.sleep(100);
        }
    }

}
