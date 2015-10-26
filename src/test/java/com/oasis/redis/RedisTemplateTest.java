package com.oasis.redis;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import com.lordofthejars.nosqlunit.proxy.RedirectProxy;
import com.lordofthejars.nosqlunit.redis.EmbeddedRedis;
import com.lordofthejars.nosqlunit.redis.EmbeddedRedis.EmbeddedRedisRuleBuilder;
import com.lordofthejars.nosqlunit.redis.embedded.NoArgsJedis;
import com.oasis.redis.pool.RedisConnectionPool;
import com.oasis.redis.pool.RedisPool;

import redis.clients.jedis.Jedis;

public class RedisTemplateTest {
    private RedisTemplate redisTemplate;

    @ClassRule
    public static EmbeddedRedis embeddedRedisRule = EmbeddedRedisRuleBuilder.newEmbeddedRedisRule().build();

    @Before
    public void setUp() {
        Jedis embeddedJedis = RedirectProxy.createProxy(NoArgsJedis.class, new EmbeddedJedisExt()); 

        RedisConnectionPool connPool = Mockito.mock(RedisConnectionPool.class);
        RedisPool redisPool = Mockito.mock(RedisPool.class);

        Mockito.when(connPool.getMasterPool()).thenReturn(redisPool);
        Mockito.when(connPool.getSlavePool()).thenReturn(redisPool);

        Mockito.when(redisPool.getResource()).thenReturn(embeddedJedis);

        redisTemplate = new RedisTemplate(connPool);
    }

    @Test
    public void stringActions() {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        redisTemplate.set(key, value);

        // get/set
        assertEquals(redisTemplate.get(key), value);
        assertNull(redisTemplate.get(notExistKey));

        // getAsInt/getAsLong
        assertTrue(redisTemplate.getAsInt(key) == 123);
        assertNull(redisTemplate.getAsInt(notExistKey));

        assertEquals(redisTemplate.getAsLong(key).longValue(), 123L);
        assertNull(redisTemplate.getAsLong(notExistKey));

        // setnx
        assertFalse(redisTemplate.setnx(key, value));
        assertTrue(redisTemplate.setnx(key + "nx", value));

        // incr/decr
        redisTemplate.incr(key);
        assertEquals(redisTemplate.get(key), "124");
        redisTemplate.decr(key);
        assertEquals(redisTemplate.get(key), "123");

        // del
        assertTrue(redisTemplate.del(key));
        assertFalse(redisTemplate.del(notExistKey));
    }

    @Test
    public void hashActions() {
        String key = "test.string.key";
        String field1 = "aa";
        String field2 = "bb";
        String notExistField = field1 + "not.exist";
        String value1 = "123";
        String value2 = "456";

        // hget/hset
        redisTemplate.hset(key, field1, value1);
        assertEquals(redisTemplate.hget(key, field1), value1);
        assertNull(redisTemplate.hget(key, notExistField));

        // hmget/hmset
        Map<String, String> map = new HashMap<String, String>();
        map.put(field1, value1);
        map.put(field2, value2);
        redisTemplate.hmset(key, map);

        List<String> valueList = redisTemplate.hmget(key, new String[] { field1, field2 });
        assertTrue(valueList.contains(value1));
        assertTrue(valueList.contains(value2));

        // hkeys
        Set<String> fieldSet = redisTemplate.hkeys(key);
        assertTrue(fieldSet.contains(field1));
        assertTrue(fieldSet.contains(field2));

        // hdel
        assertEquals(redisTemplate.hdel(key, field1).longValue(), 1L);
        assertNull(redisTemplate.hget(key, field1));
    }

    @Test
    public void listActions() {
        String key = "test.list.key";
        String value = "123";
        String value2 = "456";

        // push/pop single element
        redisTemplate.lpush(key, value);
        assertEquals(redisTemplate.llen(key).longValue(), 1L);
        assertEquals(redisTemplate.rpop(key), value);
        assertNull(redisTemplate.rpop(key));

        // push/pop two elements
        redisTemplate.lpush(key, value);
        redisTemplate.lpush(key, value2);
        assertEquals(redisTemplate.llen(key).longValue(), 2L);
        assertEquals(redisTemplate.rpop(key), value);
        assertEquals(redisTemplate.rpop(key), value2);

        // remove elements
        redisTemplate.lpush(key, value);
        redisTemplate.lpush(key, value);
        redisTemplate.lpush(key, value);
        assertEquals(redisTemplate.llen(key).longValue(), 3L);
        assertTrue(redisTemplate.lremFirst(key, value));
        assertEquals(redisTemplate.llen(key).longValue(), 2L);
        assertTrue(redisTemplate.lremAll(key, value));
        assertEquals(redisTemplate.llen(key).longValue(), 0L);
        assertFalse(redisTemplate.lremAll(key, value));
    }

    @Test
    public void orderedSetActions() {
        String key = "test.orderedSet.key";
        String member = "abc";
        String member2 = "def";
        double score1 = 1;
        double score11 = 11;
        double score2 = 2;

        // zadd
        assertTrue(redisTemplate.zadd(key, score1, member));
        assertTrue(redisTemplate.zadd(key, score2, member2));

        // zcard
        assertEquals(redisTemplate.zcard(key).longValue(), 2L);
        assertEquals(redisTemplate.zcard(key + "not.exist").longValue(), 0L);

        // zrem
        assertTrue(redisTemplate.zrem(key, member2));
        assertEquals(redisTemplate.zcard(key).longValue(), 1L);
        assertFalse(redisTemplate.zrem(key, member2 + "not.exist"));

        // unique & zscore
        assertFalse(redisTemplate.zadd(key, score11, member));
        assertEquals(redisTemplate.zcard(key).longValue(), 1L);
        assertTrue(redisTemplate.zscore(key, member).doubleValue() == score11);
        assertNull(redisTemplate.zscore(key, member + "not.exist"));
    }

}
