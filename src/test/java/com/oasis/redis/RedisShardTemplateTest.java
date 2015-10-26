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
import com.oasis.redis.pool.RedisPool;
import com.oasis.redis.pool.shard.RedisShardConnectionPool;

import redis.clients.jedis.Jedis;

public class RedisShardTemplateTest {
    private RedisShardTemplate shardTemplate;

    @ClassRule
    public static EmbeddedRedis embeddedRedisRule = EmbeddedRedisRuleBuilder.newEmbeddedRedisRule().build();

    @Before
    public void setUp() {
        Jedis embeddedJedis = RedirectProxy.createProxy(NoArgsJedis.class, new EmbeddedJedisExt());

        RedisShardConnectionPool connPool = Mockito.mock(RedisShardConnectionPool.class);
        RedisPool redisPool = Mockito.mock(RedisPool.class);

        Mockito.when(connPool.getMasterPool(Mockito.anyString())).thenReturn(redisPool);
        Mockito.when(connPool.getSlavePool(Mockito.anyString())).thenReturn(redisPool);

        Mockito.when(redisPool.getResource()).thenReturn(embeddedJedis);

        shardTemplate = new RedisShardTemplate(connPool);
    }

    @Test
    public void stringActions() {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        shardTemplate.set(key, value);

        // get/set
        assertEquals(shardTemplate.get(key), value);
        assertNull(shardTemplate.get(notExistKey));

        // getAsInt/getAsLong
        assertTrue(shardTemplate.getAsInt(key) == 123);
        assertNull(shardTemplate.getAsInt(notExistKey));

        assertEquals(shardTemplate.getAsLong(key).longValue(), 123L);
        assertNull(shardTemplate.getAsLong(notExistKey));

        // setnx
        assertFalse(shardTemplate.setnx(key, value));
        assertTrue(shardTemplate.setnx(key + "nx", value));

        // incr/decr
        shardTemplate.incr(key);
        assertEquals(shardTemplate.get(key), "124");
        shardTemplate.decr(key);
        assertEquals(shardTemplate.get(key), "123");

        // del
        assertTrue(shardTemplate.del(key));
        assertFalse(shardTemplate.del(notExistKey));
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
        shardTemplate.hset(key, field1, value1);
        assertEquals(shardTemplate.hget(key, field1), value1);
        assertNull(shardTemplate.hget(key, notExistField));

        // hmget/hmset
        Map<String, String> map = new HashMap<String, String>();
        map.put(field1, value1);
        map.put(field2, value2);
        shardTemplate.hmset(key, map);

        List<String> valueList = shardTemplate.hmget(key, new String[] { field1, field2 });
        assertTrue(valueList.contains(value1));
        assertTrue(valueList.contains(value2));

        // hkeys
        Set<String> fieldSet = shardTemplate.hkeys(key);
        assertTrue(fieldSet.contains(field1));
        assertTrue(fieldSet.contains(field2));

        // hdel
        assertEquals(shardTemplate.hdel(key, field1).longValue(), 1L);
        assertNull(shardTemplate.hget(key, field1));
    }

    @Test
    public void listActions() {
        String key = "test.list.key";
        String value = "123";
        String value2 = "456";

        // push/pop single element
        shardTemplate.lpush(key, value);
        assertEquals(shardTemplate.llen(key).longValue(), 1L);
        assertEquals(shardTemplate.rpop(key), value);
        assertNull(shardTemplate.rpop(key));

        // push/pop two elements
        shardTemplate.lpush(key, value);
        shardTemplate.lpush(key, value2);
        assertEquals(shardTemplate.llen(key).longValue(), 2L);
        assertEquals(shardTemplate.rpop(key), value);
        assertEquals(shardTemplate.rpop(key), value2);

        // remove elements
        shardTemplate.lpush(key, value);
        shardTemplate.lpush(key, value);
        shardTemplate.lpush(key, value);
        assertEquals(shardTemplate.llen(key).longValue(), 3L);
        assertTrue(shardTemplate.lremFirst(key, value));
        assertEquals(shardTemplate.llen(key).longValue(), 2L);
        assertTrue(shardTemplate.lremAll(key, value));
        assertEquals(shardTemplate.llen(key).longValue(), 0L);
        assertFalse(shardTemplate.lremAll(key, value));
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
        assertTrue(shardTemplate.zadd(key, score1, member));
        assertTrue(shardTemplate.zadd(key, score2, member2));

        // zcard
        assertEquals(shardTemplate.zcard(key).longValue(), 2L);
        assertEquals(shardTemplate.zcard(key + "not.exist").longValue(), 0L);

        // zrem
        assertTrue(shardTemplate.zrem(key, member2));
        assertEquals(shardTemplate.zcard(key).longValue(), 1L);
        assertFalse(shardTemplate.zrem(key, member2 + "not.exist"));

        // unique & zscore
        assertFalse(shardTemplate.zadd(key, score11, member));
        assertEquals(shardTemplate.zcard(key).longValue(), 1L);
        assertTrue(shardTemplate.zscore(key, member).doubleValue() == score11);
        assertNull(shardTemplate.zscore(key, member + "not.exist"));
    }

}
