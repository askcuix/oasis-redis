package com.oasis.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.RedisExecution.PipelineAction;
import com.oasis.redis.RedisExecution.RedisAction;
import com.oasis.redis.RedisExecution.RedisReturnAction;
import com.oasis.redis.pool.shard.RedisShardConnectionPool;
import com.oasis.redis.pool.shard.RedisShardPool;
import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

/**
 * RedisShardTemplate provide operation template for jedis connection retrieve
 * and
 * return back.
 * 
 * RedisReturnAction<T> and RedisAction callback interface provided, for
 * whether have return value.
 * 
 * Some common function provided.
 * 
 * @author Chris
 *
 */
public class RedisShardTemplate {
    private static final Logger logger = LoggerFactory.getLogger(RedisShardTemplateTest.class);

    private RedisShardConnectionPool connPool;

    public RedisShardTemplate(RedisShardConnectionPool connPool) {
        this.connPool = connPool;
    }

    public RedisShardPool getWritePool(String key) {
        RedisShardPool writePool = connPool.getMasterPool(key);
        if (writePool == null) {
            throw new IllegalStateException("[" + key + "] Redis Master pool is unavailable.");
        }

        return writePool;
    }

    public RedisShardPool getReadPool(String key) {
        RedisShardPool readPool = connPool.getSlavePool(key);
        if (readPool == null) {
            throw new IllegalStateException("[" + key + "] Redis Slave pool is unavailable.");
        }

        return readPool;
    }

    /**
     * Execute a call back write action with result.
     */
    public <T> T executeWrite(String key, RedisReturnAction<T> redisAction) throws JedisException {
        Jedis jedis = null;
        RedisShardPool redisPool = null;
        boolean broken = false;

        try {
            redisPool = getWritePool(key);
            jedis = redisPool.getResource();
            return redisAction.execute(jedis);
        } catch (JedisException e) {
            logger.error("Redis connection error.", e);
            broken = true;

            throw e;
        } finally {
            closeResource(redisPool, jedis, broken);
        }
    }

    /**
     * Execute a call back write action without result.
     */
    public void executeWrite(String key, RedisAction redisAction) throws JedisException {
        Jedis jedis = null;
        RedisShardPool redisPool = null;
        boolean broken = false;

        try {
            redisPool = getWritePool(key);
            jedis = redisPool.getResource();
            redisAction.execute(jedis);
        } catch (JedisException e) {
            logger.error("Redis connection error.", e);
            broken = true;

            throw e;
        } finally {
            closeResource(redisPool, jedis, broken);
        }
    }

    /**
     * Execute a call back read action.
     */
    public <T> T executeRead(String key, RedisReturnAction<T> redisAction) throws JedisException {
        Jedis jedis = null;
        RedisShardPool redisPool = null;
        boolean broken = false;

        try {
            redisPool = getReadPool(key);
            jedis = redisPool.getResource();
            return redisAction.execute(jedis);
        } catch (JedisException e) {
            logger.error("Redis connection error.", e);
            broken = true;

            throw e;
        } finally {
            closeResource(redisPool, jedis, broken);
        }
    }

    /**
     * Execute with a call back action with result in pipeline.
     */
    public List<Object> doPiplineAndReturn(String key, PipelineAction pipelineAction) throws JedisException {
        Jedis jedis = null;
        RedisShardPool redisPool = null;
        boolean broken = false;

        try {
            redisPool = getWritePool(key);
            jedis = redisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            pipelineAction.execute(pipeline);
            return pipeline.syncAndReturnAll();
        } catch (JedisException e) {
            logger.error("Redis connection error.", e);
            broken = true;

            throw e;
        } finally {
            closeResource(redisPool, jedis, broken);
        }
    }

    /**
     * Execute with a call back action without result in pipeline.
     */
    public void doPipline(String key, PipelineAction pipelineAction) throws JedisException {
        Jedis jedis = null;
        RedisShardPool redisPool = null;
        boolean broken = false;

        try {
            redisPool = getWritePool(key);
            jedis = redisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            pipelineAction.execute(pipeline);
            pipeline.sync();
        } catch (JedisException e) {
            logger.error("Redis connection error.", e);
            broken = true;

            throw e;
        } finally {
            closeResource(redisPool, jedis, broken);
        }
    }

    /**
     * Return jedis connection to the pool, call different return methods
     * depends on the connection broken status.
     */
    public void closeResource(RedisShardPool redisPool, Jedis jedis, boolean conectionBroken) {
        if ((redisPool == null) || redisPool.isClosed()) {
            return;
        }

        try {
            jedis.close();

            if (conectionBroken) {
                connPool.returnBrokenPool(redisPool);
            }
        } catch (Exception e) {
            logger.error("return back jedis failed, will fore close the jedis.", e);
            RedisUtil.destroyJedis(jedis);
        }

    }

    // / Common Actions ///

    /**
     * Remove the specified keys. If a given key does not exist no operation is
     * performed for this key.
     * 
     * return false if one of the key is not exist.
     */
    public Boolean del(final String key) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.del(key) == 1 ? true : false;
            }
        });
    }

    /**
     * Test if the specified key exists. The command returns "1" if the key
     * exists, otherwise "0" is returned.
     * 
     * Note that even keys set with an empty string as value will return "1".
     * 
     * return true if the key exists, otherwise false
     */
    public Boolean exists(final String key) {
        return executeRead(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.exists(key);
            }
        });
    }

    // / String Actions ///

    /**
     * Get the value of the specified key. If the key does not exist null is
     * returned. If the value stored at key is not a string an error is returned
     * because GET can only handle string values.
     */
    public String get(final String key) {
        return executeRead(key, new RedisReturnAction<String>() {

            @Override
            public String execute(Jedis jedis) {
                return jedis.get(key);
            }
        });
    }

    /**
     * Get the value of the specified key as Long.If the key does not exist null
     * is returned.
     */
    public Long getAsLong(final String key) {
        String result = get(key);
        return result != null ? Long.valueOf(result) : null;
    }

    /**
     * Get the value of the specified key as Integer.If the key does not exist
     * null is returned.
     */
    public Integer getAsInt(final String key) {
        String result = get(key);
        return result != null ? Integer.valueOf(result) : null;
    }

    /**
     * Set the string value as value of the key. The string can't be longer than
     * 1073741824 bytes (1 GB).
     */
    public void set(final String key, final String value) {
        executeWrite(key, new RedisAction() {

            @Override
            public void execute(Jedis jedis) {
                jedis.set(key, value);
            }
        });
    }

    /**
     * The command is exactly equivalent to the following group of commands:
     * {@link #set(String, String) SET} + {@link #expire(String, int) EXPIRE}.
     * The operation is atomic.
     */
    public void setex(final String key, final String value, final int seconds) {
        executeWrite(key, new RedisAction() {

            @Override
            public void execute(Jedis jedis) {
                jedis.setex(key, seconds, value);
            }
        });
    }

    /**
     * SETNX works exactly like {@link #setNX(String, String) SET} with the only
     * difference that if the key already exists no operation is performed.
     * SETNX actually means "SET if Not eXists".
     * 
     * return true if the key was set.
     */
    public Boolean setnx(final String key, final String value) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.setnx(key, value) == 1 ? true : false;
            }
        });
    }

    /**
     * GETSET is an atomic set this value and return the old value command. Set
     * key to the string value and return the old value stored at key. The
     * string can't be longer than 1073741824 bytes (1 GB).
     */
    public String getSet(final String key, final String value) {
        return executeWrite(key, new RedisReturnAction<String>() {

            @Override
            public String execute(Jedis jedis) {
                return jedis.getSet(key, value);
            }
        });
    }

    /**
     * Increment the number stored at key by one. If the key does not exist or
     * contains a value of a wrong type, set the key to the value of "0" before
     * to perform the increment operation.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are
     * not "integer" types. Simply the string stored at the key is parsed as a
     * base 10 64 bit signed integer, incremented, and then converted back as a
     * string.
     * 
     * @return Integer reply, this commands will reply with the new value of key
     *         after the increment.
     */
    public Long incr(final String key) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.incr(key);
            }
        });
    }

    public Long incrBy(final String key, final Long increment) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.incrBy(key, increment);
            }
        });
    }

    public Double incrByFloat(final String key, final double increment) {
        return executeWrite(key, new RedisReturnAction<Double>() {
            @Override
            public Double execute(Jedis jedis) {
                return jedis.incrByFloat(key, increment);
            }
        });
    }

    /**
     * Decrement the number stored at key by one. If the key does not exist or
     * contains a value of a wrong type, set the key to the value of "0" before
     * to perform the decrement operation.
     */
    public Long decr(final String key) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.decr(key);
            }
        });
    }

    public Long decrBy(final String key, final Long decrement) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.decrBy(key, decrement);
            }
        });
    }

    // / Hash Actions ///

    /**
     * If key holds a hash, retrieve the value associated to the specified
     * field.
     * <p>
     * If the field is not found or the key does not exist, a special 'nil'
     * value is returned.
     */
    public String hget(final String key, final String field) {
        return executeRead(key, new RedisReturnAction<String>() {
            @Override
            public String execute(Jedis jedis) {
                return jedis.hget(key, field);
            }
        });
    }

    public List<String> hmget(final String key, final String... fields) {
        return executeRead(key, new RedisReturnAction<List<String>>() {
            @Override
            public List<String> execute(Jedis jedis) {
                return jedis.hmget(key, fields);
            }
        });
    }

    public Map<String, String> hgetAll(final String key) {
        return executeRead(key, new RedisReturnAction<Map<String, String>>() {
            @Override
            public Map<String, String> execute(Jedis jedis) {
                return jedis.hgetAll(key);
            }
        });
    }

    public void hset(final String key, final String field, final String value) {
        executeWrite(key, new RedisAction() {

            @Override
            public void execute(Jedis jedis) {
                jedis.hset(key, field, value);
            }
        });
    }

    public void hmset(final String key, final Map<String, String> map) {
        executeWrite(key, new RedisAction() {

            @Override
            public void execute(Jedis jedis) {
                jedis.hmset(key, map);
            }
        });
    }

    public Boolean hsetnx(final String key, final String fieldName, final String value) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.hsetnx(key, fieldName, value) == 1 ? true : false;
            }
        });
    }

    public Long hincrBy(final String key, final String fieldName, final long increment) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.hincrBy(key, fieldName, increment);
            }
        });
    }

    public Double hincrByFloat(final String key, final String fieldName, final double increment) {
        return executeWrite(key, new RedisReturnAction<Double>() {
            @Override
            public Double execute(Jedis jedis) {
                return jedis.hincrByFloat(key, fieldName, increment);
            }
        });
    }

    public Long hdel(final String key, final String... fieldsNames) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.hdel(key, fieldsNames);
            }
        });
    }

    public Boolean hexists(final String key, final String fieldName) {
        return executeRead(key, new RedisReturnAction<Boolean>() {
            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.hexists(key, fieldName);
            }
        });
    }

    public Set<String> hkeys(final String key) {
        return executeRead(key, new RedisReturnAction<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.hkeys(key);
            }
        });
    }

    public Long hlen(final String key) {
        return executeRead(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.hlen(key);
            }
        });
    }

    // / List Actions ///

    public Long lpush(final String key, final String... values) {
        return executeWrite(key, new RedisReturnAction<Long>() {
            @Override
            public Long execute(Jedis jedis) {
                return jedis.lpush(key, values);
            }
        });
    }

    public String rpop(final String key) {
        return executeWrite(key, new RedisReturnAction<String>() {

            @Override
            public String execute(Jedis jedis) {
                return jedis.rpop(key);
            }
        });
    }

    public String brpop(final int timeout, final String key) {
        return executeWrite(key, new RedisReturnAction<String>() {

            @Override
            public String execute(Jedis jedis) {
                List<String> nameValuePair = jedis.brpop(timeout, key);
                if (nameValuePair != null) {
                    return nameValuePair.get(1);
                } else {
                    return null;
                }
            }
        });
    }

    public Long llen(final String key) {
        return executeRead(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.llen(key);
            }
        });
    }

    public String lindex(final String key, final long index) {
        return executeRead(key, new RedisReturnAction<String>() {

            @Override
            public String execute(Jedis jedis) {
                return jedis.lindex(key, index);
            }
        });
    }

    public List<String> lrange(final String key, final int start, final int end) {
        return executeRead(key, new RedisReturnAction<List<String>>() {

            @Override
            public List<String> execute(Jedis jedis) {
                return jedis.lrange(key, start, end);
            }
        });
    }

    public void ltrim(final String key, final int start, final int end) {
        executeWrite(key, new RedisAction() {
            @Override
            public void execute(Jedis jedis) {
                jedis.ltrim(key, start, end);
            }
        });
    }

    public void ltrimFromLeft(final String key, final int size) {
        executeWrite(key, new RedisAction() {
            @Override
            public void execute(Jedis jedis) {
                jedis.ltrim(key, 0, size - 1);
            }
        });
    }

    public Boolean lremFirst(final String key, final String value) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {
            @Override
            public Boolean execute(Jedis jedis) {
                Long count = jedis.lrem(key, 1, value);
                return (count == 1);
            }
        });
    }

    public Boolean lremAll(final String key, final String value) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {
            @Override
            public Boolean execute(Jedis jedis) {
                Long count = jedis.lrem(key, 0, value);
                return (count > 0);
            }
        });
    }

    // / Set Actions ///
    public Boolean sadd(final String key, final String member) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.sadd(key, member) == 1 ? true : false;
            }
        });
    }

    public Set<String> smembers(final String key) {
        return executeRead(key, new RedisReturnAction<Set<String>>() {

            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.smembers(key);
            }
        });
    }

    // / Sorted Set Actions ///

    public Boolean zadd(final String key, final double score, final String member) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.zadd(key, score, member) == 1 ? true : false;
            }
        });
    }

    public Double zscore(final String key, final String member) {
        return executeRead(key, new RedisReturnAction<Double>() {

            @Override
            public Double execute(Jedis jedis) {
                return jedis.zscore(key, member);
            }
        });
    }

    public Long zrank(final String key, final String member) {
        return executeRead(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.zrank(key, member);
            }
        });
    }

    public Long zrevrank(final String key, final String member) {
        return executeRead(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.zrevrank(key, member);
            }
        });
    }

    public Long zcount(final String key, final double min, final double max) {
        return executeRead(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.zcount(key, min, max);
            }
        });
    }

    public Set<String> zrange(final String key, final int start, final int end) {
        return executeRead(key, new RedisReturnAction<Set<String>>() {

            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.zrange(key, start, end);
            }
        });
    }

    public Set<Tuple> zrangeWithScores(final String key, final int start, final int end) {
        return executeRead(key, new RedisReturnAction<Set<Tuple>>() {

            @Override
            public Set<Tuple> execute(Jedis jedis) {
                return jedis.zrangeWithScores(key, start, end);
            }
        });
    }

    public Set<String> zrevrange(final String key, final int start, final int end) {
        return executeRead(key, new RedisReturnAction<Set<String>>() {

            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.zrevrange(key, start, end);
            }
        });
    }

    public Set<Tuple> zrevrangeWithScores(final String key, final int start, final int end) {
        return executeRead(key, new RedisReturnAction<Set<Tuple>>() {

            @Override
            public Set<Tuple> execute(Jedis jedis) {
                return jedis.zrevrangeWithScores(key, start, end);
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return executeRead(key, new RedisReturnAction<Set<String>>() {

            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.zrangeByScore(key, min, max);
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return executeRead(key, new RedisReturnAction<Set<Tuple>>() {

            @Override
            public Set<Tuple> execute(Jedis jedis) {
                return jedis.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return executeRead(key, new RedisReturnAction<Set<String>>() {

            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.zrevrangeByScore(key, max, min);
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return executeRead(key, new RedisReturnAction<Set<Tuple>>() {

            @Override
            public Set<Tuple> execute(Jedis jedis) {
                return jedis.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    public Boolean zrem(final String key, final String member) {
        return executeWrite(key, new RedisReturnAction<Boolean>() {

            @Override
            public Boolean execute(Jedis jedis) {
                return jedis.zrem(key, member) == 1 ? true : false;
            }
        });
    }

    public Long zremByScore(final String key, final double start, final double end) {
        return executeWrite(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.zremrangeByScore(key, start, end);
            }
        });
    }

    public Long zremByRank(final String key, final long start, final long end) {
        return executeWrite(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.zremrangeByRank(key, start, end);
            }
        });
    }

    public Long zcard(final String key) {
        return executeRead(key, new RedisReturnAction<Long>() {

            @Override
            public Long execute(Jedis jedis) {
                return jedis.zcard(key);
            }
        });
    }

}
