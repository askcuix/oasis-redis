package com.oasis.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

/**
 * Redis execute actions.
 * 
 * @author Chris
 *
 */
public class RedisExecution {

    /**
     * Callback interface for template with result.
     */
    public interface RedisReturnAction<T> {
        T execute(Jedis jedis);
    }

    /**
     * Callback interface for template without result.
     */
    public interface RedisAction {
        void execute(Jedis jedis);
    }

    /**
     * Callback interface for pipline without result.
     */
    public interface PipelineAction {
        void execute(Pipeline Pipeline);
    }

    /**
     * Callback interface for transaction without result.
     * 
     */
    public interface TransactionAction {
        void execute(Transaction transaction);
    }
}
