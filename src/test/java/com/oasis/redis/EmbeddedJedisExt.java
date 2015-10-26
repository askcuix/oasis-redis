package com.oasis.redis;

import com.lordofthejars.nosqlunit.redis.embedded.EmbeddedJedis;

/**
 * Extension for EmbeddedJedis to handle some unimplemented jedis method.
 * 
 * @author Chris
 *
 */
public class EmbeddedJedisExt extends EmbeddedJedis {

    public void close() {
        // do nothing
    }

}
