package com.oasis.redis.sentinel;

import java.util.List;

import redis.clients.jedis.HostAndPort;

/**
 * Listener to sentinel failover event.
 * 
 * @author Chris
 *
 */
public interface SentinelEventReceiver {

    /**
     * Get master name which monitored by sentinel.
     * 
     * @return master name
     */
    public String getMasterName();

    /**
     * Get sentinels info.
     * 
     * @return sentinels
     */
    public List<String> getSentinels();

    /**
     * Refresh master pool if failover occurred.
     * 
     * @param master
     */
    public void refreshMasterPool(HostAndPort master);

    /**
     * Refresh slave pool if failover occurred.
     * 
     * @param slaveList
     */
    public void refreshSlavePool(List<HostAndPort> slaveList);
}
