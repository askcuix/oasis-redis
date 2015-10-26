package com.oasis.redis.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.pool.RedisPool;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * Redis utility tools.
 * 
 * @author Chris
 *
 */
public class RedisUtil {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    /**
     * Get connected client count from redis info.
     * 
     * @param info redis info
     * @return connected count. return 0 if info is empty or not matched.
     */
    public static int getConnectedClientNum(String info) {
        if (info == null || "".equals(info)) {
            return 0;
        }

        Pattern p = Pattern.compile("connected_clients:(\\d+)");
        Matcher m = p.matcher(info);
        if (m.find()) {
            return Integer.valueOf(m.group(1));
        }

        return 0;
    }

    /**
     * Check master from redis info.
     * 
     * @param info redis info
     * @return whether is master. return false if info is empty or not matched.
     */
    public static boolean isMaster(String info) {
        if (info == null || "".equals(info)) {
            return false;
        }

        Pattern p = Pattern.compile("role:(\\w+)");
        Matcher m = p.matcher(info);
        if (m.find()) {
            String isMaster = (m.group(1));
            return "master".equals(isMaster) ? true : false;
        }

        return false;
    }

    /**
     * Convert redis server info to HostAndPort.
     * 
     * @param hostPortPair [host, port]
     * @return HostAndPort
     */
    public static HostAndPort toHostAndPort(List<String> hostPortPair) {
        String host = hostPortPair.get(0);
        int port = Integer.parseInt(hostPortPair.get(1));

        return new HostAndPort(host, port);
    }

    /**
     * Convert redis server info to HostAndPort.
     * 
     * @param server "host:port"
     * @return HostAndPort
     */
    public static HostAndPort toHostAndPort(String server) {
        return toHostAndPort(Arrays.asList(server.split(":")));
    }

    /**
     * Destroy jedis instance.
     */
    public static void destroyJedis(Jedis jedis) {
        if ((jedis != null) && jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                    // do nothing
                }
                jedis.disconnect();
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    /**
     * Check jedis is alive.
     * 
     * @param jedis
     * @return true if alive
     */
    public static boolean ping(Jedis jedis) {
        if (jedis == null) {
            return false;
        }

        try {
            String result = jedis.ping();
            return (result != null) && result.equals("PONG");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Lookup master server for given master name by sentinel.
     * 
     * @param masterName
     * @param sentinel
     * @return master
     */
    public static HostAndPort lookupMasterBySentinel(String masterName, HostAndPort sentinel) {
        Jedis jedis = null;

        try {
            jedis = new Jedis(sentinel.getHost(), sentinel.getPort());

            HostAndPort master = lookupMasterBySentinel(masterName, jedis);
            if (master == null) {
                logger.error("Fail to get master for given name[{}] by sentinel: {}", masterName, sentinel);

                return null;
            }

            logger.info("Found master[{}] for given name[{}] by sentinel: {}", master, masterName, sentinel);

            return master;
        } catch (Exception e) {
            logger.error("Lookup master for given name[" + masterName + "] by sentinel[" + sentinel + "] error.", e);
        } finally {
            destroyJedis(jedis);
        }

        return null;
    }

    /**
     * Lookup master server for given master name by jedis.
     * 
     * @param masterName
     * @param sentinelJedis
     * @return master
     */
    public static HostAndPort lookupMasterBySentinel(String masterName, Jedis sentinelJedis) {
        List<String> masterAddr = sentinelJedis.sentinelGetMasterAddrByName(masterName);

        if (masterAddr == null || masterAddr.isEmpty() || masterAddr.size() != 2) {
            return null;
        }

        return toHostAndPort(masterAddr);
    }

    /**
     * Lookup slave servers for given master name by sentinel.
     * 
     * @param masterName
     * @param sentinel
     * @return slaves
     */
    public static List<HostAndPort> lookupSlavesBySentinel(String masterName, HostAndPort sentinel) {
        List<HostAndPort> slaveHAPList = new ArrayList<HostAndPort>();

        Jedis jedis = null;

        try {
            jedis = new Jedis(sentinel.getHost(), sentinel.getPort());

            slaveHAPList = lookupSlavesBySentinel(masterName, jedis);
            if (slaveHAPList == null) {
                logger.error("Can't get slave of master[{}] from sentinel: {}", masterName, sentinel);

                return new ArrayList<HostAndPort>();
            }

            logger.info("Found slaves of redis master[{}] by sentinel[{}]: {}", masterName, sentinel,
                    CollectionUtil.join(slaveHAPList, ", "));
        } catch (Exception e) {
            logger.error("Lookup slave of redis master[" + masterName + "] by sentinel[" + sentinel + "] error.", e);
        } finally {
            destroyJedis(jedis);
        }

        return slaveHAPList;
    }

    /**
     * Lookup slave servers for given master name by jedis.
     * 
     * @param masterName
     * @param sentinelJedis
     * @return slaves
     */
    public static List<HostAndPort> lookupSlavesBySentinel(String masterName, Jedis sentinelJedis) {
        List<Map<String, String>> slaveInfoList = sentinelJedis.sentinelSlaves(masterName);
        if ((slaveInfoList == null) || slaveInfoList.isEmpty()) {
            return null;
        }

        List<HostAndPort> slaveHAPList = new ArrayList<HostAndPort>();

        for (Map<String, String> slaveInfo : slaveInfoList) {
            String ipPort = slaveInfo.get("name");

            if (ipPort == null || "".equals(ipPort)) {
                logger.warn("Slave info is invalid: {}", slaveInfo);

                continue;
            }

            String flags = slaveInfo.get("flags");
            if (flags.contains("disconnected")) {
                logger.warn("Ignore disconnected slave[{}]: {}", ipPort, slaveInfo);

                continue;
            }

            HostAndPort slaveHAP = toHostAndPort(ipPort);
            slaveHAPList.add(slaveHAP);
        }

        return slaveHAPList;
    }

    /**
     * Extract redis server info from connection pool.
     * 
     * @param redisPoolList
     * @return server info
     */
    public static String extractServerInfo(List<RedisPool> redisPoolList) {
        if (redisPoolList == null || redisPoolList.isEmpty()) {
            return null;
        }

        StringBuilder serverInfo = new StringBuilder();
        for (int i = 0; i < redisPoolList.size(); i++) {
            RedisPool pool = redisPoolList.get(i);
            serverInfo.append(pool.getHost()).append(":").append(pool.getPort());

            if (i < redisPoolList.size() - 1) {
                serverInfo.append(", ");
            }
        }

        return serverInfo.toString();
    }

}
