package com.oasis.redis.sentinel;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oasis.redis.util.RedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPubSub;

public class SentinelEventSubscriber extends JedisPubSub {
    private static final Logger logger = LoggerFactory.getLogger(SentinelEventSubscriber.class);

    private String masterName;
    private HostAndPort sentinel;
    private SentinelEventReceiver eventReceiver;

    public SentinelEventSubscriber(HostAndPort sentinel, SentinelEventReceiver eventReceiver) {
        this.sentinel = sentinel;

        this.eventReceiver = eventReceiver;
        this.masterName = eventReceiver.getMasterName();
    }

    @Override
    public void onMessage(String channel, String message) {
        logger.info("Sentinel {} published {}: {}.", sentinel, channel, message);

        if ("+switch-master".equals(channel)) {
            handleSwitchMaster(message);
        } else if ("+sdown".equals(channel)) {
            handleSlaveDown(message);
        } else if ("-sdown".equals(channel)) {
            handleSlaveUp(message);
        } else if ("+slave".equals(channel)) {
            handleAddSlave(message);
        }
    }

    /**
     * Handle sentinel event "+switch-master".
     * 
     * switch-master <master name> <oldip> <oldport> <newip> <newport>
     * 
     * @param message
     */
    private void handleSwitchMaster(String message) {
        if (message == null || "".equals(message)) {
            return;
        }

        String[] switchMasterMsg = message.split(" ");

        if (switchMasterMsg.length != 5) {
            logger.warn("Invalid +switch-master message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (!masterName.equals(switchMasterMsg[0])) {
            logger.warn(
                    "Ignoring unmatched +switch-master message received on sentinel {}, valid master name: {}, message: {}",
                    sentinel, masterName, message);

            return;
        }

        logger.info("Process sentinel[{}] received +switch-master message: {}", sentinel, message);

        // update master
        HostAndPort masterAdd = RedisUtil.toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
        eventReceiver.refreshMasterPool(masterAdd);

        // update slaves
        List<HostAndPort> slaves = RedisUtil.lookupSlavesBySentinel(masterName, sentinel);

        // retry
        if (slaves == null || slaves.isEmpty()) {
            int retry = 5;
            while (retry > 0) {
                logger.warn("Wait 1s to retry lookup slaves info, maybe role change is not complete......");
                try {
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }

                slaves = RedisUtil.lookupSlavesBySentinel(masterName, sentinel);

                if (slaves != null && !slaves.isEmpty()) {
                    break;
                }

                retry--;
            }

            if (slaves == null || slaves.isEmpty()) {
                logger.error("Still not found slaves after retry 5 times......");
            }
        }

        eventReceiver.refreshSlavePool(slaves);
    }

    /**
     * Handle sentinel event "+sdown".
     * 
     * +sdown <instance-type> <name> <ip> <port> @
     * <master-name> <master-ip> <master-port>
     * 
     * @param message
     */
    private void handleSlaveDown(String message) {
        if (message == null || "".equals(message)) {
            return;
        }

        String[] sdownMsg = message.split(" ");

        if (!"slave".equals(sdownMsg[0])) {
            logger.info("Ignore non-slave +sdown message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (sdownMsg.length != 8) {
            logger.warn("Invalid +sdown message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (!masterName.equals(sdownMsg[5])) {
            logger.warn("Ignoring unmatched +sdown message received on sentinel {}, valid master name: {}, message: {}",
                    sentinel, masterName, message);

            return;
        }

        logger.info("Redis slave[{}:{}] of master[{}] is down.", sdownMsg[2], sdownMsg[3], sdownMsg[5]);

        // update slave
        List<HostAndPort> slaves = RedisUtil.lookupSlavesBySentinel(masterName, sentinel);
        eventReceiver.refreshSlavePool(slaves);
    }

    /**
     * Handle sentinel event "-sdown".
     * 
     * -sdown <instance-type> <name> <ip> <port> @
     * <master-name> <master-ip> <master-port>
     * 
     * @param message
     */
    private void handleSlaveUp(String message) {
        if (message == null || "".equals(message)) {
            return;
        }

        String[] sdownMsg = message.split(" ");

        if (!"slave".equals(sdownMsg[0])) {
            logger.info("Ignore non-slave -sdown message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (sdownMsg.length != 8) {
            logger.warn("Invalid -sdown message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (!masterName.equals(sdownMsg[5])) {
            logger.warn("Ignoring unmatched -sdown message received on sentinel {}, valid master name: {}, message: {}",
                    sentinel, masterName, message);

            return;
        }

        logger.info("Redis slave[{}:{}] of master[{}] is up.", sdownMsg[2], sdownMsg[3], sdownMsg[5]);

        // update slaves
        List<HostAndPort> slaves = RedisUtil.lookupSlavesBySentinel(masterName, sentinel);
        eventReceiver.refreshSlavePool(slaves);
    }

    /**
     * Handle sentinel event "+slave".
     * 
     * +slave <instance-type> <name> <ip> <port> @
     * <master-name> <master-ip> <master-port>
     * 
     * @param message
     */
    private void handleAddSlave(String message) {
        if (message == null || "".equals(message)) {
            return;
        }

        String[] sdownMsg = message.split(" ");

        if (!"slave".equals(sdownMsg[0])) {
            logger.info("Ignore non-slave +slave message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (sdownMsg.length != 8) {
            logger.warn("Invalid +slave message received on sentinel {}, message: {}", sentinel, message);

            return;
        }

        if (!masterName.equals(sdownMsg[5])) {
            logger.warn("Ignoring unmatched +slave message received on sentinel {}, valid master name: {}, message: {}",
                    sentinel, masterName, message);

            return;
        }

        logger.info("Redis slave[{}:{}] of master[{}] is added.", sdownMsg[2], sdownMsg[3], sdownMsg[5]);

        // update slaves
        List<HostAndPort> slaves = RedisUtil.lookupSlavesBySentinel(masterName, sentinel);
        eventReceiver.refreshSlavePool(slaves);
    }
}
