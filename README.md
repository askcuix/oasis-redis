# oasis-redis

Redis存储层的实现。利用Redis的Master-Slave模式实现主从复制，sentinel作为高可用的实现，通过一致性哈希实现缓存式分片，或者采用Redis Cluster实现存储式分片，基于Jedis对以上特性进行封装，从代码层面屏蔽繁琐的实现细节，从而得到具有读写分离、分片、故障转移和按IDC分组访问等特性的Redis存储层实现。

## RedisSingleConnectionPool

用于单Redis实例部署模型的连接池实现，通常用于测试，或数据量较小及可靠性要求较低的场景。

### 使用方式

可以使用下面代码的方式，也可以配置成spring中的bean。

``` Java 
   RedisSingleConnectionPool connPool = new RedisSingleConnectionPool("127.0.0.1:6379", new JedisPoolConfig());
   connPool.init();
   
   Jedis jedis = connPool.getRedisPool().getResource();
   
   connPool.destroy();
```

## RedisReplicaConnectionPool

用于Redis Master-Slave部署模型的连接池实现，该模型使用较为广泛，通常为一主一从或一主多从。此连接池实现了读写分离，并可通过不同IDC配置相应的从库以实现就近访问。多从库时采用Round-Robin方式分配读库。

### 使用方式

可以使用下面代码的方式，也可以配置成spring中的bean。

``` Java 
   List<String> redisServers = new ArrayList<String>();
   redisServers.add("127.0.0.1:6379");
   redisServers.add("127.0.0.1:6380");

   RedisReplicaConnectionPool connPool = new RedisReplicaConnectionPool(redisServers);
   connPool.init();
   
   Jedis writeJedis = connPool.getMasterPool().getResource();
   Jedis readJedis = connPool.getSlavePool().getResource();
   
   connPool.destroy();
```

## RedisSentinelConnectionPool

用于Redis sentinel部署模型的连接池实现，Redis采用主从结构，Sentinel的数量至少为3个。Jedis的JedisSentinelPool只监听了switch-master事件，连接池也只包含了master，未将slave加入连接池。RedisSentinelConnectionPool在此基础上增加了+sdown、-sdown等事件的监听，并实现了读写分离。

### 使用方式

可以使用下面代码的方式，也可以配置成spring中的bean。

``` Java 
   String MASTER_NAME = "haSentinel";
   
   List<String> sentinels = new ArrayList<String>();
   redisServers.add("127.0.0.1:26379");
   redisServers.add("127.0.0.1:26380");
   redisServers.add("127.0.0.1:26381");

   RedisSentinelConnectionPool connPool = new RedisSentinelConnectionPool(MASTER_NAME, sentinels);
   connPool.init();
   
   Jedis writeJedis = connPool.getMasterPool().getResource();
   Jedis readJedis = connPool.getSlavePool().getResource();
   
   connPool.destroy();
```

## RedisCacheShardConnectionPool

用于Redis cache分片部署模型的连接池实现，cache分片的场景主要是数据量较大，单Redis或单Redis主从的压力较大，但数据允许部分丢失，因此该模型未引入sentinel提供高可用，部署结构为每个分片均为主从结构。该分片基于一致性哈希，当增加或减少分片时，产生的数据移动最小，可用性最高。Jedis的ShardedJedisPool依旧没有提供从库的操作，该连接池提供了读写分离，在主从均不可用的情况下，将移除该分片。

### 使用方式

可以使用下面代码的方式，也可以配置成spring中的bean。

``` Java 
   Map<String, List<String>> shardMap = new HashMap<String, List<String>>();

   List<String> shard1 = new ArrayList<String>();
   shard1.add(“10.21.8.225:6379”);
   shard1.add(“10.21.8.225:6380”);
   shardMap.put("shard1", shard1);

   List<String> shard2 = new ArrayList<String>();
   shard2.add(“10.21.8.226:6379”);
   shard2.add(“10.21.8.226:6380”);
   shardMap.put("shard2", shard2);

   List<String> shard3 = new ArrayList<String>();
   shard3.add(“10.21.8.227:6379”);
   shard3.add(“10.21.8.227:6380”);
   shardMap.put("shard3", shard3);

   RedisCacheShardConnectionPool connPool = new RedisCacheShardConnectionPool(shardMap);
   connPool.init();
   
   Jedis writeJedis = connPool.getMasterPool(key).getResource();
   Jedis readJedis = connPool.getSlavePool(key).getResource();
   
   connPool.destroy();
```

# Redis应用

## MasterElector

分布式服务中最常见的master选举，对于一些简单的服务，如果使用Zookeeper来达到该目的则太重了，不如利用当前应用中的Redis来达到该目的。

当前实例用setNX争夺Master成功后，会不断的更新expireTime，直到实例停止，或者实例挂掉。其它Slave会定时检查Master Key是否已过期，如果已过期则重新发起争夺。在实例挂掉的情况下，可能两倍的检查周期内没有Master。

``` Java 
   MasterElector elector = new MasterElector(redisConnectionPool, 5);
   elector.start();
   
   boolean isMaster = elector.isMaster()
   
   elector.stop();
```
