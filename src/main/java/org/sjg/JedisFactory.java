package org.sjg;


import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class JedisFactory {
	private static CacheUtil cacheUtil = cacheUtil();
	public static JedisSentinelPool jedisSentinelPool() {
		java.util.Set<String> sentinelSet = new java.util.HashSet<String>();
//		sentinelSet.add("10.76.64.241:26379");
//		sentinelSet.add("10.76.64.241:36379");
		sentinelSet.add("10.76.64.158:26379");
		return new JedisSentinelPool("mymaster", sentinelSet, jedisPoolConfig(),"foobared");
	}
	public static JedisPoolConfig jedisPoolConfig() {
		System.out.println("call jedisPoolConfig....");
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(1000);
		jedisPoolConfig.setMaxIdle(160);
		jedisPoolConfig.setMaxWaitMillis(10000);
		jedisPoolConfig.setTestOnBorrow(true);
		return jedisPoolConfig;
	}
	public static CacheUtil cacheUtil() {
		if(cacheUtil != null) return cacheUtil;
		return new CacheUtil(jedisSentinelPool());
	}
}