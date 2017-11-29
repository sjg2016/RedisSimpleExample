package org.sjg;

import java.util.List;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisLock {

	@Test
	public void setExpired() throws Exception{  
		Jedis jedis = JedisFactory.cacheUtil().getJedis();  
		System.out.println(jedis.del("abcdefg"));  //删除这个key
		System.out.println(jedis.ttl("abcdefg"));  //-2
		System.out.println(jedis.expire("abcdefg", 100)); //设置expire
		System.out.println(jedis.ttl("abcdefg"));       //-2		
		System.out.println(jedis.set("abcdefg", "12323232")); //添加这个key
		System.out.println(jedis.ttl("abcdefg")); //-1

	   }  
 
}
