package org.sjg;

import static org.hamcrest.CoreMatchers.startsWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisLock {
	
	public void setExpired() throws Exception{  
		Jedis jedis = JedisFactory.cacheUtil().getJedis();  
		System.out.println(jedis.del("abcdefg"));  //删除这个key
		System.out.println(jedis.ttl("abcdefg"));  //-2
		System.out.println(jedis.expire("abcdefg", 100)); //设置expire
		System.out.println(jedis.ttl("abcdefg"));       //-2		
		System.out.println(jedis.set("abcdefg", "12323232")); //添加这个key
		System.out.println(jedis.ttl("abcdefg")); //-1

	   }  
	
	@Test
	public void testLock() throws Exception{
		CacheUtil util = JedisFactory.cacheUtil();  	
		String identifier = util.acquireLockWithTimeout("mykey", 10*1000, 10*1000);
		System.out.println(identifier);
		System.out.println(identifier+" locked released:"+util.releaseLock("mykey", identifier));
		
	}
	static int numOfAcquireSuccess = 0;
	static int numOfAcquireTimeout = 0;
	static int numOfReleaseLockSuccess = 0;
	static int numOfReleaseLockFialed = 0;
	static int threadCount = 10;
	private final CountDownLatch latch = new CountDownLatch(threadCount);
	@Test
	public void testMultiThreadLock() throws Exception{

		
		for(int i=0;i<threadCount;i++) {
			// Lambda Runnable
			Runnable task2 = ()-> { 
				for(int j = 0;j<100;j++) {
					try {
						String identifier = JedisFactory.cacheUtil().acquireLockWithTimeout("mykey", 10*1000, 10*1000);
						if(identifier == null) {
							numOfAcquireTimeout++;
							return;
						}
						numOfAcquireSuccess++;
						if(JedisFactory.cacheUtil().releaseLock("mykey", identifier)) {
							numOfReleaseLockSuccess++;
						}else {
							numOfReleaseLockFialed++;
						}
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				latch.countDown();
                };
			 
			// start the thread
			new Thread(task2).start();
		
		}
		try {
			latch.await(); // 主线程等待
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("numOfAcquireSuccess:"+numOfAcquireSuccess);
		System.out.println("numOfAcquireTimeout:"+numOfAcquireTimeout);
		System.out.println("numOfReleaseLockSuccess:"+numOfReleaseLockSuccess);
		System.out.println("numOfReleaseLockFialed:"+numOfReleaseLockFialed);
		
	}
}
