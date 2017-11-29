package org.sjg;

import java.util.List;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisWatch {
	/**2、事务
	  事务是保证事务内的所有命令是原子操作，一般配合watch使用，事务的执行结果和pipeline一样都是采用异步的方式获取结果，
	  multi.exec()提交事务，如果执行成功，其返回的结果和pipeline一样是所有命令的返回值，
	  如果事务里面有两个命令那么事务的exec返回值会把两个命令的返回值组合在一起返回。如果事务被取消返回null。
	   3、watch
	     一般是和事务一起使用，当对某个key进行watch后如果其他的客户端对这个key进行了更改，那么本次事务会被取消，
	     事务的exec会返回null。jedis.watch(key)都会返回OK
	**/
	@Test
	public void testWach() throws Exception{  
		Jedis jedis = JedisFactory.cacheUtil().getJedis();  
	       String watch = jedis.watch("testabcd");  
	       System.out.println(Thread.currentThread().getName()+"--"+watch);  
	       Transaction multi = jedis.multi();  
	       multi.set("testabcd", "23432");  
	       try {  
	           Thread.sleep(30000);  
	       } catch (InterruptedException e) {  
	           e.printStackTrace();  
	       }  
	       List<Object> exec = multi.exec();  
	       System.out.println("---"+exec);  
	       jedis.unwatch();  
	   }  
	@Test
	 public void testWatch2() throws Exception{  
		   Jedis jedis = JedisFactory.cacheUtil().getJedis();  
	       String watch = jedis.watch("testabcd2");  
	       System.out.println(Thread.currentThread().getName()+"--"+watch);  
	       Transaction multi = jedis.multi();  
	       multi.set("testabcd", "125");  
	       List<Object> exec = multi.exec();  
	       System.out.println("--->>"+exec);  
	   }  
}
