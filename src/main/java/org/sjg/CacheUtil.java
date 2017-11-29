package org.sjg;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class CacheUtil {
	private JedisSentinelPool sentinelPool;
	public CacheUtil(JedisSentinelPool sentinelPool) {
		this.sentinelPool = sentinelPool;
	}
	public synchronized Jedis getJedis() throws Exception {
		try {
			if (sentinelPool != null) {
				// log.debug("------ sentinelPool is not null,return instance. -------");
				Jedis jedis = sentinelPool.getResource();

				if (jedis == null) {
					throw new Exception("getJedis returns NULL.");
				}
				return jedis;
			} else {
				return null;
			}
		} catch (JedisException e) {
			return null;
		}
	}

	public void colse() {
		if (sentinelPool != null) {
			sentinelPool.destroy();
		}
	}


	   /**
     * 获取分布式锁
     * 
     * @param lockName
     *            竞争获取锁key
     * @param acquireTimeoutInMS
     *            获取锁超时时间
     * @param lockTimeoutInMS
     *            锁的超时时间
     * @return 获取锁标识
     */
    public String acquireLockWithTimeout(String lockName, long acquireTimeoutInMS, long lockTimeoutInMS) throws Exception{
        Jedis jedis = null;
        boolean broken = false;
        String retIdentifier = null;
        try {
        	jedis = this.getJedis();
            String identifier = UUID.randomUUID().toString();
            String lockKey = "lock:" + lockName;
            int lockExpire = (int) (lockTimeoutInMS / 1000);

            long end = System.currentTimeMillis() + acquireTimeoutInMS;
            while (System.currentTimeMillis() < end) {
                if (jedis.setnx(lockKey, identifier) == 1) { //1-设置成功
                	jedis.expire(lockKey, lockExpire);
                    retIdentifier = identifier;
                }
                if (jedis.ttl(lockKey) == -1) {    //-1:没有设置TTL但是有这个key -2:不存在这个key；这里针对获取key值的处理但是超时设置不成功的情况
                	jedis.expire(lockKey, lockExpire);
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
        return retIdentifier;
    }
	/**
	 * 执行循环：watch key->value没有变化的情况下执行del事务，如果失败进入下一次循环，直到del成功，则返回true。
	 *          watch key->value值发生变化的情况下，说明超时了，这时unwatch，退出，返回false
	 * 
	 * @param lockName
	 * @param identifier
	 * @return
	 * @throws Exception
	 */
    public boolean releaseLock(String lockName, String identifier) throws Exception {
        Jedis jedis = null;
        boolean broken = false;
        String lockKey = "lock:" + lockName;
        boolean retFlag = false;
        try {
        	jedis = this.getJedis();
            while (true) {
            	jedis.watch(lockKey);
                if (identifier.equals(jedis.get(lockKey))) {
                    Transaction trans = jedis.multi();
                    trans.del(lockKey);
                    List<Object> results = trans.exec();
                    if (results == null) { //事务执行失败，说明key对应的value发生了变化，尝试下一次释放
                        continue;
                    }
                    retFlag = true;
                }
                jedis.unwatch();
                break;
            }

        } catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
        return retFlag;
    }
    public boolean releaseLockByPipeline(String lockName, String identifier) throws Exception {
        Jedis jedis = null;
        boolean broken = false;
        lockName = "lock:" + lockName;
        boolean retFlag = false;
        try {
        	jedis = this.getJedis();
    		Pipeline pl = jedis.pipelined();
    		while(true) {
    			pl.watch(lockName);
    			if(pl.get(lockName).get().equals(identifier)) {
    				pl.multi();
    				pl.del(lockName);
    				Response<List<Object>> results = pl.exec();
    				if (results == null) { //事务执行失败，说明key对应的value发生了变化，尝试下一次释放
                        continue;
                    }
    				 retFlag = true;
    			}
    			//If you call EXEC or DISCARD, there's no need to manually call UNWATCH.
                break;
            }

        } catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
        return retFlag;
    }
	public String setString(String key, String value) throws Exception {
		String result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.set(key, value);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public static String setString(String key,Object obj) { String result =
	 * null; Jedis jedis = null; try { jedis = getJedis(); if (jedis != null) {
	 * result = jedis.set(key, toJson(obj));
	 * 
	 * } } catch (JedisException e) { } finally { returnResource(jedis); }
	 * return result; }
	 */
	public String getSet(String key, String value) throws Exception {
		String result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.getSet(key, value);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {

			closeResource(jedis, broken);
		}
		return result;
	}
	
	public Long getEffectTime(String key) throws Exception {
		Long result = -1L;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.ttl(key);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {

			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public static String getSet(String key,Object obj) { String result =
	 * null; Jedis jedis = null; try { jedis = getJedis(); if (jedis != null) {
	 * result = jedis.getSet(key, toJson(obj)); } } catch (JedisException e) { }
	 * finally { returnResource(jedis); } return result; }
	 */

	public String setString(String key, String value, String nxxx, String expx,
			long time) throws Exception {
		String result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.set(key, value, nxxx, expx, time);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public String setString(String key, Object obj, String nxxx, String
	 * expx,long time){ String result = null; Jedis jedis = null; try { jedis =
	 * getJedis(); if (jedis != null) { result = jedis.set(key,
	 * toJson(obj),nxxx,expx,time); } } catch (JedisException e) { } finally {
	 * returnResource(jedis); } return result; }
	 */

	public boolean hexists(String key, String field) throws Exception {
		boolean result = false;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.hexists(key, field);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public long exists(String... keys) throws Exception {
		Long result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.exists(keys);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Set<String> keys(String pattern) throws Exception {
		Set<String> result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.keys(pattern);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public long del(String... key) throws Exception {
		Jedis jedis = null;
		long result = 0;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.del(key);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public long expire(String key, int seconds) throws Exception {
		Jedis jedis = null;
		long result = 0;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.expire(key, seconds);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public long ttl(String key) throws Exception {
		Jedis jedis = null;
		long result = 0;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.ttl(key);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Long getLen(String key) throws Exception {
		Long len = 0l;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				len = jedis.llen(key);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return len;
	}

	public Long append(String key, String value) throws Exception {
		Long result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.append(key, value);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Long hdel(String key, String... fields) throws Exception {
		Long result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.hdel(key, fields);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Boolean hexits(String key, String field) throws Exception {
		Boolean result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.hexists(key, field);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public static <T>T getString(String key,Class<T> classOfT){ String result
	 * = null; Jedis jedis = null; T t ; try { jedis = getJedis(); if (jedis !=
	 * null) { result = jedis.get(key); t = (result == null ? null :
	 * JSON.parseObject(result, classOfT)); } } catch (JedisException e) { }
	 * finally { returnResource(jedis); } return t; }
	 */

	public String getString(String key) throws Exception {
		String result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.get(key);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public <T>T hget(String key,String field,Class<T> classOfT){ String
	 * result = null; Jedis jedis = null; T t ; try { jedis = getJedis(); if
	 * (jedis != null) { result = jedis.hget(key, field); t = (result == null ?
	 * null : JSON.parseObject(result, classOfT)); } } catch (JedisException e)
	 * { } finally { returnResource(jedis); } return t; }
	 */

	public String hget(String key, String field) throws Exception {
		String result = null;
		Jedis jedis = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			if (jedis != null) {
				result = jedis.hget(key, field);
			}
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Map<String, String> hgetAll(String key) throws Exception {
		Jedis jedis = null;
		Map<String, String> map = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			map = jedis.hgetAll(key);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return map;
	}

	public Long hincrby(String key, String field, long value) throws Exception {
		Jedis jedis = null;
		Long result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hincrBy(key, field, value);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Set<String> hkeys(String key) throws Exception {
		Jedis jedis = null;
		Set<String> result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hkeys(key);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Long hlen(String key) throws Exception {
		Jedis jedis = null;
		Long result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hlen(key);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public List<String> hmget(String key, String... field) throws Exception {
		Jedis jedis = null;
		List<String> result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hmget(key);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public String hmset(String key, Map<String, String> value)
			throws Exception {
		Jedis jedis = null;
		String result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hmset(key, value);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Long hset(String key, String field, String value) throws Exception {
		Jedis jedis = null;
		Long result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hset(key, field, value);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public Long hset(String key,String field,Object obj){ Jedis jedis = null;
	 * Long result = null; try { jedis = getJedis(); result =
	 * jedis.hset(key,field,toJson(obj)); } catch (JedisException e) { }finally{
	 * returnResource(jedis); } return result; }
	 */

	public List<String> hvals(String key) throws Exception {
		Jedis jedis = null;
		List<String> result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hvals(key);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	public Long hsetnx(String key, String field, String value) throws Exception {
		Jedis jedis = null;
		Long result = null;
		boolean broken = false;
		try {
			jedis = getJedis();
			result = jedis.hsetnx(key, field, value);
		} catch (JedisException e) {
			broken = handleJedisException(e);
			throw e;
		} finally {
			closeResource(jedis, broken);
		}
		return result;
	}

	/*
	 * public Long hsetnx(String key,String field,Object obj){ Jedis jedis =
	 * null; Long result = null; try { jedis = getJedis(); result =
	 * jedis.hsetnx(key, field, toJson(obj)); } catch (JedisException e) {
	 * }finally{ returnResource(jedis); } return result; }
	 */

	private static boolean handleJedisException(JedisException jedisException) {
		if (jedisException instanceof JedisConnectionException) {
		} else if (jedisException instanceof JedisDataException) {
			if ((jedisException.getMessage() != null)
					&& (jedisException.getMessage().indexOf("READONLY") != -1)) {
			} else {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("deprecation")
	private void closeResource(Jedis jedis, boolean conectionBroken) {
		try {
			if (conectionBroken) {
				sentinelPool.returnBrokenResource(jedis);
			} else {
				sentinelPool.returnResource(jedis);
			}
		} catch (Exception e) {
			colse();
		}
	}
}
