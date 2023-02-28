package com.yuhe.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
 * Redis工具类，用户获取Jedis连接，操作Redis
 */

object MyRedisUtils {

  var jdisPool: JedisPool = null;

  def getJedisFromPool(): Jedis = {
    if(jdisPool == null) {
      // 如果连接池对象不存在，则创建
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) // 最大空闲
      jedisPoolConfig.setMinIdle(20) // 最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) // 忙碌是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长
      jedisPoolConfig.setTestOnBorrow(true) // 每次获得连接时进行测试
      jdisPool = new JedisPool(jedisPoolConfig,"node100",6379)
    }
    // 如果连接池对象存在，则直接获取jedis对象返回
    jdisPool.getResource
  }

}
