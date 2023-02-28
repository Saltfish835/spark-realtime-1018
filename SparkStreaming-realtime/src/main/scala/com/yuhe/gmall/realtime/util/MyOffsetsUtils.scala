package com.yuhe.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable


/**
 * offset管理工具类，用于往redis里存储和读取offset
 *
 * 管理方案：
 *  1、后置提交偏移量 -- 需要手动控制偏移量提交
 *  2、手动控制偏移量提交 -- spark streaming提供的手动提交方案不适用，因为DStream的流会进行转换
 *  3、所以我们手动提取偏移量，维护在redis中
 *    -- 从kafka中消费到数据后，先提取出偏移量
 *    -- 等数据成功写出后，将偏移量存储在redis中
 *    -- 从kafka消费数据之前，先到redis中读取偏移量，使用读取到的偏移量来消费数据
 *
 */
object MyOffsetsUtils {


  /**
   * 往redis中存储offset
   *  1、从消费的数据中提取出offset
   *    offsetRanges: Array[OffsetRange]
   *  2、offset的结构是什么
   *    <groupId+topic+partition, offset>
   *  3、在redis中怎么存
   *    类型： hash
   *    key：groupId+topic
   *    value：<partition,offset>，<partition,offset>
   *    写入api：hset/hmset
   *    读取api：hgetall
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val offsets: util.HashMap[String, String] = new util.HashMap[String, String]()
    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition
      val endOffset = offsetRange.untilOffset //当前这个批次最后一条消息的偏移量
      //将<partition,endOffset>存入map
      offsets.put(partition.toString,endOffset.toString)
    }
    println("提交offset: "+offsets)

    //将<groupId+topic,<partition,offset>>写入redis
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisKey: String = s"offsets:$topic:$groupId"
    jedis.hset(redisKey,offsets)

    // 注意关闭连接
    jedis.close()
  }



  /**
   * 从redis中读取存储的offset
   *  让SparkStreaming从指定的offset进行消费，Spark中已提供
   *  Spark的指定参数格式为：Map[TopicPartition,Long]
   *
   */
  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisKey: String = s"offsets:$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取到offset: "+offsets)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    // 将Java的map转换成Scala的map进行迭代
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      val tp: TopicPartition = new TopicPartition(topic,partition.toInt)
      results.put(tp, offset.toLong)
    }
    jedis.close()
    results.toMap
  }





}
