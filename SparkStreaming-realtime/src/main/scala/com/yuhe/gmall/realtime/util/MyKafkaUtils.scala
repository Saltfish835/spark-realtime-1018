package com.yuhe.gmall.realtime.util
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import java.util

import org.apache.kafka.common.TopicPartition

object MyKafkaUtils {

  /**
   * 消费者配置
   */
  private val consumerConf: mutable.Map[String, Object] = mutable.Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.122.101:9092,192.168.122.102:9092,192.168.122.103:9092",

    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",

    // 自动提交偏移量
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )


  /**
   * 获取KafkaDStream对象，使用默认的offset
   * @param ssc
   * @param topic
   * @param groupId
   * @return
   */
  def getKafkaDStream(ssc:StreamingContext, topic:String, groupId:String): InputDStream[ConsumerRecord[String, String]] = {
    // 消费者必须指定消费者组ID
    consumerConf.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    //创建KafkaDStreaming
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConf))

    //Scala返回值两种方式
    //一：使用return
    //二：将返回值写在最后一行
    kafkaDStream
  }


  /**
   * 使用指定的offset进行消费
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    consumerConf.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic),
        consumerConf, offsets))
    kafkaDStream
  }




  /**
   * 获取Kafka生产者对象
   * @return
   */
  def createProducer():KafkaProducer[String, String] = {
    val producerConf: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]

    producerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.122.101:9092,192.168.122.102:9092,192.168.122.103:9092")
    producerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    //设置应答级别
    producerConf.put(ProducerConfig.ACKS_CONFIG, "all")

    //配置幂等性
    producerConf.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConf)

    //返回值
    producer
  }



  val producer = createProducer()

  /**
   * 发送消息
   * 使用粘性分区策略
   * @param topic
   * @param msg
   * @return
   */
  def send(topic:String, msg:String)={
    producer.send(new ProducerRecord[String, String](topic,msg))
  }

  /**
   * 发送消息
   * 指定key
   * @param topic
   * @param key
   * @param msg
   * @return
   */
  def send(topic:String, key:String, msg:String)={
    producer.send(new ProducerRecord[String,String](topic,key,msg))
  }


  /**
   * 关闭producer
   */
  def close() = {
    if(producer != null) {
      producer.close()
    }
  }


}


