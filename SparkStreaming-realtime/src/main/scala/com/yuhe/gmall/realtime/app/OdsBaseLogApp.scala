package com.yuhe.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yuhe.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.yuhe.gmall.realtime.util.MyKafkaUtils

object OdsBaseLogApp {


  def main(args: Array[String]): Unit = {

    // 1、准备Spark环境,local[4]表示以本地4 core资源运行
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[3]")
    // Second表示采集周期
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic: String = "ODS_BASE_LOG_1018"
    val groupId: String = "ODS_BASE_LOG_GROUP_1018"

    // 2、获取kafkaDStream对象
    val kafkaDStream: InputDStream[ConsumerRecord[String,String]] = MyKafkaUtils.getKafkaDStream(ssc,topic,groupId)

    // 3、数据处理
    // 3.1 转换,将字符串转换成JSON对象
    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map(consumerRecord => {
      val log: String = consumerRecord.value()
      val jsonObj: JSONObject = JSON.parseObject(log)
      // 返回json对象
      jsonObj
    })

    // 查看一下
    //jsonObjDStream.print(100)

    // 3.2 分流
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC_1018" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC_1018" //页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC_1018" //页面事件
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC_1018" //启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC_1018" // 错误数据


    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            // 拿到数据，进行分流
            // 分流错误数据
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            if(errObj != null) { //说明本条数据是错误数据
              // 将错误数据发送到DWD_ERROR_LOG_TOPIC
              MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, errObj.toJSONString)
            }else { // 说明本条数据不是错误数据

              // 提取公共字段
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              val ar: String = commonObj.getString("ar")
              val uid: String = commonObj.getString("uid")
              val ch: String = commonObj.getString("ch")
              val os: String = commonObj.getString("os")
              val isNew: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val vc: String = commonObj.getString("vc")
              val ba: String = commonObj.getString("ba")

              // 提取时间戳
              val ts: Long = jsonObj.getLong("ts")

              // 页面数据
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if(pageObj != null) { // 说明当前是页面数据
                // 提取page字段
                val pageId: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String = pageObj.getString("item_type")
                val duringTime: Long = pageObj.getLong("during_time")
                val lastPageId: String = pageObj.getString("last_page_id")
                val sourceType: String = pageObj.getString("source_type")

                // 将数据封装成Bean对象
                var pageLog: PageLog = PageLog(mid,uid,ar,ch,isNew,md,os,vc,pageId,lastPageId,ba,pageItem,pageItemType,sourceType,duringTime,ts)

                // 将数据发送到DWD_PAGE_LOG_TOPIC
                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                // 提取曝光数据
                val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                if(displaysJsonArr != null && displaysJsonArr.size() > 0) {
                  for(i <- 0 until displaysJsonArr.size()) {
                    // 提取到每条曝光数据
                    val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                    // 提取每个曝光字段
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val posId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")

                    // 将数据封装成Bean对象
                    var pageDisplayLog: PageDisplayLog = PageDisplayLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,displayType,displayItem,displayItemType,order,posId,ts)

                    // 将数据发送到DWD_PAGE_DISPLAY_TOPIC
                    MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                  }
                }


                // 提取事件数据
                val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                if(actionJsonArr != null && actionJsonArr.size() > 0 ){
                  for(i <- 0 until actionJsonArr.size()){
                    val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                    //提取字段
                    val actionId: String = actionObj.getString("action_id")
                    val actionItem: String = actionObj.getString("item")
                    val actionItemType: String = actionObj.getString("item_type")
                    val actionTs: Long = actionObj.getLong("ts")

                    //封装PageActionLog
                    var pageActionLog =
                      PageActionLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,actionId,actionItem,actionItemType,actionTs,ts)
                    //写出到DWD_PAGE_ACTION_TOPIC
                    MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC , JSON.toJSONString(pageActionLog , new SerializeConfig(true)))
                  }
                }
              }

              // 提取启动数据
              val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
              if(startJsonObj != null ){
                //提取字段
                val entry: String = startJsonObj.getString("entry")
                val loadingTime: Long = startJsonObj.getLong("loading_time")
                val openAdId: String = startJsonObj.getString("open_ad_id")
                val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                //封装StartLog
                var startLog =
                  StartLog(mid,uid,ar,ch,isNew,md,os,vc,ba,entry,openAdId,loadingTime,openAdMs,openAdSkipMs,ts)
                //写出DWD_START_LOG_TOPIC
                MyKafkaUtils.send(DWD_START_LOG_TOPIC , JSON.toJSONString(startLog ,new SerializeConfig(true)))
              }

            }
          }
        )
      }
    )




    ssc.start()
    ssc.awaitTermination()


  }


}
