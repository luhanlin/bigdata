package com.luhanlin.spark.warn

import java.util.Timer

import com.luhanlin.redis.client.JedisSingle
import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import com.luhanlin.spark.streaming.kafka.kafka2es.Kafka2esStreaming
import com.luhanlin.spark.warn.service.BlackRuleWarning
import com.luhanlin.spark.warn.timer.SyncRule2Redis
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager
import redis.clients.jedis.Jedis

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-30 20:39
  */
object WarningStreamingTask extends Serializable with Logging{

  def main(args: Array[String]): Unit = {

     //定义一个定时器去定时同步 MYSQL到REDIS
     val timer : Timer = new Timer
    //SyncRule2Redis 任务类
    //0 第一次开始执行
    //1*60*1000  隔多少时间执行一次
    timer.schedule(new SyncRule2Redis,0,1*60*1000)

     //从kafka中获取数据流
     //val topics = "chl_test2".split(",")
    //kafka topic
     val topics = "test01".split(",")
       val ssc = SparkContextFactory.newSparkLocalStreamingContext("WarningStreamingTask111", java.lang.Long.valueOf(10),2)
    //  val ssc:StreamingContext = SparkContextFactory.newSparkStreamingContext("Kafka2esStreaming", java.lang.Long.valueOf(10))

    //构建kafkaManager
    val kafkaManager = new KafkaManager(
      Spark_Kafka_ConfigUtil.getKafkaParam(Kafka2esStreaming.kafkaConfig.getProperty("bootstrap.servers"), "WarningStreamingTask111")
    )
    //使用kafkaManager创建DStreaming流
    val kafkaDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
      //添加一个日期分组字段
      //如果数据其他的转换，可以先在这里进行统一转换
       .persist(StorageLevel.MEMORY_AND_DISK)

    kafkaDS.foreachRDD(rdd=>{

      //流量预警
//      val count_flow = rdd.map(x=>{
//        val flow = java.lang.Long.valueOf(x.get("collect_time"))
//        flow
//      }).reduce(_+_)
//      if(count_flow > 1719179595L){
//          println("流量预警: 阈值[1719179595L] 实际值:"+ count_flow)
//      }

//        if(!rdd.isEmpty()){
//         val flow_all = rdd.map(x=>{
//           val flow = java.lang.Long.valueOf(x.get("collect_time"))
//           flow
//         }).foreach(x=>{
//           println(x)
//         })
//         if(flow_all > 8557305985L){
//            println("流量预警，规定值:8557305985------实际值" +  flow_all)
//         }
//       }

      //客户端连接之类的 最好不要放在RDD外面，因为在处理partion时，
      // 数据需要分发到各个节点上去，
      //数据分发必须需要序列化才可以，如果不能序列化，分发会报错
      //如果这个数据 包括他里面的内容 都可以序列化，那么可以直接放在RDD外面
      rdd.foreachPartition(partition => {
          var jedis:Jedis = null
          try {
              jedis = JedisSingle.getJedis(15)
              while (partition.hasNext) {
                val map = partition.next()
                val table = map.get("table")
                val mapObject = map.asInstanceOf[java.util.Map[String,Object]]
                println(table)
                //开始比对
                BlackRuleWarning.blackWarning(mapObject,jedis)
              }
          } catch {
            case e => e.printStackTrace()
          } finally {
            JedisSingle.close(jedis)
          }
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
