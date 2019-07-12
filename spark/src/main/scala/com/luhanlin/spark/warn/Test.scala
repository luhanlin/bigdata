package com.luhanlin.spark.warn

import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import com.luhanlin.spark.streaming.kafka.kafka2es.Kafka2esStreaming
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * author: KING
  * description:
  * Date:Created in 2019-06-04 16:32
  */
object Test extends Serializable with Logging {


  def main(args: Array[String]): Unit = {
    val topics = "test01".split(",")
    val ssc = SparkContextFactory.newSparkLocalStreamingContext("WarningStreamingTask222", java.lang.Long.valueOf(10), 1)
    //  val ssc:StreamingContext = SparkContextFactory.newSparkStreamingContext("Kafka2esStreaming", java.lang.Long.valueOf(10))

    //构建kafkaManager
    val kafkaManager = new KafkaManager(
      Spark_Kafka_ConfigUtil.getKafkaParam(Kafka2esStreaming.kafkaConfig.getProperty("bootstrap.servers"), "WarningStreamingTask222")
    )
    //使用kafkaManager创建DStreaming流
    val kafkaDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
      //添加一个日期分组字段
      //如果数据其他的转换，可以先在这里进行统一转换
      .persist(StorageLevel.MEMORY_AND_DISK)

    kafkaDS.foreachRDD(rdd => {
        val count = rdd.map(x=>{
          val flow = java.lang.Long.valueOf(x.get("collect_time"))
          flow
        }).reduce(_+_)

        println("count==========" + count)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
