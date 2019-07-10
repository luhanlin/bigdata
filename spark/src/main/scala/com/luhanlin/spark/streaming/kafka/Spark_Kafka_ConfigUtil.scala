package com.luhanlin.spark.streaming.kafka

import org.apache.spark.Logging

/**
  * author:
  * description:
  * Date:Created in 2019-04-17 17:03
  */
object Spark_Kafka_ConfigUtil extends Serializable with Logging{

  def getKafkaParam(brokerList:String,groupId : String): Map[String,String]={
    val kafkaParam=Map[String,String](
      "bootstrap.servers" -> brokerList,
      "auto.offset.reset" -> "smallest",
      "group.id" -> groupId,
      "refresh.leader.backoff.ms" -> "1000",
      "num.consumer.fetchers" -> "8")
    kafkaParam
  }

}
