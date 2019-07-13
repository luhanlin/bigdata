package com.luhanlin.spark.streaming.kafka.kafka2hbase

import java.util.Properties

import com.bigdata.common.config.ConfigUtil
import com.luhanlin.hbase.hbase.config.HBaseTableUtil
import com.luhanlin.hbase.hbase.spilt.SpiltRegionUtil
import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * author:
  * Date:Created in 2019-04-23 10:25
  */
object TZ_kafka2hbase extends Serializable with Logging{

  val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
  val topics = "chl_test8".split(",")

  def main(args: Array[String]): Unit = {
    //sparkParamCheck(args)

    val hbase_table = "test:chl_test8"
    HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct)


    val kafkaManager = new KafkaManager(
      Spark_Kafka_ConfigUtil.getKafkaParam(kafkaConfig.getProperty("bootstrap.servers"), "TZ_kafka2hbase")
    )

    val ssc = SparkContextFactory.newSparkLocalStreamingContext("TZ_kafka2hbase",
      java.lang.Long.valueOf(10),1)

    val kafkaDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
      .persist(StorageLevel.MEMORY_AND_DISK)

    Kafka2hbaseJob.insertHbase(kafkaDS,hbase_table)
    ssc.start()
    ssc.awaitTermination()
  }
}
