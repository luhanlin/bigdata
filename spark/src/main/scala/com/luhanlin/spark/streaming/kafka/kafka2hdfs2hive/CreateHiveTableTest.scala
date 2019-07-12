package com.luhanlin.spark.streaming.kafka.kafka2hdfs2hive

import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.hive.HiveConf
import org.apache.spark.sql.hive.HiveContext

/**
  * author: KING
  * description:
  * Date:Created in 2019-06-01 22:24
  */
object CreateHiveTableTest {


  def main(args: Array[String]): Unit = {
     val ssc = SparkContextFactory.newSparkLocalStreamingContext("Kafka2HiveTest", java.lang.Long.valueOf(10),1)
//    val ssc = SparkContextFactory.newSparkStreamingContext("Kafka2HiveTest", java.lang.Long.valueOf(10))

    //1.创建HIVE表  hiveSQL已經創建好了
    val sc = ssc.sparkContext
    val hiveContext: HiveContext = HiveConf.getHiveContext(sc)
    hiveContext.setConf("spark.sql.parquet.mergeSchema", "true")
    Kafka2HiveTest.createHiveTable(hiveContext)


    ssc.start()
    ssc.awaitTermination()
  }
}
