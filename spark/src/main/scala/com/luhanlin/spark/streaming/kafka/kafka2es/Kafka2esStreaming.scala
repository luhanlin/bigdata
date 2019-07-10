package com.luhanlin.spark.streaming.kafka.kafka2es

import java.util
import java.util.Properties

import com.bigdata.common.config.ConfigUtil
import com.bigdata.common.datatype.DataTypeProperties
import com.luhanlin.spark.common.SparkConfFactory
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-23 21:23
  */
object Kafka2esStreaming extends Serializable with Logging{
  //获取数据类型
  private val dataTypes: util.Set[String] = DataTypeProperties.dataTypeMap.keySet()

  val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
  val topics = "test01".split(",")


  def main(args: Array[String]): Unit = {

    val ssc = SparkConfFactory.newSparkLocalStreamingContext("TZ_kafka2es", java.lang.Long.valueOf(10),1)

    //构建kafkaManager
    val kafkaManager = new KafkaManager(
      Spark_Kafka_ConfigUtil.getKafkaParam(kafkaConfig.getProperty("bootstrap.servers"), "TZ3")
    )
    //使用kafkaManager创建DStreaming流
    val kafkaDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
      .persist(StorageLevel.MEMORY_AND_DISK)

    //使用par并发集合可以是任务并发执行。在资源充足的情况下
    dataTypes.foreach(dataType=>{
      //过滤出单个类别的数据种类
      val tableDS = kafkaDS.filter(x=>{dataType.equals(x.get("table"))})
      Kafka2EsJob.insertData2Es(dataType,tableDS)
    })

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 启动参数检查
    * @param args
    */
  def sparkParamCheck(args: Array[String]): Unit ={
    if (args.length == 4) {
      if (StringUtils.isBlank(args(1))) {
        logInfo("kafka集群地址不能为空")
        logInfo("kafka集群地址格式为     主机1名：9092,主机2名：9092,主机3名：9092...")
        logInfo("格式为     主机1名：9092,主机2名：9092,主机3名：9092...")
        System.exit(-1)
      }
      if (StringUtils.isBlank(args(2))) {
        logInfo("kafka topic1不能为空")
        System.exit(-1)
      }
      if (StringUtils.isBlank(args(3))) {
        logInfo("kafka topic2不能为空")
        System.exit(-1)
      }
    }else{
      logError("启动参数个数错误")
    }
  }

  def startJob(ds:DStream[String]): Unit ={
  }


}
