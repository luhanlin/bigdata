package com.luhanlin.spark.streaming.kafka

import com.luhanlin.spark.common.SparkConfFactory
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}


/**
  * author: KING
  * description: 演示streaming 从kafka里面消费数据 不持久offset
  * Date:Created in 2019-05-21 21:08
  */
object SparkStreamingKafkaTest extends Serializable with Logging{

  val topic = "test01"
  def main(args: Array[String]): Unit = {

    //获取streamingContext参数
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("SparkStreamingKafkaTest",
      10L,
      1)
    //获取kafka配置信息
    val kafkaParams = getKafkaParam(topic,"consumer1")
    //从kafka中获取DS流
    val kafkaDS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))
    //(null,{"rksj":"1558178497","latitude":"24.000000","imsi":"000000000000000"})
    kafkaDS.foreachRDD(rdd=>{
      //获取rdd中保存的 kafka offsets信息
      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsList.foreach(x=>{
        println("获取rdd中的偏移信息" + x)
      })
      rdd.foreach(x=>{
        x==""
//        println(x)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取kafkaParams
    * @param kafkaTopic
    * @param groupId
    * @return
    */
  def getKafkaParam(kafkaTopic:String,groupId : String): Map[String,String]={
    val kafkaParam=Map[String,String](
      "metadata.broker.list" -> "bigdata124:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> groupId,
      "refresh.leader.backoff.ms" -> "1000",
      "num.consumer.fetchers" -> "8")
    kafkaParam
  }

}
