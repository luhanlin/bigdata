package com.luhanlin.spark.common

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
/**
  * author: KING
  * description:
  * Date:Created in 2019-05-21 21:04
  */
object SparkConfFactory extends Serializable with Logging {


  /**
    * 获取sparkconf
    * @param appName
    * @param threads
    * @return
    */
  def newSparkLocalConf(appName: String = "spark local", threads: Int = 1): SparkConf = {
    new SparkConf().setMaster(s"local[$threads]").setAppName(appName)
  }



  def newSparkLocalStreamingContext(appName: String = "sparkStreaming",
                                    batchInterval:Long=30L,
                                    threads:Int=4): StreamingContext ={

    val sparkConf = SparkConfFactory.newSparkLocalConf(appName,threads)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    new StreamingContext(sparkConf,Seconds(batchInterval))

  }


}
