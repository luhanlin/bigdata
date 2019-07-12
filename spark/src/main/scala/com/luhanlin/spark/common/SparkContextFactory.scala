
package com.luhanlin.spark.common

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkContext}

object SparkContextFactory {

  def newSparkBatchContext(appName:String = "sparkBatch") : SparkContext = {
    val sparkConf = SparkConfFactory.newSparkBatchConf(appName)
    new SparkContext(sparkConf)
  }

  def newSparkLocalBatchContext(appName:String = "sparkLocalBatch" , threads : Int = 2) : SparkContext = {

    val sparkConf = SparkConfFactory.newSparkLocalConf(appName, threads)
    sparkConf.set("","")
    new SparkContext(sparkConf)
  }

  def getAccumulator(appName:String = "sparkBatch") : Accumulator[Int] = {
    val sparkConf = SparkConfFactory.newSparkBatchConf(appName)
    val accumulator: Accumulator[Int] = new SparkContext(sparkConf).accumulator(0,"")
    accumulator
  }

  /**
    * 创建本地流streamingContext
    * @param appName             appName
    * @param batchInterval      多少秒读取一次
    * @param threads             开启多少个线程
    * @return
    */
  def newSparkLocalStreamingContext(appName:String = "sparkStreaming" ,
                                    batchInterval:Long = 30L ,
                                    threads : Int = 4) : StreamingContext = {

    val sparkConf =  SparkConfFactory.newSparkLocalConf(appName, threads)
    // sparkConf.set("spark.streaming.receiver.maxRate","10000")
     sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    new StreamingContext(sparkConf, Seconds(batchInterval))
  }


  /**
    * 创建集群模式streamingContext
    * 这里不设置线程数，在submit中指定
    * @param appName
    * @param batchInterval
    * @return
    */
  def newSparkStreamingContext(appName:String = "sparkStreaming" , batchInterval:Long = 30L) : StreamingContext = {
    
     val sparkConf = SparkConfFactory.newSparkStreamingConf(appName)
     new StreamingContext(sparkConf, Seconds(batchInterval))
  }

  def startSparkStreaming(ssc:StreamingContext){
     ssc.start()
	 ssc.awaitTermination()
	 ssc.stop()
  }


}