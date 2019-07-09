package org.apache.spark.streaming.kafka

import com.luhanlin.spark.streaming.kafka.SparkStreamingKafkaTest
import org.apache.spark.Logging

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-21 21:47
  */
object KafkaClusterTest extends Serializable with Logging{

  val topic = "test01"
  //获取kafka配置信息
  val kafkaParams = SparkStreamingKafkaTest.getKafkaParam(topic,"consumer2")
  //构造一个KafkaCluster
  @transient
  private var cluster = new KafkaCluster(kafkaParams)

  //定义一个单例
  def kc():KafkaCluster ={
    if(cluster == null){
      cluster = new KafkaCluster(kafkaParams)
    }
    cluster
  }

  def main(args: Array[String]): Unit = {
     //获取消费者着组名
     val groupId = "console-consumer-20586"
    //获取kafka本身的偏移量  Either类型可以认为就是封装了2钟信息
    val partitionsE = kc.getPartitions(Set(topic))
    require(partitionsE.isRight,s"获取partion失败")
    println("partitionsE=="  + partitionsE)
    val partitions = partitionsE.right.get
    println("打印分区信息")
    partitions.foreach(println(_))



    //获取kafka最早的offsets
    val earliestLeader = kc.getEarliestLeaderOffsets(partitions)
    require(earliestLeader.isRight,s"获取earliestLeader失败")
    val earliestLeaderOffsets = earliestLeader.right.get
    println("kafka最早的偏移量")
    earliestLeaderOffsets.foreach(println(_))


    //获取kafka最末尾的offsets
    val latestLeader = kc.getLatestLeaderOffsets(partitions)
    require(latestLeader.isRight,s"获取latestLeader失败")
    val latestLeaderOffsets = latestLeader.right.get
    println("kafka最末尾的偏移量")
    latestLeaderOffsets.foreach(println(_))

    //获取消费者的offsets
    val ConsumerOffsetE = kc.getConsumerOffsets(groupId,partitions)
    require(ConsumerOffsetE.isRight,s"获取ConsumerOffsetE失败")
    val consumerOffsets = ConsumerOffsetE.right.get
    println("打印消费者偏移量")
    consumerOffsets.foreach(println(_))
  }






}
