package com.luhanlin.spark.streaming.kafka.kafka2hbase

import java.util.Properties

import com.bigdata.common.config.ConfigUtil
import com.luhanlin.hbase.hbase.config.HBaseTableUtil
import com.luhanlin.hbase.hbase.insert.HBaseInsertHelper
import com.luhanlin.hbase.hbase.spilt.SpiltRegionUtil
import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-31 15:30
  */
object DataRelationStreaming extends Serializable with Logging{



  // 读取需要关联的配置文件字段
  // phone_mac,phone,username,send_mail,imei,imsi
  val relationFields = ConfigUtil.getInstance()
    .getProperties("spark/relation.properties")
    .get("relationfield")
    .toString
    .split(",")
  def main(args: Array[String]): Unit = {


    //初始化hbase表
   // initRelationHbaseTable(relationFields)

   val ssc = SparkContextFactory.newSparkLocalStreamingContext("DataRelationStreaming", java.lang.Long.valueOf(10),1)
    //  val ssc = SparkContextFactory.newSparkStreamingContext("DataRelationStreaming", java.lang.Long.valueOf(10))
    val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
    val topics = "chl_test8".split(",")
    val kafkaDS = new KafkaManager(Spark_Kafka_ConfigUtil
                                  .getKafkaParam(kafkaConfig.getProperty("bootstrap.servers"),
                                  "DataRelationStreaming9"))
                                  .createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
                                  .persist(StorageLevel.MEMORY_AND_DISK)


    kafkaDS.foreachRDD(rdd=>{

      rdd.foreachPartition(partition=>{
          //对partion进行遍历
          while (partition.hasNext){

            //获取每一条流数据
            val map = partition.next()
            //获取mac 主键
            var phone_mac:String = map.get("phone_mac")
            //获取所有关联字段 //phone_mac,phone,username,send_mail,imei,imsi
            relationFields.foreach(relationFeild =>{
                //relationFields 是关联字段，需要进行关联处理的，所有判断
               // map中是不是包含这个字段，如果包含的话，取出来进行处理
                if(map.containsKey(relationFeild)){
                  //创建主关联  并遍历关联字段进行关联
                  val put = new Put(phone_mac.getBytes())
                  //取关联字段的值
                  //TODO  到这里  主关联表的 主键和值都有了  然后封装成PUT写入hbase主关联表就行了
                  val value = map.get(relationFeild)
                  //自定义版本号  通过 (表字段名 + 字段值 取hashCOde)
                  //因为值有可能是字符串，但是版本号必须是long类型，所以这里我们需要
                  //将字符串影射唯一数字，而且必须是正整数
                  val versionNum = (relationFeild+value).hashCode() & Integer.MAX_VALUE
                  put.addColumn("cf".getBytes(), Bytes.toBytes(relationFeild),versionNum ,Bytes.toBytes(value.toString))
                  HBaseInsertHelper.put("test:relation",put)
                  println(s"往主关联表 test:relation 里面写入数据  rowkey=>${phone_mac} version=>${versionNum} 类型${relationFeild} value=>${value}")

                  //建立二级索引
                  //使用关联字段的值最为二级索引的rowkey
                  // 二级索引就是把这个字段的值作为索引表rowkey,
                  // 把这个字段的mac做为索引表的值
                  val put_2 = new Put(value.getBytes())//把这个字段的值作为索引表rowkey,
                  val table_name = s"test:${relationFeild}"//往索引表里面取写
                  //使用主表的rowkey  就是 取hash作为二级索引的版本号
                  val versionNum_2 = phone_mac.hashCode() & Integer.MAX_VALUE
                  put_2.addColumn("cf".getBytes(), Bytes.toBytes("phone_mac"),versionNum_2 ,Bytes.toBytes(phone_mac.toString))
                  HBaseInsertHelper.put(table_name,put_2)
                  println(s"往二级索表 ${table_name}里面写入数据  rowkey=>${value} version=>${versionNum_2} value=>${phone_mac}")
                }
            })
          }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }



  def initRelationHbaseTable(relationFields:Array[String]): Unit ={

    //初始化总关联表
    val relation_table = "test:relation"
     HBaseTableUtil.createTable(relation_table,
       "cf",
       true,
       -1,
       100,
       SpiltRegionUtil.getSplitKeysBydinct)
    //HBaseTableUtil.deleteTable(relation_table)

    //遍历所有关联字段，根据字段创建二级索引表
    relationFields.foreach(field=>{
      val hbase_table = s"test:${field}"
        HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
      // HBaseTableUtil.deleteTable(hbase_table)
    })
  }

}
