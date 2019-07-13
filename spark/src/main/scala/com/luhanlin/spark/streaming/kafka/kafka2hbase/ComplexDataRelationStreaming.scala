package com.luhanlin.spark.streaming.kafka.kafka2hbase

import java.util
import java.util.Properties

import com.bigdata.common.config.ConfigUtil
import com.luhanlin.hbase.hbase.config.HBaseTableUtil
import com.luhanlin.hbase.hbase.extractor.{MapRowExtrator, SingleColumnMultiVersionRowExtrator}
import com.luhanlin.hbase.hbase.insert.HBaseInsertHelper
import com.luhanlin.hbase.hbase.search.{HBaseSearchService, HBaseSearchServiceImpl}
import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:
  * Date:Created in 2019-06-08 21:43
  */
object ComplexDataRelationStreaming extends Serializable with Logging {

  val complex_relationfield = ConfigUtil.getInstance()
    .getProperties("spark/relation.properties")
    .get("complex_relationfield")
    .toString
    .split(",")
  def main(args: Array[String]): Unit = {


    //初始化hbase表
   //nitRelationHbaseTable(complex_relationfield)

    val ssc = SparkContextFactory.newSparkLocalStreamingContext("ComplexDataRelationStreaming", java.lang.Long.valueOf(10), 1)
    //  val ssc = SparkContextFactory.newSparkStreamingContext("DataRelationStreaming", java.lang.Long.valueOf(10))
    val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
    val topics = "chl_test8".split(",")
    val kafkaDS = new KafkaManager(Spark_Kafka_ConfigUtil
      .getKafkaParam(kafkaConfig.getProperty("bootstrap.servers"),
        "ComplexDataRelationStreaming"))
      .createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
      .persist(StorageLevel.MEMORY_AND_DISK)


    kafkaDS.foreachRDD(rdd => {

      rdd.foreachPartition(partion => {

        while (partion.hasNext) {
          //双向关联
          val map = partion.next()
          //获取表名
          val table = map.get("table")
          //首先判断是不是特殊表
          if (table.equals("card")) {

            val card = map.get("card")
            val phone = map.get("phone")
            if (StringUtils.isNotBlank(phone)) {
              //从索引表中去找手机号码，判断是
              var table:Table = null
              var exists = false
              //如果存在，则说明是总关联表先入库，则直接将phone 合并到总关联表钟
              try {
                table = HBaseTableUtil.getTable("test:phone")
                exists = HBaseSearchServiceImpl.existsRowkey(table, phone)

                if (exists) {
                  //從索引表中获取总关联表的rowkey  获取phone对应的多版本 MAC
                  val baseSearchService: HBaseSearchService = new HBaseSearchServiceImpl()
                  val get = new Get(phone.getBytes)
                  get.setMaxVersions(100)
                  val set = new util.HashSet[String]()
                  val extrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), set)
                  val macSet = baseSearchService.search(table.getName.getNameAsString, get, extrator)


                  macSet.foreach(macRowkey => {
                    val put = new Put(macRowkey.getBytes())
                    val value = card
                    val versionNum = ("card" + value).hashCode() & Integer.MAX_VALUE
                    put.addColumn("cf".getBytes(), Bytes.toBytes("card"), versionNum, Bytes.toBytes(value.toString))
                    HBaseInsertHelper.put("test:relation", put)

                    //构建索引表
                    val put_2 = new Put(value.getBytes())
                    val table_name = s"test:card"
                    //使用主表的rowkey 取hash作为二级索引的版本号
                    val versionNum_2 = macRowkey.hashCode() & Integer.MAX_VALUE
                    put_2.addColumn("cf".getBytes(), Bytes.toBytes("phone_mac"), versionNum_2, Bytes.toBytes(macRowkey.toString))
                    HBaseInsertHelper.put(table_name, put_2)

                  })

                } else {
                  //如果不存在，则说明是总关联数据后入，需要关联进去的数据先入，
                  //将这条数据放入缓存表中
                  val put = new Put(phone.getBytes())
                  put.addColumn("cf".getBytes(), Bytes.toBytes("card"), Bytes.toBytes(card.toString))
                  HBaseInsertHelper.put("cache:phone", put)
                }
              } catch {
                case e => logError(null,e)
              }finally {
                HBaseTableUtil.close(table)
              }
            }

          } else {
            //如果不是特殊表，处理通用表
            val phone_mac: String = map.get("phone_mac")
            //获取所有关联字段 //phone_mac,wechat,send_mail
            complex_relationfield.foreach(relationFeild => {

              if (map.containsKey(relationFeild)) {

                //因为需要双向关联，所以这里也要通过phone取去查找缓存表中是否存在phone
                if("phone".equals(relationFeild)){
                  //判断次电话号码在缓存表中是否存在
                  val phone = map.get("phone")
                  var table:Table = null
                  var exists = false
                  try {
                    table = HBaseTableUtil.getTable("cache:phone")
                    exists = HBaseSearchServiceImpl.existsRowkey(table, phone)
                  } catch {
                    case e => logError(null,e)
                  }finally {
                    HBaseTableUtil.close(table)
                  }

                  //如果缓存表中存在这个电话号码，则将缓存中的身份取出来，写入到总关联表中
                  if(exists){
                    val baseSearchServiceI : HBaseSearchService = new HBaseSearchServiceImpl()
                    val cacheMap = baseSearchServiceI.search("cache:phone",new Get(phone.getBytes()),new MapRowExtrator)
                    val card =  cacheMap.get("card")

                    /**写入数据到总关联表中，总关联表以MAC作为主键**/
                    //  使用MAC作为主键
                    val put = new Put(phone_mac.getBytes())
                    //使用身份证作为值
                    val value = card
                    //使用"card" + 身份证号 作为版本号
                    val versionNum = ("card" + value).hashCode() & Integer.MAX_VALUE
                    put.addColumn("cf".getBytes(), Bytes.toBytes("card"), versionNum, Bytes.toBytes(value.toString))
                    HBaseInsertHelper.put("test:relation", put)
                  }
                }
                  //主关联表
                  val put = new Put(phone_mac.getBytes())
                  val value = map.get(relationFeild)
                  val versionNum = (relationFeild + value).hashCode() & Integer.MAX_VALUE
                  put.addColumn("cf".getBytes(), Bytes.toBytes(relationFeild), versionNum, Bytes.toBytes(value.toString))
                 println("put====" + put)
                 HBaseInsertHelper.put("test:relation", put)
                  //建立二级索引
                  //使用关联字段的值最为二级索引的rowkey
                  val put_2 = new Put(value.getBytes())
                  val table_name = s"test:${relationFeild}"
                  //使用主表的rowkey 取hash作为二级索引的版本号
                  val versionNum_2 = phone_mac.hashCode() & Integer.MAX_VALUE
                  put_2.addColumn("cf".getBytes(), Bytes.toBytes("phone_mac"), versionNum_2, Bytes.toBytes(phone_mac.toString))
                  HBaseInsertHelper.put(table_name, put_2)

              }
            })
          }
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def initRelationHbaseTable(complex_relationfield: Array[String]): Unit = {

    val relation_table = "test:relation"
    //HBaseTableUtil.createTable(relation_table, "cf", true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
    HBaseTableUtil.deleteTable(relation_table)

    val cache_table =  "cache:phone"
    //HBaseTableUtil.createTable(cache_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct)
    HBaseTableUtil.deleteTable(cache_table)

    DataRelationStreaming.relationFields.foreach(field => {
      val hbase_table = s"test:${field}"
      //HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
      HBaseTableUtil.deleteTable(hbase_table)
    })
  }

}
