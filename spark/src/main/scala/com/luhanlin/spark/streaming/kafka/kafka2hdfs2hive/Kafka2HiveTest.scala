package com.luhanlin.spark.streaming.kafka.kafka2hdfs2hive

import java.util

import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.hdfs.HdfsAdmin
import com.luhanlin.spark.hive.HiveConf
import com.luhanlin.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import com.luhanlin.spark.streaming.kafka.kafka2es.Kafka2esStreaming
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-29 14:02
  */
object Kafka2HiveTest extends Serializable with Logging{

  val topics = "test01".split(",")
  //获取所有数据类型
  //获取所有数据的Schema


  def main(args: Array[String]): Unit = {
     val ssc = SparkContextFactory.newSparkLocalStreamingContext("Kafka2HiveTest", java.lang.Long.valueOf(10),1)
//    val ssc = SparkContextFactory.newSparkStreamingContext("Kafka2HiveTest", java.lang.Long.valueOf(10))

    //1.创建HIVE表  hiveSQL已經創建好了
    val sc = ssc.sparkContext
    val hiveContext: HiveContext = HiveConf.getHiveContext(sc)
    hiveContext.setConf("spark.sql.parquet.mergeSchema", "true")
    createHiveTable(hiveContext)

    //kafka拿到流数据
    val kafkaDS = new KafkaManager(Spark_Kafka_ConfigUtil
                                    .getKafkaParam(Kafka2esStreaming.kafkaConfig.getProperty("bootstrap.servers"),
                                      "Kafka2HiveTest"))
                                    .createJsonToJMapStringDirectStreamWithOffset(ssc, topics.toSet)
                                    .persist(StorageLevel.MEMORY_AND_DISK)


    HiveConfig.tables.foreach(table=>{

      //过滤出单一数据类型(获取和table相同类型的所有数据)
       val tableDS = kafkaDS.filter(x => {table.equals(x.get("table"))})
      //获取数据类型的schema 表结构
      val schema = HiveConfig.mapSchema.get(table)
      //获取这个表的所有字段
      val schemaFields: Array[String] = schema.fieldNames
      tableDS.foreachRDD(rdd=>{


        //TODO 数据写入HDFS
/*        val sc = rdd.sparkContext
        val hiveContext = HiveConf.getHiveContext(sc)
        hiveContext.sql(s"USE DEFAULT")*/
        //将RDD转为DF   原因：要加字段描述，写比较方便 DF
        val tableDF = rdd2DF(rdd,schemaFields,hiveContext,schema)
        //多种数据一起处理
        val path_all = s"hdfs://bigdata124:8020${HiveConfig.hive_root_path}${table}"
        val exists = HdfsAdmin.get().getFs.exists(new Path(path_all))
        //2.写到HDFS   不管存不存在我们都要把数据写入进去 通过追加的方式

        //每10秒写一次，写一次会生成一个文件
        tableDF.write.mode(SaveMode.Append).parquet(path_all)
        //3.加载数据到HIVE
        if (!exists) {
          //如果不存在 进行首次加载
          System.out.println("===================开始加载数据到分区=============")
          hiveContext.sql(s"ALTER TABLE ${table} LOCATION '${path_all}'")
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 创建HIVE表
    * @param hiveContext
    */
  def createHiveTable(hiveContext: HiveContext): Unit ={
    val keys = HiveConfig.hiveTableSQL.keySet()
    keys.foreach(key=>{
      val sql = HiveConfig.hiveTableSQL.get(key)
      //通过hiveContext 和已经创建好的SQL语句去创建HIVE表
      hiveContext.sql(sql)
      println(s"创建表${key}成功")
    })
  }


  /**
    * 将RDD转为DF
    * @param rdd
    * @param schemaFields
    * @param hiveContext
    * @param schema
    * @return
    */
  def rdd2DF(rdd:RDD[util.Map[String,String]],
             schemaFields: Array[String],
             hiveContext:HiveContext,
             schema:StructType): DataFrame ={

    //将RDD[Map[String,String]]转为RDD[ROW]
    val rddRow = rdd.map(record => {
        val listRow: util.ArrayList[Object] = new util.ArrayList[Object]()
        for (schemaField <- schemaFields) {
          listRow.add(record.get(schemaField))
        }
        Row.fromSeq(listRow)
    }).repartition(1)
    //构建DF
    //def createDataFrame(rowRDD: RDD[Row], schema: StructType)
    val typeDF = hiveContext.createDataFrame(rddRow, schema)
    typeDF
  }

}
