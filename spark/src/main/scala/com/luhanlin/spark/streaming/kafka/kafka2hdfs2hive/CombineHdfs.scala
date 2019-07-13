package com.luhanlin.spark.streaming.kafka.kafka2hdfs2hive

import com.luhanlin.spark.common.SparkContextFactory
import com.luhanlin.spark.hdfs.HdfsAdmin
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:  合并HDFS小文件任务
  * Date:Created in 2019-06-01 13:24
  */
object CombineHdfs extends Serializable with Logging{


  def main(args: Array[String]): Unit = {
  //  val sparkContext = SparkContextFactory.newSparkBatchContext("CombineHdfs")

    val sparkContext = SparkContextFactory.newSparkLocalBatchContext("CombineHdfs")
    //创建一个 sparkSQL
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    //遍历表 就是遍历HIVE表
    HiveConfig.tables.foreach(table=>{
          //获取HDFS文件目录
        val table_path =s"${HiveConfig.hive_root_path}$table"  //apps/hive/warehouse/external/mail
        //通过sparkSQL 加载 这些目录的文件
        val tableDF = sqlContext.read.load(table_path)
        //先获取原来数据种的所有文件  HDFS文件 API
        val fileSystem:FileSystem = HdfsAdmin.get().getFs
        //通过globStatus 获取目录下的正则匹配文件
        val arrayFileStatus = fileSystem.globStatus(new Path(table_path+"/part*"))
        //stat2Paths将文件状态转为文件路径   //这个文件路径是用来删除的
        val paths = FileUtil.stat2Paths(arrayFileStatus)
        //写入合并文件  //repartition 需要根据生产中实际情况去定义
         tableDF.repartition(1).write.mode(SaveMode.Append).parquet(table_path)
         println("写入" + table_path +"成功")
        //删除小文件
        paths.foreach(path =>{
          HdfsAdmin.get().getFs.delete(path)
          println("删除文件" + path + "成功")
        })

    })
  }
}
