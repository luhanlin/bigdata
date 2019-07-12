package com.luhanlin.spark.streaming.kafka.kafka2hdfs2hive

import java.util

import org.apache.commons.configuration.{CompositeConfiguration, ConfigurationException, PropertiesConfiguration}
import org.apache.spark.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-04-29 09:23
  */
object HiveConfig extends Serializable with Logging {

  //HIVE 文件根目录
  var hive_root_path = "/user/hive_table/del_cache_table/"
  var hiveFieldPath = "es/mapping/fieldmapping.properties"

  var config: CompositeConfiguration = null
  //所有的表
  var tables: util.List[_] = null
  //表对应所有的字段映射,可以通过table名获取 这个table的所有字段
  var tableFieldsMap: util.Map[String, util.HashMap[String, String]] = null
  //StructType
  var mapSchema: util.Map[String, StructType] = null
  //建表语句
  var hiveTableSQL: util.Map[String, String] = null


  /**
    * 主要就是创建mapSchema  和  hiveTableSQL
    */
  initParams()

  def main(args: Array[String]): Unit = {
  }

  /**
    * 初始化HIVE参数
    */
  def initParams(): Unit = {
    //加载es/mapping/fieldmapping.properties 配置文件
    config = HiveConfig.readCompositeConfiguration(hiveFieldPath)
    println("==========================config====================================")
    config.getKeys.foreach(key => {
      println(key + ":" + config.getProperty(key.toString))
    })
    println("==========================tables====================================")
    //wechat,mail,qq
    tables = config.getList("tables")
    tables.foreach(table => {
      println(table)
    })
    println("======================tableFieldsMap================================")
    //(qq,{qq.imsi=string, qq.id=string, qq.send_message=string, qq.filename=string})
    tableFieldsMap = HiveConfig.getKeysByType()
    tableFieldsMap.foreach(x => {
      println(x)
    })
    println("=========================mapSchema===================================")
    mapSchema = HiveConfig.createSchema()
    mapSchema.foreach(x => {
      println(x)
    })
    println("=========================hiveTableSQL===================================")
    hiveTableSQL = HiveConfig.getHiveTables()
    hiveTableSQL.foreach(x => {
      println(x)
    })
  }


  /**
    * 读取hive 字段配置文件
    *
    * @param path
    * @return
    */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {
    logInfo("加载配置文件 " + path)
    //多配置工具
    val compositeConfiguration = new CompositeConfiguration
    try {
      val configuration = new PropertiesConfiguration(path)
      compositeConfiguration.addConfiguration(configuration)
    } catch {
      case e: ConfigurationException => {
        logError("加载配置文件 " + path + "失败", e)
      }
    }
    logInfo("加载配置文件" + path + "成功。 ")
    compositeConfiguration
  }


  /**
    * 获取table-字段 对应关系
    * 使用 util.Map[String,util.HashMap[String, String结构保存
    *
    * @return
    */
  def getKeysByType(): util.Map[String, util.HashMap[String, String]] = {

    val map = new util.HashMap[String, util.HashMap[String, String]]()
    //wechat, mail, qq
    val iteratorTable = tables.iterator()
    //对每个表进行遍历
    while (iteratorTable.hasNext) {
      //使用一个MAP保存一种对应关系
      val fieldMap = new util.HashMap[String, String]()
      //获取一个表
      val table: String = iteratorTable.next().toString
      //获取这个表的所有字段
      val fields = config.getKeys(table)
      //获取通用字段  这里暂时没有
      val commonKeys: util.Iterator[String] = config.getKeys("common").asInstanceOf[util.Iterator[String]]
      //将通用字段放到map结构中去
      while (commonKeys.hasNext) {
        val key = commonKeys.next()
        fieldMap.put(key.replace("common", table), config.getString(key))
      }
      //将每种表的私有字段放到map中去
      while (fields.hasNext) {
        val field = fields.next().toString
        fieldMap.put(field, config.getString(field))
      }
      map.put(table, fieldMap)
    }
    map
  }


  /**
    * 构建建表语句
    *
    * @return
    */
  def getHiveTables(): util.Map[String, String] = {

    val hiveTableSqlMap: util.Map[String, String] = new util.HashMap[String, String]()

    //获取没中数据的建表语句
    tables.foreach(table => {

      var sql: String = s"CREATE external TABLE IF NOT EXISTS ${table} ("

      val tableFields = config.getKeys(table.toString)
      tableFields.foreach(tableField => {
        //qq.imsi=string, qq.id=string, qq.send_message=string
        val fieldType = config.getProperty(tableField.toString)
        val field = tableField.toString.split("\\.")(1)
        sql = sql + field
        fieldType match {
          //就是将配置中的类型映射为HIVE 建表语句中的类型
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ => println("Nothing Matched!!" + fieldType)
        }

      })
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s")STORED AS PARQUET location '${hive_root_path}${table}'"
      /*      sql = sql + s") partitioned by(year string,month string,day string) STORED AS PARQUET " +
              s"location '${path}${table}'"*/
      hiveTableSqlMap.put(table.toString, sql)
    })
    hiveTableSqlMap
  }

  /**
    * 使用tableFieldsMap
    * 对每种类型数据创建对应的Schema
    *
    * @return
    */
  def createSchema(): util.Map[String, StructType] = {
    // schema  表结构
    /*   CREATE TABLE `warn_message` (
         //arrayStructType
         `id` int(11) NOT NULL AUTO_INCREMENT,
         `alarmRuleid` varchar(255) DEFAULT NULL,
         `alarmType` varchar(255) DEFAULT NULL,
         `sendType` varchar(255) DEFAULT NULL,
         `sendMobile` varchar(255) DEFAULT NULL,
         `sendEmail` varchar(255) DEFAULT NULL,
         `sendStatus` varchar(255) DEFAULT NULL,
         `senfInfo` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
         `hitTime` datetime DEFAULT NULL,
         `checkinTime` datetime DEFAULT NULL,
         `isRead` varchar(255) DEFAULT NULL,
         `readAccounts` varchar(255) DEFAULT NULL,
         `alarmaccounts` varchar(255) DEFAULT NULL,
         `accountid` varchar(11) DEFAULT NULL,
         PRIMARY KEY (`id`)
       ) ENGINE=MyISAM AUTO_INCREMENT=528 DEFAULT CHARSET=latin1;*/


    val mapStructType: util.Map[String, StructType] = new util.HashMap[String, StructType]()

    for (table <- tables) {
      //通过tableFieldsMap 拿到这个表的所有字段
      val tableFields = tableFieldsMap.get(table)
      //对这个字段进行遍历
      val keyIterator = tableFields.keySet().iterator()
      //创建ArrayBuffer
      var arrayStructType = ArrayBuffer[StructField]()
      while (keyIterator.hasNext) {
        val key = keyIterator.next()
        val value = tableFields.get(key)
        //将key拆分 获取 "."后面的部分作为数据字段
        val field = key.split("\\.")(1)
        value match {
          /* case "string" => arrayStructType += StructField(field, StringType, true)
           case "long"   => arrayStructType += StructField(field, LongType, true)
           case "double"   => arrayStructType += StructField(field, DoubleType, true)*/
          case "string" => arrayStructType += StructField(field, StringType, true)
          case "long" => arrayStructType += StructField(field, StringType, true)
          case "double" => arrayStructType += StructField(field, StringType, true)
          case _ => println("Nothing Matched!!" + value)
        }
      }
      val schema = StructType(arrayStructType)
      mapStructType.put(table.toString, schema)
    }
    mapStructType
  }

}
