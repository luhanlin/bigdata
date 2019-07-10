package com.luhanlin.spark.common.convert

import java.util

import com.bigdata.common.config.ConfigUtil
import org.apache.spark.Logging

import scala.collection.JavaConversions._

/**
  * author: KING
  * description:  数据类型转换
  * Date:Created in 2019-05-23 21:58
  */
object DataConvert extends Serializable with Logging{

   val fieldMappingPath = "es/mapping/fieldmapping.properties"

   private val typeFieldMap: util.HashMap[String, util.HashMap[String, String]] = getEsFieldtypeMap()


  /**
    * 将MMap<String,String>转化为Map<String,Object>
    */
  def strMap2esObjectMap(map : java.util.Map[String,String]): java.util.Map[String,Object] ={

    //获取配置文件中的数据类型
    val table = map.get("table")
    //获取配置文件钟的数据类型的 字段类型
    val fieldmappingMap = typeFieldMap.get(table)
    //获取数据类型的所有字段
    val tableKeySet = fieldmappingMap.keySet()


    var objectMap = new java.util.HashMap[String, Object]()
    //获取真实数据的所有字段
    val set = map.keySet().iterator()
    try {
      //遍历真实数据的所有字段
      while (set.hasNext()) {
        val key = set.next()
        var dataType:String = "string"

        //如果在配置文件中的key包含真实数据的key
        if(tableKeySet.contains(key)){
          //则获取真实数据字段的数据类型
          dataType = fieldmappingMap.get(key)
        }

        dataType match {
          case "long" => objectMap = BaseDataConvert.mapString2Long(map, key, objectMap)
          case "string" => objectMap = BaseDataConvert.mapString2String(map, key, objectMap)
          case "double" => objectMap = BaseDataConvert.mapString2Double(map, key, objectMap)
          case _ => objectMap = BaseDataConvert.mapString2String(map, key, objectMap)
        }
      }
    } catch {
      case e: Exception => logInfo("转换异常", e)
    }
    println("转换后" + objectMap)
    objectMap
  }



  /**
    * 读取 "es/mapping/fieldmapping.properties 配置文件
    * 主要作用是将 真实数据 根据配置来作数据类型转换 转换为和ES mapping结构保持一致
    * @return
    */
  def getEsFieldtypeMap(): util.HashMap[String, util.HashMap[String, String]] = {

   // ["wechat":["phone_mac":"string","latitude":"long"]]
    //定义返回Map
   val mapMap = new util.HashMap[String, util.HashMap[String, String]]
    val properties = ConfigUtil.getInstance().getProperties(fieldMappingPath)
    val tables = properties.get("tables").toString.split(",")
    val tableFields = properties.keySet()

    tables.foreach(table => {
      val map = new util.HashMap[String, String]()
      tableFields.foreach(tableField => {
        if (tableField.toString.startsWith(table)) {
          val key = tableField.toString.split("\\.")(1)
          val value = properties.get(tableField).toString
          map.put(key, value)
        }
      })
      mapMap.put(table, map)
    })
    mapMap
  }
}
