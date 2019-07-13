package com.luhanlin.spark.streaming.kafka.kafka2hbase

import java.util

/**
  * author: KING
  * description:
  * Date:Created in 2019-06-04 22:01
  */
object Test {
  def main(args: Array[String]): Unit = {
  //  DataRelationStreaming.initRelationHbaseTable(DataRelationStreaming.relationFields)
  // ComplexDataRelationStreaming.initRelationHbaseTable(ComplexDataRelationStreaming.complex_relationfield)
  /*  val table = HBaseTableUtil.getTable("test:relation")
    val exists = HBaseSearchServiceImpl.existsRowkey(table, "1c-41-cd-ae-4f-4f")
    println(exists)*/
    //從索引表中获取总关联表的rowkey

   /* val baseSearchService : HBaseSearchService = new HBaseSearchServiceImpl()
    val get = new Get("1c-41-cd-ae-4f-4f".getBytes())
    get.setMaxVersions(100)
     val set = new util.HashSet[String]()
    val extrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(),"imei".getBytes(),set)
    val clSet = baseSearchService.search("test:relation",get,extrator)
    println(clSet)*/

    //println(("aaaaaaaaaaaaaa").hashCode() & Integer.MAX_VALUE)


    val list  = new util.ArrayList[String]

    DataRelationStreaming.relationFields.foreach(x=>{
      //把这个list当成hbase
      list.add(x)
    })

  }
}
