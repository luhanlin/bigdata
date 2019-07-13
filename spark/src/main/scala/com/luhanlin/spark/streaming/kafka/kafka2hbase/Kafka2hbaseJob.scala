package com.luhanlin.spark.streaming.kafka.kafka2hbase

import java.util

import com.luhanlin.hbase.hbase.insert.HBaseInsertHelper
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

/**
  * author:
  * description:
  * Date:Created in 2019-04-23 10:27
  */
object Kafka2hbaseJob extends Serializable with Logging{

    def insertHbase(wifiDS: DStream[java.util.Map[String, String]],hbase_table:String): Unit ={
       wifiDS.foreachRDD(rdd => {
        val putRDD =  rdd.map(x=>{
          val rowkey = x.get("id")
          var put = new Put((rowkey.getBytes()))
          val keys = x.keySet()
          keys.foreach(key=>{
            put.addColumn("cf".getBytes(), Bytes.toBytes(key),Bytes.toBytes(x.get(key)))
          })
          put
        })

        putRDD.foreachPartition(partion =>{
          var list = new util.ArrayList[Put]()
          while (partion.hasNext){
            val put = partion.next()
            list.add(put)
          }
          HBaseInsertHelper.put(hbase_table, list,10000)
          logInfo("批量写入" + hbase_table + ":" + list.size() + "条数据")
        })

      })
    }
}
